# include "WebRTCP2PClient.hpp"

# include <cstring>

namespace
{
	using Client = s3d::WebRTCP2PClient;

	extern "C"
	{
		int32 s3dWebRTCP2PCreate(const char32* options);
		void s3dWebRTCP2PDestroy(int32 handle);
		int32 s3dWebRTCP2PStart(int32 handle, int32 operation, const char32* roomID, const char32* roomInfo);
		void s3dWebRTCP2PLeave(int32 handle);
		int32 s3dWebRTCP2PUpdate(int32 handle, char* output, int32 outputSize);
		int32 s3dWebRTCP2PSend(int32 handle, uint32 messageID, const void* payload, int32 payloadSize, int32 targetType, const char32* peerIDs);
		int32 s3dWebRTCP2PSetProperty(int32 handle, int32 key, const char32* value);
		int32 s3dWebRTCP2PSetOpen(int32 handle, int32 isOpen);
		int32 s3dWebRTCP2PRefreshRooms(int32 handle);
	}

	s3d::String MakePropertiesJSON(const Client::RoomPropertyTable& properties)
	{
		s3d::JSON json = s3d::JSON::Parse(U"{}", s3d::AllowExceptions::No);
		for (const auto& [key, value] : properties)
		{
			json[s3d::Format(key)] = value;
		}
		return json.formatMinimum();
	}

	s3d::String MakeClientOptionsJSON(const Client::ClientOptions& options, const s3d::String& peerID)
	{
		s3d::JSON json = s3d::JSON::Parse(U"{}", s3d::AllowExceptions::No);
		json[U"signalingURL"] = options.signalingURL;
		json[U"peerID"] = peerID;
		json[U"autoReconnect"] = options.autoReconnect;

		s3d::Array<s3d::JSON> iceServers;
		for (const auto& server : options.iceServers)
		{
			s3d::JSON ice = s3d::JSON::Parse(U"{}", s3d::AllowExceptions::No);
			ice[U"urls"] = server.url;
			if (not server.username.isEmpty()) ice[U"username"] = server.username;
			if (not server.credential.isEmpty()) ice[U"credential"] = server.credential;
			iceServers << std::move(ice);
		}
		if (not iceServers.isEmpty()) json[U"iceServers"] = iceServers;
		return json.formatMinimum();
	}

	int32 StateFromString(s3d::StringView value)
	{
		if (value == U"joiningRoom") return 1;
		if (value == U"inRoom") return 2;
		if (value == U"leavingRoom") return 3;
		if (value == U"rejoiningRoom") return 4;
		return 0;
	}
}

namespace s3d
{
	WebRTCP2PClient::SendTarget::SendTarget(Type type, Array<PeerID> peerIDs)
		: m_type(type)
		, m_peerIDs(std::move(peerIDs))
	{
	}

	WebRTCP2PClient::SendTarget WebRTCP2PClient::SendTarget::Others()
	{
		return SendTarget{ Type::Others, {} };
	}

	WebRTCP2PClient::SendTarget WebRTCP2PClient::SendTarget::All()
	{
		return SendTarget{ Type::All, {} };
	}

	WebRTCP2PClient::SendTarget WebRTCP2PClient::SendTarget::Peers(const Array<PeerID>& peerIDs)
	{
		return SendTarget{ Type::PeerList, peerIDs };
	}

	bool WebRTCP2PClient::SendTarget::isOthers() const noexcept { return m_type == Type::Others; }
	bool WebRTCP2PClient::SendTarget::isAll() const noexcept { return m_type == Type::All; }
	bool WebRTCP2PClient::SendTarget::isPeerList() const noexcept { return m_type == Type::PeerList; }
	const Array<WebRTCP2PClient::PeerID>& WebRTCP2PClient::SendTarget::peerIDs() const noexcept { return m_peerIDs; }

	class WebRTCP2PClient::Impl
	{
	public:
		ClientOptions options;
		int32 handle = 0;
		ClientState state = ClientState::Disconnected;
		Role role = Role::None;
		PeerID peerID;
		RoomID roomID;
		PeerID hostPeerID;
		Array<PeerID> peerIDs;
		Array<PeerID> connectedPeerIDs;
		RoomInfo roomInfo;
		Array<RoomInfo> roomList;

		Impl(const ClientOptions& value, WebRTCP2PClient* owner)
			: options(value)
			, peerID(value.peerID.isEmpty() ? UUIDValue::Generate().str() : value.peerID)
			, owner(owner)
		{
			const String json = MakeClientOptionsJSON(options, peerID);
			handle = s3dWebRTCP2PCreate(json.data());
		}

		~Impl()
		{
			if (handle != 0) s3dWebRTCP2PDestroy(handle);
		}

		void update()
		{
			if (handle == 0) return;
			std::string buffer(256 * 1024, '\0');
			for (;;)
			{
				const int32 length = s3dWebRTCP2PUpdate(handle, buffer.data(), static_cast<int32>(buffer.size()));
				if (length <= 0) return;
				const String eventText = Unicode::FromUTF8(std::string_view{ buffer.data(), static_cast<size_t>(length) });
				handleEvent(JSON::Parse(eventText, AllowExceptions::No));
			}
		}

		void handleEvent(const JSON& event)
		{
			if (not event.isObject() || not event.hasElement(U"type")) return;
			const String type = event[U"type"].getString();

			if (type == U"state")
			{
				state = static_cast<ClientState>(StateFromString(event[U"state"].getString()));
				if (state == ClientState::Disconnected || state == ClientState::LeavingRoom)
				{
					peerIDs.clear();
					connectedPeerIDs.clear();
				}
				owner->onStateChanged(state);
				return;
			}
			if (type == U"joined" || type == U"reconnected")
			{
				updateRoomInfo(event[U"room"]);
				updatePeerIDs(event[U"room"]);
				if (type == U"joined") owner->onJoinedRoom(roomInfo);
				else owner->onReconnectedRoom();
				return;
			}
			if (type == U"reconnecting")
			{
				owner->onReconnectingRoom(ReconnectReason::SignalingLost);
				return;
			}
			if (type == U"role")
			{
				const Role previous = role;
				role = RoleFromString(event[U"role"].getString());
				hostPeerID = event[U"hostPeerId"].getString();
				if (previous != role) owner->onRoleChanged(role, hostPeerID);
				return;
			}
			if (type == U"member-joined")
			{
				const PeerID peerID = event[U"peerId"].getString();
				addUnique(peerIDs, peerID);
				owner->onMemberJoined(peerID);
				return;
			}
			if (type == U"member-reconnected")
			{
				const PeerID peerID = event[U"peerId"].getString();
				addUnique(peerIDs, peerID);
				owner->onMemberReconnected(peerID);
				return;
			}
			if (type == U"member-left")
			{
				const PeerID peerID = event[U"peerId"].getString();
				removeValue(peerIDs, peerID);
				removeValue(connectedPeerIDs, peerID);
				owner->onMemberLeft(peerID, LeaveReasonFromString(event[U"reason"].getString()));
				return;
			}
			if (type == U"peer-connected")
			{
				const PeerID peerID = event[U"peerId"].getString();
				addUnique(connectedPeerIDs, peerID);
				owner->onPeerConnected(peerID);
				return;
			}
			if (type == U"peer-disconnected")
			{
				const PeerID peerID = event[U"peerId"].getString();
				removeValue(connectedPeerIDs, peerID);
				owner->onPeerDisconnected(peerID);
				return;
			}
			if (type == U"room-info")
			{
				updateRoomInfo(event[U"room"]);
				owner->onRoomInfoChanged(roomInfo);
				return;
			}
			if (type == U"room-list")
			{
				roomList.clear();
				const JSON rooms = event[U"rooms"];
				for (size_t i = 0; i < rooms.size(); ++i)
				{
					RoomInfo info;
					readRoomInfo(info, rooms[i]);
					roomList << std::move(info);
				}
				owner->onRoomListUpdated();
				return;
			}
			if (type == U"message")
			{
				const Blob payload = Base64::Decode(event[U"payload"].getString(), SkipValidation::Yes);
				Deserializer<MemoryViewReader> reader(payload.data(), payload.size());
				owner->onMessage(event[U"from"].getString(), event[U"id"].get<uint32>(), reader);
				return;
			}
			if (type == U"error")
			{
				RoomError error;
				error.code = ErrorCodeFromString(event[U"code"].getString());
				error.message = event[U"message"].getString();
				owner->onRoomError(error);
				return;
			}
		}

		static Role RoleFromString(StringView value)
		{
			if (value == U"host") return Role::Host;
			if (value == U"client") return Role::Client;
			return Role::None;
		}

		static MemberLeaveReason LeaveReasonFromString(StringView value)
		{
			if (value == U"left") return MemberLeaveReason::Left;
			if (value == U"replaced" || value == U"reconnected") return MemberLeaveReason::Replaced;
			if (value == U"room-closed") return MemberLeaveReason::RoomClosed;
			return MemberLeaveReason::Disconnected;
		}

		static RoomErrorCode ErrorCodeFromString(StringView value)
		{
			if (value == U"room-not-found") return RoomErrorCode::RoomNotFound;
			if (value == U"room-exists") return RoomErrorCode::RoomAlreadyExists;
			if (value == U"room-closed") return RoomErrorCode::RoomClosed;
			if (value == U"room-full") return RoomErrorCode::RoomFull;
			if (value == U"signaling-closed") return RoomErrorCode::SignalingError;
			return RoomErrorCode::Unknown;
		}

		void updateRoomInfo(const JSON& value)
		{
			readRoomInfo(roomInfo, value);
			roomID = roomInfo.id;
		}

		void updatePeerIDs(const JSON& value)
		{
			peerIDs.clear();
			if (not value.hasElement(U"peers")) return;
			const JSON peers = value[U"peers"];
			for (size_t i = 0; i < peers.size(); ++i) addUnique(peerIDs, peers[i].getString());
		}

		static void addUnique(Array<PeerID>& values, const PeerID& value)
		{
			if (not values.contains(value)) values << value;
		}

		static void removeValue(Array<PeerID>& values, const PeerID& value)
		{
			values.remove(value);
		}

		static void readRoomInfo(RoomInfo& destination, const JSON& value)
		{
			if (value.hasElement(U"id")) destination.id = value[U"id"].getString();
			else if (value.hasElement(U"roomId")) destination.id = value[U"roomId"].getString();
			destination.listed = value[U"listed"].getOr<bool>(true);
			destination.isOpen = value[U"isOpen"].getOr<bool>(true);
			destination.participantCount = value[U"participantCount"].getOr<int32>(0);
			destination.maxParticipants = value[U"maxParticipants"].getOr<int32>(8);
			destination.properties.clear();
			const JSON properties = value.hasElement(U"properties") ? value[U"properties"] : JSON::Parse(U"{}", AllowExceptions::No);
			if (properties.isObject())
			{
				for (const auto& item : properties)
				{
					try
					{
						destination.properties.emplace(ParseInt<uint8>(item.key), item.value.getString());
					}
					catch (...) {}
				}
			}
		}

		WebRTCP2PClient* owner;
	};

	WebRTCP2PClient::WebRTCP2PClient()
		: WebRTCP2PClient(ClientOptions{})
	{
	}

	WebRTCP2PClient::WebRTCP2PClient(const ClientOptions& options)
		: m_impl(std::make_unique<Impl>(options, this))
	{
	}

	WebRTCP2PClient::~WebRTCP2PClient() = default;

	void WebRTCP2PClient::update()
	{
		m_impl->update();
	}

	WebRTCP2PClient::ClientState WebRTCP2PClient::getState() const noexcept { return m_impl->state; }
	WebRTCP2PClient::Role WebRTCP2PClient::getRole() const noexcept { return m_impl->role; }
	const WebRTCP2PClient::PeerID& WebRTCP2PClient::getPeerID() const noexcept { return m_impl->peerID; }
	const WebRTCP2PClient::RoomID& WebRTCP2PClient::getRoomID() const noexcept { return m_impl->roomID; }
	const WebRTCP2PClient::PeerID& WebRTCP2PClient::getHostPeerID() const noexcept { return m_impl->hostPeerID; }
	Array<WebRTCP2PClient::PeerID> WebRTCP2PClient::getPeerIDs() const { return m_impl->peerIDs; }
	Array<WebRTCP2PClient::PeerID> WebRTCP2PClient::getConnectedPeerIDs() const { return m_impl->connectedPeerIDs; }
	const WebRTCP2PClient::RoomInfo& WebRTCP2PClient::getRoomInfo() const noexcept { return m_impl->roomInfo; }
	const Array<WebRTCP2PClient::RoomInfo>& WebRTCP2PClient::getRoomList() const noexcept { return m_impl->roomList; }

	bool WebRTCP2PClient::createRoom(const RoomID& roomID)
	{
		return createRoom(roomID, RoomCreationInfo{});
	}

	bool WebRTCP2PClient::createRoom(const RoomID& roomID, const RoomCreationInfo& info)
	{
		JSON json = JSON::Parse(U"{}", AllowExceptions::No);
		json[U"listed"] = info.listed;
		json[U"isOpen"] = info.isOpen;
		json[U"maxParticipants"] = info.maxParticipants;
		json[U"properties"] = JSON::Parse(MakePropertiesJSON(info.properties), AllowExceptions::No);
		return s3dWebRTCP2PStart(m_impl->handle, 0, roomID.data(), json.formatMinimum().data()) != 0;
	}

	bool WebRTCP2PClient::joinRoom(const RoomID& roomID)
	{
		return s3dWebRTCP2PStart(m_impl->handle, 1, roomID.data(), nullptr) != 0;
	}

	bool WebRTCP2PClient::joinOrCreateRoom(const RoomID& roomID)
	{
		return joinOrCreateRoom(roomID, RoomCreationInfo{});
	}

	bool WebRTCP2PClient::joinOrCreateRoom(const RoomID& roomID, const RoomCreationInfo& info)
	{
		JSON json = JSON::Parse(U"{}", AllowExceptions::No);
		json[U"listed"] = info.listed;
		json[U"isOpen"] = info.isOpen;
		json[U"maxParticipants"] = info.maxParticipants;
		json[U"properties"] = JSON::Parse(MakePropertiesJSON(info.properties), AllowExceptions::No);
		return s3dWebRTCP2PStart(m_impl->handle, 2, roomID.data(), json.formatMinimum().data()) != 0;
	}

	void WebRTCP2PClient::leaveRoom() { s3dWebRTCP2PLeave(m_impl->handle); }
	bool WebRTCP2PClient::refreshRoomList() { return s3dWebRTCP2PRefreshRooms(m_impl->handle) != 0; }
	bool WebRTCP2PClient::setRoomOpen(bool isOpen) { return s3dWebRTCP2PSetOpen(m_impl->handle, isOpen ? 1 : 0) != 0; }

	bool WebRTCP2PClient::setRoomProperty(uint8 key, StringView value)
	{
		const String copy{ value };
		return s3dWebRTCP2PSetProperty(m_impl->handle, key, copy.data()) != 0;
	}

	String WebRTCP2PClient::getRoomProperty(uint8 key) const
	{
		if (const auto it = m_impl->roomInfo.properties.find(key); it != m_impl->roomInfo.properties.end()) return it->second;
		return {};
	}

	const WebRTCP2PClient::RoomPropertyTable& WebRTCP2PClient::getRoomProperties() const noexcept { return m_impl->roomInfo.properties; }

	WebRTCP2PClient::SendResult WebRTCP2PClient::send(MessageID messageID, const Payload& payload, const SendTarget& target)
	{
		int32 targetType = 0;
		if (target.isAll()) targetType = 1;
		else if (target.isPeerList()) targetType = 2;

		String peerIDsJSON;
		const char32* peerIDs = nullptr;
		if (target.isPeerList())
		{
			Array<JSON> values;
			for (const auto& peerID : target.peerIDs()) values << JSON{ peerID };
			peerIDsJSON = JSON{ values }.formatMinimum();
			peerIDs = peerIDsJSON.data();
		}

		const int32 result = s3dWebRTCP2PSend(m_impl->handle, messageID, payload.data(), static_cast<int32>(payload.size()), targetType, peerIDs);
		return static_cast<SendResult>(result);
	}

	WebRTCP2PClient::SendResult WebRTCP2PClient::send(MessageID messageID, const Serializer<MemoryWriter>& writer, const SendTarget& target)
	{
		Payload payload(static_cast<size_t>(writer->size()));
		if (not payload.isEmpty()) std::memcpy(payload.data(), writer->getBlob().data(), payload.size_bytes());
		return send(messageID, payload, target);
	}
}
