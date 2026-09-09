# include <Siv3D.hpp>
# include "WebRTCP2PClient.hpp"

namespace
{
	class TestClient final : public s3d::WebRTCP2PClient
	{
	public:
		TestClient()
			: WebRTCP2PClient([]
			{
				ClientOptions options;
				options.signalingURL = U"wss://webrtc-p2p-signaling.webrtc-p2p-demo.workers.dev/signal";
				return options;
			}())
		{
		}

		Array<String> logs;

		void log(String message)
		{
			logs << std::move(message);
			if (logs.size() > 18) logs.erase(logs.begin());
		}

		void onStateChanged(ClientState state) override { log(U"state: {}"_fmt(static_cast<int32>(state))); }
		void onJoinedRoom(const RoomInfo& roomInfo) override { log(U"joined: {}"_fmt(roomInfo.id)); }
		void onReconnectingRoom(ReconnectReason) override { log(U"reconnecting"); }
		void onReconnectedRoom() override { log(U"reconnected"); }
		void onRoomError(const RoomError& error) override { log(U"error: {}"_fmt(error.message)); }
		void onMemberJoined(const PeerID& peerID) override { log(U"member joined: {}"_fmt(peerID)); }
		void onMemberReconnected(const PeerID& peerID) override { log(U"member reconnected: {}"_fmt(peerID)); }
		void onMemberLeft(const PeerID& peerID, MemberLeaveReason) override { log(U"member left: {}"_fmt(peerID)); }
		void onPeerConnected(const PeerID& peerID) override { log(U"peer connected: {}"_fmt(peerID)); }
		void onPeerDisconnected(const PeerID& peerID) override { log(U"peer disconnected: {}"_fmt(peerID)); }

		void onMessage(const PeerID& sender, MessageID messageID, Deserializer<MemoryViewReader>& reader) override
		{
			String message;
			reader(message);
			log(U"message {} from {}: {}"_fmt(messageID, sender, message));
		}
	};
}

void Main()
{
	Scene::SetBackground(ColorF{ 0.93, 0.95, 0.98 });

	TestClient client;
	TextEditState roomInput{ U"siv3d-test-room" };
	TextEditState messageInput{ U"hello from Siv3D for Web" };
	bool listed = true;
	bool isOpen = true;
	const Font titleFont{ 20 };
	const Font bodyFont{ 16 };
	const Font logFont{ 14 };

	while (System::Update())
	{
		client.update();

		SimpleGUI::TextBox(roomInput, Vec2{ 20, 20 }, 260, 64);
		SimpleGUI::TextBox(messageInput, Vec2{ 20, 70 }, 260, 256);
		SimpleGUI::CheckBox(listed, U"listed", Vec2{ 300, 20 }, 100);
		SimpleGUI::CheckBox(isOpen, U"open", Vec2{ 300, 60 }, 100);

		if (SimpleGUI::Button(U"Join or Create", Vec2{ 20, 120 }, 150))
		{
			WebRTCP2PClient::RoomCreationInfo info;
			info.listed = listed;
			info.isOpen = isOpen;
			info.maxParticipants = 8;
			info.properties[0] = U"siv3d-web-test";
			client.joinOrCreateRoom(roomInput.text, info);
		}

		if (SimpleGUI::Button(U"Refresh Rooms", Vec2{ 180, 120 }, 130)) client.refreshRoomList();
		if (SimpleGUI::Button(U"Send All", Vec2{ 320, 120 }, 110)) client.send(1, WebRTCP2PClient::SendTarget::All(), messageInput.text);
		if (SimpleGUI::Button(U"Send Others", Vec2{ 440, 120 }, 130)) client.send(2, WebRTCP2PClient::SendTarget::Others(), messageInput.text);
		if (SimpleGUI::Button(U"Leave", Vec2{ 580, 120 }, 100)) client.leaveRoom();

		titleFont(U"WebRTCP2PClient test").draw(20, 180, Palette::Black);
		bodyFont(U"Peer ID: {}"_fmt(client.getPeerID())).draw(20, 215, Palette::Black);
		bodyFont(U"Room: {}"_fmt(client.getRoomID())).draw(20, 245, Palette::Black);
		bodyFont(U"Logs").draw(20, 285, Palette::Black);

		for (size_t i = 0; i < client.logs.size(); ++i)
		{
			logFont(client.logs[i]).draw(20, 315 + i * 21, Palette::Black);
		}
	}
}
