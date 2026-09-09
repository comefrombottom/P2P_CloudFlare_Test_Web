mergeInto(LibraryManager.library, {
    $s3dWebRTCP2PBridge: {
        clients: {},
        nextHandle: 1,

        create: function (options) {
            const handle = this.nextHandle++;
            this.clients[handle] = new this.Client(options);
            return handle;
        },

        get: function (handle) {
            return this.clients[handle] ?? null;
        },

        remove: function (handle) {
            const client = this.clients[handle];
            if (client) client.destroy();
            delete this.clients[handle];
        },

        encode: function (bytes) {
            let result = "";
            const chunkSize = 0x8000;
            for (let i = 0; i < bytes.length; i += chunkSize) {
                result += String.fromCharCode(...bytes.subarray(i, i + chunkSize));
            }
            return btoa(result);
        },

        decode: function (value) {
            const binary = atob(value || "");
            const bytes = new Uint8Array(binary.length);
            for (let i = 0; i < binary.length; ++i) bytes[i] = binary.charCodeAt(i);
            return bytes;
        },

        utf32ToString: function (ptr) {
            let result = "";
            for (let index = ptr >>> 2; HEAPU32[index] !== 0; ++index) {
                result += String.fromCodePoint(HEAPU32[index]);
            }
            return result;
        },

        writeUtf8: function (text, ptr, capacity) {
            const bytes = new TextEncoder().encode(text);
            const length = Math.min(bytes.length, Math.max(0, capacity - 1));
            HEAPU8.set(bytes.subarray(0, length), ptr);
            if (capacity > 0) HEAPU8[ptr + length] = 0;
            return bytes.length;
        },

        Client: class {
            constructor(options = {}) {
                this.signalingUrl = options.signalingURL || this.defaultSignalingUrl();
                this.peerId = options.peerID || this.createId();
                this.iceServers = options.iceServers || [{ urls: "stun:stun.cloudflare.com:3478" }];
                this.autoReconnect = options.autoReconnect !== false;
                this.socket = null;
                this.peers = new Map();
                this.events = [];
                this.state = "disconnected";
                this.roomId = null;
                this.operation = null;
                this.roomInfo = { listed: true, isOpen: true, maxParticipants: 8, properties: {} };
                this.role = "none";
                this.hostPeerId = null;
                this.memberIds = new Set();
                this.leaveRequested = true;
                this.generation = 0;
                this.reconnectTimer = null;
                this.heartbeatTimer = null;
                this.lastPongAt = 0;
                this.reconnectAttempts = 0;
                this.rejoinPending = false;
            }

            defaultSignalingUrl() {
                const scheme = globalThis.location?.protocol === "https:" ? "wss:" : "ws:";
                return `${scheme}//${globalThis.location?.host || "localhost"}/signal`;
            }

            createId() {
                return globalThis.crypto?.randomUUID?.()
                    || `${Date.now()}-${Math.random().toString(36).slice(2)}`;
            }

            enqueue(event) {
                this.events.push(event);
            }

            poll() {
                return this.events.shift() || null;
            }

            destroy() {
                this.leaveRequested = true;
                this.generation += 1;
                this.clearReconnectTimer();
                this.stopHeartbeat();
                this.closeAllPeers("destroyed", false);
                this.closeSocket(this.socket);
                this.socket = null;
            }

            start(roomId, operation, creationInfo) {
                if (this.state !== "disconnected") return false;
                this.roomId = roomId;
                this.operation = operation;
                this.roomInfo = operation === "join"
                    ? { listed: true, isOpen: true, maxParticipants: 8, properties: {} }
                    : this.normalizeRoomInfo(creationInfo);
                this.state = "joiningRoom";
                this.enqueue({ type: "state", state: this.state });
                this.role = "none";
                this.hostPeerId = null;
                this.memberIds.clear();
                this.leaveRequested = false;
                this.rejoinPending = false;
                this.reconnectAttempts = 0;
                this.clearReconnectTimer();
                this.openSignaling(false, this.generation);
                return true;
            }

            normalizeRoomInfo(info = {}) {
                const maxParticipants = Number(info.maxParticipants ?? 8);
                if (!Number.isInteger(maxParticipants) || maxParticipants < 1 || maxParticipants > 8) {
                    throw new Error("maxParticipants must be between 1 and 8");
                }
                const properties = info.properties && typeof info.properties === "object"
                    ? { ...info.properties } : {};
                return {
                    listed: info.listed !== false,
                    isOpen: info.isOpen !== false,
                    maxParticipants,
                    properties,
                };
            }

            leave() {
                if (this.state === "disconnected") return;
                this.leaveRequested = true;
                this.generation += 1;
                this.clearReconnectTimer();
                this.stopHeartbeat();
                if (this.socket?.readyState === WebSocket.OPEN) {
                    try { this.socket.send(JSON.stringify({ type: "leave" })); } catch {}
                }
                this.closeAllPeers("left", false);
                this.closeSocket(this.socket, 1000, "Left room");
                this.socket = null;
                this.state = "disconnected";
                this.enqueue({ type: "state", state: this.state });
                this.roomId = null;
                this.operation = null;
                this.role = "none";
                this.hostPeerId = null;
                this.memberIds.clear();
                this.roomInfo = { listed: true, isOpen: true, maxParticipants: 8, properties: {} };
            }

            openSignaling(reconnect, generation) {
                if (this.leaveRequested || generation !== this.generation || !this.roomId) return;
                const url = new URL(this.signalingUrl, globalThis.location?.href || undefined);
                url.searchParams.set("room", this.roomId);
                url.searchParams.set("peer", this.peerId);
                url.searchParams.set("listed", this.roomInfo.listed ? "1" : "0");
                url.searchParams.set("open", this.roomInfo.isOpen ? "1" : "0");
                url.searchParams.set("maxParticipants", String(this.roomInfo.maxParticipants));
                url.searchParams.set("config", JSON.stringify(this.roomInfo.properties));
                url.searchParams.set("operation", this.operation || "join-or-create");
                url.searchParams.set("reconnect", reconnect ? "1" : "0");

                const socket = new WebSocket(url);
                this.socket = socket;
                socket.addEventListener("open", () => {
                    if (this.socket !== socket || generation !== this.generation) return;
                    this.lastPongAt = Date.now();
                    this.reconnectAttempts = 0;
                    this.startHeartbeat(socket);
                });
                socket.addEventListener("message", (event) => {
                    if (this.socket !== socket || generation !== this.generation) return;
                    try {
                        this.handleSignal(JSON.parse(event.data)).catch((error) => {
                            this.fail(error.code || "protocol-error", error.message);
                        });
                    } catch (error) {
                        this.fail("protocol-error", error.message);
                    }
                });
                socket.addEventListener("close", () => this.handleSignalingLoss(socket));
                socket.addEventListener("error", () => this.handleSignalingLoss(socket));
            }

            async handleSignal(message) {
                switch (message.type) {
                    case "welcome":
                        this.role = message.role || "client";
                        this.hostPeerId = message.hostPeerId || null;
                        this.roomInfo = {
                            listed: message.listed !== false,
                            isOpen: message.isOpen !== false,
                            maxParticipants: Number(message.maxParticipants || 8),
                            properties: { ...(message.config || {}) },
                        };
                        this.memberIds = new Set(message.peers || []);
                        this.enqueueRoomInfo();
                        if (this.role === "host") {
                            await Promise.all([...this.memberIds].map((peerId) => this.startOffer(peerId)));
                        }
                        this.maybeCompleteJoin();
                        break;
                    case "role": {
                        const previous = this.role;
                        this.role = message.role || this.role;
                        this.hostPeerId = message.hostPeerId || this.hostPeerId;
                        if (previous !== this.role) this.enqueue({ type: "role", role: this.role, hostPeerId: this.hostPeerId || "" });
                        if (this.role === "host") {
                            await Promise.all([...this.memberIds].map((peerId) => this.startOffer(peerId, true)));
                        }
                        break;
                    }
                    case "peer-joined":
                        this.memberIds.add(message.peerId);
                        if (message.role === "host") this.hostPeerId = message.peerId;
                        this.enqueue({ type: "member-joined", peerId: message.peerId });
                        if (this.role === "host") await this.startOffer(message.peerId);
                        break;
                    case "peer-reconnected":
                        this.memberIds.add(message.peerId);
                        if (message.role === "host") this.hostPeerId = message.peerId;
                        this.removePeer(message.peerId, "reconnected", true);
                        this.enqueue({ type: "member-reconnected", peerId: message.peerId });
                        if (this.role === "host") await this.startOffer(message.peerId, true);
                        break;
                    case "peer-left":
                        this.memberIds.delete(message.peerId);
                        if (message.peerId === this.hostPeerId) this.hostPeerId = null;
                        this.enqueue({ type: "member-left", peerId: message.peerId, reason: message.reason || "disconnected" });
                        this.removePeer(message.peerId, message.reason || "disconnected", true);
                        break;
                    case "host-changed":
                        this.hostPeerId = message.hostPeerId || null;
                        if (this.role === "host") {
                            await Promise.all((message.peers || []).map((peerId) => this.startOffer(peerId, true)));
                        }
                        break;
                    case "room-config":
                        this.roomInfo.properties = { ...(message.config || {}) };
                        this.enqueueRoomInfo();
                        break;
                    case "room-open":
                        this.roomInfo.isOpen = message.isOpen !== false;
                        this.enqueueRoomInfo();
                        break;
                    case "room-error":
                        this.fail(message.code || "room-error", message.message || "Room error");
                        break;
                    case "peer-repair":
                        if (this.role === "host") await this.startOffer(message.from, true);
                        break;
                    case "offer":
                        await this.acceptOffer(message.from, message);
                        break;
                    case "answer":
                        await this.acceptAnswer(message.from, message);
                        break;
                    case "candidate":
                        await this.acceptCandidate(message.from, message);
                        break;
                    case "pong":
                        this.lastPongAt = Date.now();
                        break;
                    default:
                        break;
                }
            }

            enqueueRoomInfo() {
                this.enqueue({ type: "room-info", room: {
                    id: this.roomId || "",
                    listed: this.roomInfo.listed,
                    isOpen: this.roomInfo.isOpen,
                    participantCount: this.memberIds.size + 1,
                    maxParticipants: this.roomInfo.maxParticipants,
                    peers: [...this.memberIds],
                    properties: { ...this.roomInfo.properties },
                } });
            }

            maybeCompleteJoin() {
                if (this.state !== "joiningRoom" && this.state !== "rejoiningRoom") return;
                const connected = [...this.peers.values()].filter((peer) => peer.dataChannel?.readyState === "open").length;
                const ready = this.role === "host"
                    ? this.memberIds.size === 0 || connected > 0
                    : Boolean(this.hostPeerId && this.peers.get(this.hostPeerId)?.dataChannel?.readyState === "open");
                if (!ready) return;
                const rejoining = this.rejoinPending;
                this.state = "inRoom";
                this.enqueue({ type: "state", state: this.state });
                this.enqueue({ type: rejoining ? "reconnected" : "joined", room: {
                    id: this.roomId,
                    listed: this.roomInfo.listed,
                    isOpen: this.roomInfo.isOpen,
                    participantCount: this.memberIds.size + 1,
                    maxParticipants: this.roomInfo.maxParticipants,
                    peers: [...this.memberIds],
                    properties: { ...this.roomInfo.properties },
                } });
                this.rejoinPending = false;
            }

            createPeer(remotePeerId, createDataChannel = false) {
                const existing = this.peers.get(remotePeerId);
                if (existing) return existing;
                const connection = new RTCPeerConnection({ iceServers: this.iceServers });
                const peer = { connection, dataChannel: null, pendingCandidates: [], pendingLocalCandidates: [], negotiationId: null, localDescriptionSent: false, offerStarted: false, openNotified: false, disconnectedTimer: null };
                this.peers.set(remotePeerId, peer);
                connection.addEventListener("icecandidate", (event) => {
                    if (this.peers.get(remotePeerId) !== peer || !event.candidate) return;
                    const signal = { type: "candidate", to: remotePeerId, negotiationId: peer.negotiationId, candidate: event.candidate.toJSON?.() || event.candidate };
                    if (peer.localDescriptionSent) this.sendSignal(signal); else peer.pendingLocalCandidates.push(signal);
                });
                connection.addEventListener("connectionstatechange", () => {
                    if (this.peers.get(remotePeerId) !== peer) return;
                    const state = connection.connectionState;
                    if (state === "disconnected") {
                        if (peer.disconnectedTimer === null) peer.disconnectedTimer = setTimeout(() => {
                            peer.disconnectedTimer = null;
                            if (this.peers.get(remotePeerId) === peer && connection.connectionState === "disconnected") this.handlePeerFailure(remotePeerId, peer, "connection-disconnected");
                        }, 2000);
                    } else if (state === "failed" || state === "closed") {
                        this.handlePeerFailure(remotePeerId, peer, `connection-${state}`);
                    } else if (peer.disconnectedTimer !== null) {
                        clearTimeout(peer.disconnectedTimer);
                        peer.disconnectedTimer = null;
                    }
                });
                connection.addEventListener("datachannel", (event) => this.attachDataChannel(remotePeerId, peer, event.channel));
                if (createDataChannel) this.attachDataChannel(remotePeerId, peer, connection.createDataChannel("messages"));
                return peer;
            }

            async startOffer(remotePeerId, force = false) {
                if (force) this.removePeer(remotePeerId, "repair", true);
                const peer = this.createPeer(remotePeerId, true);
                if (peer.offerStarted) return;
                peer.offerStarted = true;
                peer.negotiationId = this.createId();
                const offer = await peer.connection.createOffer();
                await peer.connection.setLocalDescription(offer);
                this.sendLocalDescription(peer, { type: "offer", to: remotePeerId, negotiationId: peer.negotiationId, offer: peer.connection.localDescription });
            }

            async acceptOffer(remotePeerId, message) {
                if (!remotePeerId || !message.negotiationId) return;
                const previous = this.peers.get(remotePeerId);
                if (previous && previous.negotiationId !== message.negotiationId) this.removePeer(remotePeerId, "renegotiated", true);
                const peer = this.createPeer(remotePeerId, false);
                peer.negotiationId = message.negotiationId;
                await peer.connection.setRemoteDescription(message.offer);
                await this.flushCandidates(peer);
                const answer = await peer.connection.createAnswer();
                await peer.connection.setLocalDescription(answer);
                this.sendLocalDescription(peer, { type: "answer", to: remotePeerId, negotiationId: peer.negotiationId, answer: peer.connection.localDescription });
            }

            async acceptAnswer(remotePeerId, message) {
                const peer = this.peers.get(remotePeerId);
                if (!peer || peer.negotiationId !== message.negotiationId) return;
                await peer.connection.setRemoteDescription(message.answer);
                await this.flushCandidates(peer);
            }

            async acceptCandidate(remotePeerId, message) {
                const peer = this.peers.get(remotePeerId);
                if (!peer || peer.negotiationId !== message.negotiationId || !message.candidate) return;
                if (!peer.connection.remoteDescription) peer.pendingCandidates.push(message.candidate);
                else await peer.connection.addIceCandidate(message.candidate);
            }

            async flushCandidates(peer) {
                for (const candidate of peer.pendingCandidates.splice(0)) await peer.connection.addIceCandidate(candidate);
            }

            sendLocalDescription(peer, signal) {
                if (!this.sendSignal(signal)) return;
                peer.localDescriptionSent = true;
                for (const candidate of peer.pendingLocalCandidates.splice(0)) this.sendSignal(candidate);
            }

            attachDataChannel(remotePeerId, peer, channel) {
                peer.dataChannel = channel;
                channel.binaryType = "arraybuffer";
                channel.addEventListener("open", () => {
                    if (this.peers.get(remotePeerId) !== peer || peer.dataChannel !== channel) return;
                    if (!peer.openNotified) {
                        peer.openNotified = true;
                        this.enqueue({ type: "peer-connected", peerId: remotePeerId });
                        this.maybeCompleteJoin();
                    }
                });
                channel.addEventListener("message", (event) => {
                    if (this.peers.get(remotePeerId) !== peer || peer.dataChannel !== channel) return;
                    let packet;
                    try { packet = JSON.parse(typeof event.data === "string" ? event.data : new TextDecoder().decode(event.data)); } catch { return; }
                    if (!packet || packet.kind !== "app-message" || !Number.isInteger(packet.id) || typeof packet.payload !== "string") return;
                    if (this.role === "host") this.routeApplicationMessage(remotePeerId, packet.id, packet.payload, packet.target);
                    else this.enqueue({ type: "message", from: packet.from || remotePeerId, id: packet.id, payload: packet.payload });
                });
                channel.addEventListener("close", () => {
                    if (this.peers.get(remotePeerId) === peer && peer.dataChannel === channel) this.handlePeerFailure(remotePeerId, peer, "datachannel-closed");
                });
                channel.addEventListener("error", () => {
                    if (this.peers.get(remotePeerId) === peer && peer.dataChannel === channel) this.handlePeerFailure(remotePeerId, peer, "datachannel-error");
                });
            }

            handlePeerFailure(remotePeerId, peer, reason) {
                if (this.peers.get(remotePeerId) !== peer || this.leaveRequested) return;
                this.removePeer(remotePeerId, reason, true);
                if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
                if (this.role === "host") this.startOffer(remotePeerId, true).catch(() => {});
                else if (remotePeerId === this.hostPeerId) this.sendSignal({ type: "peer-repair", to: this.hostPeerId });
            }

            send(messageID, payload, target) {
                if (this.state !== "inRoom" && this.state !== "joiningRoom" && this.state !== "rejoiningRoom") return 1;
                const packet = { kind: "app-message", id: messageID >>> 0, from: this.peerId, payload, target };
                if (this.role !== "host") {
                    const channel = this.peers.get(this.hostPeerId)?.dataChannel;
                    if (!channel || channel.readyState !== "open") return 2;
                    try { channel.send(JSON.stringify(packet)); return 0; } catch { return 2; }
                }
                this.routeApplicationMessage(this.peerId, packet.id, packet.payload, packet.target);
                return 0;
            }

            routeApplicationMessage(sourcePeerId, messageID, payload, target) {
                const recipients = new Set();
                if (target === "all") {
                    recipients.add(this.peerId);
                    for (const peerId of this.peers.keys()) recipients.add(peerId);
                } else if (target === "others") {
                    for (const peerId of this.peers.keys()) recipients.add(peerId);
                    recipients.delete(sourcePeerId);
                    if (sourcePeerId !== this.peerId) {
                        this.enqueue({ type: "message", from: sourcePeerId, id: messageID >>> 0, payload });
                    }
                } else if (Array.isArray(target)) {
                    for (const peerId of target) recipients.add(peerId);
                } else {
                    return;
                }
                const packet = JSON.stringify({ kind: "app-message", id: messageID >>> 0, from: sourcePeerId, payload });
                if (recipients.has(this.peerId)) this.enqueue({ type: "message", from: sourcePeerId, id: messageID >>> 0, payload });
                for (const peerId of recipients) {
                    if (peerId === this.peerId) continue;
                    const channel = this.peers.get(peerId)?.dataChannel;
                    if (channel?.readyState === "open") {
                        try { channel.send(packet); } catch {}
                    }
                }
            }

            setProperty(key, value) {
                if (!this.sendSignal({ type: "room-config-set", key: String(key), value: String(value) })) return false;
                this.roomInfo.properties[String(key)] = String(value);
                return true;
            }

            setOpen(isOpen) {
                if (!this.sendSignal({ type: "room-open-set", isOpen: Boolean(isOpen) })) return false;
                this.roomInfo.isOpen = Boolean(isOpen);
                return true;
            }

            async refreshRoomList() {
                try {
                    const url = new URL(this.signalingUrl, globalThis.location?.href || undefined);
                    url.protocol = url.protocol === "wss:" ? "https:" : "http:";
                    url.pathname = "/rooms";
                    url.search = "";
                    const response = await fetch(url);
                    const body = await response.json();
                    this.enqueue({ type: "room-list", rooms: body.rooms || [] });
                    return true;
                } catch {
                    this.enqueue({ type: "error", code: "directory-error", message: "Failed to fetch room list" });
                    return false;
                }
            }

            handleSignalingLoss(socket) {
                if (this.socket !== socket) return;
                this.socket = null;
                this.stopHeartbeat();
                this.closeSocket(socket);
                if (this.leaveRequested) return;
                if (this.state === "inRoom") {
                    this.state = "rejoiningRoom";
                    this.rejoinPending = true;
                    this.enqueue({ type: "state", state: this.state });
                    this.enqueue({ type: "reconnecting", reason: "signaling-lost" });
                    this.closeAllPeers("signaling-lost", true);
                }
                if (this.autoReconnect) this.scheduleReconnect();
                else this.fail("signaling-closed", "Signaling connection closed");
            }

            scheduleReconnect() {
                if (this.reconnectTimer || this.leaveRequested) return;
                const delay = Math.min(1000 * (2 ** this.reconnectAttempts), 10000);
                this.reconnectAttempts += 1;
                this.reconnectTimer = setTimeout(() => {
                    this.reconnectTimer = null;
                    this.openSignaling(true, this.generation);
                }, delay);
            }

            startHeartbeat(socket) {
                this.stopHeartbeat();
                this.heartbeatTimer = setInterval(() => {
                    if (this.socket !== socket) return;
                    if (Date.now() - this.lastPongAt > 25000) {
                        this.handleSignalingLoss(socket);
                        return;
                    }
                    this.sendSignal({ type: "ping", timestamp: Date.now() });
                }, 10000);
            }

            stopHeartbeat() {
                if (this.heartbeatTimer !== null) clearInterval(this.heartbeatTimer);
                this.heartbeatTimer = null;
            }

            clearReconnectTimer() {
                if (this.reconnectTimer !== null) clearTimeout(this.reconnectTimer);
                this.reconnectTimer = null;
            }

            sendSignal(message) {
                if (this.socket?.readyState !== WebSocket.OPEN) return false;
                try { this.socket.send(JSON.stringify(message)); return true; } catch { return false; }
            }

            removePeer(remotePeerId, reason = "disconnected", notify = true) {
                const peer = this.peers.get(remotePeerId);
                if (!peer) return;
                this.peers.delete(remotePeerId);
                if (peer.disconnectedTimer !== null) clearTimeout(peer.disconnectedTimer);
                const wasOpen = peer.openNotified;
                try { peer.dataChannel?.close(); } catch {}
                try { peer.connection.close(); } catch {}
                if (notify && wasOpen) this.enqueue({ type: "peer-disconnected", peerId: remotePeerId, reason });
            }

            closeAllPeers(reason, notify) {
                for (const peerId of [...this.peers.keys()]) this.removePeer(peerId, reason, notify);
            }

            closeSocket(socket, code, reason) {
                if (!socket) return;
                try { socket.close(code, reason); } catch {}
            }

            fail(code, message) {
                this.enqueue({ type: "error", code, message });
            }
        },
    },

    s3dWebRTCP2PCreate: function (options_ptr) {
        const options = JSON.parse(s3dWebRTCP2PBridge.utf32ToString(options_ptr));
        return s3dWebRTCP2PBridge.create(options);
    },
    s3dWebRTCP2PCreate__sig: "ii",
    s3dWebRTCP2PCreate__deps: ["$s3dWebRTCP2PBridge"],

    s3dWebRTCP2PDestroy: function (handle) {
        s3dWebRTCP2PBridge.remove(handle);
    },
    s3dWebRTCP2PDestroy__sig: "vi",
    s3dWebRTCP2PDestroy__deps: ["$s3dWebRTCP2PBridge"],

    s3dWebRTCP2PStart: function (handle, operation, room_ptr, info_ptr) {
        const client = s3dWebRTCP2PBridge.get(handle);
        if (!client) return 0;
        let info = {};
        if (info_ptr) info = JSON.parse(s3dWebRTCP2PBridge.utf32ToString(info_ptr));
        return client.start(s3dWebRTCP2PBridge.utf32ToString(room_ptr), operation === 0 ? "create" : operation === 1 ? "join" : "join-or-create", info) ? 1 : 0;
    },
    s3dWebRTCP2PStart__sig: "iiiii",
    s3dWebRTCP2PStart__deps: ["$s3dWebRTCP2PBridge"],

    s3dWebRTCP2PLeave: function (handle) {
        s3dWebRTCP2PBridge.get(handle)?.leave();
    },
    s3dWebRTCP2PLeave__sig: "vi",
    s3dWebRTCP2PLeave__deps: ["$s3dWebRTCP2PBridge"],

    s3dWebRTCP2PUpdate: function (handle, out_ptr, out_size) {
        const event = s3dWebRTCP2PBridge.get(handle)?.poll();
        if (!event) return 0;
        const text = JSON.stringify(event);
        return s3dWebRTCP2PBridge.writeUtf8(text, out_ptr, out_size);
    },
    s3dWebRTCP2PUpdate__sig: "iiii",
    s3dWebRTCP2PUpdate__deps: ["$s3dWebRTCP2PBridge"],

    s3dWebRTCP2PSend: function (handle, message_id, payload_ptr, payload_size, target_type, peer_ids_ptr) {
        const client = s3dWebRTCP2PBridge.get(handle);
        if (!client) return 1;
        const bytes = HEAPU8.slice(payload_ptr, payload_ptr + payload_size);
        let target = target_type === 1 ? "all" : "others";
        if (target_type === 2) target = JSON.parse(s3dWebRTCP2PBridge.utf32ToString(peer_ids_ptr));
        return client.send(message_id, s3dWebRTCP2PBridge.encode(bytes), target);
    },
    s3dWebRTCP2PSend__sig: "iiiiiii",
    s3dWebRTCP2PSend__deps: ["$s3dWebRTCP2PBridge"],

    s3dWebRTCP2PSetProperty: function (handle, key, value_ptr) {
        return s3dWebRTCP2PBridge.get(handle)?.setProperty(key, s3dWebRTCP2PBridge.utf32ToString(value_ptr)) ? 1 : 0;
    },
    s3dWebRTCP2PSetProperty__sig: "iiii",
    s3dWebRTCP2PSetProperty__deps: ["$s3dWebRTCP2PBridge"],

    s3dWebRTCP2PSetOpen: function (handle, is_open) {
        return s3dWebRTCP2PBridge.get(handle)?.setOpen(Boolean(is_open)) ? 1 : 0;
    },
    s3dWebRTCP2PSetOpen__sig: "iii",
    s3dWebRTCP2PSetOpen__deps: ["$s3dWebRTCP2PBridge"],

    s3dWebRTCP2PRefreshRooms: function (handle) {
        const client = s3dWebRTCP2PBridge.get(handle);
        if (!client) return 0;
        client.refreshRoomList();
        return 1;
    },
    s3dWebRTCP2PRefreshRooms__sig: "ii",
    s3dWebRTCP2PRefreshRooms__deps: ["$s3dWebRTCP2PBridge"],
});
