const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// rooms: Map of roomId -> { director, cameras, viewers, tallyState }
const rooms = new Map();
const generateRoomId = () => Math.random().toString(36).slice(2, 8).toUpperCase();
const generateViewerId = () => "viewer-" + Math.random().toString(36).slice(2, 8);

const getOrCreateRoom = (roomId) => {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, {
      director: null,
      cameras: new Map(),   // cameraId -> { ws, name, label }
      viewers: new Map(),   // viewerId -> ws
      tallyState: {},       // cameraId -> "program" | "preview" | "idle"
      slots: [],            // configured camera slots from director
    });
  }
  return rooms.get(roomId);
};

const send = (ws, msg) => {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg));
};

console.log(`PGM signaling server running on port ${PORT}`);

wss.on("connection", (ws) => {
  let clientRole     = null;
  let clientRoomId   = null;
  let clientCameraId = null;
  let clientViewerId = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // Normalize room IDs to uppercase so "ab12cd" and "AB12CD" are the same room
    if (msg.roomId) msg.roomId = msg.roomId.toUpperCase().trim();

    switch (msg.type) {

      case "register": {
        clientRole = msg.role;

        // ── Director ──────────────────────────────────────────────────
        if (msg.role === "director") {
          clientRoomId = msg.roomId || generateRoomId();
          const room = getOrCreateRoom(clientRoomId);
          room.director = ws;
          console.log(`🎬 Director connected to room ${clientRoomId}`);
          send(ws, { type: "room-assigned", roomId: clientRoomId });
          // Send existing cameras
          room.cameras.forEach(({ name, label }, cameraId) => {
            send(ws, { type: "camera-connected", cameraId, name, label });
          });
        }

        // ── Camera ────────────────────────────────────────────────────
        if (msg.role === "camera") {
          clientRoomId   = msg.roomId || "default";
          clientCameraId = msg.cameraId;
          const room = getOrCreateRoom(clientRoomId);
          room.cameras.set(msg.cameraId, { ws, name: msg.name, label: msg.label });
          console.log(`📷 Camera ${msg.name} in room ${clientRoomId}`);
          // Notify director
          if (room.director?.readyState === 1) {
            send(room.director, { type: "camera-connected", cameraId: msg.cameraId, name: msg.name, label: msg.label });
          }
          // Notify viewers
          room.viewers.forEach(viewerWs => {
            send(viewerWs, { type: "camera-connected", cameraId: msg.cameraId, name: msg.name, label: msg.label });
          });
        }

        // ── Viewer ────────────────────────────────────────────────────
        if (msg.role === "viewer") {
          clientRoomId   = msg.roomId || "default";
          clientViewerId = generateViewerId();
          const room = getOrCreateRoom(clientRoomId);
          room.viewers.set(clientViewerId, ws);
          console.log(`👁  Viewer ${clientViewerId} in room ${clientRoomId}`);
          // Send viewer its ID
          send(ws, { type: "viewer-assigned", viewerId: clientViewerId });
          // Send existing cameras
          room.cameras.forEach(({ name, label }, cameraId) => {
            send(ws, { type: "camera-connected", cameraId, name, label });
          });
          // Send configured slots (includes disconnected cameras)
          if (room.slots.length > 0) {
            send(ws, { type: "room-slots", slots: room.slots });
          }
          // Send current tally state
          Object.entries(room.tallyState).forEach(([cameraId, state]) => {
            send(ws, { type: "tally", cameraId, state });
          });
        }
        break;
      }

      // ── SDP Offer (director or viewer → camera) ────────────────────
      case "sdp-offer": {
        const room = rooms.get(clientRoomId);
        const cam  = room?.cameras.get(msg.to);
        if (cam) {
          const peerId = clientRole === "director" ? "director" : clientViewerId;
          send(cam.ws, { type: "sdp-offer", sdp: msg.sdp, peerId });
        }
        break;
      }

      // ── SDP Answer (camera → director or viewer) ───────────────────
      case "sdp-answer": {
        const room   = rooms.get(clientRoomId);
        const peerId = msg.peerId || "director";
        if (peerId === "director") {
          if (room?.director) send(room.director, { type: "sdp-answer", from: clientCameraId, sdp: msg.sdp });
        } else {
          const viewerWs = room?.viewers.get(peerId);
          if (viewerWs) send(viewerWs, { type: "sdp-answer", from: clientCameraId, sdp: msg.sdp });
        }
        break;
      }

      // ── ICE Candidates ─────────────────────────────────────────────
      case "ice-candidate": {
        const room = rooms.get(clientRoomId);
        if (!room) break;

        if (clientRole === "director") {
          const cam = room.cameras.get(msg.to);
          if (cam) send(cam.ws, { type: "ice-candidate", candidate: msg.candidate, peerId: "director" });

        } else if (clientRole === "viewer") {
          const cam = room.cameras.get(msg.to);
          if (cam) send(cam.ws, { type: "ice-candidate", candidate: msg.candidate, peerId: clientViewerId });

        } else if (clientRole === "camera") {
          const peerId = msg.peerId || "director";
          if (peerId === "director") {
            if (room.director) send(room.director, { type: "ice-candidate", from: clientCameraId, candidate: msg.candidate });
          } else {
            const viewerWs = room.viewers.get(peerId);
            if (viewerWs) send(viewerWs, { type: "ice-candidate", from: clientCameraId, candidate: msg.candidate });
          }
        }
        break;
      }

      // ── Tally (director → cameras + viewers) ──────────────────────
      case "tally": {
        const room = rooms.get(clientRoomId);
        if (!room) break;
        // Track tally state in room
        if (msg.cameraId === "all") {
          room.cameras.forEach((_, cameraId) => { room.tallyState[cameraId] = msg.state; });
          room.cameras.forEach(({ ws: camWs }) => send(camWs, { type: "tally", state: msg.state }));
        } else {
          room.tallyState[msg.cameraId] = msg.state;
          const cam = room.cameras.get(msg.cameraId);
          if (cam) send(cam.ws, { type: "tally", state: msg.state });
        }
        // Forward tally to all viewers
        room.viewers.forEach(viewerWs => {
          send(viewerWs, { type: "tally", cameraId: msg.cameraId, state: msg.state });
        });
        break;
      }

      // ── Audio levels (A1 console ↔ director) ─────────────────────────
      case "audio-level": {
        const room = rooms.get(clientRoomId);
        if (!room) break;
        // Broadcast to director and all viewers so audio consoles stay in sync
        if (room.director?.readyState === 1) {
          send(room.director, { type: "audio-level", cameraId: msg.cameraId, volume: msg.volume, muted: msg.muted });
        }
        room.viewers.forEach(viewerWs => {
          send(viewerWs, { type: "audio-level", cameraId: msg.cameraId, volume: msg.volume, muted: msg.muted });
        });
        break;
      }

      // ── Rename (director → camera) ─────────────────────────────────
      case "rename": {
        const room = rooms.get(clientRoomId);
        if (!room) break;
        const cam = room.cameras.get(msg.cameraId);
        if (cam) {
          cam.name  = msg.name;
          cam.label = msg.label;
          send(cam.ws, { type: "rename", name: msg.name, label: msg.label });
        }
        // Also notify viewers of the rename
        room.viewers.forEach(viewerWs => {
          send(viewerWs, { type: "camera-renamed", cameraId: msg.cameraId, name: msg.name, label: msg.label });
        });
        break;
      }
    }
  });

  ws.on("close", () => {
    if (!clientRoomId) return;
    const room = rooms.get(clientRoomId);
    if (!room) return;

    if (clientRole === "camera" && clientCameraId) {
      room.cameras.delete(clientCameraId);
      delete room.tallyState[clientCameraId];
      console.log(`📷 Camera ${clientCameraId} left room ${clientRoomId}`);
      if (room.director?.readyState === 1) send(room.director, { type: "camera-disconnected", cameraId: clientCameraId });
      room.viewers.forEach(viewerWs => send(viewerWs, { type: "camera-disconnected", cameraId: clientCameraId }));
    }

    if (clientRole === "director") {
      room.director = null;
      console.log(`🎬 Director left room ${clientRoomId}`);
    }

    if (clientRole === "viewer" && clientViewerId) {
      room.viewers.delete(clientViewerId);
      console.log(`👁  Viewer ${clientViewerId} left room ${clientRoomId}`);
    }

    // Clean up empty rooms
    if (!room.director && room.cameras.size === 0 && room.viewers.size === 0) {
      rooms.delete(clientRoomId);
    }
  });

  ws.on("error", (err) => console.error("WebSocket error:", err.message));
});
