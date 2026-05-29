const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// rooms: Map of roomId -> { director: ws|null, cameras: Map of cameraId -> { ws, name, label } }
const rooms = new Map();

const generateRoomId = () => Math.random().toString(36).slice(2, 8).toUpperCase();

const getOrCreateRoom = (roomId) => {
  if (!rooms.has(roomId)) {
    rooms.set(roomId, { director: null, cameras: new Map() });
  }
  return rooms.get(roomId);
};

const send = (ws, msg) => {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg));
};

console.log(`PGM signaling server running on port ${PORT}`);

wss.on("connection", (ws) => {
  let clientRole = null;
  let clientRoomId = null;
  let clientCameraId = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      case "register": {
        clientRole = msg.role;

        if (msg.role === "director") {
          // Use provided roomId or generate a new one
          clientRoomId = msg.roomId || generateRoomId();
          const room = getOrCreateRoom(clientRoomId);
          room.director = ws;

          console.log(`🎬 Director connected to room ${clientRoomId}`);

          // Send room ID back so director can store it
          send(ws, { type: "room-assigned", roomId: clientRoomId });

          // Send list of cameras already in this room
          room.cameras.forEach(({ name, label }, cameraId) => {
            send(ws, { type: "camera-connected", cameraId, name, label });
          });
        }

        if (msg.role === "camera") {
          clientRoomId = msg.roomId || "default";
          clientCameraId = msg.cameraId;
          const room = getOrCreateRoom(clientRoomId);
          room.cameras.set(msg.cameraId, { ws, name: msg.name, label: msg.label });

          console.log(`📷 Camera ${msg.name} connected to room ${clientRoomId}`);

          // Notify director
          if (room.director?.readyState === 1) {
            send(room.director, { type: "camera-connected", cameraId: msg.cameraId, name: msg.name, label: msg.label });
          }
        }
        break;
      }

      case "sdp-offer": {
        const room = rooms.get(clientRoomId);
        const cam = room?.cameras.get(msg.to);
        if (cam) send(cam.ws, { type: "sdp-offer", sdp: msg.sdp });
        break;
      }

      case "sdp-answer": {
        const room = rooms.get(clientRoomId);
        if (room?.director) send(room.director, { type: "sdp-answer", from: clientCameraId, sdp: msg.sdp });
        break;
      }

      case "ice-candidate": {
        const room = rooms.get(clientRoomId);
        if (!room) break;
        if (clientRole === "director") {
          const cam = room.cameras.get(msg.to);
          if (cam) send(cam.ws, { type: "ice-candidate", candidate: msg.candidate });
        } else {
          if (room.director) send(room.director, { type: "ice-candidate", from: clientCameraId, candidate: msg.candidate });
        }
        break;
      }

      case "tally": {
        const room = rooms.get(clientRoomId);
        if (!room) break;
        if (msg.cameraId === "all") {
          room.cameras.forEach(({ ws: camWs }) => send(camWs, { type: "tally", state: msg.state }));
        } else {
          const cam = room.cameras.get(msg.cameraId);
          if (cam) send(cam.ws, { type: "tally", state: msg.state });
        }
        break;
      }

      // Director renames a camera slot — forward to the camera page
      case "rename": {
        const room = rooms.get(clientRoomId);
        if (!room) break;
        const cam = room.cameras.get(msg.cameraId);
        if (cam) {
          // Update stored name
          cam.name = msg.name;
          cam.label = msg.label;
          // Tell camera page to update its display
          send(cam.ws, { type: "rename", name: msg.name, label: msg.label });
        }
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
      console.log(`📷 Camera ${clientCameraId} disconnected from room ${clientRoomId}`);
      if (room.director?.readyState === 1) {
        send(room.director, { type: "camera-disconnected", cameraId: clientCameraId });
      }
      // Clean up empty rooms (no director and no cameras)
      if (!room.director && room.cameras.size === 0) rooms.delete(clientRoomId);
    }

    if (clientRole === "director") {
      room.director = null;
      console.log(`🎬 Director disconnected from room ${clientRoomId}`);
      if (room.cameras.size === 0) rooms.delete(clientRoomId);
    }
  });

  ws.on("error", (err) => console.error("WebSocket error:", err.message));
});
