const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// Track connected clients
// cameras: Map of cameraId -> { ws, name, label }
// director: the single director WebSocket (null if not connected)
const cameras = new Map();
let director = null;

console.log(`PGM signaling server running on port ${PORT}`);

wss.on("connection", (ws) => {
  let clientRole = null; // "camera" or "director"
  let clientCameraId = null;

  // ── Incoming messages ──────────────────────────────────────────────────────
  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return; // ignore malformed messages
    }

    switch (msg.type) {

      // Camera or director announces itself
      case "register": {
        clientRole = msg.role;

        if (msg.role === "camera") {
          clientCameraId = msg.cameraId;
          cameras.set(msg.cameraId, { ws, name: msg.name, label: msg.label });

          console.log(`📷 Camera registered: ${msg.name} (${msg.cameraId})`);

          // Tell the director a new camera is available
          if (director && director.readyState === 1) {
            send(director, {
              type: "camera-connected",
              cameraId: msg.cameraId,
              name: msg.name,
              label: msg.label,
            });
          }
        }

        if (msg.role === "director") {
          director = ws;
          console.log(`🎬 Director connected`);

          // Send director the list of cameras already online
          cameras.forEach(({ name, label }, cameraId) => {
            send(ws, { type: "camera-connected", cameraId, name, label });
          });
        }
        break;
      }

      // Director sends a WebRTC offer to a specific camera
      case "sdp-offer": {
        const cam = cameras.get(msg.to);
        if (cam) {
          send(cam.ws, { type: "sdp-offer", sdp: msg.sdp });
        }
        break;
      }

      // Camera sends its WebRTC answer back to the director
      case "sdp-answer": {
        if (director) {
          send(director, { type: "sdp-answer", from: clientCameraId, sdp: msg.sdp });
        }
        break;
      }

      // ICE candidates flow both ways — director <-> cameras
      case "ice-candidate": {
        if (clientRole === "director") {
          // Director sending ICE to a specific camera
          const cam = cameras.get(msg.to);
          if (cam) send(cam.ws, { type: "ice-candidate", candidate: msg.candidate });
        } else {
          // Camera sending ICE back to director
          if (director) send(director, { type: "ice-candidate", from: clientCameraId, candidate: msg.candidate });
        }
        break;
      }

      // Director updates tally state for a camera (or all cameras)
      case "tally": {
        if (msg.cameraId === "all") {
          cameras.forEach(({ ws: camWs }) => {
            send(camWs, { type: "tally", state: msg.state });
          });
        } else {
          const cam = cameras.get(msg.cameraId);
          if (cam) send(cam.ws, { type: "tally", state: msg.state });
        }
        break;
      }
    }
  });

  // ── Client disconnects ─────────────────────────────────────────────────────
  ws.on("close", () => {
    if (clientRole === "camera" && clientCameraId) {
      cameras.delete(clientCameraId);
      console.log(`📷 Camera disconnected: ${clientCameraId}`);

      // Notify director
      if (director && director.readyState === 1) {
        send(director, { type: "camera-disconnected", cameraId: clientCameraId });
      }
    }

    if (clientRole === "director") {
      director = null;
      console.log(`🎬 Director disconnected`);
    }
  });

  ws.on("error", (err) => {
    console.error("WebSocket error:", err.message);
  });
});

// ── Helper ─────────────────────────────────────────────────────────────────
function send(ws, msg) {
  if (ws && ws.readyState === 1) {
    ws.send(JSON.stringify(msg));
  }
}
