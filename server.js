const http = require("http");
const WebSocket = require("ws");

const server = http.createServer();

const wss = new WebSocket.Server({ server });

const rooms = {};

wss.on("connection", (ws) => {
  let peerId, room;

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    if (msg.type === "join") {
      peerId = msg.peerId;
      room = msg.room || "default";

      if (!rooms[room]) rooms[room] = {};
      rooms[room][peerId] = ws;

      const existing = Object.keys(rooms[room]).filter((id) => id !== peerId);
      ws.send(JSON.stringify({ type: "existing-peers", data: existing }));

      broadcast(room, { type: "peer-joined", from: peerId }, peerId);
    }

    if (msg.type === "signal" && msg.to) {
      const target = rooms[room]?.[msg.to];
      if (target) target.send(JSON.stringify(msg));
    }
  });

  ws.on("close", () => {
    if (!room || !peerId) return;
    delete rooms[room][peerId];
    broadcast(room, { type: "peer-left", from: peerId });
  });
});

function broadcast(room, msg, except) {
  Object.entries(rooms[room] || {}).forEach(([id, ws]) => {
    if (id === except) return;
    ws.send(JSON.stringify(msg));
  });
}

server.listen(8080);
