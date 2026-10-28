// server.js — خادم signaling بسيط
const { WebSocketServer } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Signaling server running');
});

const wss = new WebSocketServer({ server });
const rooms = new Map(); // roomCode -> Map(peerId -> ws)

wss.on('connection', (ws) => {
  let myRoom = null;
  let myPeerId = null;

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data); } catch { return; }

    if (msg.type === 'join') {
      myRoom = msg.room;
      myPeerId = msg.peerId;

      if (!rooms.has(myRoom)) rooms.set(myRoom, new Map());
      const room = rooms.get(myRoom);

      // أرسل للوافد الجديد قائمة الموجودين
      const existing = Array.from(room.keys());
      ws.send(JSON.stringify({ type: 'peers', peers: existing }));

      // أبلغ الموجودين بوصول مشارك جديد
      for (const [peerId, peerWs] of room) {
        if (peerWs.readyState === 1) {
          peerWs.send(JSON.stringify({ type: 'peer-joined', peerId: myPeerId }));
        }
      }

      room.set(myPeerId, ws);
      return;
    }

    // مرر الرسائل (offer/answer/ice) للطرف المقصود
    if (msg.room && msg.to) {
      const room = rooms.get(msg.room);
      if (!room) return;
      const targetWs = room.get(msg.to);
      if (targetWs && targetWs.readyState === 1) {
        targetWs.send(JSON.stringify(msg));
      }
    }

    if (msg.type === 'leave') {
      handleLeave();
    }
  });

  function handleLeave() {
    if (!myRoom || !myPeerId) return;
    const room = rooms.get(myRoom);
    if (!room) return;
    room.delete(myPeerId);
    if (room.size === 0) rooms.delete(myRoom);
    else {
      for (const peerWs of room.values()) {
        if (peerWs.readyState === 1) {
          peerWs.send(JSON.stringify({ type: 'peer-left', peerId: myPeerId }));
        }
      }
    }
  }

  ws.on('close', handleLeave);
});

server.listen(PORT, () => console.log('Signaling on port', PORT));