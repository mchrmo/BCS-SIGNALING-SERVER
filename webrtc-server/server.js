import { WebSocketServer } from "ws";
import { createServer } from "http";
import https from "https";
import { v4 as uuid } from "uuid";
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

const s3 = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  }
});

// Poradie backendov: najprv produkcia, potom dev. Ak prod vráti "User not found"
// (testovacie účty existujú len na dev site), notifikácia sa skúsi na ďalšom.
const FRAPPE_NOTIFY_URLS = [
  "https://bcservices.f.frappe.cloud/api/method/bcservices.api.notify.send_notification",
  "https://dev1.babylogroup.com/api/method/bcservices.api.notify.send_notification"
];

function safeFilename(name) {
  return name.normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[^a-zA-Z0-9._-]/g, "_");
}

function send(ws, type, payload = {}) {
  if (ws && ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify({ type, ...payload }), (err) => {
      if (err) console.error("⚠️ Send error:", err);
    });
  }
}

function broadcastToRoom(roomId, except, type, payload = {}) {
  const peers = rooms.get(roomId);
  if (!peers) return;
  for (const client of peers) {
    if (client !== except && client.readyState === client.OPEN) {
      send(client, type, payload);
    }
  }
}

/**
 * Komu spravu dorucit. Skupinovy chat posiela zoznam clenov v `members`,
 * 1:1 chat jedineho prijemcu v `to`. Seba sameho vzdy vynechame.
 */
function chatRecipients(data, sender) {
  if (Array.isArray(data.members) && data.members.length) {
    return [...new Set(data.members)].filter(m => m && m !== sender);
  }
  return data.to ? [data.to] : [];
}

function sendPushNotification(toUser, fromUser, fromName, content, kind, urlIndex = 0, group = null) {
  if (urlIndex >= FRAPPE_NOTIFY_URLS.length) return;
  const data = JSON.stringify({
    "to_user": toUser,
    "from_user": fromUser,
    "from_name": fromName || "Niekto",
    ...(group ? { "group_id": group.id, "group_name": group.name } : {}),
    "content": kind === 'file' ? "📎 Poslal vám súbor" : content
  });

  const url = new URL(FRAPPE_NOTIFY_URLS[urlIndex]);
  const options = {
    hostname: url.hostname,
    path: url.pathname,
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(data)
    }
  };

  const req = https.request(options, (res) => {
    let responseBody = '';
    res.on('data', (chunk) => { responseBody += chunk; });
    res.on('end', () => {
      const userNotFound = responseBody.includes("User not found");
      if (res.statusCode >= 200 && res.statusCode < 300 && !userNotFound) {
        console.log(`OK Frappe notify Success (${res.statusCode}) via ${url.hostname}`);
      } else if (userNotFound) {
        console.log(`.. ${url.hostname}: user ${toUser} not found, trying next backend`);
        sendPushNotification(toUser, fromUser, fromName, content, kind, urlIndex + 1, group);
      } else {
        console.error(`ERR Frappe notify Error (${res.statusCode}) via ${url.hostname}:`, responseBody);
      }
    });
  });

  req.on('error', (e) => console.error(`❌ Frappe Request Failed: ${e.message}`));
  req.write(data);
  req.end();
}

function roomPeerCount(roomId) {
  return rooms.get(roomId)?.size || 0;
}

/**
 * Odchod ucastnika z hovorovej miestnosti.
 * Konferencia: ostatni len odstrania jeho spojenie (peer-left) a hovor bezi dalej.
 * Ked ostane uz len jeden clovek, hovor realne skoncil -> posleme aj call-ended
 * (drzi spravanie 1:1 hovorov a starsich verzii appky).
 */
function handlePeerLeave(ws, roomId, username) {
  if (!roomId) return;
  broadcastToRoom(roomId, ws, "peer-left", { peerId: username });
  rooms.get(roomId)?.delete(ws);

  const pending = pendingCalls.get(roomId);
  if (pending && pending.fromUsername === username) {
    // Volajuci zrusil hovor skor, nez ho niekto prijal (pendingCalls sa maze
    // pri prvom accepte). Bez tohto by ostatnym telefonom zvonilo dalej,
    // pretoze v miestnosti ich ostalo viac ako jeden.
    pendingCalls.delete(roomId);
    pendingAccepts.delete(roomId);
    console.log("Caller " + username + " cancelled room " + roomId + " - ending for all");
    broadcastToRoom(roomId, ws, "call-ended", { from: username });
  } else if (roomPeerCount(roomId) === 1) {
    // Ostal posledny clovek -> hovor realne skoncil.
    broadcastToRoom(roomId, ws, "call-ended", { from: username });
  }

  if (roomPeerCount(roomId) === 0) rooms.delete(roomId);
}

const server = createServer((req, res) => {
  if (req.method === "POST" && req.url === "/upload-url") {
    let body = "";
    req.on("data", c => body += c);
    req.on("end", async () => {
      try {
        const { filename, contentType } = JSON.parse(body);
        const key = `chat/${Date.now()}-${safeFilename(filename)}`;
        const cmd = new PutObjectCommand({
          Bucket: process.env.AWS_S3_BUCKET,
          Key: key,
          ContentType: contentType
        });
        const uploadUrl = await getSignedUrl(s3, cmd, { expiresIn: 60 });
        res.end(JSON.stringify({ uploadUrl, key }));
      } catch (e) {
        res.writeHead(500); res.end(e.toString());
      }
    });
    return;
  }

  if (req.method === "POST" && req.url === "/download-url") {
    let body = "";
    req.on("data", c => body += c);
    req.on("end", async () => {
      try {
        const { key } = JSON.parse(body);
        const cmd = new GetObjectCommand({
          Bucket: process.env.AWS_S3_BUCKET,
          Key: key,
          ResponseContentDisposition: "attachment"
        });
        const downloadUrl = await getSignedUrl(s3, cmd, { expiresIn: 300 });
        res.end(JSON.stringify({ downloadUrl }));
      } catch (e) {
        res.writeHead(500); res.end(e.toString());
      }
    });
    return;
  }

  // Health check endpoint pre keep-alive ping z externého servisu
  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: "ok", time: new Date().toISOString() }));
    return;
  }

  res.end("WebRTC & Chat signaling server ✅");
});

const wss = new WebSocketServer({ server, path: "/ws" });

const rooms = new Map();
const meta = new Map();
const users = new Map();
const pendingMessages = new Map();
const pendingCalls = new Map();
// 🔥 NEW: queue pre accept signály ak caller ešte nie je v roomu keď callee accepts
const pendingAccepts = new Map();

// Max pocet ucastnikov v jednej hovorovej miestnosti (konferencia: ja + 4).
const MAX_ROOM_PEERS = 5;

// Limity offline fronty (ochrana proti rastu pamäte ak príjemca nikdy nepošle chat-ack)
const MAX_QUEUED_PER_USER = 200;          // max počet nedoručených správ na používateľa
const MESSAGE_TTL_MS = 7 * 24 * 60 * 60 * 1000;  // 7 dní

wss.on("connection", (ws) => {
  const id = uuid();

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  meta.set(ws, { id, roomId: null, username: null });
  console.log("🔌 Client connected:", id);

  ws.on("message", (raw) => {
    ws.isAlive = true;

    let data;
    try { data = JSON.parse(raw.toString()); }
    catch { return; }

    const { type } = data;
    const info = meta.get(ws);

    // Application-level ping/pong handler
    if (type === "ping") {
      send(ws, "pong", { timestamp: data.timestamp });
      return;
    }

    if (type === "join") {
      info.roomId = data.roomId;
      info.username = data.username;

      if (!rooms.has(info.roomId)) rooms.set(info.roomId, new Set());

      // Konferencia: strop poctu ucastnikov. Re-join toho isteho pouzivatela
      // sa nepocita (stary socket sa o chvilu zavrie).
      const existing = [...rooms.get(info.roomId)]
        .filter(p => meta.get(p)?.username && meta.get(p).username !== info.username);
      if (existing.length >= MAX_ROOM_PEERS) {
        console.log("Room " + info.roomId + " full - rejecting " + info.username);
        send(ws, "room-full", { roomId: info.roomId, max: MAX_ROOM_PEERS });
        return;
      }

      rooms.get(info.roomId).add(ws);

      const old = users.get(info.username);
      if (old && old !== ws) {
        // Re-join toho isteho usera: stary socket odstran z miestnosti TICHO
        // (bez peer-left/call-ended) - user nikam neodisiel, len ma novy socket.
        const oldInfo = meta.get(old);
        if (oldInfo) {
          oldInfo.superseded = true;
          if (oldInfo.roomId) rooms.get(oldInfo.roomId)?.delete(old);
        }
        try { old.terminate(); } catch {}
      }
      users.set(info.username, ws);

      console.log("Joined: " + info.username + " -> " + info.roomId + " (peers: " + existing.length + ")");

      // peers = kto uz v miestnosti je. Novoprichadzajuci im posle offer,
      // oni cakaju - tym sa vyhneme sucasnym offerom z oboch stran (glare).
      send(ws, "joined", {
        roomId: info.roomId,
        username: info.username,
        peers: existing.map(p => meta.get(p).username)
      });

      broadcastToRoom(info.roomId, ws, "peer-joined", {
        peerId: info.username,
        username: info.username
      });

      if (pendingCalls.has(info.roomId)) {
        const callInfo = pendingCalls.get(info.roomId);
        if (callInfo.fromUsername !== info.username) {
          send(ws, "incoming-call", {
            from: callInfo.fromUsername,
            callerName: callInfo.callerName,
            roomId: info.roomId,
            callId: callInfo.callId
          });
        }
      }

      // 🔥 NEW: Replay queued accept ak existuje (caller pripojil neskôr ako accept prišiel)
      if (pendingAccepts.has(info.roomId)) {
        const accept = pendingAccepts.get(info.roomId);
        if (accept.fromUsername !== info.username) {
          console.log(`📤 Replaying queued accept for room=${info.roomId} to ${info.username}`);
          send(ws, "call-accepted", {
            from: accept.fromUsername,
            callId: accept.callId
          });
          pendingAccepts.delete(info.roomId);
        }
      }

      const queue = pendingMessages.get(info.username);
      if (queue) {
        for (const msg of queue.values()) {
          send(ws, "chat-message", msg);
        }
      }
      return;
    }

    const { roomId, username } = info;
    if (!roomId) return;

    if (type === "call") {
      info.callerName = data.callerName || username;
      const peers = rooms.get(roomId);
      const otherPeers = [...peers].filter(p => p !== ws);

      pendingCalls.set(roomId, {
        callId: data.callId,
        callerName: info.callerName,
        fromUsername: username
      });

      if (otherPeers.length > 0) {
        broadcastToRoom(roomId, ws, "incoming-call", {
          from: username,
          callerName: info.callerName,
          roomId,
          callId: data.callId
        });
      }
      return;
    }

    // 🔥 NEW: Robust accept handler — queue ak caller v roomu nie je
    if (type === "accept") {
      pendingCalls.delete(roomId);

      const peers = rooms.get(roomId);
      const otherPeers = peers ? [...peers].filter(p => p !== ws) : [];

      if (otherPeers.length > 0) {
        broadcastToRoom(roomId, ws, "call-accepted", {
          from: username,
          callId: data.callId
        });
        console.log(`✅ Accept delivered for room=${roomId}`);
      } else {
        console.log(`📥 Queueing accept for room=${roomId} (caller not present)`);
        pendingAccepts.set(roomId, {
          callId: data.callId,
          fromUsername: username,
          timestamp: Date.now()
        });

        // Auto-cleanup po 30s
        const ts = Date.now();
        setTimeout(() => {
          const stored = pendingAccepts.get(roomId);
          if (stored && stored.timestamp === ts) {
            pendingAccepts.delete(roomId);
            console.log(`🗑️ Expired queued accept for room=${roomId}`);
          }
        }, 30000);
      }
      return;
    }

    if (type === "reject" || type === "hangup") {
      pendingCalls.delete(roomId);
      pendingAccepts.delete(roomId); // 🔥 NEW: cleanup pending accept on hangup
      broadcastToRoom(roomId, ws, "call-ended", {
        from: username,
        callId: data.callId
      });
      return;
    }

    if (["offer", "answer", "candidate"].includes(type)) {
      // Mesh: kazda dvojica ma vlastne spojenie, preto signal patri konkretnemu
      // ucastnikovi. Bez "to" (starsi klient, 1:1) sa posle celej miestnosti.
      if (data.to) {
        const target = [...(rooms.get(roomId) || [])]
          .find(p => meta.get(p)?.username === data.to);
        if (target) {
          send(target, type, { from: username, ...data });
        } else {
          console.log("Signal " + type + " for " + data.to + " - not in room " + roomId);
        }
      } else {
        broadcastToRoom(roomId, ws, type, { from: username, ...data });
      }
      return;
    }

    // Konferencny stav ucastnika (meno, mute) - kazdy ho broadcastne miestnosti
    // po pripojeni a pri kazdej zmene, aby ostatni vedeli koho poculi a ci ma mute.
    if (type === "conf-state") {
      broadcastToRoom(roomId, ws, "conf-state", { from: username, ...data });
      return;
    }

    // Prizvany uz niekde telefonuje - odpoved ide priamo prizyvajucemu,
    // ten NIE JE v miestnosti hovoru prijemcu, preto routujeme globalne.
    if (type === "busy") {
      const target = users.get(data.to);
      if (target) {
        send(target, "peer-busy", { from: username, callId: data.callId });
      }
      return;
    }

    if (type === "chat-message") {
      const { content, kind, filename, messageId, groupId, groupName } = data;
      const targets = chatRecipients(data, username);
      const group = groupId ? { id: groupId, name: groupName || "Skupina" } : null;

      for (const to of targets) {
        const msg = {
          messageId, from: username, to, content, kind, filename,
          timestamp: new Date().toISOString(),
          ...(group ? { groupId: group.id, groupName: group.name, members: data.members } : {})
        };

        // Doruc cez socket ak je prijemca pripojeny (zivy chat vo popredi).
        const recipient = users.get(to);
        if (recipient && recipient.readyState === recipient.OPEN) {
          try {
            recipient.send(JSON.stringify({ type: "chat-message", ...msg }));
          } catch (e) {}
        }

        // Push posielame VZDY. iOS appka si banner potlaci sama, ak ma prave
        // ten chat otvoreny (willPresent). Suspendovana appka drzi socket "OPEN"
        // este ~30s, takze by inak vyzerala online a push by neprisiel.
        sendPushNotification(to, username, info.username, content, kind, 0, group);

        if (!pendingMessages.has(to)) pendingMessages.set(to, new Map());
        const queue = pendingMessages.get(to);
        queue.set(messageId, msg);
        // Strop: ak fronta prerastie limit, zahod najstarsiu spravu
        while (queue.size > MAX_QUEUED_PER_USER) {
          queue.delete(queue.keys().next().value);
        }
      }
      return;
    }

    // Zmena skupiny (premenovanie, pridanie clena, odchod) — oznam ostatnym,
    // aby si appka stiahla aktualny stav a nemusela sa restartovat.
    if (type === "group-changed") {
      for (const to of chatRecipients(data, username)) {
        const recipient = users.get(to);
        if (recipient && recipient.readyState === recipient.OPEN) {
          try {
            recipient.send(JSON.stringify({
              type: "group-changed", from: username, groupId: data.groupId
            }));
          } catch (e) {}
        }
      }
      return;
    }

    // Uprava / zmazanie / reakcia — rovnake routovanie ako pri sprave:
    // skupine sa posle vsetkym clenom, 1:1 jedinemu prijemcovi.
    if (type === "chat-edit" || type === "chat-delete" || type === "chat-reaction") {
      const { messageId, content, emoji, groupId } = data;
      for (const to of chatRecipients(data, username)) {
        const recipient = users.get(to);
        if (recipient && recipient.readyState === recipient.OPEN) {
          const payload = { type, from: username, to, messageId };
          if (type === "chat-edit") payload.content = content;
          if (type === "chat-reaction") payload.emoji = emoji;
          if (groupId) payload.groupId = groupId;
          try { recipient.send(JSON.stringify(payload)); } catch (e) {}
        }
      }
      return;
    }

    if (type === "chat-ack") {
      const queue = pendingMessages.get(username);
      if (queue?.delete(data.messageId)) {
        if (queue.size === 0) pendingMessages.delete(username);
      }
      return;
    }

    if (type === "leave") {
      handlePeerLeave(ws, roomId, username);
      // Zmaž mapping len ak patrí TOMUTO socketu (inak by leave starého socketu
      // zmazal mapping nového po re-joine)
      if (users.get(username) === ws) users.delete(username);
      const pending = pendingCalls.get(roomId);
      if (pending && pending.fromUsername === username) {
        pendingCalls.delete(roomId);
      }
      // 🔥 NEW: cleanup queued accept
      const pendingA = pendingAccepts.get(roomId);
      if (pendingA && pendingA.fromUsername === username) {
        pendingAccepts.delete(roomId);
      }
    }
  });

  ws.on("close", () => {
    const info = meta.get(ws);
    if (info?.superseded) {
      meta.delete(ws);
      return;
    }
    if (info?.roomId) {
      handlePeerLeave(ws, info.roomId, info.username);
      const pending = pendingCalls.get(info.roomId);
      if (pending && pending.fromUsername === info.username) {
        pendingCalls.delete(info.roomId);
      }
      // 🔥 NEW: cleanup queued accept
      const pendingA = pendingAccepts.get(info.roomId);
      if (pendingA && pendingA.fromUsername === info.username) {
        pendingAccepts.delete(info.roomId);
      }
    }
    // Zmaž mapping len ak stále ukazuje na TENTO socket — pri re-joine je starý
    // socket terminated a jeho close by inak zmazal mapping NOVÉHO socketu,
    // čím by sa rozbilo doručovanie chatu až do ďalšieho joinu.
    if (info?.username && users.get(info.username) === ws) {
      users.delete(info.username);
    }
    meta.delete(ws);
    console.log("❌ Client disconnected:", id);
  });
});

const interval = setInterval(function ping() {
  wss.clients.forEach(function each(ws) {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

// Pravidelné čistenie offline fronty od správ starších ako TTL
const cleanupInterval = setInterval(function cleanupPending() {
  const now = Date.now();
  for (const [user, queue] of pendingMessages) {
    for (const [messageId, msg] of queue) {
      if (now - new Date(msg.timestamp).getTime() > MESSAGE_TTL_MS) {
        queue.delete(messageId);
      }
    }
    if (queue.size === 0) pendingMessages.delete(user);
  }
}, 60 * 60 * 1000); // raz za hodinu

wss.on('close', function close() {
  clearInterval(interval);
  clearInterval(cleanupInterval);
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () =>
  console.log(`🚀 Signaling + Chat + Calls running on :${PORT}`)
);
