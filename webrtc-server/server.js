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

// ===========================================================================
// LOGOVANIE
// ---------------------------------------------------------------------------
// Kazdy riadok ma tvar:  <cas> <ikona> UDALOST kluc=hodnota kluc=hodnota
// Preto sa da v Render logu filtrovat cez vyhladavanie, napr. "CALL-01315"
// vypise cely zivot jedneho hovoru, "❌" len chyby.
// ===========================================================================

const LOG_DEBUG = process.env.LOG_DEBUG === "1";

function stamp() {
  return new Date().toISOString().slice(11, 23);
}

function fields(obj) {
  return Object.entries(obj)
    .filter(([, v]) => v !== undefined && v !== null && v !== "")
    .map(([k, v]) => `${k}=${v}`)
    .join(" ");
}

function log(icon, event, data = {}) {
  console.log(`${stamp()} ${icon} ${event} ${fields(data)}`.trimEnd());
}

const logOk = (event, data) => log("✅", event, data);
const logInfo = (event, data) => log("•", event, data);
const logWarn = (event, data) => log("⚠️", event, data);
const logErr = (event, data) => log("❌", event, data);
const logDbg = (event, data) => { if (LOG_DEBUG) log("·", event, data); };

// ===========================================================================
// DIAGNOSTIKA HOVOROV
// ---------------------------------------------------------------------------
// Pre kazdy hovor si drzime, co sa v nom realne stalo. Ked hovor skonci,
// vypise sa jednoriadkovy verdikt — z neho je hned vidno, ci sa hovor spojil
// a ak nie, v ktorom kroku to zaseklo.
// ===========================================================================

const calls = new Map();               // callId -> statistika
const CALL_STATS_TTL_MS = 10 * 60 * 1000;
// Po prijati hovoru musi do tohto casu dojst SDP odpoved, inak je nieco zle.
const CALL_STUCK_MS = 20000;

function callStat(callId) {
  if (!callId) return null;
  let s = calls.get(callId);
  if (!s) {
    s = {
      callId,
      caller: null,
      ringing: new Set(),
      accepted: new Set(),
      offers: 0,
      answers: 0,
      candidates: 0,
      dropped: 0,
      reconnects: 0,
      startedAt: Date.now(),
      acceptedAt: null,
      firstOfferAt: null,
      firstAnswerAt: null,
      stuckLogged: false,
      done: false
    };
    calls.set(callId, s);
  }
  return s;
}

function since(from) {
  return from ? `${((Date.now() - from) / 1000).toFixed(1)}s` : undefined;
}

/**
 * Verdikt hovoru. Toto je riadok, ktory chces v Render logu hladat ako prvy.
 *  SPOJENY      - vymenila sa ponuka aj odpoved, hovor realne nabehol
 *  NEPRIJATY    - nikto hovor neprijal (nezdvihol / zrusene pocas zvonenia)
 *  BEZ_PONUKY   - hovor prijaty, ale ziadna SDP ponuka neprisla
 *  BEZ_ODPOVEDE - ponuka odosla, odpoved sa nikdy nevratila
 */
function finishCall(callId, reason) {
  const s = calls.get(callId);
  if (!s || s.done) return;
  s.done = true;

  let verdict;
  if (s.accepted.size === 0) verdict = "NEPRIJATY";
  else if (!s.firstOfferAt) verdict = "BEZ_PONUKY";
  else if (!s.firstAnswerAt) verdict = "BEZ_ODPOVEDE";
  else verdict = "SPOJENY";

  log(verdict === "SPOJENY" ? "📊" : "❌", "CALL-SUMMARY", {
    call: callId,
    verdikt: verdict,
    dovod: reason,
    volajuci: s.caller,
    zvonilo: [...s.ringing].join(",") || "-",
    prijali: [...s.accepted].join(",") || "-",
    ponuky: s.offers,
    odpovede: s.answers,
    kandidati: s.candidates,
    zahodene_signaly: s.dropped,
    reconnecty: s.reconnects,
    trvanie: since(s.startedAt)
  });

  setTimeout(() => calls.delete(callId), CALL_STATS_TTL_MS).unref?.();
}

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
  return name.normalize("NFD").replace(/[̀-ͯ]/g, "").replace(/[^a-zA-Z0-9._-]/g, "_");
}

function send(ws, type, payload = {}) {
  if (ws && ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify({ type, ...payload }), (err) => {
      if (err) logErr("SEND-FAIL", { typ: type, chyba: err.message });
    });
  }
}

function broadcastToRoom(roomId, except, type, payload = {}) {
  const peers = rooms.get(roomId);
  if (!peers) return 0;
  let n = 0;
  for (const client of peers) {
    if (client !== except && client.readyState === client.OPEN) {
      send(client, type, payload);
      n++;
    }
  }
  return n;
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
        logDbg("PUSH-OK", { pre: toUser, host: url.hostname, status: res.statusCode });
      } else if (userNotFound) {
        logDbg("PUSH-RETRY", { pre: toUser, host: url.hostname, dovod: "user not found" });
        sendPushNotification(toUser, fromUser, fromName, content, kind, urlIndex + 1, group);
      } else {
        logErr("PUSH-FAIL", { pre: toUser, host: url.hostname, status: res.statusCode, telo: responseBody.slice(0, 200) });
      }
    });
  });

  req.on('error', (e) => logErr("PUSH-ERROR", { pre: toUser, chyba: e.message }));
  req.write(data);
  req.end();
}

// ===========================================================================
// MIESTNOSTI
// ===========================================================================

const rooms = new Map();
const meta = new Map();
const users = new Map();
const pendingMessages = new Map();
const pendingCalls = new Map();
const pendingAccepts = new Map();

// Max pocet ucastnikov v jednej hovorovej miestnosti (konferencia: ja + 4).
const MAX_ROOM_PEERS = 5;

// Limity offline fronty (ochrana proti rastu pamäte ak príjemca nikdy nepošle chat-ack)
const MAX_QUEUED_PER_USER = 200;
const MESSAGE_TTL_MS = 7 * 24 * 60 * 60 * 1000;

// Kolko cakame na navrat ucastnika, ktoremu spadlo spojenie, kym hovor
// vyhlasime za ukonceny. Pad socketu NIE JE zavesenie — appka sa pri kazdom
// vypadku siete (alebo prebudeni Render instancie) pripaja znova.
const RECONNECT_GRACE_MS = Number(process.env.RECONNECT_GRACE_MS || 8000);
const pendingLeaves = new Map();       // "room|username" -> { timer }

function roomPeerCount(roomId) {
  return rooms.get(roomId)?.size || 0;
}

/** Idle miestnost sa vzdy vola ako pouzivatel; vsetko ostatne je hovor. */
function isCallRoom(roomId, username) {
  return Boolean(roomId) && roomId !== username;
}

function leaveKey(roomId, username) {
  return `${roomId}|${username}`;
}

function cancelPendingLeave(roomId, username) {
  const key = leaveKey(roomId, username);
  const entry = pendingLeaves.get(key);
  if (!entry) return false;
  clearTimeout(entry.timer);
  pendingLeaves.delete(key);
  return true;
}

/**
 * Skutocny odchod ucastnika z hovoru — uz vieme, ze sa nevrati.
 * Konferencia: ostatni len odstrania jeho spojenie (peer-left) a hovor bezi dalej.
 * Ked ostane uz len jeden clovek, hovor realne skoncil -> posleme aj call-ended.
 */
function finalizeLeave(roomId, username, reason) {
  broadcastToRoom(roomId, null, "peer-left", { peerId: username });

  const stat = calls.get(roomId);
  const pending = pendingCalls.get(roomId);

  if (pending && pending.fromUsername === username) {
    // Volajuci odisiel skor, nez hovor niekto prijal. Bez tohto by ostatnym
    // telefonom zvonilo dalej.
    pendingCalls.delete(roomId);
    pendingAccepts.delete(roomId);
    const n = broadcastToRoom(roomId, null, "call-ended", { from: username, callId: roomId });
    logWarn("CALL-CANCEL", { call: roomId, volajuci: username, dovod: reason, oznamene: n });
    finishCall(roomId, `volajuci odisiel: ${reason}`);
  } else if (roomPeerCount(roomId) === 1) {
    broadcastToRoom(roomId, null, "call-ended", { from: username, callId: roomId });
    logInfo("CALL-END", { call: roomId, kto: username, dovod: reason, poznamka: "ostal posledny ucastnik" });
    finishCall(roomId, `ostal posledny ucastnik (${reason})`);
  } else {
    logInfo("PEER-LEAVE", { call: roomId, kto: username, dovod: reason, zostava: roomPeerCount(roomId) });
    if (stat) stat.accepted.delete(username);
  }

  if (roomPeerCount(roomId) === 0) {
    rooms.delete(roomId);
    finishCall(roomId, `miestnost prazdna (${reason})`);
  }
}

/**
 * Ucastnikovi spadol socket. Toto NIE JE zavesenie — pockame, ci sa vrati.
 * Bez tohto okna kazdy vypadok siete volajuceho poslal volanemu "call-ended"
 * a hovor sa uz nikdy nespojil, hoci sa volajuci o sekundu pripojil spat.
 */
function scheduleLeave(ws, roomId, username, socketId) {
  const wasInRoom = rooms.get(roomId)?.delete(ws);

  if (!isCallRoom(roomId, username)) {
    if (roomPeerCount(roomId) === 0) rooms.delete(roomId);
    logDbg("IDLE-DISCONNECT", { user: username, sock: socketId });
    return;
  }
  if (!wasInRoom) {
    logDbg("DISCONNECT-NOT-IN-ROOM", { call: roomId, user: username, sock: socketId });
    return;
  }

  cancelPendingLeave(roomId, username);
  const timer = setTimeout(() => {
    pendingLeaves.delete(leaveKey(roomId, username));
    logErr("PEER-LOST", {
      call: roomId,
      user: username,
      po: `${(RECONNECT_GRACE_MS / 1000).toFixed(0)}s`,
      dovod: "spojenie sa neobnovilo"
    });
    finalizeLeave(roomId, username, "spojenie sa neobnovilo");
  }, RECONNECT_GRACE_MS);
  timer.unref?.();
  pendingLeaves.set(leaveKey(roomId, username), { timer });

  logWarn("PEER-DISCONNECT", {
    call: roomId,
    user: username,
    sock: socketId,
    cakam: `${(RECONNECT_GRACE_MS / 1000).toFixed(0)}s`,
    poznamka: "spadol socket, nie zavesenie"
  });
}

/** Odchod na vlastnu ziadost (leave / hangup / prechod do inej miestnosti). */
function explicitLeave(ws, roomId, username, reason) {
  if (!roomId) return;
  const wasInRoom = rooms.get(roomId)?.delete(ws);
  cancelPendingLeave(roomId, username);

  if (!isCallRoom(roomId, username)) {
    if (roomPeerCount(roomId) === 0) rooms.delete(roomId);
    return;
  }
  if (!wasInRoom) return;
  finalizeLeave(roomId, username, reason);
}

/**
 * Posle ucastnikovi vsetko, co ho v novej miestnosti caka: kto tam je,
 * ci sa zvoni a ci uz niekto stihol hovor prijat.
 */
function deliverRoomState(ws, info) {
  const roomId = info.roomId;

  const others = [...(rooms.get(roomId) || [])]
    .filter(p => p !== ws && meta.get(p)?.username && meta.get(p).username !== info.username)
    .map(p => meta.get(p).username);

  send(ws, "joined", { roomId, username: info.username, peers: others });
  broadcastToRoom(roomId, ws, "peer-joined", {
    peerId: info.username,
    username: info.username
  });

  const callInfo = pendingCalls.get(roomId);
  if (callInfo && callInfo.fromUsername !== info.username) {
    send(ws, "incoming-call", {
      from: callInfo.fromUsername,
      callerName: callInfo.callerName,
      roomId,
      callId: callInfo.callId
    });
    const rs = callStat(roomId);
    if (rs) rs.ringing.add(info.username);
    logInfo("RING-REPLAY", { call: roomId, pre: info.username, od: callInfo.fromUsername });
  }

  // Prijatie, ktore dorazilo skor, nez sa volajuci stihol pripojit.
  const accept = pendingAccepts.get(roomId);
  if (accept && accept.fromUsername !== info.username) {
    send(ws, "call-accepted", { from: accept.fromUsername, callId: accept.callId });
    pendingAccepts.delete(roomId);
    logOk("ACCEPT-REPLAY", {
      call: roomId,
      pre: info.username,
      od: accept.fromUsername,
      cakalo: since(accept.timestamp)
    });
  }

  return others;
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
        logErr("UPLOAD-URL-FAIL", { chyba: e.message });
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
        logErr("DOWNLOAD-URL-FAIL", { chyba: e.message });
        res.writeHead(500); res.end(e.toString());
      }
    });
    return;
  }

  // Health check + zivy pohlad na stav (kto je online, ake hovory bezia).
  if (req.method === "GET" && (req.url === "/health" || req.url === "/diag")) {
    const activeCalls = [...calls.values()]
      .filter(s => !s.done)
      .map(s => ({
        callId: s.callId,
        caller: s.caller,
        vRoom: roomPeerCount(s.callId),
        prijali: [...s.accepted],
        ponuky: s.offers,
        odpovede: s.answers,
        vek_s: Math.round((Date.now() - s.startedAt) / 1000)
      }));

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: "ok",
      time: new Date().toISOString(),
      online: users.size,
      rooms: rooms.size,
      cakajuce_odchody: pendingLeaves.size,
      cakajuce_zvonenia: pendingCalls.size,
      cakajuce_prijatia: pendingAccepts.size,
      aktivne_hovory: activeCalls
    }, null, 2));
    return;
  }

  res.end("WebRTC & Chat signaling server ✅");
});

const wss = new WebSocketServer({ server, path: "/ws" });

wss.on("connection", (ws) => {
  const id = uuid();
  const shortId = id.slice(0, 8);

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  meta.set(ws, { id, shortId, roomId: null, username: null });
  logDbg("SOCKET-OPEN", { sock: shortId });

  ws.on("message", (raw) => {
    ws.isAlive = true;

    let data;
    try { data = JSON.parse(raw.toString()); }
    catch { logWarn("BAD-JSON", { sock: shortId }); return; }

    const { type } = data;
    const info = meta.get(ws);
    if (!info) return;

    if (type === "ping") {
      send(ws, "pong", { timestamp: data.timestamp });
      return;
    }

    // ---------------------------------------------------------------- JOIN
    if (type === "join") {
      const newRoom = data.roomId;
      const username = data.username;
      if (!newRoom || !username) {
        logErr("JOIN-INVALID", { sock: shortId, roomId: newRoom, username });
        return;
      }

      const oldRoom = info.roomId;

      // Strop poctu ucastnikov (konferencia). Re-join toho isteho pouzivatela
      // sa nepocita — stary socket sa o chvilu zavrie.
      if (!rooms.has(newRoom)) rooms.set(newRoom, new Set());
      const existing = [...rooms.get(newRoom)]
        .filter(p => p !== ws && meta.get(p)?.username && meta.get(p).username !== username);
      if (existing.length >= MAX_ROOM_PEERS) {
        logWarn("ROOM-FULL", { call: newRoom, odmietnuty: username, max: MAX_ROOM_PEERS });
        send(ws, "room-full", { roomId: newRoom, max: MAX_ROOM_PEERS });
        return;
      }

      // Ten isty socket meni miestnost: odhlas ho zo starej. Bez tohto v nej
      // ostaval navzdy a signaly hovoru sa rozposielali do prazdna.
      if (oldRoom && oldRoom !== newRoom) {
        explicitLeave(ws, oldRoom, info.username || username, `prechod do ${newRoom}`);
      }

      info.roomId = newRoom;
      info.username = username;
      rooms.get(newRoom).add(ws);

      // Vratil sa po vypadku spojenia? Zrus napanovany odchod — hovor pokracuje.
      const recovered = cancelPendingLeave(newRoom, username);
      if (recovered) {
        const s = calls.get(newRoom);
        if (s) s.reconnects++;
        logOk("PEER-RECONNECT", {
          call: newRoom,
          user: username,
          sock: shortId,
          poznamka: "vratil sa v ochrannej lehote, hovor pokracuje"
        });
      }

      const old = users.get(username);
      if (old && old !== ws) {
        // Re-join toho isteho usera: stary socket odstran TICHO — user nikam
        // neodisiel, len ma nove spojenie.
        const oldInfo = meta.get(old);
        if (oldInfo) {
          oldInfo.superseded = true;
          if (oldInfo.roomId) rooms.get(oldInfo.roomId)?.delete(old);
          cancelPendingLeave(oldInfo.roomId, username);
        }
        try { old.terminate(); } catch {}
        logDbg("SOCKET-SUPERSEDED", { user: username, stary: oldInfo?.shortId, novy: shortId });
      }
      users.set(username, ws);

      const others = deliverRoomState(ws, info);

      log(isCallRoom(newRoom, username) ? "📞" : "•", "JOIN", {
        user: username,
        room: newRoom,
        sock: shortId,
        peers: others.length,
        z: oldRoom && oldRoom !== newRoom ? oldRoom : undefined
      });

      const queue = pendingMessages.get(username);
      if (queue) {
        for (const msg of queue.values()) send(ws, "chat-message", msg);
      }
      return;
    }

    const { roomId, username } = info;
    if (!roomId) {
      logWarn("MSG-BEFORE-JOIN", { sock: shortId, typ: type });
      return;
    }

    // ---------------------------------------------------------------- CALL
    if (type === "call") {
      info.callerName = data.callerName || username;
      const callId = data.callId || roomId;

      pendingCalls.set(roomId, {
        callId,
        callerName: info.callerName,
        fromUsername: username
      });

      const s = callStat(roomId);
      s.caller = username;

      const delivered = broadcastToRoom(roomId, ws, "incoming-call", {
        from: username,
        callerName: info.callerName,
        roomId,
        callId
      });
      for (const p of rooms.get(roomId) || []) {
        const u = meta.get(p)?.username;
        if (u && u !== username) s.ringing.add(u);
      }

      log(delivered > 0 ? "📞" : "⚠️", "CALL-START", {
        call: roomId,
        volajuci: username,
        zvoni_v_miestnosti: delivered,
        poznamka: delivered === 0 ? "volany este nie je pripojeny, caka sa na VoIP push" : undefined
      });
      return;
    }

    // -------------------------------------------------------------- ACCEPT
    if (type === "accept") {
      // Smerujeme podla callId zo spravy, NIE podla miestnosti socketu — ked sa
      // predstava klienta a servera o miestnosti rozide, prijatie by dorazilo tam,
      // kde volajuci nie je.
      const callRoom = data.callId || roomId;

      if (callRoom !== info.roomId) {
        if (info.roomId) explicitLeave(ws, info.roomId, username, `prijatie hovoru ${callRoom}`);
        if (!rooms.has(callRoom)) rooms.set(callRoom, new Set());
        info.roomId = callRoom;
        rooms.get(callRoom).add(ws);
        cancelPendingLeave(callRoom, username);
        deliverRoomState(ws, info);
        logInfo("ACCEPT-MOVE", { call: callRoom, user: username, poznamka: "presunuty do miestnosti hovoru" });
      }

      pendingCalls.delete(callRoom);

      const s = callStat(callRoom);
      s.accepted.add(username);
      if (!s.acceptedAt) s.acceptedAt = Date.now();

      const peers = rooms.get(callRoom);
      const otherPeers = peers ? [...peers].filter(p => p !== ws) : [];

      if (otherPeers.length > 0) {
        broadcastToRoom(callRoom, ws, "call-accepted", { from: username, callId: data.callId });
        logOk("ACCEPT-DELIVERED", {
          call: callRoom,
          prijal: username,
          dorucene: otherPeers.length,
          zvonilo: since(s.startedAt)
        });
      } else {
        const ts = Date.now();
        pendingAccepts.set(callRoom, { callId: data.callId, fromUsername: username, timestamp: ts });

        // Toto je ten pripad, ktory na produkcii lamal hovory: prijatie dorazilo,
        // ale volajuci v miestnosti nie je (spadol mu socket alebo sa este nestihol
        // pripojit). Odlozime ho a prehrame, ked pride.
        logErr("ACCEPT-ORPHANED", {
          call: callRoom,
          prijal: username,
          dovod: "volajuci nie je v miestnosti",
          caka_na_navrat: pendingLeaves.has(leaveKey(callRoom, s.caller || "")) ? "ano" : "nie",
          platnost: "30s"
        });

        const t = setTimeout(() => {
          const stored = pendingAccepts.get(callRoom);
          if (stored && stored.timestamp === ts) {
            pendingAccepts.delete(callRoom);
            logErr("ACCEPT-EXPIRED", { call: callRoom, prijal: username, poznamka: "volajuci sa nevratil do 30s" });
            finishCall(callRoom, "volajuci sa nevratil po prijati");
          }
        }, 30000);
        t.unref?.();
      }
      return;
    }

    // ------------------------------------------------------- REJECT/HANGUP
    if (type === "reject" || type === "hangup") {
      pendingCalls.delete(roomId);
      pendingAccepts.delete(roomId);
      const n = broadcastToRoom(roomId, ws, "call-ended", { from: username, callId: data.callId || roomId });
      logInfo("HANGUP", { call: roomId, kto: username, typ: type, oznamene: n });
      finishCall(roomId, `${type} od ${username}`);
      return;
    }

    // --------------------------------------------------------- SDP / ICE
    if (["offer", "answer", "candidate"].includes(type)) {
      const s = callStat(roomId);
      if (type === "offer") { s.offers++; if (!s.firstOfferAt) s.firstOfferAt = Date.now(); }
      if (type === "answer") { s.answers++; if (!s.firstAnswerAt) s.firstAnswerAt = Date.now(); }
      if (type === "candidate") s.candidates++;

      // Mesh: kazda dvojica ma vlastne spojenie, preto signal patri konkretnemu
      // ucastnikovi. Bez "to" (starsi klient, 1:1) sa posle celej miestnosti.
      if (data.to) {
        const target = [...(rooms.get(roomId) || [])]
          .find(p => meta.get(p)?.username === data.to);
        if (target) {
          send(target, type, { from: username, ...data });
          if (type !== "candidate") {
            logInfo("SDP", { call: roomId, typ: type, od: username, pre: data.to });
          }
        } else {
          s.dropped++;
          logErr("SIGNAL-DROP", {
            call: roomId,
            typ: type,
            od: username,
            pre: data.to,
            dovod: "prijemca nie je v miestnosti",
            v_miestnosti: [...(rooms.get(roomId) || [])].map(p => meta.get(p)?.username).join(",") || "-"
          });
        }
      } else {
        const n = broadcastToRoom(roomId, ws, type, { from: username, ...data });
        if (n === 0) {
          s.dropped++;
          logErr("SIGNAL-DROP", { call: roomId, typ: type, od: username, dovod: "miestnost je prazdna" });
        } else if (type !== "candidate") {
          logInfo("SDP", { call: roomId, typ: type, od: username, pre: "broadcast" });
        }
      }
      return;
    }

    // Konferencny stav ucastnika (meno, mute).
    if (type === "conf-state") {
      broadcastToRoom(roomId, ws, "conf-state", { from: username, ...data });
      return;
    }

    // Prizvany uz niekde telefonuje — odpoved ide priamo prizyvajucemu.
    if (type === "busy") {
      const target = users.get(data.to);
      if (target) {
        send(target, "peer-busy", { from: username, callId: data.callId });
        logInfo("BUSY", { call: data.callId, kto: username, oznamene: data.to });
      } else {
        logErr("BUSY-DROP", { call: data.callId, kto: username, pre: data.to, dovod: "prijemca nie je online" });
      }
      return;
    }

    // ---------------------------------------------------------------- CHAT
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

        const recipient = users.get(to);
        if (recipient && recipient.readyState === recipient.OPEN) {
          try {
            recipient.send(JSON.stringify({ type: "chat-message", ...msg }));
          } catch (e) {
            logErr("CHAT-SEND-FAIL", { pre: to, chyba: e.message });
          }
        }

        // Push posielame VZDY. iOS appka si banner potlaci sama, ak ma prave
        // ten chat otvoreny (willPresent).
        sendPushNotification(to, username, info.username, content, kind, 0, group);

        if (!pendingMessages.has(to)) pendingMessages.set(to, new Map());
        const queue = pendingMessages.get(to);
        queue.set(messageId, msg);
        while (queue.size > MAX_QUEUED_PER_USER) {
          queue.delete(queue.keys().next().value);
        }
      }
      return;
    }

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

    // --------------------------------------------------------------- LEAVE
    if (type === "leave") {
      explicitLeave(ws, roomId, username, "leave od pouzivatela");
      if (users.get(username) === ws) users.delete(username);
      info.roomId = null;
      return;
    }
  });

  ws.on("close", () => {
    const info = meta.get(ws);
    if (!info) return;

    if (info.superseded) {
      meta.delete(ws);
      return;
    }

    if (info.roomId && info.username) {
      scheduleLeave(ws, info.roomId, info.username, info.shortId);
    }

    if (info.username && users.get(info.username) === ws) {
      users.delete(info.username);
    }
    meta.delete(ws);
    logDbg("SOCKET-CLOSE", { sock: info.shortId, user: info.username, room: info.roomId });
  });

  ws.on("error", (err) => {
    const info = meta.get(ws);
    logErr("SOCKET-ERROR", { sock: info?.shortId, user: info?.username, chyba: err.message });
  });
});

const interval = setInterval(function ping() {
  wss.clients.forEach(function each(ws) {
    if (ws.isAlive === false) {
      const info = meta.get(ws);
      logWarn("HEARTBEAT-TIMEOUT", { sock: info?.shortId, user: info?.username, room: info?.roomId });
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

// Hovor bol prijaty, ale SDP odpoved nikdy neprisla -> nieco je zle TERAZ,
// nie az na konci. Vypiseme to hned, nech sa to da chytit v zivom logu.
const stuckInterval = setInterval(function stuckCheck() {
  const now = Date.now();
  for (const s of calls.values()) {
    if (s.done || s.stuckLogged || !s.acceptedAt) continue;
    if (!s.firstAnswerAt && now - s.acceptedAt > CALL_STUCK_MS) {
      s.stuckLogged = true;
      logErr("CALL-STUCK", {
        call: s.callId,
        volajuci: s.caller,
        prijali: [...s.accepted].join(",") || "-",
        ponuky: s.offers,
        odpovede: s.answers,
        zahodene_signaly: s.dropped,
        v_miestnosti: roomPeerCount(s.callId),
        dovod: s.offers === 0
          ? "po prijati neprisla ziadna SDP ponuka"
          : "ponuka odosla, odpoved sa nevratila"
      });
    }
  }
}, 5000);
stuckInterval.unref?.();

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
}, 60 * 60 * 1000);

wss.on('close', function close() {
  clearInterval(interval);
  clearInterval(cleanupInterval);
  clearInterval(stuckInterval);
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () =>
  console.log(`🚀 Signaling + Chat + Calls running on :${PORT} (grace=${RECONNECT_GRACE_MS}ms)`)
);

export { server, wss };
