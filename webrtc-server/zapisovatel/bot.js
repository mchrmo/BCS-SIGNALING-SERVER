// Jeden zapisovatel = jeden hovor. Pripoji sa na signaling ako skryty ucastnik,
// od kazdeho telefonu len PRIJIMA zvuk (nic neposiela) a kazdeho ucastnika
// zapisuje do samostatneho suboru. Ponuku (offer) posiela vzdy telefon.
//
// Render neprijima UDP, preto ide vsetko cez TURN (iceTransportPolicy relay).

import fs from "node:fs";
import path from "node:path";
import { WebSocket } from "ws";
import { RTCPeerConnection } from "werift";
import { MediaRecorder } from "werift/nonstandard";
import { logInfo, logOk, logWarn, logErr } from "./log.js";

export const BOT_PREFIX = "ai-zapisovatel";
export const BOT_NAME = "AI zapisovateľ";
const MAX_HOVOR_MS = 2 * 60 * 60 * 1000;
const GATHER_TIMEOUT_MS = 10000;

const isBot = (u) => typeof u === "string" && u.startsWith(BOT_PREFIX);
const safe = (s) => s.replace(/[^\w.@-]/g, "_");

function waitForGathering(pc) {
  if (pc.iceGatheringState === "complete") return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, GATHER_TIMEOUT_MS);
    pc.iceGatheringStateChange.subscribe((state) => {
      if (state === "complete") { clearTimeout(timer); resolve(); }
    });
  });
}

/**
 * Nahrava hovor, kym neskonci. Vrati { callId, dir, tracks[], cancelled, reason }.
 * tracks: { email, name, file, offsetS } - offsetS = kedy (od vstupu zapisovatela)
 * dorazil prvy zvuk danej stopy; podla neho sa stopy zoradia do dialogu.
 */
export function nahravajHovor(callId, { signalingUrl, secret, iceServers, baseDir, ctrl }) {
  return new Promise((resolve) => {
    const username = `${BOT_PREFIX}+${callId}`;
    const dir = path.join(baseDir, safe(callId));
    fs.mkdirSync(dir, { recursive: true });

    const startedAt = Date.now();
    const names = new Map();          // email -> meno z conf-state
    const current = new Map();        // email -> aktualne spojenie
    const all = [];                   // vsetky spojenia (telefon moze spojenie postavit znova)
    const pendingCandidates = new Map();
    let seq = 0;
    let done = false;
    let cancelled = null;

    const ws = new WebSocket(signalingUrl);
    const send = (o) => { if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(o)); };
    const announce = () => send({ type: "conf-state", name: BOT_NAME, muted: true });
    const maxTimer = setTimeout(() => finish("max dĺžka hovoru"), MAX_HOVOR_MS);

    if (ctrl) ctrl.cancel = (reason) => { cancelled = reason || "zrusene"; finish(cancelled); };

    async function closeConn(conn) {
      if (conn.closed) return;
      conn.closed = true;
      try { if (conn.recorder?.started) await conn.recorder.stop(); } catch (e) {
        logWarn("AI-NAHRAVKA-STOP", { call: callId, chyba: e.message });
      }
      try { await conn.pc.close(); } catch {}
    }

    async function finish(reason) {
      if (done) return;
      done = true;
      clearTimeout(maxTimer);
      send({ type: "leave" });
      try { ws.close(); } catch {}
      for (const conn of all) await closeConn(conn);

      const tracks = all
        // Aspon ~1 s zvuku (50 paketov po 20 ms) - kratsie spojenie (napr. tesne
        // pred zavesenim) necha po sebe len neuplny subor.
        .filter(c => c.packets >= 50 && fs.existsSync(c.file) && fs.statSync(c.file).size > 2048)
        .map(c => ({
          email: c.email,
          name: names.get(c.email) || c.email,
          file: c.file,
          offsetS: (c.firstRtpAt - startedAt) / 1000
        }));
      logInfo("AI-NAHRAVANIE-KONIEC", {
        call: callId, dovod: reason, stopy: tracks.length,
        trvanie: `${Math.round((Date.now() - startedAt) / 1000)}s`, zrusene: cancelled ? "ano" : undefined
      });
      resolve({ callId, dir, tracks, cancelled, reason });
    }

    async function onOffer(from, sdp) {
      if (done || isBot(from)) return;
      const previous = current.get(from);
      if (previous) await closeConn(previous);   // telefon spojenie postavil znova

      const conn = {
        email: from,
        file: path.join(dir, `${safe(from)}-${++seq}.webm`),
        packets: 0,
        firstRtpAt: null,
        closed: false,
        pc: new RTCPeerConnection({ iceServers, iceTransportPolicy: "relay" })
      };
      current.set(from, conn);
      all.push(conn);

      conn.pc.onTrack.subscribe(async (track) => {
        if (track.kind !== "audio" || conn.recorder || conn.closed) return;
        try {
          track.onReceiveRtp.subscribe(() => {
            conn.packets++;
            if (!conn.firstRtpAt) conn.firstRtpAt = Date.now();
          });
          conn.recorder = new MediaRecorder({ path: conn.file, numOfTracks: 1 });
          conn.recorder.onError.subscribe((e) => logErr("AI-NAHRAVKA-CHYBA", { call: callId, chyba: e.message }));
          await conn.recorder.addTrack(track);
        } catch (e) {
          logErr("AI-NAHRAVKA-CHYBA", { call: callId, od: from, chyba: e.message });
        }
      });
      conn.pc.connectionStateChange.subscribe((state) => {
        const data = { call: callId, od: from, stav: state };
        if (state === "connected") logOk("AI-SPOJENIE", data);
        else if (state === "failed") logErr("AI-SPOJENIE", data);
        else logInfo("AI-SPOJENIE", data);
      });

      try {
        await conn.pc.setRemoteDescription({ type: "offer", sdp });
        for (const t of conn.pc.getTransceivers()) t.setDirection("recvonly");
        for (const c of pendingCandidates.get(from) || []) await conn.pc.addIceCandidate(c).catch(() => {});
        pendingCandidates.delete(from);
        await conn.pc.setLocalDescription(await conn.pc.createAnswer());
        // Odpoved posielame az so vsetkymi kandidatmi (bez trickle) - jednoduchsie
        // a telefon ich vie precitat priamo z SDP.
        await waitForGathering(conn.pc);
        if (conn.closed || done) return;
        const answerSdp = conn.pc.localDescription.sdp;
        const relays = (answerSdp.match(/typ relay/g) || []).length;
        if (!relays) logErr("AI-TURN-ZLYHAL", { call: callId, pre: from, dovod: "ziadny relay kandidat" });
        send({ type: "answer", sdp: answerSdp, callId, to: from });
        logInfo("AI-ODPOVED", { call: callId, pre: from, kandidati: relays });
      } catch (e) {
        logErr("AI-OFFER-CHYBA", { call: callId, od: from, chyba: e.message });
      }
    }

    ws.on("open", () => send({ type: "join", roomId: callId, username, botToken: secret }));
    ws.on("close", () => finish("spojenie so signalingom skončilo"));
    ws.on("error", (e) => logErr("AI-WS-CHYBA", { call: callId, chyba: e.message }));
    ws.on("message", (raw) => {
      let m;
      try { m = JSON.parse(raw.toString()); } catch { return; }
      switch (m.type) {
        case "joined":
          logOk("AI-V-HOVORE", { call: callId, ludia: (m.peers || []).filter(p => !isBot(p)).length });
          announce();
          break;
        case "peer-joined":
          // Nový účastník (napr. prizvaný do konferencie) sa musí dozvedieť, že tu som.
          if (!isBot(m.peerId)) announce();
          break;
        case "conf-state":
          if (m.from && m.name && !isBot(m.from)) names.set(m.from, m.name);
          break;
        case "offer":
          if (m.from && m.sdp) onOffer(m.from, m.sdp);
          break;
        case "candidate": {
          if (!m.from || !m.candidate) break;
          const cand = { candidate: m.candidate, sdpMLineIndex: m.sdpMLineIndex, sdpMid: m.sdpMid };
          const conn = current.get(m.from);
          if (conn?.pc.remoteDescription) conn.pc.addIceCandidate(cand).catch(() => {});
          else {
            if (!pendingCandidates.has(m.from)) pendingCandidates.set(m.from, []);
            pendingCandidates.get(m.from).push(cand);
          }
          break;
        }
        case "peer-left": {
          const conn = current.get(m.peerId);
          if (conn) closeConn(conn);
          break;
        }
        case "call-ended":
        case "hangup":
          finish("koniec hovoru");
          break;
        case "room-full":
          finish("miestnosť plná");
          break;
      }
    });
  });
}
