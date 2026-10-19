// AI zapisovatel - samostatny proces, ktory spusta server.js (fork).
// Prijima od servera { type: "start" | "cancel", callId }, nahrava hovory
// a po skonceni ich spracuva jeden po druhom. Nahravky sa po spracovani
// VZDY zmazu; upratovac zmaze aj zvysky po pripadnom pade.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { nahravajHovor } from "./bot.js";
import { spracujHovor, ulozStav } from "./spracovanie.js";
import { logInfo, logWarn, logErr } from "./log.js";

const BASE = path.join(os.tmpdir(), "zapisovatel");
const MAX_SUBEZNE = Number(process.env.AI_MAX_HOVOROV || 3);
const MAX_VEK_MS = 60 * 60 * 1000;

const iceServers = [{
  // TURN cez TLS na 443: Render neprijima UDP a obycajne TCP na 443 Metered
  // pre werift nepridelil relay (overene); TLS aj TCP na porte 80 funguju.
  urls: process.env.TURN_URL || "turns:standard.relay.metered.ca:443?transport=tcp",
  username: process.env.TURN_USERNAME,
  credential: process.env.TURN_CREDENTIAL
}];

const active = new Map();   // callId -> ctrl (cancel)
const queue = [];
let busy = false;

function status() {
  process.send?.({ type: "status", nahrava: active.size, vo_fronte: queue.length, spracuva: busy });
}

function remove(dir) {
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch (e) {
    logErr("AI-MAZANIE-ZLYHALO", { chyba: e.message });
  }
}

/** Zmaze vsetko, co nepatri prave nahravanemu ani cakajucemu hovoru a je stare. */
function sweep(maxAgeMs) {
  if (!fs.existsSync(BASE)) return;
  const keep = new Set([...queue.map(v => v.dir)]);
  for (const name of fs.readdirSync(BASE)) {
    const dir = path.join(BASE, name);
    if (keep.has(dir) || [...active.keys()].some(id => dir.endsWith(id))) continue;
    const age = Date.now() - fs.statSync(dir).mtimeMs;
    if (age >= maxAgeMs) {
      remove(dir);
      logWarn("AI-UPRATANE", { priecinok: name, vek: `${Math.round(age / 60000)}min` });
    }
  }
}

async function processQueue() {
  if (busy) return;
  busy = true;
  status();
  while (queue.length) {
    const v = queue.shift();
    status();
    try {
      await spracujHovor(v);
    } catch (e) {
      logErr("AI-SPRACOVANIE-ZLYHALO", { call: v.callId, chyba: e.message });
    } finally {
      remove(v.dir);
      logInfo("AI-ZMAZANE", { call: v.callId });
    }
  }
  busy = false;
  status();
}

async function start(callId) {
  if (active.has(callId)) return;
  if (active.size >= MAX_SUBEZNE) {
    logWarn("AI-PLNE", { call: callId, nahrava: active.size, max: MAX_SUBEZNE });
    return;
  }
  const ctrl = {};
  active.set(callId, ctrl);
  status();
  try {
    const v = await nahravajHovor(callId, {
      signalingUrl: process.env.SIGNALING_URL,
      secret: process.env.AI_SECRET,
      iceServers,
      baseDir: BASE,
      ctrl
    });
    queue.push(v);
    if (!v.cancelled && v.tracks.length) await ulozStav(callId, "Čaká");
  } catch (e) {
    logErr("AI-NAHRAVANIE-ZLYHALO", { call: callId, chyba: e.message });
  } finally {
    active.delete(callId);
    status();
  }
  processQueue();
}

process.on("message", (m) => {
  if (m?.type === "start" && m.callId) start(m.callId);
  if (m?.type === "cancel" && m.callId) active.get(m.callId)?.cancel?.(m.dovod);
});
// Server skoncil -> skonci aj zapisovatel (inak by ostal visiet ako sirota).
process.on("disconnect", () => process.exit(0));

// Po starte nic nenahravame, takze vsetko v BASE su zvysky po pade.
sweep(0);
setInterval(() => sweep(MAX_VEK_MS), 30 * 60 * 1000).unref();
fs.mkdirSync(BASE, { recursive: true });
logInfo("AI-PROCES-START", { max_hovorov: MAX_SUBEZNE, nasucho: process.env.AI_DRY_RUN === "1" ? "ano" : undefined });
status();
