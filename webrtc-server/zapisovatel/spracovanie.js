// Spracovanie nahraneho hovoru:
//   1. v kazdej stope najde useky reci (ticho by Whisper "dopisal" vymyslenymi vetami)
//   2. useky reci prepise cez Groq (whisper-large-v3) a vrati im povodne casy
//   3. stopy zlozi do dialogu "Meno: text" podla casu
//   4. Claude z dialogu spravi zhrnutie
//   5. zhrnutie ulozi do Frappe (Dennik hovorov)
// Subory hovoru maze volajuci (index.js) vzdy, aj ked tu nieco zlyha.

import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import ffmpegPath from "ffmpeg-static";
import Anthropic from "@anthropic-ai/sdk";
import { logInfo, logOk, logWarn, logErr } from "./log.js";

const DRY_RUN = process.env.AI_DRY_RUN === "1";
const SAMPLE_RATE = 16000;
const FRAME_S = 0.02;
const MAX_CHUNK_S = 10 * 60;           // Groq: subor do 25 MB, 10 min FLAC je ~10 MB
const MIN_SLOV = 8;                    // menej slov = nebolo co zhrnut

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function run(args, { onStdout } = {}) {
  return new Promise((resolve, reject) => {
    const p = spawn(ffmpegPath, ["-hide_banner", "-loglevel", "error", ...args]);
    let err = "";
    p.stderr.on("data", d => { err += d.toString(); });
    if (onStdout) p.stdout.on("data", onStdout);
    p.on("error", reject);
    p.on("close", code => code === 0 ? resolve()
      : reject(new Error(`ffmpeg ${code}: ${err.slice(-300).replace(/\s+/g, " ").trim()}`)));
  });
}

// ---------------------------------------------------------------- 1. useky reci

/** Energia kazdeho 20 ms ramca v dBFS (dekodovane na 16 kHz mono). */
async function frameLevels(file) {
  const levels = [];
  const frameBytes = SAMPLE_RATE * FRAME_S * 2;
  let rest = Buffer.alloc(0);
  await run(["-i", file, "-ac", "1", "-ar", String(SAMPLE_RATE), "-f", "s16le", "pipe:1"], {
    onStdout: (chunk) => {
      let buf = rest.length ? Buffer.concat([rest, chunk]) : chunk;
      let off = 0;
      for (; off + frameBytes <= buf.length; off += frameBytes) {
        let sum = 0;
        for (let i = off; i < off + frameBytes; i += 2) {
          const v = buf.readInt16LE(i);
          sum += v * v;
        }
        const rms = Math.sqrt(sum / (frameBytes / 2));
        levels.push(rms > 0 ? 20 * Math.log10(rms / 32768) : -100);
      }
      rest = buf.subarray(off);
    }
  });
  return levels;
}

/**
 * Useky reci [{a, b}] v sekundach. Prah sa odvodi od sumu pozadia danej stopy
 * (20. percentil), takze funguje pri tichom aj hlucnejsom mikrofone.
 */
export function speechSegments(levels) {
  if (!levels.length) return [];
  const sorted = [...levels].sort((x, y) => x - y);
  const floor = sorted[Math.floor(sorted.length * 0.2)];
  const threshold = Math.max(-50, floor + 12);

  const raw = [];
  let start = null, quiet = 0;
  const hangover = Math.round(0.3 / FRAME_S);
  levels.forEach((db, i) => {
    if (db > threshold) {
      if (start === null) start = i;
      quiet = 0;
    } else if (start !== null && ++quiet > hangover) {
      raw.push({ a: start * FRAME_S, b: (i - quiet + 1) * FRAME_S });
      start = null; quiet = 0;
    }
  });
  if (start !== null) raw.push({ a: start * FRAME_S, b: levels.length * FRAME_S });

  const total = levels.length * FRAME_S;
  const merged = [];
  for (const s of raw) {
    const seg = { a: Math.max(0, s.a - 0.2), b: Math.min(total, s.b + 0.2) };
    const last = merged[merged.length - 1];
    if (last && seg.a - last.b < 0.6) last.b = Math.max(last.b, seg.b);
    else merged.push(seg);
  }
  return merged.filter(s => s.b - s.a >= 0.25);
}

/** Rozdel useky reci na casti do MAX_CHUNK_S a ku kazdej si zapamataj mapu casov. */
function chunkSegments(segments) {
  const chunks = [];
  let cur = null;
  for (const s of segments) {
    const len = s.b - s.a;
    if (!cur || cur.length + len > MAX_CHUNK_S) {
      cur = { parts: [], length: 0 };
      chunks.push(cur);
    }
    cur.parts.push({ a: s.a, b: s.b, cStart: cur.length });
    cur.length += len;
  }
  return chunks;
}

function toOriginalTime(chunk, t) {
  let part = chunk.parts[0];
  for (const p of chunk.parts) {
    if (p.cStart <= t) part = p; else break;
  }
  return part.a + Math.min(t - part.cStart, part.b - part.a);
}

async function cutSpeech(file, chunk, out) {
  const expr = chunk.parts.map(p => `between(t,${p.a.toFixed(2)},${p.b.toFixed(2)})`).join("+");
  await run(["-y", "-i", file, "-af", `aselect='${expr}',asetpts=N/SR/TB`,
    "-ac", "1", "-ar", String(SAMPLE_RATE), "-c:a", "flac", out]);
}

// ---------------------------------------------------------------- 2. prepis

// Vety, ktore Whisper casto "pocuje" v tichu alebo sume, hoci ich nikto nepovedal.
const HALUCINACIE = new Set([
  "ďakujem za pozornosť", "ďakujem za pozretie", "ďakujem za sledovanie",
  "titulky vytvoril", "titulky", "pokračovanie nabudúce", "odoberajte náš kanál"
]);
const normalize = (t) => t.toLowerCase().replace(/[.,!?…"„“]/g, "").replace(/\s+/g, " ").trim();
const isHallucination = (t) => {
  const n = normalize(t);
  return !n || HALUCINACIE.has(n) || n.startsWith("titulky ");
};

async function transcribe(file) {
  for (let attempt = 1; attempt <= 4; attempt++) {
    const form = new FormData();
    form.append("file", new Blob([fs.readFileSync(file)]), path.basename(file));
    form.append("model", "whisper-large-v3");
    form.append("language", "sk");
    form.append("response_format", "verbose_json");
    form.append("temperature", "0");
    const res = await fetch("https://api.groq.com/openai/v1/audio/transcriptions", {
      method: "POST",
      headers: { Authorization: `Bearer ${process.env.GROQ_API_KEY}` },
      body: form
    });
    if (res.ok) return (await res.json()).segments || [];
    const wait = Number(res.headers.get("retry-after")) || 20;
    if ((res.status === 429 || res.status >= 500) && attempt < 4) {
      logWarn("AI-GROQ-CAKAM", { status: res.status, o: `${wait}s`, pokus: attempt });
      await sleep(wait * 1000);
      continue;
    }
    throw new Error(`Groq HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`);
  }
}

/** Prepis jednej stopy -> riadky { t (sekundy od zaciatku nahravania), name, text }. */
async function transcribeTrack(track, workDir, idx) {
  const segments = speechSegments(await frameLevels(track.file));
  const speechS = segments.reduce((s, x) => s + (x.b - x.a), 0);
  if (speechS < 1) return { lines: [], speechS };

  const lines = [];
  const chunks = chunkSegments(segments);
  for (let i = 0; i < chunks.length; i++) {
    const out = path.join(workDir, `rec-${idx}-${i}.flac`);
    await cutSpeech(track.file, chunks[i], out);
    if (DRY_RUN) continue;
    for (const seg of await transcribe(out)) {
      if (isHallucination(seg.text)) continue;
      lines.push({
        t: track.offsetS + toOriginalTime(chunks[i], seg.start),
        name: track.name,
        text: seg.text.trim()
      });
    }
  }
  return { lines, speechS };
}

// ---------------------------------------------------------------- 4. zhrnutie

const SYSTEM = `Si zapisovateľ telefonických hovorov medzi finančným poradcom a jeho klientmi.
Dostaneš automatický prepis hovoru v slovenčine. Každý riadok má čas a meno toho, kto hovorí. Prepis robil stroj, takže môže obsahovať chybne rozpoznané slová, mená alebo čísla a výnimočne aj krátku vetu, ktorá v hovore nezaznela.

Napíš krátke zhrnutie hovoru v slovenčine, ktoré sa uloží do denníka hovorov. Použi presne tento tvar a nič pred ani za ním:

Téma: <jedna veta>
Zhrnutie: <2 až 4 vety o tom, čo sa riešilo>
Dohodnuté kroky:
- <kto – čo – dokedy; ak nič nedohodli, napíš „žiadne“>
Dôležité údaje: <sumy, dátumy, produkty, termíny, ktoré zazneli; inak „žiadne“>

Píš len to, čo z prepisu naozaj vyplýva, nič si nedomýšľaj. Keď je niečo v prepise nejasné alebo zjavne zle rozpoznané, napíš to s poznámkou „(nejasné)“.`;

const fmt = (s) => `${String(Math.floor(s / 60)).padStart(2, "0")}:${String(Math.floor(s % 60)).padStart(2, "0")}`;

async function summarize(callId, transcript, participants) {
  const client = new Anthropic();
  try {
    const response = await client.beta.messages.create({
      model: "claude-opus-5-5",
      max_tokens: 16000,
      betas: ["server-side-fallback-2026-07-01"],
      fallbacks: "default",
      output_config: { effort: "medium" },
      system: SYSTEM,
      messages: [{
        role: "user",
        content: `Účastníci: ${participants.join(", ")}\n\nPrepis hovoru:\n\n${transcript}`
      }]
    });
    if (response.stop_reason === "refusal") {
      throw new Error(`Claude odmietol (${response.stop_details?.category ?? "?"})`);
    }
    const text = response.content.filter(b => b.type === "text").map(b => b.text).join("\n").trim();
    const u = response.usage;
    logInfo("AI-CLAUDE", {
      call: callId, model: response.model, vstup: u.input_tokens, vystup: u.output_tokens,
      cena: `$${((u.input_tokens * 4 + u.output_tokens * 20) / 1e6).toFixed(4)}`
    });
    return text;
  } catch (e) {
    if (e instanceof Anthropic.AuthenticationError) throw new Error("Claude: neplatný API kľúč");
    if (e instanceof Anthropic.RateLimitError) throw new Error("Claude: prekročený limit");
    if (e instanceof Anthropic.APIError) throw new Error(`Claude: chyba ${e.status}`);
    throw e;
  }
}

// ---------------------------------------------------------------- 5. Frappe

export async function ulozStav(callId, stav, zhrnutie = "") {
  if (DRY_RUN) {
    logInfo("AI-ULOZENIE-NASUCHO", { call: callId, stav });
    return true;
  }
  const url = `${process.env.FRAPPE_URL.replace(/\/$/, "")}/api/method/bcservices.api.call.save_ai_summary`;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-AI-Secret": process.env.AI_SECRET },
        body: JSON.stringify({ callId, stav, zhrnutie })
      });
      const body = await res.json().catch(() => ({}));
      if (res.ok && body.message?.success) {
        logOk("AI-ULOZENE", { call: callId, stav });
        return true;
      }
      logWarn("AI-ULOZENIE-ZLYHALO", { call: callId, status: res.status, chyba: body.message?.error, pokus: attempt });
      if (res.status === 403 || body.message?.error) return false;   // opakovanie nepomoze
    } catch (e) {
      logWarn("AI-ULOZENIE-ZLYHALO", { call: callId, chyba: e.message, pokus: attempt });
    }
    await sleep([2000, 5000, 15000][attempt - 1]);
  }
  return false;
}

// ---------------------------------------------------------------- spolu

export async function spracujHovor(v) {
  const { callId } = v;
  if (v.cancelled) {
    await ulozStav(callId, "Bez záznamu");
    return;
  }
  if (!v.tracks.length) {
    await ulozStav(callId, "Bez záznamu");
    return;
  }
  const started = Date.now();
  try {
    const lines = [];
    let speechS = 0;
    // Jedna poskodena stopa (napr. spojenie nabehlo tesne pred zavesenim
    // a subor ostal neuplny) nesmie zhodit cely hovor - preskoci sa.
    let failed = 0;
    let lastError = null;
    for (const [i, track] of v.tracks.entries()) {
      try {
        const r = await transcribeTrack(track, v.dir, i);
        lines.push(...r.lines);
        speechS += r.speechS;
      } catch (e) {
        failed++;
        lastError = e;
        logWarn("AI-STOPA-PRESKOCENA", { call: callId, stopa: i + 1, chyba: e.message.slice(0, 200) });
      }
    }
    if (failed && failed === v.tracks.length) throw lastError;
    lines.sort((x, y) => x.t - y.t);
    const words = lines.reduce((n, l) => n + l.text.split(/\s+/).length, 0);
    logInfo("AI-PREPIS", { call: callId, stopy: v.tracks.length, rec: `${Math.round(speechS)}s`, riadky: lines.length, slova: words });

    if (DRY_RUN) {
      await ulozStav(callId, "Bez záznamu");
      return;
    }
    if (words < MIN_SLOV) {
      await ulozStav(callId, "Bez záznamu");
      return;
    }

    const transcript = lines.map(l => `[${fmt(l.t)}] ${l.name}: ${l.text}`).join("\n");
    const participants = [...new Set(v.tracks.map(t => t.name))];
    const zhrnutie = await summarize(callId, transcript, participants);
    await ulozStav(callId, "Hotové", zhrnutie);
    logOk("AI-SPRACOVANE", { call: callId, trvalo: `${Math.round((Date.now() - started) / 1000)}s` });
  } catch (e) {
    logErr("AI-SPRACOVANIE-ZLYHALO", { call: callId, chyba: e.message });
    // Technicka pricina ide aj do Frappe, nech sa da zistit bez pristupu k logu.
    // Chybove hlasky (ffmpeg, Groq, Claude) neobsahuju nic z hovoru.
    await ulozStav(callId, "Chyba", `Technická chyba (pre vývojára): ${e.message}`.slice(0, 500));
  }
}
