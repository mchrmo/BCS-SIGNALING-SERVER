// Posle hotove zhrnutie hovoru do n8n (system uloh: z hovoru vzniknu karty v Trelle).
// Zapina ho env N8N_WEBHOOK_URL; N8N_WEBHOOK_TOKEN ide v hlavicke X-Zapisovatel-Token
// (v n8n Webhook uzol -> Authentication: Header Auth s rovnakym menom a hodnotou).
// Nezavisi od Frappe: zhrnutie odide aj vtedy, ked zapis do Frappe zlyha.

import { logOk, logWarn } from "./log.js";

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

/** Ucastnici hovoru bez duplicit: [{ meno, email }]. */
export function ucastniciZoStop(tracks) {
  const podlaEmailu = new Map();
  for (const t of tracks) {
    if (t.email && !podlaEmailu.has(t.email)) podlaEmailu.set(t.email, { meno: t.name || t.email, email: t.email });
  }
  return [...podlaEmailu.values()];
}

export async function posliDoN8n(callId, zhrnutie, ucastnici, { pauzy = [2000, 5000], timeoutMs = 15000 } = {}) {
  const url = process.env.N8N_WEBHOOK_URL;
  if (!url) return false;

  const headers = { "Content-Type": "application/json" };
  if (process.env.N8N_WEBHOOK_TOKEN) headers["X-Zapisovatel-Token"] = process.env.N8N_WEBHOOK_TOKEN;
  const body = JSON.stringify({ call_id: callId, ucastnici, zhrnutie });

  for (let pokus = 1; pokus <= pauzy.length + 1; pokus++) {
    try {
      const res = await fetch(url, { method: "POST", headers, body, signal: AbortSignal.timeout(timeoutMs) });
      if (res.ok) {
        logOk("AI-N8N-ODOSLANE", { call: callId, ucastnici: ucastnici.length });
        return true;
      }
      logWarn("AI-N8N-ZLYHALO", { call: callId, status: res.status, pokus });
      // Zly token alebo vypnuty webhook: opakovanie nepomoze.
      if ([401, 403, 404].includes(res.status)) return false;
    } catch (e) {
      logWarn("AI-N8N-ZLYHALO", { call: callId, chyba: e.message, pokus });
    }
    if (pokus <= pauzy.length) await sleep(pauzy[pokus - 1]);
  }
  return false;
}
