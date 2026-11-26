// Zapis stavu a zhrnutia do Frappe (Dennik hovorov).
//
// Dva sposoby podla env:
//  - FRAPPE_API_KEY + FRAPPE_API_SECRET: standardne REST API Frappe
//    (PUT /api/resource/Dennik hovorov/<callId>, polia ai_stav a ai_zhrnutie).
//    Staci, ked su polia na site ako Custom Fields a technicky pouzivatel
//    ma pravo zapisu len do Dennika hovorov - netreba ziadny kod v bcservices.
//  - inak endpoint bcservices.api.call.save_ai_summary s hlavickou X-AI-Secret
//    (vetva ai-zhrnutie-hovorov v BCS-FRAPPE).

import { logInfo, logOk, logWarn } from "./log.js";

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function poziadavka(callId, stav, zhrnutie) {
  const base = process.env.FRAPPE_URL.replace(/\/$/, "");
  const { FRAPPE_API_KEY: key, FRAPPE_API_SECRET: secret } = process.env;
  if (key && secret) {
    return {
      rest: true,
      url: `${base}/api/resource/${encodeURIComponent("Dennik hovorov")}/${encodeURIComponent(callId)}`,
      init: {
        method: "PUT",
        headers: { "Content-Type": "application/json", Authorization: `token ${key}:${secret}` },
        body: JSON.stringify({ ai_stav: stav, ai_zhrnutie: zhrnutie })
      }
    };
  }
  return {
    rest: false,
    url: `${base}/api/method/bcservices.api.call.save_ai_summary`,
    init: {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-AI-Secret": process.env.AI_SECRET },
      body: JSON.stringify({ callId, stav, zhrnutie })
    }
  };
}

export async function ulozStav(callId, stav, zhrnutie = "", { pauzy = [2000, 5000] } = {}) {
  if (process.env.AI_DRY_RUN === "1") {
    logInfo("AI-ULOZENIE-NASUCHO", { call: callId, stav });
    return true;
  }
  const z = poziadavka(callId, stav, zhrnutie);
  for (let pokus = 1; pokus <= pauzy.length + 1; pokus++) {
    try {
      const res = await fetch(z.url, { ...z.init, signal: AbortSignal.timeout(20000) });
      const body = await res.json().catch(() => ({}));
      if (z.rest ? res.ok : (res.ok && body.message?.success)) {
        logOk("AI-ULOZENE", { call: callId, stav, cez: z.rest ? "rest" : "endpoint" });
        return true;
      }
      const chyba = z.rest ? body.exc_type : body.message?.error;
      logWarn("AI-ULOZENIE-ZLYHALO", { call: callId, status: res.status, chyba, pokus });
      // Zly kluc, chybajuce pravo alebo neexistujuci hovor: opakovanie nepomoze.
      // (Pri REST sa opakuje napr. TimestampMismatchError, ked hovor sucasne uklada appka.)
      if (z.rest ? [401, 403, 404].includes(res.status) : (res.status === 403 || body.message?.error)) return false;
    } catch (e) {
      logWarn("AI-ULOZENIE-ZLYHALO", { call: callId, chyba: e.message, pokus });
    }
    if (pokus <= pauzy.length) await sleep(pauzy[pokus - 1]);
  }
  return false;
}
