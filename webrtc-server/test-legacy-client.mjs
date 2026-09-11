// Kompatibilita s appkou, ktora je uz vonku (App Store).
// Stary klient sa sprava inak ako novy:
//   - pri KAZDEJ zmene miestnosti otvara NOVE spojenie (nikdy ho nepouziva znova)
//   - offer/answer posiela BEZ pola "to" (1:1, ziadny mesh)
//   - na koniec hovoru posiela "hangup", nie "leave"
//   - prichadzajuci "call-ended" IGNORUJE, ak v nom nie je callId
import { spawn } from "child_process";
import { fileURLToPath } from "url";
import { dirname } from "path";
import { WebSocket } from "ws";

const PORT = 18097;
const GRACE = 1500;
const WS_URL = `ws://127.0.0.1:${PORT}/ws`;

let logLines = [];
const srv = spawn(process.execPath, ["server.js"], {
  cwd: dirname(fileURLToPath(import.meta.url)),
  env: { ...process.env, PORT: String(PORT), RECONNECT_GRACE_MS: String(GRACE) }
});
srv.stdout.on("data", d => logLines.push(...d.toString().split("\n").filter(Boolean)));
srv.stderr.on("data", d => console.error("SRV-ERR", d.toString()));
const sleep = ms => new Promise(r => setTimeout(r, ms));

/** Simulacia starej appky: jeden pouzivatel, nove spojenie pri kazdej miestnosti. */
function legacyApp(name) {
  let ws = null;
  const got = [];
  /** call-ended bez callId stara appka zahodi — presne ako verzia 2.9. */
  const endedCallIds = [];

  async function openRoom(room) {
    const old = ws;
    ws = new WebSocket(WS_URL);
    await new Promise(res => ws.on("open", res));
    ws.on("message", m => {
      const msg = JSON.parse(m.toString());
      got.push(msg);
      if ((msg.type === "call-ended" || msg.type === "hangup") && typeof msg.callId === "string") {
        endedCallIds.push(msg.callId);
      }
    });
    ws.send(JSON.stringify({ type: "join", roomId: room, username: name }));
    if (old) old.close();                    // stary socket sa zavrie AZ TERAZ
    await sleep(150);
  }

  return {
    name, got, endedCallIds,
    openRoom,
    /** Poradie ako pri vypadku siete: stare spojenie zomrie prv, nez vznikne nove. */
    async reopenRoomAfterDrop(room) {
      if (ws) ws.terminate();
      await sleep(200);
      ws = new WebSocket(WS_URL);
      await new Promise(res => ws.on("open", res));
      ws.on("message", m => {
        const msg = JSON.parse(m.toString());
        got.push(msg);
        if ((msg.type === "call-ended" || msg.type === "hangup") && typeof msg.callId === "string") {
          endedCallIds.push(msg.callId);
        }
      });
      ws.send(JSON.stringify({ type: "join", roomId: room, username: name }));
      await sleep(150);
    },
    drop: () => ws?.terminate(),
    send: o => ws.send(JSON.stringify(o)),
    has: t => got.some(g => g.type === t),
    types: () => got.map(g => g.type),
    clear: () => { got.length = 0; endedCallIds.length = 0; },
    close: () => ws?.close()
  };
}

let pass = 0, fail = 0;
const check = (label, cond, extra = "") => {
  if (cond) { pass++; console.log(`  ✅ ${label}`); }
  else { fail++; console.log(`  ❌ ${label} ${extra}`); }
};
const logHas = re => logLines.some(l => re.test(l));

await sleep(700);

console.log("\nLEGACY 1 — bezny hovor starou appkou (nove sokety, offer bez 'to')");
{
  const A = legacyApp("stary.volajuci@x.sk"), B = legacyApp("stary.volany@x.sk");
  await A.openRoom("stary.volajuci@x.sk");
  await B.openRoom("stary.volany@x.sk");
  await A.openRoom("CALL-L1");
  A.send({ type: "call", callId: "CALL-L1", callerName: "Stary" }); await sleep(120);
  await B.openRoom("CALL-L1");
  check("volany dostal incoming-call", B.has("incoming-call"), B.types().join(","));
  B.send({ type: "accept", callId: "CALL-L1" }); await sleep(200);
  check("volajuci dostal call-accepted", A.has("call-accepted"), A.types().join(","));
  A.send({ type: "offer", sdp: "o", callId: "CALL-L1" }); await sleep(100);
  check("volany dostal offer (broadcast bez 'to')", B.has("offer"), B.types().join(","));
  B.send({ type: "answer", sdp: "a", callId: "CALL-L1" }); await sleep(100);
  check("volajuci dostal answer", A.has("answer"), A.types().join(","));
  A.send({ type: "hangup", callId: "CALL-L1" }); await sleep(200);
  check("volany dostal call-ended AJ S callId (stara appka ho inak zahodi)",
        B.endedCallIds.includes("CALL-L1"), JSON.stringify(B.endedCallIds));
  check("summary = SPOJENY", logHas(/CALL-SUMMARY call=CALL-L1 verdikt=SPOJENY/));
  A.close(); B.close(); await sleep(200);
}

console.log("\nLEGACY 2 — PRESNE SCENAR Z RENDER LOGU: volajucemu spadne socket, volany prave prijima");
logLines = [];
{
  const A = legacyApp("michal@x.sk"), B = legacyApp("petra@x.sk");
  await A.openRoom("CALL-L2");
  A.send({ type: "call", callId: "CALL-L2", callerName: "Michal" }); await sleep(120);
  await B.openRoom("CALL-L2");
  B.clear();

  A.drop();                                  // vypadok siete volajuceho
  await sleep(250);
  check("volanemu NEPRESTALO zvonit (ziadny call-ended)", B.endedCallIds.length === 0, JSON.stringify(B.endedCallIds));

  B.send({ type: "accept", callId: "CALL-L2" }); await sleep(200);
  check("server prijatie odlozil, nezahodil", logHas(/ACCEPT-ORPHANED call=CALL-L2/));

  await A.reopenRoomAfterDrop("CALL-L2");    // appka sa pripojila znova
  await sleep(250);
  check("volajuci dostal odlozene prijatie", A.has("call-accepted"), A.types().join(","));
  check("log hlasi PEER-RECONNECT", logHas(/PEER-RECONNECT call=CALL-L2/));

  A.send({ type: "offer", sdp: "o", callId: "CALL-L2" }); await sleep(120);
  check("hovor pokracuje: volany dostal offer", B.has("offer"), B.types().join(","));
  A.close(); B.close(); await sleep(200);
}

console.log("\nLEGACY 3 — volajuci zrusi zvonenie: volanemu musi prestat zvonit HNED");
logLines = [];
{
  const A = legacyApp("zrus@x.sk"), B = legacyApp("prijem@x.sk");
  await A.openRoom("CALL-L3");
  A.send({ type: "call", callId: "CALL-L3", callerName: "Z" }); await sleep(100);
  await B.openRoom("CALL-L3");
  B.clear();
  const t0 = Date.now();
  A.send({ type: "hangup", callId: "CALL-L3" }); await sleep(250);
  const elapsed = Date.now() - t0;
  check("volanemu prestalo zvonit okamzite (nie az po lehote)",
        B.endedCallIds.includes("CALL-L3") && elapsed < GRACE, `${elapsed}ms ${JSON.stringify(B.endedCallIds)}`);
  A.close(); B.close(); await sleep(200);
}

console.log("\nLEGACY 4 — nove spojenie predbehne zatvorenie stareho (supersede)");
logLines = [];
{
  const A = legacyApp("rychly@x.sk"), B = legacyApp("druhy@x.sk");
  await A.openRoom("CALL-L4");
  A.send({ type: "call", callId: "CALL-L4", callerName: "R" }); await sleep(100);
  await B.openRoom("CALL-L4");
  B.clear();
  await A.openRoom("CALL-L4");                // join noveho socketu ide PRED close stareho
  await sleep(300);
  check("volanemu neprisiel falosny call-ended", B.endedCallIds.length === 0, JSON.stringify(B.endedCallIds));
  B.send({ type: "accept", callId: "CALL-L4" }); await sleep(200);
  check("prijatie dorazilo volajucemu", A.has("call-accepted"), A.types().join(","));
  A.close(); B.close(); await sleep(200);
}

console.log("\nLEGACY 5 — starym klientom stale funguje chat");
logLines = [];
{
  const A = legacyApp("chat1@x.sk"), B = legacyApp("chat2@x.sk");
  await A.openRoom("chat1@x.sk"); await B.openRoom("chat2@x.sk");
  A.send({ type: "chat-message", to: "chat2@x.sk", content: "ahoj", kind: "text", messageId: "lm1" });
  await sleep(250);
  check("prijemca dostal spravu", B.has("chat-message"), B.types().join(","));
  A.close(); B.close(); await sleep(200);
}

console.log(`\n===== ${pass} OK / ${fail} ZLYHALO =====`);
srv.kill();
process.exit(fail ? 1 : 0);
