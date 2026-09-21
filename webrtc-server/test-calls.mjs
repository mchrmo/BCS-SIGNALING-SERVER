import { spawn } from "child_process";
import { fileURLToPath } from "url";
import { dirname } from "path";
import { WebSocket } from "ws";

const PORT = 18099;
const GRACE = 1500;
const WS_URL = `ws://127.0.0.1:${PORT}/ws`;

let logLines = [];
const srv = spawn(process.execPath, ["server.js"], {
  cwd: dirname(fileURLToPath(import.meta.url)),
  env: { ...process.env, PORT: String(PORT), RECONNECT_GRACE_MS: String(GRACE) }
});
srv.stdout.on("data", d => { const s = d.toString(); logLines.push(...s.split("\n").filter(Boolean)); });
srv.stderr.on("data", d => console.error("SRV-ERR", d.toString()));

const sleep = ms => new Promise(r => setTimeout(r, ms));

function client(name) {
  const ws = new WebSocket(WS_URL);
  const got = [];
  ws.on("message", m => got.push(JSON.parse(m.toString())));
  const api = {
    name, ws, got,
    ready: new Promise(res => ws.on("open", res)),
    send: o => ws.send(JSON.stringify(o)),
    join: room => ws.send(JSON.stringify({ type: "join", roomId: room, username: name })),
    types: () => got.map(g => g.type),
    has: t => got.some(g => g.type === t),
    clear: () => { got.length = 0; },
    kill: () => ws.terminate(),
    close: () => ws.close()
  };
  return api;
}

let pass = 0, fail = 0;
function check(label, cond, extra = "") {
  if (cond) { pass++; console.log(`  ✅ ${label}`); }
  else { fail++; console.log(`  ❌ ${label} ${extra}`); }
}
const logHas = re => logLines.some(l => re.test(l));

await sleep(700);

// ---------------------------------------------------------------- TEST 1
console.log("\nTEST 1 — bezny hovor od zaciatku do konca");
{
  const A = client("a@x.sk"), B = client("b@x.sk");
  await Promise.all([A.ready, B.ready]);
  A.join("a@x.sk"); B.join("b@x.sk"); await sleep(120);
  A.join("CALL-1"); await sleep(120);
  A.send({ type: "call", callId: "CALL-1", callerName: "Ander" }); await sleep(120);
  B.join("CALL-1"); await sleep(150);
  check("volany dostal incoming-call", B.has("incoming-call"));
  B.send({ type: "accept", callId: "CALL-1" }); await sleep(150);
  check("volajuci dostal call-accepted", A.has("call-accepted"));
  A.send({ type: "offer", sdp: "o", to: "b@x.sk", callId: "CALL-1" }); await sleep(80);
  check("volany dostal offer", B.has("offer"));
  B.send({ type: "answer", sdp: "a", to: "a@x.sk", callId: "CALL-1" }); await sleep(80);
  check("volajuci dostal answer", A.has("answer"));
  A.send({ type: "leave" }); await sleep(200);
  check("summary = SPOJENY", logHas(/CALL-SUMMARY call=CALL-1 verdikt=SPOJENY/), logLines.filter(l=>/CALL-1/.test(l)).slice(-2).join(" | "));
  A.close(); B.close(); await sleep(150);
}

// ---------------------------------------------------------------- TEST 2
console.log("\nTEST 2 — volajucemu spadne socket pocas zvonenia a vrati sa (JADRO OPRAVY)");
logLines = [];
{
  const A = client("a@x.sk"), B = client("b@x.sk");
  await Promise.all([A.ready, B.ready]);
  A.join("CALL-2"); B.join("CALL-2"); await sleep(120);
  A.send({ type: "call", callId: "CALL-2", callerName: "Ander" }); await sleep(120);
  B.clear();
  A.kill();                                  // spadnute spojenie, NIE zavesenie
  await sleep(300);
  check("volany NEDOSTAL call-ended pocas ochrannej lehoty", !B.has("call-ended"), B.types().join(","));
  check("log hlasi PEER-DISCONNECT", logHas(/PEER-DISCONNECT call=CALL-2/));

  const A2 = client("a@x.sk");               // appka sa pripojila znova
  await A2.ready; A2.join("CALL-2"); await sleep(250);
  check("log hlasi PEER-RECONNECT", logHas(/PEER-RECONNECT call=CALL-2/));
  check("volany stale nedostal call-ended", !B.has("call-ended"), B.types().join(","));

  B.send({ type: "accept", callId: "CALL-2" }); await sleep(200);
  check("prijatie dorazilo volajucemu po reconnecte", A2.has("call-accepted"), A2.types().join(","));
  A2.close(); B.close(); await sleep(150);
}

// ---------------------------------------------------------------- TEST 3
console.log("\nTEST 3 — volajuci sa uz nevrati: hovor sa po lehote ukonci");
logLines = [];
{
  const A = client("c@x.sk"), B = client("d@x.sk");
  await Promise.all([A.ready, B.ready]);
  A.join("CALL-3"); B.join("CALL-3"); await sleep(120);
  A.send({ type: "call", callId: "CALL-3", callerName: "C" }); await sleep(120);
  B.clear();
  A.kill();
  await sleep(GRACE + 500);
  check("volany dostal call-ended az po lehote", B.has("call-ended"), B.types().join(","));
  check("log hlasi PEER-LOST", logHas(/PEER-LOST call=CALL-3/));
  check("summary = NEPRIJATY", logHas(/CALL-SUMMARY call=CALL-3 verdikt=NEPRIJATY/));
  B.close(); await sleep(150);
}

// ---------------------------------------------------------------- TEST 4
console.log("\nTEST 4 — prijatie prislo skor, nez sa volajuci pripojil");
logLines = [];
{
  const B = client("f@x.sk");
  await B.ready;
  B.join("CALL-4"); await sleep(120);
  B.send({ type: "accept", callId: "CALL-4" }); await sleep(150);
  check("log hlasi ACCEPT-ORPHANED", logHas(/ACCEPT-ORPHANED call=CALL-4/));
  const A = client("e@x.sk");
  await A.ready; A.join("CALL-4"); await sleep(200);
  check("volajuci dostal odlozene prijatie", A.has("call-accepted"), A.types().join(","));
  check("log hlasi ACCEPT-REPLAY", logHas(/ACCEPT-REPLAY call=CALL-4/));
  A.close(); B.close(); await sleep(150);
}

// ---------------------------------------------------------------- TEST 5
console.log("\nTEST 5 — ten isty socket meni miestnost (idle -> hovor -> idle)");
logLines = [];
{
  const A = client("g@x.sk"), B = client("h@x.sk");
  await Promise.all([A.ready, B.ready]);
  A.join("g@x.sk"); await sleep(100);
  A.join("CALL-5"); B.join("CALL-5"); await sleep(150);
  A.send({ type: "call", callId: "CALL-5", callerName: "G" }); await sleep(100);
  B.send({ type: "accept", callId: "CALL-5" }); await sleep(150);
  check("prijatie dorucene bez noveho socketu", A.has("call-accepted"));
  B.clear();
  A.join("g@x.sk"); await sleep(250);        // navrat do idle = odchod z hovoru
  check("volany dostal call-ended po navrate volajuceho do idle", B.has("call-ended"), B.types().join(","));
  check("stara miestnost sa vycistila", logHas(/JOIN user=g@x\.sk room=g@x\.sk .*z=CALL-5/), logLines.filter(l=>/JOIN user=g/.test(l)).join(" | "));
  A.close(); B.close(); await sleep(150);
}

// ---------------------------------------------------------------- TEST 6
console.log("\nTEST 6 — signal pre niekoho, kto v miestnosti nie je");
logLines = [];
{
  const A = client("i@x.sk");
  await A.ready; A.join("CALL-6"); await sleep(120);
  A.send({ type: "offer", sdp: "o", to: "ktosi@x.sk", callId: "CALL-6" }); await sleep(150);
  check("log hlasi SIGNAL-DROP", logHas(/SIGNAL-DROP call=CALL-6 typ=offer/), logLines.slice(-3).join(" | "));
  A.close(); await sleep(150);
}

// ---------------------------------------------------------------- TEST 7
console.log("\nTEST 7 — chat funguje aj po zmenach");
logLines = [];
{
  const A = client("j@x.sk"), B = client("k@x.sk");
  await Promise.all([A.ready, B.ready]);
  A.join("j@x.sk"); B.join("k@x.sk"); await sleep(120);
  A.send({ type: "chat-message", to: "k@x.sk", content: "ahoj", kind: "text", messageId: "m1" });
  await sleep(200);
  check("prijemca dostal chat-message", B.has("chat-message"), B.types().join(","));
  A.close(); B.close(); await sleep(150);
}

// ---------------------------------------------------------------- TEST 8
console.log("\nTEST 8 — /health ukazuje zive hovory");
{
  const r = await fetch(`http://127.0.0.1:${PORT}/health`);
  const j = await r.json();
  check("health vracia diagnostiku", typeof j.online === "number" && Array.isArray(j.aktivne_hovory), JSON.stringify(j).slice(0, 120));
}

// ---------------------------------------------------------------- TEST 9
console.log("\nTEST 9 — /send-chat (Share rozsirenie, bez WebSocketu)");
logLines = [];
{
  const B = client("prijemca@x.sk");
  await B.ready;
  B.join("prijemca@x.sk"); await sleep(120);

  const r = await fetch(`http://127.0.0.1:${PORT}/send-chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      from: "odosielatel@x.sk", fromName: "Odosielatel", to: "prijemca@x.sk",
      content: "zo zdielania", kind: "text", messageId: "sm1"
    })
  });
  const j = await r.json();
  await sleep(250);
  check("endpoint potvrdil dorucenie", j.ok === true && j.delivered === 1, JSON.stringify(j));
  check("prijemca dostal spravu cez socket", B.has("chat-message"), B.types().join(","));

  const bad = await fetch(`http://127.0.0.1:${PORT}/send-chat`, {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ content: "bez prijemcu" })
  });
  check("neuplna poziadavka vrati 400", bad.status === 400, String(bad.status));
  check("log hlasi SEND-CHAT-INVALID", logHas(/SEND-CHAT-INVALID/));
  B.close(); await sleep(150);
}

console.log(`\n===== ${pass} OK / ${fail} ZLYHALO =====`);
srv.kill();
process.exit(fail ? 1 : 0);
