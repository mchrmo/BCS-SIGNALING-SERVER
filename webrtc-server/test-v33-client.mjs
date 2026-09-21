// Simulacia appky VERZIE 3.3 — tej, ktora je teraz na App Store.
// Sprava sa presne ako jej SignalingClient: pri kazdej zmene miestnosti
// otvara NOVE spojenie, offer/answer smeruje cez pole "to", posiela conf-state
// a na koniec hovoru posiela "leave".
import { spawn } from "child_process";
import { fileURLToPath } from "url";
import { dirname } from "path";
import { WebSocket } from "ws";

const PORT = 18093;
const srv = spawn(process.execPath, ["server.js"], {
  cwd: dirname(fileURLToPath(import.meta.url)),
  env: { ...process.env, PORT: String(PORT), RECONNECT_GRACE_MS: "1500" },
  stdio: "ignore"
});
const WS_URL = `ws://127.0.0.1:${PORT}/ws`;
const sleep = ms => new Promise(r => setTimeout(r, ms));

function app33(name) {
  let ws = null;
  const got = [];
  const attach = () => ws.on("message", m => got.push(JSON.parse(m.toString())));

  // enterCallRoom / leaveCallRoom vo verzii 3.3 = vzdy openSocket()
  async function enterRoom(room) {
    const old = ws;
    ws = new WebSocket(WS_URL);
    await new Promise(r => ws.on("open", r));
    attach();
    ws.send(JSON.stringify({ type: "join", roomId: room, username: name }));
    if (old) old.close();
    await sleep(160);
  }
  return {
    name, got, enterRoom,
    async reconnectTo(room) {                        // vypadok siete -> auto-reconnect
      if (ws) ws.terminate();
      await sleep(200);
      ws = new WebSocket(WS_URL);
      await new Promise(r => ws.on("open", r));
      attach();
      ws.send(JSON.stringify({ type: "join", roomId: room, username: name }));
      await sleep(160);
    },
    drop: () => ws?.terminate(),
    send: o => ws.send(JSON.stringify(o)),
    confState: muted => ws.send(JSON.stringify({ type: "conf-state", name, muted: !!muted })),
    has: t => got.some(g => g.type === t),
    clear: () => { got.length = 0; },
    close: () => ws?.close()
  };
}

await sleep(700);
const R = [];

// --- A: volajucemu spadne spojenie pocas zvonenia, volany prave prijima -----
{
  const A = app33("michal@x.sk"), B = app33("petra@x.sk");
  await A.enterRoom("michal@x.sk");                  // idle
  await B.enterRoom("petra@x.sk");                   // idle
  await A.enterRoom("CALL-A");                       // volajuci: startCall
  A.send({ type: "call", callId: "CALL-A", callerName: "Michal" });
  A.confState(false); await sleep(120);
  await B.enterRoom("CALL-A");                       // volany: pre-connect z VoIP pushu
  B.clear();

  A.drop();                                          // vypadok volajuceho
  await sleep(250);
  const ringingKilled = B.has("call-ended");

  B.send({ type: "accept", callId: "CALL-A" });      // pouzivatel zdvihol
  B.confState(false);
  await sleep(200);
  await A.reconnectTo("CALL-A");
  await sleep(300);
  const acceptArrived = A.has("call-accepted");

  A.send({ type: "offer", sdp: "o", to: "petra@x.sk", callId: "CALL-A" });
  await sleep(150);
  const offerArrived = B.has("offer");

  R.push(["A. volajucemu spadne spojenie pocas zvonenia",
          ["volanemu prestalo zvonit", ringingKilled, false],
          ["volajuci dostal prijatie", acceptArrived, true],
          ["ponuka dorazila volanemu", offerArrived, true]]);
  A.close(); B.close(); await sleep(200);
}

// --- B: volanemu spadne spojenie hned po prijati hovoru ---------------------
{
  const A = app33("a2@x.sk"), B = app33("b2@x.sk");
  await A.enterRoom("CALL-B");
  A.send({ type: "call", callId: "CALL-B", callerName: "A2" }); A.confState(false);
  await sleep(120);
  await B.enterRoom("CALL-B");
  B.send({ type: "accept", callId: "CALL-B" }); B.confState(false);
  await sleep(200);
  A.clear();
  B.drop();                                          // vypadok volaneho
  await sleep(250);
  const callKilled = A.has("call-ended");
  await B.reconnectTo("CALL-B");
  await sleep(300);
  A.send({ type: "offer", sdp: "o", to: "b2@x.sk", callId: "CALL-B" });
  await sleep(150);
  const recovered = B.has("offer");

  R.push(["B. volanemu spadne spojenie po prijati",
          ["hovor sa ukoncil volajucemu", callKilled, false],
          ["po navrate hovor pokracuje", recovered, true]]);
  A.close(); B.close(); await sleep(200);
}

// --- C: normalne zavesenie musi ukoncit hovor OKAMZITE ----------------------
{
  const A = app33("a3@x.sk"), B = app33("b3@x.sk");
  await A.enterRoom("CALL-C");
  A.send({ type: "call", callId: "CALL-C", callerName: "A3" }); await sleep(100);
  await B.enterRoom("CALL-C");
  B.send({ type: "accept", callId: "CALL-C" }); await sleep(150);
  B.clear();
  const t0 = Date.now();
  A.send({ type: "leave" });
  await sleep(300);
  const ended = B.has("call-ended");
  const ms = Date.now() - t0;
  R.push(["C. normalne zavesenie",
          ["druha strana dostala koniec hovoru", ended, true],
          ["stalo sa to do 1 s", ended && ms < 1000, true]]);
  A.close(); B.close(); await sleep(200);
}

let bad = 0;
for (const [title, ...rows] of R) {
  console.log(`\n${title}`);
  for (const [label, actual, expected] of rows) {
    const ok = actual === expected;
    if (!ok) bad++;
    console.log(`   ${ok ? "✅" : "❌"} ${label}: ${actual ? "ANO" : "NIE"}  (ocakavane: ${expected ? "ANO" : "NIE"})`);
  }
}
console.log(`\n===== ${bad === 0 ? "vsetko OK" : bad + " ZLYHANI"} =====`);
srv.kill();
process.exit(bad ? 1 : 0);
