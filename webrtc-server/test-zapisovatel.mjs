// Test pravidiel AI zapisovatela v signaling serveri (bez skutocneho zvuku,
// prepisu a Frappe - zapisovatel bezi "nasucho", AI_DRY_RUN=1).
import { spawn } from "child_process";
import { fileURLToPath } from "url";
import { dirname } from "path";
import { WebSocket } from "ws";

const PORT = 18098;
const WS_URL = `ws://127.0.0.1:${PORT}/ws`;
const SECRET = "test-tajomstvo";
const FEATURES = ["ai-zapisovatel"];

let logLines = [];
const srv = spawn(process.execPath, ["server.js"], {
  cwd: dirname(fileURLToPath(import.meta.url)),
  env: {
    ...process.env, PORT: String(PORT), RECONNECT_GRACE_MS: "1500",
    AI_ZAPISOVATEL: "on", AI_DRY_RUN: "1", AI_SECRET: SECRET
  }
});
srv.stdout.on("data", d => { logLines.push(...d.toString().split("\n").filter(Boolean)); });
srv.stderr.on("data", d => console.error("SRV-ERR", d.toString()));

const sleep = ms => new Promise(r => setTimeout(r, ms));
const logHas = re => logLines.some(l => re.test(l));
async function waitLog(re, ms = 4000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { if (logHas(re)) return true; await sleep(50); }
  return false;
}

function client(name, features) {
  const ws = new WebSocket(WS_URL);
  const got = [];
  ws.on("message", m => got.push(JSON.parse(m.toString())));
  return {
    name, ws, got,
    ready: new Promise(res => ws.on("open", res)),
    closed: new Promise(res => ws.on("close", res)),
    send: o => ws.send(JSON.stringify(o)),
    join: (room, extra = {}) => ws.send(JSON.stringify({ type: "join", roomId: room, username: name, ...(features ? { features } : {}), ...extra })),
    has: (t, pred = () => true) => got.some(g => g.type === t && pred(g)),
    close: () => ws.close()
  };
}

let pass = 0, fail = 0;
function check(label, cond, extra = "") {
  if (cond) { pass++; console.log(`  ✅ ${label}`); }
  else { fail++; console.log(`  ❌ ${label} ${extra}`); }
}
const isBotPeer = g => typeof g.peerId === "string" && g.peerId.startsWith("ai-zapisovatel");

/** A zavola B, B prijme. Vrati klientov. */
async function hovor(callId, fa = FEATURES, fb = FEATURES) {
  const A = client("a@x.sk", fa), B = client("b@x.sk", fb);
  await Promise.all([A.ready, B.ready]);
  A.join(callId); B.join(callId); await sleep(120);
  A.send({ type: "call", callId, callerName: "Ander" }); await sleep(100);
  B.send({ type: "accept", callId }); await sleep(100);
  return { A, B };
}

await waitLog(/AI-PROCES-START/, 5000);

// ---------------------------------------------------------------- TEST 1
console.log("\nTEST 1 — zapisovatel bez tajneho kluca sa nedostane dnu");
{
  const X = client("ai-zapisovatel+CALL-Z1");
  await X.ready;
  X.join("CALL-Z1");
  const closed = await Promise.race([X.closed.then(() => true), sleep(1500).then(() => false)]);
  check("server spojenie zavrel", closed);
  check("log hlasi BOT-ODMIETNUTY", logHas(/BOT-ODMIETNUTY .*user=ai-zapisovatel\+CALL-Z1/));
}

// ---------------------------------------------------------------- TEST 2
console.log("\nTEST 2 — prijaty hovor: zapisovatel sa pripoji a ohlasi");
let room2;
{
  room2 = await hovor("CALL-Z2");
  check("log hlasi AI-START", await waitLog(/AI-START call=CALL-Z2/));
  await waitLog(/AI-V-HOVORE call=CALL-Z2/);
  await sleep(300);
  check("volany videl vstup zapisovatela", room2.B.has("peer-joined", isBotPeer));
  check("zapisovatel sa ohlasil menom", room2.A.has("conf-state", g => g.name === "AI zapisovateľ"));
  const h = await fetch(`http://127.0.0.1:${PORT}/health`).then(r => r.json());
  check("/health ukazuje 1 nahravanie", h.zapisovatel?.nahrava === 1, JSON.stringify(h.zapisovatel));
}

// ---------------------------------------------------------------- TEST 3
console.log("\nTEST 3 — zapisovatel sa nepocita do limitu 5 ludi");
{
  const extra = ["c@x.sk", "d@x.sk", "e@x.sk"].map(n => client(n, FEATURES));
  await Promise.all(extra.map(c => c.ready));
  extra.forEach(c => c.join("CALL-Z2")); await sleep(200);
  check("5. clovek sa dostal dnu (nie room-full)", !extra.some(c => c.has("room-full")));
  const F = client("f@x.sk", FEATURES); await F.ready;
  F.join("CALL-Z2"); await sleep(200);
  check("6. clovek dostal room-full", F.has("room-full"));
  for (const c of [...extra, F]) { c.send({ type: "leave" }); c.close(); }
  await sleep(200);
}

// ---------------------------------------------------------------- TEST 4
console.log("\nTEST 4 — zavesenie 1:1 hovoru ukonci hovor aj so zapisovatelom v miestnosti");
{
  const { A, B } = room2;
  B.got.length = 0;
  A.send({ type: "leave" }); await sleep(250);
  check("volany dostal call-ended", B.has("call-ended"));
  check("zapisovatel skoncil nahravanie", await waitLog(/AI-NAHRAVANIE-KONIEC call=CALL-Z2/));
  check("zapisovatel zapisal stav (nasucho)", await waitLog(/AI-ULOZENIE-NASUCHO call=CALL-Z2 stav=Bez záznamu/));
  check("nahravky zmazane", await waitLog(/AI-ZMAZANE call=CALL-Z2/));
  A.close(); B.close(); await sleep(150);
}

// ---------------------------------------------------------------- TEST 5
console.log("\nTEST 5 — volany ma staru appku: zapisovatel sa nepripoji");
{
  const { A, B } = await hovor("CALL-Z3", FEATURES, null);
  await sleep(400);
  check("log hlasi AI-PRESKOCENE", logHas(/AI-PRESKOCENE call=CALL-Z3/));
  check("ziadny zapisovatel v miestnosti", !A.has("peer-joined", isBotPeer));
  A.send({ type: "leave" }); A.close(); B.close(); await sleep(150);
}

// ---------------------------------------------------------------- TEST 6
console.log("\nTEST 6 — do hovoru sa prida niekto so starou appkou: nahravanie sa zrusi");
{
  const { A, B } = await hovor("CALL-Z4");
  await waitLog(/AI-V-HOVORE call=CALL-Z4/);
  const C = client("c@x.sk", null); await C.ready;
  C.join("CALL-Z4"); await sleep(300);
  check("log hlasi AI-ZRUSENE", logHas(/AI-ZRUSENE call=CALL-Z4/));
  check("zapisovatel odisiel", await waitLog(/AI-NAHRAVANIE-KONIEC call=CALL-Z4 .*zrusene=ano/));
  check("hovor ludi bezi dalej (bez call-ended)", !B.has("call-ended"));
  for (const c of [A, B, C]) { c.send({ type: "leave" }); c.close(); }
  await sleep(200);
}

console.log(`\n${pass} OK, ${fail} zlyhalo`);
srv.kill();
process.exit(fail ? 1 : 0);
