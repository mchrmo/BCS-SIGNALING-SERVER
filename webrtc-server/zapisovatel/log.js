// Rovnaky format ako server.js: <cas> <ikona> UDALOST kluc=hodnota
// Do logu NIKDY nepatri text hovoru ani zhrnutia - len callId, dlzky, pocty, ceny.

function fields(obj) {
  return Object.entries(obj)
    .filter(([, v]) => v !== undefined && v !== null && v !== "")
    .map(([k, v]) => `${k}=${v}`)
    .join(" ");
}

function log(icon, event, data = {}) {
  const stamp = new Date().toISOString().slice(11, 23);
  console.log(`${stamp} ${icon} ${event} ${fields(data)}`.trimEnd());
}

export const logOk = (event, data) => log("✅", event, data);
export const logInfo = (event, data) => log("•", event, data);
export const logWarn = (event, data) => log("⚠️", event, data);
export const logErr = (event, data) => log("❌", event, data);
