// build_snapshot.mjs
// Runs in GitHub Actions: fetches GBFS, maintains a compact history, and writes snapshot.json.
// Schedule: every 5 minutes (see workflow). Keeps ~36h of samples.

import fs from "node:fs";

const INFO_URL   = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json";
const STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json";

const MEMORY_HRS = 36;
const SHORT_WIN_MS = 15 * 60 * 1000; // 15 minutes

const now = Date.now();

async function getJSON(url) {
  const r = await fetch(url, { headers: { "User-Agent": "citibike-viz/1.0 (contact: you@example.com)" }});
  if (!r.ok) throw new Error(`Fetch failed ${r.status} for ${url}`);
  return r.json();
}

// Load or init history: { stations: { [id]: [{t, bikes}] } }
function loadHistory() {
  try {
    const raw = fs.readFileSync("history.json", "utf8");
    return JSON.parse(raw);
  } catch {
    return { stations: {} };
  }
}

function saveHistory(history) {
  // prune old samples
  const cutoff = now - MEMORY_HRS * 60 * 60 * 1000;
  for (const id in history.stations) {
    history.stations[id] = history.stations[id].filter(s => s.t >= cutoff);
    if (history.stations[id].length === 0) delete history.stations[id];
  }
  fs.writeFileSync("history.json", JSON.stringify(history));
}

// Turn a sequence of bike counts into deltas between consecutive samples
function toDeltas(samples) {
  const out = [];
  for (let i = 1; i < samples.length; i++) {
    const prev = samples[i - 1];
    const curr = samples[i];
    const delta = curr.bikes - prev.bikes; // + = returns/dropoffs, - = pickups
    if (delta !== 0) out.push({ t: curr.t, delta });
  }
  return out;
}

function sumNet(deltas, fromMs, toMs) {
  let net = 0;
  for (let i = deltas.length - 1; i >= 0; i--) {
    const e = deltas[i];
    if (e.t < fromMs) break;
    if (e.t <= toMs) net += e.delta;
  }
  return net;
}

function midnight(ms) {
  const d = new Date(ms);
  d.setHours(0, 0, 0, 0);
  return d.getTime();
}
function hourStart(ms) {
  const d = new Date(ms);
  d.setMinutes(0, 0, 0);
  return d.getTime();
}

(async function main() {
  const history = loadHistory();

  const [info, status] = await Promise.all([getJSON(INFO_URL), getJSON(STATUS_URL)]);
  const stationsInfo = info.data.stations;
  const stationsStatus = status.data.stations;

  // Build a quick map of latest bikes by id
  const latest = {};
  for (const s of stationsStatus) {
    if (typeof s.num_bikes_available === "number") {
      latest[s.station_id] = s.num_bikes_available;
    }
  }

  // Append current sample to history
  for (const s of stationsInfo) {
    const id = s.station_id;
    const bikes = latest[id];
    if (typeof bikes !== "number") continue;
    if (!history.stations[id]) history.stations[id] = [];
    const arr = history.stations[id];
    // avoid duplicate timestamp entries if workflow runs twice in same minute
    if (arr.length === 0 || arr[arr.length - 1].t < now - 60 * 1000) {
      arr.push({ t: now, bikes });
    }
  }

  // Compute snapshot for three windows
  const byStation = {};
  let totPickups = 0, totReturns = 0;

  const dayFrom   = midnight(now);
  const hourFrom  = hourStart(now);
  const nowFrom   = now - SHORT_WIN_MS;

  // Choose which window to publish by default (you can change this)
  const publishWindow = "day";

  for (const s of stationsInfo) {
    const id = s.station_id;
    const samples = history.stations[id];
    if (!samples || samples.length < 2) continue;

    const deltas = toDeltas(samples);

    const net_day  = sumNet(deltas, dayFrom, now);
    const net_hour = sumNet(deltas, hourFrom, now);
    const net_now  = sumNet(deltas, nowFrom, now);

    // Pick which window to expose (day/hour/now)
    const net =
      publishWindow === "hour" ? net_hour :
      publishWindow === "now"  ? net_now  :
      net_day;

    if (net !== 0) {
      byStation[id] = net;
      if (net < 0) totPickups += (-net); else totReturns += net;
    }
  }

  // Build hourly system totals for a simple bar chart, if you want later
  const hourly = {};
  for (const s of stationsInfo) {
    const id = s.station_id;
    const samples = history.stations[id];
    if (!samples || samples.length < 2) continue;
    const deltas = toDeltas(samples);
    for (const e of deltas) {
      const h = new Date(e.t); h.setMinutes(0, 0, 0);
      const key = `${h.getFullYear()}-${String(h.getMonth()+1).padStart(2,"0")}-${String(h.getDate()).padStart(2,"0")} ${String(h.getHours()).padStart(2,"0")}`;
      if (!hourly[key]) hourly[key] = { pickups: 0, returns: 0 };
      if (e.delta < 0) hourly[key].pickups += (-e.delta); else hourly[key].returns += e.delta;
    }
  }

  const snapshot = {
    generated_at: new Date(now).toISOString(),
    window: publishWindow,              // "day" by default
    short_window_minutes: 15,
    stations: Object.entries(byStation).map(([id, net]) => ({ id, net })),
    totals: { pickups: totPickups, returns: totReturns },
    hourly
  };

  fs.writeFileSync("snapshot.json", JSON.stringify(snapshot));
  saveHistory(history);
})().catch(err => {
  console.error(err);
  process.exit(1);
});
