/** Typed-ish client for the Meridian read plane. Every payload
 * carries `available`; false means no compiled build: pages render
 * their designed empty state with the server's own reason. */

async function get(url) {
  try {
    const r = await fetch(url);
    if (!r.ok) return { available: false, reason: `${url} → ${r.status}` };
    return await r.json();
  } catch {
    return { available: false, reason: "console unreachable: is the server running?" };
  }
}

// Ask posts carry the same contract as the gets: the body always
// answers `available`, so a refusal (no build, a busy turn) is data the
// page can render, never an exception it has to guess at.
async function post(url, body) {
  try {
    const r = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body ?? {}),
    });
    const payload = await r.json().catch(() => ({}));
    if (!r.ok && payload.available === undefined) {
      return { available: false, reason: `${url} → ${r.status}` };
    }
    return payload;
  } catch {
    return { available: false, reason: "console unreachable: is the server running?" };
  }
}

export const api = {
  home: () => get("/api/meridian/home"),
  sources: () => get("/api/meridian/sources"),
  planes: () => get("/api/lumi/planes"),
  metrics: (params = {}) => {
    const search = new URLSearchParams();
    if (params.q) search.set("q", params.q);
    if (params.status) search.set("status", params.status);
    if (params.lob) search.set("lob", params.lob);
    if (params.table) search.set("table", params.table);
    return get(`/api/meridian/explorer/metrics?${search}`);
  },
  artifactFile: (rel) =>
    get(`/api/meridian/artifact_file?rel=${encodeURIComponent(rel)}`),
  tables: () => get("/api/meridian/explorer/tables"),
  metric: (id) => get(`/api/meridian/metric/${encodeURIComponent(id)}`),
  table: (physical) =>
    get(`/api/meridian/table/${encodeURIComponent(physical)}`),
  graphMap: () => get("/api/meridian/graph_map"),
  builds: () => get("/api/meridian/builds"),
  enrichRuns: () => get("/api/meridian/enrich_runs"),
  artifacts: () => get("/api/meridian/artifacts"),
  stageArtifact: (payload) =>
    fetch("/api/meridian/artifacts", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }).then(async (r) => ({ ok: r.ok, ...(await r.json()) })),
  feedback: (payload) =>
    fetch("/api/meridian/feedback", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }),

  /* ── Ask (E18): sessions, turns, the stream ──────────────
   * The page never learns a model endpoint and never holds a key.
   * It posts a turn, then reads the event stream the server emits. */
  askSessions: (limit = 30) => get(`/api/sessions?limit=${limit}`),
  askNewSession: (kind = "analyst") => post("/api/sessions", { kind }),
  askSession: (id) => get(`/api/sessions/${encodeURIComponent(id)}`),
  askSend: (id, text, choice = null) =>
    post(`/api/sessions/${encodeURIComponent(id)}/messages`,
      { text, choice }),
  askStop: (id) => post(`/api/sessions/${encodeURIComponent(id)}/stop`),
  askFeedback: (id, payload) =>
    post(`/api/sessions/${encodeURIComponent(id)}/feedback`, payload),
  askStreamUrl: (id, after = 0) =>
    `/api/sessions/${encodeURIComponent(id)}/stream?after=${after}`,
};
