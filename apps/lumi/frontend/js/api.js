/** Typed-ish client for the Meridian read plane. Every payload
 * carries `available`; false means no compiled build — pages render
 * their designed empty state with the server's own reason. */

async function get(url) {
  try {
    const r = await fetch(url);
    if (!r.ok) return { available: false, reason: `${url} → ${r.status}` };
    return await r.json();
  } catch {
    return { available: false, reason: "console unreachable — is the server running?" };
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
    return get(`/api/meridian/explorer/metrics?${search}`);
  },
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
};
