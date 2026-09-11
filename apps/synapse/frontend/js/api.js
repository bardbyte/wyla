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
  planes: () => get("/api/synapse/planes"),
  metrics: (params = {}) => {
    const search = new URLSearchParams();
    if (params.q) search.set("q", params.q);
    if (params.status) search.set("status", params.status);
    if (params.lob) search.set("lob", params.lob);
    if (params.table) search.set("table", params.table);
    if (params.limit) search.set("limit", String(params.limit));
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
  askSkills: () => get("/api/skills"),
  chatSessions: (limit = 40) => get(`/api/chat/sessions?limit=${limit}`),
  // every chat, or the ones a fuzzy query finds, with the lines that matched
  chatSearch: (q = "", limit = 200) =>
    get(`/api/chat/search?q=${encodeURIComponent(q)}&limit=${limit}`),
  chatNewSession: () => post("/api/chat/sessions", {}),
  chatSession: (id) => get(`/api/chat/sessions/${encodeURIComponent(id)}`),
  chatSend: (id, text, depth = "", mode = "", model = "", files = []) =>
    post(`/api/chat/sessions/${encodeURIComponent(id)}/messages`,
         { text, depth, mode, model, files }),
  // files on a chat: what may be attached, add one (base64), drop one
  chatFileSupport: () => get("/api/chat/files/support"),
  chatUpload: (id, name, data_b64) =>
    post(`/api/chat/sessions/${encodeURIComponent(id)}/files`,
         { name, data_b64 }),
  chatRemoveFile: async (id, fileId) => {
    const r = await fetch(`/api/chat/sessions/${encodeURIComponent(id)
      }/files/${encodeURIComponent(fileId)}`, { method: "DELETE" });
    return r.json();
  },
  // the dials explained — this surface reads the depth entries alone
  chatDials: () => get("/api/chat/dials"),
  // the person pressed Run on a proposed query: no model call
  chatRun: (id, body) =>
    post(`/api/chat/sessions/${encodeURIComponent(id)}/run`, body),
  // the picture of a run's rows: no model call
  chatChart: (id, body) =>
    post(`/api/chat/sessions/${encodeURIComponent(id)}/chart`, body),
  chatStop: (id) => post(`/api/chat/sessions/${encodeURIComponent(id)}/stop`),
  chatRename: (id, title) =>
    post(`/api/chat/sessions/${encodeURIComponent(id)}/rename`, { title }),
  chatSkills: () => get("/api/chat/skills"),
  // the creators: a draft in the house format, a saved own skill, a
  // file as text to draft from
  chatDraft: (body) => post("/api/chat/skills/draft", body),
  chatSaveSkill: (name, text) => post("/api/chat/skills/mine", { name, text }),
  chatDeleteSkill: async (name) => {
    const r = await fetch(`/api/chat/skills/mine/${encodeURIComponent(name)}`,
                          { method: "DELETE" });
    return r.json();
  },
  chatFileText: (name, data_b64) => post("/api/chat/files/text", { name, data_b64 }),
  chatSetSkills: (id, names) =>
    post(`/api/chat/sessions/${encodeURIComponent(id)}/skills`, { names }),
  // the approval workflow: a submission goes to the manager with the
  // model's read; approve publishes, reject sends it back, resubmit
  // bumps the version; the board lists them all with the notices
  chatReviews: () => get("/api/chat/reviews"),
  chatReview: (id) => get(`/api/chat/reviews/${encodeURIComponent(id)}`),
  chatSubmitReview: (body) => post("/api/chat/reviews", body),
  chatDecideReview: (id, decision, comment = "") =>
    post(`/api/chat/reviews/${encodeURIComponent(id)}/decision`, { decision, comment }),
  chatResubmitReview: (id, body) =>
    post(`/api/chat/reviews/${encodeURIComponent(id)}/resubmit`, body),
  chatWithdrawReview: (id) =>
    post(`/api/chat/reviews/${encodeURIComponent(id)}/withdraw`, {}),
  chatReviewFileUrl: (id) => `/api/chat/reviews/${encodeURIComponent(id)}/file`,
  chatReviewsSeen: () => post("/api/chat/reviews/seen", {}),
  chatProjects: () => get("/api/chat/projects"),
  chatNewProject: (name, instructions = "") =>
    post("/api/chat/projects", { name, instructions }),
  chatSetProject: (id, projectId) =>
    post(`/api/chat/sessions/${encodeURIComponent(id)}/project`,
         { project_id: projectId }),
  chatStar: (id, on) =>
    post(`/api/chat/sessions/${encodeURIComponent(id)}/star`, { on }),
  chatArchive: (id, on) =>
    post(`/api/chat/sessions/${encodeURIComponent(id)}/archive`, { on }),
  // memory.md: the document, and the document back
  chatMemoryDoc: () => get("/api/chat/memory.md"),
  chatSaveMemoryDoc: async (text) => {
    const r = await fetch("/api/chat/memory.md", { method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text }) });
    return r.json();
  },
  chatMemories: (projectId = "") =>
    get(`/api/chat/memories?project_id=${encodeURIComponent(projectId)}`),
  chatRetireMemory: (id) =>
    post(`/api/chat/memories/${encodeURIComponent(id)}/retire`, {}),
  chatPptxUrl: (id) =>
    `/api/chat/artifacts/${encodeURIComponent(id)}/export.pptx`,
  chatArtifact: (id, version = null) =>
    get(`/api/chat/artifacts/${encodeURIComponent(id)}`
        + (version ? `?version=${version}` : "")),
  chatArtifactVersions: (id) =>
    get(`/api/chat/artifacts/${encodeURIComponent(id)}/versions`),
  chatStreamUrl: (id, after = 0) =>
    `/api/chat/sessions/${encodeURIComponent(id)}/stream?after=${after}`,
  askSetSkills: (id, names) =>
    post(`/api/sessions/${encodeURIComponent(id)}/skills`, { names }),
  askFeedback: (id, payload) =>
    post(`/api/sessions/${encodeURIComponent(id)}/feedback`, payload),
  askRestorePlan: (id, version) =>
    post(`/api/sessions/${encodeURIComponent(id)}/plan/restore`,
      { version }),
  askStreamUrl: (id, after = 0) =>
    `/api/sessions/${encodeURIComponent(id)}/stream?after=${after}`,
};
