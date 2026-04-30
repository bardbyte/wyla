# Onboarding — share this in Slack

Copy everything below the `---` divider into Slack to onboard a teammate.
Keep this file generic; if you want to personalize, prepend a `Hey {name},`
line in your DM, but leave this template alone so it stays reusable.

---

Sharing how to get the LUMI agent stack running on your laptop. ~5 min setup.

**Repo:** https://github.com/bardbyte/wyla
**Stack:** Gemini 3.1 Pro on Vertex AI (`prj-d-ea-poc`) via Google ADK web UI.

### One-time setup

1. Get a service account JSON for `prj-d-ea-poc`. GCP console → IAM → Service Accounts → pick the SA that has `roles/aiplatform.user` → Keys → Add key (JSON). Save it **outside the repo**, e.g. `~/Downloads/key.json`.

2. Clone, venv, install:
```
git clone https://github.com/bardbyte/wyla
cd wyla
python3 -m venv .venv && source .venv/bin/activate
pip install google-adk truststore openpyxl sqlglot
```

### Every session — two commands
```
source agent_test/setup_vertex_env.sh ~/Downloads/key.json
adk web apps/
```

Open http://localhost:8000. Two agents in the sidebar:
- `vertex_smoke` — dice/prime canonical test, run this first to confirm Vertex auth works
- `curator` — Gemini 3.1 Pro auditor for gold-query Excels

In `curator`, **type a file path** (don't use the paperclip — Gemini rejects `.xlsx` as multimodal input):
> Audit the Excel at `/Users/yourname/Downloads/gold_queries.xlsx`. Is it ready?

### What's actually happening

**No .env file needed.** `setup_vertex_env.sh` exports four env vars into the current shell:
- `GOOGLE_APPLICATION_CREDENTIALS=<your key path>`
- `GOOGLE_GENAI_USE_VERTEXAI=true`
- `GOOGLE_CLOUD_PROJECT=prj-d-ea-poc`
- `GOOGLE_CLOUD_LOCATION=global`

The agent's tools open the Excel from disk via `openpyxl` — only structured tool outputs (sheet names, sample markdown tables, SQL parse results) ever travel to Gemini. The binary stays on your laptop.

### If something breaks

| Symptom | Fix |
|---|---|
| `403 Permission denied` on the model call | SA missing `roles/aiplatform.user` on the project |
| `SSLCertVerificationError` | Confirm VPN is on. `truststore` reads from macOS Keychain — corporate root CA is normally pre-installed on Amex laptops |
| `ModuleNotFoundError` | You're not in the repo root — `cd wyla` first |
| `400 mime type ... not supported` | You attached the file via the paperclip — type the path in chat instead |
| UI shows no agents | You ran `adk web` against the wrong dir — must be `adk web apps/` from the repo root |

Ping me if you hit anything else.
