Run all LUMI preflight checks in order. Stop if any fail.

1. Run: ./scripts/preflight_deps.sh
   If FAIL: list missing packages and install command

2. Run: ./scripts/preflight_github.sh
   If FAIL: check VPN, auth, repo URL

3. Run: ./scripts/preflight_mdm.sh
   If FAIL: check VPN, endpoint URL, table name format
   If PASS: confirm sample_mdm_response.json was saved to fixtures

4. Run: ./scripts/preflight_llm.sh
   If FAIL: check gcloud auth, project, Vertex AI access

Report: all pass/fail results and any actions needed
