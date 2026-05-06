#!/bin/bash
# =============================================================================
# ILAM — Submit Flink SQL Job via SQL Gateway REST API
# =============================================================================

set -e

GATEWAY_URL="http://localhost:8083"
SQL_FILE="ingestion/streaming/flink_cdr_job.sql"

echo "============================================"
echo "ILAM — Flink SQL Gateway Job Submission"
echo "============================================"

# 1. Ouvrir une session avec checkpointing activé
echo "Opening SQL Gateway session..."
SESSION=$(curl -s -X POST \
  "${GATEWAY_URL}/v1/sessions" \
  -H "Content-Type: application/json" \
  -d '{
    "properties": {
      "execution.checkpointing.interval": "30s",
      "execution.checkpointing.mode": "EXACTLY_ONCE",
      "parallelism.default": "1"
    }
  }')

SESSION_ID=$(echo $SESSION | python3 -c \
  "import sys,json; print(json.load(sys.stdin)['sessionHandle'])")
echo "Session ID: $SESSION_ID"

# 2. Soumettre les statements SQL
echo "Submitting SQL statements..."
python3 << PYEOF
import requests, json, time

GATEWAY = "${GATEWAY_URL}"
SESSION = "${SESSION_ID}"

with open("${SQL_FILE}") as f:
    content = f.read()

# Supprimer les commentaires et découper par ;
lines = [l for l in content.split('\n') if not l.strip().startswith('--')]
content_clean = '\n'.join(lines)
statements = [s.strip() for s in content_clean.split(';') if s.strip()]

print(f"Found {len(statements)} statements to execute")

for i, stmt in enumerate(statements):
    print(f"\n[{i+1}/{len(statements)}] {stmt[:70].strip()}...")

    resp = requests.post(
        f"{GATEWAY}/v1/sessions/{SESSION}/statements",
        headers={"Content-Type": "application/json"},
        json={"statement": stmt + ";"}
    )

    if resp.status_code != 200:
        print(f"  HTTP ERROR {resp.status_code}: {resp.text[:200]}")
        continue

    op_handle = resp.json().get('operationHandle')
    if not op_handle:
        print(f"  No operation handle — skipping")
        continue

    # Attendre completion
    status = 'INITIALIZED'
    for attempt in range(60):
        status_resp = requests.get(
            f"{GATEWAY}/v1/sessions/{SESSION}/operations/{op_handle}/status"
        )
        status = status_resp.json().get('status', 'UNKNOWN')

        if status == 'FINISHED':
            print(f"  OK (FINISHED)")
            break
        elif status == 'RUNNING':
            print(f"  OK (RUNNING — streaming job)")
            break
        elif status in ['ERROR', 'CANCELED', 'CLOSED']:
            result_resp = requests.get(
                f"{GATEWAY}/v1/sessions/{SESSION}/operations/{op_handle}/result/0"
            )
            print(f"  ERROR: {result_resp.text[:300]}")
            break
        else:
            time.sleep(1)

print("\n============================================")
print("All statements submitted successfully.")
print(f"Gateway session: {SESSION}")
print("Check Flink UI: http://localhost:8081")
print("============================================")
PYEOF

# Garder la session active
echo ""
echo "Keeping session alive — jobs are streaming."
echo "Press Ctrl+C to stop."
echo ""

while true; do
    # Vérifier les jobs toutes les 30 secondes
    JOBS=$(curl -s http://localhost:8081/jobs | \
        python3 -c "import sys,json; \
        jobs=json.load(sys.stdin)['jobs']; \
        running=[j for j in jobs if j['status']=='RUNNING']; \
        print(f'Running: {len(running)} job(s)')" 2>/dev/null)
    echo "[$(date '+%H:%M:%S')] $JOBS"
    sleep 30
done
