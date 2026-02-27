#!/usr/bin/env bash
set -euo pipefail

SSH_KEY="/home/jbeck/git/claude/ssh/linode"
HOST="root@192.46.223.21"
DEST="/opt/driftwatch/"
GIT_SHA=$(git rev-parse --short HEAD)

echo "==> Syncing code to ${HOST}:${DEST} (${GIT_SHA})"
rsync -avz --delete \
  --exclude '.git' \
  --exclude 'deploy/data' \
  --exclude 'deploy/.env' \
  --exclude 'deploy/.env.prod' \
  --exclude 'deploy/out' \
  --exclude 'deploy/maintenance.sh' \
  --exclude '__pycache__' \
  --exclude '.pytest_cache' \
  --exclude '.claude' \
  -e "ssh -i ${SSH_KEY}" \
  ./ "${HOST}:${DEST}"

echo "==> Rebuilding + restarting container (GIT_SHA=${GIT_SHA})"
ssh -i "${SSH_KEY}" "${HOST}" \
  "cd /opt/driftwatch/deploy && GIT_SHA=${GIT_SHA} docker compose -f docker-compose.prod.yml up -d --build"

echo "==> Waiting for health..."
for i in $(seq 1 40); do
    if ssh -i "${SSH_KEY}" "${HOST}" \
        "curl -sf --max-time 3 http://localhost:8422/health" >/dev/null 2>&1; then
        echo "==> Healthy after $((i * 15))s"
        ssh -i "${SSH_KEY}" "${HOST}" \
          "curl -s http://localhost:8422/health/extended | python3 -m json.tool"
        exit 0
    fi
    echo "    waiting... (${i}/40)"
    sleep 15
done

echo "!!! Health check timed out after 10 minutes"
echo "==> Recent logs:"
ssh -i "${SSH_KEY}" "${HOST}" "docker logs driftwatch --tail 30"
exit 1
