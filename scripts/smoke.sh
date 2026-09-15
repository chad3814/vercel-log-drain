#!/usr/bin/env bash
set -euo pipefail

IMAGE="vercel-log-drain:smoke"
NAME="vld-smoke-$$"
PORT="18080"

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> building image"
docker build --build-arg APP_VERSION=smoke -t "$IMAGE" .

echo "==> starting container"
docker run -d --name "$NAME" \
  -p "127.0.0.1:${PORT}:8080" \
  -e AUTH_MODE=disabled \
  -e LOG_LEVEL=info \
  --tmpfs /config:uid=10001,gid=10001 \
  --tmpfs /spool:uid=10001,gid=10001 \
  --tmpfs /logs:uid=10001,gid=10001 \
  "$IMAGE" >/dev/null

echo "==> waiting for health"
for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${PORT}/healthz" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS "http://127.0.0.1:${PORT}/healthz" >/dev/null || {
  echo "FAIL: healthz never became ready"; docker logs "$NAME"; exit 1; }

echo "==> creating a drain"
CREATED=$(curl -fsS -X POST "http://127.0.0.1:${PORT}/api/admin/drains" \
  -H 'content-type: application/json' -d '{"name":"smoke"}')
DRAIN_ID=$(printf '%s' "$CREATED" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).id))')
SECRET=$(printf '%s' "$CREATED" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>console.log(JSON.parse(s).secret))')

echo "==> configuring a file sink"
CONFIG=$(curl -fsS "http://127.0.0.1:${PORT}/api/admin/config")
UPDATED=$(printf '%s' "$CONFIG" | SECRET="$SECRET" node -e '
let raw = "";
process.stdin.on("data", (d) => (raw += d)).on("end", () => {
  const { config, etag } = JSON.parse(raw);
  config.sinks = [
    {
      name: "smoke-file",
      enabled: true,
      filter: {},
      maxSpoolBytes: 1048576,
      maxBatchEvents: 1000,
      maxBatchBytes: 1048576,
      config: {
        type: "file",
        directory: "/logs",
        filePrefix: "events",
        retentionDays: 0,
        freeSpaceFloorBytes: 0,
      },
    },
  ];
  process.stdout.write(JSON.stringify({ config, etag }));
});')
curl -fsS -X PUT "http://127.0.0.1:${PORT}/api/admin/config" \
  -H 'content-type: application/json' -d "$UPDATED" >/dev/null

echo "==> posting a signed delivery"
BODY='[{"id":"smoke-1","timestamp":1573817187330,"source":"lambda","projectId":"p1","level":"info","message":"smoke test"}]'
SIG=$(SECRET="$SECRET" BODY="$BODY" node -e '
const { createHmac } = require("node:crypto");
process.stdout.write(createHmac("sha1", process.env.SECRET).update(process.env.BODY).digest("hex"));')

STATUS=$(curl -s -o /tmp/vld-smoke-response -w '%{http_code}' \
  -X POST "http://127.0.0.1:${PORT}/api/drain/${DRAIN_ID}" \
  -H "x-vercel-signature: ${SIG}" -H 'content-type: application/json' -d "$BODY")
[ "$STATUS" = "200" ] || { echo "FAIL: expected 200, got $STATUS"; cat /tmp/vld-smoke-response; docker logs "$NAME"; exit 1; }

echo "==> asserting a rejected signature is refused"
BAD=$(curl -s -o /dev/null -w '%{http_code}' \
  -X POST "http://127.0.0.1:${PORT}/api/drain/${DRAIN_ID}" \
  -H "x-vercel-signature: $(printf 'f%.0s' $(seq 1 40))" -d "$BODY")
[ "$BAD" = "403" ] || { echo "FAIL: expected 403 for a bad signature, got $BAD"; exit 1; }

echo "==> waiting for the file sink to write"
for _ in $(seq 1 30); do
  if docker exec "$NAME" sh -c 'grep -q smoke-1 /logs/events-*.jsonl' 2>/dev/null; then break; fi
  sleep 1
done
docker exec "$NAME" sh -c 'grep -q smoke-1 /logs/events-*.jsonl' || {
  echo "FAIL: file sink never wrote the event"
  docker exec "$NAME" sh -c 'ls -la /logs /spool/smoke-file 2>&1' || true
  docker logs "$NAME"
  exit 1
}

echo "==> asserting readiness and a drained spool"
curl -fsS "http://127.0.0.1:${PORT}/readyz" >/dev/null || {
  echo "FAIL: readyz reported not ready"; docker logs "$NAME"; exit 1; }

echo "==> asserting the process runs as uid 10001"
UID_IN_CONTAINER=$(docker exec "$NAME" id -u)
[ "$UID_IN_CONTAINER" = "10001" ] || { echo "FAIL: expected uid 10001, got $UID_IN_CONTAINER"; exit 1; }

echo "SMOKE PASSED"
