#!/usr/bin/env bash
# End-to-end verification for the multi-target aggregator.
#
# It starts 99 healthy mock node_exporter endpoints plus one endpoint that
# never answers, runs the real node_exporter_multitarget binary against all
# 100 targets, and records:
#   * the aggregated /metrics output,
#   * evidence that all 99 healthy targets still expose node_* metrics,
#   * the failure metric for the single hung target,
#   * the round-level failure ratio (1/100).
#
# All evidence is written under artifacts/e2e/ so the run can be reviewed
# after the fact. The script exits non-zero if any assertion fails.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/artifacts/e2e"
BIN="${ROOT_DIR}/artifacts/node_exporter_multitarget"
MOCK_BIN="${ROOT_DIR}/artifacts/mocktarget"
TARGETS_FILE="${OUT_DIR}/targets.txt"
MOCK_PIDS=()
AGG_PID=""
PORT_BASE=20100

log() { printf '[verify] %s\n' "$*"; }

cleanup() {
  if [[ -n "${AGG_PID}" ]]; then
    kill "${AGG_PID}" 2>/dev/null || true
  fi
  for pid in "${MOCK_PIDS[@]:-}"; do
    kill "${pid}" 2>/dev/null || true
  done
}
trap cleanup EXIT

rm -rf "${OUT_DIR}"
mkdir -p "${OUT_DIR}"

log "building aggregator binary"
(cd "${ROOT_DIR}" && go build -o "${BIN}" ./cmd/multitarget && go build -o "${MOCK_BIN}" ./multitarget/cmd/mocktarget)

log "starting 100 mock targets (99 healthy, 1 blackhole)"
: > "${TARGETS_FILE}"
for i in $(seq 0 99); do
  port=$((PORT_BASE + i))
  name="host-$(printf '%03d' "${i}")"
  if [[ "${i}" -eq 37 ]]; then
    "${MOCK_BIN}" --listen="127.0.0.1:${port}" --name="${name}" --blackhole \
      >"${OUT_DIR}/mock-${name}.log" 2>&1 &
  else
    "${MOCK_BIN}" --listen="127.0.0.1:${port}" --name="${name}" \
      >"${OUT_DIR}/mock-${name}.log" 2>&1 &
  fi
  MOCK_PIDS+=("$!")
  printf '%s http://127.0.0.1:%s/metrics\n' "${name}" "${port}" >> "${TARGETS_FILE}"
done

log "waiting for mock targets to come up"
for i in $(seq 0 99); do
  if [[ "${i}" -eq 37 ]]; then
    continue
  fi
  port=$((PORT_BASE + i))
  for _ in $(seq 1 40); do
    if curl -fsS "http://127.0.0.1:${port}/metrics" >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
  done
done

log "starting aggregator on :19101 with a 2s round budget and 300ms target timeout"
"${BIN}" \
  --targets.file="${TARGETS_FILE}" \
  --scrape.target-timeout=300ms \
  --scrape.round-timeout=2s \
  --scrape.concurrency=25 \
  --web.listen-address=:19101 \
  --log.level=warn \
  >"${OUT_DIR}/aggregator.log" 2>&1 &
AGG_PID=$!

log "waiting for aggregator to be ready"
for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:19101/-/healthy" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

log "scraping aggregated /metrics"
curl -fsS "http://127.0.0.1:19101/metrics" -o "${OUT_DIR}/metrics.txt"

fail=0

healthy_up="$(grep -c '^node_up{.*target="host-' "${OUT_DIR}/metrics.txt" || true)"
if [[ "${healthy_up}" -ne 99 ]]; then
  log "FAIL: expected node_up from 99 healthy targets, got ${healthy_up}"
  fail=1
else
  log "OK: 99 healthy targets produced node_up metrics"
fi

if grep '^node_up' "${OUT_DIR}/metrics.txt" | grep -q 'target="host-037"'; then
  log "FAIL: hung target must not contribute node_up metrics"
  fail=1
else
  log "OK: hung target host-037 contributed no node_up metrics"
fi

hung_failure="$(grep 'node_multitarget_target_scrape_failures_total{reason="timeout",target="host-037"}' "${OUT_DIR}/metrics.txt" | awk '{print $2}')"
if [[ "${hung_failure}" != "1" ]]; then
  log "FAIL: expected host-037 timeout failure counter 1, got '${hung_failure}'"
  fail=1
else
  log "OK: host-037 recorded with reason=timeout, counter=${hung_failure}"
fi

hung_up="$(grep 'node_multitarget_target_up{target="host-037"}' "${OUT_DIR}/metrics.txt" | awk '{print $2}')"
if [[ "${hung_up}" != "0" ]]; then
  log "FAIL: expected host-037 up=0, got '${hung_up}'"
  fail=1
else
  log "OK: host-037 reports up=0"
fi

ratio="$(grep '^node_multitarget_target_failure_ratio ' "${OUT_DIR}/metrics.txt" | awk '{print $2}')"
if [[ "${ratio}" != "0.01" ]]; then
  log "FAIL: expected failure ratio 0.01, got '${ratio}'"
  fail=1
else
  log "OK: failure ratio is 0.01 (1/100)"
fi

round_duration="$(grep '^node_multitarget_scrape_round_duration_seconds ' "${OUT_DIR}/metrics.txt" | awk '{print $2}')"
log "round duration seconds: ${round_duration}"

{
  echo "healthy node_up series: ${healthy_up}"
  echo "host-037 failure counter (timeout): ${hung_failure}"
  echo "host-037 up: ${hung_up}"
  echo "failure ratio: ${ratio}"
  echo "round duration seconds: ${round_duration}"
} > "${OUT_DIR}/summary.txt"

if [[ "${fail}" -ne 0 ]]; then
  log "verification FAILED, see ${OUT_DIR}"
  exit 1
fi
log "verification PASSED, evidence written to ${OUT_DIR}"
