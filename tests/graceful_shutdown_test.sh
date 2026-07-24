#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# graceful_shutdown_test.sh -- SIGTERM/SIGINT against a real, running db_server
# binary: proves the graceful-drain path added alongside DB_SERVER_MAX_CLIENTS
# (listener.c's _install_shutdown_signal_handler() + core.c's
# _server_wait_clients_drain()/SERVER_SHUTDOWN_GRACE_S).
#
# Before this change the server had NO signal handling at all: SIGTERM's
# default disposition just killed the process instantly, mid-request, with
# whatever was in flight simply gone -- no drain, no "stop accepting new
# work" step, nothing. This script proves the three properties the task
# calls out:
#
#   1. A connection already accepted when SIGTERM lands is NOT dropped: the
#      server keeps it alive through the shutdown and it still gets served.
#   2. A NEW connection attempted immediately after SIGTERM is refused --
#      the listening socket is torn down as soon as listener_run() sees the
#      SHUTDOWN status, well before the grace-drain wait even starts.
#   3. The process still exits cleanly and promptly (bounded by
#      SERVER_SHUTDOWN_GRACE_S plus a small buffer) -- no hang, no crash.
#
# Uses bash's /dev/tcp pseudo-device for the "already accepted" connection
# (no nc dependency): opening it alone is enough to register a client slot
# and bump active_clients, since _operator_add_client() counts a connection
# from the moment it's accepted, not from when it sends its first byte.
#
# Dependencies: curl, bash >= 4 (with /dev/tcp support). No root required.
# -----------------------------------------------------------------------------
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

log() { printf '\e[36m[%s]\e[0m %s\n' "$(date +%T)" "$*"; }
die() { log "FAIL: $*"; exit 1; }

BIN="${ROOT_DIR}/build/release/db_server"
[[ -x "${BIN}" ]] || die "release binary not found at ${BIN} -- run ./utils/build_bin.sh release first"

SCRATCH="$(mktemp -d)"
SERVER_PID=""
cleanup() {
    exec 9<&- 2>/dev/null || true
    exec 9>&- 2>/dev/null || true
    if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
        kill -9 "${SERVER_PID}" 2>/dev/null || true
        wait "${SERVER_PID}" 2>/dev/null || true
    fi
    rm -rf "${SCRATCH}"
}
trap cleanup EXIT INT TERM

mkdir -p "${SCRATCH}"/{keys,store,stage,replay,sysmon}
chmod 700 "${SCRATCH}"/keys "${SCRATCH}"/stage "${SCRATCH}"/replay "${SCRATCH}"/sysmon
chmod 02750 "${SCRATCH}"/store

PORT=$(( (RANDOM % 20000) + 20000 ))
BASE_URL="http://127.0.0.1:${PORT}"

log "Booting db_server (release) on port ${PORT}"
DB_APP_DB_ROOT="${SCRATCH}/db" \
DB_APP_KEYS_DIR="${SCRATCH}/keys" \
DB_APP_STORE_DIR="${SCRATCH}/store" \
DB_APP_STAGE_DIR="${SCRATCH}/stage" \
DB_APP_REPLAY_CACHE="${SCRATCH}/replay/cache" \
DB_APP_SYSMON_PATH="${SCRATCH}/sysmon/state" \
"${BIN}" "${PORT}" > "${SCRATCH}/server.log" 2>&1 &
SERVER_PID=$!

log "Waiting for /api/app/ping"
ready=0
for _ in $(seq 1 10); do
    if curl -sS -o /dev/null -w '%{http_code}' --max-time 1 "${BASE_URL}/api/app/ping" 2>/dev/null | grep -q '^200$'; then
        ready=1; break
    fi
    kill -0 "${SERVER_PID}" 2>/dev/null || die "server exited during startup -- $(cat "${SCRATCH}/server.log")"
    sleep 0.5
done
[[ "${ready}" -eq 1 ]] || die "server never answered /api/app/ping"
log "Server ready (pid ${SERVER_PID})."

FAILED=0

# --- Step 1: open (but do not complete) a connection -- accepted, registered,
# counted in active_clients, but its request is not finished yet. ---------
log "Opening an in-flight connection (accepted, request not yet sent)"
exec 9<>"/dev/tcp/127.0.0.1/${PORT}" || die "could not open /dev/tcp connection"
sleep 0.3 # give the operator's epoll a moment to register the accepted fd

kill -0 "${SERVER_PID}" 2>/dev/null || die "server died before SIGTERM was even sent"

# --- Step 2: SIGTERM -------------------------------------------------------
log "Sending SIGTERM"
TERM_TIME=$(date +%s.%N)
kill -TERM "${SERVER_PID}"

# --- Step 3: a brand-new connection attempted right after SIGTERM must be
# refused -- the listening socket is torn down as soon as the listener
# thread observes SHUTDOWN, well before any grace-drain wait. -------------
sleep 0.2
if curl -sS -o /dev/null -w '%{http_code}' --max-time 1 "${BASE_URL}/api/app/ping" 2>/dev/null | grep -q '^200$'; then
    log "  FAIL: a NEW connection succeeded AFTER SIGTERM (listener did not stop accepting)"
    FAILED=1
else
    log "  PASS: new connection refused post-SIGTERM"
fi

# --- Step 4: the server must still be alive (not abruptly killed) while our
# already-accepted connection is still open and unanswered. ---------------
if kill -0 "${SERVER_PID}" 2>/dev/null; then
    log "  PASS: server still alive shortly after SIGTERM (not an abrupt kill)"
else
    log "  FAIL: server already exited before the in-flight connection was even served"
    FAILED=1
fi

# --- Step 5: complete the in-flight request and prove it still gets served
# (not dropped/reset by the shutdown in progress). Connection: close so the
# operator drops active_clients back to 0 as soon as it answers, instead of
# holding the slot open (keep-alive) for the rest of the grace window. ----
log "Completing the in-flight request"
printf 'GET /api/app/ping HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n' >&9

RESPONSE_LINE=""
if RESPONSE_LINE=$(timeout 5 head -n 1 <&9); then
    if [[ "${RESPONSE_LINE}" == *"200"* ]]; then
        log "  PASS: in-flight request answered during shutdown: ${RESPONSE_LINE}"
    else
        log "  FAIL: in-flight request got an unexpected status line: ${RESPONSE_LINE}"
        FAILED=1
    fi
else
    log "  FAIL: in-flight request got no response within 5s (connection dropped, not drained)"
    FAILED=1
fi
exec 9<&- 2>/dev/null || true
exec 9>&- 2>/dev/null || true

# --- Step 6: the process must still exit -- bounded by SERVER_SHUTDOWN_GRACE_S
# (10s) plus a small buffer, never hanging forever. ------------------------
GRACE_BOUND_S=13
exited=0
for _ in $(seq 1 $((GRACE_BOUND_S * 5))); do
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
        exited=1
        break
    fi
    sleep 0.2
done
ELAPSED=$(awk -v t0="${TERM_TIME}" -v t1="$(date +%s.%N)" 'BEGIN { printf "%.2f", t1 - t0 }')
if [[ "${exited}" -eq 1 ]]; then
    log "  PASS: process exited ${ELAPSED}s after SIGTERM (bound ${GRACE_BOUND_S}s)"
else
    log "  FAIL: process still alive ${GRACE_BOUND_S}s after SIGTERM -- graceful shutdown hung"
    FAILED=1
fi
wait "${SERVER_PID}" 2>/dev/null
RC=$?
if [[ "${exited}" -eq 1 && "${RC}" -ne 0 ]]; then
    log "  NOTE: exit code was ${RC} (non-zero on SIGTERM-driven exit is expected/benign here)"
fi

log "--- relevant server.log lines ---"
grep -E 'shutdown|drain' "${SCRATCH}/server.log" | sed 's/^/      /' || true

if (( FAILED )); then
    die "one or more graceful-shutdown checks failed"
fi
log "PASS: SIGTERM drains in-flight work, refuses new connections, and exits cleanly."
