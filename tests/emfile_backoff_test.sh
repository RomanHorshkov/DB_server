#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# emfile_backoff_test.sh -- genuine process fd-table exhaustion (EMFILE) against
# a real, running db_server binary, driven with an artificially low RLIMIT_NOFILE
# on the server process itself (not the test driver).
#
# Before this change, listener.c's accept handlers treated an accept()/accept4()
# failure uniformly: log + return STATUS_FAILURE. Because the listening socket
# stays EPOLLIN-ready (level-triggered) for as long as a connection sits
# unaccepted in the kernel backlog, a SUSTAINED EMFILE (the fd table stays full
# until something else frees an fd) meant listener_run()'s reactor loop would
# call epoll_wait() -> accept() -> EMFILE -> log -> repeat, as fast as the CPU
# could spin -- a busy-loop pegging a core at 100% for as long as the
# exhaustion lasted, on top of a log line per spin. listener.c's
# _backoff_on_fd_exhaustion() now sleeps briefly and rate-limits the log
# instead.
#
# This script proves, against the real binary:
#   1. Genuine EMFILE is detected and logged (not silently swallowed).
#   2. The server process does NOT crash under sustained exhaustion.
#   3. The listener thread does NOT busy-spin a CPU core while exhausted
#      (sampled process CPU time over a fixed wall-clock window).
#   4. The server RECOVERS once fds free up again -- not a permanent wedge or
#      crash-loop: a fresh connection after closing some of the exhausting
#      ones is accepted and answered normally.
#
# The low ulimit is applied ONLY to the server subprocess (a subshell that
# sets it then execs the binary), never to the test driver itself, which
# needs its own generous fd budget to hold every opened connection.
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
declare -a CONN_FDS=()
cleanup() {
    for fd in "${CONN_FDS[@]}"; do
        eval "exec ${fd}<&- 2>/dev/null" || true
        eval "exec ${fd}>&- 2>/dev/null" || true
    done
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

# A single operator (DB_SERVER_WORKERS=1) keeps the idle baseline fd count small
# and deterministic (~20 fds: log/epoll/timer/wakeup/LMDB/replay fds) so a modest
# RLIMIT_NOFILE=40 leaves just enough headroom to reach genuine exhaustion with a
# small, fast burst of connections rather than needing hundreds.
log "Booting db_server (release) on port ${PORT} with RLIMIT_NOFILE=40"
( ulimit -n 40
  exec env \
    DB_SERVER_WORKERS=1 \
    DB_APP_DB_ROOT="${SCRATCH}/db" \
    DB_APP_KEYS_DIR="${SCRATCH}/keys" \
    DB_APP_STORE_DIR="${SCRATCH}/store" \
    DB_APP_STAGE_DIR="${SCRATCH}/stage" \
    DB_APP_REPLAY_CACHE="${SCRATCH}/replay/cache" \
    DB_APP_SYSMON_PATH="${SCRATCH}/sysmon/state" \
    "${BIN}" "${PORT}" ) > "${SCRATCH}/server.log" 2>&1 &
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

# --- Step 1: open a burst of raw connections (accepted-but-idle, exactly like
# graceful_shutdown_test.sh's technique) -- each one consumes one fd on the
# SERVER side. 60 is comfortably past the ~19-fd headroom RLIMIT_NOFILE=40
# leaves above the ~20-fd idle baseline. -----------------------------------
log "Opening 60 connections against a 40-fd server ulimit"
for i in $(seq 1 60); do
    newfd=$(( 100 + i ))
    if exec {newfd}<>"/dev/tcp/127.0.0.1/${PORT}" 2>/dev/null; then
        CONN_FDS+=("${newfd}")
    fi
    # Not every attempt needs to succeed at the bash/kernel level for this test;
    # the backlog can absorb more than the server can accept() -- what matters is
    # genuine fd exhaustion happens SOMEWHERE in this burst.
done
log "Opened ${#CONN_FDS[@]} connections."
sleep 1

# --- Step 2: the server must still be alive. -------------------------------
if kill -0 "${SERVER_PID}" 2>/dev/null; then
    log "  PASS: server survived the connection burst (no crash)"
else
    log "  FAIL: server died under fd exhaustion -- $(tail -20 "${SCRATCH}/server.log")"
    FAILED=1
fi

# --- Step 3: EMFILE/ENFILE was actually detected and logged. --------------
if grep -q 'fd table exhausted' "${SCRATCH}/server.log"; then
    log "  PASS: EMFILE/ENFILE detected and logged"
else
    log "  FAIL: no fd-exhaustion log line found -- burst may not have actually exhausted the table"
    FAILED=1
fi

# --- Step 4: no busy-loop. Sample this process's own CPU time (utime+stime,
# clock ticks) over a real 1s wall-clock window while still exhausted; a
# regression back to an unconditional epoll_wait->accept->fail spin would
# consume close to 100% of a core (~all of CLK_TCK ticks/second). A generous
# 50%-of-one-core bound catches a real busy-loop without being flaky. -------
if kill -0 "${SERVER_PID}" 2>/dev/null; then
    CLK_TCK=$(getconf CLK_TCK)
    read_ticks() { awk '{print $14+$15}' "/proc/${SERVER_PID}/stat" 2>/dev/null; }
    T0=$(read_ticks)
    sleep 1
    T1=$(read_ticks)
    if [[ -n "${T0}" && -n "${T1}" ]]; then
        DELTA_MS=$(( (T1 - T0) * 1000 / CLK_TCK ))
        log "  process CPU consumed over 1s window: ${DELTA_MS}ms"
        if (( DELTA_MS < 500 )); then
            log "  PASS: no busy-loop (well under one core saturated)"
        else
            log "  FAIL: process consumed ${DELTA_MS}ms of CPU in a 1000ms window -- looks like a busy-loop"
            FAILED=1
        fi
    else
        log "  SKIP: could not read /proc/${SERVER_PID}/stat (process exited mid-sample?)"
    fi
else
    log "  SKIP: server not alive, cannot sample CPU"
fi

# --- Step 5: recovery -- free up fds, then prove a fresh connection is
# accepted and answered normally (not a permanent wedge). Close EVERY opened
# connection: with the server's own accept() failing under exhaustion, an
# unknown number of these 60 attempts never actually consumed a SERVER-side
# fd in the first place (they are sitting in the kernel backlog, not
# accepted) -- there is no way to tell which ones from the client side, so
# only closing all of them reliably frees every fd the server DID accept. ---
log "Closing every opened connection to free server-side fds"
for fd in "${CONN_FDS[@]}"; do
    eval "exec ${fd}<&- 2>/dev/null" || true
    eval "exec ${fd}>&- 2>/dev/null" || true
done
CONN_FDS=()

recovered=0
for _ in $(seq 1 10); do
    if curl -sS -o /dev/null -w '%{http_code}' --max-time 1 "${BASE_URL}/api/app/ping" 2>/dev/null | grep -q '^200$'; then
        recovered=1; break
    fi
    sleep 0.5
done
if [[ "${recovered}" -eq 1 ]]; then
    log "  PASS: server recovered -- accepts and answers new connections again after fds freed"
else
    log "  FAIL: server never recovered after fds were freed (permanent wedge?)"
    FAILED=1
fi

# --- cleanup: close remaining connections, then a clean SIGTERM shutdown. --
for fd in "${CONN_FDS[@]}"; do
    eval "exec ${fd}<&- 2>/dev/null" || true
    eval "exec ${fd}>&- 2>/dev/null" || true
done
CONN_FDS=()
kill -TERM "${SERVER_PID}" 2>/dev/null || true
for _ in $(seq 1 30); do
    kill -0 "${SERVER_PID}" 2>/dev/null || break
    sleep 0.2
done
if kill -0 "${SERVER_PID}" 2>/dev/null; then
    log "  FAIL: server did not exit cleanly after final SIGTERM"
    FAILED=1
else
    log "  PASS: clean shutdown after the test"
fi

if (( FAILED )); then
    die "one or more EMFILE-handling checks failed"
fi
log "PASS: genuine fd exhaustion is detected, survived without a busy-loop, and recovers cleanly."
