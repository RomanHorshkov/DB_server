#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# socket_activation_restart_test.sh -- proves systemd socket activation's core
# restart guarantee against a real db_server binary: install/systemd/api.socket
# creates and owns the listening AF_UNIX socket independently of
# install/systemd/home_server.service, so a service restart (systemctl
# restart home_server, or a crash-restart via Restart=on-failure) can NEVER
# drop a connection already sitting in the kernel accept backlog -- only the
# accept() of it is delayed until the new service instance comes up. The
# backend itself never binds/chmods/unlinks that socket (sd_activation.c);
# this script reproduces the exact LISTEN_PID/LISTEN_FDS/LISTEN_FDNAMES
# protocol against the real binary rather than asserting it in the abstract.
#
# The fd-passing choreography (fork/dup2 onto fd 3, keep the listening socket
# alive in a separate process across two server generations) needs real POSIX
# fd control bash cannot express directly, so the actual drive is a small
# Python helper (tests/support/sd_activation_restart_probe.py) -- this script
# just builds its environment/args and checks its PASS/FAIL protocol, the
# same shape as every other script in this directory driving the real binary.
#
# Dependencies: python3, bash >= 4. No root required.
# -----------------------------------------------------------------------------
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

log() { printf '\e[36m[%s]\e[0m %s\n' "$(date +%T)" "$*"; }
die() { log "FAIL: $*"; exit 1; }

BIN="${ROOT_DIR}/build/release/db_server"
[[ -x "${BIN}" ]] || die "release binary not found at ${BIN} -- run ./utils/build_bin.sh release first"

PROBE="${SCRIPT_DIR}/support/sd_activation_restart_probe.py"
[[ -f "${PROBE}" ]] || die "probe script not found at ${PROBE}"
command -v python3 >/dev/null 2>&1 || die "python3 not found"

SCRATCH="$(mktemp -d)"
cleanup() { rm -rf "${SCRATCH}"; }
trap cleanup EXIT INT TERM

mkdir -p "${SCRATCH}"/{keys,store,stage,replay,sysmon}
chmod 700 "${SCRATCH}"/keys "${SCRATCH}"/stage "${SCRATCH}"/replay "${SCRATCH}"/sysmon
chmod 02750 "${SCRATCH}"/store

SOCK_PATH="${SCRATCH}/api.sock"

log "Driving socket-activation restart probe (unix socket ${SOCK_PATH})"
OUT="${SCRATCH}/probe.log"
DB_APP_DB_ROOT="${SCRATCH}/db" \
DB_APP_KEYS_DIR="${SCRATCH}/keys" \
DB_APP_STORE_DIR="${SCRATCH}/store" \
DB_APP_STAGE_DIR="${SCRATCH}/stage" \
DB_APP_REPLAY_CACHE="${SCRATCH}/replay/cache" \
DB_APP_SYSMON_PATH="${SCRATCH}/sysmon/state" \
timeout 60 python3 "${PROBE}" "${SOCK_PATH}" "${BIN}" > "${OUT}" 2>"${SCRATCH}/probe.stderr"
RC=$?

log "--- probe stderr (diagnostics) ---"
sed 's/^/      /' "${SCRATCH}/probe.stderr"
log "--- probe stdout ---"
sed 's/^/      /' "${OUT}"

if [[ "${RC}" -ne 0 ]]; then
    die "probe exited ${RC} (timeout or crash -- see diagnostics above)"
fi
if ! grep -q '^RESULT: PASS$' "${OUT}"; then
    die "probe did not report RESULT: PASS"
fi

log "PASS: a service restart adopts the pre-existing listening socket and serves every connection that queued while it was down."
