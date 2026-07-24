#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# config_validation_test.sh -- startup config validation (config_validate.c),
# driven against the real db_server binary, not a unit-test double.
#
# Before this, core.c's server_init() had ZERO validation of its startup
# config surface: a garbage port, an out-of-range DB_SERVER_WORKERS, or a
# non-power-of-two DB_SERVER_RING_CAPACITY would either crash deep inside
# getaddrinfo()/bind() with an unhelpful error, or -- worse, for the two env
# overrides -- silently fall back to an auto-computed default with only a WARN
# log, never actually stopping the server from starting with a config nobody
# asked for. config_validate_startup() now runs FIRST, at the very top of
# server_init(), and fails fast: non-zero exit, one clear ERROR line, no
# socket/thread/DB_app touched at all.
#
# tests/core/config_validate_tests.c already unit-tests every rejection path
# against config_validate_startup() directly (in-process, no exec). This
# script is the other half the project's testing standard requires: proof
# that the real compiled binary actually calls that gate before anything else
# -- and that a VALID config is not accidentally rejected along the way.
#
# Dependencies: bash >= 4. No root required, no network.
# -----------------------------------------------------------------------------
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

log() { printf '\e[36m[%s]\e[0m %s\n' "$(date +%T)" "$*"; }
die() { log "FAIL: $*"; exit 1; }

BIN="${ROOT_DIR}/build/release/db_server"
[[ -x "${BIN}" ]] || die "release binary not found at ${BIN} -- run ./utils/build_bin.sh release first"

SCRATCH="$(mktemp -d)"
trap 'rm -rf "${SCRATCH}"' EXIT

FAILED=0

# assert_rejected <label> <expected_stderr_substring> <env assignments...> -- <argv...>
# Runs the binary with a short timeout (a correctly-rejected config exits almost instantly;
# a regression that lets it past validation would otherwise hang the test waiting on epoll).
assert_rejected() {
    local label="$1" expect="$2"; shift 2
    local out rc
    out="$(timeout 5s "$@" 2>&1)"; rc=$?
    if [[ "${rc}" -eq 0 ]]; then
        log "  FAIL [${label}]: expected non-zero exit, got 0"
        FAILED=1
        return
    fi
    if [[ "${rc}" -eq 124 ]]; then
        log "  FAIL [${label}]: binary did not exit within the timeout (validation did not reject it?)"
        FAILED=1
        return
    fi
    if ! grep -qF -- "${expect}" <<<"${out}"; then
        log "  FAIL [${label}]: exit=${rc} but stderr did not contain '${expect}':"
        printf '%s\n' "${out}" | sed 's/^/      /'
        FAILED=1
        return
    fi
    log "  PASS [${label}] (exit=${rc})"
}

log "Bad TCP port (out of range)"
assert_rejected "port_out_of_range" "is not a valid TCP port" \
    "${BIN}" 99999

log "TCP port 0"
assert_rejected "port_zero" "is not a valid TCP port" \
    "${BIN}" 0

log "Non-numeric port"
assert_rejected "port_non_numeric" "is not a valid TCP port" \
    "${BIN}" not-a-port

log "Unix spec with a missing parent directory"
assert_rejected "unix_missing_parent" "does not exist" \
    "${BIN}" "/this/almost-certainly/does-not/exist/api.sock"

log "DB_SERVER_WORKERS=0"
DB_SERVER_WORKERS=0 \
    assert_rejected "workers_zero" "DB_SERVER_WORKERS" \
    "${BIN}" 34901

log "DB_SERVER_RING_CAPACITY=7 (not a power of two, below the floor)"
DB_SERVER_RING_CAPACITY=7 \
    assert_rejected "ring_capacity_bad" "DB_SERVER_RING_CAPACITY" \
    "${BIN}" 34902

# --- control: a genuinely valid config must NOT be rejected -------------------
log "Control: valid config actually boots (regression guard against over-rejection)"
mkdir -p "${SCRATCH}"/{keys,store,stage,replay,sysmon}
chmod 700 "${SCRATCH}"/keys "${SCRATCH}"/stage "${SCRATCH}"/replay "${SCRATCH}"/sysmon
chmod 02750 "${SCRATCH}"/store

PORT=$(( (RANDOM % 20000) + 40000 ))
DB_APP_DB_ROOT="${SCRATCH}/db" \
DB_APP_KEYS_DIR="${SCRATCH}/keys" \
DB_APP_STORE_DIR="${SCRATCH}/store" \
DB_APP_STAGE_DIR="${SCRATCH}/stage" \
DB_APP_REPLAY_CACHE="${SCRATCH}/replay/cache" \
DB_APP_SYSMON_PATH="${SCRATCH}/sysmon/state" \
DB_SERVER_WORKERS=2 \
DB_SERVER_RING_CAPACITY=16 \
"${BIN}" "${PORT}" > "${SCRATCH}/server.log" 2>&1 &
SERVER_PID=$!

ready=0
for _ in $(seq 1 10); do
    kill -0 "${SERVER_PID}" 2>/dev/null || break
    if grep -q 'C Server initialized' "${SCRATCH}/server.log" 2>/dev/null; then
        ready=1
        break
    fi
    sleep 0.5
done

if [[ "${ready}" -eq 1 ]]; then
    log "  PASS [valid_config_boots]"
else
    log "  FAIL [valid_config_boots]: server did not report a clean init:"
    sed 's/^/      /' "${SCRATCH}/server.log" 2>/dev/null
    FAILED=1
fi

kill "${SERVER_PID}" 2>/dev/null || true
wait "${SERVER_PID}" 2>/dev/null || true

if (( FAILED )); then
    die "one or more config-validation checks failed"
fi
log "PASS: startup config validation rejects every bad config and accepts a valid one."
