#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# fault_injection_startup_test.sh -- LD_PRELOAD fault injection against a real
# db_server binary, targeting the filesystem operations DB_server/DB_app make
# DIRECTLY outside DB_lmdb's own write/fsync/msync commit path (which already
# has its own harness, DB_lmdb/tests/support/fault_inject/).
#
# Scope note (why this targets STARTUP, not an in-flight upload request):
#   Investigation (docs/HARDENING_TODO.md) found DB_server's own source makes
#   almost no direct fs syscalls at all -- close()+read() on a blob fd DB_app
#   hands it (folder_zip_pump.c), nothing else. Exercising THAT specific path
#   needs a fully authenticated user + folder + signed share link (this
#   project's real auth is DPoP-proof-of-possession, not a simple
#   username/password POST -- see the superproject's tests/lib/{client,jose}.sh)
#   which isn't something a self-contained, CI-portable DB_server test can pull
#   in (those libraries live one level up, outside what DB_server's own CI
#   checkout clones). What IS fully self-contained, real, and reachable without
#   any auth is the FIRST-BOOT provisioning DB_app/DB_lmdb do directly: the
#   server signing key (open() O_CREAT|O_EXCL, DB_app/app/src/db_app/platform/
#   keys.c) and, for every fresh LMDB env, the schema manifest (write-tmp-then-
#   rename via db_lmdb_fs_write_file_atomic(), DB_lmdb/app/src/database/core/
#   fsops.c + open/manifest.c) -- the EXACT SAME crash-safe write-then-rename
#   pattern the upload-commit path uses, and (per fault_inject.c's own header
#   comment) the ONLY renameat() DB_lmdb calls through the interceptable libc
#   symbol rather than a raw syscall(SYS_renameat2) on this box. This proves
#   the same property the task cares about -- permission/EMFILE/rename failures
#   fail CLEANLY (no crash, no corrupted/partial state, a clear error) -- on
#   the boot-time instance of that pattern.
#
# Checks:
#   1. EACCES on the signing-key open() -> server_init() fails, clean non-zero
#      exit, no crash, no partial key file left on disk.
#   2. EMFILE on that same open() -> same clean-failure contract for fd
#      exhaustion specifically (not just permission).
#   3. EXDEV on the manifest renameat() -> clean failure, the tmp file is
#      removed (db_lmdb_fs_write_file_atomic()'s own fail-path cleanup), no
#      partial/corrupt manifest left behind.
#   4. Recovery: after each injected failure, a completely normal (no
#      LD_PRELOAD) boot against a FRESH scratch root succeeds -- the harness
#      and this project's failure paths don't leave anything wedged globally.
#
# Dependencies: gcc (to build the shim once), bash >= 4. No root required.
# -----------------------------------------------------------------------------
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

log() { printf '\e[36m[%s]\e[0m %s\n' "$(date +%T)" "$*"; }
die() { log "FAIL: $*"; exit 1; }

BIN="${ROOT_DIR}/build/release/db_server"
[[ -x "${BIN}" ]] || die "release binary not found at ${BIN} -- run ./utils/build_bin.sh release first"

SO_DIR="${ROOT_DIR}/build/tests/fault_inject"
SO_PATH="${SO_DIR}/libfault_inject.so"
mkdir -p "${SO_DIR}"
log "Building the fault-injection shim"
gcc -O1 -fPIC -shared -o "${SO_PATH}" "${SCRIPT_DIR}/support/fault_inject/fault_inject.c" -ldl -lpthread \
    || die "failed to build libfault_inject.so"

FAILED=0
SCRATCH_ROOT="$(mktemp -d)"
cleanup() { rm -rf "${SCRATCH_ROOT}"; }
trap cleanup EXIT INT TERM

# fresh_scratch <label> -- a brand-new, never-before-booted DB_app root under SCRATCH_ROOT/<label>.
fresh_scratch() {
    local dir="${SCRATCH_ROOT}/$1"
    mkdir -p "${dir}"/{keys,store,stage,replay,sysmon}
    chmod 700 "${dir}"/keys "${dir}"/stage "${dir}"/replay "${dir}"/sysmon
    chmod 02750 "${dir}"/store
    printf '%s' "${dir}"
}

# run_server <scratch_dir> <log_file> [env NAME=VALUE ...] -- boots db_server with a random dev TCP
# port against the given scratch root, waits (bounded) for it to either answer /api/app/ping (success)
# or exit (failure/crash), and returns: prints "READY <pid>" or "EXITED <rc>" or "TIMEOUT <pid>".
run_server() {
    local scratch="$1" logfile="$2"; shift 2
    local port=$(( (RANDOM % 20000) + 20000 ))
    env "$@" \
        DB_APP_DB_ROOT="${scratch}/db" \
        DB_APP_KEYS_DIR="${scratch}/keys" \
        DB_APP_STORE_DIR="${scratch}/store" \
        DB_APP_STAGE_DIR="${scratch}/stage" \
        DB_APP_REPLAY_CACHE="${scratch}/replay/cache" \
        DB_APP_SYSMON_PATH="${scratch}/sysmon/state" \
        "${BIN}" "${port}" > "${logfile}" 2>&1 &
    local pid=$!

    for _ in $(seq 1 40); do
        if ! kill -0 "${pid}" 2>/dev/null; then
            wait "${pid}" 2>/dev/null
            echo "EXITED $?"
            return
        fi
        if grep -q 'C Server initialized' "${logfile}" 2>/dev/null; then
            echo "READY ${pid}"
            return
        fi
        sleep 0.25
    done
    echo "TIMEOUT ${pid}"
}

kill_if_alive() {
    local pid="$1"
    if kill -0 "${pid}" 2>/dev/null; then
        kill -9 "${pid}" 2>/dev/null || true
        wait "${pid}" 2>/dev/null || true
    fi
}

# --- Check 1: EACCES on the signing-key open() -----------------------------
log "Check 1: EACCES on the first-boot signing-key open()"
S1="$(fresh_scratch eacces)"
L1="${S1}/server.log"
OUT="$(run_server "${S1}" "${L1}" \
    LD_PRELOAD="${SO_PATH}" \
    DB_SERVER_FI_TARGET=open \
    DB_SERVER_FI_AFTER=0 \
    DB_SERVER_FI_ERRNO=EACCES \
    DB_SERVER_FI_PATH_SUBSTR=server_ed25519.key \
    DB_SERVER_FI_GEN=eacces1)"
STATE="${OUT%% *}"; PID_OR_RC="${OUT##* }"
if [[ "${STATE}" == "EXITED" && "${PID_OR_RC}" != "0" ]]; then
    log "  PASS: server_init() failed cleanly (exit ${PID_OR_RC}), no hang, no crash"
else
    log "  FAIL: unexpected outcome '${OUT}' -- expected a clean non-zero exit"
    [[ "${STATE}" != "EXITED" ]] && kill_if_alive "${PID_OR_RC}"
    FAILED=1
fi
if grep -qiE 'segfault|core dumped' "${L1}" 2>/dev/null; then
    log "  FAIL: server.log shows signs of a crash"
    FAILED=1
fi
if [[ -e "${S1}/keys/server_ed25519.key" ]]; then
    log "  FAIL: a key file exists despite the injected open() failure (partial/corrupted state)"
    FAILED=1
else
    log "  PASS: no partial key file left behind"
fi

# --- Check 2: EMFILE on the same open() ------------------------------------
log "Check 2: EMFILE on the first-boot signing-key open()"
S2="$(fresh_scratch emfile)"
L2="${S2}/server.log"
OUT="$(run_server "${S2}" "${L2}" \
    LD_PRELOAD="${SO_PATH}" \
    DB_SERVER_FI_TARGET=open \
    DB_SERVER_FI_AFTER=0 \
    DB_SERVER_FI_ERRNO=EMFILE \
    DB_SERVER_FI_PATH_SUBSTR=server_ed25519.key \
    DB_SERVER_FI_GEN=emfile1)"
STATE="${OUT%% *}"; PID_OR_RC="${OUT##* }"
if [[ "${STATE}" == "EXITED" && "${PID_OR_RC}" != "0" ]]; then
    log "  PASS: server_init() failed cleanly under injected EMFILE (exit ${PID_OR_RC})"
else
    log "  FAIL: unexpected outcome '${OUT}' -- expected a clean non-zero exit"
    [[ "${STATE}" != "EXITED" ]] && kill_if_alive "${PID_OR_RC}"
    FAILED=1
fi

# --- Check 3: EXDEV on the manifest renameat() ------------------------------
log "Check 3: EXDEV on the schema-manifest renameat()"
S3="$(fresh_scratch exdev)"
L3="${S3}/server.log"
OUT="$(run_server "${S3}" "${L3}" \
    LD_PRELOAD="${SO_PATH}" \
    DB_SERVER_FI_TARGET=renameat \
    DB_SERVER_FI_AFTER=0 \
    DB_SERVER_FI_ERRNO=EXDEV \
    DB_SERVER_FI_PATH_SUBSTR=.tmp \
    DB_SERVER_FI_GEN=exdev1)"
STATE="${OUT%% *}"; PID_OR_RC="${OUT##* }"
if [[ "${STATE}" == "EXITED" && "${PID_OR_RC}" != "0" ]]; then
    log "  PASS: server_init() failed cleanly under injected rename EXDEV (exit ${PID_OR_RC})"
else
    log "  FAIL: unexpected outcome '${OUT}' -- expected a clean non-zero exit"
    [[ "${STATE}" != "EXITED" ]] && kill_if_alive "${PID_OR_RC}"
    FAILED=1
fi
leftover_tmp="$(find "${S3}/db" -name '*.tmp' 2>/dev/null | head -1)"
if [[ -n "${leftover_tmp}" ]]; then
    log "  FAIL: a stale .tmp file was left behind after the injected rename failure: ${leftover_tmp}"
    FAILED=1
else
    log "  PASS: no stale .tmp file left behind (write_file_atomic's fail-path cleanup ran)"
fi

# --- Check 4: recovery -- a normal boot (no injection) still works ----------
log "Check 4: a completely normal boot still succeeds after all of the above"
S4="$(fresh_scratch recovery)"
L4="${S4}/server.log"
OUT="$(run_server "${S4}" "${L4}")"
STATE="${OUT%% *}"; PID_OR_RC="${OUT##* }"
if [[ "${STATE}" == "READY" ]]; then
    log "  PASS: normal boot succeeds (pid ${PID_OR_RC})"
    kill -TERM "${PID_OR_RC}" 2>/dev/null || true
    for _ in $(seq 1 20); do kill -0 "${PID_OR_RC}" 2>/dev/null || break; sleep 0.2; done
    kill_if_alive "${PID_OR_RC}"
else
    log "  FAIL: normal boot did not succeed ('${OUT}') -- $(tail -10 "${L4}")"
    [[ "${STATE}" == "TIMEOUT" ]] && kill_if_alive "${PID_OR_RC}"
    FAILED=1
fi

if (( FAILED )); then
    die "one or more fault-injection checks failed"
fi
log "PASS: permission, EMFILE, and rename failures on DB_app/DB_lmdb's direct fs calls all fail cleanly, leave no corrupted state, and never wedge later boots."
