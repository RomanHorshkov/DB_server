#!/usr/bin/env python3
"""
sd_activation_restart_probe.py -- drives the exact systemd socket-activation
restart guarantee against a real db_server binary: the LISTENING socket is
owned by the "activator" (systemd in production; this script's own process
here), never by the backend service process, so a service restart can never
drop a connection that is already queued in the kernel accept backlog --
only accept()ing it is delayed until the new service instance comes up.

This is deliberately written in Python, not bash: reproducing the real
protocol needs raw fork()/dup2()/exec() control over file descriptor 3
(SD_LISTEN_FDS_START) and a listening socket that outlives TWO separate
child process generations -- bash cannot express "keep this fd open in me,
hand a non-cloexec duplicate of it to each child I exec" as directly.

Protocol reproduced (matches app/src/core/listener/sd_activation.c exactly):
  - bind+listen an AF_UNIX SOCK_STREAM socket at the given path (systemd's
    job in production -- install/systemd/api.socket's ListenStream=).
  - for each server generation: fork, dup2 the listening socket onto fd 3,
    set LISTEN_PID=<child pid> LISTEN_FDS=1 LISTEN_FDNAMES=api, exec the
    server binary with NO argv (argc==1, exactly main.c's documented
    activated-run usage).

Exit code 0 + a final line "RESULT: PASS" means every check passed; any
other outcome (non-zero exit, "RESULT: FAIL", or no RESULT line at all --
e.g. a hang the caller had to timeout) is a failure. All diagnostic detail
goes to stderr; stdout is reserved for the PASS/FAIL protocol the bash
wrapper checks.
"""
import contextlib
import os
import socket
import sys
import time

SD_LISTEN_FDS_START = 3


def log(msg):
    print(f"[probe] {msg}", file=sys.stderr, flush=True)


def spawn_generation(lsock, server_bin):
    """Fork + exec a fresh server process inheriting lsock as the activated 'api' socket."""
    pid = os.fork()
    if pid == 0:
        try:
            os.dup2(lsock.fileno(), SD_LISTEN_FDS_START)
            # dup2(fd, fd) (the listening socket already happens to sit at fd 3, e.g. right after
            # fork when nothing lower-numbered was opened) is a POSIX no-op that does NOT clear
            # FD_CLOEXEC on that fd -- and Python sockets are FD_CLOEXEC by default (PEP 446). Left
            # alone, fd 3 would vanish at exec() and the server would see "not a listening socket".
            # Clearing it explicitly (not relying on dup2's usual duplicate-and-clear behavior)
            # covers both the real-duplicate and the same-fd cases uniformly.
            os.set_inheritable(SD_LISTEN_FDS_START, True)
            env = os.environ.copy()
            env["LISTEN_PID"] = str(os.getpid())
            env["LISTEN_FDS"] = "1"
            env["LISTEN_FDNAMES"] = "api"
            os.execve(server_bin, [server_bin], env)
        except Exception as exc:  # noqa: BLE001 -- child must never fall back into the parent's code
            os.write(2, f"[probe child] exec failed: {exc}\n".encode())
        os._exit(127)
    return pid


def wait_ready(sock_path, timeout_s):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                s.settimeout(1.0)
                s.connect(sock_path)
                s.sendall(b"GET /api/app/ping HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
                resp = s.recv(64)
                if b"200" in resp:
                    return True
        except OSError:
            pass
        time.sleep(0.2)
    return False


def proc_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def wait_exit(pid, timeout_s):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            done_pid, _status = os.waitpid(pid, os.WNOHANG)
            if done_pid == pid:
                return True
        except ChildProcessError:
            return True
        time.sleep(0.1)
    return False


def main():
    if len(sys.argv) != 3:
        log("usage: sd_activation_restart_probe.py <unix_socket_path> <server_bin>")
        return 2
    sock_path, server_bin = sys.argv[1], sys.argv[2]

    failed = False
    with contextlib.suppress(FileNotFoundError):
        os.unlink(sock_path)

    lsock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    lsock.bind(sock_path)
    os.chmod(sock_path, 0o660)
    lsock.listen(1024)
    log(f"bound+listening at {sock_path} (systemd's role) -- this socket outlives both server generations")

    # --- Generation 1: normal activated boot -----------------------------
    gen1_pid = spawn_generation(lsock, server_bin)
    log(f"generation 1 pid={gen1_pid}")
    if not wait_ready(sock_path, 10):
        log("FAIL: generation 1 never answered /api/app/ping")
        return _finish(True, lsock, sock_path, [gen1_pid])
    log("generation 1 ready and serving")

    # --- Kill generation 1 (SIGTERM -- the graceful path) -----------------
    os.kill(gen1_pid, 15)
    if not wait_exit(gen1_pid, 10):
        log("FAIL: generation 1 did not exit after SIGTERM")
        failed = True
    else:
        log("generation 1 exited")

    # --- While NO server process is running, queue connections against the
    # activator-owned socket -- exactly what happens during a real systemd
    # restart window (socket stays up, service is momentarily down). -------
    queued = []
    n_queued = 5
    for i in range(n_queued):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(10.0)
        s.connect(sock_path)  # succeeds at the kernel level -- no accept()or is running
        s.sendall(f"GET /api/app/ping HTTP/1.1\r\nHost: x{i}\r\nConnection: close\r\n\r\n".encode())
        queued.append(s)
    log(f"queued {len(queued)} connections while the service is down (kernel backlog only)")

    # --- Generation 2: restart, reusing the SAME listening fd -------------
    gen2_pid = spawn_generation(lsock, server_bin)
    log(f"generation 2 pid={gen2_pid}")
    if not wait_ready(sock_path, 10):
        log("FAIL: generation 2 never came up / never answered a NEW connection")
        return _finish(True, lsock, sock_path, [gen2_pid], queued)
    log("generation 2 ready")

    # --- Every connection queued BEFORE generation 2 existed must still get
    # served -- the real property under test. ------------------------------
    served = 0
    for i, s in enumerate(queued):
        try:
            resp = s.recv(256)
        except OSError as exc:
            log(f"FAIL: queued connection {i} errored waiting for a response: {exc}")
            failed = True
            continue
        if b"200" in resp.split(b"\r\n", 1)[0]:
            served += 1
        else:
            log(f"FAIL: queued connection {i} got an unexpected response: {resp[:80]!r}")
            failed = True
        s.close()
    log(f"{served}/{n_queued} pre-restart-queued connections were served by generation 2")
    if served != n_queued:
        failed = True

    # --- generation 2 must still be healthy for brand-new work too --------
    if not wait_ready(sock_path, 5):
        log("FAIL: generation 2 stopped answering new connections after serving the queued ones")
        failed = True

    return _finish(failed, lsock, sock_path, [gen2_pid])


def _finish(failed, lsock, sock_path, pids_to_kill, extra_sockets=None):
    for s in extra_sockets or []:
        with contextlib.suppress(Exception):
            s.close()
    for pid in pids_to_kill:
        if proc_alive(pid):
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, 15)
            wait_exit(pid, 5)
            if proc_alive(pid):
                with contextlib.suppress(ProcessLookupError):
                    os.kill(pid, 9)
                wait_exit(pid, 5)
    with contextlib.suppress(Exception):
        lsock.close()
    with contextlib.suppress(FileNotFoundError):
        os.unlink(sock_path)

    if failed:
        print("RESULT: FAIL")
        return 1
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
