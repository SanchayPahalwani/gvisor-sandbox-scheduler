"""Fork server that runs as PID 1 inside a gss sandbox.

It imports the modules named in GSS_PRIME once, then listens on a Unix socket.
For each connection it forks a supervisor; the supervisor receives the client's
stdin, stdout and stderr (SCM_RIGHTS) and the code, forks a worker that runs the
code on those descriptors, and reports the worker's exit code back on the
connection. Nothing here uses threads, so fork is safe.

Protocol (client -> zygote): one sendmsg with a 4-byte big-endian payload length
and SCM_RIGHTS carrying fds 0, 1, 2; then the JSON payload
{"code": str, "timeout_s": number, "env": {str: str}}.
Reply: a 4-byte big-endian signed exit code (negative = killed by that signal),
then the connection is closed.
"""

import array
import importlib
import json
import linecache
import os
import select
import signal
import socket
import struct
import sys
import time
import traceback

SOCKET_DEFAULT = "/tmp/gss-zygote.sock"
SANDBOX_FILENAME = "<sandbox>"
MAX_PAYLOAD = 64 << 20
INT_SIZE = array.array("i").itemsize


class ProtocolError(Exception):
    pass


def recv_exact(conn, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = conn.recv(min(n - len(buf), 1 << 20))
        if not chunk:
            raise ProtocolError("connection closed mid-request")
        buf += chunk
    return bytes(buf)


def recv_request(conn):
    """Returns (fds, code, timeout_s, env) or raises ProtocolError/OSError."""
    fds = []
    head = b""
    try:
        while len(head) < 4:
            data, ancdata, flags, _ = conn.recvmsg(
                4 - len(head), socket.CMSG_SPACE(3 * INT_SIZE)
            )
            for level, kind, cdata in ancdata:
                if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                    usable = len(cdata) - len(cdata) % INT_SIZE
                    fds.extend(array.array("i", cdata[:usable]))
            if flags & socket.MSG_CTRUNC:
                raise ProtocolError("control message truncated")
            if not data:
                raise ProtocolError("connection closed before header")
            head += data
        if len(fds) != 3:
            raise ProtocolError("want 3 file descriptors, got %d" % len(fds))
        (length,) = struct.unpack(">I", head)
        if length > MAX_PAYLOAD:
            raise ProtocolError("payload of %d bytes is too large" % length)
        req = json.loads(recv_exact(conn, length))
        code = req["code"]
        timeout_s = req.get("timeout_s") or 0
        env = req.get("env") or {}
        if not isinstance(code, str):
            raise ProtocolError("code is not a string")
        if not isinstance(timeout_s, (int, float)) or timeout_s < 0:
            raise ProtocolError("bad timeout_s")
        if not isinstance(env, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in env.items()
        ):
            raise ProtocolError("env is not a map of strings")
    except (ValueError, KeyError, TypeError) as e:
        for fd in fds:
            os.close(fd)
        raise ProtocolError("bad request: %s" % e)
    except BaseException:
        for fd in fds:
            os.close(fd)
        raise
    return fds, code, float(timeout_s), env


def run_worker(fds, code, env):
    """Runs in the worker process; never returns."""
    status = 1
    try:
        os.setpgid(0, 0)
        for target, fd in enumerate(fds):
            os.dup2(fd, target)
        for fd in fds:
            if fd > 2:
                os.close(fd)
        os.environ.update(env)
        if os.path.isdir("/work"):
            os.chdir("/work")
        sys.stdin = sys.__stdin__ = open(0, "r", encoding="utf-8", closefd=False)
        sys.stdout = sys.__stdout__ = open(
            1, "w", buffering=1, encoding="utf-8", closefd=False
        )
        sys.stderr = sys.__stderr__ = open(
            2, "w", buffering=1, encoding="utf-8", errors="backslashreplace",
            closefd=False,
        )
        # Lets tracebacks show the user's source lines.
        linecache.cache[SANDBOX_FILENAME] = (
            len(code), None, code.splitlines(True), SANDBOX_FILENAME,
        )
        try:
            exec(compile(code, SANDBOX_FILENAME, "exec"), {"__name__": "__main__"})
            status = 0
        except SystemExit as e:
            if e.code is None:
                status = 0
            elif isinstance(e.code, int):
                status = e.code
            else:
                print(e.code, file=sys.stderr)
                status = 1
        except BaseException as e:
            tb = e.__traceback__
            while tb is not None and tb.tb_frame.f_code.co_filename != SANDBOX_FILENAME:
                tb = tb.tb_next
            traceback.print_exception(type(e), e, tb)
            status = 1
    finally:
        for stream in (sys.stdout, sys.stderr):
            try:
                stream.flush()
            except BaseException:
                pass
        os._exit(status & 0xFF)


def kill_group(pgid):
    try:
        os.killpg(pgid, signal.SIGKILL)
    except OSError:
        pass


def supervise(conn):
    """Runs in the supervisor process; never returns."""
    try:
        try:
            fds, code, timeout_s, env = recv_request(conn)
        except (ProtocolError, OSError) as e:
            print("zygote: %s" % e, file=sys.stderr, flush=True)
            os._exit(1)

        wake_r, wake_w = os.pipe()
        os.set_blocking(wake_r, False)
        os.set_blocking(wake_w, False)
        # The zygote ignores SIGCHLD; the supervisor needs to see and reap its worker.
        signal.signal(signal.SIGCHLD, lambda signum, frame: None)
        signal.set_wakeup_fd(wake_w)

        pid = os.fork()
        if pid == 0:
            signal.set_wakeup_fd(-1)
            signal.signal(signal.SIGCHLD, signal.SIG_DFL)
            os.close(wake_r)
            os.close(wake_w)
            conn.close()
            run_worker(fds, code, env)
        try:
            os.setpgid(pid, pid)  # also done by the worker; whichever runs first wins
        except OSError:
            pass
        for fd in fds:
            os.close(fd)

        deadline = time.monotonic() + timeout_s if timeout_s > 0 else None
        while True:
            wpid, status = os.waitpid(pid, os.WNOHANG)
            if wpid == pid:
                kill_group(pid)  # leftovers the worker started
                send_code(conn, os.waitstatus_to_exitcode(status))
                os._exit(0)
            remaining = None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    kill_group(pid)
                    _, status = os.waitpid(pid, 0)
                    send_code(conn, os.waitstatus_to_exitcode(status))
                    os._exit(0)
            readable, _, _ = select.select([conn, wake_r], [], [], remaining)
            if wake_r in readable:
                try:
                    while os.read(wake_r, 64):
                        pass
                except BlockingIOError:
                    pass
            if conn in readable:
                try:
                    gone = not conn.recv(4096)
                except OSError:
                    gone = True
                if gone:
                    kill_group(pid)
                    os.waitpid(pid, 0)
                    os._exit(0)
    except BaseException:
        try:
            traceback.print_exc()
            sys.stderr.flush()
        finally:
            os._exit(1)


def send_code(conn, code):
    try:
        conn.sendall(struct.pack(">i", code))
    except OSError:
        pass


def main():
    # Fork safety: BLAS must not start helper threads in the process that forks.
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    path = os.environ.get("GSS_ZYGOTE_SOCK") or SOCKET_DEFAULT

    start = time.monotonic()
    for name in os.environ.get("GSS_PRIME", "").split(","):
        name = name.strip()
        if name:
            importlib.import_module(name)
    prime_ms = int((time.monotonic() - start) * 1000)

    signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(path)
    listener.listen(64)
    print("READY prime_ms=%d" % prime_ms, flush=True)

    while True:
        try:
            conn, _ = listener.accept()
        except OSError as e:
            print("zygote: accept: %s" % e, file=sys.stderr, flush=True)
            time.sleep(0.01)  # do not spin on a persistent error such as EMFILE
            continue
        try:
            pid = os.fork()
        except OSError as e:
            print("zygote: fork: %s" % e, file=sys.stderr, flush=True)
            conn.close()
            continue
        if pid == 0:
            listener.close()
            supervise(conn)
        conn.close()


if __name__ == "__main__":
    main()
