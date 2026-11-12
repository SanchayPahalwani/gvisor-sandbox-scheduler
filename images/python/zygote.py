"""Fork server that runs as PID 1 inside a gss sandbox.

It imports the modules named in GSS_PRIME once, then listens on a Unix socket.
For each connection it forks a supervisor; the supervisor receives the client's
stdin, stdout and stderr (SCM_RIGHTS) and the code, forks a worker that runs the
code on those descriptors, and reports the worker's exit code back on the
connection. Nothing here uses threads, so fork is safe.

Protocol (client -> zygote): one sendmsg with a 4-byte big-endian payload length
and SCM_RIGHTS carrying fds 0, 1, 2; then the JSON payload
{"code": str, "timeout_s": number, "env": {str: str}}.
Reply: a 4-byte big-endian signed exit code (negative = killed by that signal;
125 = the worker could not be set up), then the connection is closed. A request
that cannot be parsed gets a "zygote: ..." line on the client's stderr and no
exit code.
"""

import array
import atexit
import gc
import importlib
import json
import linecache
import math
import os
import select
import signal
import socket
import struct
import sys
import time
import traceback
import types

SOCKET_DEFAULT = "/tmp/gss-zygote.sock"
SANDBOX_FILENAME = "<sandbox>"
MAX_PAYLOAD = 64 << 20
MAX_TIMEOUT_S = 86400
EXIT_SETUP = 125  # the worker could not be set up; the code did not run
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
    """Returns (fds, payload bytes); raises ProtocolError or OSError."""
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
        return fds, recv_exact(conn, length)
    except BaseException:
        for fd in fds:
            os.close(fd)
        raise


def parse_request(payload):
    """Returns (code, timeout_s, env) or raises ProtocolError."""
    try:
        req = json.loads(payload)
    except ValueError as e:
        raise ProtocolError("bad request: %s" % e)
    if not isinstance(req, dict):
        raise ProtocolError("bad request: not a JSON object")
    code = req.get("code")
    timeout_s = req.get("timeout_s") or 0
    env = req.get("env") or {}
    if not isinstance(code, str):
        raise ProtocolError("bad request: code is not a string")
    try:
        # An integer too large for a float makes math.isnan raise OverflowError.
        bad_timeout = (
            isinstance(timeout_s, bool)
            or not isinstance(timeout_s, (int, float))
            or math.isnan(timeout_s)
            or not 0 <= timeout_s <= MAX_TIMEOUT_S
        )
    except OverflowError:
        bad_timeout = True
    if bad_timeout:
        raise ProtocolError(
            "bad request: timeout_s %r is outside 0..%d" % (timeout_s, MAX_TIMEOUT_S)
        )
    if not isinstance(env, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in env.items()
    ):
        raise ProtocolError("bad request: env is not a map of strings")
    return code, float(timeout_s), env


def run_worker(fds, code, env):
    """Runs in the worker process; never returns."""
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
        # Look like `python3 -c`: a fresh __main__ module, argv, and cwd first on the path.
        main_module = types.ModuleType("__main__")
        sys.modules["__main__"] = main_module
        sys.argv = ["-c"]
        sys.path[0] = ""
        # Lets tracebacks show the user's source lines.
        linecache.cache[SANDBOX_FILENAME] = (
            len(code), None, code.splitlines(True), SANDBOX_FILENAME,
        )
    except BaseException as e:
        # fd 2 is the client's stderr once dup2 has run, the zygote's before that.
        try:
            os.write(2, ("zygote: worker setup: %s\n" % e).encode("utf-8", "replace"))
        finally:
            os._exit(EXIT_SETUP)

    status = run_code(code, main_module.__dict__)
    del main_module
    status = shut_down(status)
    os._exit(status & 0xFF)


def run_code(code, namespace):
    """Runs the user's code and returns its exit status."""
    try:
        exec(compile(code, SANDBOX_FILENAME, "exec"), namespace)
        return 0
    except SystemExit as e:
        return exit_status(e)
    except BaseException as e:
        print_user_exception(e)
        return 1


def exit_status(e):
    if e.code is None:
        return 0
    if isinstance(e.code, int):
        return e.code
    try:
        print(e.code, file=sys.stderr)
    except BaseException:
        pass
    return 1


def print_user_exception(e):
    tb = e.__traceback__
    while tb is not None and tb.tb_frame.f_code.co_filename != SANDBOX_FILENAME:
        tb = tb.tb_next
    try:
        traceback.print_exception(type(e), e, tb)
    except BaseException:
        pass


def shut_down(status):
    """Does what interpreter exit does for `python3 -c`, as far as practical: wait
    for non-daemon threads, run atexit handlers, drop __main__ so objects
    are finalized (open files flushed), then flush stdio."""
    if "threading" in sys.modules:
        try:
            sys.modules["threading"]._shutdown()
        except SystemExit as e:
            status = exit_status(e)
        except BaseException as e:
            print_user_exception(e)
    try:
        atexit._run_exitfuncs()
    except BaseException:
        pass
    # The order Py_FinalizeEx uses: collect, drop __main__, collect again. Objects
    # are finalized while the globals they refer to still exist, so __del__ and
    # daemon threads see a normal module, and unclosed files are flushed even when
    # they sit in a cycle with a function's __globals__.
    try:
        gc.collect()
        sys.modules.pop("__main__", None)
        gc.collect()
    except BaseException:
        pass
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except BaseException:
            pass
    return status


def kill_worker(pid):
    """Kills the worker's process group and the worker itself, in case it left
    the group. The pid cannot be reused before the supervisor reaps it."""
    for kill in (os.killpg, os.kill):
        try:
            kill(pid, signal.SIGKILL)
        except OSError:
            pass


def supervise(conn):
    """Runs in the supervisor process; never returns."""
    pid = None
    try:
        try:
            fds, payload = recv_request(conn)
        except (ProtocolError, OSError) as e:
            print("zygote: %s" % e, file=sys.stderr, flush=True)
            os._exit(1)
        try:
            code, timeout_s, env = parse_request(payload)
        except ProtocolError as e:
            # Tell the client on its own stderr, then close without an exit code.
            try:
                os.write(fds[2], ("zygote: %s\n" % e).encode("utf-8", "replace"))
            except OSError:
                pass
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
                # Background processes the code started are left alone.
                send_code(conn, os.waitstatus_to_exitcode(status))
                os._exit(0)
            remaining = None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    kill_worker(pid)
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
                    kill_worker(pid)
                    os.waitpid(pid, 0)
                    os._exit(0)
    except BaseException:
        try:
            if pid:
                kill_worker(pid)
            traceback.print_exc()
            sys.stderr.flush()
        finally:
            os._exit(1)


def reseed_numpy():
    """Gives numpy's legacy global random state fresh OS entropy in a forked
    child. numpy seeds it once, when numpy.random is imported, so without this
    every Exec of a sandbox, and every sandbox restored from one checkpoint
    image, would draw the same numbers from np.random.* (and pandas' sample()
    without random_state). CPython reseeds `random` after fork itself. If
    numpy.random was never imported here, the child seeds it on import."""
    module = sys.modules.get("numpy.random")
    if module is not None:
        module.seed()


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
    # Runs in every fork: the supervisor and the worker.
    os.register_at_fork(after_in_child=reseed_numpy)
    # Move everything that exists now (the interpreter plus the primed modules)
    # into the permanent generation. Each worker runs gc.collect() at exit, and
    # without the freeze that pass would scan the whole primed heap on every exec
    # (about 20 ms with numpy and pandas). Frozen objects are never scanned, and
    # freezing also keeps the collector from touching them, so forked children
    # share those pages instead of copying them.
    gc.collect()
    gc.freeze()
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
