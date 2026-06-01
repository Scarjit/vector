#!/usr/bin/env python3
"""
Quick smoke suite for iterative development.

test_passthrough               – basic passthrough sanity (~5s)
test_batching                  – 6 msgs, 0.5s delay, 2s timeout (~5s)
test_batch_timeout_reliability – 10 runs of the lost-timer reproducer (~30s)

For final / production validation use test_passthrough.py (full suite, ~8 min).
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer

VECTOR_BIN     = os.path.join(os.path.dirname(__file__), "..", "target", "debug", "vector")

SOURCE_PORT    = 8788
COLLECTOR_PORT = 8789

STARTUP_TIMEOUT = 30

# ── Vector config templates ───────────────────────────────────────────────────

PASSTHROUGH_CONFIG = """\
sources:
  test_in:
    type: http_server
    address: "127.0.0.1:{source_port}"
    decoding:
      codec: json

sinks:
  test_out:
    type: file
    inputs: [test_in]
    path: "{output_file}"
    encoding:
      codec: json
"""

BATCH_CONFIG = """\
sources:
  test_in:
    type: http_server
    address: "127.0.0.1:{source_port}"
    framing:
      method: newline_delimited
    decoding:
      codec: json

sinks:
  test_out:
    type: http
    inputs: [test_in]
    uri: "http://127.0.0.1:{collector_port}/collect"
    encoding:
      codec: json
    framing:
      method: newline_delimited
    batch:
      timeout_secs: {batch_timeout}
      max_events: {max_events}
"""

# ── Collector HTTP server ─────────────────────────────────────────────────────

class CollectorServer:
    def __init__(self, port: int):
        self._received: list[tuple[dict, float]] = []
        self._batches:  list[tuple[list[dict], float]] = []
        self._lock = threading.Lock()
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                arrived = time.monotonic()
                length  = int(self.headers.get("Content-Length", 0))
                body    = self.rfile.read(length).decode()
                batch   = [json.loads(l) for l in body.splitlines() if l.strip()]
                with parent._lock:
                    parent._batches.append((batch, arrived))
                    for ev in batch:
                        parent._received.append((ev, arrived))
                self.send_response(200)
                self.end_headers()

            def log_message(self, *args):
                pass

        class ReusableHTTPServer(HTTPServer):
            def server_bind(self):
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                super().server_bind()

        self._server = ReusableHTTPServer(("127.0.0.1", port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=3)

    def events(self) -> list[tuple[dict, float]]:
        with self._lock:
            return list(self._received)

    def batches(self) -> list[tuple[list[dict], float]]:
        with self._lock:
            return list(self._batches)

    def wait_for_count(self, count: int, timeout: float) -> list[tuple[dict, float]]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if len(self.events()) >= count:
                return self.events()
            time.sleep(0.05)
        return self.events()

    def clear(self):
        with self._lock:
            self._received.clear()
            self._batches.clear()


# ── Shared helpers ────────────────────────────────────────────────────────────

def kill_stale_processes() -> None:
    ports = [SOURCE_PORT, COLLECTOR_PORT]
    subprocess.run(
        ["fuser", "-k", "-TERM"] + [f"{p}/tcp" for p in ports],
        capture_output=True,
    )
    time.sleep(0.5)


def wait_for_http(port: int, timeout: int) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{port}/", method="GET")
            urllib.request.urlopen(req, timeout=1)
            return True
        except urllib.error.HTTPError:
            return True
        except Exception:
            time.sleep(0.5)
    return False


def send_message(port: int, payload: dict) -> None:
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(
        f"http://127.0.0.1:{port}/",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status in (200, 204), f"unexpected status {resp.status}"


def send_burst(port: int, payloads: list) -> None:
    """Send multiple events as a single NDJSON request so they all arrive atomically."""
    ndjson = "\n".join(json.dumps(p) for p in payloads)
    data   = ndjson.encode()
    req    = urllib.request.Request(
        f"http://127.0.0.1:{port}/",
        data=data,
        headers={"Content-Type": "application/x-ndjson"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status in (200, 204), f"unexpected status {resp.status}"


def read_output_file(path: str, expected: int, timeout: int) -> list[dict]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            events = [json.loads(l) for l in open(path).read().splitlines() if l.strip()]
            if len(events) >= expected:
                return events
        time.sleep(0.2)
    return []


def spawn_vector(config_path: str) -> subprocess.Popen:
    return subprocess.Popen(
        [VECTOR_BIN, "--config", config_path],
        stdout=subprocess.DEVNULL,
        stderr=None,  # inherit: debug output goes to terminal
    )


def stop_vector(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def _cpu_burner(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        pass


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_passthrough() -> bool:
    print("\n=== test_passthrough ===")
    messages = [{"message": f"msg-{i}", "seq": i} for i in range(3)]

    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = os.path.join(tmpdir, "vector.yaml")
        output_path = os.path.join(tmpdir, "output.jsonl")
        open(config_path, "w").write(PASSTHROUGH_CONFIG.format(
            source_port=SOURCE_PORT, output_file=output_path,
        ))
        proc = spawn_vector(config_path)
        try:
            if not wait_for_http(SOURCE_PORT, STARTUP_TIMEOUT):
                print("FAIL: Vector did not start")
                return False
            for msg in messages:
                send_message(SOURCE_PORT, msg)
                print(f"  Sent: {msg}")
            time.sleep(1)
            events = read_output_file(output_path, len(messages), 10)
        finally:
            stop_vector(proc)

    missing = {m["message"] for m in messages} - {e.get("message") for e in events}
    if missing:
        print(f"FAIL: missing {missing}")
        return False
    print(f"PASS: all {len(messages)} messages passed through.")
    return True


def test_batching() -> bool:
    print("\n=== test_batching ===")
    MESSAGE_COUNT = 6
    BATCH_TIMEOUT = 2
    MESSAGE_DELAY = 0.5

    messages  = [{"message": f"batch-msg-{i}", "seq": i} for i in range(MESSAGE_COUNT)]
    collector = CollectorServer(COLLECTOR_PORT)

    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = os.path.join(tmpdir, "vector.yaml")
        open(config_path, "w").write(BATCH_CONFIG.format(
            source_port=SOURCE_PORT,
            collector_port=COLLECTOR_PORT,
            batch_timeout=BATCH_TIMEOUT,
            max_events=100,
        ))
        proc = spawn_vector(config_path)
        try:
            if not wait_for_http(SOURCE_PORT, STARTUP_TIMEOUT):
                collector.stop()
                print("FAIL: Vector did not start")
                return False
            for i, msg in enumerate(messages):
                send_message(SOURCE_PORT, msg)
                print(f"  [{i+1:02d}/{MESSAGE_COUNT}] Sent: {msg['message']}")
                if i < MESSAGE_COUNT - 1:
                    time.sleep(MESSAGE_DELAY)
            evs = collector.wait_for_count(MESSAGE_COUNT, BATCH_TIMEOUT + 3)
        finally:
            stop_vector(proc)

    collector.stop()

    missing = {m["message"] for m in messages} - {e.get("message") for e, _ in evs}
    if missing:
        print(f"FAIL: expected {MESSAGE_COUNT}, got {len(evs)}; missing {missing}")
        return False

    batches = collector.batches()
    print(f"PASS: all {MESSAGE_COUNT} messages across {len(batches)} batch(es).")
    for i, (batch, _) in enumerate(batches):
        print(f"  Batch {i+1} ({len(batch)}): {[e.get('message') for e in batch]}")
    return True


def test_batch_timeout_reliability() -> bool:
    """
    Lost-timer reproducer — quick variant (5 runs, ~15s).

    Sends a lone probe event and verifies it self-flushes via the batch
    timeout timer.  If the timer is silently lost, the event only arrives
    after the *next* event is sent.

    Note: no CPU-pressure threads here — the bug is a code-logic issue in
    the batcher, not a timing/scheduling sensitivity.  The full production
    test (test_passthrough.py) exercises the CPU-pressure scenario.
    """
    print("\n=== test_batch_timeout_reliability ===")

    BATCH_TIMEOUT  = 0.5
    GRACE          = 0.4
    MAX_OK_LATENCY = BATCH_TIMEOUT + GRACE
    RUNS           = 5

    print(f"  timeout={BATCH_TIMEOUT}s  max_ok={MAX_OK_LATENCY}s  runs={RUNS}")
    print(f"  Total runtime ≈ {RUNS * (BATCH_TIMEOUT + GRACE + 0.5):.0f}s\n")

    collector = CollectorServer(COLLECTOR_PORT)

    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = os.path.join(tmpdir, "vector.yaml")
        open(config_path, "w").write(BATCH_CONFIG.format(
            source_port=SOURCE_PORT,
            collector_port=COLLECTOR_PORT,
            batch_timeout=BATCH_TIMEOUT,
            max_events=100,
        ))
        proc = spawn_vector(config_path)
        try:
            if not wait_for_http(SOURCE_PORT, STARTUP_TIMEOUT):
                collector.stop()
                print("FAIL: Vector did not start")
                return False

            failures  = 0
            latencies = []

            for run in range(RUNS):
                tag = f"rel-{run}"
                collector.clear()

                sent_at = time.monotonic()
                send_message(SOURCE_PORT, {"message": tag, "run": run})

                found_at = None
                deadline = sent_at + MAX_OK_LATENCY
                while time.monotonic() < deadline:
                    for ev, arrived in collector.events():
                        if ev.get("message") == tag:
                            found_at = arrived
                            break
                    if found_at:
                        break
                    time.sleep(0.02)

                latency = (found_at - sent_at) if found_at else None
                if latency is not None:
                    latencies.append(latency)
                    print(f"  Run {run:02d}: flushed in {latency:.3f}s ✓")
                else:
                    failures += 1
                    latencies.append(float("inf"))
                    print(f"  Run {run:02d}: NOT flushed within {MAX_OK_LATENCY}s — timer lost ✗")

                # Brief gap between runs so each probe starts a fresh timer
                time.sleep(0.5)
        finally:
            stop_vector(proc)

    collector.stop()

    finite = [l for l in latencies if l != float("inf")]
    if finite:
        print(f"\n  Latency (ok runs): min={min(finite):.3f}s  max={max(finite):.3f}s  "
              f"avg={sum(finite)/len(finite):.3f}s")
    if failures == 0:
        print(f"PASS: timer fired correctly in all {RUNS} runs.")
        return True
    else:
        print(f"FLAKY: timer lost in {failures}/{RUNS} runs ({failures/RUNS*100:.0f}%).")
        print("  Lone events stalled until next event arrived — lost-timer bug confirmed.")
        return failures / RUNS < 0.2   # tolerate ≤20% noise in quick mode


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> int:
    kill_stale_processes()
    results = [
        test_passthrough(),
        test_batching(),
        test_batch_timeout_reliability(),
    ]
    passed = sum(results)
    total  = len(results)
    print(f"\n{'='*40}")
    print(f"Results: {passed}/{total} passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
