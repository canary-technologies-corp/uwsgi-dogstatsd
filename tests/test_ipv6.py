#!/usr/bin/env python3
"""
Test that the dogstatsd plugin can send UDP metrics over both IPv4 and IPv6.

Starts a UDP listener on each protocol, launches uWSGI with the dogstatsd
plugin pointed at each listener, and verifies metrics are received.
"""

import os
import socket
import subprocess
import sys
import time
import select
import signal

PLUGIN_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TIMEOUT = 15  # seconds to wait for metrics


def find_free_port(family):
    """Find a free UDP port for the given address family."""
    sock = socket.socket(family, socket.SOCK_DGRAM)
    if family == socket.AF_INET:
        sock.bind(("127.0.0.1", 0))
    else:
        sock.bind(("::1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def receive_metrics(sock, timeout):
    """Receive UDP packets until timeout, return all received data."""
    packets = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        ready, _, _ = select.select([sock], [], [], min(remaining, 1.0))
        if ready:
            data, _ = sock.recvfrom(8192)
            packets.append(data.decode("utf-8", errors="replace"))
    return packets


def build_plugin():
    """Build the dogstatsd plugin and return the path to the .so file."""
    # Find uwsgi binary
    uwsgi_bin = subprocess.run(
        ["which", "uwsgi"], capture_output=True, text=True
    ).stdout.strip()
    if not uwsgi_bin:
        print("SKIP: uwsgi not found in PATH")
        sys.exit(0)

    print(f"Building plugin with {uwsgi_bin}...")
    result = subprocess.run(
        [uwsgi_bin, "--build-plugin", PLUGIN_DIR],
        capture_output=True,
        text=True,
        cwd="/tmp",
    )
    plugin_path = "/tmp/dogstatsd_plugin.so"
    if result.returncode != 0 or not os.path.exists(plugin_path):
        print(f"FAIL: Plugin build failed:\n{result.stderr}\n{result.stdout}")
        sys.exit(1)
    print("Plugin built successfully.")
    return plugin_path


def run_uwsgi_test(plugin_path, address, port, label):
    """
    Run uWSGI with dogstatsd pushing to address:port, verify metrics received.
    Returns True if metrics were received.
    """
    # Determine address family from the address
    if ":" in address:
        family = socket.AF_INET6
        bracket_addr = f"[{address}]"
    else:
        family = socket.AF_INET
        bracket_addr = address

    # Start UDP listener
    sock = socket.socket(family, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((address, port))
    sock.setblocking(False)

    print(f"  [{label}] Listening on {address}:{port}")

    # Run uWSGI with the dogstatsd plugin
    # Use --master --no-server to start uWSGI without an HTTP server
    # The stats pusher runs on a timer regardless
    uwsgi_addr = f"{bracket_addr}:{port}"
    cmd = [
        "uwsgi",
        "--plugins-dir", os.path.dirname(plugin_path),
        "--plugin", "dogstatsd",
        "--enable-metrics",
        "--stats-push", f"dogstatsd:{uwsgi_addr},test",
        "--master",
        "--workers", "1",
        "--http", "127.0.0.1:0",
        "--stats-pusher-default-freq", "1",
        "--wsgi-file", os.path.join(PLUGIN_DIR, "example", "app.py"),
    ]

    print(f"  [{label}] Starting uWSGI: stats-push dogstatsd:{uwsgi_addr},test")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
    )

    try:
        packets = receive_metrics(sock, TIMEOUT)
    finally:
        # Kill the entire process group
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()
        sock.close()

    if packets:
        # Verify we got actual dogstatsd metrics (format: name:value|type)
        metric_count = sum(1 for p in packets for line in p.splitlines() if "|" in line and ":" in line)
        print(f"  [{label}] PASS: Received {metric_count} metrics in {len(packets)} packets")
        # Print a sample
        for line in packets[0].splitlines()[:3]:
            print(f"    {line}")
        return True
    else:
        # Dump uwsgi output for debugging
        stdout = proc.stdout.read().decode("utf-8", errors="replace") if proc.stdout else ""
        print(f"  [{label}] FAIL: No metrics received within {TIMEOUT}s")
        if stdout:
            print(f"  uWSGI output (last 500 chars):\n{stdout[-500:]}")
        return False


def main():
    plugin_path = build_plugin()

    # Check if the example app exists
    app_path = os.path.join(PLUGIN_DIR, "example", "app.py")
    if not os.path.exists(app_path):
        print(f"SKIP: example app not found at {app_path}")
        sys.exit(0)

    results = {}

    # Test IPv4
    print("\nTesting IPv4 (127.0.0.1)...")
    ipv4_port = find_free_port(socket.AF_INET)
    results["IPv4"] = run_uwsgi_test(plugin_path, "127.0.0.1", ipv4_port, "IPv4")

    # Test IPv6 (only if available)
    print("\nTesting IPv6 (::1)...")
    try:
        sock6 = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        sock6.bind(("::1", 0))
        sock6.close()
        ipv6_available = True
    except OSError:
        ipv6_available = False

    if ipv6_available:
        ipv6_port = find_free_port(socket.AF_INET6)
        results["IPv6"] = run_uwsgi_test(plugin_path, "::1", ipv6_port, "IPv6")
    else:
        print("  [IPv6] SKIP: IPv6 not available on this host")

    # Summary
    print("\n" + "=" * 60)
    print("RESULTS:")
    for proto, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {proto}: {status}")
    print("=" * 60)

    if all(results.values()):
        print("\nAll tests passed.")
        return 0
    else:
        print("\nSome tests failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
