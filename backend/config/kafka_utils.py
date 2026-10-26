"""Kafka utilities for network resolution and bootstrap server discovery across Windows/WSL environments."""

import os
import socket
import subprocess
import sys
import logging

logger = logging.getLogger(__name__)


def resolve_kafka_bootstrap_servers() -> str:
    """Resolves reachable Kafka bootstrap address.

    Falls back to WSL IP on Windows if localhost:9092 is not bridged by the host firewall.
    """
    configured = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    if configured.startswith("localhost:"):
        configured = configured.replace("localhost:", "127.0.0.1:")

    if ":" not in configured:
        return configured

    host, port_str = configured.split(":")
    port = int(port_str)

    # Test direct connection to configured address
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            s.connect((host, port))
            return configured
    except Exception:
        pass

    # If on Windows and host is localhost/127.0.0.1, check WSL2 internal IP
    if sys.platform == "win32" and host in ("localhost", "127.0.0.1"):
        try:
            output = subprocess.check_output(
                ["wsl", "hostname", "-I"],
                text=True,
                timeout=2,
                stderr=subprocess.DEVNULL,
            )
            ips = output.strip().split()
            if ips:
                wsl_ip = ips[0]
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_wsl:
                    s_wsl.settimeout(0.5)
                    s_wsl.connect((wsl_ip, port))
                    logger.info("Kafka resolved via WSL2 bridge address: %s:%d", wsl_ip, port)
                    return f"{wsl_ip}:{port}"
        except Exception as e:
            logger.debug("WSL IP detection failed: %s", e)

    return configured


def is_kafka_reachable(bootstrap_servers: str, timeout: float = 1.0) -> bool:
    """Checks if Kafka bootstrap server is accepting TCP connections."""
    try:
        parts = bootstrap_servers.split(",")
        first = parts[0].strip()
        if ":" in first:
            host, port_str = first.split(":")
            port = int(port_str)
        else:
            host = first
            port = 9092
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def wait_for_kafka(bootstrap_servers: str, max_retries: int = 60, retry_delay: float = 2.0) -> bool:
    """Waits for Kafka broker to become reachable with friendly console messages."""
    import time

    for attempt in range(1, max_retries + 1):
        if is_kafka_reachable(bootstrap_servers, timeout=1.0):
            return True
        print(
            f"[WAITING] Kafka at {bootstrap_servers} is not reachable yet. "
            f"Please ensure Kafka is running (.\\scripts\\start_kafka.bat). Retrying in {int(retry_delay)}s...",
            flush=True,
        )
        time.sleep(retry_delay)
    return False
