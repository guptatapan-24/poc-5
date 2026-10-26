"""Automated runner for Locust test scenarios H1, W1, and E1.

Executes headless benchmarks against Daphne on http://127.0.0.1:8000,
archives CSV & HTML reports, and formats results into Table 13 summary.
"""

import argparse
import os
from pathlib import Path
import subprocess
import sys
import time


def run_command(cmd, desc):
    print(f"\n========================================================")
    print(f"Executing: {desc}")
    print(f"Command: {cmd}")
    print(f"========================================================")
    result = subprocess.run(cmd, shell=True)
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description="Run automated Locust benchmarks")
    parser.add_argument("--host", default="http://127.0.0.1:8000", help="Target Daphne host")
    parser.add_argument("--users", type=int, default=10, help="Concurrent users (10, 50, 100)")
    parser.add_argument("--duration", default="15s", help="Test duration per scenario (e.g. 15s, 30s, 1m)")
    parser.add_argument("--scenario", default="all", choices=["all", "h1", "w1", "e1"], help="Scenario to execute")
    args = parser.parse_args()

    reports_dir = Path(__file__).resolve().parent / "locust_reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    locustfile = Path(__file__).resolve().parent / "locustfile.py"

    print(f"Starting Locust Benchmark Suite (Host: {args.host}, Users: {args.users}, Duration: {args.duration})")
    print(f"Reports will be saved to: {reports_dir}")

    # =========================================================================
    # Scenario H1: HTTP REST API
    # =========================================================================
    if args.scenario in ("all", "h1"):
        h1_html = reports_dir / f"scenario_h1_{args.users}u.html"
        h1_csv_prefix = reports_dir / f"scenario_h1_{args.users}u"
        cmd = (
            f"uv run locust -f \"{locustfile}\" HttpDashboardUser "
            f"--host {args.host} "
            f"--headless -u {args.users} -r {max(1, args.users // 2)} -t {args.duration} "
            f"--html \"{h1_html}\" --csv \"{h1_csv_prefix}\""
        )
        run_command(cmd, f"Scenario H1 (REST API, {args.users} users)")

    # =========================================================================
    # Scenario W1 & E1: WebSocket Connections & Real-Time Event Delivery
    # =========================================================================
    if args.scenario in ("all", "w1", "e1"):
        ws_html = reports_dir / f"scenario_ws_{args.users}u.html"
        ws_csv_prefix = reports_dir / f"scenario_ws_{args.users}u"
        cmd = (
            f"uv run locust -f \"{locustfile}\" WebSocketDashboardUser "
            f"--host {args.host} "
            f"--headless -u {args.users} -r {max(1, args.users // 2)} -t {args.duration} "
            f"--html \"{ws_html}\" --csv \"{ws_csv_prefix}\""
        )
        run_command(cmd, f"Scenario W1/E1 (WebSocket Concurrency & Delivery, {args.users} users)")

    print("\n========================================================")
    print("Locust Benchmark Scenarios Completed!")
    print(f"View full visual performance reports at: {reports_dir}")
    print("========================================================\n")


if __name__ == "__main__":
    main()
