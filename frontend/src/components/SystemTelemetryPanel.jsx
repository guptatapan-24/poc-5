"use client";

import React, { useState, useEffect, useRef } from "react";
import { Cpu, Users, Zap, HardDrive, Activity, Terminal, ArrowUpRight } from "lucide-react";

export function SystemTelemetryPanel({ telemetry }) {
  // Maintain a rolling history of past 30 data points for real-time sparkline graph
  const [history, setHistory] = useState([]);
  const lastTimestampRef = useRef("");

  useEffect(() => {
    if (!telemetry || !telemetry.timestamp || telemetry.timestamp === lastTimestampRef.current) {
      return;
    }
    lastTimestampRef.current = telemetry.timestamp;

    setHistory((prev) => {
      const nextPoint = {
        users: telemetry.active_connections || 0,
        cpu: telemetry.cpu_percent || 0,
        rps: telemetry.requests_per_second || 0,
        time: new Date(telemetry.timestamp).toLocaleTimeString(),
      };
      const updated = [...prev, nextPoint];
      if (updated.length > 30) updated.shift();
      return updated;
    });
  }, [telemetry]);

  const activeUsers = telemetry?.active_connections ?? 1;
  const peakUsers = telemetry?.peak_connections ?? activeUsers;
  const cpuPercent = telemetry?.cpu_percent ?? 0;
  const procCpu = telemetry?.process_cpu_percent ?? 0;
  const rps = telemetry?.requests_per_second ?? 0;
  const totalReqs = telemetry?.total_requests ?? 0;
  const procRamMb = telemetry?.process_memory_mb ?? 0;
  const sysRamPct = telemetry?.system_memory_percent ?? 0;

  // CPU progress bar color
  const cpuColor =
    cpuPercent > 80 ? "var(--accent-rose)" : cpuPercent > 50 ? "var(--accent-amber)" : "var(--accent-emerald)";

  // SVG Chart points calculation
  const width = 360;
  const height = 64;
  const maxUsersVal = Math.max(10, ...history.map((h) => h.users));
  const maxCpuVal = 100;

  const userPath =
    history.length > 1
      ? history
          .map((pt, idx) => {
            const x = (idx / (history.length - 1)) * width;
            const y = height - (pt.users / maxUsersVal) * (height - 10) - 5;
            return `${idx === 0 ? "M" : "L"}${x},${y}`;
          })
          .join(" ")
      : "";

  const cpuPath =
    history.length > 1
      ? history
          .map((pt, idx) => {
            const x = (idx / (history.length - 1)) * width;
            const y = height - (pt.cpu / maxCpuVal) * (height - 10) - 5;
            return `${idx === 0 ? "M" : "L"}${x},${y}`;
          })
          .join(" ")
      : "";

  return (
    <div className="glass-panel" id="system-telemetry-card" style={{ marginBottom: "20px" }}>
      <div className="card-header" style={{ borderBottom: "1px solid rgba(255, 255, 255, 0.06)", paddingBottom: "12px" }}>
        <h2 className="card-title" style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "0.95rem" }}>
          <Activity size={18} color="var(--accent-cyan)" />
          Server Telemetry & Locust Load Monitor
        </h2>

        <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
          <span
            className="badge"
            style={{
              background: activeUsers > 5 ? "rgba(168, 85, 247, 0.15)" : "rgba(16, 185, 129, 0.15)",
              color: activeUsers > 5 ? "#c084fc" : "#10b981",
              border: `1px solid ${activeUsers > 5 ? "rgba(168, 85, 247, 0.3)" : "rgba(16, 185, 129, 0.3)"}`,
              fontSize: "0.72rem",
              fontWeight: 600,
            }}
          >
            <span
              className="status-dot"
              style={{
                background: activeUsers > 5 ? "#c084fc" : "#10b981",
                boxShadow: activeUsers > 5 ? "0 0 8px #a855f7" : "0 0 6px #10b981",
              }}
            />
            {activeUsers > 1 ? `Locust Load Active: ${activeUsers} Users` : "Idle / 1 Client"}
          </span>

          <span
            className="badge badge-live"
            style={{ fontSize: "0.7rem", background: "rgba(56, 189, 248, 0.12)", color: "#38bdf8" }}
          >
            Stream Rate: 1s
          </span>
        </div>
      </div>

      <div className="card-body" style={{ paddingTop: "14px" }}>
        {/* 4 Telemetry Metrics Grid */}
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(210px, 1fr))",
            gap: "12px",
            marginBottom: "16px",
          }}
        >
          {/* Card 1: Active Users */}
          <div
            className="telemetry-card"
            style={{
              background: "rgba(15, 23, 42, 0.6)",
              border: "1px solid rgba(255, 255, 255, 0.08)",
              borderRadius: "10px",
              padding: "12px 14px",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "6px" }}>
              <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.04em" }}>
                Active Users
              </span>
              <Users size={16} color="var(--accent-cyan)" />
            </div>
            <div style={{ display: "flex", alignItems: "baseline", gap: "6px" }}>
              <span className="mono-text" style={{ fontSize: "1.6rem", fontWeight: 800, color: "#f8fafc" }}>
                {activeUsers}
              </span>
              <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>clients</span>
            </div>
            <div style={{ fontSize: "0.7rem", color: "var(--text-muted)", marginTop: "4px" }}>
              Peak reached: <span style={{ color: "var(--accent-cyan)", fontWeight: 600 }}>{peakUsers} concurrent</span>
            </div>
          </div>

          {/* Card 2: Throughput RPS */}
          <div
            className="telemetry-card"
            style={{
              background: "rgba(15, 23, 42, 0.6)",
              border: "1px solid rgba(255, 255, 255, 0.08)",
              borderRadius: "10px",
              padding: "12px 14px",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "6px" }}>
              <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.04em" }}>
                Inbound Throughput
              </span>
              <Zap size={16} color="var(--accent-amber)" />
            </div>
            <div style={{ display: "flex", alignItems: "baseline", gap: "6px" }}>
              <span className="mono-text" style={{ fontSize: "1.6rem", fontWeight: 800, color: "var(--accent-amber)" }}>
                {rps.toFixed(1)}
              </span>
              <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>req/s</span>
            </div>
            <div style={{ fontSize: "0.7rem", color: "var(--text-muted)", marginTop: "4px" }}>
              Processed: <span style={{ color: "var(--text-secondary)", fontWeight: 600 }}>{totalReqs.toLocaleString()} reqs</span>
            </div>
          </div>

          {/* Card 3: Server CPU */}
          <div
            className="telemetry-card"
            style={{
              background: "rgba(15, 23, 42, 0.6)",
              border: "1px solid rgba(255, 255, 255, 0.08)",
              borderRadius: "10px",
              padding: "12px 14px",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "6px" }}>
              <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.04em" }}>
                Server CPU Load
              </span>
              <Cpu size={16} color={cpuColor} />
            </div>
            <div style={{ display: "flex", alignItems: "baseline", gap: "6px" }}>
              <span className="mono-text" style={{ fontSize: "1.6rem", fontWeight: 800, color: cpuColor }}>
                {cpuPercent.toFixed(1)}%
              </span>
              <span style={{ fontSize: "0.72rem", color: "var(--text-muted)" }}>(system)</span>
            </div>
            {/* Progress bar */}
            <div style={{ width: "100%", height: "4px", background: "rgba(255,255,255,0.08)", borderRadius: "2px", marginTop: "6px", overflow: "hidden" }}>
              <div
                style={{
                  width: `${Math.min(100, Math.max(3, cpuPercent))}%`,
                  height: "100%",
                  background: cpuColor,
                  transition: "width 0.4s ease, background 0.4s ease",
                }}
              />
            </div>
            <div style={{ fontSize: "0.68rem", color: "var(--text-muted)", marginTop: "4px" }}>
              Daphne Worker: <span style={{ color: "var(--text-secondary)" }}>{procCpu.toFixed(1)}%</span>
            </div>
          </div>

          {/* Card 4: Memory Usage */}
          <div
            className="telemetry-card"
            style={{
              background: "rgba(15, 23, 42, 0.6)",
              border: "1px solid rgba(255, 255, 255, 0.08)",
              borderRadius: "10px",
              padding: "12px 14px",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "6px" }}>
              <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.04em" }}>
                Process Memory
              </span>
              <HardDrive size={16} color="var(--accent-emerald)" />
            </div>
            <div style={{ display: "flex", alignItems: "baseline", gap: "6px" }}>
              <span className="mono-text" style={{ fontSize: "1.6rem", fontWeight: 800, color: "#f8fafc" }}>
                {procRamMb.toFixed(0)}
              </span>
              <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>MB RSS</span>
            </div>
            {/* Progress bar */}
            <div style={{ width: "100%", height: "4px", background: "rgba(255,255,255,0.08)", borderRadius: "2px", marginTop: "6px", overflow: "hidden" }}>
              <div
                style={{
                  width: `${Math.min(100, sysRamPct)}%`,
                  height: "100%",
                  background: "var(--accent-emerald)",
                  transition: "width 0.4s ease",
                }}
              />
            </div>
            <div style={{ fontSize: "0.68rem", color: "var(--text-muted)", marginTop: "4px" }}>
              System RAM: <span style={{ color: "var(--text-secondary)" }}>{sysRamPct.toFixed(1)}%</span>
            </div>
          </div>
        </div>

        {/* Real-Time Live Graph Strip */}
        <div
          style={{
            background: "rgba(10, 15, 29, 0.65)",
            border: "1px solid rgba(255, 255, 255, 0.06)",
            borderRadius: "8px",
            padding: "10px 14px",
            display: "flex",
            flexDirection: "column",
            gap: "8px",
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <div style={{ display: "flex", alignItems: "center", gap: "12px", fontSize: "0.75rem" }}>
              <span style={{ color: "var(--text-secondary)", fontWeight: 600 }}>Live Load Curve (Past 30s):</span>
              <span style={{ display: "flex", alignItems: "center", gap: "4px", color: "var(--accent-cyan)" }}>
                <span style={{ width: "8px", height: "8px", borderRadius: "50%", background: "var(--accent-cyan)" }} />
                Active Clients (Peak {maxUsersVal})
              </span>
              <span style={{ display: "flex", alignItems: "center", gap: "4px", color: "#c084fc" }}>
                <span style={{ width: "8px", height: "8px", borderRadius: "50%", background: "#c084fc" }} />
                Server CPU %
              </span>
            </div>

            <span style={{ fontSize: "0.68rem", color: "var(--text-muted)" }}>
              Updated: {new Date(telemetry?.timestamp || Date.now()).toLocaleTimeString()}
            </span>
          </div>

          {/* SVG Sparkline */}
          <div style={{ width: "100%", height: "64px", position: "relative" }}>
            <svg
              viewBox={`0 0 ${width} ${height}`}
              preserveAspectRatio="none"
              style={{ width: "100%", height: "100%", overflow: "visible" }}
            >
              {/* Grid guide lines */}
              <line x1="0" y1="10" x2={width} y2="10" stroke="rgba(255,255,255,0.05)" strokeDasharray="3 3" />
              <line x1="0" y1="36" x2={width} y2="36" stroke="rgba(255,255,255,0.05)" strokeDasharray="3 3" />
              <line x1="0" y1="58" x2={width} y2="58" stroke="rgba(255,255,255,0.05)" strokeDasharray="3 3" />

              {/* User Curve (Cyan) */}
              {userPath && (
                <path
                  d={userPath}
                  fill="none"
                  stroke="var(--accent-cyan)"
                  strokeWidth="2.5"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  style={{ filter: "drop-shadow(0 0 4px rgba(6, 182, 212, 0.4))" }}
                />
              )}

              {/* CPU Curve (Purple) */}
              {cpuPath && (
                <path
                  d={cpuPath}
                  fill="none"
                  stroke="#c084fc"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeDasharray="4 2"
                  style={{ filter: "drop-shadow(0 0 3px rgba(192, 132, 252, 0.3))" }}
                />
              )}
            </svg>
          </div>
        </div>
      </div>
    </div>
  );
}
