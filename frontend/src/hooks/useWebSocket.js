"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { fetchRecentEvents, fetchSystemMetrics } from "../services/api";

const WS_URL = "ws://localhost:8000/ws/events/";

export function useWebSocket() {
  const [status, setStatus] = useState("DISCONNECTED"); // "CONNECTED" | "CONNECTING" | "DISCONNECTED" | "RECONNECTING"
  const [events, setEvents] = useState([]);
  const [replayStatus, setReplayStatus] = useState({
    status: null,
    replayId: null,
    count: 0,
    message: null,
  });
  const [systemInfo, setSystemInfo] = useState(null);
  const [telemetry, setTelemetry] = useState({
    active_connections: 1,
    peak_connections: 1,
    requests_per_second: 0.0,
    total_requests: 0,
    cpu_percent: 0.0,
    process_cpu_percent: 0.0,
    process_memory_mb: 0.0,
    system_memory_percent: 0.0,
    system_memory_used_mb: 0.0,
    system_memory_total_mb: 0.0,
    timestamp: new Date().toISOString(),
  });


  // Catch-up synchronization: recovers any alerts missed during network blips or reconnects
  const syncRecentEvents = useCallback(async () => {
    try {
      const data = await fetchRecentEvents(50);
      if (data && data.events && data.events.length > 0) {
        setEvents((prev) => {
          const existingIds = new Set(prev.map((e) => e.event_id));
          const toAdd = [];
          for (const item of data.events) {
            const evt = item.event || item;
            if (evt && evt.event_id && !existingIds.has(evt.event_id)) {
              toAdd.push({
                ...evt,
                delivery_mode: item.delivery_mode || evt.processing_mode || "LIVE",
                received_at: item.received_at || evt.event_timestamp || new Date().toISOString(),
                kafka_details: item.details,
              });
              existingIds.add(evt.event_id);
            }
          }
          if (toAdd.length === 0) return prev;
          const combined = [...toAdd, ...prev];
          combined.sort(
            (a, b) =>
              new Date(b.event_timestamp || b.received_at) -
              new Date(a.event_timestamp || a.received_at)
          );
          return combined;
        });
      }
    } catch (err) {
      console.warn("Failed to catch up recent events:", err);
    }
  }, []);

  // Generate or retrieve stable client ID for private directed replay routing
  const clientIdRef = useRef(null);
  const [clientId, setClientId] = useState(null);

  useEffect(() => {
    if (typeof window !== "undefined") {
      let id = sessionStorage.getItem("stock_stream_client_id");
      if (!id) {
        id = `client-${Math.random().toString(36).substring(2, 9)}`;
        sessionStorage.setItem("stock_stream_client_id", id);
      }
      clientIdRef.current = id;
      setClientId(id);
    }
  }, []);

  const wsRef = useRef(null);
  const reconnectTimeoutRef = useRef(null);
  const backoffRef = useRef(1000); // 1s initial backoff
  const isMountedRef = useRef(true);

  const connect = useCallback(() => {
    if (!isMountedRef.current) return;
    if (typeof window === "undefined" || typeof WebSocket === "undefined") return;
    if (wsRef.current && (wsRef.current.readyState === WebSocket.OPEN || wsRef.current.readyState === WebSocket.CONNECTING)) {
      return;
    }

    setStatus((prev) => (prev === "DISCONNECTED" ? "CONNECTING" : "RECONNECTING"));
    const activeId = clientIdRef.current || (typeof sessionStorage !== "undefined" ? sessionStorage.getItem("stock_stream_client_id") : null) || "client-init";
    const url = `${WS_URL}?client_id=${activeId}`;

    try {
      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        if (!isMountedRef.current || wsRef.current !== ws) return;
        setStatus("CONNECTED");
        backoffRef.current = 1000; // Reset backoff on success
        if (reconnectTimeoutRef.current) {
          clearTimeout(reconnectTimeoutRef.current);
          reconnectTimeoutRef.current = null;
        }
        // Catch up on any events that fired while disconnected
        syncRecentEvents();
      };

      ws.onmessage = (event) => {
        if (!isMountedRef.current || wsRef.current !== ws) return;
        try {
          const message = JSON.parse(event.data);

          if (message.message_type === "stock_event" && message.event) {
            // Append received stock event with strict deduplication
            setEvents((prev) => {
              const isDuplicate = prev.some(
                (e) => e.event_id === message.event.event_id && e.delivery_mode === message.delivery_mode
              );
              if (isDuplicate) return prev;
              return [
                {
                  ...message.event,
                  delivery_mode: message.delivery_mode,
                  replay_id: message.replay_id,
                  received_at: new Date().toISOString(),
                  kafka_details: message.details,
                },
                ...prev,
              ];
            });
          } else if (message.message_type === "replay_status") {
            setReplayStatus({
              status: message.details?.status || "UPDATED",
              replayId: message.replay_id,
              count: message.details?.replayed_count || 0,
              message: message.details?.message || "",
            });
          } else if (message.message_type === "system_metrics" && message.metrics) {
            setTelemetry(message.metrics);
          } else if (message.message_type === "system_status") {
            setSystemInfo(message.details);
            if (message.metrics) {
              setTelemetry(message.metrics);
            }
          }
        } catch (err) {
          console.error("Failed to parse WebSocket message:", err);
        }
      };

      ws.onerror = (err) => {
        console.warn("WebSocket encounter:", err);
      };

      ws.onclose = (event) => {
        if (!isMountedRef.current || wsRef.current !== ws) return;
        setStatus("DISCONNECTED");
        wsRef.current = null;

        // Schedule bounded reconnect (min 1s, max 5s)
        if (!reconnectTimeoutRef.current) {
          const nextDelay = Math.min(backoffRef.current, 5000);
          backoffRef.current = Math.min(backoffRef.current * 1.5, 5000);
          reconnectTimeoutRef.current = setTimeout(() => {
            reconnectTimeoutRef.current = null;
            if (isMountedRef.current) {
              setStatus("RECONNECTING");
              connect();
            }
          }, nextDelay);
        }
      };
    } catch (e) {
      console.warn("Error establishing WebSocket:", e);
      setStatus("DISCONNECTED");
      wsRef.current = null;
    }
  }, [syncRecentEvents]);

  useEffect(() => {
    isMountedRef.current = true;
    connect();

    return () => {
      isMountedRef.current = false;
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
      if (wsRef.current) {
        const socket = wsRef.current;
        wsRef.current = null;
        socket.onopen = null;
        socket.onmessage = null;
        socket.onerror = null;
        socket.onclose = null;
        try {
          socket.close();
        } catch {
          // ignore
        }
      }
    };
  }, []);


  const sendJson = useCallback((data) => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify(data));
      return true;
    }
    return false;
  }, []);

  // Periodic heartbeat ping to keep connection actively open
  useEffect(() => {
    const pingTimer = setInterval(() => {
      if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
        try {
          wsRef.current.send(JSON.stringify({ action: "ping" }));
        } catch {
          // ignore transient send failures
        }
      }
    }, 10000);
    return () => clearInterval(pingTimer);
  }, []);

  const clearEvents = useCallback(() => {
    // Only clears local state, Kafka records are preserved
    setEvents([]);
  }, []);

  const manualReconnect = useCallback(() => {
    if (wsRef.current) {
      const socket = wsRef.current;
      wsRef.current = null;
      socket.onopen = null;
      socket.onmessage = null;
      socket.onerror = null;
      socket.onclose = null;
      socket.close();
    }
    if (reconnectTimeoutRef.current) clearTimeout(reconnectTimeoutRef.current);
    backoffRef.current = 1000;
    connect();
  }, [connect]);

  // Periodic polling fallback for telemetry metrics (ensures numbers are live even before WebSocket connects)
  useEffect(() => {
    let isMounted = true;
    const fetchMetrics = async () => {
      try {
        const res = await fetchSystemMetrics();
        if (isMounted && res && res.metrics) {
          setTelemetry((prev) => ({ ...prev, ...res.metrics }));
        }
      } catch (err) {
        // Silently ignore if offline
      }
    };

    fetchMetrics();
    const interval = setInterval(fetchMetrics, 3000);
    return () => {
      isMounted = false;
      clearInterval(interval);
    };
  }, []);

  return {
    status,
    events,
    replayStatus,
    systemInfo,
    telemetry,
    clientId: clientId || clientIdRef.current || "client-synced",
    sendJson,
    clearEvents,
    reconnect: manualReconnect,
  };
}

