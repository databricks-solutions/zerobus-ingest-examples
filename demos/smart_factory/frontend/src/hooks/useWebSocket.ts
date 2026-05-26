import { useEffect, useRef, useState, useCallback } from "react";
import { WSMessage, MachineConfig, SensorReading } from "../types";

interface UseWebSocketReturn {
  machines: Record<string, MachineConfig>;
  latestReadings: Record<string, Record<string, SensorReading>>;
  faultStates: Record<string, boolean>;
  eventLog: SensorReading[];
  totalEvents: number;
  eventsPerSec: number;
  connectionStatus: "connecting" | "connected" | "disconnected";
  injectFault: (machineId: string) => void;
  clearFault: (machineId: string) => void;
  clearAllFaults: () => void;
}

const MAX_LOG_SIZE = 50;

export function useWebSocket(): UseWebSocketReturn {
  const [machines, setMachines] = useState<Record<string, MachineConfig>>({});
  const [latestReadings, setLatestReadings] = useState<
    Record<string, Record<string, SensorReading>>
  >({});
  const [faultStates, setFaultStates] = useState<Record<string, boolean>>({});
  const [eventLog, setEventLog] = useState<SensorReading[]>([]);
  const [totalEvents, setTotalEvents] = useState(0);
  const [eventsPerSec, setEventsPerSec] = useState(0);
  const recentCountRef = useRef(0);
  const [connectionStatus, setConnectionStatus] = useState<
    "connecting" | "connected" | "disconnected"
  >("connecting");
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeout = useRef<number>(0);

  const connect = useCallback(() => {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
    wsRef.current = ws;
    setConnectionStatus("connecting");

    ws.onopen = () => {
      setConnectionStatus("connected");
      reconnectTimeout.current = 0;
    };

    ws.onmessage = (event) => {
      const msg: WSMessage = JSON.parse(event.data);

      if (msg.type === "init" && msg.machines) {
        setMachines(msg.machines);
      }

      if (msg.fault_states) {
        setFaultStates(msg.fault_states);
      }

      if (msg.events) {
        // Track event counts
        setTotalEvents((prev) => prev + msg.events!.length);
        recentCountRef.current += msg.events!.length;

        // Update latest readings per machine+sensor
        setLatestReadings((prev) => {
          const next = { ...prev };
          for (const reading of msg.events!) {
            if (!next[reading.machine_id]) {
              next[reading.machine_id] = {};
            }
            next[reading.machine_id] = {
              ...next[reading.machine_id],
              [reading.sensor_name]: reading,
            };
          }
          return next;
        });

        // Append to event log (keep last N)
        setEventLog((prev) => {
          const updated = [...msg.events!, ...prev];
          return updated.slice(0, MAX_LOG_SIZE);
        });
      }
    };

    ws.onclose = () => {
      setConnectionStatus("disconnected");
      // Exponential backoff reconnect
      const delay = Math.min(1000 * Math.pow(2, reconnectTimeout.current), 10000);
      reconnectTimeout.current++;
      setTimeout(connect, delay);
    };

    ws.onerror = () => {
      ws.close();
    };
  }, []);

  useEffect(() => {
    connect();
    // Calculate events/sec every second
    const epsInterval = setInterval(() => {
      setEventsPerSec(recentCountRef.current);
      recentCountRef.current = 0;
    }, 1000);
    return () => {
      wsRef.current?.close();
      clearInterval(epsInterval);
    };
  }, [connect]);

  const sendAction = useCallback((action: string, machineId?: string) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(
        JSON.stringify({ action, machine_id: machineId })
      );
    }
  }, []);

  return {
    machines,
    latestReadings,
    faultStates,
    eventLog,
    totalEvents,
    eventsPerSec,
    connectionStatus,
    injectFault: (id) => sendAction("inject_fault", id),
    clearFault: (id) => sendAction("clear_fault", id),
    clearAllFaults: () => sendAction("clear_all"),
  };
}
