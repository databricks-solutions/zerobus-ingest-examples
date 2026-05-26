import React from "react";
import { SensorReading, SensorConfig, getAnomalyStatus, AnomalyStatus } from "../types";
import { MachineConfig } from "../types";
import { AlertTriangle, AlertCircle } from "lucide-react";

interface EventFeedProps {
  events: SensorReading[];
  machines: Record<string, MachineConfig>;
}

const anomalyBadge: Record<AnomalyStatus, { icon: React.ReactNode; cls: string } | null> = {
  NORMAL: null,
  WARNING: {
    icon: <AlertTriangle className="w-3 h-3" />,
    cls: "text-amber-400",
  },
  CRITICAL: {
    icon: <AlertCircle className="w-3 h-3" />,
    cls: "text-red-400",
  },
};

const SENSOR_SHORT: Record<string, string> = {
  temperature_c: "TEMP",
  vibration_mm_s: "VIBR",
  spindle_rpm: "RPM",
  pressure_bar: "PRES",
  cycle_count: "CYCL",
  speed_m_min: "SPEE",
  load_weight_kg: "LOAD",
  motor_current_a: "CURR",
};

export default function EventFeed({ events, machines }: EventFeedProps) {
  return (
    <div className="bg-factory-card rounded-xl border border-factory-border p-4 flex flex-col h-full">
      <h2 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">
        Live Event Feed
      </h2>
      <div className="flex-1 overflow-y-auto space-y-1 min-h-0">
        {events.length === 0 && (
          <p className="text-gray-600 text-xs text-center py-8">
            Waiting for sensor data...
          </p>
        )}
        {events.map((event, i) => {
          const machineConfig = machines[event.machine_id];
          const sensorConfig = machineConfig?.sensors?.[event.sensor_name];
          const status = sensorConfig
            ? getAnomalyStatus(event.value, sensorConfig)
            : "NORMAL";
          const badge = anomalyBadge[status];
          const time = new Date(event.timestamp).toLocaleTimeString();

          return (
            <div
              key={`${event.timestamp}-${event.machine_id}-${event.sensor_name}-${i}`}
              className={`flex items-center gap-2 text-xs py-1.5 px-2 rounded transition-colors ${
                status === "CRITICAL"
                  ? "bg-red-500/10"
                  : status === "WARNING"
                  ? "bg-amber-500/5"
                  : "hover:bg-white/5"
              }`}
            >
              <span className="text-gray-600 font-mono w-16 shrink-0">
                {time}
              </span>
              <span className="text-gray-400 w-10 shrink-0 font-mono">
                {SENSOR_SHORT[event.sensor_name] || event.sensor_name.slice(0, 4).toUpperCase()}
              </span>
              <span className="text-gray-300 truncate flex-1">
                {machineConfig?.display_name || event.machine_id}
              </span>
              <span className="text-gray-200 font-mono w-16 text-right shrink-0">
                {event.value.toFixed(1)}
              </span>
              <span className="text-gray-500 w-10 shrink-0">
                {event.unit}
              </span>
              {badge && (
                <span className={`shrink-0 ${badge.cls}`}>{badge.icon}</span>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
