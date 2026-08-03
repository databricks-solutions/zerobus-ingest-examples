import React from "react";
import { MachineConfig, SensorReading, getAnomalyStatus, AnomalyStatus } from "../types";
import SensorGauge from "./SensorGauge";
import { Cog, ArrowDownUp, Gauge } from "lucide-react";

interface MachineCardProps {
  machineId: string;
  config: MachineConfig;
  readings: Record<string, SensorReading> | undefined;
  isFaulting: boolean;
}

const MACHINE_ICONS: Record<string, React.ReactNode> = {
  cnc_mill: <Cog className="w-5 h-5" />,
  hydraulic_press: <ArrowDownUp className="w-5 h-5" />,
  conveyor_belt: <Gauge className="w-5 h-5" />,
};

function getMachineStatus(
  config: MachineConfig,
  readings: Record<string, SensorReading> | undefined
): AnomalyStatus {
  if (!readings) return "NORMAL";
  let worst: AnomalyStatus = "NORMAL";
  for (const [sensorName, sensorCfg] of Object.entries(config.sensors)) {
    const reading = readings[sensorName];
    if (!reading) continue;
    const status = getAnomalyStatus(reading.value, sensorCfg);
    if (status === "CRITICAL") return "CRITICAL";
    if (status === "WARNING") worst = "WARNING";
  }
  return worst;
}

const glowClasses: Record<AnomalyStatus, string> = {
  NORMAL: "animate-glow-green border-emerald-500/40",
  WARNING: "animate-glow-amber border-amber-500/50",
  CRITICAL: "animate-glow-red border-red-500/60",
};

const statusBadge: Record<AnomalyStatus, { label: string; cls: string }> = {
  NORMAL: { label: "Healthy", cls: "bg-emerald-500/20 text-emerald-400" },
  WARNING: { label: "Warning", cls: "bg-amber-500/20 text-amber-400" },
  CRITICAL: { label: "Critical", cls: "bg-red-500/20 text-red-400" },
};

export default function MachineCard({
  machineId,
  config,
  readings,
  isFaulting,
}: MachineCardProps) {
  const status = getMachineStatus(config, readings);
  const glow = glowClasses[status];
  const badge = statusBadge[status];

  return (
    <div
      className={`bg-factory-card rounded-xl border ${glow} p-5 transition-all duration-500`}
    >
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <span className="text-gray-400">{MACHINE_ICONS[config.type]}</span>
          <h3 className="text-sm font-semibold text-gray-200">
            {config.display_name}
          </h3>
        </div>
        <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${badge.cls}`}>
          {badge.label}
        </span>
      </div>

      {/* Sensor Gauges */}
      <div className="grid grid-cols-3 gap-2">
        {Object.entries(config.sensors).map(([sensorName, sensorCfg]) => (
          <SensorGauge
            key={sensorName}
            name={sensorName}
            value={readings?.[sensorName]?.value}
            config={sensorCfg}
          />
        ))}
      </div>

      {/* Machine ID */}
      <div className="mt-3 text-center">
        <span className="text-[10px] text-gray-500 font-mono">{machineId}</span>
      </div>
    </div>
  );
}
