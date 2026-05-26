import React from "react";
import { MachineConfig, SensorReading, getAnomalyStatus, AnomalyStatus } from "../types";
import { Radio, Shield, BrainCircuit } from "lucide-react";

interface FactoryFloorProps {
  machines: Record<string, MachineConfig>;
  readings: Record<string, Record<string, SensorReading>>;
  faultStates: Record<string, boolean>;
  totalEvents: number;
  eventsPerSec: number;
}

function getMachineOverallStatus(
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

const statusColors: Record<AnomalyStatus, { fill: string; glow: string; ring: string; bg: string }> = {
  NORMAL: { fill: "#10b981", glow: "#10b98133", ring: "#10b98177", bg: "#10b98110" },
  WARNING: { fill: "#f59e0b", glow: "#f59e0b44", ring: "#f59e0b88", bg: "#f59e0b15" },
  CRITICAL: { fill: "#ef4444", glow: "#ef444455", ring: "#ef4444aa", bg: "#ef444418" },
};

const machineOrder = ["CNC_Mill_01", "Hydraulic_Press_01", "Conveyor_Belt_01"];

// Bigger, more detailed machine icons
const machineIcons: Record<string, React.ReactNode> = {
  cnc_mill: (
    <g>
      <rect x="-16" y="-20" width="32" height="40" rx="3" fill="none" stroke="currentColor" strokeWidth="1.5" />
      <rect x="-10" y="-14" width="20" height="16" rx="1" fill="none" stroke="currentColor" strokeWidth="1" opacity="0.6" />
      <circle cx="0" cy="-6" r="4" fill="none" stroke="currentColor" strokeWidth="1" />
      <line x1="0" y1="-10" x2="0" y2="-2" stroke="currentColor" strokeWidth="1" />
      <line x1="-4" y1="-6" x2="4" y2="-6" stroke="currentColor" strokeWidth="1" />
      <rect x="-8" y="6" width="16" height="6" rx="1" fill="none" stroke="currentColor" strokeWidth="1" opacity="0.5" />
      <circle cx="-4" cy="16" r="2" fill="none" stroke="currentColor" strokeWidth="0.8" opacity="0.4" />
      <circle cx="4" cy="16" r="2" fill="none" stroke="currentColor" strokeWidth="0.8" opacity="0.4" />
    </g>
  ),
  hydraulic_press: (
    <g>
      <rect x="-18" y="-8" width="36" height="28" rx="2" fill="none" stroke="currentColor" strokeWidth="1.5" />
      <rect x="-8" y="-22" width="16" height="14" rx="2" fill="none" stroke="currentColor" strokeWidth="1.5" />
      <line x1="-4" y1="-8" x2="-4" y2="0" stroke="currentColor" strokeWidth="2" />
      <line x1="4" y1="-8" x2="4" y2="0" stroke="currentColor" strokeWidth="2" />
      <rect x="-12" y="2" width="24" height="4" rx="1" fill="currentColor" opacity="0.2" />
      <rect x="-14" y="10" width="28" height="6" rx="1" fill="none" stroke="currentColor" strokeWidth="1" opacity="0.5" />
      <circle cx="-8" cy="13" r="1.5" fill="currentColor" opacity="0.3" />
      <circle cx="0" cy="13" r="1.5" fill="currentColor" opacity="0.3" />
      <circle cx="8" cy="13" r="1.5" fill="currentColor" opacity="0.3" />
    </g>
  ),
  conveyor_belt: (
    <g>
      <rect x="-24" y="-10" width="48" height="20" rx="10" fill="none" stroke="currentColor" strokeWidth="1.5" />
      <circle cx="-14" cy="0" r="6" fill="none" stroke="currentColor" strokeWidth="1" />
      <circle cx="14" cy="0" r="6" fill="none" stroke="currentColor" strokeWidth="1" />
      <circle cx="-14" cy="0" r="2" fill="currentColor" opacity="0.3" />
      <circle cx="14" cy="0" r="2" fill="currentColor" opacity="0.3" />
      <line x1="-8" y1="-10" x2="-8" y2="10" stroke="currentColor" strokeWidth="0.5" opacity="0.3" />
      <line x1="0" y1="-10" x2="0" y2="10" stroke="currentColor" strokeWidth="0.5" opacity="0.3" />
      <line x1="8" y1="-10" x2="8" y2="10" stroke="currentColor" strokeWidth="0.5" opacity="0.3" />
      {/* Packages on belt */}
      <rect x="-5" y="-16" width="10" height="6" rx="1" fill="none" stroke="currentColor" strokeWidth="0.8" opacity="0.5" />
    </g>
  ),
};

const SENSOR_SHORT: Record<string, string> = {
  temperature_c: "TEMP",
  vibration_mm_s: "VIBR",
  spindle_rpm: "RPM",
  pressure_bar: "PRES",
  cycle_count: "CYCL",
  speed_m_min: "SPEED",
  load_weight_kg: "LOAD",
  motor_current_a: "CURR",
};

export default function FactoryFloor({ machines, readings, faultStates, totalEvents, eventsPerSec }: FactoryFloorProps) {
  const ids = machineOrder.filter((id) => machines[id]);

  return (
    <div className="bg-factory-card rounded-xl border border-factory-border p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-3">
          <h2 className="text-sm font-bold text-gray-300 uppercase tracking-wider">
            Factory Floor Simulation
          </h2>
          <span className="text-xs text-amber-400/80 bg-amber-500/10 border border-amber-500/20 px-2.5 py-1 rounded-full font-medium">
            Simulated IoT Devices
          </span>
        </div>
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2 text-sm text-emerald-500 font-medium">
            <Radio className="w-4 h-4" />
            <span>Sensors streaming via ZeroBus</span>
          </div>
          <div className="flex items-center gap-3 text-xs">
            <span className="text-blue-400 font-mono font-bold">{eventsPerSec}<span className="text-gray-500 font-normal"> evt/s</span></span>
            <span className="text-gray-600">|</span>
            <span className="text-gray-400 font-mono">{totalEvents.toLocaleString()}<span className="text-gray-500"> total</span></span>
          </div>
        </div>
      </div>

      {/* Factory visualization */}
      <div className="grid grid-cols-3 gap-6 mt-4">
        {ids.map((machineId, i) => {
          const config = machines[machineId];
          const machineReadings = readings[machineId];
          const status = getMachineOverallStatus(config, machineReadings);
          const colors = statusColors[status];
          const isFaulting = faultStates[machineId] || false;

          return (
            <div key={machineId} className="flex flex-col items-center">
              {/* Machine visual */}
              <div className="relative">
                {/* Outer glow */}
                <svg width="110" height="110" viewBox="-70 -70 140 140">
                  {/* Pulsing background */}
                  <circle r="60" fill={colors.bg}>
                    {status !== "NORMAL" && (
                      <animate
                        attributeName="r"
                        values="56;64;56"
                        dur={status === "CRITICAL" ? "0.8s" : "2s"}
                        repeatCount="indefinite"
                      />
                    )}
                  </circle>

                  {/* Ring */}
                  <circle
                    r="48"
                    fill="#111827"
                    stroke={colors.ring}
                    strokeWidth="2"
                  />

                  {/* Sensor dots around the ring */}
                  {Object.entries(config.sensors).map(([sName, sCfg], si) => {
                    const angle = -90 + si * 120;
                    const rad = (angle * Math.PI) / 180;
                    const cx = Math.cos(rad) * 48;
                    const cy = Math.sin(rad) * 48;
                    const reading = machineReadings?.[sName];
                    const sStatus = reading ? getAnomalyStatus(reading.value, sCfg) : "NORMAL";
                    const sColor = statusColors[sStatus].fill;
                    return (
                      <g key={sName}>
                        <circle cx={cx} cy={cy} r="5" fill="#111827" stroke={sColor} strokeWidth="1.5" />
                        <circle cx={cx} cy={cy} r="2" fill={sColor}>
                          {sStatus !== "NORMAL" && (
                            <animate attributeName="opacity" values="1;0.3;1" dur="0.8s" repeatCount="indefinite" />
                          )}
                        </circle>
                      </g>
                    );
                  })}

                  {/* Machine icon */}
                  <g style={{ color: colors.fill }}>
                    {machineIcons[config.type]}
                  </g>
                </svg>

                {/* Fault indicator */}
                {isFaulting && (
                  <div className="absolute -top-2 -right-2 w-7 h-7 bg-red-500/90 rounded-full flex items-center justify-center animate-bounce">
                    <span className="text-sm">🔥</span>
                  </div>
                )}
              </div>

              {/* Machine name */}
              <h3 className="text-sm font-semibold text-gray-200 mt-2">
                {config.display_name}
              </h3>

              {/* Status badge */}
              <span
                className="text-[10px] font-semibold mt-0.5"
                style={{ color: colors.fill }}
              >
                {status}
              </span>

              {/* Live sensor readouts */}
              <div className="mt-3 w-full space-y-1">
                {Object.entries(config.sensors).map(([sName, sCfg]) => {
                  const reading = machineReadings?.[sName];
                  const val = reading?.value;
                  const sStatus = val !== undefined ? getAnomalyStatus(val, sCfg) : "NORMAL";
                  const sColor = statusColors[sStatus].fill;
                  return (
                    <div
                      key={sName}
                      className="flex items-center justify-between text-[11px] px-2 py-1 rounded"
                      style={{ backgroundColor: sStatus !== "NORMAL" ? statusColors[sStatus].bg : "transparent" }}
                    >
                      <span className="text-gray-500 font-mono">{SENSOR_SHORT[sName]}</span>
                      <span className="font-mono font-medium" style={{ color: sColor }}>
                        {val !== undefined ? val.toFixed(1) : "—"}{" "}
                        <span className="text-gray-600 text-[9px]">{sCfg.unit}</span>
                      </span>
                    </div>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>

      {/* Pipeline + UC banner */}
      <div className="mt-5 pt-5 border-t border-factory-border flex items-center justify-between">
        <div className="flex items-center gap-4 text-sm">
          <PipelineNode label="Factory Floor" sublabel="IoT Sensors" color="text-gray-300" />
          <div className="flex flex-col items-center">
            <span className="text-blue-500 text-lg">&rarr;</span>
          </div>
          <PipelineNode label="ZeroBus" sublabel="Direct to Delta" color="text-blue-400" />
          <span className="text-gray-600 text-lg">&rarr;</span>
          <PipelineNode label="Bronze" sublabel="Validated" color="text-orange-400" />
          <span className="text-gray-600 text-lg">&rarr;</span>
          <div className="text-center">
            <div className="flex items-center gap-1.5">
              <span className="font-semibold text-gray-200">Silver</span>
              <span className="flex items-center gap-1 text-xs text-purple-400 bg-purple-500/10 px-2 py-0.5 rounded-full font-medium">
                <BrainCircuit className="w-3.5 h-3.5" />ML
              </span>
            </div>
            <span className="text-[11px] text-gray-500">Anomaly Scored</span>
          </div>
          <span className="text-gray-600 text-lg">&rarr;</span>
          <PipelineNode label="Gold" sublabel="Health KPIs" color="text-yellow-400" />
          <span className="text-gray-600 text-lg">&rarr;</span>
          <PipelineNode label="Dashboard" sublabel="Live" color="text-emerald-400" />
        </div>
        <div className="flex items-center gap-3 bg-gradient-to-r from-blue-500/10 to-emerald-500/10 border border-blue-500/20 rounded-lg px-5 py-3">
          <Shield className="w-6 h-6 text-blue-400 shrink-0" />
          <div>
            <p className="text-sm font-semibold text-gray-100">Governed by Unity Catalog</p>
            <p className="text-[11px] text-gray-400">End-to-end lineage, access control & audit</p>
          </div>
        </div>
      </div>
    </div>
  );
}

function PipelineNode({ label, sublabel, color }: { label: string; sublabel: string; color: string }) {
  return (
    <div className="text-center">
      <div className={`font-semibold ${color}`}>{label}</div>
      <span className="text-[11px] text-gray-500">{sublabel}</span>
    </div>
  );
}

