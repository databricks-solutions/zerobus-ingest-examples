import React, { useMemo } from "react";
import { SensorConfig, getAnomalyStatus } from "../types";

interface SensorGaugeProps {
  name: string;
  value: number | undefined;
  config: SensorConfig;
}

const SENSOR_LABELS: Record<string, string> = {
  temperature_c: "Temperature",
  vibration_mm_s: "Vibration",
  spindle_rpm: "Spindle RPM",
  pressure_bar: "Pressure",
  cycle_count: "Cycle Rate",
  speed_m_min: "Belt Speed",
  load_weight_kg: "Load Weight",
  motor_current_a: "Motor Current",
};

export default function SensorGauge({ name, value, config }: SensorGaugeProps) {
  const displayValue = value ?? config.min;
  const status = value !== undefined ? getAnomalyStatus(displayValue, config) : "NORMAL";

  const statusColors = {
    NORMAL: { stroke: "#10b981", text: "text-emerald-400", bg: "bg-emerald-500/10" },
    WARNING: { stroke: "#f59e0b", text: "text-amber-400", bg: "bg-amber-500/10" },
    CRITICAL: { stroke: "#ef4444", text: "text-red-400", bg: "bg-red-500/10" },
  };

  const colors = statusColors[status];

  // SVG arc gauge
  const radius = 40;
  const strokeWidth = 6;
  const cx = 50;
  const cy = 55;
  const startAngle = -225;
  const endAngle = 45;
  const totalAngle = endAngle - startAngle;

  const pct = useMemo(() => {
    const range = config.max - config.min;
    if (range === 0) return 0;
    return Math.max(0, Math.min(1, (displayValue - config.min) / range));
  }, [displayValue, config.min, config.max]);

  const valueAngle = startAngle + pct * totalAngle;

  function polarToCartesian(angle: number) {
    const rad = (angle * Math.PI) / 180;
    return {
      x: cx + radius * Math.cos(rad),
      y: cy + radius * Math.sin(rad),
    };
  }

  function describeArc(start: number, end: number) {
    const s = polarToCartesian(start);
    const e = polarToCartesian(end);
    const largeArc = end - start > 180 ? 1 : 0;
    return `M ${s.x} ${s.y} A ${radius} ${radius} 0 ${largeArc} 1 ${e.x} ${e.y}`;
  }

  const bgArc = describeArc(startAngle, endAngle);
  const valueArc =
    pct > 0.01 ? describeArc(startAngle, valueAngle) : "";

  return (
    <div className={`flex flex-col items-center p-3 rounded-lg ${colors.bg} transition-all duration-500`}>
      <svg viewBox="0 0 100 75" className="w-24 h-[4.5rem]">
        {/* Background arc */}
        <path
          d={bgArc}
          fill="none"
          stroke="#374151"
          strokeWidth={strokeWidth}
          strokeLinecap="round"
        />
        {/* Value arc */}
        {valueArc && (
          <path
            d={valueArc}
            fill="none"
            stroke={colors.stroke}
            strokeWidth={strokeWidth}
            strokeLinecap="round"
            className="transition-all duration-700 ease-out"
          />
        )}
        {/* Center value */}
        <text
          x={cx}
          y={cy - 5}
          textAnchor="middle"
          fill={colors.stroke}
          fontSize="14"
          fontWeight="bold"
          fontFamily="monospace"
        >
          {displayValue.toFixed(config.max > 1000 ? 0 : 1)}
        </text>
        <text
          x={cx}
          y={cy + 8}
          textAnchor="middle"
          fill="#9ca3af"
          fontSize="8"
        >
          {config.unit}
        </text>
      </svg>
      <span className="text-xs text-gray-400 mt-1">
        {SENSOR_LABELS[name] || name}
      </span>
    </div>
  );
}
