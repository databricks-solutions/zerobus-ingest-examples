import React, { useEffect, useState, useCallback } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  LineChart, Line, Legend, Cell,
} from "recharts";
import { RefreshCw, AlertTriangle, Activity, Database, BrainCircuit } from "lucide-react";
import PipelineBanner from "./PipelineBanner";

interface HealthRow {
  machine_id: string;
  machine_type: string;
  avg_health_score: string;
  total_criticals: string;
  total_warnings: string;
  worst_sensor_health: string;
  last_activity: string;
}

interface KpiRow {
  machine_id: string;
  sensor_name: string;
  avg_value: string;
  max_value: string;
  min_value: string;
  health_score: string;
  critical_count: string;
  warning_count: string;
  total_readings: string;
}

interface AnomalyRow {
  machine_id: string;
  sensor_name: string;
  value: string;
  anomaly_status: string;
  timestamp: string;
  unit: string;
}

interface TrendRow {
  machine_id: string;
  sensor_name: string;
  value: string;
  anomaly_status: string;
  timestamp: string;
}

const STATUS_COLORS: Record<string, string> = {
  CRITICAL: "#ef4444",
  WARNING: "#f59e0b",
  NORMAL: "#10b981",
};

const MACHINE_COLORS = ["#3b82f6", "#8b5cf6", "#06b6d4"];

export default function DashboardView({ totalEvents }: { totalEvents?: number }) {
  const [health, setHealth] = useState<HealthRow[]>([]);
  const [kpis, setKpis] = useState<KpiRow[]>([]);
  const [anomalies, setAnomalies] = useState<AnomalyRow[]>([]);
  const [trends, setTrends] = useState<TrendRow[]>([]);
  const [landingCount, setLandingCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [hRes, kRes, aRes] = await Promise.all([
        fetch("/api/dashboard/health"),
        fetch("/api/dashboard/kpis"),
        fetch("/api/dashboard/anomalies"),
      ]);
      const newHealth = await hRes.json();
      const newKpis = await kRes.json();
      const newAnomalies = await aRes.json();
      if (Array.isArray(newHealth) && newHealth.length > 0) setHealth(newHealth);
      if (Array.isArray(newKpis) && newKpis.length > 0) setKpis(newKpis);
      if (Array.isArray(newAnomalies)) setAnomalies(newAnomalies);
      setLastRefresh(new Date());
    } catch (e) {
      console.error("Dashboard fetch error:", e);
    }
    setLoading(false);
  }, [landingCount]);

  const fetchTrends = useCallback(async () => {
    try {
      const res = await fetch("/api/dashboard/trends");
      const data = await res.json();
      if (Array.isArray(data) && data.length > 0) setTrends(data);
    } catch {}
  }, []);

  useEffect(() => {
    fetchData();
    const mainInterval = setInterval(fetchData, 5000);
    const trendsInterval = setInterval(fetchTrends, 3000);
    return () => { clearInterval(mainInterval); clearInterval(trendsInterval); };
  }, [fetchData, fetchTrends]);

  // Health bar chart data
  const healthChartData = health.map((h) => ({
    machine: h.machine_id.replace("_01", "").replace(/_/g, " "),
    "Health Score": Number(h.avg_health_score),
    Criticals: Number(h.total_criticals),
    Warnings: Number(h.total_warnings),
  }));

  // Sensor unit lookup
  const SENSOR_UNITS: Record<string, string> = {
    temperature_c: "°C", vibration_mm_s: "mm/s", spindle_rpm: "RPM",
    pressure_bar: "bar", cycle_count: "cpm",
    speed_m_min: "m/min", load_weight_kg: "kg", motor_current_a: "A",
  };

  // KPI data for sensor breakdown
  const kpiChartData = kpis.map((k) => ({
    name: `${k.machine_id.split("_")[0]} / ${k.sensor_name}`,
    machine: k.machine_id,
    sensor: k.sensor_name,
    unit: SENSOR_UNITS[k.sensor_name] || "",
    avg: Number(k.avg_value),
    max: Number(k.max_value),
    min: Number(k.min_value),
    health: Number(k.health_score),
    criticals: Number(k.critical_count),
    warnings: Number(k.warning_count),
    readings: Number(k.total_readings),
  }));

  return (
    <div className="space-y-5">
      {/* Compact stats bar */}
      <div className="bg-factory-card rounded-xl border border-factory-border px-5 py-3 flex items-center justify-between">
        <div className="flex items-center gap-6">
          <MiniStat icon={<Database className="w-3.5 h-3.5" />} label="Events" value={(totalEvents ?? landingCount).toLocaleString()} color="text-blue-400" />
          <MiniStat icon={<Activity className="w-3.5 h-3.5" />} label="Machines" value={String(health.length)} color="text-emerald-400" />
          <MiniStat icon={<BrainCircuit className="w-3.5 h-3.5" />} label="Anomalies" value={String(anomalies.length)} color="text-purple-400" />
        </div>
        <div className="flex items-center gap-3">
          <span className="text-[10px] text-gray-500">
            {lastRefresh ? lastRefresh.toLocaleTimeString() : "—"}
          </span>
          <button
            onClick={fetchData}
            disabled={loading}
            className={`p-1.5 rounded-lg transition-all ${
              loading ? "bg-gray-800 text-gray-600" : "bg-blue-500/20 text-blue-400 hover:bg-blue-500/30"
            }`}
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? "animate-spin" : ""}`} />
          </button>
        </div>
      </div>

      {/* Sensor Trends — full width, top position */}
      <div className="bg-factory-card rounded-xl border border-factory-border p-5">
        <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-4">
          Sensor Trends — Live
        </h3>
        <ResponsiveContainer width="100%" height={240}>
          <LineChart
            data={(() => {
              const byTime: Record<string, Record<string, number>> = {};
              [...trends].reverse().forEach((t) => {
                const time = new Date(t.timestamp).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
                const key = `${t.machine_id.split("_")[0]}-${t.sensor_name}`;
                if (!byTime[time]) byTime[time] = {};
                byTime[time][key] = Number(t.value);
              });
              return Object.entries(byTime).map(([time, vals]) => ({ time, ...vals }));
            })()}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
            <XAxis dataKey="time" tick={{ fill: "#9ca3af", fontSize: 9 }} interval="preserveStartEnd" />
            <YAxis tick={{ fill: "#9ca3af", fontSize: 10 }} />
            <Tooltip
              contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: "8px", fontSize: 11 }}
              labelStyle={{ color: "#d1d5db" }}
            />
            <Legend wrapperStyle={{ fontSize: 10 }} />
            <Line type="monotone" dataKey="CNC-temperature_c" name="CNC Temp" stroke="#ef4444" dot={false} strokeWidth={1.5} isAnimationActive={false} />
            <Line type="monotone" dataKey="CNC-vibration_mm_s" name="CNC Vibr" stroke="#f59e0b" dot={false} strokeWidth={1.5} isAnimationActive={false} />
            <Line type="monotone" dataKey="Hydraulic-pressure_bar" name="Press Bar" stroke="#8b5cf6" dot={false} strokeWidth={1.5} isAnimationActive={false} />
            <Line type="monotone" dataKey="Conveyor-speed_m_min" name="Belt Speed" stroke="#06b6d4" dot={false} strokeWidth={1.5} isAnimationActive={false} />
            <Line type="monotone" dataKey="Conveyor-motor_current_a" name="Motor Curr" stroke="#10b981" dot={false} strokeWidth={1.5} isAnimationActive={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Two-column layout: Health (left) | Anomalies (right) */}
      <div className="grid grid-cols-2 gap-6">
        {/* LEFT: Health */}
        <div className="space-y-6">
          {/* Health Scores Chart */}
          <div className="bg-factory-card rounded-xl border border-factory-border p-5 h-[280px]">
            <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-4">
              Machine Health Scores
            </h3>
            <ResponsiveContainer width="100%" height={180}>
              <BarChart data={healthChartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <XAxis dataKey="machine" tick={{ fill: "#9ca3af", fontSize: 11 }} />
                <YAxis domain={[0, 100]} tick={{ fill: "#9ca3af", fontSize: 11 }} />
                <Tooltip
                  contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: "8px" }}
                  labelStyle={{ color: "#d1d5db" }}
                />
                <Bar dataKey="Health Score" radius={[4, 4, 0, 0]}>
                  {healthChartData.map((entry, i) => (
                    <Cell
                      key={i}
                      fill={
                        entry["Health Score"] > 70
                          ? "#10b981"
                          : entry["Health Score"] > 40
                          ? "#f59e0b"
                          : "#ef4444"
                      }
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Sensor Health Table (condensed) */}
          <div className="bg-factory-card rounded-xl border border-factory-border p-5 h-[340px] overflow-y-auto">
            <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-3">
              Sensor Breakdown
            </h3>
            <table className="w-full text-xs">
              <thead>
                <tr className="text-gray-500 border-b border-factory-border">
                  <th className="text-left py-1.5 px-2">Machine</th>
                  <th className="text-left py-1.5 px-2">Sensor</th>
                  <th className="text-right py-1.5 px-2">Avg</th>
                  <th className="text-right py-1.5 px-2">Max</th>
                  <th className="text-right py-1.5 px-2">Health</th>
                </tr>
              </thead>
              <tbody>
                {kpiChartData.map((row, i) => (
                  <tr key={i} className="border-b border-factory-border/50 hover:bg-white/5">
                    <td className="py-1.5 px-2 text-gray-300">{row.machine.replace("_01", "")}</td>
                    <td className="py-1.5 px-2 text-gray-400 font-mono">{row.sensor}</td>
                    <td className="py-1.5 px-2 text-right text-gray-300 font-mono">{row.avg} <span className="text-gray-600 text-[9px]">{row.unit}</span></td>
                    <td className="py-1.5 px-2 text-right text-gray-400 font-mono">{row.max} <span className="text-gray-600 text-[9px]">{row.unit}</span></td>
                    <td className="py-1.5 px-2 text-right">
                      <span className={`font-semibold ${
                        row.health > 70 ? "text-emerald-400" : row.health > 40 ? "text-amber-400" : "text-red-400"
                      }`}>
                        {row.health}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* RIGHT: Anomalies */}
        <div className="space-y-6">
          {/* Anomaly Counts Chart */}
          <div className="bg-factory-card rounded-xl border border-factory-border p-5 h-[280px]">
            <div className="flex items-center gap-2 mb-4">
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
                Anomaly Counts
              </h3>
              <span className="flex items-center gap-1.5 text-xs text-purple-400 bg-purple-500/10 px-2 py-0.5 rounded-full font-medium">
                <BrainCircuit className="w-3.5 h-3.5" />
                ML in SDP
              </span>
            </div>
            <ResponsiveContainer width="100%" height={180}>
              <BarChart data={healthChartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                <XAxis dataKey="machine" tick={{ fill: "#9ca3af", fontSize: 11 }} />
                <YAxis tick={{ fill: "#9ca3af", fontSize: 11 }} />
                <Tooltip
                  contentStyle={{ background: "#111827", border: "1px solid #374151", borderRadius: "8px" }}
                  labelStyle={{ color: "#d1d5db" }}
                />
                <Legend wrapperStyle={{ fontSize: 11 }} />
                <Bar dataKey="Criticals" fill="#ef4444" radius={[4, 4, 0, 0]} />
                <Bar dataKey="Warnings" fill="#f59e0b" radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Anomaly Log */}
          <div className="bg-factory-card rounded-xl border border-factory-border p-5 h-[340px] overflow-y-auto">
            <div className="flex items-center gap-2 mb-3">
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
                Anomaly Log
              </h3>
              <span className="flex items-center gap-1.5 text-xs text-purple-400 bg-purple-500/10 px-2 py-0.5 rounded-full font-medium">
                <BrainCircuit className="w-3.5 h-3.5" />
                Real-time
              </span>
            </div>
            <div className="max-h-52 overflow-y-auto">
              <table className="w-full text-xs">
                <thead className="sticky top-0 bg-factory-card">
                  <tr className="text-gray-500 border-b border-factory-border">
                    <th className="text-left py-1.5 px-2">Time</th>
                    <th className="text-left py-1.5 px-2">Machine</th>
                    <th className="text-left py-1.5 px-2">Sensor</th>
                    <th className="text-right py-1.5 px-2">Value</th>
                    <th className="text-center py-1.5 px-2">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {anomalies.length === 0 && (
                    <tr>
                      <td colSpan={5} className="text-center py-6 text-gray-600">
                        No anomalies yet
                      </td>
                    </tr>
                  )}
                  {anomalies.map((a, i) => (
                    <tr key={i} className="border-b border-factory-border/50 hover:bg-white/5">
                      <td className="py-1 px-2 text-gray-500 font-mono text-[10px]">
                        {new Date(a.timestamp).toLocaleTimeString()}
                      </td>
                      <td className="py-1 px-2 text-gray-300 text-[10px]">
                        {a.machine_id.replace("_01", "").replace(/_/g, " ")}
                      </td>
                      <td className="py-1 px-2 text-gray-400 font-mono text-[10px]">
                        {a.sensor_name}
                      </td>
                      <td className="py-1 px-2 text-right text-gray-300 font-mono text-[10px]">
                        {Number(a.value).toFixed(1)} <span className="text-gray-600">{a.unit}</span>
                      </td>
                      <td className="py-1 px-2 text-center">
                        <span className={`px-1.5 py-0.5 rounded-full text-[9px] font-semibold ${
                          a.anomaly_status === "CRITICAL"
                            ? "bg-red-500/20 text-red-400"
                            : "bg-amber-500/20 text-amber-400"
                        }`}>
                          {a.anomaly_status}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>

      {/* Pipeline Banner */}
      <PipelineBanner />
    </div>
  );
}

function MiniStat({
  icon,
  label,
  value,
  color,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  color: string;
}) {
  return (
    <div className="flex items-center gap-2">
      <span className={color}>{icon}</span>
      <span className="text-[10px] text-gray-500 uppercase">{label}</span>
      <span className={`text-sm font-bold font-mono ${color}`}>{value}</span>
    </div>
  );
}

