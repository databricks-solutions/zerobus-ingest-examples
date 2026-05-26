import React, { useState } from "react";
import { ChevronDown, ChevronUp, Zap, Gauge, Quote } from "lucide-react";

export default function ZeroBusInfo() {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="bg-factory-card rounded-xl border border-blue-500/20 overflow-hidden transition-all">
      {/* Collapsed: headline metrics */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between px-5 py-3 hover:bg-blue-500/5 transition-colors"
      >
        <div className="flex items-center gap-3">
          <Zap className="w-4 h-4 text-blue-400" />
          <span className="text-sm font-semibold text-blue-400">ZeroBus Ingest</span>
          <span className="text-xs text-gray-600 mx-1">|</span>
          <span className="text-sm text-emerald-400 font-mono font-bold">≤200ms</span>
          <span className="text-xs text-gray-400">ack</span>
          <span className="text-xs text-gray-700 mx-0.5">·</span>
          <span className="text-sm text-emerald-400 font-mono font-bold">≤5s</span>
          <span className="text-xs text-gray-400">in Delta</span>
          <span className="text-xs text-gray-700 mx-0.5">·</span>
          <span className="text-sm text-emerald-400 font-mono font-bold">10 GB/s</span>
          <span className="text-xs text-gray-400">aggregate</span>
          <span className="text-xs text-gray-700 mx-0.5">·</span>
          <span className="text-xs text-gray-300 font-medium">No Kafka required</span>
        </div>
        {expanded ? (
          <ChevronUp className="w-4 h-4 text-gray-500" />
        ) : (
          <ChevronDown className="w-4 h-4 text-gray-500" />
        )}
      </button>

      {/* Expanded: detailed metrics */}
      {expanded && (
        <div className="px-5 pb-5 pt-2 border-t border-factory-border">
          <div className="grid grid-cols-3 gap-6 mt-3">
            {/* Speed */}
            <div>
              <div className="flex items-center gap-1.5 mb-3">
                <Zap className="w-4 h-4 text-blue-400" />
                <span className="text-xs text-gray-500 uppercase tracking-wider font-semibold">Speed</span>
              </div>
              <div className="space-y-3">
                <MetricRow label="Durable ack (median)" value="≤ 200 ms" />
                <MetricRow label="Visible in Delta (median)" value="≤ 5 sec" />
                <MetricRow label="End-to-end (p95)" value="≤ 30 sec" />
              </div>
            </div>

            {/* Throughput */}
            <div>
              <div className="flex items-center gap-1.5 mb-3">
                <Gauge className="w-4 h-4 text-blue-400" />
                <span className="text-xs text-gray-500 uppercase tracking-wider font-semibold">Throughput</span>
              </div>
              <div className="space-y-3">
                <MetricRow label="Per stream" value="100 MB/s" />
                <MetricRow label="Per table (aggregate)" value="10 GB/s" />
                <MetricRow label="Records/sec per stream" value="15,000" />
                <MetricRow label="Concurrent clients" value="Thousands" />
              </div>
            </div>

            {/* Why it matters */}
            <div>
              <div className="flex items-center gap-1.5 mb-3">
                <span className="text-xs text-gray-500 uppercase tracking-wider font-semibold">Why It Matters</span>
              </div>
              <div className="space-y-3 text-sm text-gray-400">
                <p>· No message bus — straight to Delta</p>
                <p>· Fully serverless — zero infra management</p>
                <p>· Open standards — Delta, UC, gRPC/REST</p>
                <p>· SDKs: Python, Java, Go, Rust, TypeScript</p>
              </div>
            </div>
          </div>

          {/* Customer proof */}
          <div className="mt-5 grid grid-cols-2 gap-4">
            <div className="flex items-start gap-3 bg-gradient-to-r from-blue-500/10 to-blue-500/5 border border-blue-500/20 rounded-lg px-5 py-4">
              <Quote className="w-5 h-5 text-blue-400/70 shrink-0 mt-0.5" />
              <div>
                <p className="text-sm text-gray-300 italic leading-relaxed">
                  "ZeroBus reduced our telemetry resolution latency from days to minutes."
                </p>
                <p className="text-xs text-blue-400/80 mt-2 font-medium">
                  — Joby Aviation, Flight Telemetry
                </p>
              </div>
            </div>
            <div className="flex items-start gap-3 bg-gradient-to-r from-emerald-500/10 to-emerald-500/5 border border-emerald-500/20 rounded-lg px-5 py-4">
              <div className="shrink-0 mt-0.5 text-center">
                <p className="text-2xl font-bold text-emerald-400 font-mono">33%</p>
                <p className="text-[9px] text-emerald-400/70 uppercase">cheaper</p>
              </div>
              <div>
                <p className="text-sm text-gray-300 leading-relaxed">
                  <span className="text-emerald-400 font-semibold">33% cost savings</span> and <span className="text-emerald-400 font-semibold">40% faster</span> data transmission vs Kafka.
                </p>
                <p className="text-xs text-emerald-400/80 mt-2 font-medium">
                  — Bosch, Manufacturing IoT
                </p>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function MetricRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center gap-3">
      <span className="text-sm text-gray-500">{label}</span>
      <span className="text-sm text-emerald-400 font-mono font-bold">{value}</span>
    </div>
  );
}
