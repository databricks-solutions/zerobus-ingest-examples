import React from "react";
import { Shield, BrainCircuit } from "lucide-react";

export default function PipelineBanner() {
  return (
    <div className="bg-factory-card rounded-xl border border-factory-border p-5">
      <div className="flex items-center justify-between">
        {/* Pipeline Steps */}
        <div className="flex items-center gap-4 text-sm">
          <Step label="ZeroBus" sublabel="Push API" color="text-blue-400" />
          <Arrow />
          <Step label="Bronze" sublabel="Validated" color="text-orange-400" />
          <Arrow />
          <div className="text-center">
            <div className="flex items-center gap-1.5">
              <span className="font-semibold text-gray-200">Silver</span>
              <span className="flex items-center gap-1 text-xs text-purple-400 bg-purple-500/10 px-2 py-0.5 rounded-full font-medium">
                <BrainCircuit className="w-3.5 h-3.5" />
                ML
              </span>
            </div>
            <div className="text-xs text-gray-500">Anomaly Scored</div>
          </div>
          <Arrow />
          <Step label="Gold" sublabel="Health KPIs" color="text-yellow-400" />
          <Arrow />
          <Step label="Dashboard" sublabel="Live" color="text-emerald-400" />
        </div>

        {/* UC Governance Badge */}
        <div className="flex items-center gap-3 bg-gradient-to-r from-blue-500/10 to-emerald-500/10 border border-blue-500/20 rounded-lg px-4 py-2.5">
          <Shield className="w-5 h-5 text-blue-400 shrink-0" />
          <div>
            <p className="text-sm font-semibold text-gray-100">
              Governed by Unity Catalog
            </p>
            <p className="text-[11px] text-gray-400">
              End-to-end lineage, access control & audit
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

function Step({
  label,
  sublabel,
  color,
}: {
  label: string;
  sublabel: string;
  color: string;
}) {
  return (
    <div className="text-center">
      <div className={`font-semibold ${color}`}>{label}</div>
      <div className="text-xs text-gray-500">{sublabel}</div>
    </div>
  );
}

function Arrow() {
  return (
    <span className="text-gray-600 text-lg">&#x2192;</span>
  );
}
