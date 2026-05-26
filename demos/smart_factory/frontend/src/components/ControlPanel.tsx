import React, { useState } from "react";
import { MachineConfig } from "../types";
import { Zap, ZapOff, RotateCcw, Trash2, Loader2 } from "lucide-react";

interface ControlPanelProps {
  machines: Record<string, MachineConfig>;
  faultStates: Record<string, boolean>;
  onInjectFault: (machineId: string) => void;
  onClearFault: (machineId: string) => void;
  onClearAll: () => void;
}

export default function ControlPanel({
  machines,
  faultStates,
  onInjectFault,
  onClearFault,
  onClearAll,
}: ControlPanelProps) {
  const anyFault = Object.values(faultStates).some(Boolean);
  const [resetting, setResetting] = useState(false);

  const handleReset = async () => {
    if (!confirm("Reset all demo data? This clears all tables for a fresh start.")) return;
    setResetting(true);
    try {
      await fetch("/api/reset-data", { method: "POST" });
    } catch {}
    setResetting(false);
  };

  return (
    <div className="bg-factory-card rounded-xl border border-factory-border p-4">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
          Fault Injection
        </h2>
        <div className="flex items-center gap-2">
          <button
            onClick={handleReset}
            disabled={resetting}
            className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg transition-all bg-gray-800 text-gray-400 hover:bg-red-500/20 hover:text-red-400 border border-gray-700 hover:border-red-500/30"
          >
            {resetting ? <Loader2 className="w-3 h-3 animate-spin" /> : <Trash2 className="w-3 h-3" />}
            Reset Data
          </button>
          <button
            onClick={onClearAll}
            disabled={!anyFault}
            className={`flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg transition-all ${
              anyFault
                ? "bg-emerald-500/20 text-emerald-400 hover:bg-emerald-500/30"
                : "bg-gray-800 text-gray-600 cursor-not-allowed"
            }`}
          >
            <RotateCcw className="w-3 h-3" />
            Clear All
          </button>
        </div>
      </div>
      <div className="flex gap-3">
        {Object.entries(machines).map(([machineId, config]) => {
          const isFaulting = faultStates[machineId] || false;
          return (
            <button
              key={machineId}
              onClick={() =>
                isFaulting ? onClearFault(machineId) : onInjectFault(machineId)
              }
              className={`flex-1 flex items-center justify-center gap-2 py-3 px-4 rounded-lg text-sm font-medium transition-all ${
                isFaulting
                  ? "bg-red-500/20 text-red-400 border border-red-500/30 hover:bg-red-500/30"
                  : "bg-gray-800 text-gray-300 border border-gray-700 hover:bg-gray-700 hover:text-white"
              }`}
            >
              {isFaulting ? (
                <ZapOff className="w-4 h-4" />
              ) : (
                <Zap className="w-4 h-4" />
              )}
              {isFaulting ? `Stop: ${config.display_name}` : `Fault: ${config.display_name}`}
            </button>
          );
        })}
      </div>
    </div>
  );
}
