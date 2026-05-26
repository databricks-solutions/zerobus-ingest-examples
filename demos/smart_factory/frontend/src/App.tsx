import React, { useState, useEffect, useCallback } from "react";
import { useWebSocket } from "./hooks/useWebSocket";
import FactoryFloor from "./components/FactoryFloor";
import MachineCard from "./components/MachineCard";
import EventFeed from "./components/EventFeed";
import ControlPanel from "./components/ControlPanel";
import DashboardView from "./components/DashboardView";
import PipelineBanner from "./components/PipelineBanner";
import ZeroBusInfo from "./components/ZeroBusInfo";
import {
  Activity, Wifi, WifiOff, Factory, BarChart3,
  Play, Square, Loader2, Radio, CircleOff,
} from "lucide-react";

type Tab = "factory" | "dashboard";

export default function App() {
  const {
    machines,
    latestReadings,
    faultStates,
    eventLog,
    totalEvents,
    eventsPerSec,
    connectionStatus,
    injectFault,
    clearFault,
    clearAllFaults,
  } = useWebSocket();

  const [activeTab, setActiveTab] = useState<Tab>("factory");
  const [pipelineState, setPipelineState] = useState("UNKNOWN");
  const [pipelineLoading, setPipelineLoading] = useState(false);
  const [simRunning, setSimRunning] = useState(true);
  const [simLoading, setSimLoading] = useState(false);

  const machineIds = Object.keys(machines);
  const hasData = machineIds.length > 0;

  // Poll pipeline + simulator status
  const fetchStatus = useCallback(async () => {
    try {
      const [pRes, sRes] = await Promise.all([
        fetch("/api/pipeline/status"),
        fetch("/api/simulator/status"),
      ]);
      const pData = await pRes.json();
      const sData = await sRes.json();
      setPipelineState(pData.state || "UNKNOWN");
      setSimRunning(sData.running ?? true);
    } catch {}
  }, []);

  useEffect(() => {
    fetchStatus();
    const interval = setInterval(fetchStatus, 8000);
    return () => clearInterval(interval);
  }, [fetchStatus]);

  const handlePipelineToggle = async () => {
    setPipelineLoading(true);
    const isRunning = pipelineState === "RUNNING";
    try {
      await fetch(`/api/pipeline/${isRunning ? "stop" : "start"}`, { method: "POST" });
      setTimeout(fetchStatus, 2000);
      setTimeout(fetchStatus, 5000);
      setTimeout(fetchStatus, 10000);
    } catch {}
    setPipelineLoading(false);
  };

  const handleSimToggle = async () => {
    setSimLoading(true);
    try {
      await fetch(`/api/simulator/${simRunning ? "stop" : "start"}`, { method: "POST" });
      setTimeout(fetchStatus, 1000);
    } catch {}
    setSimLoading(false);
  };

  const pipelineRunning = pipelineState === "RUNNING";

  return (
    <div className="min-h-screen bg-factory-bg flex flex-col">
      {/* Header */}
      <header className="border-b border-factory-border bg-factory-card/80 backdrop-blur-sm sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-6 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Activity className="w-5 h-5 text-blue-500" />
            <h1 className="text-lg font-bold text-gray-100">SmartFactory</h1>
            <span className="text-xs text-gray-500 border border-gray-700 rounded px-2 py-0.5">
              IoT Demo
            </span>
          </div>

          {/* Tabs */}
          <div className="flex items-center gap-1 bg-gray-800/50 rounded-lg p-0.5">
            <button
              onClick={() => setActiveTab("factory")}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-all ${
                activeTab === "factory"
                  ? "bg-gray-700 text-white"
                  : "text-gray-400 hover:text-gray-200"
              }`}
            >
              <Factory className="w-3.5 h-3.5" />
              IoT Simulation
            </button>
            <button
              onClick={() => setActiveTab("dashboard")}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-all ${
                activeTab === "dashboard"
                  ? "bg-gray-700 text-white"
                  : "text-gray-400 hover:text-gray-200"
              }`}
            >
              <BarChart3 className="w-3.5 h-3.5" />
              Operations Dashboard
            </button>
          </div>

          <div className="flex items-center gap-3">
            {/* Simulator Control */}
            <button
              onClick={handleSimToggle}
              disabled={simLoading}
              className={`flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg font-medium transition-all ${
                simLoading
                  ? "bg-gray-800 text-gray-500"
                  : simRunning
                  ? "bg-emerald-500/20 text-emerald-400 hover:bg-emerald-500/30"
                  : "bg-gray-800 text-gray-400 hover:bg-gray-700"
              }`}
            >
              {simLoading ? (
                <Loader2 className="w-3 h-3 animate-spin" />
              ) : simRunning ? (
                <Radio className="w-3 h-3" />
              ) : (
                <CircleOff className="w-3 h-3" />
              )}
              {simRunning ? "Streaming" : "Stream Paused"}
            </button>

            {/* Pipeline Control */}
            <button
              onClick={handlePipelineToggle}
              disabled={pipelineLoading}
              className={`flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-lg font-medium transition-all ${
                pipelineLoading
                  ? "bg-gray-800 text-gray-500"
                  : pipelineRunning
                  ? "bg-blue-500/20 text-blue-400 hover:bg-blue-500/30"
                  : "bg-gray-800 text-gray-400 hover:bg-gray-700"
              }`}
            >
              {pipelineLoading ? (
                <Loader2 className="w-3 h-3 animate-spin" />
              ) : pipelineRunning ? (
                <Square className="w-3 h-3" />
              ) : (
                <Play className="w-3 h-3" />
              )}
              {pipelineLoading
                ? "Working..."
                : pipelineRunning
                ? "Pipeline Running"
                : "Start Pipeline"}
            </button>

            {/* Connection Status */}
            <div className="flex items-center gap-1.5">
              {connectionStatus === "connected" ? (
                <>
                  <Wifi className="w-3.5 h-3.5 text-emerald-500" />
                  <span className="text-xs text-emerald-400">Live</span>
                </>
              ) : connectionStatus === "connecting" ? (
                <>
                  <Wifi className="w-3.5 h-3.5 text-amber-500 animate-pulse" />
                  <span className="text-xs text-amber-400">Connecting</span>
                </>
              ) : (
                <>
                  <WifiOff className="w-3.5 h-3.5 text-red-500" />
                  <span className="text-xs text-red-400">Disconnected</span>
                </>
              )}
            </div>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 max-w-7xl mx-auto px-6 py-6 w-full">
        {/* Factory Floor — always mounted, hidden via CSS */}
        <div className={activeTab === "factory" ? "" : "hidden"}>
          {!hasData ? (
            <div className="flex items-center justify-center h-64">
              <div className="text-center">
                <Activity className="w-8 h-8 text-gray-600 mx-auto mb-3 animate-pulse" />
                <p className="text-gray-500 text-sm">
                  Connecting to factory sensors...
                </p>
              </div>
            </div>
          ) : (
            <div className="space-y-6">
              <FactoryFloor
                machines={machines}
                readings={latestReadings}
                faultStates={faultStates}
                totalEvents={totalEvents}
                eventsPerSec={eventsPerSec}
              />
              <ZeroBusInfo />
              <div className="grid grid-cols-12 gap-6">
                <div className="col-span-8 space-y-4">
                  <div className="grid grid-cols-3 gap-4">
                    {machineIds.map((machineId) => (
                      <MachineCard
                        key={machineId}
                        machineId={machineId}
                        config={machines[machineId]}
                        readings={latestReadings[machineId]}
                        isFaulting={faultStates[machineId] || false}
                      />
                    ))}
                  </div>
                  <ControlPanel
                    machines={machines}
                    faultStates={faultStates}
                    onInjectFault={injectFault}
                    onClearFault={clearFault}
                    onClearAll={clearAllFaults}
                  />
                </div>
                <div className="col-span-4" style={{ maxHeight: "420px" }}>
                  <EventFeed events={eventLog} machines={machines} />
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Dashboard — always mounted, hidden via CSS */}
        <div className={activeTab === "dashboard" ? "" : "hidden"}>
          <DashboardView totalEvents={totalEvents} />
        </div>
      </main>
    </div>
  );
}
