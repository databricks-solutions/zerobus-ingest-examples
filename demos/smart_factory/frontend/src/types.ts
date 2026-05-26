export interface SensorConfig {
  unit: string;
  min: number;
  max: number;
  warning_threshold: number;
  critical_threshold: number;
}

export interface MachineConfig {
  type: string;
  display_name: string;
  sensors: Record<string, SensorConfig>;
}

export interface SensorReading {
  machine_id: string;
  machine_type: string;
  sensor_name: string;
  value: number;
  unit: string;
  timestamp: string;
  is_fault: boolean;
}

export interface WSMessage {
  type: "init" | "sensor_data";
  machines?: Record<string, MachineConfig>;
  events?: SensorReading[];
  fault_states?: Record<string, boolean>;
}

export type AnomalyStatus = "NORMAL" | "WARNING" | "CRITICAL";

export function getAnomalyStatus(
  value: number,
  config: SensorConfig
): AnomalyStatus {
  const { warning_threshold, critical_threshold } = config;

  // High-value fault sensors (threshold ascending)
  if (warning_threshold < critical_threshold) {
    if (value >= critical_threshold) return "CRITICAL";
    if (value >= warning_threshold) return "WARNING";
    return "NORMAL";
  }
  // Low-value fault sensors (speed drops, cycle count drops)
  if (value <= critical_threshold) return "CRITICAL";
  if (value <= warning_threshold) return "WARNING";
  return "NORMAL";
}
