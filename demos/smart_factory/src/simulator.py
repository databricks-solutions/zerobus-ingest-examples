"""
SmartFactory IoT Sensor Simulator.

Simulates 3 factory machines generating realistic sensor telemetry.
Each machine has 3 sensors with configurable normal/fault distributions.
Supports fault injection for live demo scenarios.
"""

import random
import time
from datetime import datetime, timezone
from dataclasses import dataclass, field


MACHINES = {
    "CNC_Mill_01": {
        "type": "cnc_mill",
        "display_name": "CNC Mill",
        "sensors": {
            "temperature_c": {
                "normal_mean": 45, "normal_std": 3,
                "fault_mean": 85, "fault_std": 5,
                "min": 20, "max": 120,
                "unit": "°C",
                "warning_threshold": 55, "critical_threshold": 75,
            },
            "vibration_mm_s": {
                "normal_mean": 2.5, "normal_std": 0.5,
                "fault_mean": 8.0, "fault_std": 1.0,
                "min": 0, "max": 15,
                "unit": "mm/s",
                "warning_threshold": 4.0, "critical_threshold": 6.5,
            },
            "spindle_rpm": {
                "normal_mean": 3500, "normal_std": 100,
                "fault_mean": 4800, "fault_std": 200,
                "min": 0, "max": 6000,
                "unit": "RPM",
                "warning_threshold": 4200, "critical_threshold": 4800,
            },
        },
    },
    "Hydraulic_Press_01": {
        "type": "hydraulic_press",
        "display_name": "Hydraulic Press",
        "sensors": {
            "pressure_bar": {
                "normal_mean": 180, "normal_std": 10,
                "fault_mean": 280, "fault_std": 15,
                "min": 0, "max": 350,
                "unit": "bar",
                "warning_threshold": 210, "critical_threshold": 270,
            },
            "temperature_c": {
                "normal_mean": 55, "normal_std": 4,
                "fault_mean": 95, "fault_std": 6,
                "min": 20, "max": 130,
                "unit": "°C",
                "warning_threshold": 70, "critical_threshold": 90,
            },
            "cycle_count": {
                "normal_mean": 12, "normal_std": 2,
                "fault_mean": 3, "fault_std": 1,
                "min": 0, "max": 30,
                "unit": "cycles/min",
                "warning_threshold": 6, "critical_threshold": 3,
            },
        },
    },
    "Conveyor_Belt_01": {
        "type": "conveyor_belt",
        "display_name": "Conveyor Belt",
        "sensors": {
            "speed_m_min": {
                "normal_mean": 15, "normal_std": 1,
                "fault_mean": 5, "fault_std": 1.5,
                "min": 0, "max": 25,
                "unit": "m/min",
                "warning_threshold": 9, "critical_threshold": 6,
            },
            "load_weight_kg": {
                "normal_mean": 250, "normal_std": 30,
                "fault_mean": 450, "fault_std": 20,
                "min": 0, "max": 500,
                "unit": "kg",
                "warning_threshold": 350, "critical_threshold": 430,
            },
            "motor_current_a": {
                "normal_mean": 8, "normal_std": 1,
                "fault_mean": 18, "fault_std": 2,
                "min": 0, "max": 25,
                "unit": "A",
                "warning_threshold": 12, "critical_threshold": 17,
            },
        },
    },
}


@dataclass
class MachineState:
    is_faulting: bool = False
    fault_progress: float = 0.0  # 0.0 = normal, 1.0 = full fault
    drift_rate: float = 0.1  # how fast fault develops per tick


class SensorSimulator:
    """Generates realistic IoT sensor readings with fault injection support."""

    def __init__(self):
        self.states: dict[str, MachineState] = {
            mid: MachineState() for mid in MACHINES
        }

    def inject_fault(self, machine_id: str) -> bool:
        if machine_id not in self.states:
            return False
        self.states[machine_id].is_faulting = True
        return True

    def clear_fault(self, machine_id: str) -> bool:
        if machine_id not in self.states:
            return False
        state = self.states[machine_id]
        state.is_faulting = False
        state.fault_progress = 0.0
        return True

    def clear_all_faults(self):
        for state in self.states.values():
            state.is_faulting = False
            state.fault_progress = 0.0

    def get_fault_states(self) -> dict[str, bool]:
        return {mid: s.is_faulting for mid, s in self.states.items()}

    def generate_tick(self) -> list[dict]:
        """Generate one tick of readings for all machines (9 events total)."""
        events = []
        now = datetime.now(timezone.utc)

        for machine_id, config in MACHINES.items():
            state = self.states[machine_id]

            # Advance fault progress
            if state.is_faulting and state.fault_progress < 1.0:
                state.fault_progress = min(1.0, state.fault_progress + state.drift_rate)
            elif not state.is_faulting and state.fault_progress > 0.0:
                state.fault_progress = max(0.0, state.fault_progress - state.drift_rate * 2)

            for sensor_name, sensor_cfg in config["sensors"].items():
                value = self._generate_value(sensor_cfg, state.fault_progress)
                events.append({
                    "machine_id": machine_id,
                    "machine_type": config["type"],
                    "sensor_name": sensor_name,
                    "value": round(value, 2),
                    "unit": sensor_cfg["unit"],
                    "timestamp": now.isoformat(),
                    "is_fault": state.is_faulting,
                })

        return events

    def _generate_value(self, cfg: dict, fault_progress: float) -> float:
        """Generate a sensor value blending normal and fault distributions."""
        normal_val = random.gauss(cfg["normal_mean"], cfg["normal_std"])
        fault_val = random.gauss(cfg["fault_mean"], cfg["fault_std"])

        # Blend based on fault progress
        value = normal_val * (1 - fault_progress) + fault_val * fault_progress

        # Clamp to sensor range
        return max(cfg["min"], min(cfg["max"], value))


def get_machine_configs() -> dict:
    """Return machine configs for the frontend."""
    result = {}
    for machine_id, config in MACHINES.items():
        sensors = {}
        for sensor_name, sensor_cfg in config["sensors"].items():
            sensors[sensor_name] = {
                "unit": sensor_cfg["unit"],
                "min": sensor_cfg["min"],
                "max": sensor_cfg["max"],
                "warning_threshold": sensor_cfg["warning_threshold"],
                "critical_threshold": sensor_cfg["critical_threshold"],
            }
        result[machine_id] = {
            "type": config["type"],
            "display_name": config["display_name"],
            "sensors": sensors,
        }
    return result
