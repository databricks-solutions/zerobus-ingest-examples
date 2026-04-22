"""
Anomaly Detection and Behavioral Monitoring.

Features:
- Behavioral baseline learning (normal patterns)
- Statistical anomaly detection (deviation from baseline)
- ML-based anomaly scoring
- Traffic pattern analysis
- Performance anomaly detection
- Integration with incident response

NIS2 Compliance: Article 21.2(f) - Continuous Monitoring
"""

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import statistics


class AnomalyType(Enum):
    """Type of anomaly detected."""
    AUTHENTICATION_ANOMALY = "authentication_anomaly"  # Unusual login patterns
    TRAFFIC_ANOMALY = "traffic_anomaly"                # Unusual data flow
    PERFORMANCE_ANOMALY = "performance_anomaly"        # CPU/memory spikes
    BEHAVIORAL_ANOMALY = "behavioral_anomaly"          # Unusual user behavior
    GEOGRAPHIC_ANOMALY = "geographic_anomaly"          # Login from unusual location
    TIME_ANOMALY = "time_anomaly"                      # Activity at unusual time
    VOLUME_ANOMALY = "volume_anomaly"                  # Unusual data volume


class AnomalySeverity(Enum):
    """Severity of detected anomaly."""
    CRITICAL = "critical"  # >3 standard deviations
    HIGH = "high"          # 2-3 standard deviations
    MEDIUM = "medium"      # 1-2 standard deviations
    LOW = "low"            # <1 standard deviation


@dataclass
class Anomaly:
    """Detected anomaly."""
    anomaly_id: str
    anomaly_type: AnomalyType
    severity: AnomalySeverity
    description: str
    detected_at: str

    # Context
    user: Optional[str] = None
    source_ip: Optional[str] = None
    component: Optional[str] = None

    # Metrics
    observed_value: Optional[float] = None
    expected_value: Optional[float] = None
    deviation_score: Optional[float] = None  # Z-score or similar

    # Details
    details: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'anomaly_id': self.anomaly_id,
            'anomaly_type': self.anomaly_type.value,
            'severity': self.severity.value,
            'description': self.description,
            'detected_at': self.detected_at,
            'user': self.user,
            'source_ip': self.source_ip,
            'component': self.component,
            'observed_value': self.observed_value,
            'expected_value': self.expected_value,
            'deviation_score': self.deviation_score,
            'details': self.details,
            'correlation_id': self.correlation_id,
        }


@dataclass
class BaselineMetrics:
    """Baseline metrics for anomaly detection."""
    metric_name: str
    mean: float
    std_dev: float
    min_value: float
    max_value: float
    sample_count: int
    last_updated: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'metric_name': self.metric_name,
            'mean': self.mean,
            'std_dev': self.std_dev,
            'min_value': self.min_value,
            'max_value': self.max_value,
            'sample_count': self.sample_count,
            'last_updated': self.last_updated,
        }


class BaselineLearner:
    """
    Learns normal behavior patterns (baseline).

    Uses rolling window statistics to establish normal ranges.
    """

    def __init__(self, window_size: int = 1000, learning_period_days: int = 7):
        """
        Initialize baseline learner.

        Args:
            window_size: Number of samples to keep for statistics
            learning_period_days: Days to learn baseline before detecting
        """
        self.window_size = window_size
        self.learning_period_days = learning_period_days
        self.learning_start = datetime.utcnow()

        # Rolling windows for each metric
        self.metric_windows: Dict[str, deque] = defaultdict(lambda: deque(maxlen=window_size))

        # Baseline metrics
        self.baselines: Dict[str, BaselineMetrics] = {}

        self.logger = logging.getLogger(__name__)

    def add_sample(self, metric_name: str, value: float):
        """
        Add sample to learning window.

        Args:
            metric_name: Name of metric
            value: Observed value
        """
        self.metric_windows[metric_name].append(value)

        # Update baseline if enough samples
        if len(self.metric_windows[metric_name]) >= 30:  # Minimum 30 samples
            self._update_baseline(metric_name)

    def _update_baseline(self, metric_name: str):
        """Update baseline metrics for a metric."""
        values = list(self.metric_windows[metric_name])

        if len(values) < 2:
            return

        mean = statistics.mean(values)
        std_dev = statistics.stdev(values) if len(values) > 1 else 0.0

        self.baselines[metric_name] = BaselineMetrics(
            metric_name=metric_name,
            mean=mean,
            std_dev=std_dev,
            min_value=min(values),
            max_value=max(values),
            sample_count=len(values),
            last_updated=datetime.utcnow().isoformat() + 'Z'
        )

    def get_baseline(self, metric_name: str) -> Optional[BaselineMetrics]:
        """Get baseline for metric."""
        return self.baselines.get(metric_name)

    def is_learning_complete(self) -> bool:
        """Check if learning period is complete."""
        elapsed = datetime.utcnow() - self.learning_start
        return elapsed.days >= self.learning_period_days

    def calculate_z_score(self, metric_name: str, value: float) -> Optional[float]:
        """
        Calculate z-score (standard deviations from mean).

        Args:
            metric_name: Name of metric
            value: Observed value

        Returns:
            Z-score or None if baseline not established
        """
        baseline = self.baselines.get(metric_name)
        if not baseline or baseline.std_dev == 0:
            return None

        z_score = (value - baseline.mean) / baseline.std_dev
        return z_score


class AnomalyDetector:
    """
    Detects anomalies using statistical methods.

    Uses z-score analysis to detect deviations from baseline.
    """

    def __init__(self, baseline_learner: BaselineLearner):
        """
        Initialize anomaly detector.

        Args:
            baseline_learner: Baseline learner instance
        """
        self.baseline_learner = baseline_learner
        self.logger = logging.getLogger(__name__)

        # Thresholds (z-scores)
        self.threshold_critical = 3.0   # >3 std devs
        self.threshold_high = 2.0       # 2-3 std devs
        self.threshold_medium = 1.5     # 1.5-2 std devs

    def detect_anomaly(
        self,
        metric_name: str,
        value: float,
        anomaly_type: AnomalyType,
        context: Optional[Dict[str, Any]] = None
    ) -> Optional[Anomaly]:
        """
        Detect if value is anomalous.

        Args:
            metric_name: Name of metric
            value: Observed value
            anomaly_type: Type of anomaly
            context: Additional context (user, ip, etc.)

        Returns:
            Anomaly if detected, None otherwise
        """
        # Check if learning is complete
        if not self.baseline_learner.is_learning_complete():
            # Still learning, add sample but don't detect
            self.baseline_learner.add_sample(metric_name, value)
            return None

        # Add sample and get baseline
        self.baseline_learner.add_sample(metric_name, value)
        baseline = self.baseline_learner.get_baseline(metric_name)

        if not baseline:
            return None

        # Calculate z-score
        z_score = self.baseline_learner.calculate_z_score(metric_name, value)
        if z_score is None:
            return None

        # Determine severity
        abs_z = abs(z_score)
        severity = None

        if abs_z >= self.threshold_critical:
            severity = AnomalySeverity.CRITICAL
        elif abs_z >= self.threshold_high:
            severity = AnomalySeverity.HIGH
        elif abs_z >= self.threshold_medium:
            severity = AnomalySeverity.MEDIUM
        else:
            # Not anomalous
            return None

        # Create anomaly
        context = context or {}

        anomaly_id = f"ANOM-{int(time.time())}-{metric_name}"

        description = (
            f"{metric_name}: observed {value:.2f}, "
            f"expected {baseline.mean:.2f} (±{baseline.std_dev:.2f}), "
            f"deviation: {abs_z:.2f} std devs"
        )

        anomaly = Anomaly(
            anomaly_id=anomaly_id,
            anomaly_type=anomaly_type,
            severity=severity,
            description=description,
            detected_at=datetime.utcnow().isoformat() + 'Z',
            user=context.get('user'),
            source_ip=context.get('source_ip'),
            component=context.get('component'),
            observed_value=value,
            expected_value=baseline.mean,
            deviation_score=z_score,
            details={
                'metric_name': metric_name,
                'baseline_std_dev': baseline.std_dev,
                'baseline_min': baseline.min_value,
                'baseline_max': baseline.max_value,
            },
            correlation_id=context.get('correlation_id'),
        )

        self.logger.warning(f"Anomaly detected: {anomaly.description}")

        return anomaly


class BehavioralMonitor:
    """
    Monitors user and system behavior for anomalies.

    Tracks:
    - Authentication patterns (login times, frequencies)
    - Data access patterns
    - Configuration changes
    - Performance metrics
    """

    def __init__(self, baseline_dir: Path = Path('baselines')):
        """
        Initialize behavioral monitor.

        Args:
            baseline_dir: Directory to store baselines
        """
        self.baseline_dir = baseline_dir
        self.baseline_dir.mkdir(parents=True, exist_ok=True)

        # Baseline learner
        self.baseline_learner = BaselineLearner(window_size=1000, learning_period_days=7)

        # Anomaly detector
        self.detector = AnomalyDetector(self.baseline_learner)

        # User behavior tracking
        self.user_login_times: Dict[str, List[int]] = defaultdict(list)  # hour of day
        self.user_ip_addresses: Dict[str, set] = defaultdict(set)

        self.logger = logging.getLogger(__name__)

    def process_authentication_event(self, event: Dict[str, Any]) -> Optional[Anomaly]:
        """
        Process authentication event and detect anomalies.

        Args:
            event: Authentication event

        Returns:
            Anomaly if detected
        """
        user = event.get('user', 'unknown')
        source_ip = event.get('source_ip', 'unknown')
        timestamp_str = event.get('timestamp', '')

        # Parse timestamp
        try:
            if timestamp_str.endswith('Z'):
                timestamp_str = timestamp_str[:-1]
            dt = datetime.fromisoformat(timestamp_str)
            hour = dt.hour
        except (ValueError, AttributeError):
            return None

        # Track login time
        self.user_login_times[user].append(hour)

        # Check for time anomaly (unusual login hour)
        if len(self.user_login_times[user]) >= 30:
            # Calculate average login hour
            avg_hour = statistics.mean(self.user_login_times[user][-100:])
            std_hour = statistics.stdev(self.user_login_times[user][-100:]) if len(self.user_login_times[user][-100:]) > 1 else 3.0

            deviation = abs(hour - avg_hour)

            # Account for circular nature of hours (23 -> 0)
            if deviation > 12:
                deviation = 24 - deviation

            # Detect if outside normal hours
            if std_hour > 0 and deviation / std_hour > 2.0:
                return Anomaly(
                    anomaly_id=f"ANOM-TIME-{int(time.time())}",
                    anomaly_type=AnomalyType.TIME_ANOMALY,
                    severity=AnomalySeverity.MEDIUM,
                    description=f"User {user} logged in at unusual hour {hour}:00 (typical: {int(avg_hour)}:00)",
                    detected_at=datetime.utcnow().isoformat() + 'Z',
                    user=user,
                    source_ip=source_ip,
                    observed_value=float(hour),
                    expected_value=avg_hour,
                    deviation_score=deviation / std_hour if std_hour > 0 else 0,
                )

        # Track IP address
        if source_ip not in self.user_ip_addresses[user]:
            self.user_ip_addresses[user].add(source_ip)

            # If user has logged in from multiple IPs before, this might be suspicious
            if len(self.user_ip_addresses[user]) == 1:
                # First IP, not anomalous
                pass
            elif len(self.user_ip_addresses[user]) <= 5:
                # Reasonable number of IPs, log but don't alert
                self.logger.info(f"User {user} logged in from new IP: {source_ip}")
            else:
                # Many IPs, potentially compromised account
                return Anomaly(
                    anomaly_id=f"ANOM-GEO-{int(time.time())}",
                    anomaly_type=AnomalyType.GEOGRAPHIC_ANOMALY,
                    severity=AnomalySeverity.HIGH,
                    description=f"User {user} logged in from new IP {source_ip} (total: {len(self.user_ip_addresses[user])} IPs)",
                    detected_at=datetime.utcnow().isoformat() + 'Z',
                    user=user,
                    source_ip=source_ip,
                    details={'known_ips': list(self.user_ip_addresses[user])},
                )

        return None

    def process_performance_metric(self, metric: Dict[str, Any]) -> Optional[Anomaly]:
        """
        Process performance metric and detect anomalies.

        Args:
            metric: Performance metric

        Returns:
            Anomaly if detected
        """
        metric_name = metric.get('metric_name', 'unknown')
        metric_value = metric.get('metric_value')
        component = metric.get('component', 'unknown')

        if metric_value is None:
            return None

        # Detect anomaly
        anomaly = self.detector.detect_anomaly(
            metric_name=f"{component}.{metric_name}",
            value=metric_value,
            anomaly_type=AnomalyType.PERFORMANCE_ANOMALY,
            context={'component': component}
        )

        return anomaly

    def process_data_volume(self, source: str, volume: float) -> Optional[Anomaly]:
        """
        Process data volume and detect anomalies.

        Args:
            source: Data source name
            volume: Data volume (messages, bytes, etc.)

        Returns:
            Anomaly if detected
        """
        metric_name = f"data_volume.{source}"

        anomaly = self.detector.detect_anomaly(
            metric_name=metric_name,
            value=volume,
            anomaly_type=AnomalyType.VOLUME_ANOMALY,
            context={'component': source}
        )

        return anomaly

    def get_baseline_summary(self) -> Dict[str, Any]:
        """Get summary of learned baselines."""
        return {
            'learning_complete': self.baseline_learner.is_learning_complete(),
            'learning_start': self.baseline_learner.learning_start.isoformat(),
            'baselines_count': len(self.baseline_learner.baselines),
            'baselines': {
                name: baseline.to_dict()
                for name, baseline in self.baseline_learner.baselines.items()
            }
        }


class AnomalyManager:
    """
    Manages detected anomalies.

    Stores, tracks, and correlates anomalies.
    """

    def __init__(self, anomaly_dir: Path = Path('anomalies')):
        """
        Initialize anomaly manager.

        Args:
            anomaly_dir: Directory to store anomalies
        """
        self.anomaly_dir = anomaly_dir
        self.anomaly_dir.mkdir(parents=True, exist_ok=True)

        self.anomalies: Dict[str, Anomaly] = {}
        self.logger = logging.getLogger(__name__)

    def add_anomaly(self, anomaly: Anomaly):
        """
        Add detected anomaly.

        Args:
            anomaly: Detected anomaly
        """
        self.anomalies[anomaly.anomaly_id] = anomaly
        self._save_anomaly(anomaly)

        self.logger.warning(
            f"Anomaly {anomaly.anomaly_id} ({anomaly.severity.value}): "
            f"{anomaly.description}"
        )

    def get_recent_anomalies(
        self,
        hours: int = 24,
        severity: Optional[AnomalySeverity] = None
    ) -> List[Anomaly]:
        """
        Get recent anomalies.

        Args:
            hours: Hours to look back
            severity: Filter by severity

        Returns:
            List of anomalies
        """
        cutoff = datetime.utcnow() - timedelta(hours=hours)

        result = []
        for anomaly in self.anomalies.values():
            # Parse timestamp
            try:
                ts = anomaly.detected_at
                if ts.endswith('Z'):
                    ts = ts[:-1]
                dt = datetime.fromisoformat(ts)

                if dt < cutoff:
                    continue

                if severity and anomaly.severity != severity:
                    continue

                result.append(anomaly)
            except (ValueError, AttributeError):
                continue

        return sorted(result, key=lambda a: a.detected_at, reverse=True)

    def get_summary(self) -> Dict[str, Any]:
        """Get anomaly summary statistics."""
        total = len(self.anomalies)

        by_type = defaultdict(int)
        by_severity = defaultdict(int)

        for anomaly in self.anomalies.values():
            by_type[anomaly.anomaly_type.value] += 1
            by_severity[anomaly.severity.value] += 1

        return {
            'total': total,
            'by_type': dict(by_type),
            'by_severity': dict(by_severity),
        }

    def _save_anomaly(self, anomaly: Anomaly):
        """Save anomaly to disk."""
        anomaly_file = self.anomaly_dir / f"{anomaly.anomaly_id}.json"
        with open(anomaly_file, 'w') as f:
            json.dump(anomaly.to_dict(), f, indent=2, default=str)


class AnomalyDetectionSystem:
    """
    Complete anomaly detection system.

    Integrates behavioral monitoring, anomaly detection, and management.
    """

    def __init__(
        self,
        baseline_dir: Path = Path('baselines'),
        anomaly_dir: Path = Path('anomalies'),
        enable_incident_integration: bool = True
    ):
        """
        Initialize anomaly detection system.

        Args:
            baseline_dir: Directory for baselines
            anomaly_dir: Directory for anomalies
            enable_incident_integration: Enable automatic incident creation for CRITICAL/HIGH anomalies
        """
        self.monitor = BehavioralMonitor(baseline_dir)
        self.manager = AnomalyManager(anomaly_dir)
        self.logger = logging.getLogger(__name__)
        self.enable_incident_integration = enable_incident_integration
        self._incident_manager = None

    async def process_event(self, event: Dict[str, Any]):
        """
        Process event and detect anomalies.

        Args:
            event: Security or performance event
        """
        event_type = event.get('event_category', '').split('.')[0]

        # Route to appropriate handler
        anomaly = None

        if event_type == 'auth':
            anomaly = self.monitor.process_authentication_event(event)
        elif event.get('log_type') == 'performance':
            anomaly = self.monitor.process_performance_metric(event)

        # Add anomaly if detected
        if anomaly:
            self.manager.add_anomaly(anomaly)

            # Integrate with incident response for CRITICAL/HIGH anomalies
            if self.enable_incident_integration and anomaly.severity in (AnomalySeverity.CRITICAL, AnomalySeverity.HIGH):
                await self._create_incident_from_anomaly(anomaly)

    async def _create_incident_from_anomaly(self, anomaly: Anomaly) -> None:
        """
        Create incident from detected anomaly.

        Args:
            anomaly: Detected anomaly

        Creates incidents for CRITICAL and HIGH severity anomalies to enable
        automated response and notification workflows.
        """
        try:
            # Lazy import to avoid circular dependency
            if self._incident_manager is None:
                from unified_connector.core.incident_response import (
                    get_incident_manager,
                    IncidentAlert,
                    IncidentSeverity
                )
                self._incident_manager = get_incident_manager()

            # Map anomaly severity to incident severity
            severity_map = {
                AnomalySeverity.CRITICAL: IncidentSeverity.CRITICAL,
                AnomalySeverity.HIGH: IncidentSeverity.HIGH,
                AnomalySeverity.MEDIUM: IncidentSeverity.MEDIUM,
                AnomalySeverity.LOW: IncidentSeverity.LOW,
            }

            # Map anomaly type to incident category
            category_map = {
                AnomalyType.AUTHENTICATION_ANOMALY: "authentication",
                AnomalyType.TRAFFIC_ANOMALY: "data_exfiltration",
                AnomalyType.PERFORMANCE_ANOMALY: "performance",
                AnomalyType.BEHAVIORAL_ANOMALY: "suspicious_activity",
                AnomalyType.GEOGRAPHIC_ANOMALY: "authentication",
                AnomalyType.TIME_ANOMALY: "suspicious_activity",
                AnomalyType.VOLUME_ANOMALY: "data_exfiltration",
            }

            # Create incident alert from anomaly
            alert = IncidentAlert(
                alert_type=category_map.get(anomaly.anomaly_type, "anomaly_detection"),
                severity=severity_map[anomaly.severity],
                source="anomaly_detection",
                title=f"{anomaly.anomaly_type.value.replace('_', ' ').title()} Detected",
                description=anomaly.description,
                details={
                    'anomaly_id': anomaly.anomaly_id,
                    'metric_name': anomaly.metric_name,
                    'value': anomaly.value,
                    'z_score': anomaly.z_score,
                    'baseline_mean': anomaly.baseline_mean,
                    'baseline_std': anomaly.baseline_std,
                    **anomaly.details
                },
                timestamp=anomaly.detected_at
            )

            # Create incident
            incident = self._incident_manager.create_incident(alert)

            self.logger.info(
                f"Created incident {incident.incident_id} from anomaly {anomaly.anomaly_id} "
                f"(severity: {anomaly.severity.value})"
            )

            # Link anomaly to incident
            anomaly.details['incident_id'] = incident.incident_id
            self.manager.anomalies[anomaly.anomaly_id] = anomaly
            self.manager._save_anomaly(anomaly)

        except Exception as e:
            self.logger.error(f"Failed to create incident from anomaly {anomaly.anomaly_id}: {e}", exc_info=True)

    def get_baseline_status(self) -> Dict[str, Any]:
        """Get baseline learning status."""
        return self.monitor.get_baseline_summary()

    def get_recent_anomalies(self, hours: int = 24) -> List[Anomaly]:
        """Get recent anomalies."""
        return self.manager.get_recent_anomalies(hours=hours)

    def get_summary(self) -> Dict[str, Any]:
        """Get system summary."""
        return {
            'baseline_status': self.get_baseline_status(),
            'anomaly_summary': self.manager.get_summary(),
            'incident_integration': self.enable_incident_integration,
        }


# Global instance
_anomaly_system: Optional[AnomalyDetectionSystem] = None


def get_anomaly_system(
    baseline_dir: Optional[Path] = None,
    anomaly_dir: Optional[Path] = None
) -> AnomalyDetectionSystem:
    """
    Get global anomaly detection system instance.

    Args:
        baseline_dir: Baseline directory
        anomaly_dir: Anomaly directory

    Returns:
        AnomalyDetectionSystem instance
    """
    global _anomaly_system
    if _anomaly_system is None:
        if baseline_dir is None:
            baseline_dir = Path('baselines')
        if anomaly_dir is None:
            anomaly_dir = Path('anomalies')

        _anomaly_system = AnomalyDetectionSystem(baseline_dir, anomaly_dir)

    return _anomaly_system
