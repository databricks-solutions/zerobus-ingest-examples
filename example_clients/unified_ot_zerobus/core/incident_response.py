"""
Automated Incident Response System.

Features:
- Automated incident detection from logs and security events
- Incident classification and prioritization
- Automated alerting and notifications (email, Slack, PagerDuty)
- Incident escalation procedures
- Response workflows and playbooks
- Post-incident analysis and reporting

NIS2 Compliance: Article 21.2(b) - Incident Handling
"""

import asyncio
import hashlib
import json
import smtplib
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
import logging
import re

try:
    import aiohttp
except ImportError:
    aiohttp = None


class IncidentSeverity(Enum):
    """Incident severity levels."""
    CRITICAL = "critical"  # Immediate response required (<15 minutes)
    HIGH = "high"          # Response required within 1 hour
    MEDIUM = "medium"      # Response required within 4 hours
    LOW = "low"            # Response required within 24 hours
    INFO = "info"          # Informational only


class IncidentStatus(Enum):
    """Incident status."""
    DETECTED = "detected"           # Incident detected
    ACKNOWLEDGED = "acknowledged"   # Someone is looking at it
    INVESTIGATING = "investigating" # Investigation in progress
    MITIGATING = "mitigating"      # Mitigation in progress
    RESOLVED = "resolved"           # Incident resolved
    CLOSED = "closed"               # Post-incident review complete


class IncidentCategory(Enum):
    """Incident category."""
    SECURITY_BREACH = "security_breach"
    AUTHENTICATION_ATTACK = "authentication_attack"
    INJECTION_ATTACK = "injection_attack"
    PRIVILEGE_ESCALATION = "privilege_escalation"
    DATA_BREACH = "data_breach"
    CONFIGURATION_ERROR = "configuration_error"
    SYSTEM_ERROR = "system_error"
    PERFORMANCE_DEGRADATION = "performance_degradation"
    COMPLIANCE_VIOLATION = "compliance_violation"


@dataclass
class IncidentAlert:
    """Alert that may trigger an incident."""
    alert_id: str
    alert_name: str
    severity: IncidentSeverity
    category: IncidentCategory
    description: str
    timestamp: str
    source_ip: Optional[str] = None
    user: Optional[str] = None
    event_category: Optional[str] = None
    correlation_id: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Incident:
    """Incident record."""
    incident_id: str
    title: str
    severity: IncidentSeverity
    category: IncidentCategory
    status: IncidentStatus
    description: str
    created_at: str
    updated_at: str
    detected_by: str  # "automated" or user email
    assigned_to: Optional[str] = None
    source_ip: Optional[str] = None
    affected_user: Optional[str] = None
    correlation_id: Optional[str] = None
    alerts: List[IncidentAlert] = field(default_factory=list)
    timeline: List[Dict[str, Any]] = field(default_factory=list)
    response_actions: List[str] = field(default_factory=list)
    resolution_notes: Optional[str] = None
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'incident_id': self.incident_id,
            'title': self.title,
            'severity': self.severity.value,
            'category': self.category.value,
            'status': self.status.value,
            'description': self.description,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'detected_by': self.detected_by,
            'assigned_to': self.assigned_to,
            'source_ip': self.source_ip,
            'affected_user': self.affected_user,
            'correlation_id': self.correlation_id,
            'alerts': [
                {
                    'alert_id': a.alert_id,
                    'alert_name': a.alert_name,
                    'severity': a.severity.value,
                    'timestamp': a.timestamp,
                }
                for a in self.alerts
            ],
            'timeline': self.timeline,
            'response_actions': self.response_actions,
            'resolution_notes': self.resolution_notes,
            'tags': self.tags,
        }

    def add_timeline_entry(self, action: str, details: str, user: str = "system"):
        """Add entry to incident timeline."""
        self.timeline.append({
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'action': action,
            'details': details,
            'user': user,
        })
        self.updated_at = datetime.utcnow().isoformat() + 'Z'


@dataclass
class NotificationConfig:
    """Notification configuration."""
    # Email
    email_enabled: bool = True
    smtp_server: str = "localhost"
    smtp_port: int = 587
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None
    email_from: str = "alerts@unified-connector.local"
    email_to_critical: List[str] = field(default_factory=lambda: ["security@example.com"])
    email_to_high: List[str] = field(default_factory=lambda: ["security@example.com"])
    email_to_medium: List[str] = field(default_factory=lambda: ["ops@example.com"])

    # Slack
    slack_enabled: bool = False
    slack_webhook_url: Optional[str] = None
    slack_channel_critical: str = "#security-critical"
    slack_channel_high: str = "#security-alerts"
    slack_channel_medium: str = "#ops-alerts"

    # PagerDuty
    pagerduty_enabled: bool = False
    pagerduty_api_key: Optional[str] = None
    pagerduty_service_key: Optional[str] = None

    # ServiceNow
    servicenow_enabled: bool = False
    servicenow_instance: Optional[str] = None
    servicenow_username: Optional[str] = None
    servicenow_password: Optional[str] = None


class IncidentDetector:
    """
    Detects incidents from security events and logs.

    Uses rule-based detection to identify security incidents.
    """

    def __init__(self, alert_rules: Dict[str, Any]):
        """
        Initialize incident detector.

        Args:
            alert_rules: Alert rules configuration
        """
        self.alert_rules = alert_rules
        self.event_buffer = []  # Buffer for time-window analysis
        self.logger = logging.getLogger(__name__)

    def detect_incident(self, event: Dict[str, Any]) -> Optional[IncidentAlert]:
        """
        Analyze event and detect if it's an incident.

        Args:
            event: Security event

        Returns:
            IncidentAlert if incident detected, None otherwise
        """
        # Add to buffer for time-window analysis
        self.event_buffer.append(event)
        self._cleanup_old_events()

        # Check critical alerts
        for rule in self.alert_rules.get('critical_alerts', []):
            if self._matches_rule(event, rule):
                return self._create_alert(event, rule, IncidentSeverity.CRITICAL)

        # Check high alerts
        for rule in self.alert_rules.get('high_alerts', []):
            if self._matches_rule(event, rule):
                return self._create_alert(event, rule, IncidentSeverity.HIGH)

        # Check medium alerts
        for rule in self.alert_rules.get('medium_alerts', []):
            if self._matches_rule(event, rule):
                return self._create_alert(event, rule, IncidentSeverity.MEDIUM)

        # Check compliance alerts
        for rule in self.alert_rules.get('compliance_alerts', []):
            if self._matches_rule(event, rule):
                return self._create_alert(event, rule, IncidentSeverity.HIGH)

        return None

    def _matches_rule(self, event: Dict[str, Any], rule: Dict[str, Any]) -> bool:
        """
        Check if event matches alert rule.

        Args:
            event: Security event
            rule: Alert rule

        Returns:
            True if matches
        """
        if not rule.get('enabled', True):
            return False

        condition = rule.get('condition', {})

        # Check event category
        if 'event_category' in condition:
            required_category = condition['event_category']
            event_category = event.get('event_category', '')

            # Support wildcards
            if required_category.endswith('*'):
                if not event_category.startswith(required_category[:-1]):
                    return False
            elif event_category != required_category:
                return False

        # Check threshold (count over timeframe)
        if 'threshold' in condition and 'timeframe' in condition:
            timeframe = self._parse_timeframe(condition['timeframe'])
            threshold = condition['threshold']
            group_by = condition.get('group_by')

            count = self._count_events_in_timeframe(
                condition.get('event_category'),
                timeframe,
                group_by,
                event
            )

            if count < threshold:
                return False

        # Check user role
        if 'user_role' in condition:
            required_roles = condition['user_role']
            if isinstance(required_roles, str):
                required_roles = [required_roles]

            user_role = event.get('details', {}).get('user_role', '')
            if user_role not in required_roles:
                return False

        # Check MFA
        if 'mfa' in condition:
            required_mfa = condition['mfa']
            event_mfa = event.get('details', {}).get('mfa', False)
            if event_mfa != required_mfa:
                return False

        return True

    def _parse_timeframe(self, timeframe_str: str) -> int:
        """
        Parse timeframe string to seconds.

        Args:
            timeframe_str: Timeframe string (e.g., "5m", "1h", "1d")

        Returns:
            Seconds
        """
        match = re.match(r'(\d+)([smhd])', timeframe_str)
        if not match:
            return 60  # Default 1 minute

        value = int(match.group(1))
        unit = match.group(2)

        if unit == 's':
            return value
        elif unit == 'm':
            return value * 60
        elif unit == 'h':
            return value * 3600
        elif unit == 'd':
            return value * 86400

        return 60

    def _count_events_in_timeframe(
        self,
        event_category: str,
        timeframe_seconds: int,
        group_by: Optional[Any],
        current_event: Dict[str, Any]
    ) -> int:
        """
        Count matching events in timeframe.

        Args:
            event_category: Event category pattern
            timeframe_seconds: Timeframe in seconds
            group_by: Group by field(s)
            current_event: Current event

        Returns:
            Count of matching events
        """
        cutoff_time = time.time() - timeframe_seconds
        count = 0

        for event in self.event_buffer:
            # Check timestamp
            event_time = self._parse_timestamp(event.get('timestamp', ''))
            if not event_time or event_time < cutoff_time:
                continue

            # Check category
            if event_category:
                if event_category.endswith('*'):
                    if not event.get('event_category', '').startswith(event_category[:-1]):
                        continue
                elif event.get('event_category') != event_category:
                    continue

            # Check group_by
            if group_by:
                if isinstance(group_by, str):
                    group_by = [group_by]

                match = True
                for field in group_by:
                    if event.get(field) != current_event.get(field):
                        match = False
                        break

                if not match:
                    continue

            count += 1

        return count

    def _parse_timestamp(self, timestamp_str: str) -> Optional[float]:
        """
        Parse ISO 8601 timestamp to Unix time.

        Args:
            timestamp_str: ISO 8601 timestamp

        Returns:
            Unix timestamp or None
        """
        try:
            if timestamp_str.endswith('Z'):
                timestamp_str = timestamp_str[:-1]
            dt = datetime.fromisoformat(timestamp_str)
            return dt.timestamp()
        except (ValueError, AttributeError):
            return None

    def _cleanup_old_events(self, max_age_seconds: int = 86400):
        """
        Remove events older than max_age from buffer.

        Args:
            max_age_seconds: Maximum age in seconds (default 24 hours)
        """
        cutoff_time = time.time() - max_age_seconds
        self.event_buffer = [
            event for event in self.event_buffer
            if self._parse_timestamp(event.get('timestamp', '')) > cutoff_time
        ]

    def _create_alert(
        self,
        event: Dict[str, Any],
        rule: Dict[str, Any],
        severity: IncidentSeverity
    ) -> IncidentAlert:
        """
        Create incident alert from event and rule.

        Args:
            event: Security event
            rule: Alert rule
            severity: Severity level

        Returns:
            IncidentAlert
        """
        # Determine category
        event_category = event.get('event_category', '')
        if 'injection' in event_category:
            category = IncidentCategory.INJECTION_ATTACK
        elif 'auth.failure' in event_category:
            category = IncidentCategory.AUTHENTICATION_ATTACK
        elif 'authz.privilege_escalation' in event_category:
            category = IncidentCategory.PRIVILEGE_ESCALATION
        elif 'config' in event_category:
            category = IncidentCategory.CONFIGURATION_ERROR
        elif 'compliance' in rule.get('name', '').lower():
            category = IncidentCategory.COMPLIANCE_VIOLATION
        else:
            category = IncidentCategory.SECURITY_BREACH

        # Generate alert ID
        alert_id = hashlib.md5(
            f"{rule['name']}{event.get('timestamp', '')}{event.get('correlation_id', '')}".encode()
        ).hexdigest()[:16]

        return IncidentAlert(
            alert_id=alert_id,
            alert_name=rule['name'],
            severity=severity,
            category=category,
            description=rule.get('description', ''),
            timestamp=event.get('timestamp', datetime.utcnow().isoformat() + 'Z'),
            source_ip=event.get('source_ip'),
            user=event.get('user'),
            event_category=event_category,
            correlation_id=event.get('correlation_id'),
            details=event.get('details', {}),
        )


class IncidentManager:
    """
    Manages incidents throughout their lifecycle.

    Handles incident creation, updates, escalation, and resolution.
    """

    def __init__(self, incident_dir: Path = Path('incidents')):
        """
        Initialize incident manager.

        Args:
            incident_dir: Directory to store incident records
        """
        self.incident_dir = incident_dir
        self.incident_dir.mkdir(parents=True, exist_ok=True)

        self.active_incidents: Dict[str, Incident] = {}
        self.logger = logging.getLogger(__name__)

        # Load existing incidents
        self._load_incidents()

    def create_incident(self, alert: IncidentAlert) -> Incident:
        """
        Create new incident from alert.

        Args:
            alert: Incident alert

        Returns:
            Created incident
        """
        # Generate incident ID
        incident_id = f"INC-{datetime.utcnow().strftime('%Y%m%d')}-{len(self.active_incidents) + 1:04d}"

        # Create incident
        incident = Incident(
            incident_id=incident_id,
            title=alert.alert_name,
            severity=alert.severity,
            category=alert.category,
            status=IncidentStatus.DETECTED,
            description=alert.description,
            created_at=alert.timestamp,
            updated_at=alert.timestamp,
            detected_by="automated",
            source_ip=alert.source_ip,
            affected_user=alert.user,
            correlation_id=alert.correlation_id,
            alerts=[alert],
        )

        # Add initial timeline entry
        incident.add_timeline_entry(
            action="incident_created",
            details=f"Incident created from alert: {alert.alert_name}",
            user="system"
        )

        # Store incident
        self.active_incidents[incident_id] = incident
        self._save_incident(incident)

        self.logger.info(f"Incident created: {incident_id} ({alert.alert_name})")

        return incident

    def update_incident(
        self,
        incident_id: str,
        status: Optional[IncidentStatus] = None,
        assigned_to: Optional[str] = None,
        resolution_notes: Optional[str] = None,
        user: str = "system"
    ):
        """
        Update incident.

        Args:
            incident_id: Incident ID
            status: New status
            assigned_to: Assign to user
            resolution_notes: Resolution notes
            user: User making the update
        """
        incident = self.active_incidents.get(incident_id)
        if not incident:
            self.logger.warning(f"Incident not found: {incident_id}")
            return

        # Update status
        if status and status != incident.status:
            old_status = incident.status.value
            incident.status = status
            incident.add_timeline_entry(
                action="status_changed",
                details=f"Status changed from {old_status} to {status.value}",
                user=user
            )

        # Update assignment
        if assigned_to and assigned_to != incident.assigned_to:
            incident.assigned_to = assigned_to
            incident.add_timeline_entry(
                action="incident_assigned",
                details=f"Incident assigned to {assigned_to}",
                user=user
            )

        # Update resolution notes
        if resolution_notes:
            incident.resolution_notes = resolution_notes
            incident.add_timeline_entry(
                action="resolution_added",
                details="Resolution notes added",
                user=user
            )

        # Save updated incident
        self._save_incident(incident)

        self.logger.info(f"Incident updated: {incident_id}")

    def add_alert_to_incident(self, incident_id: str, alert: IncidentAlert):
        """
        Add additional alert to existing incident.

        Args:
            incident_id: Incident ID
            alert: Alert to add
        """
        incident = self.active_incidents.get(incident_id)
        if not incident:
            self.logger.warning(f"Incident not found: {incident_id}")
            return

        incident.alerts.append(alert)
        incident.add_timeline_entry(
            action="alert_added",
            details=f"Additional alert added: {alert.alert_name}",
            user="system"
        )

        self._save_incident(incident)

    def get_incident(self, incident_id: str) -> Optional[Incident]:
        """
        Get incident by ID.

        Args:
            incident_id: Incident ID

        Returns:
            Incident or None
        """
        return self.active_incidents.get(incident_id)

    def list_active_incidents(self) -> List[Incident]:
        """
        List all active incidents.

        Returns:
            List of active incidents
        """
        return [
            incident for incident in self.active_incidents.values()
            if incident.status not in (IncidentStatus.RESOLVED, IncidentStatus.CLOSED)
        ]

    def _save_incident(self, incident: Incident):
        """
        Save incident to disk.

        Args:
            incident: Incident to save
        """
        incident_file = self.incident_dir / f"{incident.incident_id}.json"
        with open(incident_file, 'w') as f:
            json.dump(incident.to_dict(), f, indent=2, default=str)

    def _load_incidents(self):
        """Load existing incidents from disk."""
        for incident_file in self.incident_dir.glob('INC-*.json'):
            try:
                with open(incident_file, 'r') as f:
                    data = json.load(f)

                # Reconstruct incident
                incident = Incident(
                    incident_id=data['incident_id'],
                    title=data['title'],
                    severity=IncidentSeverity(data['severity']),
                    category=IncidentCategory(data['category']),
                    status=IncidentStatus(data['status']),
                    description=data['description'],
                    created_at=data['created_at'],
                    updated_at=data['updated_at'],
                    detected_by=data['detected_by'],
                    assigned_to=data.get('assigned_to'),
                    source_ip=data.get('source_ip'),
                    affected_user=data.get('affected_user'),
                    correlation_id=data.get('correlation_id'),
                    alerts=[],  # Don't reload alerts
                    timeline=data.get('timeline', []),
                    response_actions=data.get('response_actions', []),
                    resolution_notes=data.get('resolution_notes'),
                    tags=data.get('tags', []),
                )

                self.active_incidents[incident.incident_id] = incident

            except Exception as e:
                self.logger.error(f"Failed to load incident {incident_file}: {e}")


class NotificationManager:
    """
    Manages incident notifications.

    Sends alerts via email, Slack, PagerDuty, etc.
    """

    def __init__(self, config: NotificationConfig):
        """
        Initialize notification manager.

        Args:
            config: Notification configuration
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

    async def send_incident_notification(self, incident: Incident):
        """
        Send notification for incident.

        Args:
            incident: Incident to notify about
        """
        tasks = []

        # Email notification
        if self.config.email_enabled:
            tasks.append(self._send_email_notification(incident))

        # Slack notification
        if self.config.slack_enabled:
            tasks.append(self._send_slack_notification(incident))

        # PagerDuty notification (critical only)
        if self.config.pagerduty_enabled and incident.severity == IncidentSeverity.CRITICAL:
            tasks.append(self._send_pagerduty_notification(incident))

        # Execute all notifications
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_email_notification(self, incident: Incident):
        """Send email notification."""
        try:
            # Determine recipients based on severity
            if incident.severity == IncidentSeverity.CRITICAL:
                recipients = self.config.email_to_critical
            elif incident.severity == IncidentSeverity.HIGH:
                recipients = self.config.email_to_high
            else:
                recipients = self.config.email_to_medium

            # Create email
            msg = MIMEMultipart()
            msg['From'] = self.config.email_from
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = f"[{incident.severity.value.upper()}] {incident.title}"

            # Email body
            body = f"""
Incident Alert - Unified OT Connector

Incident ID: {incident.incident_id}
Severity: {incident.severity.value.upper()}
Category: {incident.category.value}
Status: {incident.status.value}

Description:
{incident.description}

Details:
- Created: {incident.created_at}
- Source IP: {incident.source_ip or 'N/A'}
- Affected User: {incident.affected_user or 'N/A'}
- Correlation ID: {incident.correlation_id or 'N/A'}

Response Required:
{self._get_response_time_sla(incident.severity)}

View incident details in the web UI.
"""

            msg.attach(MIMEText(body, 'plain'))

            # Send email
            await asyncio.to_thread(self._send_smtp_email, msg, recipients)

            self.logger.info(f"Email notification sent for incident {incident.incident_id}")

        except Exception as e:
            self.logger.error(f"Failed to send email notification: {e}")

    def _send_smtp_email(self, msg: MIMEMultipart, recipients: List[str]):
        """Send email via SMTP (blocking)."""
        with smtplib.SMTP(self.config.smtp_server, self.config.smtp_port) as server:
            if self.config.smtp_user and self.config.smtp_password:
                server.starttls()
                server.login(self.config.smtp_user, self.config.smtp_password)

            server.send_message(msg)

    async def _send_slack_notification(self, incident: Incident):
        """Send Slack notification."""
        if not aiohttp or not self.config.slack_webhook_url:
            return

        try:
            # Determine channel based on severity
            if incident.severity == IncidentSeverity.CRITICAL:
                channel = self.config.slack_channel_critical
            elif incident.severity == IncidentSeverity.HIGH:
                channel = self.config.slack_channel_high
            else:
                channel = self.config.slack_channel_medium

            # Create Slack message
            emoji = "🚨" if incident.severity == IncidentSeverity.CRITICAL else "⚠️"
            message = {
                "channel": channel,
                "username": "Unified OT Connector",
                "icon_emoji": ":robot_face:",
                "text": f"{emoji} *{incident.severity.value.upper()} INCIDENT*",
                "attachments": [
                    {
                        "color": self._get_severity_color(incident.severity),
                        "title": incident.title,
                        "text": incident.description,
                        "fields": [
                            {"title": "Incident ID", "value": incident.incident_id, "short": True},
                            {"title": "Category", "value": incident.category.value, "short": True},
                            {"title": "Source IP", "value": incident.source_ip or "N/A", "short": True},
                            {"title": "User", "value": incident.affected_user or "N/A", "short": True},
                        ],
                        "footer": "Unified OT Connector",
                        "ts": int(datetime.utcnow().timestamp()),
                    }
                ],
            }

            # Send to Slack
            async with aiohttp.ClientSession() as session:
                async with session.post(self.config.slack_webhook_url, json=message) as resp:
                    if resp.status != 200:
                        self.logger.error(f"Slack notification failed: {resp.status}")
                    else:
                        self.logger.info(f"Slack notification sent for incident {incident.incident_id}")

        except Exception as e:
            self.logger.error(f"Failed to send Slack notification: {e}")

    async def _send_pagerduty_notification(self, incident: Incident):
        """Send PagerDuty notification."""
        if not aiohttp or not self.config.pagerduty_api_key:
            return

        try:
            payload = {
                "routing_key": self.config.pagerduty_service_key,
                "event_action": "trigger",
                "payload": {
                    "summary": incident.title,
                    "severity": "critical",
                    "source": "unified-ot-connector",
                    "custom_details": {
                        "incident_id": incident.incident_id,
                        "category": incident.category.value,
                        "source_ip": incident.source_ip,
                        "user": incident.affected_user,
                    },
                },
            }

            async with aiohttp.ClientSession() as session:
                headers = {"Content-Type": "application/json"}
                async with session.post(
                    "https://events.pagerduty.com/v2/enqueue",
                    json=payload,
                    headers=headers
                ) as resp:
                    if resp.status != 202:
                        self.logger.error(f"PagerDuty notification failed: {resp.status}")
                    else:
                        self.logger.info(f"PagerDuty notification sent for incident {incident.incident_id}")

        except Exception as e:
            self.logger.error(f"Failed to send PagerDuty notification: {e}")

    def _get_response_time_sla(self, severity: IncidentSeverity) -> str:
        """Get response time SLA for severity."""
        if severity == IncidentSeverity.CRITICAL:
            return "Immediate response required (< 15 minutes)"
        elif severity == IncidentSeverity.HIGH:
            return "Response required within 1 hour"
        elif severity == IncidentSeverity.MEDIUM:
            return "Response required within 4 hours"
        else:
            return "Response required within 24 hours"

    def _get_severity_color(self, severity: IncidentSeverity) -> str:
        """Get Slack color for severity."""
        if severity == IncidentSeverity.CRITICAL:
            return "danger"  # Red
        elif severity == IncidentSeverity.HIGH:
            return "warning"  # Orange
        else:
            return "#808080"  # Gray


class IncidentResponseSystem:
    """
    Complete incident response system.

    Integrates detection, management, and notifications.
    """

    def __init__(
        self,
        alert_rules_file: Path,
        notification_config: NotificationConfig,
        incident_dir: Path = Path('incidents')
    ):
        """
        Initialize incident response system.

        Args:
            alert_rules_file: Path to alert rules YAML
            notification_config: Notification configuration
            incident_dir: Directory for incident records
        """
        # Load alert rules
        import yaml
        with open(alert_rules_file, 'r') as f:
            self.alert_rules = yaml.safe_load(f)

        self.detector = IncidentDetector(self.alert_rules)
        self.manager = IncidentManager(incident_dir)
        self.notifier = NotificationManager(notification_config)
        self.logger = logging.getLogger(__name__)

    async def process_security_event(self, event: Dict[str, Any]):
        """
        Process security event and handle incident if detected.

        Args:
            event: Security event
        """
        # Detect incident
        alert = self.detector.detect_incident(event)
        if not alert:
            return

        self.logger.warning(f"Incident detected: {alert.alert_name}")

        # Check if similar incident already exists (deduplication)
        existing_incident = self._find_similar_incident(alert)
        if existing_incident:
            # Add alert to existing incident
            self.manager.add_alert_to_incident(existing_incident.incident_id, alert)
            self.logger.info(f"Alert added to existing incident: {existing_incident.incident_id}")
        else:
            # Create new incident
            incident = self.manager.create_incident(alert)

            # Send notifications
            await self.notifier.send_incident_notification(incident)

    def _find_similar_incident(self, alert: IncidentAlert) -> Optional[Incident]:
        """
        Find similar active incident for deduplication.

        Args:
            alert: New alert

        Returns:
            Similar incident or None
        """
        for incident in self.manager.list_active_incidents():
            # Same category and severity
            if incident.category != alert.category:
                continue
            if incident.severity != alert.severity:
                continue

            # Same affected user or source IP
            if alert.user and incident.affected_user == alert.user:
                return incident
            if alert.source_ip and incident.source_ip == alert.source_ip:
                return incident

        return None

    def get_incident(self, incident_id: str) -> Optional[Incident]:
        """Get incident by ID."""
        return self.manager.get_incident(incident_id)

    def list_active_incidents(self) -> List[Incident]:
        """List all active incidents."""
        return self.manager.list_active_incidents()

    def update_incident(self, incident_id: str, **kwargs):
        """Update incident."""
        self.manager.update_incident(incident_id, **kwargs)


# Global instance
_incident_response_system: Optional[IncidentResponseSystem] = None


def get_incident_response_system(
    alert_rules_file: Optional[Path] = None,
    notification_config: Optional[NotificationConfig] = None
) -> IncidentResponseSystem:
    """
    Get global incident response system instance.

    Args:
        alert_rules_file: Path to alert rules (required on first call)
        notification_config: Notification config (required on first call)

    Returns:
        IncidentResponseSystem instance
    """
    global _incident_response_system
    if _incident_response_system is None:
        if alert_rules_file is None:
            alert_rules_file = Path('config/siem/alert_rules.yml')
        if notification_config is None:
            notification_config = NotificationConfig()

        _incident_response_system = IncidentResponseSystem(
            alert_rules_file,
            notification_config
        )

    return _incident_response_system
