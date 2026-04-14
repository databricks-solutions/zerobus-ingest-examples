"""Web UI Server for Unified Connector.

Provides REST API and web interface for:
- Protocol server discovery and configuration
- ZeroBus target configuration
- Data flow monitoring
- Credential management
- OAuth2 authentication and RBAC (NIS2 compliance)
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from aiohttp import web
import aiohttp_cors
from typing import Callable

# Authentication and authorization (NIS2 compliance)
from unified_connector.web.auth import AuthenticationManager, auth_middleware
from unified_connector.web.rbac import (
    require_permission, require_role, Permission, Role, get_role_info
)
from unified_connector.web.security_headers import (
    security_headers_middleware, log_security_headers_status
)
from unified_connector.web.input_validation import (
    validate_source_config, validate_source_name, validate_protocol,
    validate_host, validate_port, ValidationError
)
from unified_connector.web.correlation_middleware import correlation_id_middleware
from unified_connector.core.structured_logging import get_structured_logger
from unified_connector.core.tls_manager import create_ssl_context_from_config

logger = logging.getLogger(__name__)


class WebServer:
    """Web server for configuration and monitoring."""

    def __init__(self, config: Dict[str, Any], bridge, discovery):
        """Initialize web server.

        Args:
            config: Application configuration
            bridge: UnifiedBridge instance
            discovery: DiscoveryService instance
        """
        self.config = config
        self.bridge = bridge
        self.discovery = discovery

        web_ui_config = config.get('web_ui', {})
        self.host = web_ui_config.get('host', '0.0.0.0')
        self.port = web_ui_config.get('port', 8080)
        self.tls_config = web_ui_config.get('tls', {})

        self.app = None
        self.runner = None
        self.auth_manager = None
        self.security_logger = get_structured_logger()
        self.ssl_context = None

    async def start(self):
        """Start web server."""
        self.app = web.Application()

        static_dir = Path(__file__).parent / "static"

        # Setup correlation ID tracking (NIS2 compliance - Article 21.2(b))
        self.app.middlewares.append(correlation_id_middleware)
        logger.info("✓ Correlation ID tracking enabled")

        # Setup security headers (NIS2 compliance - Article 21.2(a))
        # Will be logged after TLS setup
        self.app.middlewares.append(security_headers_middleware)

        # Setup authentication (NIS2 compliance - Article 21.2(g))
        auth_config = self.config.get('web_ui', {}).get('authentication', {})
        if auth_config.get('enabled', False):
            self.auth_manager = AuthenticationManager(auth_config)
            await self.auth_manager.setup(self.app)
            # Add auth middleware
            self.app.middlewares.append(auth_middleware)
            logger.info("✓ Authentication enabled (NIS2 compliant)")
        else:
            logger.warning("⚠️  Authentication disabled - NOT NIS2 compliant!")

        # Setup CORS
        cors = aiohttp_cors.setup(self.app, defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers="*",
                allow_headers="*",
                allow_methods="*"
            )
        })

        # API Routes
        routes = [
            # Authentication (NIS2 compliance)
            web.get('/login', self.handle_login),
            web.get('/login/callback', self.handle_oauth_callback),
            web.post('/logout', self.handle_logout),
            web.get('/api/auth/status', self.get_auth_status),
            web.get('/api/auth/permissions', self.get_user_permissions),
            web.get('/api/auth/roles', self.get_role_info),

            # Discovery
            web.get('/api/discovery/servers', self.get_discovered_servers),
            web.post('/api/discovery/scan', self.trigger_discovery_scan),
            web.post('/api/discovery/test', self.test_server_connection),

            # Sources
            web.get('/api/sources', self.get_sources),
            web.post('/api/sources', self.add_source),
            web.delete('/api/sources/{name}', self.remove_source),
            web.put('/api/sources/{name}', self.update_source),

            web.post('/api/sources/{name}/start', self.start_source),
            web.post('/api/sources/{name}/stop', self.stop_source),

            # Bridge control
            web.post('/api/bridge/start', self.start_bridge),
            web.post('/api/bridge/stop', self.stop_bridge),

            # ZeroBus
            web.get('/api/zerobus/config', self.get_zerobus_config),
            web.post('/api/zerobus/config', self.update_zerobus_config),
            web.post('/api/zerobus/start', self.start_zerobus),
            web.post('/api/zerobus/stop', self.stop_zerobus),
            web.get('/api/zerobus/diagnostics', self.get_zerobus_diagnostics),

            # Monitoring
            web.get('/api/metrics', self.get_metrics),
            web.get('/api/status', self.get_status),
            web.get('/api/diagnostics/pipeline', self.get_pipeline_diagnostics),

            # Health
            web.get('/health', self.health_check),

            # Static files (Web UI)
            web.get('/legacy', self.serve_legacy_index),
            web.get('/', self.serve_index),
        ]

        for route in routes:
            resource = self.app.router.add_route(route.method, route.path, route.handler)
            cors.add(resource)

        # Static assets for Web UI
        if static_dir.exists():
            self.app.router.add_static('/static/', path=str(static_dir), name='static')

        # Setup TLS/SSL (NIS2 compliance - Article 21.2(h))
        if self.tls_config.get('enabled', False):
            try:
                self.ssl_context = create_ssl_context_from_config(self.tls_config)
                # Store HSTS config in app for security headers middleware
                self.app.hsts_config = self.tls_config.get('hsts', {})
                logger.info("✓ TLS/SSL enabled (NIS2 compliant)")
            except Exception as e:
                logger.error(f"Failed to setup TLS/SSL: {e}")
                logger.warning("⚠️  Falling back to HTTP - NOT NIS2 compliant!")
                self.ssl_context = None
        else:
            logger.warning("⚠️  TLS/SSL disabled - NOT NIS2 compliant!")

        # Log security headers status
        log_security_headers_status(https_enabled=self.ssl_context is not None)

        # Start server
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port, ssl_context=self.ssl_context)
        await site.start()

        protocol = "https" if self.ssl_context else "http"
        logger.info(f"Web UI started on {protocol}://{self.host}:{self.port}")

    async def stop(self):
        """Stop web server."""
        if self.runner:
            await self.runner.cleanup()
        logger.info("Web UI stopped")

    # API Handlers

    @require_permission(Permission.READ)
    async def get_discovered_servers(self, request: web.Request) -> web.Response:
        """Get list of discovered protocol servers."""
        protocol = request.query.get('protocol')

        proto_enum = None
        if protocol:
            try:
                from unified_connector.core.discovery import ProtocolType
                proto_enum = ProtocolType(protocol)
            except Exception:
                proto_enum = None

        servers = self.discovery.get_discovered_servers(protocol=proto_enum)

        return web.json_response({
            'servers': [s.to_dict() for s in servers],
            'count': len(servers)
        })

    @require_permission(Permission.WRITE)
    async def trigger_discovery_scan(self, request: web.Request) -> web.Response:
        """Trigger immediate discovery scan."""
        try:
            asyncio.create_task(self.discovery.discover_all())
            return web.json_response({'status': 'ok', 'message': 'Discovery scan started'})
        except Exception as e:
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.READ)
    async def test_server_connection(self, request: web.Request) -> web.Response:
        """Test connection to a specific server."""
        try:
            data = await request.json()
            protocol = data.get('protocol')
            host = data.get('host')
            port = data.get('port')

            if not all([protocol, host, port]):
                return web.json_response(
                    {'status': 'error', 'message': 'Missing required fields: protocol, host, port'},
                    status=400
                )

            # Validate input (NIS2 compliance - prevent injection attacks)
            try:
                protocol = validate_protocol(protocol)
                host = validate_host(host)
                port = validate_port(port)
            except ValidationError as e:
                return web.json_response(
                    {'status': 'error', 'message': f'Validation error: {str(e)}'},
                    status=400
                )

            try:
                from unified_connector.core.discovery import ProtocolType
                protocol_enum = ProtocolType(str(protocol))
            except Exception:
                return web.json_response({"status": "error", "message": f"Unknown protocol: {protocol}"}, status=400)

            result = await self.discovery.test_server_connection(protocol_enum, host, port)
            return web.json_response(result)

        except Exception as e:
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.READ)
    async def get_sources(self, request: web.Request) -> web.Response:
        """Get configured data sources."""
        sources = self.config.get('sources', [])
        return web.json_response({'sources': sources})

    @require_permission(Permission.WRITE)
    async def add_source(self, request: web.Request) -> web.Response:
        """Add new data source."""
        try:
            data = await request.json()

            # Validate and sanitize input (NIS2 compliance - prevent injection attacks)
            try:
                data = validate_source_config(data)
            except ValidationError as e:
                # Log validation failure with structured logging
                user = request.get('user')
                self.security_logger.validation_failed(
                    user=user.email if user else 'anonymous',
                    action='add_source',
                    reason=str(e),
                    input_data=str(data),
                    component='web_server',
                    source_ip=request.remote
                )
                return web.json_response(
                    {'status': 'error', 'message': f'Validation error: {str(e)}'},
                    status=400
                )

            # Add to bridge
            await self.bridge.add_source(
                source_name=data['name'],
                source_type=data['protocol'],
                source_config=data
            )

            # Update config file
            from unified_connector.core.config_loader import ConfigLoader
            config_loader = ConfigLoader()
            config = config_loader.load()
            config['sources'].append(data)
            config_loader.save(config)

            return web.json_response({'status': 'ok', 'message': f"Source '{data['name']}' added"})

        except Exception as e:
            logger.error(f"Failed to add source: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.DELETE)
    async def remove_source(self, request: web.Request) -> web.Response:
        """Remove data source."""
        try:
            source_name = request.match_info['name']

            # Remove from bridge
            await self.bridge.remove_source(source_name)

            # Update config file
            from unified_connector.core.config_loader import ConfigLoader
            config_loader = ConfigLoader()
            config = config_loader.load()
            config['sources'] = [s for s in config['sources'] if s.get('name') != source_name]
            config_loader.save(config)

            return web.json_response({'status': 'ok', 'message': f"Source '{source_name}' removed"})

        except Exception as e:
            logger.error(f"Failed to remove source: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.WRITE)
    async def update_source(self, request: web.Request) -> web.Response:
        """Update data source configuration."""
        try:
            source_name = request.match_info['name']

            # Validate source name from URL
            try:
                source_name = validate_source_name(source_name)
            except ValidationError as e:
                return web.json_response(
                    {'status': 'error', 'message': f'Invalid source name: {str(e)}'},
                    status=400
                )

            data = await request.json()

            # Validate and sanitize input
            try:
                data = validate_source_config(data)
            except ValidationError as e:
                return web.json_response(
                    {'status': 'error', 'message': f'Validation error: {str(e)}'},
                    status=400
                )

            # Remove and re-add source
            await self.bridge.remove_source(source_name)
            await self.bridge.add_source(
                source_name=data['name'],
                source_type=data['protocol'],
                source_config=data
            )

            # Update config file
            from unified_connector.core.config_loader import ConfigLoader
            config_loader = ConfigLoader()
            config = config_loader.load()
            config['sources'] = [
                data if s.get('name') == source_name else s
                for s in config['sources']
            ]
            config_loader.save(config)

            return web.json_response({'status': 'ok', 'message': f"Source '{source_name}' updated"})

        except Exception as e:
            logger.error(f"Failed to update source: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)



    @require_permission(Permission.START_STOP)
    async def start_source(self, request: web.Request) -> web.Response:
        """Start a single configured source."""
        try:
            name = request.match_info['name']
            res = await self.bridge.start_source(name)
            return web.json_response(res, status=200 if res.get('status') == 'ok' else 500)
        except Exception as e:
            logger.error(f"Failed to start source: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.START_STOP)
    async def stop_source(self, request: web.Request) -> web.Response:
        """Stop a single active source."""
        try:
            name = request.match_info['name']
            res = await self.bridge.stop_source(name)
            return web.json_response(res, status=200 if res.get('status') == 'ok' else 500)
        except Exception as e:
            logger.error(f"Failed to stop source: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)


    @require_permission(Permission.READ)
    async def get_zerobus_config(self, request: web.Request) -> web.Response:
        """Get ZeroBus configuration."""
        zerobus_config = self.config.get('zerobus', {})

        # Don't expose sensitive credentials
        safe_config = {**zerobus_config}
        if 'auth' in safe_config:
            safe_config['auth'] = {
                'client_id': safe_config['auth'].get('client_id', ''),
                'client_secret': '***' if safe_config['auth'].get('client_secret') else ''
            }

        return web.json_response(safe_config)


    @require_permission(Permission.READ)
    async def get_zerobus_diagnostics(self, request: web.Request) -> web.Response:
        """Return ZeroBus config diagnostics (no node metrics)."""
        import os
        import socket
        from urllib.parse import urlparse
        from unified_connector.core.credential_manager import CredentialManager

        deep = request.query.get('deep', '').lower() in ('1', 'true', 'yes')

        cfg = self.config.get('zerobus', {})
        default_target = cfg.get('default_target', {}) or {}
        auth = cfg.get('auth', {}) or {}

        workspace_host = cfg.get('workspace_host') or default_target.get('workspace_host')
        zerobus_endpoint = cfg.get('zerobus_endpoint')

        table = {
            'catalog': default_target.get('catalog'),
            'schema': default_target.get('schema'),
            'table': default_target.get('table'),
        }

        def _has_value(v: object) -> bool:
            return isinstance(v, str) and bool(v.strip())

        def _mask(v: object) -> str:
            if not _has_value(v):
                return ""
            s = str(v).strip()
            if len(s) <= 10:
                return "***"
            return f"{s[:4]}…{s[-4:]}"

        def _cred_present(key: str) -> bool:
            # Best-effort presence check for encrypted credential store.
            try:
                cm = CredentialManager()
                val = cm.get_credential(key)
                return _has_value(val)
            except Exception:
                return False

        env_client_id = os.getenv("CONNECTOR_ZEROBUS_CLIENT_ID", "").strip()
        env_client_secret = os.getenv("CONNECTOR_ZEROBUS_CLIENT_SECRET", "").strip()
        client_id_val = auth.get("client_id", "")
        client_secret_val = auth.get("client_secret", "")

        client_id_source = (
            "env"
            if env_client_id
            else "credential_store"
            if _cred_present("zerobus.client_id")
            else "config"
        )
        client_secret_source = (
            "env"
            if env_client_secret
            else "credential_store"
            if _cred_present("zerobus.client_secret")
            else "config"
        )

        result: dict[str, object] = {
            'enabled': bool(cfg.get('enabled', False)),
            'workspace_host': workspace_host or '',
            'zerobus_endpoint': zerobus_endpoint or '',
            'target_table': table,
            'auth': {
                'client_id_present': _has_value(client_id_val),
                'client_id_source': client_id_source,
                'client_id_masked': _mask(client_id_val),
                'client_secret_present': _has_value(client_secret_val),
                'client_secret_source': client_secret_source,
            },
            'master_password_set': bool(os.getenv('CONNECTOR_MASTER_PASSWORD')),
            'checks': {},
        }

        async def _dns_check(host: str) -> dict[str, object]:
            loop = asyncio.get_running_loop()
            try:
                addrs = await asyncio.wait_for(
                    loop.getaddrinfo(host, 443, type=socket.SOCK_STREAM),
                    timeout=1.5,
                )
                ips = sorted({a[4][0] for a in addrs})[:6]
                return {'ok': True, 'addresses': ips}
            except Exception as e:
                return {'ok': False, 'error': str(e)}

        async def _tcp_check(host: str) -> dict[str, object]:
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, 443),
                    timeout=2.0,
                )
                writer.close()
                await writer.wait_closed()
                return {'ok': True}
            except Exception as e:
                return {'ok': False, 'error': str(e)}

        checks: dict[str, object] = {}

        if _has_value(zerobus_endpoint):
            dns, tcp = await asyncio.gather(
                _dns_check(zerobus_endpoint),
                _tcp_check(zerobus_endpoint),
            )
            checks['zerobus_dns_ok'] = dns['ok']
            if dns.get('addresses'):
                checks['zerobus_dns_addresses'] = dns['addresses']
            if dns.get('error'):
                checks['zerobus_dns_error'] = dns['error']

            checks['zerobus_tcp_443_ok'] = tcp['ok']
            if tcp.get('error'):
                checks['zerobus_tcp_443_error'] = tcp['error']
        else:
            checks['zerobus_dns_ok'] = False
            checks['zerobus_tcp_443_ok'] = False

        if _has_value(workspace_host):
            try:
                host = urlparse(workspace_host).netloc or workspace_host
                if ':' in host:
                    host = host.split(':', 1)[0]

                dns, tcp = await asyncio.gather(
                    _dns_check(host),
                    _tcp_check(host),
                )

                checks['workspace_dns_ok'] = dns['ok']
                if dns.get('addresses'):
                    checks['workspace_dns_addresses'] = dns['addresses']
                if dns.get('error'):
                    checks['workspace_dns_error'] = dns['error']

                checks['workspace_tcp_443_ok'] = tcp['ok']
                if tcp.get('error'):
                    checks['workspace_tcp_443_error'] = tcp['error']
            except Exception as e:
                checks['workspace_parse_ok'] = False
                checks['workspace_parse_error'] = str(e)
        else:
            checks['workspace_dns_ok'] = False
            checks['workspace_tcp_443_ok'] = False

        if deep:
            try:
                from unified_connector.core.zerobus_client import ZeroBusClient

                zb_config = {
                    'zerobus': {
                        'enabled': True,
                        'workspace_host': workspace_host,
                        'zerobus_endpoint': zerobus_endpoint,
                        'target': table,
                        'auth': auth,
                        'batch': cfg.get('batch', {}),
                        'retry': cfg.get('retry', {}),
                        'circuit_breaker': cfg.get('circuit_breaker', {}),
                    }
                }

                client = ZeroBusClient(zb_config)
                await asyncio.wait_for(client.connect(protobuf_descriptor=None), timeout=12.0)
                await client.close()
                checks['deep_stream_create_ok'] = True
            except Exception as e:
                checks['deep_stream_create_ok'] = False
                checks['deep_stream_create_error'] = str(e)

        result['checks'] = checks
        return web.json_response(result)



    @require_permission(Permission.CONFIGURE)
    async def update_zerobus_config(self, request: web.Request) -> web.Response:
        """Update ZeroBus configuration."""
        try:
            data = await request.json()

            # Store credentials securely
            from unified_connector.core.credential_manager import CredentialManager
            cred_manager = CredentialManager()

            auth_in = (data.get('auth') or {}) if isinstance(data, dict) else {}
            warning: str | None = None

            client_id = (auth_in.get('client_id') or '').strip() if isinstance(auth_in, dict) else ''
            client_secret = (auth_in.get('client_secret') or '').strip() if isinstance(auth_in, dict) else ''

            stored_client_id = False
            stored_client_secret = False

            # client_id is not a secret, but we still prefer to store consistently.
            if client_id:
                stored_client_id = bool(cred_manager.update_credential('zerobus.client_id', client_id))
                if not stored_client_id:
                    # Fall back to saving client_id in config.yaml (safe)
                    warning = warning or "Client ID could not be stored in the credential store; it will be saved in config.yaml."

            # client_secret MUST NOT be written to config.yaml.
            if client_secret:
                stored_client_secret = bool(cred_manager.update_credential('zerobus.client_secret', client_secret))
                if not stored_client_secret:
                    warning = (
                        "Client secret was NOT stored (missing CONNECTOR_MASTER_PASSWORD). "
                        "Set CONNECTOR_MASTER_PASSWORD (recommended) or provide CONNECTOR_ZEROBUS_CLIENT_SECRET env var."
                    )

            # Update config file (IMPORTANT: load raw to avoid persisting injected secrets)
            from unified_connector.core.config_loader import ConfigLoader
            config_loader = ConfigLoader(credential_manager=cred_manager)
            config_raw = config_loader.load(inject_credentials=False)

            existing = config_raw.get('zerobus', {}) or {}
            merged = {**existing, **data}

            # Sanitize auth before persisting
            existing_auth = (existing.get('auth') or {}) if isinstance(existing, dict) else {}
            auth_out = dict(existing_auth) if isinstance(existing_auth, dict) else {}

            if client_id:
                auth_out['client_id'] = '${credential:zerobus.client_id}' if stored_client_id else client_id
            # Always keep client_secret as placeholder if user attempted to set it.
            if client_secret:
                auth_out['client_secret'] = '${credential:zerobus.client_secret}'

            merged['auth'] = auth_out
            config_raw['zerobus'] = merged

            ok = config_loader.save(config_raw)
            if not ok:
                return web.json_response({'status': 'error', 'message': 'Failed to save configuration'}, status=500)

            # Reload injected config into memory so GET/diagnostics reflect latest saved values
            self.config = config_loader.load(inject_credentials=True)
            try:
                # Update bridge config view (without restarting running tasks)
                self.bridge.config = self.config
                self.bridge.sources = self.config.get('sources', [])
                self.bridge.zerobus_config = self.config.get('zerobus', {}) or {}
                self.bridge.zerobus_config_enabled = bool(self.bridge.zerobus_config.get('enabled', False))
                self.bridge.batch_size = self.bridge.zerobus_config.get('batch', {}).get('max_records', 1000)
                self.bridge.batch_timeout_sec = self.bridge.zerobus_config.get('batch', {}).get('timeout_seconds', 5.0)
            except Exception:
                pass

            resp = {'status': 'ok', 'message': 'ZeroBus configuration updated'}
            if warning:
                resp['warning'] = warning
            return web.json_response(resp)

        except Exception as e:
            logger.error(f"Failed to update ZeroBus config: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)








    @require_permission(Permission.START_STOP)
    async def start_bridge(self, request: web.Request) -> web.Response:
        """Start protocol sources (bridge) in background."""
        try:
            asyncio.create_task(self.bridge.start_infra())
            return web.json_response({'status': 'ok', 'message': 'Bridge starting'})
        except Exception as e:
            logger.error(f"Failed to start bridge: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.START_STOP)
    async def stop_bridge(self, request: web.Request) -> web.Response:
        """Stop protocol sources (bridge)."""
        try:
            await self.bridge.stop()
            return web.json_response({'status': 'ok', 'message': 'Bridge stopped'})
        except Exception as e:
            logger.error(f"Failed to stop bridge: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)


    @require_permission(Permission.START_STOP)
    async def start_zerobus(self, request: web.Request) -> web.Response:
        """Start ZeroBus streaming."""
        try:
            result = await self.bridge.start_zerobus()
            return web.json_response(result)
        except Exception as e:
            logger.error(f"Failed to start ZeroBus: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.START_STOP)
    async def stop_zerobus(self, request: web.Request) -> web.Response:
        """Stop ZeroBus streaming."""
        try:
            result = await self.bridge.stop_zerobus()
            return web.json_response(result)
        except Exception as e:
            logger.error(f"Failed to stop ZeroBus: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.READ)
    async def get_metrics(self, request: web.Request) -> web.Response:
        """Get connector metrics."""
        try:
            metrics = self.bridge.get_metrics()
            return web.json_response(metrics)
        except Exception as e:
            logger.error(f"Failed to get metrics: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.READ)
    async def get_pipeline_diagnostics(self, request: web.Request) -> web.Response:
        """Get message transformation pipeline diagnostics.

        GET /api/diagnostics/pipeline

        Returns sample messages at each transformation stage.
        """
        try:
            diagnostics = self.bridge.get_pipeline_diagnostics()
            return web.json_response(diagnostics)
        except Exception as e:
            logger.error(f"Failed to get pipeline diagnostics: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.READ)
    async def get_status(self, request: web.Request) -> web.Response:
        """Get connector status."""
        try:
            status = self.bridge.get_status()

            # Add discovery stats (discovered servers are *reachable*, not necessarily configured/connected)
            try:
                servers = self.discovery.get_discovered_servers()
                status['discovery_enabled'] = True
                status['discovery_count'] = len(servers)
                by_proto = {}
                for s in servers:
                    proto = getattr(s, 'protocol', None)
                    key = proto.value if hasattr(proto, 'value') else str(proto)
                    by_proto[key] = by_proto.get(key, 0) + 1
                status['discovery_by_protocol'] = by_proto
            except Exception:
                status['discovery_enabled'] = False
            return web.json_response(status)
        except Exception as e:
            logger.error(f"Failed to get status: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    async def health_check(self, request: web.Request) -> web.Response:
        """Health check endpoint."""
        return web.json_response({'status': 'healthy'})

    def _legacy_index_html(self) -> str:
        # NOTE: Keep legacy HTML intact for parity/fallback at /legacy.
        return """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Unified OT Zerobus Connector Client</title>
  <link rel="stylesheet" href="/static/style.css" />
</head>
<body>
  <div class="container">
    <div class="topbar">
      <div class="title">
        <h1>Unified OT Zerobus Connector Client</h1>
      </div>
    </div>

    <div class="grid">
      <div class="card collapsible" style="grid-column: 1 / -1">
        <div class="card-header">
          <h2>Overview</h2>
          <div class="hint">High-level health + refresh</div>
          <button class="iconbtn" type="button" data-toggle="collapse" data-target="#overviewBody" aria-label="Toggle Overview">▾</button>
        </div>
        <div id="overviewBody" class="card-body" style="display:none">
          <div class="pillrow">
            <div id="badgeDiscovery" class="pill warn"><strong>Discovery: unknown</strong></div>
            <div id="badgeSources" class="pill warn"><strong>Sources: unknown</strong></div>
            <div id="badgeZerobus" class="pill warn"><strong>ZeroBus: unknown</strong></div>
            <button id="btnRefresh" class="btn btn-secondary" type="button">Refresh all</button>
          </div>
        </div>
      </div>
      <div class="card collapsible" style="grid-column: 1 / -1">
        <div class="card-header">
          <h2>Discovery</h2>
          <div class="hint">Scan and test protocol servers</div>
          <button class="iconbtn" type="button" data-toggle="collapse" data-target="#discoveryBody" aria-label="Toggle Discovery">▾</button>
        </div>
        <div id="discoveryBody" class="card-body" style="display:none">
          <div class="row" style="justify-content:flex-end">
            <button id="btnDiscoveryScan" class="btn btn-primary" type="button">Scan</button>
          </div>
          <div class="discovery-rows">
            <div class="discovery-row">
              <div class="row" style="justify-content: space-between; align-items: baseline">
                <div class="section-title" style="margin: 0">OPC-UA</div>
                <div id="discoveryCountOpcua" class="muted">—</div>
              </div>
              <table class="table table-compact" style="margin-top: 8px">
                <thead>
                  <tr>
                    <th>Host</th>
                    <th>Port</th>
                    <th>Endpoint</th>
                    <th>Reachable</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody id="discoveryTbodyOpcua"></tbody>
              </table>
            </div>

            <div class="discovery-row">
              <div class="row" style="justify-content: space-between; align-items: baseline">
                <div class="section-title" style="margin: 0">MQTT</div>
                <div id="discoveryCountMqtt" class="muted">—</div>
              </div>
              <table class="table table-compact" style="margin-top: 8px">
                <thead>
                  <tr>
                    <th>Host</th>
                    <th>Port</th>
                    <th>Endpoint</th>
                    <th>Reachable</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody id="discoveryTbodyMqtt"></tbody>
              </table>
            </div>

            <div class="discovery-row">
              <div class="row" style="justify-content: space-between; align-items: baseline">
                <div class="section-title" style="margin: 0">Modbus</div>
                <div id="discoveryCountModbus" class="muted">—</div>
              </div>
              <table class="table table-compact" style="margin-top: 8px">
                <thead>
                  <tr>
                    <th>Host</th>
                    <th>Port</th>
                    <th>Endpoint</th>
                    <th>Reachable</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody id="discoveryTbodyModbus"></tbody>
              </table>
            </div>
          </div>

          <div class="section-title">Discovery test result</div>
          <div id="discoveryTestResult" class="codebox">Click “Test” on a discovered endpoint to see detailed diagnostics here.</div>
        </div>
      </div>

      <!-- ZeroBus full width for better editing experience -->
      <div class="card collapsible" style="grid-column: 1 / -1">
        <div class="card-header">
          <h2>ZeroBus</h2>
          <div class="hint">ZeroBus configuration</div>
          <button class="iconbtn" type="button" data-toggle="collapse" data-target="#zerobusBody" aria-label="Toggle ZeroBus">▾</button>
        </div>
        <div id="zerobusBody" class="card-body" style="display:none">
          <div class="zerobus-fields split">
            <label class="full">
              Workspace host
              <input id="zbWorkspace" placeholder="https://adb-..." />
            </label>
            <label class="full">
              ZeroBus endpoint
              <input id="zbZerobusEndpoint" placeholder="<workspaceId>.zerobus.<region>.cloud.databricks.com" />
            </label>

            <label class="full">
              Target table (catalog.schema.table)
              <input id="zbTableFqn" placeholder="main.iot_data.sensor_readings" />
            </label>

            <label>
              Client ID
              <input id="zbClientId" placeholder="<oauth-client-id>" />
            </label>
            <label>
              Client secret
              <input id="zbClientSecret" type="password" placeholder="(leave blank to keep existing)" />
              <div id="zbSecretHint" class="muted" style="margin-top:6px"></div>
            </label>

            <label>
              Enabled
              <div class="row" style="gap:8px">
                <input id="zbEnabled" type="checkbox" style="width:18px;height:18px" />
                <span class="muted">Allow streaming when started</span>
              </div>
            </label>
          </div>

          <div class="section-title" style="margin-top: 20px">Proxy Settings (for Purdue Layer 3.5)</div>
          <div class="muted" style="margin-bottom: 12px">Configure proxy for outbound connections to Databricks cloud (Layer 4+). Leave blank to use environment variables.</div>
          <div class="zerobus-fields split">
            <label>
              Enable Proxy
              <div class="row" style="gap:8px">
                <input id="zbProxyEnabled" type="checkbox" style="width:18px;height:18px" />
                <span class="muted">Route cloud traffic through proxy</span>
              </div>
            </label>

            <label>
              Use Environment Variables
              <div class="row" style="gap:8px">
                <input id="zbProxyUseEnv" type="checkbox" style="width:18px;height:18px" checked />
                <span class="muted">Use HTTP_PROXY/HTTPS_PROXY env vars</span>
              </div>
            </label>

            <label class="full">
              HTTP Proxy
              <input id="zbProxyHttp" placeholder="http://proxy.company.com:8080" />
            </label>

            <label class="full">
              HTTPS Proxy
              <input id="zbProxyHttps" placeholder="http://proxy.company.com:8080" />
            </label>

            <label class="full">
              No Proxy
              <input id="zbProxyNoProxy" placeholder="localhost,127.0.0.1,.internal" />
              <div class="muted" style="margin-top:6px">Comma-separated list of hosts to bypass proxy</div>
            </label>

            <div class="row" style="grid-column: 1 / -1; margin-top: 6px">
              <button id="btnLoadZerobus" class="btn btn-secondary" type="button">Reload</button>
              <button id="btnSaveZerobus" class="btn btn-primary" type="button">Save</button>
              <button id="btnStartZerobus" class="btn btn-good" type="button">Start</button>
              <button id="btnStopZerobus" class="btn btn-warning" type="button">Stop</button>
              <button id="btnZerobusDiag" class="btn btn-secondary" type="button">Diagnostics</button>
            </div>
          </div>

          <div class="section-title">ZeroBus diagnostics</div>
          <div id="zbDiag" class="codebox">Click "Diagnostics" to run checks.</div>
</div>
          </div>
        </div>
      </div>

      <div class="card collapsible" style="grid-column: 1 / -1"> 
        <div class="card-header">
          <h2>Sources</h2>
          <div class="hint">Add/edit/delete connector sources</div>
          <button class="iconbtn" type="button" data-toggle="collapse" data-target="#sourcesBody" aria-label="Toggle Sources">▾</button>
        </div>
        <div id="sourcesBody" class="card-body" style="display:none">
          <div class="row" style="justify-content:flex-end; gap:8px">
            <button id="btnStartBridge" class="btn btn-secondary" type="button">Start bridge</button>
            <button id="btnStartSelected" class="btn btn-good" type="button">Start selected</button>
            <button id="btnStopSelected" class="btn btn-warning" type="button">Stop selected</button>
          </div>
          <div class="split">
            <form id="sourceForm">
              <input id="srcMode" type="hidden" value="create" />
              <input id="srcOriginalName" type="hidden" value="" />

              <div class="split">
                <label>Name <input id="srcName" placeholder="plant_opcua_server" required /></label>
                <label>
                  Protocol
                  <select id="srcProtocol">
                    <option value="opcua">OPC-UA</option>
                    <option value="mqtt">MQTT</option>
                    <option value="modbus">Modbus</option>
                  </select>
                </label>
              </div>

              <label>Endpoint <input id="srcEndpoint" placeholder="opc.tcp://192.168.1.100:4840" required /></label>

              <div class="split">
                <label>Site <input id="srcSite" placeholder="plant1" /></label>
                <label>Area <input id="srcArea" placeholder="production" /></label>
              </div>
              <div class="split">
                <label>Line <input id="srcLine" placeholder="line1" /></label>
                <label>Equipment <input id="srcEquipment" placeholder="plc1" /></label>
              </div>

              <label>
                <span>Enabled</span>
                <div class="row" style="gap:8px">
                  <input id="srcEnabled" type="checkbox" style="width:18px;height:18px" checked />
                  <span class="muted">Start source when connector runs</span>
                </div>
              </label>

              <div class="row" style="margin-top:10px">
                <button id="srcSubmit" class="btn btn-primary" type="submit">Add source</button>
                <button id="srcReset" class="btn btn-secondary" type="button">Reset</button>
              </div>

              <div class="section-title">Notes</div>
              <div class="muted">Editing a source updates the on-disk config and restarts the source inside the running bridge.</div>
            </form>

            <div>
              <div class="subcard">
                <div class="subcard-header">
                  <div class="section-title" style="margin: 0">Configured sources</div>
                  <div class="row" style="gap:8px">
                    <button id="btnRefreshSources" class="btn btn-secondary" type="button">Refresh</button>
                    <button class="iconbtn" type="button" data-toggle="collapse" data-target="#configuredSourcesBody" aria-label="Toggle Configured sources">▾</button>
                  </div>
                </div>
                <div id="configuredSourcesBody" class="subcard-body" style="display:none">
                  <table class="table" style="margin-top: 10px">
                    <thead>
                      <tr>
                        <th style="width:44px"><input id="srcSelectAll" type="checkbox" /></th>
                        <th>Name</th>
                        <th>Protocol</th>
                        <th>Endpoint</th>
                        <th>Active</th>
                        <th>Enabled</th>
                        <th>Action</th>
                        <th style="width:44px"></th>
                      </tr>
                    </thead>
                    <tbody id="sourcesTbody"></tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div class="card collapsible" style="grid-column: 1 / -1">
        <div class="card-header">
          <h2>Status</h2>
          <div class="hint">Live connector state</div>
          <button class="iconbtn" type="button" data-toggle="collapse" data-target="#statusBody" aria-label="Toggle Status">▾</button>
        </div>
        <div id="statusBody" class="card-body" style="display:none">
          <div class="row" style="justify-content: flex-end">
            <button id="btnRefreshStatus" class="btn btn-secondary" type="button">Refresh</button>
          </div>
          <div id="statusKVs" class="kvs"></div>
        </div>
      </div>

      <div class="card collapsible" style="grid-column: 1 / -1">
        <div class="card-header">
          <h2>Metrics</h2>
          <div class="hint">Counters & performance</div>
          <button class="iconbtn" type="button" data-toggle="collapse" data-target="#metricsBody" aria-label="Toggle Metrics">▾</button>
        </div>
        <div id="metricsBody" class="card-body" style="display:none">
          <div class="row" style="justify-content: flex-end">
            <button id="btnRefreshMetrics" class="btn btn-secondary" type="button">Refresh</button>
          </div>
          <div id="metricsKVs" class="kvs"></div>
        </div>
      </div>

      <div class="card collapsible" style="grid-column: 1 / -1">
        <div class="card-header">
          <h2>Pipeline Diagnostics</h2>
          <div class="hint">Message transformation pipeline: Raw Protocol → Vendor Detection → ISA-95 → ZeroBus</div>
          <button class="iconbtn" type="button" data-toggle="collapse" data-target="#pipelineBody" aria-label="Toggle Pipeline">▾</button>
        </div>
        <div id="pipelineBody" class="card-body" style="display:none">
          <div class="row" style="justify-content: space-between; margin-bottom: 16px">
            <div class="row" style="gap: 12px">
              <div id="vendorKepware" class="pill secondary"><strong>Kepware: 0</strong></div>
              <div id="vendorSparkplug" class="pill secondary"><strong>Sparkplug B: 0</strong></div>
              <div id="vendorHoneywell" class="pill secondary"><strong>Honeywell: 0</strong></div>
              <div id="vendorGeneric" class="pill secondary"><strong>Generic: 0</strong></div>
            </div>
            <button id="btnRefreshPipeline" class="btn btn-secondary" type="button">Refresh</button>
          </div>

          <div id="pipelineFlowContainer">
            <!-- Per-vendor pipelines will be dynamically rendered here -->
          </div>

          <div class="section-title" style="margin-top: 24px">Normalization Status</div>
          <div id="pipelineNormStatus" class="muted">ISA-95 normalization: checking...</div>
        </div>
      </div>
    </div>
  </div>

  <div id="toast" class="toast"></div>
  <script src="/static/app.js"></script>
</body>
</html>        """
    async def _auth_gate(self, request: web.Request) -> Optional[web.Response]:
        """Return a redirect response if auth is enabled and user not authenticated."""
        if self.auth_manager:
            user = request.get('user')
            if not user:
                return web.HTTPFound('/static/login.html')
        return None

    async def serve_legacy_index(self, request: web.Request) -> web.Response:
        """Serve legacy (vanilla JS) UI at /legacy."""
        gated = await self._auth_gate(request)
        if gated:
            return gated
        return web.Response(text=self._legacy_index_html(), content_type='text/html')

    async def serve_index(self, request: web.Request) -> web.Response:
        """Serve React UI at / (fallback to legacy if build missing)."""
        gated = await self._auth_gate(request)
        if gated:
            return gated

        # If React build exists, serve it. Otherwise fall back to legacy UI.
        static_dir = Path(__file__).parent / "static" / "connector-ui"
        index_file = static_dir / "index.html"
        if index_file.exists():
            return web.FileResponse(path=index_file)

        return await self.serve_legacy_index(request)

    # Authentication Handlers (NIS2 compliance)

    async def handle_login(self, request: web.Request) -> web.Response:
        """Redirect to OAuth provider for authentication."""
        if not self.auth_manager:
            return web.json_response(
                {'status': 'error', 'message': 'Authentication not enabled'},
                status=503
            )
        return await self.auth_manager.handle_login(request)

    async def handle_oauth_callback(self, request: web.Request) -> web.Response:
        """Handle OAuth callback after user authentication."""
        if not self.auth_manager:
            return web.json_response(
                {'status': 'error', 'message': 'Authentication not enabled'},
                status=503
            )
        return await self.auth_manager.handle_oauth_callback(request)

    async def handle_logout(self, request: web.Request) -> web.Response:
        """Logout user and clear session."""
        if not self.auth_manager:
            return web.json_response(
                {'status': 'error', 'message': 'Authentication not enabled'},
                status=503
            )
        return await self.auth_manager.handle_logout(request)

    async def get_auth_status(self, request: web.Request) -> web.Response:
        """Get current authentication status and user info."""
        auth_manager = request.app.get('auth_manager')
        auth_enabled = auth_manager and auth_manager.enabled if auth_manager else False

        user = request.get('user')
        if not user:
            return web.json_response({
                'auth_enabled': auth_enabled,
                'authenticated': False,
                'user': None
            })

        return web.json_response({
            'auth_enabled': auth_enabled,
            'authenticated': True,
            'user': user.to_dict()
        })

    async def get_user_permissions(self, request: web.Request) -> web.Response:
        """Get permissions for current user."""
        user = request.get('user')
        if not user:
            return web.json_response(
                {'status': 'error', 'message': 'Not authenticated'},
                status=401
            )

        return web.json_response({
            'role': user.role.value,
            'permissions': user.get_permissions()
        })

    async def get_role_info(self, request: web.Request) -> web.Response:
        """Get information about all roles and permissions."""
        return web.json_response(get_role_info())
