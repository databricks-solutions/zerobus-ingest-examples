"""The connector's web server: REST API and web page.

- Sparkplug B sources: add, edit, remove, start, stop
- ZeroBus target configuration and diagnostics
- Status, metrics and the Sparkplug health
- Optional login (OAuth2) with role-based permissions
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from aiohttp import web
import aiohttp_cors
from typing import Callable

# Authentication and authorization (off by default)
from sparkplug_connector.web.auth import AuthenticationManager, auth_middleware
from sparkplug_connector.web.rbac import (
    require_permission, require_role, Permission, Role, get_role_info
)
from sparkplug_connector.web.security_headers import (
    security_headers_middleware, log_security_headers_status
)
from sparkplug_connector.web.input_validation import validate_source_config, validate_source_name, ValidationError
from sparkplug_connector.web.correlation_middleware import correlation_id_middleware
from sparkplug_connector.core.structured_logging import get_structured_logger
from sparkplug_connector.core.tls_manager import create_ssl_context_from_config

logger = logging.getLogger(__name__)


SECRET_KEY_PARTS = ('password', 'secret', 'token', 'private_key', 'api_key')

NO_UI_HTML = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Sparkplug Zerobus connector</title></head>
<body style="font-family: sans-serif; margin: 2em">
<h1>Sparkplug Zerobus connector</h1>
<p>The web UI isn't built into this copy: run <code>npm ci &amp;&amp; npm run build</code> in <code>webui/</code>
(it builds into <code>sparkplug_connector/web/static/connector-ui/</code>), then restart the connector.</p>
<p>The API works without it: <a href="/health">/health</a>, <a href="/api/status">/api/status</a>,
<a href="/api/sparkplug/health">/api/sparkplug/health</a>.</p>
</body></html>
"""


def _masked(value: Any) -> Any:
    """Sources for the API: values of password/secret/token keys replaced (the forms never send them back)."""
    if isinstance(value, dict):
        return {k: ('********' if v and any(p in str(k).lower() for p in SECRET_KEY_PARTS) else _masked(v))
                for k, v in value.items()}
    if isinstance(value, list):
        return [_masked(v) for v in value]
    return value


def _same(a: Any, b: Any) -> bool:
    """A form value equal to the running one (the file may spell a number or switch as text)."""
    return a == b or (not isinstance(a, (dict, list)) and str(a).strip().lower() == str(b).strip().lower())


def _edited(entry: Dict[str, Any], current: Dict[str, Any], form: Dict[str, Any]) -> Dict[str, Any]:
    """A source's ``entry`` (the running config, or the file's, with its placeholders) after a form edit:
    fields the form doesn't send are kept; a sent field equal to the running value (``current``) keeps
    the entry's spelling; a changed one is set, an empty one removed; nested settings the same way."""
    out = dict(entry)
    for key, value in form.items():
        now = current.get(key)
        if isinstance(value, dict):
            out[key] = _edited(entry.get(key) if isinstance(entry.get(key), dict) else {},
                               now if isinstance(now, dict) else {}, value)
        elif key in entry and _same(value, now):
            continue
        elif value in ('', None):
            out.pop(key, None)
        else:
            out[key] = value
    return out


class WebServer:
    """Web server for configuration and monitoring."""

    def __init__(self, config: Dict[str, Any], bridge, config_path: Optional[Path] = None):
        """Initialize web server.

        Args:
            config: Application configuration
            bridge: Bridge instance
            config_path: the file the connector was started with (source changes are saved there;
                None: the package's default config.yaml)
        """
        self.config = config
        self.bridge = bridge
        self.config_path = Path(config_path) if config_path else None

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

        # Correlation IDs in the logs
        self.app.middlewares.append(correlation_id_middleware)
        logger.info("✓ Correlation ID tracking enabled")

        # Security headers (logged after the TLS setup)
        self.app.middlewares.append(security_headers_middleware)

        # Login (web_ui.authentication)
        auth_config = self.config.get('web_ui', {}).get('authentication', {})
        if auth_config.get('enabled', False):
            self.auth_manager = AuthenticationManager(auth_config)
            await self.auth_manager.setup(self.app)
            # Add auth middleware
            self.app.middlewares.append(auth_middleware)
            logger.info("✓ Authentication enabled")
        else:
            logger.warning("⚠️  Authentication disabled: anyone who reaches the web UI can change the configuration")

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
            # Authentication
            web.get('/login', self.handle_login),
            web.get('/login/callback', self.handle_oauth_callback),
            web.post('/logout', self.handle_logout),
            web.get('/api/auth/status', self.get_auth_status),
            web.get('/api/auth/permissions', self.get_user_permissions),
            web.get('/api/auth/roles', self.get_role_info),

            web.get('/api/sources', self.get_sources),
            web.post('/api/sources', self.add_source),
            web.delete('/api/sources/{name}', self.remove_source),
            web.put('/api/sources/{name}', self.update_source),

            web.post('/api/sources/{name}/start', self.start_source),
            web.post('/api/sources/{name}/stop', self.stop_source),
            web.post('/api/sources/{name}/rebirth', self.request_rebirth),

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
            web.get('/api/sparkplug/health', self.get_sparkplug_health),
            web.get('/api/sparkplug/edge-nodes', self.get_sparkplug_edge_nodes),
            web.get('/api/status', self.get_status),

            # Health
            web.get('/health', self.health_check),

            # Web UI
            web.get('/', self.serve_index),
        ]

        for route in routes:
            resource = self.app.router.add_route(route.method, route.path, route.handler)
            cors.add(resource)

        # Static assets for Web UI
        if static_dir.exists():
            self.app.router.add_static('/static/', path=str(static_dir), name='static')

        # HTTPS (web_ui.tls)
        if self.tls_config.get('enabled', False):
            try:
                self.ssl_context = create_ssl_context_from_config(self.tls_config)
                # Store HSTS config in app for security headers middleware
                self.app.hsts_config = self.tls_config.get('hsts', {})
                logger.info("✓ TLS/SSL enabled")
            except Exception as e:
                logger.error(f"Failed to setup TLS/SSL: {e}")
                logger.warning("⚠️  Falling back to HTTP")
                self.ssl_context = None
        else:
            logger.info("TLS/SSL disabled: the web UI is plain HTTP")

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
    async def get_sources(self, request: web.Request) -> web.Response:
        """Get configured data sources (passwords and secrets masked)."""
        sources = self.bridge.sources if self.bridge is not None else self.config.get('sources', [])
        return web.json_response({'sources': _masked(sources)})

    def _save_sources(self, change) -> None:
        """Applies ``change`` (sources -> sources) to the sources in the file the connector was started
        with, as written there: ``${env:...}``/``${credential:...}`` placeholders stay placeholders."""
        from sparkplug_connector.core.config_loader import ConfigLoader
        config_loader = ConfigLoader(config_path=self.config_path)
        config = config_loader.load_raw()
        config['sources'] = change(list(config.get('sources') or []))
        if not config_loader.save(config):
            raise RuntimeError(f"saving the configuration to {config_loader.config_path} failed")

    @require_permission(Permission.WRITE)
    async def add_source(self, request: web.Request) -> web.Response:
        """Add new data source."""
        try:
            data = await request.json()

            # Validate and sanitize input
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
            self._save_sources(lambda sources: sources + [data])

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
            self._save_sources(lambda sources: [s for s in sources if s.get('name') != source_name])

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

            # The form shows only some fields: the others (credentials, topics, TLS, ...) are kept
            current = next((s for s in self.bridge.sources if s.get('name') == source_name), None)
            if current is None:
                return web.json_response({'status': 'error', 'message': f"Source not found: {source_name}"}, status=404)
            await self.bridge.update_source(source_name, _edited(current, current, data))

            # Update config file (unchanged fields keep the file's spelling, e.g. a ${env:...} placeholder)
            self._save_sources(lambda sources: [
                _edited(s, current, data) if s.get('name') == source_name else s for s in sources])

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


    @require_permission(Permission.START_STOP)
    async def request_rebirth(self, request: web.Request) -> web.Response:
        """Ask an edge node for a rebirth (``{"group_id": ..., "edge_node_id": ...}``): the source sends an NCMD."""
        try:
            name = request.match_info['name']
            try:
                body = await request.json()
            except ValueError:
                body = None
            if not isinstance(body, dict):
                return web.json_response({'status': 'error', 'message': 'a JSON object with group_id and edge_node_id'},
                                         status=400)
            res = await self.bridge.request_rebirth(name, body.get('group_id'), body.get('edge_node_id'))
            code = res.pop('code', None)
            status = 200 if res['status'] == 'ok' else {'not_found': 404, 'invalid': 400}.get(code, 409)
            return web.json_response(res, status=status)
        except Exception as e:
            logger.error(f"Failed to request a rebirth: {e}", exc_info=True)
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
        from sparkplug_connector.core.credential_manager import CredentialManager

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
            # the endpoint may be written with or without https:// (the SDK takes both)
            zerobus_host = (urlparse(zerobus_endpoint).netloc or zerobus_endpoint).split(':', 1)[0]
            dns, tcp = await asyncio.gather(
                _dns_check(zerobus_host),
                _tcp_check(zerobus_host),
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
                from sparkplug_connector.core.confirmed_writer import ZerobusRowStreams

                streams = ZerobusRowStreams(zerobus_endpoint, workspace_host,
                                            f"{table['catalog']}.{table['schema']}.{table['table']}",
                                            auth.get('client_id', ''), auth.get('client_secret', ''))
                stream = await asyncio.wait_for(streams.open(), timeout=12.0)
                await stream.close()
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
            from sparkplug_connector.core.credential_manager import CredentialManager
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

            # Update config file (IMPORTANT: load raw to avoid persisting injected secrets): the file the
            # connector was started with, as written there (${env:...} placeholders stay placeholders)
            from sparkplug_connector.core.config_loader import ConfigLoader
            config_loader = ConfigLoader(config_path=self.config_path, credential_manager=cred_manager)
            config_raw = config_loader.load_raw()

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
    async def get_sparkplug_health(self, request: web.Request) -> web.Response:
        """Per Sparkplug source: host state, MQTT, rows, Zerobus writer, NBIRTH calls and write-ahead log."""
        try:
            return web.json_response(self.bridge.sparkplug_health())
        except Exception as e:
            logger.error(f"Failed to get Sparkplug health: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.READ)
    async def get_sparkplug_edge_nodes(self, request: web.Request) -> web.Response:
        """Per Sparkplug source: its edge nodes and devices, online or offline, since when."""
        try:
            return web.json_response(self.bridge.sparkplug_edge_nodes())
        except Exception as e:
            logger.error(f"Failed to get the Sparkplug edge nodes: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    @require_permission(Permission.READ)
    async def get_status(self, request: web.Request) -> web.Response:
        """Get connector status."""
        try:
            status = self.bridge.get_status()

            return web.json_response(status)
        except Exception as e:
            logger.error(f"Failed to get status: {e}", exc_info=True)
            return web.json_response({'status': 'error', 'message': str(e)}, status=500)

    async def health_check(self, request: web.Request) -> web.Response:
        """Health check endpoint."""
        return web.json_response({'status': 'healthy'})

    async def _auth_gate(self, request: web.Request) -> Optional[web.Response]:
        """Return a redirect response if auth is enabled and user not authenticated."""
        if self.auth_manager:
            user = request.get('user')
            if not user:
                return web.HTTPFound('/static/login.html')
        return None

    async def serve_index(self, request: web.Request) -> web.Response:
        """The React UI at / (a note on how to build it if the build is missing; the API works either way)."""
        gated = await self._auth_gate(request)
        if gated:
            return gated

        index_file = Path(__file__).parent / "static" / "connector-ui" / "index.html"
        if index_file.exists():
            return web.FileResponse(path=index_file)
        return web.Response(text=NO_UI_HTML, content_type='text/html')

    # Authentication Handlers

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
