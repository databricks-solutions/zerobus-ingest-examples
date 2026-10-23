"""
Authentication module for Web UI: a Databricks workspace's OAuth, or client certificates.

Supports:
- OAuth2 authentication with a Databricks workspace (the only provider; MFA is the workspace identity
  provider's business: the connector can't see how a login was authenticated)
- Client Certificate authentication (mutual TLS)
- Session management with encrypted cookies
- Login/logout flows
"""

import logging
import os
from typing import Dict, Any, Optional
from urllib.parse import urlencode, urlsplit

from aiohttp import web
from aiohttp_session import setup as setup_session, get_session, new_session
from aiohttp_session.cookie_storage import EncryptedCookieStorage
from cryptography import fernet

# Import custom Databricks OAuth client
from sparkplug_connector.web.databricks_oauth import DatabricksOAuthClient, generate_state

logger = logging.getLogger(__name__)

PENDING_LOGINS = 5      # logins in progress per browser session (each its own OAuth state)


class AuthenticationManager:
    """Manages OAuth2 authentication and session handling."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize authentication manager.

        Args:
            config: Authentication configuration from config.yaml
        """
        self.config = config
        self.enabled = config.get('enabled', False)

        if not self.enabled:
            logger.info("Authentication disabled")
            return

        self.method = config.get('method', 'oauth2')  # 'oauth2' or 'client_cert'
        self.oauth_config = config.get('oauth', {})
        self.session_config = config.get('session', {})
        self.rbac_config = config.get('rbac', {})

        # Client certificate settings
        self.client_cert_config = config.get('client_cert', {})

        # OAuth2 settings
        self.provider = self.oauth_config.get('provider', 'databricks')
        self.client_id = self._get_env_or_config('client_id')
        self.client_secret = self._get_env_or_config('client_secret')
        self.tenant_id = self._get_env_or_config('tenant_id', '')
        self.redirect_uri = self.oauth_config.get('redirect_uri', 'http://localhost:8082/login/callback')
        self.scopes = self.oauth_config.get('scopes', ['openid', 'email', 'profile'])

        if self.enabled and self.method == 'oauth2':
            if self.provider != 'databricks':
                raise ValueError(f"web_ui.authentication.oauth.provider: only 'databricks' (a Databricks workspace's "
                                 f"OAuth) is supported, not {self.provider!r}")
            if config.get('require_mfa'):
                raise ValueError("web_ui.authentication.require_mfa: the connector can't see how a Databricks login "
                                 "was authenticated; require MFA in the workspace's identity provider (single sign-on)")

        # Session settings
        self.session_secret = self._get_env_or_config('secret_key', key_path='session')
        self.session_max_age = self.session_config.get('max_age_seconds', 28800)  # 8 hours

        # OAuth client (initialized in setup())
        self.oauth = None
        self.oauth_client = None

        logger.info(f"Authentication manager initialized: method={self.method}, provider={self.provider}")

    def _get_env_or_config(self, key: str, default: str = '', key_path: Optional[str] = None) -> str:
        """
        Get value from environment variable or config, with fallback.

        Args:
            key: Configuration key
            default: Default value if not found
            key_path: Path in config dict (e.g., 'session' for session.secret_key)

        Returns:
            Configuration value
        """
        # Try environment variable first (uppercase with OAUTH_ prefix)
        env_key = f"OAUTH_{key.upper()}"
        env_value = os.getenv(env_key)
        if env_value:
            return env_value

        # Try SESSION_ prefix for session keys
        if key_path == 'session':
            env_key = f"SESSION_{key.upper()}"
            env_value = os.getenv(env_key)
            if env_value:
                return env_value

        # Try config dict
        config_dict = self.session_config if key_path == 'session' else self.oauth_config
        config_value = config_dict.get(key, '')

        # Check if config value is ${env:VAR_NAME} format
        if isinstance(config_value, str) and config_value.startswith('${env:'):
            env_var = config_value[6:-1]  # Extract VAR_NAME from ${env:VAR_NAME}
            return os.getenv(env_var, default)

        return config_value or default

    async def setup(self, app: web.Application):
        """
        Set up authentication for the application.

        Args:
            app: aiohttp Application instance
        """
        if not self.enabled:
            logger.info("Authentication disabled, skipping setup")
            return

        # Set up session management with encrypted cookies
        secret_key = self.session_secret
        logger.info(f"Session secret loaded: {type(secret_key)}, length={len(str(secret_key)) if secret_key else 0}")
        if not secret_key:
            logger.error("Session secret key not configured! Set SESSION_SECRET_KEY environment variable.")
            raise ValueError("Session secret key required for authentication")

        # Convert secret key to Fernet key format (32 url-safe base64-encoded bytes)
        import base64
        import hashlib
        # Derive Fernet key from session secret using SHA256
        key_material = hashlib.sha256(secret_key.encode('utf-8')).digest()
        fernet_key = base64.urlsafe_b64encode(key_material)
        logger.info(f"Fernet key derived: {len(fernet_key)} bytes")

        # Create Fernet cipher instance (EncryptedCookieStorage can accept Fernet object directly)
        fernet_cipher = fernet.Fernet(fernet_key)
        logger.info("✓ Fernet cipher created successfully")

        storage = EncryptedCookieStorage(fernet_cipher, max_age=self.session_max_age)
        setup_session(app, storage)

        app['auth_manager'] = self
        if self.method != 'oauth2':           # client_cert: the role comes from the certificate, no OAuth
            logger.info(f"✓ Authentication: {self.method}")
            return

        # A Databricks workspace's OAuth (the only provider, checked in __init__)
        workspace_host = self.oauth_config.get('workspace_host', '')
        if not workspace_host:
            raise ValueError("workspace_host required for Databricks OAuth")
        self.oauth_client = DatabricksOAuthClient(
            client_id=self.client_id,
            client_secret=self.client_secret,
            workspace_host=workspace_host,
            redirect_uri=self.redirect_uri,
            scopes=self.scopes
        )
        logger.info(f"✓ Databricks OAuth configured: workspace={workspace_host}")

    async def handle_login(self, request: web.Request) -> web.Response:
        """
        Handle login initiation - redirect to OAuth provider.

        Args:
            request: aiohttp Request

        Returns:
            Redirect response to OAuth provider
        """
        if self.method != 'oauth2':
            return web.Response(text="No OAuth login: web_ui.authentication.method is client_cert", status=404)
        # The session cookie must be set on the host the provider sends the user back to (the redirect URI's):
        # a login started on another name of the same server (127.0.0.1 vs localhost) moves there first
        callback = urlsplit(self.redirect_uri)
        if callback.netloc and request.host != callback.netloc:
            return web.HTTPFound(f"{callback.scheme}://{callback.netloc}/login")

        # Generate state for CSRF protection
        state = generate_state()

        # Store state in session: the last few, so a second login (another tab, a retry) doesn't invalidate
        # one still in progress
        session = await get_session(request)
        session['oauth_states'] = (list(session.get('oauth_states') or []) + [state])[-PENDING_LOGINS:]

        # Get authorization URL
        auth_url = self.oauth_client.get_authorization_url(state)
        logger.info(f"Redirecting to Databricks OAuth: {auth_url}")

        return web.HTTPFound(auth_url)

    async def handle_oauth_callback(self, request: web.Request) -> web.Response:
        """
        Handle OAuth callback after user authentication.

        Args:
            request: aiohttp Request with authorization code

        Returns:
            Redirect to home page or error page
        """
        try:
            if self.method != 'oauth2':
                return web.Response(text="No OAuth login: web_ui.authentication.method is client_cert", status=404)
            # Get authorization code and state from query params
            code = request.query.get('code')
            state = request.query.get('state')

            if not code:
                return web.Response(text="Missing authorization code", status=400)

            # Verify state (CSRF protection): one this browser's session started, used once
            session = await get_session(request)
            pending = list(session.get('oauth_states') or [])
            if not state or state not in pending:
                logger.warning(f"OAuth state mismatch: {len(pending)} pending in this session, got={state}")
                return web.Response(text="Invalid OAuth state (CSRF check failed)", status=403)
            pending.remove(state)
            session['oauth_states'] = pending

            # Exchange code for token
            token_data = await self.oauth_client.exchange_code_for_token(code)
            if not token_data:
                return web.Response(text="Failed to exchange authorization code", status=500)

            access_token = token_data.get('access_token')

            # Get user info (the workspace checks the token)
            userinfo = await self.oauth_client.get_user_info(access_token)
            if not userinfo:
                return web.Response(text="Failed to get user information", status=500)

            user_email = userinfo.get('email') or userinfo.get('preferred_username') or userinfo.get('sub')
            user_name = userinfo.get('name', user_email)
            user_groups = userinfo.get('groups', [])

            if not user_email:
                logger.error("No email in OAuth token")
                return web.Response(text="Authentication failed: No email in token", status=403)

            # Create user object with role (RBAC will be implemented separately)
            from sparkplug_connector.web.rbac import User
            user = User(
                email=user_email,
                name=user_name,
                groups=user_groups,
                role_mappings=self.rbac_config.get('role_mappings', {}),
                default_role=self.rbac_config.get('default_role', 'viewer')
            )

            # Create session
            session = await new_session(request)
            session['user'] = {
                'email': user.email,
                'name': user.name,
                'groups': user.groups,
                'role': user.role.value,
            }

            logger.info(f"User {user_email} logged in successfully (role={user.role.value})")

            # Redirect to home page
            return web.HTTPFound('/')

        except Exception as e:
            logger.error(f"OAuth callback failed: {e}", exc_info=True)
            return web.Response(text=f"Authentication failed: {str(e)}", status=500)

    async def handle_logout(self, request: web.Request) -> web.Response:
        """
        Handle logout - clear session.

        Args:
            request: aiohttp Request

        Returns:
            JSON for the web UI's fetch (a redirect would end at the provider, which fetch can't follow); for a
            browser form, a redirect to the login page (not /login, which would sign the user in again at once
            while the provider's own session lasts)
        """
        session = await get_session(request)
        user_email = session.get('user', {}).get('email', 'unknown')

        # Clear session
        session.invalidate()

        logger.info(f"User {user_email} logged out")

        if 'text/html' in request.headers.get('Accept', ''):
            return web.HTTPFound('/static/login.html')
        return web.json_response({'status': 'ok', 'message': 'Logged out'})

    async def get_auth_status(self, request: web.Request) -> web.Response:
        """
        Get current authentication status.

        Args:
            request: aiohttp Request

        Returns:
            JSON response with user info or 401 if not authenticated
        """
        session = await get_session(request)
        user_data = session.get('user')

        if not user_data:
            raise web.HTTPUnauthorized()

        return web.json_response({
            'authenticated': True,
            'user': user_data
        })


@web.middleware
async def auth_middleware(request: web.Request, handler):
    """
    Authentication middleware - check session or client certificate for protected routes.

    Args:
        request: aiohttp Request
        handler: Route handler

    Returns:
        Handler response or 401/302 if not authenticated
    """
    auth_manager = request.app.get('auth_manager')

    # Skip authentication if disabled
    if not auth_manager or not auth_manager.enabled:
        return await handler(request)

    # Public endpoints (no authentication required)
    public_paths = ['/login', '/login/callback', '/health', '/static']
    if any(request.path.startswith(path) for path in public_paths):
        return await handler(request)

    # Client certificate authentication
    if auth_manager.method == 'client_cert':
        user = await _authenticate_with_client_cert(request, auth_manager)
        if user:
            request['user'] = user
            return await handler(request)
        else:
            logger.warning(f"Client certificate authentication failed for {request.path}")
            raise web.HTTPUnauthorized(reason="Valid client certificate required")

    # OAuth/Session-based authentication
    # Check session
    session = await get_session(request)
    user_data = session.get('user')

    if not user_data:
        # Not authenticated
        logger.debug(f"Unauthenticated access attempt to {request.path}")

        # A page load is redirected to the login; anything else (the API, the browser's favicon or script
        # requests) gets 401: a redirect there would start another login and replace the one in progress
        if request.method == 'GET' and not request.path.startswith('/api/') and 'text/html' in request.headers.get('Accept', ''):
            return web.HTTPFound('/login')
        raise web.HTTPUnauthorized(reason="Authentication required")

    # Recreate User object and attach to request
    from sparkplug_connector.web.rbac import User, Role
    request['user'] = User(
        email=user_data['email'],
        name=user_data.get('name', user_data['email']),
        groups=user_data.get('groups', []),
        role_mappings={},  # Not needed, role already determined
        default_role='viewer'
    )
    request['user'].role = Role[user_data['role'].upper()]

    return await handler(request)


async def _authenticate_with_client_cert(request: web.Request, auth_manager) -> Optional['User']:
    """
    Authenticate user based on client certificate.

    Args:
        request: aiohttp Request
        auth_manager: AuthenticationManager instance

    Returns:
        User object if authenticated, None otherwise
    """
    # Get client certificate from SSL transport
    transport = request.transport
    if not transport:
        return None

    ssl_object = transport.get_extra_info('ssl_object')
    if not ssl_object:
        return None

    # Get peer certificate
    try:
        peer_cert_bin = ssl_object.getpeercert(binary_form=True)
        if not peer_cert_bin:
            return None

        # Parse certificate
        from cryptography import x509
        from cryptography.hazmat.backends import default_backend

        cert = x509.load_der_x509_certificate(peer_cert_bin, default_backend())

        # Extract subject fields
        subject = cert.subject
        cn = subject.get_attributes_for_oid(x509.oid.NameOID.COMMON_NAME)[0].value

        # Try to extract OU (organizational unit) for role mapping
        ou_attrs = subject.get_attributes_for_oid(x509.oid.NameOID.ORGANIZATIONAL_UNIT_NAME)
        ou = ou_attrs[0].value if ou_attrs else None

        # Map OU or CN to role
        role_mappings = auth_manager.client_cert_config.get('role_mappings', {})
        default_role = auth_manager.client_cert_config.get('default_role', 'viewer')

        # Try OU first, then CN pattern matching
        role_name = None
        if ou and ou in role_mappings:
            role_name = role_mappings[ou]
        else:
            # Check CN patterns (e.g., "client-admin" -> "admin")
            for pattern, role in role_mappings.items():
                if pattern in cn.lower():
                    role_name = role
                    break

        if not role_name:
            role_name = default_role

        # Create User object
        from sparkplug_connector.web.rbac import User
        user = User(
            email=cn,  # Use CN as email/identifier
            name=cn,
            groups=[ou] if ou else [],
            role_mappings={},
            default_role=default_role
        )

        # Set role from mapping
        from sparkplug_connector.web.rbac import Role
        user.role = Role[role_name.upper()]

        logger.info(f"Client certificate authenticated: CN={cn}, OU={ou}, role={role_name}")
        return user

    except Exception as e:
        logger.error(f"Failed to parse client certificate: {e}", exc_info=True)
        return None
