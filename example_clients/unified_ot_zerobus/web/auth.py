"""
Authentication module for Web UI using OAuth2/SAML/Client Certificates.

Supports:
- OAuth2 authentication (Azure AD, Okta, Google, Databricks)
- Client Certificate authentication (mutual TLS)
- MFA verification from OAuth tokens
- Session management with encrypted cookies
- Login/logout flows

NIS2 Compliance: Article 21.2(g) - Access Control & MFA
"""

import logging
import os
from typing import Dict, Any, Optional
from urllib.parse import urlencode

from aiohttp import web
from aiohttp_session import setup as setup_session, get_session, new_session
from aiohttp_session.cookie_storage import EncryptedCookieStorage
try:
    from authlib.integrations.aiohttp_client import OAuth
except ImportError:
    # authlib 1.6+ doesn't have aiohttp_client integration
    # For testing purposes, we'll use a placeholder
    # In production, consider using authlib.integrations.base_client
    OAuth = None
from cryptography import fernet

# Import custom Databricks OAuth client
from unified_connector.web.databricks_oauth import DatabricksOAuthClient, generate_state

logger = logging.getLogger(__name__)


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
        self.provider = self.oauth_config.get('provider', 'azure')
        self.client_id = self._get_env_or_config('client_id')
        self.client_secret = self._get_env_or_config('client_secret')
        self.tenant_id = self._get_env_or_config('tenant_id', '')
        self.redirect_uri = self.oauth_config.get('redirect_uri', 'http://localhost:8082/login/callback')
        self.scopes = self.oauth_config.get('scopes', ['openid', 'email', 'profile'])

        # MFA settings
        self.require_mfa = config.get('require_mfa', False)
        mfa_config = config.get('mfa', {})
        self.mfa_claim_name = mfa_config.get('claim_name', 'amr')
        self.mfa_required_methods = mfa_config.get('required_methods', ['mfa', 'otp', 'duo'])

        # Session settings
        self.session_secret = self._get_env_or_config('secret_key', key_path='session')
        self.session_max_age = self.session_config.get('max_age_seconds', 28800)  # 8 hours

        # OAuth client (initialized in setup())
        self.oauth = None
        self.oauth_client = None

        logger.info(f"Authentication manager initialized: provider={self.provider}, mfa_required={self.require_mfa}")

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

        # Handle Databricks provider with custom implementation
        if self.provider == 'databricks':
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
            # Store auth manager in app
            app['auth_manager'] = self
            logger.info(f"✓ Databricks OAuth configured: workspace={workspace_host}")
            return

        # For other providers, check if OAuth is available
        if OAuth is None:
            logger.error("OAuth integration not available (authlib.integrations.aiohttp_client not found)")
            logger.error("Authentication cannot be set up for non-Databricks providers.")
            logger.error("Either use provider='databricks' or install authlib with aiohttp support.")
            raise ImportError("authlib.integrations.aiohttp_client not available")

        # Set up OAuth2
        self.oauth = OAuth()

        if self.provider == 'azure':
            # Azure AD configuration
            authority = f"https://login.microsoftonline.com/{self.tenant_id}" if self.tenant_id else "https://login.microsoftonline.com/common"
            self.oauth.register(
                name='azure',
                client_id=self.client_id,
                client_secret=self.client_secret,
                server_metadata_url=f"{authority}/v2.0/.well-known/openid-configuration",
                client_kwargs={
                    'scope': ' '.join(self.scopes),
                    'prompt': 'select_account'
                }
            )
            self.oauth_client = self.oauth.azure
            logger.info(f"OAuth2 configured for Azure AD: tenant={self.tenant_id}")

        elif self.provider == 'okta':
            # Okta configuration
            okta_domain = self.oauth_config.get('okta_domain', '')
            self.oauth.register(
                name='okta',
                client_id=self.client_id,
                client_secret=self.client_secret,
                server_metadata_url=f"https://{okta_domain}/.well-known/openid-configuration",
                client_kwargs={'scope': ' '.join(self.scopes)}
            )
            self.oauth_client = self.oauth.okta
            logger.info(f"OAuth2 configured for Okta: domain={okta_domain}")

        elif self.provider == 'google':
            # Google configuration
            self.oauth.register(
                name='google',
                client_id=self.client_id,
                client_secret=self.client_secret,
                server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
                client_kwargs={'scope': ' '.join(self.scopes)}
            )
            self.oauth_client = self.oauth.google
            logger.info("OAuth2 configured for Google")

        else:
            raise ValueError(f"Unsupported OAuth provider: {self.provider}")

        # Store auth manager in app
        app['auth_manager'] = self

        logger.info("✓ Authentication setup complete")

    async def handle_login(self, request: web.Request) -> web.Response:
        """
        Handle login initiation - redirect to OAuth provider.

        Args:
            request: aiohttp Request

        Returns:
            Redirect response to OAuth provider
        """
        # For Databricks OAuth, use custom client
        if self.provider == 'databricks':
            # Generate state for CSRF protection
            state = generate_state()

            # Store state in session
            session = await new_session(request)
            session['oauth_state'] = state

            # Get authorization URL
            auth_url = self.oauth_client.get_authorization_url(state)
            logger.info(f"Redirecting to Databricks OAuth: {auth_url}")

            return web.HTTPFound(auth_url)

        # For other providers (if OAuth available)
        redirect_uri = self.redirect_uri
        return await self.oauth_client.authorize_redirect(request, redirect_uri)

    async def handle_oauth_callback(self, request: web.Request) -> web.Response:
        """
        Handle OAuth callback after user authentication.

        Args:
            request: aiohttp Request with authorization code

        Returns:
            Redirect to home page or error page
        """
        try:
            # For Databricks OAuth
            if self.provider == 'databricks':
                # Get authorization code and state from query params
                code = request.query.get('code')
                state = request.query.get('state')

                if not code:
                    return web.Response(text="Missing authorization code", status=400)

                # Verify state (CSRF protection)
                session = await get_session(request)
                expected_state = session.get('oauth_state')
                if state != expected_state:
                    logger.warning(f"OAuth state mismatch: expected={expected_state}, got={state}")
                    return web.Response(text="Invalid OAuth state (CSRF check failed)", status=403)

                # Exchange code for token
                token_data = await self.oauth_client.exchange_code_for_token(code)
                if not token_data:
                    return web.Response(text="Failed to exchange authorization code", status=500)

                access_token = token_data.get('access_token')
                id_token = token_data.get('id_token')

                # Get user info
                userinfo = await self.oauth_client.get_user_info(access_token)
                if not userinfo:
                    # Fallback: decode ID token
                    if id_token:
                        userinfo = self.oauth_client.decode_id_token(id_token)

                if not userinfo:
                    return web.Response(text="Failed to get user information", status=500)

                user_email = userinfo.get('email') or userinfo.get('preferred_username') or userinfo.get('sub')
                user_name = userinfo.get('name', user_email)
                user_groups = userinfo.get('groups', [])

            else:
                # For other providers (if OAuth available)
                # Exchange authorization code for token
                token = await self.oauth_client.authorize_access_token(request)

                # Parse user info from token
                userinfo = token.get('userinfo')
                if not userinfo:
                    # Fetch userinfo if not in token
                    userinfo = await self.oauth_client.userinfo(token=token)

                user_email = userinfo.get('email') or userinfo.get('preferred_username') or userinfo.get('upn')
                user_name = userinfo.get('name', user_email)
                user_groups = userinfo.get('groups', [])

            if not user_email:
                logger.error("No email in OAuth token")
                return web.Response(text="Authentication failed: No email in token", status=403)

            # Check MFA if required
            if self.require_mfa:
                mfa_satisfied = self.check_mfa_status(token)
                if not mfa_satisfied:
                    logger.warning(f"MFA not satisfied for user {user_email}")
                    return web.Response(
                        text="Multi-factor authentication required. Please enable MFA and try again.",
                        status=403
                    )
                logger.info(f"User {user_email} authenticated with MFA")

            # Create user object with role (RBAC will be implemented separately)
            from unified_connector.web.rbac import User
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
                'mfa_completed': self.require_mfa
            }

            logger.info(f"User {user_email} logged in successfully (role={user.role.value})")

            # Redirect to home page
            return web.HTTPFound('/')

        except Exception as e:
            logger.error(f"OAuth callback failed: {e}", exc_info=True)
            return web.Response(text=f"Authentication failed: {str(e)}", status=500)

    async def handle_logout(self, request: web.Request) -> web.Response:
        """
        Handle logout - clear session and redirect to login.

        Args:
            request: aiohttp Request

        Returns:
            Redirect response to login page
        """
        session = await get_session(request)
        user_email = session.get('user', {}).get('email', 'unknown')

        # Clear session
        session.invalidate()

        logger.info(f"User {user_email} logged out")

        return web.HTTPFound('/login')

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

    def check_mfa_status(self, token: Dict[str, Any]) -> bool:
        """
        Check if user completed MFA based on OAuth token claims.

        Args:
            token: OAuth token with claims

        Returns:
            True if MFA satisfied, False otherwise
        """
        # Check for MFA claim in token
        # Azure AD uses 'amr' (Authentication Methods Reference)
        amr = token.get(self.mfa_claim_name, [])

        if not isinstance(amr, list):
            amr = [amr]

        # Check if any required MFA method is present
        for method in self.mfa_required_methods:
            if method in amr:
                logger.debug(f"MFA satisfied with method: {method}")
                return True

        logger.debug(f"MFA not satisfied. Available methods: {amr}, required: {self.mfa_required_methods}")
        return False


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

        # For API requests, return 401
        if request.path.startswith('/api/'):
            raise web.HTTPUnauthorized(reason="Authentication required")

        # For browser requests, redirect to login
        return web.HTTPFound('/login')

    # Recreate User object and attach to request
    from unified_connector.web.rbac import User, Role
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
        from unified_connector.web.rbac import User
        user = User(
            email=cn,  # Use CN as email/identifier
            name=cn,
            groups=[ou] if ou else [],
            role_mappings={},
            default_role=default_role
        )

        # Set role from mapping
        from unified_connector.web.rbac import Role
        user.role = Role[role_name.upper()]

        logger.info(f"Client certificate authenticated: CN={cn}, OU={ou}, role={role_name}")
        return user

    except Exception as e:
        logger.error(f"Failed to parse client certificate: {e}", exc_info=True)
        return None
