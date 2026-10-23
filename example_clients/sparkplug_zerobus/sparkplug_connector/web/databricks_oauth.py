"""
Databricks OAuth2 Integration for aiohttp.

Custom implementation that doesn't rely on authlib.integrations.aiohttp_client
(which is not available in authlib 1.6+).

Uses direct HTTP requests for OAuth2 authorization code flow. The user comes from the workspace's SCIM ``Me``
endpoint with the access token (the workspace has no OIDC userinfo endpoint): the workspace checks the token,
and the answer includes the user's workspace groups for the role mapping.
"""

import logging
import secrets
from typing import Dict, Any, Optional
from urllib.parse import urlencode

import aiohttp
from aiohttp import web

logger = logging.getLogger(__name__)


class DatabricksOAuthClient:
    """Custom Databricks OAuth2 client for aiohttp."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        workspace_host: str,
        redirect_uri: str,
        scopes: list[str]
    ):
        """
        Initialize Databricks OAuth client.

        Args:
            client_id: OAuth client ID (application ID)
            client_secret: OAuth client secret
            workspace_host: Databricks workspace URL (e.g., https://xxx.cloud.databricks.com)
            redirect_uri: OAuth redirect URI
            scopes: List of OAuth scopes
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.workspace_host = workspace_host.rstrip('/')
        self.redirect_uri = redirect_uri
        self.scopes = scopes

        # Databricks OAuth endpoints
        self.authorization_endpoint = f"{self.workspace_host}/oidc/v1/authorize"
        self.token_endpoint = f"{self.workspace_host}/oidc/v1/token"
        self.userinfo_endpoint = f"{self.workspace_host}/api/2.0/preview/scim/v2/Me"

        logger.info(f"Databricks OAuth client initialized: workspace={workspace_host}")

    def get_authorization_url(self, state: str) -> str:
        """
        Get OAuth authorization URL for redirecting user.

        Args:
            state: OAuth state parameter (CSRF protection)

        Returns:
            Full authorization URL with parameters
        """
        params = {
            'client_id': self.client_id,
            'response_type': 'code',
            'redirect_uri': self.redirect_uri,
            'scope': ' '.join(self.scopes),
            'state': state,
        }
        return f"{self.authorization_endpoint}?{urlencode(params)}"

    async def exchange_code_for_token(self, code: str) -> Optional[Dict[str, Any]]:
        """
        Exchange authorization code for access token.

        Args:
            code: Authorization code from callback

        Returns:
            Token response dict or None on failure
        """
        data = {
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': self.redirect_uri,
            'client_id': self.client_id,
            'client_secret': self.client_secret,
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.token_endpoint,
                    data=data,
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                ) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        logger.error(f"Token exchange failed ({resp.status}): {error_text}")
                        return None

                    token_data = await resp.json()
                    logger.info("Successfully exchanged authorization code for token")
                    return token_data

        except Exception as e:
            logger.error(f"Error exchanging code for token: {e}")
            return None

    async def get_user_info(self, access_token: str) -> Optional[Dict[str, Any]]:
        """
        Get user information from Databricks (SCIM ``Me``).

        Args:
            access_token: OAuth access token

        Returns:
            ``email``, ``name`` and ``groups`` (workspace group names), or None on failure
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.userinfo_endpoint,
                    headers={'Authorization': f'Bearer {access_token}'}
                ) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        logger.error(f"User info request failed ({resp.status}): {error_text}")
                        return None

                    user_info = scim_user(await resp.json())
                    logger.info(f"Retrieved user info: {user_info.get('email') or 'unknown'}")
                    return user_info

        except Exception as e:
            logger.error(f"Error getting user info: {e}")
            return None


def scim_user(me: Dict[str, Any]) -> Dict[str, Any]:
    """A SCIM ``Me`` answer as ``email`` (the user name, else the primary email), ``name`` and ``groups``."""
    emails = me.get('emails') or []
    primary = next((e.get('value') for e in emails if e.get('primary')), None) or (emails[0].get('value') if emails else None)
    return {
        'email': me.get('userName') or primary,
        'name': me.get('displayName') or me.get('userName') or primary,
        'groups': [g.get('display') for g in me.get('groups') or [] if g.get('display')],
    }


def generate_state() -> str:
    """Generate random OAuth state for CSRF protection."""
    return secrets.token_urlsafe(32)
