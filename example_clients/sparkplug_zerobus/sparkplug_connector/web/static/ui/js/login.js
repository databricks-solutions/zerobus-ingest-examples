// The sign-in page (/static/login.html, only with web_ui.authentication on): an error from the OAuth callback, and
// straight to the connector when already signed in.
(function () {
  'use strict';
  const MESSAGES = {
    mfa_required: 'Multi-factor authentication is required. Please enable MFA on your account.',
    access_denied: 'Access denied. You may not have permission to use this connector.',
    invalid_token: 'Invalid authentication token. Please sign in again.',
    session_expired: 'Your session has expired. Please sign in again.',
  };
  const error = new URLSearchParams(location.search).get('error');
  if (error) {
    const el = document.getElementById('error-message');
    el.textContent = MESSAGES[error] || 'Authentication failed. Please try again.';
    el.hidden = false;
  }
  fetch('/api/auth/status', { headers: { Accept: 'application/json' } })
    .then((res) => res.json())
    .then((status) => {
      if (status.authenticated || !status.auth_enabled) location.href = '/';
    })
    .catch(() => {});
})();
