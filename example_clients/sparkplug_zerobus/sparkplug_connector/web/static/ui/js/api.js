// fetch wrapper shared by every page: JSON in and out, 401 -> the login page (only when the login is on), and API
// errors as ApiError {status, message, errors: {field: message}}. Same origin only: the server refuses changes from
// pages of another origin.
(function () {
  'use strict';

  class ApiError extends Error {
    constructor(status, message, errors) {
      super(message || ('Request failed (' + status + ')'));
      this.status = status;
      this.errors = errors || {};
    }
  }

  let authPromise = null;

  // {auth_enabled, authenticated, user, permissions}: the login's state and what the user may do (everything when
  // the login is off).
  function auth() {
    if (!authPromise) {
      authPromise = fetch('/api/auth/status', { headers: { Accept: 'application/json' } })
        .then((res) => res.json())
        .then(async (status) => {
          if (!status.auth_enabled) {     // read-only: read only, whoever opens it
            return Object.assign(status, { permissions: status.read_only ? ['read'] : null });
          }
          if (!status.authenticated) {
            location.href = '/static/login.html';
            throw new ApiError(401, 'Sign in required');
          }
          const res = await fetch('/api/auth/permissions', { headers: { Accept: 'application/json' } });
          const perms = res.ok ? await res.json() : {};
          const permissions = (perms.permissions || []).filter((p) => !status.read_only || p === 'read');
          return Object.assign(status, { permissions, role: perms.role });
        });
    }
    return authPromise;
  }

  async function request(method, path, body, options) {
    const opts = options || {};
    const headers = { Accept: 'application/json' };
    let payload;
    if (body instanceof FormData) {
      payload = body;
    } else if (body !== undefined) {
      headers['Content-Type'] = 'application/json';
      payload = JSON.stringify(body);
    }
    Object.assign(headers, opts.headers || {});
    const res = await fetch(path, { method, headers, body: payload, credentials: 'same-origin' });
    const text = res.status === 204 ? '' : await res.text();
    let data = null;
    if (text) {
      try {
        data = JSON.parse(text);
      } catch (e) {
        data = { message: text };
      }
    }
    if (res.status === 401) {
      location.href = '/static/login.html';
    }
    if (!res.ok || (data && data.status === 'error')) {
      throw new ApiError(res.status, data && data.message, data && data.errors);
    }
    return { data, headers: res.headers, status: res.status };
  }

  window.api = {
    ApiError,
    auth,
    // whether the user may (permission: read, write, configure, start_stop, delete); true when the login is off
    async can(permission) {
      const a = await auth();
      return a.permissions === null || a.permissions.includes(permission);
    },
    get: (path, opts) => request('GET', path, undefined, opts).then((r) => r.data),
    post: (path, body, opts) => request('POST', path, body === undefined ? {} : body, opts).then((r) => r.data),
    put: (path, body, opts) => request('PUT', path, body, opts).then((r) => r.data),
    del: (path, opts) => request('DELETE', path, undefined, opts).then((r) => r.data),
    raw: request,
    // a form's load: the data and the configuration's version, which its save sends back (a save on a version
    // another save has replaced is refused: 412, "reload the page")
    getVersioned: (path) => request('GET', path).then((r) => ({ data: r.data, version: r.headers.get('X-Config-Version') })),
    ifMatch: (version) => (version ? { headers: { 'If-Match': version } } : {}),
  };

  // Small shared helpers.
  window.fmt = {
    number(n) {
      return typeof n === 'number' ? n.toLocaleString() : '–';
    },
    compact(n) {     // for the tiles: 3.15B, not 3,153,600,000 (which overflows a tile); exact below a million
      if (typeof n !== 'number') return '–';
      if (Math.abs(n) < 1e6) return n.toLocaleString();
      return new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 2 }).format(n);
    },
    dateTime(ms) {
      if (!ms) {
        return '–';
      }
      const d = new Date(typeof ms === 'number' && ms < 1e11 ? ms * 1000 : ms);   // epoch seconds or ms
      return d.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'medium' });
    },
    ago(ms) {
      if (!ms) {
        return '';
      }
      const t = typeof ms === 'number' && ms < 1e11 ? ms * 1000 : new Date(ms).getTime();
      const s = Math.round((Date.now() - t) / 1000);
      if (s < 60) return s + ' s ago';
      if (s < 3600) return Math.round(s / 60) + ' min ago';
      if (s < 86400) return Math.round(s / 3600) + ' h ago';
      return Math.round(s / 86400) + ' d ago';
    },
    bytes(n) {
      if (n < 1024) return n + ' B';
      if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB';
      return (n / 1024 / 1024).toFixed(1) + ' MB';
    },
  };

  // Toasts: toast('Saved', 'ok' | 'error' | 'info').
  window.toast = function (message, kind) {
    let box = document.getElementById('toasts');
    if (!box) {
      box = document.createElement('div');
      box.id = 'toasts';
      box.className = 'toasts';
      box.setAttribute('role', 'status');
      box.setAttribute('aria-live', 'polite');
      document.body.appendChild(box);
    }
    const el = document.createElement('div');
    el.className = 'toast toast-' + (kind || 'info');
    el.textContent = message;
    box.appendChild(el);
    setTimeout(() => el.classList.add('toast-out'), kind === 'error' ? 7000 : 3500);
    setTimeout(() => el.remove(), kind === 'error' ? 7400 : 3900);
  };
})();
