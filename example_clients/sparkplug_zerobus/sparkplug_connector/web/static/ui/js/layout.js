// Shared chrome for the pages: sidebar, top bar with the connector's state pill, the "restart to apply" banner, theme
// toggle and (with the login on) the user and sign-out, plus the live status store, polled from /api/status and
// /api/sparkplug/health. Pages keep only their own <main>.
(function () {
  'use strict';

  const ICONS = {
    logo: '<path d="M2 18h20M4 18v-7M20 18v-7M2 11c4-6 16-6 20 0M8 18v-5M12 18v-6M16 18v-5"/>',
    status: '<polyline points="3 12 7 12 10 4 14 20 17 12 21 12"/>',
    server: '<rect x="3" y="4" width="18" height="7" rx="2"/><rect x="3" y="13" width="18" height="7" rx="2"/><path d="M7 7.5h.01M7 16.5h.01"/>',
    nodes: '<circle cx="6" cy="6" r="2.5"/><circle cx="18" cy="6" r="2.5"/><circle cx="12" cy="18" r="2.5"/><path d="M8 7.5l3 8M16 7.5l-3 8M8.5 6h7"/>',
    stream: '<path d="M3 7h12M3 12h18M3 17h9"/><path d="M17 4l3 3-3 3"/>',
    config: '<path d="M4 6h10M18 6h2M4 12h4M12 12h8M4 18h12M20 18h0"/><circle cx="16" cy="6" r="2"/><circle cx="10" cy="12" r="2"/><circle cx="18" cy="18" r="2"/>',
    cert: '<circle cx="8" cy="15" r="4"/><path d="M11 12l9-9M16 7l3 3M14 9l2 2"/>',
    sun: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M2 12h2M20 12h2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/>',
    moon: '<path d="M20 14.5A8 8 0 1 1 9.5 4a6.5 6.5 0 0 0 10.5 10.5z"/>',
    logout: '<path d="M15 4h3a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2h-3M10 17l-5-5 5-5M5 12h11"/>',
    play: '<polygon points="7 4 19 12 7 20 7 4"/>',
    stop: '<rect x="6" y="6" width="12" height="12" rx="1"/>',
    restart: '<path d="M3 12a9 9 0 1 0 3-6.7L3 8M3 3v5h5"/>',
    plus: '<path d="M12 5v14M5 12h14"/>',
    trash: '<path d="M4 7h16M10 11v6M14 11v6M6 7l1 12a2 2 0 0 0 2 2h6a2 2 0 0 0 2-2l1-12M9 7V4h6v3"/>',
    edit: '<path d="M4 20h4L19 9a2.8 2.8 0 0 0-4-4L4 16v4zM14 6l4 4"/>',
    eye: '<path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12z"/><circle cx="12" cy="12" r="3"/>',
    eyeOff: '<path d="M3 3l18 18M10.6 5.1A10 10 0 0 1 12 5c6.5 0 10 7 10 7a17 17 0 0 1-3.2 4M6.6 6.6C3.8 8.4 2 12 2 12s3.5 7 10 7a9.7 9.7 0 0 0 5.4-1.6M9.9 9.9a3 3 0 0 0 4.2 4.2"/>',
    upload: '<path d="M12 16V4M7 9l5-5 5 5M4 16v3a1 1 0 0 0 1 1h14a1 1 0 0 0 1-1v-3"/>',
    alert: '<path d="M12 3l10 18H2L12 3zM12 10v4M12 17h.01"/>',
    check: '<path d="M5 12l5 5L20 7"/>',
    x: '<path d="M6 6l12 12M18 6L6 18"/>',
    file: '<path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8l-5-5zM14 3v5h5"/>',
  };

  function svg(name) {
    return '<svg class="icon" viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2" '
      + 'stroke-linecap="round" stroke-linejoin="round">' + (ICONS[name] || '') + '</svg>';
  }

  // Fills <span data-icon="name"></span>, including inside <template> content that Alpine clones later.
  function fillIcons(root) {
    root.querySelectorAll('[data-icon]').forEach((el) => {
      if (!el.firstChild) {
        el.innerHTML = svg(el.getAttribute('data-icon'));
      }
    });
    root.querySelectorAll('template').forEach((t) => fillIcons(t.content));
  }

  const NAV = [
    { page: 'status', href: '/', label: 'Status', icon: 'status' },
    { page: 'sources', href: '/sources.html', label: 'Sources', icon: 'server' },
    { page: 'edge-nodes', href: '/edge-nodes.html', label: 'Edge nodes', icon: 'nodes' },
    { page: 'zerobus', href: '/zerobus.html', label: 'Zerobus', icon: 'stream' },
    { page: 'settings', href: '/settings.html', label: 'Settings', icon: 'config' },
    { page: 'certs', href: '/certs.html', label: 'Certificates', icon: 'cert' },
  ];
  const POLL_MS = 3000;

  function buildShell() {
    const main = document.querySelector('main');
    const page = document.body.getAttribute('data-page');
    if (!main || !page) {
      fillIcons(document);
      return; // the sign-in page has no chrome
    }
    const current = NAV.find((n) => n.page === page) || NAV[0];
    const links = NAV.map((n) => '<li><a href="' + n.href + '"' + (n.page === page ? ' aria-current="page"' : '') + '>'
      + svg(n.icon) + '<span>' + n.label + '</span></a></li>').join('');

    const shell = document.createElement('div');
    shell.className = 'shell';
    shell.setAttribute('x-data', 'layout');
    shell.innerHTML = ''
      + '<aside class="sidebar">'
      + '  <a class="brand" href="/">' + svg('logo') + '<span><strong>Sparkplug Zerobus</strong><small>to Databricks</small></span></a>'
      + '  <nav aria-label="Connector"><ul>' + links + '</ul></nav>'
      + '  <div class="sidebar-foot"><small>Sparkplug B to Databricks through Zerobus</small></div>'
      + '</aside>'
      + '<div class="main-col">'
      + '  <div class="page-top">'                 // sticky: the top bar and the restart banner stay in view
      + '  <header class="topbar">'
      + '    <h1>' + current.label + '</h1>'
      + '    <span class="pill" :class="stateClass" x-text="stateLabel" :title="stateTitle"></span>'
      + '    <span class="live" x-show="!live" title="The connector does not answer">offline</span>'
      + '    <span class="read-only" x-show="readOnly" x-cloak title="web_ui.read_only: changes are refused">read-only</span>'
      + '    <span class="spacer"></span>'
      + '    <button type="button" class="icon-btn" @click="toggleTheme" :title="themeTitle" aria-label="Toggle light or dark theme">'
      + '      <span x-show="!dark">' + svg('moon') + '</span><span x-show="dark">' + svg('sun') + '</span></button>'
      + '    <span class="user" x-show="authEnabled" x-text="username"></span>'
      + '    <button type="button" class="icon-btn" x-show="authEnabled" @click="signOut" title="Sign out" aria-label="Sign out">' + svg('logout') + '</button>'
      + '  </header>'
      + '  <div class="banner" x-show="restartRequired" x-cloak role="status">'
      + '    ' + svg('alert') + '<span>Saved settings take effect when the bridge restarts.</span>'
      + '    <button type="button" class="small" x-show="canRestart" @click="restartNow" :aria-busy="restarting" :disabled="restarting">'
      + '      ' + svg('restart') + 'Restart now</button>'
      + '  </div>'
      + '  </div>'
      + '</div>';
    document.body.insertBefore(shell, document.body.firstChild);
    shell.querySelector('.main-col').appendChild(main);
    main.classList.add('content');
    document.title = current.label + ' · Sparkplug Zerobus';
    fillIcons(document);
  }

  function isDark() {
    const set = document.documentElement.getAttribute('data-theme');
    if (set) {
      return set === 'dark';
    }
    return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
  }

  document.addEventListener('alpine:init', () => {
    const Alpine = window.Alpine;

    // The connector's state for every page: /api/status and /api/sparkplug/health, polled.
    Alpine.store('connector', {
      status: null,
      health: {},
      live: true,
      auth: { auth_enabled: false },
      permissions: null,
      timer: null,

      start() {
        if (this.timer) return;
        this.refresh();
        this.timer = setInterval(() => this.refresh(), POLL_MS);
      },

      async refresh() {
        const [status, health] = await Promise.allSettled([window.api.get('/api/status'), window.api.get('/api/sparkplug/health')]);
        if (status.status === 'fulfilled') this.status = status.value;
        if (health.status === 'fulfilled') this.health = health.value || {};
        this.live = status.status === 'fulfilled';
      },

      can(permission) {
        return this.permissions === null || this.permissions.includes(permission);
      },
    });

    Alpine.data('layout', () => ({
      dark: isDark(),
      restarting: false,

      init() {
        const store = Alpine.store('connector');
        window.api.auth().then((a) => {
          store.auth = a;
          store.permissions = a.permissions;
          store.start();
        }).catch(() => {});
      },

      get status() { return Alpine.store('connector').status; },
      get live() { return Alpine.store('connector').live; },
      get authEnabled() { return !!Alpine.store('connector').auth.auth_enabled; },
      get readOnly() { return !!Alpine.store('connector').auth.read_only; },
      get username() {
        const u = Alpine.store('connector').auth.user;
        return u ? (u.email || u.name || u.user_id || '') : '';
      },
      get restartRequired() { return !!(this.status && this.status.restart_required); },
      get canRestart() {
        const s = this.status;
        return !!s && Alpine.store('connector').can('start_stop') && (s.bridge_state === 'running' || s.bridge_state === 'stopped');
      },
      async restartNow() {
        this.restarting = true;
        try {
          await window.api.post('/api/bridge/restart');
          window.toast('The bridge is restarting with the saved settings', 'ok');
          await Alpine.store('connector').refresh();
        } catch (e) {
          window.toast(e.message, 'error');
        } finally {
          this.restarting = false;
        }
      },
      get themeTitle() { return this.dark ? 'Use the light theme' : 'Use the dark theme'; },

      get failing() {
        const health = Alpine.store('connector').health || {};
        // failing now; a last_error stays after recovery as history (the tables show it, muted)
        return Object.keys(health).filter((k) => health[k] && health[k].zerobus_failing_since);
      },
      get stateLabel() {
        const s = this.status;
        if (!s) return 'Loading…';
        if (s.bridge_state === 'stopping') return 'Stopping…';
        if (s.bridge_starting || s.bridge_state === 'starting') return 'Starting…';
        if (!s.bridge_running) return 'Stopped';
        if (!s.zerobus_enabled) return 'Running · Zerobus off';
        return this.failing.length ? 'Running · errors' : 'Running';
      },
      get stateClass() {
        const s = this.status;
        if (!s) return 'pill-muted';
        if (s.bridge_starting || s.bridge_state === 'starting' || s.bridge_state === 'stopping') return 'pill-warn';
        if (!s.bridge_running) return 'pill-muted';
        if (!s.zerobus_enabled || this.failing.length) return 'pill-warn';
        return 'pill-ok';
      },
      get stateTitle() {
        return this.failing.length ? 'Failing: ' + this.failing.join(', ') : '';
      },

      toggleTheme() {
        this.dark = !this.dark;
        const theme = this.dark ? 'dark' : 'light';
        document.documentElement.setAttribute('data-theme', theme);
        try {
          localStorage.setItem('connector.theme', theme);
        } catch (e) {
          // Not persisted; the choice lasts for this page.
        }
      },

      async signOut() {
        try {
          await window.api.post('/logout');
        } finally {
          location.href = '/static/login.html';
        }
      },
    }));
  });

  buildShell();
  window.icons = { svg, fillIcons };
})();
