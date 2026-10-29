// Zerobus page: the connection form (/api/zerobus/config; the client secret comes from the environment variable
// DATABRICKS_CLIENT_SECRET, or the one the file names: the page shows whether it is set and saves
// ${env:DATABRICKS_CLIENT_SECRET} when the file names none), start/stop, each open table's writer and write-ahead
// log (from /api/status's metrics) and the checks (/api/zerobus/diagnostics).
document.addEventListener('alpine:init', () => {
  'use strict';
  const Alpine = window.Alpine;
  const TABLE = /^[^.\s]+\.[^.\s]+\.[^.\s]+$/;
  const SECRET_VARIABLE = 'DATABRICKS_CLIENT_SECRET';

  Alpine.data('zerobusPage', () => ({
    loading: true,
    saving: false,
    version: null,                  // the configuration's version this form loaded (sent back with a save)
    busy: null,
    checking: null,
    form: { enabled: true, workspace_host: '', zerobus_endpoint: '', table: '', client_id: '' },
    auth: {},
    errors: {},
    formError: '',
    checkResult: null,

    async init() {
      await this.load();
    },

    async load() {
      try {
        const loaded = await window.api.getVersioned('/api/zerobus/config');
        const cfg = loaded.data;
        this.version = loaded.version;
        const t = cfg.default_target || {};
        this.form = {
          enabled: cfg.enabled === undefined ? true : !!cfg.enabled,
          workspace_host: String(cfg.workspace_host || t.workspace_host || ''),
          zerobus_endpoint: String(cfg.zerobus_endpoint || ''),
          table: [t.catalog, t.schema, t.table].filter((p) => p).join('.'),
          client_id: String((cfg.auth || {}).client_id || ''),
        };
        this.auth = cfg.auth || {};
      } catch (e) {
        this.formError = e.message;
      } finally {
        this.loading = false;
      }
    },

    can(permission) { return Alpine.store('connector').can(permission); },
    get running() { return !!(Alpine.store('connector').status || {}).zerobus_enabled; },
    get secretVariable() { return this.auth.client_secret_env || SECRET_VARIABLE; },
    get secretSet() { return !!this.auth.client_secret_env && !!this.auth.client_secret_env_set; },
    get secretNote() {
      if (this.secretSet) return 'Set in the connector\'s environment.';
      if (this.auth.client_secret_env) return 'Not set: set it in the connector\'s environment, then restart the connector.';
      if (this.auth.client_secret_other) return 'The file has a secret of its own: it stays as written.';
      return 'Set ' + SECRET_VARIABLE + ' in the connector\'s environment; saving here makes the file read it.';
    },
    invalid(field) { return this.errors[field] ? 'true' : null; },
    error(field) { return this.errors[field] || ''; },

    validate() {
      const f = this.form;
      const errors = {};
      if (!/^https:\/\/\S+$/.test(f.workspace_host.trim())) errors.workspace_host = 'https://<workspace>.cloud.databricks.com';
      if (!f.zerobus_endpoint.trim()) errors.zerobus_endpoint = 'Required';
      if (!TABLE.test(f.table.trim())) errors.table = 'catalog.schema.table';
      this.errors = errors;
      return Object.keys(errors).length === 0;
    },

    async save() {
      this.formError = '';
      if (!this.validate()) return;
      const f = this.form;
      const [catalog, schema, table] = f.table.trim().split('.');
      const body = {
        enabled: f.enabled,
        workspace_host: f.workspace_host.trim(),
        zerobus_endpoint: f.zerobus_endpoint.trim(),
        default_target: { workspace_host: f.workspace_host.trim(), catalog, schema, table },
        auth: { client_id: f.client_id.trim() },
      };
      // only where the file has no secret at all: a ${credential:...} or ${env:NAME:default} stays as written
      if (!this.auth.client_secret_env && !this.auth.client_secret_other) body.auth.client_secret = '${env:' + SECRET_VARIABLE + '}';
      this.saving = true;
      try {
        const r = await window.api.post('/api/zerobus/config', body, window.api.ifMatch(this.version));
        window.toast(r.warning || 'Saved. Stop and start Zerobus to use the new settings.', r.warning ? 'error' : 'ok');
        await this.load();
      } catch (e) {
        this.formError = e.message;
      } finally {
        this.saving = false;
      }
    },

    startZerobus() { return this.run('start', 'Zerobus started'); },
    stopZerobus() { return this.run('stop', 'Zerobus stopped'); },
    async run(action, done) {
      this.busy = action;
      try {
        await window.api.post('/api/zerobus/' + action);
        window.toast(done, 'ok');
        await Alpine.store('connector').refresh();
      } catch (e) {
        window.toast(e.message, 'error');
      } finally {
        this.busy = null;
      }
    },

    get targets() {
      const m = (Alpine.store('connector').status || {}).metrics || {};
      const writers = m.zerobus_targets || {};
      const wals = m.wal || {};
      return Object.keys(writers).sort().map((id) => {
        const w = writers[id] || {};
        const wal = wals[id];
        return {
          id,
          table: id.split('.').slice(-3).join('.'),          // catalog.schema.table (the id starts with the workspace)
          ok: w.stream_open && !w.failing_since,
          stream: w.failing_since ? 'failing since ' + window.fmt.dateTime(w.failing_since) : (w.stream_open ? 'open' : 'closed'),
          written: window.fmt.number(w.written),
          inFlight: window.fmt.number(w.batch_in_flight),
          queue: window.fmt.number(w.queue_depth) + (w.queue_high_water ? ' (high)' : ''),
          wal: wal ? window.fmt.number(wal.pending) + ' pending, ' + window.fmt.bytes(wal.bytes || 0) : 'off',
          error: w.last_error ? window.fmt.dateTime(w.last_error_at) + ': ' + w.last_error : '',
          errorNow: !!w.failing_since,
        };
      });
    },

    get checks() {
      const c = (this.checkResult && this.checkResult.checks) || {};
      return Object.keys(c).map((name) => {
        const v = c[name];
        const isFlag = typeof v === 'boolean';
        return {
          name: name.split('_').join(' '),
          value: isFlag ? (v ? 'ok' : 'failed') : (Array.isArray(v) ? v.join(', ') : String(v)),
          cls: isFlag ? (v ? 'dot-ok' : 'dot-err') : '',
        };
      });
    },
    get checkJson() { return this.checkResult ? JSON.stringify(this.checkResult, null, 2) : ''; },

    async check(deep) {
      this.checking = deep ? 'deep' : 'quick';
      try {
        this.checkResult = await window.api.get('/api/zerobus/diagnostics?deep=' + (deep ? 'true' : 'false'));
        const failed = Object.values(this.checkResult.checks || {}).some((v) => v === false);
        window.toast(failed ? 'Some checks failed' : 'All checks passed', failed ? 'error' : 'ok');
      } catch (e) {
        window.toast(e.message, 'error');
      } finally {
        this.checking = null;
      }
    },
  }));
});
