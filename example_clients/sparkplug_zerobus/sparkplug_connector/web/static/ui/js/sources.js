// Sources page: paged table with start/stop, add/edit dialog and delete confirmation, on /api/sources. The password
// comes from an environment variable: the dialog sets its name (saved as ${env:NAME}) and shows whether it is set in
// the connector's environment; a password's value is never typed here. An edit sends every field of the dialog; a
// blank one goes back to its default, fields the dialog doesn't show are kept.
document.addEventListener('alpine:init', () => {
  'use strict';
  const Alpine = window.Alpine;
  const PAGE_SIZE = 10;

  function blank() {
    return {
      name: '', endpoint: '', username: '', password_env: '', client_id: '', keepalive: '', topics: ['spBv1.0/#'],
      target_table: '', ca_cert_path: '', client_cert_path: '', client_key_path: '', check_hostname: true,
      primary_host_id: '', state_format: 'v3', offline_after_seconds: '', online_after_seconds: '',
      sequence_handling: 'strict', sequence_reordering_timeout: '', block_auto_rebirth: false,
      nbirth_enabled: false, warehouse_id: '', procedures: '', schema_prefix: '', check_views: true, enabled: true,
    };
  }

  const ENV_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

  function text(v) { return v === undefined || v === null ? '' : String(v); }
  function flag(v, dflt) {
    if (v === undefined || v === null || v === '') return dflt;
    return v === true || ['true', '1', 'yes', 'on'].includes(String(v).trim().toLowerCase());
  }

  Alpine.data('sourcesPage', () => ({
    sources: [],
    certs: [],
    loading: true,
    page: 0,
    form: blank(),
    editing: null,
    hadNbirth: false,
    hadSecurity: false,
    errors: {},
    formError: '',
    saving: false,
    version: null,                  // the configuration's version this form loaded (sent back with a save)
    original: null,
    toDelete: null,

    async init() {
      await this.load();
    },

    async load() {
      try {
        const [loaded, certs] = await Promise.all([window.api.getVersioned('/api/sources'), window.api.get('/api/certs')]);
        const sources = loaded.data;
        this.version = loaded.version;
        this.sources = (sources.sources || []).filter((s) => (s.protocol || 'sparkplug') === 'sparkplug');
        this.certs = certs.files.map((f) => f.name);
        if (this.page * PAGE_SIZE >= this.sources.length && this.page > 0) this.page = 0;
      } catch (e) {
        window.toast(e.message, 'error');
      } finally {
        this.loading = false;
      }
    },

    can(permission) { return Alpine.store('connector').can(permission); },

    // --- table ---

    get rows() { return this.sources.slice(this.page * PAGE_SIZE, (this.page + 1) * PAGE_SIZE); },
    get paged() { return this.sources.length > PAGE_SIZE; },
    get pageNote() {
      const from = this.page * PAGE_SIZE + 1;
      return from + '–' + Math.min(this.sources.length, from + PAGE_SIZE - 1) + ' of ' + this.sources.length;
    },
    get hasPrev() { return this.page > 0; },
    get hasNext() { return (this.page + 1) * PAGE_SIZE < this.sources.length; },
    prev() { if (this.hasPrev) this.page -= 1; },
    next() { if (this.hasNext) this.page += 1; },
    role(source) { return source.primary_host_id ? 'primary host ' + source.primary_host_id : 'plain subscriber'; },
    topics(source) {
      const t = source.topics || ['spBv1.0/#'];
      return Array.isArray(t) ? t : [t];
    },
    table(source) { return source.target_table || 'default'; },
    statusOf(source) {
      const list = (Alpine.store('connector').status || {}).sources || [];
      return list.find((s) => s.name === source.name) || {};
    },
    isRunning(source) { return !!this.statusOf(source).running; },
    stateClass(source) {
      const h = (Alpine.store('connector').health || {})[source.name];
      return this.isRunning(source) && h && h.state === 'ONLINE' ? 'dot-ok' : 'dot-err';
    },
    stateText(source) {
      if (this.isRunning(source)) {
        const h = (Alpine.store('connector').health || {})[source.name];
        return h ? (h.state === 'ONLINE' ? 'Online' : 'Offline') : 'Running';
      }
      return flag(source.enabled, true) ? 'Stopped' : 'Off';
    },

    async start(source) { await this.action(source, 'start', 'Started '); },
    async stop(source) { await this.action(source, 'stop', 'Stopped '); },
    async action(source, name, done) {
      try {
        await window.api.post('/api/sources/' + encodeURIComponent(source.name) + '/' + name);
        window.toast(done + source.name, 'ok');
        await Alpine.store('connector').refresh();
      } catch (e) {
        window.toast(e.message, 'error');
      }
    },

    // --- dialog ---

    get title() { return this.editing ? 'Edit ' + this.editing : 'Add source'; },
    get passwordNote() {
      const o = this.original || {};
      const name = this.form.password_env.trim();
      if (!name) {
        return o.password_other ? 'The file has a password of its own: give a variable to replace it.'
          : 'The environment variable the connector reads the password from (none: no password).';
      }
      if (name === o.password_env) {
        return o.password_env_set ? name + ' is set in the connector\'s environment.' : name + ' is not set in the connector\'s environment.';
      }
      return 'Saved as ${env:' + name + '}; set ' + name + ' in the connector\'s environment.';
    },
    get clientIdPlaceholder() { return 'dbx-' + (this.form.name || '<name>'); },
    get tlsHint() { return /^mqtts:/.test(this.form.endpoint) ? '' : 'For mqtts:// endpoints.'; },
    certOptions(current) {
      return current && !this.certs.includes(current) ? this.certs.concat([current]) : this.certs;
    },
    certLabel(name) { return this.certs.includes(name) ? name : name + ' (not in the store)'; },
    invalid(field) { return this.errors[field] ? 'true' : null; },
    error(field) { return this.errors[field] || ''; },
    addTopic() { this.form.topics.push(''); },
    removeTopic(index) { this.form.topics.splice(index, 1); },

    openDialog() {
      this.errors = {};
      this.formError = '';
      this.$refs.dialog.showModal();
      this.$nextTick(() => (this.editing ? null : this.$refs.name.focus()));
    },

    add() {
      this.form = blank();
      this.editing = null;
      this.original = null;
      this.hadNbirth = false;
      this.hadSecurity = false;
      this.openDialog();
    },

    edit(source) {
      const f = blank();
      f.password_env = text(source.password_env);
      ['name', 'endpoint', 'username', 'client_id', 'keepalive', 'target_table', 'primary_host_id',
        'offline_after_seconds', 'online_after_seconds', 'sequence_reordering_timeout']
        .forEach((k) => { f[k] = text(source[k]); });
      f.state_format = text(source.state_format) || 'v3';
      f.sequence_handling = text(source.sequence_handling) || 'strict';
      f.block_auto_rebirth = flag(source.block_auto_rebirth, false);
      f.enabled = flag(source.enabled, true);
      f.topics = this.topics(source).slice();
      const sec = source.security || {};
      f.ca_cert_path = text(sec.ca_cert_path);
      f.client_cert_path = text(sec.client_cert_path);
      f.client_key_path = text(sec.client_key_path);
      f.check_hostname = flag(sec.check_hostname, true);
      const nb = source.edge_node_schemas;
      if (nb) {
        f.nbirth_enabled = flag(nb.enabled, true);
        f.warehouse_id = text(nb.warehouse_id);
        f.procedures = Array.isArray(nb.procedures) ? nb.procedures.join(', ') : text(nb.procedures);
        f.schema_prefix = text(nb.schema_prefix);
        f.check_views = flag(nb.check_views, true);
      }
      this.form = f;
      this.editing = source.name;
      this.original = source;
      this.hadNbirth = !!nb;
      this.hadSecurity = !!source.security;
      this.openDialog();
    },

    close() { this.$refs.dialog.close(); },

    body() {
      const f = this.form;
      const num = (v) => (v === '' || v === null || v === undefined ? '' : Number(v));
      const body = {
        name: f.name.trim(), protocol: 'sparkplug', endpoint: f.endpoint.trim(), username: f.username.trim(),
        client_id: f.client_id.trim(), keepalive: num(f.keepalive),
        topics: f.topics.map((t) => t.trim()).filter((t) => t),
        target_table: f.target_table.trim(), primary_host_id: f.primary_host_id.trim(), state_format: f.state_format,
        offline_after_seconds: num(f.offline_after_seconds), online_after_seconds: num(f.online_after_seconds),
        sequence_handling: f.sequence_handling, sequence_reordering_timeout: num(f.sequence_reordering_timeout),
        block_auto_rebirth: f.block_auto_rebirth, enabled: f.enabled,
      };
      if (!this.editing) {
        Object.keys(body).forEach((k) => { if (body[k] === '') delete body[k]; });
      }
      const env = f.password_env.trim();
      const before = this.original ? text(this.original.password_env) : '';
      if (env && env !== before) body.password = '${env:' + env + '}';
      else if (!env && before) body.password = '';            // the variable removed: no password
      const tls = { ca_cert_path: f.ca_cert_path, client_cert_path: f.client_cert_path, client_key_path: f.client_key_path,
        check_hostname: f.check_hostname };
      if (this.hadSecurity || f.ca_cert_path || f.client_cert_path || f.client_key_path || !f.check_hostname) {
        body.security = tls;
      }
      if (this.hadNbirth || f.nbirth_enabled) {
        body.edge_node_schemas = {
          enabled: f.nbirth_enabled, warehouse_id: f.warehouse_id.trim(), schema_prefix: f.schema_prefix.trim(),
          procedures: f.procedures.split(',').map((p) => p.trim()).filter((p) => p), check_views: f.check_views,
        };
      }
      return body;
    },

    validate() {
      const f = this.form;
      const errors = {};
      if (!f.name.trim()) errors.name = 'Required';
      else if (!/^[A-Za-z0-9_-]+$/.test(f.name.trim())) errors.name = 'Letters, digits, - and _ only';
      if (!f.endpoint.trim()) errors.endpoint = 'Required';
      else if (!/^mqtts?:\/\/[^\s/]+(:\d{1,5})?\/?$/.test(f.endpoint.trim())) errors.endpoint = 'mqtt://host:1883 or mqtts://host:8883';
      if (!f.topics.some((t) => t.trim())) errors.topics = 'At least one topic filter';
      if (f.password_env.trim() && !ENV_NAME.test(f.password_env.trim())) errors.password_env = 'A variable name: letters, digits and _';
      if (f.keepalive !== '' && !(Number(f.keepalive) >= 1 && Number(f.keepalive) <= 65535)) errors.keepalive = '1 to 65535';
      if (f.target_table.trim() && f.target_table.trim().split('.').length !== 3) errors.target_table = 'catalog.schema.table';
      if (f.client_cert_path && !f.client_key_path) errors.client_key_path = 'Required with a client certificate';
      if (f.client_key_path && !f.client_cert_path) errors.client_cert_path = 'Required with a client key';
      if (f.nbirth_enabled && !f.warehouse_id.trim()) errors.warehouse_id = 'Required for the NBIRTH call';
      if (f.nbirth_enabled && !f.procedures.trim()) errors.procedures = 'At least one procedure';
      this.errors = errors;
      return Object.keys(errors).length === 0;
    },

    async save() {
      this.formError = '';
      if (!this.validate()) return;
      this.saving = true;
      try {
        if (this.editing) {
          await window.api.put('/api/sources/' + encodeURIComponent(this.editing), this.body(), window.api.ifMatch(this.version));
        } else {
          await window.api.post('/api/sources', this.body(), window.api.ifMatch(this.version));
        }
        this.close();
        window.toast((this.editing ? 'Saved ' : 'Added ') + this.form.name, 'ok');
        await this.load();
        await Alpine.store('connector').refresh();
      } catch (e) {
        this.errors = e.errors || {};
        this.formError = e.message;
      } finally {
        this.saving = false;
      }
    },

    askDelete(source) {
      this.toDelete = source;
      this.$refs.remove.showModal();
    },
    get deleteName() { return this.toDelete ? this.toDelete.name : ''; },
    cancelDelete() {
      this.$refs.remove.close();
      this.toDelete = null;
    },
    async confirmDelete() {
      const source = this.toDelete;
      this.$refs.remove.close();
      this.toDelete = null;
      try {
        await window.api.del('/api/sources/' + encodeURIComponent(source.name), window.api.ifMatch(this.version));
        window.toast('Deleted ' + source.name, 'ok');
        await this.load();
        await Alpine.store('connector').refresh();
      } catch (e) {
        window.toast(e.message, 'error');
      }
    },
  }));

});
