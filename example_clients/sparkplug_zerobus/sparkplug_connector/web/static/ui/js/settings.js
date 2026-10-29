// Settings page, rendered from /api/settings (schema + the file's values). Only changed keys are sent; the server
// checks them again and saves them to the file; they take effect when the bridge restarts (Save and restart).
document.addEventListener('alpine:init', () => {
  'use strict';
  const Alpine = window.Alpine;
  const NUMBER_TYPES = ['INTEGER', 'NUMBER'];

  Alpine.data('settingsPage', () => ({
    loading: true,
    saving: false,
    version: null,                  // the configuration's version this form loaded (sent back with a save)
    sections: [],
    fields: {},
    values: {},
    original: {},
    errors: {},
    formError: '',
    open: {},
    file: '',
    pendingChanges: [],

    async init() {
      window.addEventListener('beforeunload', (e) => {
        if (this.dirty) {
          e.preventDefault();
          e.returnValue = '';
        }
      });
      try {
        const loaded = await window.api.getVersioned('/api/settings');
        const settings = loaded.data;
        this.version = loaded.version;
        this.sections = settings.sections;
        const fields = {};
        settings.sections.forEach((s) => s.fields.forEach((f) => { fields[f.key] = f; }));
        this.fields = fields;
        const open = {};
        settings.sections.forEach((s, i) => { open[s.name] = i === 0; });
        this.open = open;
        this.apply(settings);
      } catch (e) {
        this.formError = e.message;
      } finally {
        this.loading = false;
      }
    },

    apply(settings) {
      this.file = settings.file || '';
      const values = {};
      Object.values(this.fields).forEach((f) => {
        const v = settings.values[f.key];
        if (f.type === 'BOOLEAN') {
          values[f.key] = v === undefined || v === null ? f.defaultValue === true : v === true || String(v).toLowerCase() === 'true';
        } else {
          values[f.key] = v === undefined || v === null ? '' : String(v);
        }
      });
      this.values = values;
      this.original = Object.assign({}, values);
      this.errors = {};
      this.formError = '';
    },

    can(permission) { return Alpine.store('connector').can(permission); },
    get canRestart() { return this.can('start_stop'); },

    // --- state ---

    get dirty() { return this.changedKeys.length > 0; },
    get changedKeys() { return Object.keys(this.values).filter((k) => this.values[k] !== this.original[k]); },
    get changeSummary() {
      const n = this.changedKeys.length;
      return n === 0 ? 'No unsaved changes' : n + (n === 1 ? ' unsaved change' : ' unsaved changes');
    },
    sectionNote(section) {
      const changed = section.fields.filter((f) => this.values[f.key] !== this.original[f.key]).length;
      const errors = section.fields.filter((f) => this.errors[f.key]).length;
      const parts = [];
      if (errors) parts.push(errors + (errors === 1 ? ' error' : ' errors'));
      if (changed) parts.push(changed + ' changed');
      return parts.join(' · ');
    },
    isOpen(section) { return !!this.open[section.name]; },
    onToggle(section, event) { this.open[section.name] = event.target.open; },
    expandAll() {
      const open = {};
      this.sections.forEach((s) => { open[s.name] = true; });
      this.open = open;
    },

    // --- fields ---

    isType(field, type) { return field.type === type; },
    isNumber(field) { return NUMBER_TYPES.includes(field.type); },
    placeholder(field) {
      return field.defaultValue === undefined || field.defaultValue === null ? '' : String(field.defaultValue);
    },
    help(field) {
      const parts = [];
      if (field.help) parts.push(field.help);
      if (field.defaultValue !== undefined && field.defaultValue !== null && field.type !== 'BOOLEAN') {
        parts.push('Default: ' + field.defaultValue + (field.unit ? ' ' + field.unit : '') + '.');
      } else if (field.unit) {
        parts.push('In ' + field.unit + '.');
      }
      return parts.join(' ');
    },
    enumDefault(field) {
      return field.defaultValue !== undefined && field.defaultValue !== null ? 'Default (' + field.defaultValue + ')' : '— not set —';
    },
    invalid(field) { return this.errors[field.key] ? 'true' : null; },
    hasError(field) { return !!this.errors[field.key]; },
    error(field) { return this.errors[field.key] || ''; },

    onInput(key) {
      if (this.errors[key]) {
        const errors = Object.assign({}, this.errors);
        delete errors[key];
        this.errors = errors;
      }
    },

    // --- validation and save ---

    validate() {
      const errors = {};
      Object.values(this.fields).forEach((f) => {
        const v = this.values[f.key];
        if (!this.isNumber(f) || v === '' || /^\$\{/.test(String(v))) return;
        const n = Number(String(v).trim());
        if (String(v).trim() === '' || Number.isNaN(n)) {
          errors[f.key] = 'Must be a number';
        } else if (f.type === 'INTEGER' && !Number.isInteger(n)) {
          errors[f.key] = 'Must be a whole number';
        } else if (f.min !== null && f.min !== undefined && n < f.min) {
          errors[f.key] = 'Must be at least ' + f.min;
        }
      });
      this.showErrors(errors);
      return Object.keys(errors).length === 0;
    },

    showErrors(errors) {
      this.errors = errors;
      const open = Object.assign({}, this.open);
      this.sections.forEach((s) => {
        if (s.fields.some((f) => errors[f.key])) open[s.name] = true;
      });
      this.open = open;
    },

    display(field, value) {
      if (value === '' || value === undefined) {
        return field.defaultValue !== undefined && field.defaultValue !== null ? 'default (' + field.defaultValue + ')' : 'not set';
      }
      return String(value);
    },

    review() {
      this.formError = '';
      if (!this.validate()) {
        this.formError = 'Fix the highlighted fields first.';
        return;
      }
      this.pendingChanges = this.changedKeys.map((k) => {
        const f = this.fields[k];
        return { key: k, label: f.label, section: f.section, from: this.display(f, this.original[k]), to: this.display(f, this.values[k]) };
      });
      this.$refs.review.showModal();
    },

    closeReview() { this.$refs.review.close(); },

    saveAndRestart() { return this.save(true); },

    async save(restart) {
      const values = {};
      this.changedKeys.forEach((k) => { values[k] = this.values[k]; });
      this.saving = true;
      try {
        const saved = await window.api.raw('PUT', '/api/settings', { values }, window.api.ifMatch(this.version));
        const settings = saved.data;
        this.version = saved.headers.get('X-Config-Version');   // the version this save made
        this.apply(settings);
        this.$refs.review.close();
        if (restart === true) {
          await window.api.post('/api/bridge/restart');
          window.toast('Saved. The bridge is restarting with the new settings.', 'ok');
        } else {
          window.toast('Saved. Restart the bridge to apply.', 'ok');
        }
        await Alpine.store('connector').refresh();
      } catch (e) {
        this.$refs.review.close();
        this.formError = e.message;
        this.showErrors(e.errors || {});
      } finally {
        this.saving = false;
      }
    },

    discard() {
      this.values = Object.assign({}, this.original);
      this.errors = {};
      this.formError = '';
    },
  }));
});
