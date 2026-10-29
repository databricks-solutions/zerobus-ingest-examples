// Status page: counters, each source's state with start/stop, Zerobus, from the shared status store and
// /api/sparkplug/edge-nodes.
document.addEventListener('alpine:init', () => {
  'use strict';
  const Alpine = window.Alpine;

  Alpine.data('statusPage', () => ({
    busy: null,
    edgeNodes: {},

    init() {
      this.loadEdgeNodes();
      setInterval(() => this.loadEdgeNodes(), 5000);
    },

    async loadEdgeNodes() {
      try {
        this.edgeNodes = await window.api.get('/api/sparkplug/edge-nodes');
      } catch (e) {
        // the store's "offline" marker covers it
      }
    },

    get store() { return Alpine.store('connector'); },
    get status() { return this.store.status || {}; },
    get health() { return this.store.health || {}; },
    can(permission) { return this.store.can(permission); },

    get summary() {
      const s = this.status;
      if (s.bridge_running === undefined) return 'Loading the connector status…';
      if (s.bridge_state === 'stopping') return 'The bridge is stopping: its sources stop and the queued rows are written.';
      if (s.bridge_state === 'starting') return 'The bridge is starting.';
      if (!s.bridge_running) return 'The bridge is not running: start it, or set bridge.autostart.';
      if (!s.zerobus_enabled) return 'The connector is running, but Zerobus is off: rows stay queued. Start it on the Zerobus page.';
      const failing = Object.keys(this.health).filter((k) => this.health[k].zerobus_failing_since);
      if (failing.length) return 'The connector is running, but writes to Databricks are failing for ' + failing.join(', ') + '.';
      return 'The connector is running and writing to Databricks.';
    },
    get canStartBridge() { return this.status.bridge_state === 'stopped'; },
    get canStopBridge() { return this.status.bridge_state === 'running'; },

    get rowsWrittenCount() {   // per table: a source's rows_written is its table's, shared with the table's other sources
      const writers = (this.status.metrics || {}).zerobus_targets || {};
      return Object.values(writers).map((w) => w.written || 0).reduce((a, b) => a + b, 0);
    },
    get rowsWritten() { return window.fmt.compact(this.rowsWrittenCount); },
    get rowsWrittenExact() { return window.fmt.number(this.rowsWrittenCount); },
    get rowsSince() {   // each Start or Restart is a new bridge: its counters start at 0
      const t = this.status.bridge_started_at;
      return 'acknowledged by Zerobus since the bridge started' + (t ? ', ' + window.fmt.dateTime(t) : '');
    },
    get queued() { return window.fmt.compact(this.status.backpressure_queue_size); },
    get queuedExact() { return window.fmt.number(this.status.backpressure_queue_size); },
    get sourcesRunning() { return window.fmt.number(this.status.active_sources); },
    get sourcesNote() { return 'running of ' + window.fmt.number(this.status.configured_sources) + ' configured'; },
    get allEdgeNodes() {
      return Object.values(this.edgeNodes || {}).reduce((a, list) => a.concat(list || []), []);
    },
    get edgeNodeCount() { return window.fmt.number(this.allEdgeNodes.length); },
    get edgeNodeBreakdown() {
      const online = this.allEdgeNodes.filter((n) => n.online).length;
      return window.fmt.number(online) + ' online · ' + window.fmt.number(this.allEdgeNodes.length - online) + ' offline';
    },

    get sources() {
      return (this.status.sources || []).map((src) => {
        const h = this.health[src.name];
        const running = !!src.running;
        const st = window.sourceState(running, src.enabled, h);
        return {
          name: src.name,
          running,
          role: h && h.primary_host_id ? 'primary host ' + h.primary_host_id : 'plain subscriber',
          state: st.text,
          stateClass: st.cls,
          mqtt: h ? (h.mqtt_connected ? 'connected' : 'not connected') : '–',
          rows: h ? window.fmt.number(h.rows_written || 0) + ' written' : '–',
          zerobus: h ? (h.zerobus_failing_since ? 'failing since ' + window.fmt.dateTime(h.zerobus_failing_since)
            : (h.zerobus_stream_open ? 'stream open' : 'no stream')) : '–',
          error: st.error || (h && h.last_error ? window.fmt.dateTime(h.zerobus_last_error_at) + ': ' + h.last_error : ''),
          errorNow: !!st.error || !!(h && h.zerobus_failing_since),     // else the last error is history
        };
      });
    },

    get zerobusOn() { return !!this.status.zerobus_enabled; },
    get zerobusState() { return this.zerobusOn ? (this.status.zerobus_connected ? 'on' : 'on, no table open') : 'off'; },
    get targets() {   // catalog.schema.table (an id starts with the workspace)
      return (this.status.zerobus_targets || []).map((id) => id.split('.').slice(-3).join('.'));
    },
    get diagnosticsNote() {
      const d = this.status.diagnostics;
      return d ? 'on, group ' + d.group_id + ' (' + window.fmt.number(d.rows_queued) + ' rows)' : 'off';
    },
    metric(key) {
      const m = (this.status.metrics || {}).bridge || {};
      return window.fmt.number(m[key]);
    },

    async startBridge() {
      this.busy = 'bridge';
      try {
        await window.api.post('/api/bridge/start');
        window.toast('The bridge is starting', 'ok');
        await this.store.refresh();
      } catch (e) {
        window.toast(e.message, 'error');
      } finally {
        this.busy = null;
      }
    },

    askStop() { this.$refs.stop.showModal(); },
    cancelStop() { this.$refs.stop.close(); },
    async confirmStop() {
      this.$refs.stop.close();
      this.busy = 'stop';
      try {
        await window.api.post('/api/bridge/stop');
        window.toast('The bridge is stopped', 'ok');
      } catch (e) {
        window.toast(e.message, 'error');
      } finally {
        this.busy = null;
        await this.store.refresh();
      }
    },

    async restartBridge() {
      this.busy = 'restart';
      try {
        await window.api.post('/api/bridge/restart');
        window.toast('The bridge is restarting', 'ok');
        await this.store.refresh();
      } catch (e) {
        window.toast(e.message, 'error');
      } finally {
        this.busy = null;
      }
    },

    startSource(name) { return this.sourceAction(name, 'start', 'Started ' + name); },
    stopSource(name) { return this.sourceAction(name, 'stop', 'Stopped ' + name); },

    async sourceAction(name, action, done) {
      this.busy = name;
      try {
        const r = await window.api.post('/api/sources/' + encodeURIComponent(name) + '/' + action);
        window.toast((r && r.message) || done, 'ok');
        await this.store.refresh();
      } catch (e) {
        window.toast(e.message, 'error');
      } finally {
        this.busy = null;
      }
    },
  }));
});
