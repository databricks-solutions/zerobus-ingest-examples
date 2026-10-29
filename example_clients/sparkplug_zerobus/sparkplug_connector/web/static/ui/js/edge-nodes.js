// Edge nodes page: /api/sparkplug/edge-nodes per running source (refreshed every 5 s), a filter, and rebirth
// requests (POST /api/sources/<source>/rebirth).
document.addEventListener('alpine:init', () => {
  'use strict';
  const Alpine = window.Alpine;

  Alpine.data('edgeNodesPage', () => ({
    data: {},
    loading: true,
    search: '',
    busy: null,

    async init() {
      await this.load();
      setInterval(() => this.load(), 5000);
    },

    async load() {
      try {
        this.data = await window.api.get('/api/sparkplug/edge-nodes');
      } catch (e) {
        // the store's "offline" marker covers it
      } finally {
        this.loading = false;
      }
    },

    can(permission) { return Alpine.store('connector').can(permission); },

    matches(n) {
      const q = this.search.trim().toLowerCase();
      if (!q) return true;
      const names = [n.group_id, n.edge_node_id].concat((n.devices || []).map((d) => d.device_id));
      return names.some((x) => String(x).toLowerCase().includes(q));
    },

    get groups() {
      return Object.keys(this.data || {}).sort().map((source) => ({
        source,
        nodes: (this.data[source] || []).filter((n) => this.matches(n)).map((n) => ({
          key: source + '/' + n.group_id + '/' + n.edge_node_id,
          group_id: n.group_id,
          edge_node_id: n.edge_node_id,
          online: !!n.online,
          sinceText: window.fmt.dateTime(n.since),
          ago: window.fmt.ago(n.since),
          changedBy: n.changed_by || '–',
          bdseq: n.bdseq === null || n.bdseq === undefined ? '–' : n.bdseq,
          devices: (n.devices || []).map((d) => ({
            device_id: d.device_id,
            online: !!d.online,
            title: (d.online ? 'online' : 'offline') + ' since ' + window.fmt.dateTime(d.since) + (d.changed_by ? ' (' + d.changed_by + ')' : ''),
          })),
        })),
      }));
    },

    get countNote() {
      const all = Object.values(this.data || {}).reduce((a, list) => a.concat(list || []), []);
      const online = all.filter((n) => n.online).length;
      return all.length ? all.length + ' edge nodes, ' + online + ' online' : '';
    },

    async rebirth(source, n) {
      this.busy = n.key;
      try {
        const r = await window.api.post('/api/sources/' + encodeURIComponent(source) + '/rebirth',
          { group_id: n.group_id, edge_node_id: n.edge_node_id });
        window.toast((r && r.message) || 'Rebirth requested', 'ok');
      } catch (e) {
        window.toast(e.message, 'error');
      } finally {
        this.busy = null;
      }
    },
  }));
});
