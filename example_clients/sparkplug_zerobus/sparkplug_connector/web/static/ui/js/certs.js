// Certificates page: upload (drop zone or file picker) with the filename rule and an overwrite confirmation,
// search, and delete with confirmation.
document.addEventListener('alpine:init', () => {
  'use strict';
  const Alpine = window.Alpine;
  // Same rule as the server: letters, digits, '_' and '-', with at most one '.' that is neither first nor last.
  const NAME = /^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)?$/;

  Alpine.data('certsPage', () => ({
    files: [],
    directory: '',
    maxSize: 1048576,
    loading: true,
    uploading: false,
    search: '',
    over: false,
    pendingFile: null,
    toDelete: null,

    async init() {
      await this.load();
    },

    async load() {
      try {
        const result = await window.api.get('/api/certs');
        this.files = result.files;
        this.maxSize = result.maxSize;
        this.directory = result.directory;
      } catch (e) {
        window.toast(e.message, 'error');
      } finally {
        this.loading = false;
      }
    },

    can(permission) { return Alpine.store('connector').can(permission); },

    get filtered() {
      const q = this.search.trim().toLowerCase();
      return q ? this.files.filter((f) => f.name.toLowerCase().includes(q)) : this.files;
    },
    get countNote() {
      if (this.files.length === 0) return '';
      return this.filtered.length === this.files.length
        ? this.files.length + (this.files.length === 1 ? ' file' : ' files')
        : this.filtered.length + ' of ' + this.files.length + ' files';
    },
    get pendingName() { return this.pendingFile ? this.pendingFile.name : ''; },
    get deleteName() { return this.toDelete ? this.toDelete.name : ''; },
    get deleteUsers() { return this.toDelete && this.toDelete.usedBy.length ? this.toDelete.usedBy.join(', ') : ''; },
    pendingUsersOf(name) {
      const existing = this.files.find((f) => f.name === name);
      return existing && existing.usedBy.length ? existing.usedBy.join(', ') : '';
    },
    get pendingUsers() {
      const existing = this.pendingFile && this.files.find((f) => f.name === this.pendingFile.name);
      return existing && existing.usedBy.length ? existing.usedBy.join(', ') : '';
    },

    size(file) { return window.fmt.bytes(file.size); },
    modified(file) { return window.fmt.dateTime(file.modified); },
    usedBy(file) { return file.usedBy.length ? file.usedBy.join(', ') : '—'; },

    choose() { this.$refs.input.click(); },
    onPick(event) {
      const file = event.target.files[0];
      event.target.value = '';
      if (file) this.start(file);
    },
    onDragOver() { this.over = true; },
    onDragLeave() { this.over = false; },
    onDrop(event) {
      this.over = false;
      const file = event.dataTransfer.files[0];
      if (file) this.start(file);
    },

    start(file) {
      if (!NAME.test(file.name) || file.name.length > 128) {
        window.toast('"' + file.name + '" is not allowed: use letters, digits, _ and -, with at most one dot. Rename the file and try again.', 'error');
        return;
      }
      if (file.size > this.maxSize) {
        window.toast(file.name + ' is larger than 1 MB.', 'error');
        return;
      }
      if (this.files.some((f) => f.name === file.name)) {
        this.pendingFile = file;
        this.$refs.overwrite.showModal();
        return;
      }
      this.upload(file, false);
    },

    cancelOverwrite() {
      this.$refs.overwrite.close();
      this.pendingFile = null;
    },
    confirmOverwrite() {
      const file = this.pendingFile;
      this.$refs.overwrite.close();
      this.pendingFile = null;
      this.upload(file, true);
    },

    async upload(file, overwrite) {
      const form = new FormData();
      form.append('file', file, file.name);
      this.uploading = true;
      try {
        const saved = await window.api.post('/api/certs' + (overwrite ? '?overwrite=true' : ''), form);
        window.toast((overwrite ? 'Replaced ' : 'Uploaded ') + saved.name
          + (overwrite && this.pendingUsersOf(saved.name) ? '. Stop and start the sources using it.' : ''), 'ok');
        await this.load();
      } catch (e) {
        window.toast(e.message, 'error');
      } finally {
        this.uploading = false;
      }
    },

    askDelete(file) {
      this.toDelete = file;
      this.$refs.remove.showModal();
    },
    cancelDelete() {
      this.$refs.remove.close();
      this.toDelete = null;
    },
    async confirmDelete() {
      const file = this.toDelete;
      this.$refs.remove.close();
      this.toDelete = null;
      try {
        await window.api.del('/api/certs/' + encodeURIComponent(file.name));
        window.toast('Deleted ' + file.name, 'ok');
        await this.load();
      } catch (e) {
        window.toast(e.message, 'error');
      }
    },
  }));
});
