package com.databricks.zerobus.kafka.connect;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DDL owner for the tables hashed to this task's control-topic partitions. Runs on the shard
 * consumer thread. A batch is coalesced per table: several CHANGE and CREATE requests for one table
 * become one plan (the union of their columns), so 512 workers asking for the same column cost one
 * ALTER. Every step re-reads the live schema first and treats "already exists" as success, so
 * replaying a batch after an ownership move (or a retry after a timeout) cannot add a column twice.
 * The version it publishes is the Delta table version, which is monotonic across owners.
 */
final class TableService {

    private static final Logger log = LoggerFactory.getLogger(TableService.class);
    private static final int ATTEMPTS = 4;
    private static final long BACKOFF_BASE_MS = 1000L;

    /** One table's requests in a batch. */
    private static final class Plan {
        final String table;
        boolean snapshot;
        boolean create;
        boolean rescueTable = true;
        final Map<String, String> createColumns = new LinkedHashMap<>();
        final Map<String, String> changes = new LinkedHashMap<>();

        Plan(String table) {
            this.table = table;
        }
    }

    private final TableCatalog catalog;
    private final String rescueName;
    private final Consumer<ControlMessage> publish;
    private final Map<String, TableCatalog.Snapshot> cache = new HashMap<>();
    private final Random random = new Random();
    private final AtomicInteger alters = new AtomicInteger();
    private final AtomicInteger creates = new AtomicInteger();
    private final AtomicInteger coalesced = new AtomicInteger();
    private String label = "table-service";

    /** rescueName is null when the tables have no rescue column. */
    TableService(TableCatalog catalog, String rescueName, Consumer<ControlMessage> publish) {
        this.catalog = catalog;
        this.rescueName = rescueName;
        this.publish = publish;
    }

    /** Names this owner in log lines (the task that holds the shard). */
    void label(String name) {
        this.label = name;
    }

    int alterCount() {
        return alters.get();
    }

    int createCount() {
        return creates.get();
    }

    /** Requests folded into another request of the same batch. */
    int coalescedCount() {
        return coalesced.get();
    }

    /** Handles one batch from the shard consumer. SCHEMA_CHANGED messages in it are ignored. */
    void handle(List<ControlMessage> batch) {
        Map<String, Plan> plans = new LinkedHashMap<>();
        int requests = 0;
        for (ControlMessage m : batch) {
            if (!m.isRequest()) {
                continue;
            }
            requests++;
            Plan p = plans.get(m.table);
            if (p == null) {
                p = new Plan(m.table);
                plans.put(m.table, p);
            } else {
                coalesced.incrementAndGet();
            }
            switch (m.type) {
                case ControlMessage.SNAPSHOT_REQUEST:
                    p.snapshot = true;
                    break;
                case ControlMessage.CHANGE_REQUEST:
                    p.changes.putAll(m.columns);
                    break;
                case ControlMessage.CREATE_REQUEST:
                    p.create = true;
                    p.createColumns.putAll(m.columns);
                    p.changes.putAll(m.columns);  // a create for a table that exists still adds what is missing
                    p.rescueTable = !ControlMessage.PLAIN.equals(m.reason);
                    break;
                default:
                    break;
            }
        }
        if (requests > 0) {
            log.info("table service: {} request(s) for {} table(s) in this batch", requests, plans.size());
        }
        for (Plan p : plans.values()) {
            try {
                process(p);
            } catch (IOException | RuntimeException e) {
                // The requester's backoff resends the request; nothing is committed as done for a failed step.
                log.warn("{}: {} failed: {}", label, p.table, e.getMessage());
            }
        }
    }

    private void process(Plan p) throws IOException {
        String t = p.table;
        if (!p.create && p.changes.isEmpty()) {
            TableCatalog.Snapshot live = describe(t);
            cache.put(t, live);
            publishState(t, live, "snapshot");
            return;
        }
        if (!p.changes.isEmpty() && !p.create) {
            TableCatalog.Snapshot cached = cache.get(t);
            if (cached != null && cached.exists && cached.columns.keySet().containsAll(p.changes.keySet())) {
                // Already present (added by an earlier batch or another owner): republish, no SQL.
                publishState(t, cached, "already-present");
                return;
            }
        }
        TableCatalog.Snapshot live = describe(t);
        if (p.create && !live.exists) {
            Map<String, String> cols = new LinkedHashMap<>(p.createColumns);
            if (p.rescueTable && rescueName != null && !cols.containsKey(rescueName)) {
                cols.put(rescueName, "VARIANT");
            }
            retry("CREATE TABLE IF NOT EXISTS " + t, () -> catalog.createIfNotExists(t, cols));
            creates.incrementAndGet();
            if (p.rescueTable && rescueName != null) {
                retry("tag " + rescueName, () -> catalog.setRescueTag(t, rescueName));
            }
            live = describe(t);
            log.info("{}: created {} with {} column(s){}", label, t, cols.size(),
                    p.rescueTable && rescueName != null ? " (rescue column " + rescueName + " tagged)" : "");
        }
        if (!live.exists) {
            cache.remove(t);
            publish.accept(ControlMessage.schemaChanged(t, -1L, false, "missing", new LinkedHashMap<>()));
            return;
        }
        Map<String, String> toAdd = new LinkedHashMap<>();
        for (Map.Entry<String, String> c : p.changes.entrySet()) {
            String name = c.getKey();
            if (name.equals(rescueName)) {
                continue;
            }
            if (!ZerobusSinkConfig.validIdentifier(name)) {
                log.warn("table service: {} rejected, not a valid identifier", name);
                continue;
            }
            String have = live.columns.get(name);
            if (have == null) {
                toAdd.put(name, c.getValue());
            } else if (!DeltaTypes.matches(c.getValue(), have)) {
                log.warn("table service: {}.{} is {} but a record needs {}; incompatible, not applied",
                        t, name, have, c.getValue());
            }
        }
        String reason = p.changes.isEmpty() ? "snapshot" : "already-present";
        if (!toAdd.isEmpty()) {
            try {
                retry("ALTER TABLE " + t, () -> catalog.addColumns(t, toAdd));
                alters.incrementAndGet();
                reason = "added " + toAdd.keySet();
                log.info("{}: ALTER TABLE {} ADD COLUMNS {} applied", label, t, toAdd.keySet());
            } catch (IOException e) {
                if (!isAlreadyPresent(e)) {
                    throw e;
                }
                reason = "already-present (concurrent owner)";  // another owner added them first
            }
            live = describe(t);
        }
        cache.put(t, live);
        publishState(t, live, reason);
    }

    private void publishState(String table, TableCatalog.Snapshot live, String reason) {
        if (!live.exists) {
            publish.accept(ControlMessage.schemaChanged(table, -1L, false, reason, new LinkedHashMap<>()));
            return;
        }
        publish.accept(ControlMessage.schemaChanged(table, live.version, true, reason, live.columns));
    }

    private TableCatalog.Snapshot describe(String table) throws IOException {
        return retryValue("DESCRIBE " + table, () -> catalog.describe(table));
    }

    private interface IoAction {
        void run() throws IOException;
    }

    private interface IoValue<T> {
        T get() throws IOException;
    }

    private void retry(String what, IoAction action) throws IOException {
        retryValue(what, () -> {
            action.run();
            return null;
        });
    }

    /** Retries transient failures (timeouts, warehouse starting, network) with jittered backoff. Permanent errors throw at once. */
    private <T> T retryValue(String what, IoValue<T> action) throws IOException {
        IOException last = null;
        for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
            try {
                return action.get();
            } catch (IOException e) {
                if (isPermanent(e)) {
                    throw e;
                }
                last = e;
                long wait = (long) (BACKOFF_BASE_MS * (1L << attempt) * (0.5 + random.nextDouble()));
                log.warn("table service: {} attempt {} failed, retrying in {} ms: {}", what, attempt + 1, wait, e.getMessage());
                try {
                    Thread.sleep(wait);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted during " + what, ie);
                }
            }
        }
        throw last;
    }

    static boolean isAlreadyPresent(IOException e) {
        String m = String.valueOf(e.getMessage());
        return m.contains("ALREADY_EXISTS") || m.contains("already exists") || m.contains("FIELD_ALREADY_EXISTS");
    }

    static boolean isPermanent(IOException e) {
        String m = String.valueOf(e.getMessage());
        return isAlreadyPresent(e) || m.contains("PARSE_SYNTAX_ERROR") || m.contains("UNSUPPORTED")
                || m.contains("DATATYPE_MISMATCH") || m.contains("INVALID_PARAMETER");
    }
}
