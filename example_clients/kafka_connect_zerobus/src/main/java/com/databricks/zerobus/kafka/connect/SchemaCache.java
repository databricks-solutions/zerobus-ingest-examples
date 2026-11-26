package com.databricks.zerobus.kafka.connect;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Per-task view of table schemas, updated from SCHEMA_CHANGED messages. Versions never move backwards. */
final class SchemaCache {

    /** Live state of a table at a Delta version. */
    static final class Snapshot {
        final boolean exists;
        final long version;
        final Map<String, String> columns;
        final long appliedAtMs;

        Snapshot(boolean exists, long version, Map<String, String> columns) {
            this.exists = exists;
            this.version = version;
            this.columns = Collections.unmodifiableMap(columns);
            this.appliedAtMs = System.currentTimeMillis();
        }
    }

    private final Map<String, Snapshot> tables = new ConcurrentHashMap<>();

    void apply(String table, boolean exists, long version, Map<String, String> columns) {
        tables.merge(table, new Snapshot(exists, version, columns),
                (old, neu) -> neu.version >= old.version ? neu : old);
    }

    Snapshot get(String table) {
        return tables.get(table);
    }
}
