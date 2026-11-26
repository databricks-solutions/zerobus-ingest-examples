package com.databricks.zerobus.kafka.connect;

import java.io.IOException;
import java.util.Map;

/** The DDL surface the table service needs. Implemented by the SQL client; faked in tests. */
public interface TableCatalog {

    /** Live state of a table. exists=false when the table is missing (version is -1 then). */
    final class Snapshot {
        public final boolean exists;
        public final long version;
        public final Map<String, String> columns;

        public Snapshot(boolean exists, long version, Map<String, String> columns) {
            this.exists = exists;
            this.version = version;
            this.columns = columns;
        }
    }

    Snapshot describe(String table) throws IOException;

    /** CREATE TABLE IF NOT EXISTS. Returns quietly when the table already exists. */
    void createIfNotExists(String table, Map<String, String> columns) throws IOException;

    /** Tags a column zerobus-rescue (applied after the create; inline TAGS is not supported in this SQL dialect). */
    void setRescueTag(String table, String column) throws IOException;

    /** One ALTER TABLE ... ADD COLUMNS for nullable columns. */
    void addColumns(String table, Map<String, String> columns) throws IOException;
}
