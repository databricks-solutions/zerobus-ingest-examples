package com.databricks.zerobus.kafka.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Column sets the connector proposes for CREATE TABLE, and the dead-letter row layout. The dead-letter
 * columns depend on the options (zerobus.dlq.include.metadata, zerobus.dlq.include.key); auto-create
 * builds only the columns that are enabled (iteration 6, D92).
 */
final class TableColumns {

    static final String FLAG_ORIGINAL_BYTES = "value_original_bytes";
    static final String FLAG_TRUNCATED = "value_truncated";

    /** Every dead-letter column, in order (all options on). deadLetter(cfg) is a subset of this. */
    static final Map<String, String> DEAD_LETTER = new LinkedHashMap<>();
    static {
        DEAD_LETTER.put("cluster", "STRING");
        DEAD_LETTER.put("topic", "STRING");
        DEAD_LETTER.put("partition", "INT");
        DEAD_LETTER.put("offset", "BIGINT");
        DEAD_LETTER.put("timestamp", "TIMESTAMP");
        // key and value are raw bytes: BINARY, sent as base64 text in JSON (see deadLetterRow)
        DEAD_LETTER.put("key", "BINARY");
        DEAD_LETTER.put("value", "BINARY");
        // iteration 5: the size of the value as sent, and whether the value was left out (oversize rows)
        DEAD_LETTER.put(FLAG_ORIGINAL_BYTES, "BIGINT");
        DEAD_LETTER.put(FLAG_TRUNCATED, "BOOLEAN");
        DEAD_LETTER.put("headers", "MAP<STRING, STRING>");
        DEAD_LETTER.put("error_message", "STRING");
        DEAD_LETTER.put("failed_at", "TIMESTAMP");
        DEAD_LETTER.put("connector", "STRING");
    }

    /** Kafka metadata columns of a dead-letter row (zerobus.dlq.include.metadata). */
    static final String[] METADATA_COLUMNS = {"cluster", "topic", "partition", "offset", "timestamp", "headers"};

    /**
     * Largest dead-letter row the connector sends (serialized JSON). Zerobus's record limit is 10 MB
     * (10,485,760 bytes); the row stays below it so a dead letter never fails the task (iteration 5, D83).
     */
    static final int DEAD_LETTER_MAX_ROW_BYTES = 9 * 1024 * 1024;
    /** Error text kept in a dead-letter row when the row is still too large without it (never in practice). */
    static final int DEAD_LETTER_MAX_ERROR_CHARS = 4096;

    private TableColumns() {}

    /**
     * Columns for CREATE TABLE IF NOT EXISTS, from the first record that needs the table. Typed mode
     * needs a schema'd value; envelope mode has a fixed shape.
     */
    static Map<String, String> forRecord(ZerobusSinkConfig cfg, SinkRecord r, boolean typed) {
        Map<String, String> cols = new LinkedHashMap<>();
        if (!typed) {
            cols.put("record_key", "STRING");
            cols.put("record_value", "VARIANT");
            if (cfg.envelopeHeaders()) {
                cols.put("record_headers", "MAP<STRING, STRING>");
            }
            cols.put("record_timestamp", "TIMESTAMP");
        } else {
            if (!(r.value() instanceof Struct)) {
                throw new TableRouter.RouteException("auto-create in typed mode needs a schema'd value (use envelope mode or a schema)");
            }
            for (Field f : ((Struct) r.value()).schema().fields()) {
                cols.put(f.name(), DeltaTypes.ddl(f.schema()));
            }
        }
        if (cfg.metadata()) {
            cols.put("_kafka", RecordMapper.Common.metadataDdl(cfg.metadataFields()));
        }
        return cols;
    }

    /** The dead-letter columns for this configuration, in DEAD_LETTER order. */
    static Map<String, String> deadLetter(ZerobusSinkConfig cfg) {
        Map<String, String> cols = new LinkedHashMap<>();
        for (Map.Entry<String, String> c : DEAD_LETTER.entrySet()) {
            String name = c.getKey();
            if (isMetadata(name) && !cfg.dlqIncludeMetadata()) {
                continue;
            }
            if (name.equals("key") && !cfg.dlqIncludeKey()) {
                continue;
            }
            cols.put(name, c.getValue());
        }
        return cols;
    }

    private static boolean isMetadata(String name) {
        for (String m : METADATA_COLUMNS) {
            if (m.equals(name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The dead-letter row for a record that could not be routed, mapped or sent. Coordinates, error text and
     * timestamps are always kept; metadata (zerobus.dlq.include.metadata) and the key (zerobus.dlq.include.key)
     * only when enabled. When the row would exceed DEAD_LETTER_MAX_ROW_BYTES, the value is left out (value null,
     * value_truncated true) and the metadata is kept, if zerobus.dlq.oversize.metadata.only is true. If that
     * option is false, the oversize row is not written and this returns null (the caller logs and skips it).
     */
    static JsonNode deadLetterRow(ZerobusSinkConfig cfg, SinkRecord r, String cluster, String error) {
        byte[] raw = valueBytes(r);
        ObjectNode o = ValueJson.MAPPER.createObjectNode();
        if (cfg.dlqIncludeMetadata()) {
            putMetadata(o, r, cluster);
        }
        o.put("error_message", error);
        // TIMESTAMP is epoch microseconds on the wire (as for every other TIMESTAMP); an ISO string is rejected by the server.
        o.put("failed_at", Instant.now().toEpochMilli() * 1000L);
        o.put("connector", cfg.connectorName());
        if (cfg.dlqIncludeKey()) {
            o.put("key", keyBase64(r));
        }
        o.put("value", raw == null ? null : Base64.getEncoder().encodeToString(raw));
        if (raw != null) {
            o.put(FLAG_ORIGINAL_BYTES, (long) raw.length);
        } else {
            o.putNull(FLAG_ORIGINAL_BYTES);
        }
        o.put(FLAG_TRUNCATED, false);
        if (cfg.dlqIncludeMetadata()) {
            ObjectNode h = ValueJson.MAPPER.createObjectNode();
            RecordMapper.Common.headers(h, r.headers());
            o.set("headers", h);
        }
        if (size(o) > DEAD_LETTER_MAX_ROW_BYTES && raw != null) {
            if (!cfg.dlqOversizeMetadataOnly()) {
                return null;  // the caller skips the row and logs it (zerobus.dlq.oversize.metadata.only=false)
            }
            o.putNull("value");
            o.put(FLAG_TRUNCATED, true);
        }
        if (size(o) > DEAD_LETTER_MAX_ROW_BYTES && cfg.dlqIncludeMetadata()) {
            o.set("headers", ValueJson.MAPPER.createObjectNode());
        }
        if (size(o) > DEAD_LETTER_MAX_ROW_BYTES && cfg.dlqIncludeKey()) {
            o.putNull("key");  // a key this large is not expected; the row still lands with its coordinates
        }
        if (size(o) > DEAD_LETTER_MAX_ROW_BYTES && error != null && error.length() > DEAD_LETTER_MAX_ERROR_CHARS) {
            o.put("error_message", error.substring(0, DEAD_LETTER_MAX_ERROR_CHARS));
        }
        return o;
    }

    private static void putMetadata(ObjectNode o, SinkRecord r, String cluster) {
        o.put("cluster", cluster);
        o.put("topic", r.originalTopic());
        o.put("partition", r.originalKafkaPartition());
        o.put("offset", r.originalKafkaOffset());
        if (r.timestamp() != null) {
            o.put("timestamp", r.timestamp() * 1000L);
        } else {
            o.putNull("timestamp");
        }
    }

    /** Serialized size of a row, as sent. */
    static int size(JsonNode row) {
        try {
            return ValueJson.MAPPER.writeValueAsBytes(row).length;
        } catch (IOException e) {
            throw new IllegalStateException("dead-letter row does not serialize: " + e.getMessage(), e);
        }
    }

    /** Key bytes as base64 (a string key is its UTF-8 bytes), for a BINARY column. */
    private static String keyBase64(SinkRecord r) {
        Object k = r.key();
        if (k == null) {
            return null;
        }
        byte[] b = k instanceof byte[] ? (byte[]) k : String.valueOf(k).getBytes(StandardCharsets.UTF_8);
        return Base64.getEncoder().encodeToString(b);
    }

    /** Value bytes: raw bytes as they are, schema'd or schemaless values as their JSON text (null for a null value). */
    static byte[] valueBytes(SinkRecord r) {
        Object v = r.value();
        if (v == null) {
            return null;
        }
        if (v instanceof byte[]) {
            return (byte[]) v;
        }
        String json = ValueJson.asJsonText(r.valueSchema(), v);
        return json == null ? null : json.getBytes(StandardCharsets.UTF_8);
    }
}
