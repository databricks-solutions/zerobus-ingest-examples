package com.databricks.zerobus.kafka.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Messages on the control topic. Keys are table names, so every message for one table lands on the
 * same control partition and therefore on the same table-service owner.
 */
public final class ControlMessage {

    /** Ask the owner for the live schema of a table (sent at startup and on first use). */
    public static final String SNAPSHOT_REQUEST = "SNAPSHOT_REQUEST";
    /** Ask the owner to add missing additive columns (name -> Delta DDL type). */
    public static final String CHANGE_REQUEST = "CHANGE_REQUEST";
    /** Ask the owner to create the table if it is missing (columns = the full proposed schema). */
    public static final String CREATE_REQUEST = "CREATE_REQUEST";
    /** Owner publishes the live schema after a snapshot, create or change. Version is the Delta table version. */
    public static final String SCHEMA_CHANGED = "SCHEMA_CHANGED";

    /** Something that can send control messages. The control channel implements it; tests use a fake. */
    public interface Port {
        void send(ControlMessage m);
    }

    public final String type;
    public final String table;
    public final long version;
    public final boolean exists;
    public final String requester;
    public final String reason;
    /** Ordered name -> Delta type. */
    public final Map<String, String> columns;

    public ControlMessage(String type, String table, long version, boolean exists, String requester,
                          String reason, Map<String, String> columns) {
        this.type = type;
        this.table = table;
        this.version = version;
        this.exists = exists;
        this.requester = requester;
        this.reason = reason;
        this.columns = Collections.unmodifiableMap(new LinkedHashMap<>(columns));
    }

    public static ControlMessage snapshotRequest(String table, String requester) {
        return new ControlMessage(SNAPSHOT_REQUEST, table, 0, true, requester, "", Collections.emptyMap());
    }

    public static ControlMessage changeRequest(String table, String requester, Map<String, String> columns) {
        return new ControlMessage(CHANGE_REQUEST, table, 0, true, requester, "", columns);
    }

    /** rescueTable: whether the owner adds the rescue column (not for the dead-letter table). */
    public static ControlMessage createRequest(String table, String requester, Map<String, String> columns, boolean rescueTable) {
        return new ControlMessage(CREATE_REQUEST, table, 0, true, requester, rescueTable ? RESCUE : PLAIN, columns);
    }

    public static final String RESCUE = "rescue";
    public static final String PLAIN = "plain";

    public static ControlMessage schemaChanged(String table, long version, boolean exists, String reason,
                                               Map<String, String> columns) {
        return new ControlMessage(SCHEMA_CHANGED, table, version, exists, "table-service", reason, columns);
    }

    public boolean isRequest() {
        return !SCHEMA_CHANGED.equals(type);
    }

    public String toJson() {
        ObjectNode o = ValueJson.MAPPER.createObjectNode();
        o.put("type", type);
        o.put("table", table);
        o.put("version", version);
        o.put("exists", exists);
        o.put("requester", requester);
        o.put("reason", reason);
        ObjectNode cols = o.putObject("columns");
        columns.forEach(cols::put);
        try {
            return ValueJson.MAPPER.writeValueAsString(o);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static ControlMessage parse(String json) throws IOException {
        JsonNode o = ValueJson.MAPPER.readTree(json);
        Map<String, String> cols = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> it = o.path("columns").fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            cols.put(e.getKey(), e.getValue().asText());
        }
        return new ControlMessage(o.path("type").asText(), o.path("table").asText(), o.path("version").asLong(0),
                o.path("exists").asBoolean(true), o.path("requester").asText(""), o.path("reason").asText(""), cols);
    }
}
