package com.databricks.zerobus.kafka.connect;

import com.databricks.zerobus.ZerobusSdk;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;

/**
 * Probe, run by hand: sends one row per probed column type through the production Arrow path
 * (ArrowTypes.schemaFor, ArrowRows.build, SdkArrowSinkFactory) to a table kc3_probe_<name> created by
 * testdata/it/probe_types.sh. Prints one PROBE line per type: OK (with the offset) or ERROR (with the server text).
 * Usage: ArrowProbe <table-prefix>. Credentials come from the environment, never from argv.
 */
final class ArrowProbe {

    private ArrowProbe() {}

    /** One probe: a table with id BIGINT and one column, the Connect schema of that column, and the row JSON. */
    static final class Case {
        final String name;
        final String column;
        final org.apache.kafka.connect.data.Schema type;
        final String json;

        Case(String name, String column, org.apache.kafka.connect.data.Schema type, String json) {
            this.name = name;
            this.column = column;
            this.type = type;
            this.json = json;
        }
    }

    static List<Case> cases() {
        org.apache.kafka.connect.data.Schema ts = Timestamp.builder().optional().build();
        org.apache.kafka.connect.data.Schema struct = SchemaBuilder.struct().optional()
                .field("a", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
                .field("s", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
                .field("ts", ts).build();
        org.apache.kafka.connect.data.Schema kafka = SchemaBuilder.struct().optional()
                .field("cluster", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
                .field("topic", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
                .field("partition", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
                .field("offset", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
                .field("key", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
                .field("timestamp", ts).build();
        List<Case> out = new ArrayList<>();
        out.add(new Case("bigint", "c", org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA, "{\"id\":1,\"c\":42}"));
        out.add(new Case("int", "c", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA, "{\"id\":1,\"c\":7}"));
        out.add(new Case("double", "c", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA, "{\"id\":1,\"c\":1.5}"));
        out.add(new Case("string", "c", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA, "{\"id\":1,\"c\":\"x\"}"));
        out.add(new Case("boolean", "c", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA, "{\"id\":1,\"c\":true}"));
        out.add(new Case("timestamp", "c", ts, "{\"id\":1,\"c\":1700000000000000}"));
        out.add(new Case("array_string", "c", SchemaBuilder.array(org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA).optional().build(),
                "{\"id\":1,\"c\":[\"a\",\"b\"]}"));
        out.add(new Case("struct", "c", struct, "{\"id\":1,\"c\":{\"a\":1,\"s\":\"s\",\"ts\":1700000000000000}}"));
        out.add(new Case("kafka_struct", "_kafka", kafka,
                "{\"id\":1,\"_kafka\":{\"cluster\":\"cl\",\"topic\":\"t\",\"partition\":0,\"offset\":5,\"key\":null,"
                        + "\"timestamp\":1700000000000000}}"));
        out.add(new Case("array_struct", "c", SchemaBuilder.array(struct).optional().build(),
                "{\"id\":1,\"c\":[{\"a\":1,\"s\":\"x\",\"ts\":null}]}"));
        return out;
    }

    public static void main(String[] args) throws Exception {
        String prefix = args.length > 0 ? args[0] : "kc3_probe_";
        ZerobusSdk sdk = new ZerobusSdk(System.getenv("ZEROBUS_SERVER_ENDPOINT"), System.getenv("DATABRICKS_WORKSPACE_URL"));
        SdkArrowSinkFactory factory = new SdkArrowSinkFactory(sdk, System.getenv("DATABRICKS_CLIENT_ID"),
                System.getenv("DATABRICKS_CLIENT_SECRET"));
        BufferAllocator alloc = new RootAllocator();
        for (Case c : cases()) {
            String table = prefix + c.name;
            String result;
            ArrowSink s = null;
            try {
                org.apache.kafka.connect.data.SchemaBuilder rec = SchemaBuilder.struct().field("id",
                        org.apache.kafka.connect.data.Schema.INT64_SCHEMA).field(c.column, c.type);
                Schema arrow = ArrowTypes.schemaFor(rec.build(), null);
                JsonNode row = ValueJson.MAPPER.readTree(c.json);
                s = factory.open(table, arrow);
                try (VectorSchemaRoot root = ArrowRows.build(alloc, arrow, Collections.singletonList(row))) {
                    long off = s.ingest(root);
                    s.flush(Duration.ofSeconds(120));
                    result = "OK offset=" + off;
                }
            } catch (Throwable t) {
                result = "ERROR " + t;
            } finally {
                if (s != null) {
                    s.close();
                }
            }
            System.out.println("PROBE " + c.name + " table=" + table + " result=" + result);
        }
        System.out.println("PROBE_DONE");
        System.exit(0);
    }
}
