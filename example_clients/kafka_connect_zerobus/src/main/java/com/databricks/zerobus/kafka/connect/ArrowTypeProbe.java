package com.databricks.zerobus.kafka.connect;

import com.databricks.zerobus.ZerobusSdk;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Iteration 4 type probe (work item 4). Each case writes one row, built directly as Arrow vectors, through the
 * SDK Arrow stream to a table prefix + case name (the runner creates the table with the SQL type). The read-back
 * is done by SQL in the runner: a type moves to the connector's Arrow path only if the write and the read-back
 * are both exact. Usage: ArrowTypeProbe [table-prefix]. Credentials come from the environment, never from argv.
 */
final class ArrowTypeProbe {

    private ArrowTypeProbe() {}

    /** Writes the value of one case into row 0 of column c. */
    interface Writer {
        void write(VectorSchemaRoot root);
    }

    static final class Case {
        final String name;
        final Field column;
        final Writer writer;
        final String expect;

        Case(String name, Field column, Writer writer, String expect) {
            this.name = name;
            this.column = column;
            this.writer = writer;
            this.expect = expect;
        }
    }

    static List<Case> cases() {
        List<Case> out = new ArrayList<>();
        out.add(new Case("float", new Field("c", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null),
                r -> ((Float4Vector) r.getVector("c")).setSafe(0, 1.5f), "1.5"));
        out.add(new Case("tinyint", new Field("c", FieldType.nullable(new ArrowType.Int(8, true)), null),
                r -> ((TinyIntVector) r.getVector("c")).setSafe(0, (byte) 7), "7"));
        out.add(new Case("smallint", new Field("c", FieldType.nullable(new ArrowType.Int(16, true)), null),
                r -> ((SmallIntVector) r.getVector("c")).setSafe(0, (short) 300), "300"));
        out.add(new Case("decimal", new Field("c", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null),
                r -> ((DecimalVector) r.getVector("c")).setSafe(0, new BigDecimal("12.34")), "12.34"));
        out.add(new Case("date", new Field("c", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
                r -> ((DateDayVector) r.getVector("c")).setSafe(0, 19000), "19000 days = 2022-01-08"));
        out.add(new Case("binary", new Field("c", FieldType.nullable(ArrowType.Binary.INSTANCE), null),
                r -> ((VarBinaryVector) r.getVector("c")).setSafe(0, "hi".getBytes(StandardCharsets.UTF_8)), "hi (6869)"));
        Field entries = new Field("entries", FieldType.notNullable(new ArrowType.Struct()), Arrays.asList(
                new Field("key", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null),
                new Field("value", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
        out.add(new Case("map_string", new Field("c", FieldType.nullable(new ArrowType.Map(false)), Collections.singletonList(entries)),
                r -> {
                    MapVector m = (MapVector) r.getVector("c");
                    m.startNewValue(0);
                    StructVector e = (StructVector) m.getDataVector();
                    e.setIndexDefined(0);
                    e.getChild("key", VarCharVector.class).setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
                    e.getChild("value", VarCharVector.class).setSafe(0, "b".getBytes(StandardCharsets.UTF_8));
                    m.endValue(0, 1);
                    m.setValueCount(1);
                }, "{a=b}"));
        return out;
    }

    public static void main(String[] args) throws Exception {
        String prefix = args.length > 0 ? args[0] : "kc4_probe_";
        ZerobusSdk sdk = new ZerobusSdk(System.getenv("ZEROBUS_SERVER_ENDPOINT"), System.getenv("DATABRICKS_WORKSPACE_URL"));
        SdkArrowSinkFactory factory = new SdkArrowSinkFactory(sdk, System.getenv("DATABRICKS_CLIENT_ID"),
                System.getenv("DATABRICKS_CLIENT_SECRET"));
        BufferAllocator alloc = new RootAllocator();
        for (Case c : cases()) {
            String table = prefix + c.name;
            String result;
            ArrowSink s = null;
            try {
                Schema schema = new Schema(Arrays.asList(new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null), c.column));
                s = factory.open(table, schema);
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
                    ((BigIntVector) root.getVector("id")).setSafe(0, 1L);
                    c.writer.write(root);
                    root.setRowCount(1);
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
            System.out.println("PROBE " + c.name + " table=" + table + " expect=" + c.expect + " result=" + result);
        }
        System.out.println("PROBE_DONE");
        System.exit(0);
    }
}
