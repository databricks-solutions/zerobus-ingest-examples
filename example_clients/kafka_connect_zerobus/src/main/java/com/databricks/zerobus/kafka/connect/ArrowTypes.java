package com.databricks.zerobus.kafka.connect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Timestamp;

/**
 * Connect types to Arrow types for format=arrow. Only the types probed on Arrow are eligible: BIGINT, INT,
 * DOUBLE, STRING, BOOLEAN, TIMESTAMP (iteration 3, probe tables kc3_probe_*); TINYINT, SMALLINT, FLOAT, DECIMAL(38,s),
 * DATE, BINARY and MAP<STRING,STRING> (iteration 4, probe tables kc4_probe_*, written and read back exactly);
 * and ARRAY and STRUCT of those. Any other type makes the table use JSON in auto mode. Names of the metadata
 * struct follow RecordMapper.Common.metadataDdl.
 */
final class ArrowTypes {

    /** Delta TIMESTAMP is UTC-adjusted; the probe confirmed this Arrow type maps to it. */
    static final ArrowType TIMESTAMP_TYPE = new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
    static final ArrowType INT32 = new ArrowType.Int(32, true);
    static final ArrowType INT64 = new ArrowType.Int(64, true);
    static final ArrowType FLOAT64 = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    static final ArrowType UTF8 = ArrowType.Utf8.INSTANCE;
    static final ArrowType BOOL = ArrowType.Bool.INSTANCE;
    static final ArrowType INT8 = new ArrowType.Int(8, true);
    static final ArrowType INT16 = new ArrowType.Int(16, true);
    static final ArrowType FLOAT32 = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    static final ArrowType BINARY = ArrowType.Binary.INSTANCE;
    static final ArrowType DATE_DAY = new ArrowType.Date(DateUnit.DAY);
    /** Delta DECIMAL is DECIMAL(38, scale) (DeltaTypes.ddl): the Arrow precision is fixed to match. */
    static final int DECIMAL_PRECISION = 38;

    private ArrowTypes() {}

    /** True when every type in the Connect schema tree has a probed Arrow mapping. */
    static boolean eligible(org.apache.kafka.connect.data.Schema s) {
        if (s.name() != null && s.name().startsWith("org.apache.kafka.connect.data.")) {
            // Logical types: only the probed ones. Time (INT32 underneath) must not pass as INT.
            return Timestamp.LOGICAL_NAME.equals(s.name()) || Date.LOGICAL_NAME.equals(s.name())
                    || Decimal.LOGICAL_NAME.equals(s.name());
        }
        switch (s.type()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case STRING:
            case BOOLEAN:
            case BYTES:
                return true;
            case MAP:
                return s.keySchema().type() == org.apache.kafka.connect.data.Schema.Type.STRING
                        && s.valueSchema().type() == org.apache.kafka.connect.data.Schema.Type.STRING;
            case ARRAY:
                return eligible(s.valueSchema());
            case STRUCT:
                for (org.apache.kafka.connect.data.Field f : s.fields()) {
                    if (!eligible(f.schema())) {
                        return false;
                    }
                }
                return true;
            default:
                return false;  // other logical types and the types not in the probed list
        }
    }

    /** Arrow field for a Connect schema. Caller has checked eligible(). */
    static Field field(String name, org.apache.kafka.connect.data.Schema s) {
        if (s.name() != null && Timestamp.LOGICAL_NAME.equals(s.name())) {
            return new Field(name, FieldType.nullable(TIMESTAMP_TYPE), null);
        }
        if (s.name() != null && Date.LOGICAL_NAME.equals(s.name())) {
            return new Field(name, FieldType.nullable(DATE_DAY), null);
        }
        if (s.name() != null && Decimal.LOGICAL_NAME.equals(s.name())) {
            String scale = s.parameters() == null ? null : s.parameters().get(Decimal.SCALE_FIELD);
            int sc = scale == null ? 0 : Integer.parseInt(scale);
            return new Field(name, FieldType.nullable(new ArrowType.Decimal(DECIMAL_PRECISION, sc, 128)), null);
        }
        switch (s.type()) {
            case INT8: return new Field(name, FieldType.nullable(INT8), null);
            case INT16: return new Field(name, FieldType.nullable(INT16), null);
            case INT32: return new Field(name, FieldType.nullable(INT32), null);
            case INT64: return new Field(name, FieldType.nullable(INT64), null);
            case FLOAT32: return new Field(name, FieldType.nullable(FLOAT32), null);
            case FLOAT64: return new Field(name, FieldType.nullable(FLOAT64), null);
            case STRING: return new Field(name, FieldType.nullable(UTF8), null);
            case BOOLEAN: return new Field(name, FieldType.nullable(BOOL), null);
            case BYTES: return new Field(name, FieldType.nullable(BINARY), null);
            case MAP: {
                Field entries = new Field("entries", FieldType.notNullable(new ArrowType.Struct()), Arrays.asList(
                        new Field("key", FieldType.notNullable(UTF8), null),
                        new Field("value", FieldType.nullable(UTF8), null)));
                return new Field(name, FieldType.nullable(new ArrowType.Map(false)), Collections.singletonList(entries));
            }
            case ARRAY:
                return new Field(name, FieldType.nullable(new ArrowType.List()),
                        Collections.singletonList(field("item", s.valueSchema())));
            case STRUCT: {
                List<Field> children = new ArrayList<>();
                for (org.apache.kafka.connect.data.Field f : s.fields()) {
                    children.add(field(f.name(), f.schema()));
                }
                return new Field(name, FieldType.nullable(new ArrowType.Struct()), children);
            }
            default:
                throw new IllegalArgumentException("no Arrow mapping for Connect type " + s.type());
        }
    }

    /** Arrow type of a metadata field of the _kafka struct (same names and order as the JSON mapper). */
    static Field metadataField(String f) {
        switch (f) {
            case "cluster": return new Field(f, FieldType.nullable(UTF8), null);
            case "topic": return new Field(f, FieldType.nullable(UTF8), null);
            case "partition": return new Field(f, FieldType.nullable(INT32), null);
            case "offset": return new Field(f, FieldType.nullable(INT64), null);
            case "key": return new Field(f, FieldType.nullable(UTF8), null);
            case "timestamp": return new Field(f, FieldType.nullable(TIMESTAMP_TYPE), null);
            default: throw new IllegalArgumentException("unknown metadata field " + f);
        }
    }

    /**
     * Arrow schema of a stream: the value's fields in record order, then the _kafka struct when metadata is on.
     * The stream pins this schema; a batch must match it (see ArrowRows.fits for the subset rule).
     */
    static Schema schemaFor(org.apache.kafka.connect.data.Schema value, List<String> metadataFields) {
        List<Field> fields = new ArrayList<>();
        for (org.apache.kafka.connect.data.Field f : value.fields()) {
            fields.add(field(f.name(), f.schema()));
        }
        if (metadataFields != null && !metadataFields.isEmpty()) {
            List<Field> kafka = new ArrayList<>();
            for (String f : metadataFields) {
                kafka.add(metadataField(f));
            }
            fields.add(new Field("_kafka", FieldType.nullable(new ArrowType.Struct()), kafka));
        }
        return new Schema(fields);
    }
}
