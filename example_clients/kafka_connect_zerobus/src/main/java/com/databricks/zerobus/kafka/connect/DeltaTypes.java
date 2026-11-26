package com.databricks.zerobus.kafka.connect;

import java.util.Locale;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

/** Maps Kafka Connect schemas to Delta DDL types, and compares them with DESCRIBE output. */
public final class DeltaTypes {

    private DeltaTypes() {}

    /** Delta DDL type for a Connect schema, e.g. BIGINT, STRUCT<a: STRING>, ARRAY<INT>. */
    public static String ddl(Schema s) {
        if (s.name() != null) {
            if (Decimal.LOGICAL_NAME.equals(s.name())) {
                String scale = s.parameters() == null ? "0" : s.parameters().getOrDefault(Decimal.SCALE_FIELD, "0");
                return "DECIMAL(38," + scale + ")";
            }
            if (Date.LOGICAL_NAME.equals(s.name())) {
                return "DATE";
            }
            if (Timestamp.LOGICAL_NAME.equals(s.name())) {
                return "TIMESTAMP";
            }
            if (Time.LOGICAL_NAME.equals(s.name())) {
                throw new DataException("Connect Time logical type has no Zerobus/Delta mapping in this connector");
            }
        }
        switch (s.type()) {
            case INT8: return "TINYINT";
            case INT16: return "SMALLINT";
            case INT32: return "INT";
            case INT64: return "BIGINT";
            case FLOAT32: return "FLOAT";
            case FLOAT64: return "DOUBLE";
            case BOOLEAN: return "BOOLEAN";
            case STRING: return "STRING";
            case BYTES: return "BINARY";
            case ARRAY: return "ARRAY<" + ddl(s.valueSchema()) + ">";
            case MAP: return "MAP<" + ddl(s.keySchema()) + "," + ddl(s.valueSchema()) + ">";
            case STRUCT: {
                StringBuilder b = new StringBuilder("STRUCT<");
                boolean first = true;
                for (org.apache.kafka.connect.data.Field f : s.fields()) {
                    if (!ZerobusSinkConfig.validIdentifier(f.name())) {
                        throw new DataException("field name is not a valid identifier: " + f.name());
                    }
                    if (!first) b.append(", ");
                    b.append(f.name()).append(": ").append(ddl(f.schema()));
                    first = false;
                }
                return b.append(">").toString();
            }
            default:
                throw new DataException("no Delta mapping for Connect type " + s.type());
        }
    }

    /** Normalised form used to compare a Connect-derived type with a DESCRIBE type. */
    public static String normalise(String t) {
        return t == null ? "" : t.replaceAll("\\s+", "").toUpperCase(Locale.ROOT);
    }

    /**
     * True when the type is a primitive we compare exactly. Complex types (STRUCT, ARRAY, MAP) are
     * not compared in this POC; the server remains the check for them.
     */
    public static boolean comparable(String ddlType) {
        String n = normalise(ddlType);
        return !(n.startsWith("STRUCT") || n.startsWith("ARRAY") || n.startsWith("MAP"));
    }

    /** True when the Connect-derived type matches the live column type (primitives only). */
    public static boolean matches(String connectDdl, String liveType) {
        if (!comparable(connectDdl) || !comparable(liveType)) {
            return true;
        }
        return normalise(connectDdl).equals(normalise(liveType));
    }
}
