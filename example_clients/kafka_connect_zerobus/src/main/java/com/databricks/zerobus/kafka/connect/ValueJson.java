package com.databricks.zerobus.kafka.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

/**
 * Converts Connect values to JSON for Zerobus JSON streams. Wire conventions follow the Zerobus
 * type table: TIMESTAMP as epoch microseconds, DATE as days since epoch, DECIMAL as text,
 * BINARY as base64 text, VARIANT as a JSON-encoded string.
 */
public final class ValueJson {

    static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonNodeFactory F = JsonNodeFactory.instance;
    private static final long MILLIS_PER_DAY = 86_400_000L;

    private ValueJson() {}

    /** Struct with a schema to a JSON object. Null fields are omitted (absent == NULL). */
    public static ObjectNode struct(Struct s) {
        ObjectNode o = F.objectNode();
        for (Field f : s.schema().fields()) {
            Object v = s.get(f);
            if (v == null) {
                continue;
            }
            if (!ZerobusSinkConfig.validIdentifier(f.name())) {
                throw new DataException("field name is not a valid identifier: " + f.name());
            }
            o.set(f.name(), value(f.schema(), v));
        }
        return o;
    }

    /** A Connect value under its schema. */
    public static JsonNode value(Schema schema, Object v) {
        if (v == null) {
            return F.nullNode();
        }
        if (schema.name() != null) {
            if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
                return F.numberNode(((Date) v).getTime() * 1000L);
            }
            if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schema.name())) {
                return F.numberNode(((Date) v).getTime() / MILLIS_PER_DAY);
            }
            if (Decimal.LOGICAL_NAME.equals(schema.name())) {
                return F.textNode(((BigDecimal) v).toPlainString());
            }
            if (Time.LOGICAL_NAME.equals(schema.name())) {
                throw new DataException("Connect Time logical type is not mapped");
            }
        }
        switch (schema.type()) {
            case STRUCT:
                return struct((Struct) v);
            case ARRAY: {
                ArrayNode a = F.arrayNode();
                for (Object e : (Iterable<?>) v) {
                    a.add(value(schema.valueSchema(), e));
                }
                return a;
            }
            case MAP: {
                ObjectNode o = F.objectNode();
                for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                    if (!(e.getKey() instanceof String)) {
                        throw new DataException("map keys must be strings for Zerobus MAP columns");
                    }
                    o.set((String) e.getKey(), value(schema.valueSchema(), e.getValue()));
                }
                return o;
            }
            case BOOLEAN: return F.booleanNode((Boolean) v);
            case INT8: return F.numberNode(((Byte) v).intValue());
            case INT16: return F.numberNode(((Short) v).intValue());
            case INT32: return F.numberNode((Integer) v);
            case INT64: return F.numberNode((Long) v);
            case FLOAT32: return F.numberNode((Float) v);
            case FLOAT64: return F.numberNode((Double) v);
            case STRING: return F.textNode((String) v);
            case BYTES: return F.textNode(base64(v));
            default:
                throw new DataException("no JSON mapping for Connect type " + schema.type());
        }
    }

    /** Schemaless value (Map, List, primitives) to JSON. */
    public static JsonNode schemaless(Object v) {
        if (v == null) {
            return F.nullNode();
        }
        if (v instanceof byte[] || v instanceof ByteBuffer) {
            return F.textNode(base64(v));
        }
        return MAPPER.valueToTree(v);
    }

    /** Text of a value as a JSON document (for VARIANT columns). Strings become JSON string literals. */
    public static String asJsonText(Schema schema, Object v) {
        try {
            if (schema != null && schema.type() == Schema.Type.STRUCT) {
                return MAPPER.writeValueAsString(struct((Struct) v));
            }
            if (schema != null) {
                return MAPPER.writeValueAsString(value(schema, v));
            }
            if (v instanceof String || v instanceof byte[] || v instanceof ByteBuffer) {
                return MAPPER.writeValueAsString(v instanceof String ? v : base64(v));
            }
            return MAPPER.writeValueAsString(schemaless(v));
        } catch (java.io.IOException e) {
            throw new DataException("value is not representable as JSON", e);
        }
    }

    static String base64(Object v) {
        byte[] bytes;
        if (v instanceof byte[]) {
            bytes = (byte[]) v;
        } else if (v instanceof ByteBuffer) {
            ByteBuffer b = ((ByteBuffer) v).duplicate();
            bytes = new byte[b.remaining()];
            b.get(bytes);
        } else {
            throw new DataException("expected bytes, got " + v.getClass().getName());
        }
        return Base64.getEncoder().encodeToString(bytes);
    }
}
