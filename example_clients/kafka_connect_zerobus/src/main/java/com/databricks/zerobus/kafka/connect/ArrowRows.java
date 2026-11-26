package com.databricks.zerobus.kafka.connect;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.connect.errors.DataException;

/** Writes mapped JSON rows into a VectorSchemaRoot of a pinned schema. Missing fields are written as null. */
final class ArrowRows {

    private ArrowRows() {}

    /** A batch of rows (in order) as one root. The caller closes it after the batch is sent. */
    static VectorSchemaRoot build(BufferAllocator alloc, Schema schema, List<JsonNode> rows) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
        try {
            root.allocateNew();
            for (Field f : schema.getFields()) {
                FieldVector v = root.getVector(f.getName());
                for (int r = 0; r < rows.size(); r++) {
                    write(v, r, rows.get(r).get(f.getName()));
                }
            }
            root.setRowCount(rows.size());
            return root;
        } catch (RuntimeException e) {
            root.close();
            throw e;
        }
    }

    /**
     * True when a row fits the pinned schema: every field it sets exists in the schema (at each struct level
     * and inside arrays). Fields the schema has and the row lacks are sent as null.
     */
    static boolean fits(JsonNode row, Schema pinned) {
        return fitsFields(row, pinned.getFields());
    }

    private static boolean fitsFields(JsonNode n, List<Field> fields) {
        if (n == null || n.isNull()) {
            return true;
        }
        Iterator<Map.Entry<String, JsonNode>> it = n.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            if (e.getValue().isNull()) {
                continue;
            }
            Field f = find(fields, e.getKey());
            if (f == null || !fitsField(e.getValue(), f)) {
                return false;
            }
        }
        return true;
    }

    private static boolean fitsField(JsonNode n, Field f) {
        if (n == null || n.isNull()) {
            return true;
        }
        if (f.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Struct) {
            return n.isObject() && fitsFields(n, f.getChildren());
        }
        if (f.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.List) {
            if (!n.isArray()) {
                return false;
            }
            for (JsonNode el : n) {
                if (!fitsField(el, f.getChildren().get(0))) {
                    return false;
                }
            }
        }
        return true;
    }

    private static Field find(List<Field> fields, String name) {
        for (Field f : fields) {
            if (f.getName().equals(name)) {
                return f;
            }
        }
        return null;
    }

    /** Integers are checked, not coerced: a text value would otherwise become 0 without an error. */
    private static JsonNode integral(FieldVector v, JsonNode n) {
        if (!n.isIntegralNumber()) {
            throw new DataException("column " + v.getName() + " (" + v.getField().getType() + ") needs an integer, got " + n);
        }
        return n;
    }

    private static void write(FieldVector v, int r, JsonNode n) {
        if (n == null || n.isNull()) {
            v.setNull(r);
            return;
        }
        if (v instanceof StructVector) {
            StructVector sv = (StructVector) v;
            if (!n.isObject()) {
                throw new DataException("Arrow struct column " + v.getName() + " needs a JSON object");
            }
            sv.setIndexDefined(r);
            for (FieldVector c : sv.getChildrenFromFields()) {
                write(c, r, n.get(c.getName()));
            }
        } else if (v instanceof MapVector) {
            // MapVector extends ListVector, so it is checked first. Keys and values are STRING (see ArrowTypes.eligible).
            if (!n.isObject()) {
                throw new DataException("Arrow map column " + v.getName() + " needs a JSON object");
            }
            MapVector mv = (MapVector) v;
            StructVector entries = (StructVector) mv.getDataVector();
            int start = mv.startNewValue(r);
            int k = 0;
            java.util.Iterator<Map.Entry<String, JsonNode>> it = n.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                int idx = start + k++;
                entries.setIndexDefined(idx);
                entries.getChild("key", VarCharVector.class).setSafe(idx, e.getKey().getBytes(StandardCharsets.UTF_8));
                JsonNode val = e.getValue();
                VarCharVector vc = entries.getChild("value", VarCharVector.class);
                if (val == null || val.isNull()) {
                    vc.setNull(idx);
                } else if (val.isTextual()) {
                    vc.setSafe(idx, val.asText().getBytes(StandardCharsets.UTF_8));
                } else {
                    throw new DataException("map value in column " + v.getName() + " needs a string, got " + val);
                }
            }
            mv.endValue(r, k);
        } else if (v instanceof ListVector) {
            ListVector lv = (ListVector) v;
            if (!n.isArray()) {
                throw new DataException("Arrow list column " + v.getName() + " needs a JSON array");
            }
            int start = lv.startNewValue(r);
            FieldVector child = lv.getDataVector();
            for (int k = 0; k < n.size(); k++) {
                write(child, start + k, n.get(k));
            }
            lv.endValue(r, n.size());
        } else if (v instanceof BigIntVector) {
            ((BigIntVector) v).setSafe(r, integral(v, n).asLong());
        } else if (v instanceof IntVector) {
            JsonNode i = integral(v, n);
            if (!i.canConvertToInt()) {
                throw new DataException("value " + i + " does not fit column " + v.getName() + " (INT)");
            }
            ((IntVector) v).setSafe(r, i.asInt());
        } else if (v instanceof Float8Vector) {
            if (!n.isNumber()) {
                throw new DataException("column " + v.getName() + " (DOUBLE) needs a number, got " + n);
            }
            ((Float8Vector) v).setSafe(r, n.asDouble());
        } else if (v instanceof VarCharVector) {
            if (!n.isTextual()) {
                throw new DataException("column " + v.getName() + " (STRING) needs a string, got " + n);
            }
            ((VarCharVector) v).setSafe(r, n.asText().getBytes(StandardCharsets.UTF_8));
        } else if (v instanceof BitVector) {
            if (!n.isBoolean()) {
                throw new DataException("column " + v.getName() + " (BOOLEAN) needs true or false, got " + n);
            }
            ((BitVector) v).setSafe(r, n.asBoolean() ? 1 : 0);
        } else if (v instanceof TimeStampVector) {
            ((TimeStampVector) v).setSafe(r, integral(v, n).asLong());
        } else if (v instanceof TinyIntVector) {
            JsonNode i = integral(v, n);
            if (!i.canConvertToInt() || i.asInt() < Byte.MIN_VALUE || i.asInt() > Byte.MAX_VALUE) {
                throw new DataException("value " + i + " does not fit column " + v.getName() + " (TINYINT)");
            }
            ((TinyIntVector) v).setSafe(r, (byte) i.asInt());
        } else if (v instanceof SmallIntVector) {
            JsonNode i = integral(v, n);
            if (!i.canConvertToInt() || i.asInt() < Short.MIN_VALUE || i.asInt() > Short.MAX_VALUE) {
                throw new DataException("value " + i + " does not fit column " + v.getName() + " (SMALLINT)");
            }
            ((SmallIntVector) v).setSafe(r, (short) i.asInt());
        } else if (v instanceof Float4Vector) {
            if (!n.isNumber()) {
                throw new DataException("column " + v.getName() + " (FLOAT) needs a number, got " + n);
            }
            ((Float4Vector) v).setSafe(r, (float) n.asDouble());
        } else if (v instanceof DecimalVector) {
            // ValueJson sends DECIMAL as its plain text; the vector's scale is the column's scale.
            if (!n.isTextual()) {
                throw new DataException("column " + v.getName() + " (DECIMAL) needs its plain text, got " + n);
            }
            DecimalVector dv = (DecimalVector) v;
            BigDecimal bd = new BigDecimal(n.asText()).setScale(dv.getScale(), RoundingMode.UNNECESSARY);
            dv.setSafe(r, bd);
        } else if (v instanceof DateDayVector) {
            JsonNode i = integral(v, n);
            if (!i.canConvertToInt()) {
                throw new DataException("value " + i + " does not fit column " + v.getName() + " (DATE)");
            }
            ((DateDayVector) v).setSafe(r, i.asInt());
        } else if (v instanceof VarBinaryVector) {
            // ValueJson sends BINARY as base64 text.
            if (!n.isTextual()) {
                throw new DataException("column " + v.getName() + " (BINARY) needs base64 text, got " + n);
            }
            ((VarBinaryVector) v).setSafe(r, Base64.getDecoder().decode(n.asText()));
        } else {
            throw new DataException("no Arrow writer for column " + v.getName() + " (" + v.getClass().getSimpleName() + ")");
        }
    }
}
