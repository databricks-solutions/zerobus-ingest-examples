package com.databricks.zerobus.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;

/** Unit: Connect to Arrow type mapping and eligibility, and the batch writer (ArrowRows). */
class ArrowTypesTest {

    private static final org.apache.kafka.connect.data.Schema STR = org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
    private static final org.apache.kafka.connect.data.Schema I64 = org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA;

    @Test
    void probedTypesAreEligibleAndMapToTheirArrowTypes() {
        assertTrue(ArrowTypes.eligible(I64));
        assertTrue(ArrowTypes.eligible(org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA));
        assertTrue(ArrowTypes.eligible(org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA));
        assertTrue(ArrowTypes.eligible(STR));
        assertTrue(ArrowTypes.eligible(org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA));
        assertTrue(ArrowTypes.eligible(Timestamp.builder().optional().build()));
        assertEquals(ArrowTypes.INT64, ArrowTypes.field("x", I64).getType());
        assertEquals(ArrowTypes.INT32, ArrowTypes.field("x", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA).getType());
        assertEquals(ArrowTypes.FLOAT64, ArrowTypes.field("x", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA).getType());
        assertEquals(ArrowTypes.UTF8, ArrowTypes.field("x", STR).getType());
        assertEquals(ArrowTypes.BOOL, ArrowTypes.field("x", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA).getType());
        assertEquals(ArrowTypes.TIMESTAMP_TYPE, ArrowTypes.field("x", Timestamp.builder().optional().build()).getType());
    }

    @Test
    void nestedStructsAndArraysAreEligibleWhenTheirLeavesAre() {
        org.apache.kafka.connect.data.Schema struct = SchemaBuilder.struct().optional().field("a", I64).field("s", STR).build();
        assertTrue(ArrowTypes.eligible(SchemaBuilder.array(struct).optional().build()));
        Field f = ArrowTypes.field("c", struct);
        assertTrue(f.getType() instanceof ArrowType.Struct);
        assertEquals(2, f.getChildren().size());
    }

    @Test
    void typesWithoutAProbeAreNotEligible() {
        assertFalse(ArrowTypes.eligible(SchemaBuilder.map(STR, I64).optional().build()), "MAP with a non-STRING value is not probed");
        assertFalse(ArrowTypes.eligible(SchemaBuilder.struct().optional().field("m", SchemaBuilder.map(STR, I64).optional().build()).build()),
                "a struct with such a MAP field is not eligible");
        assertFalse(ArrowTypes.eligible(org.apache.kafka.connect.data.Time.builder().optional().build()),
                "Time (INT32 underneath) is not INT");
    }

    @Test
    void typesProbedInIteration4AreEligibleAndMapToTheirArrowTypes() {
        assertTrue(ArrowTypes.eligible(org.apache.kafka.connect.data.Schema.OPTIONAL_INT8_SCHEMA), "TINYINT");
        assertTrue(ArrowTypes.eligible(org.apache.kafka.connect.data.Schema.OPTIONAL_INT16_SCHEMA), "SMALLINT");
        assertTrue(ArrowTypes.eligible(org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA), "FLOAT");
        assertTrue(ArrowTypes.eligible(org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA), "BINARY");
        assertTrue(ArrowTypes.eligible(org.apache.kafka.connect.data.Date.builder().optional().build()), "DATE");
        assertTrue(ArrowTypes.eligible(org.apache.kafka.connect.data.Decimal.builder(2).optional().build()), "DECIMAL");
        assertTrue(ArrowTypes.eligible(SchemaBuilder.map(STR, STR).optional().build()), "MAP<STRING,STRING>");
        assertEquals(new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(38, 2, 128),
                ArrowTypes.field("d", org.apache.kafka.connect.data.Decimal.builder(2).optional().build()).getType(),
                "DECIMAL(38, scale) as Delta's DDL");
    }

    @Test
    void writerFillsTheIteration4TypesAndRejectsOutOfRangeValues() {
        org.apache.kafka.connect.data.Schema value = SchemaBuilder.struct()
                .field("i8", org.apache.kafka.connect.data.Schema.OPTIONAL_INT8_SCHEMA)
                .field("i16", org.apache.kafka.connect.data.Schema.OPTIONAL_INT16_SCHEMA)
                .field("f", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA)
                .field("dec", org.apache.kafka.connect.data.Decimal.builder(2).optional().build())
                .field("dt", org.apache.kafka.connect.data.Date.builder().optional().build())
                .field("bin", org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA)
                .field("m", SchemaBuilder.map(STR, STR).optional().build()).build();
        Schema arrow = ArrowTypes.schemaFor(value, null);
        ObjectNode r1 = ValueJson.MAPPER.createObjectNode();
        r1.put("i8", 7).put("i16", 300).put("f", 1.5).put("dec", "12.34").put("dt", 19000)
                .put("bin", "aGk=");  // base64 of "hi"
        r1.putObject("m").put("a", "b");
        try (BufferAllocator alloc = new RootAllocator();
             VectorSchemaRoot root = ArrowRows.build(alloc, arrow, Collections.<JsonNode>singletonList(r1))) {
            assertEquals(7, ((org.apache.arrow.vector.TinyIntVector) root.getVector("i8")).get(0));
            assertEquals(300, ((org.apache.arrow.vector.SmallIntVector) root.getVector("i16")).get(0));
            assertEquals(1.5f, ((org.apache.arrow.vector.Float4Vector) root.getVector("f")).get(0));
            assertEquals(new java.math.BigDecimal("12.34"), ((org.apache.arrow.vector.DecimalVector) root.getVector("dec")).getObject(0));
            assertEquals(19000, ((org.apache.arrow.vector.DateDayVector) root.getVector("dt")).get(0));
            assertEquals("hi", new String(((org.apache.arrow.vector.VarBinaryVector) root.getVector("bin")).get(0),
                    java.nio.charset.StandardCharsets.UTF_8));
            org.apache.arrow.vector.complex.MapVector m = (org.apache.arrow.vector.complex.MapVector) root.getVector("m");
            org.apache.arrow.vector.complex.StructVector e = (org.apache.arrow.vector.complex.StructVector) m.getDataVector();
            assertEquals("a", new String(e.getChild("key", org.apache.arrow.vector.VarCharVector.class).get(0), java.nio.charset.StandardCharsets.UTF_8));
            assertEquals("b", new String(e.getChild("value", org.apache.arrow.vector.VarCharVector.class).get(0), java.nio.charset.StandardCharsets.UTF_8));
        }
        ObjectNode big = ValueJson.MAPPER.createObjectNode();
        big.put("i8", 200);  // TINYINT is -128..127
        assertThrows(org.apache.kafka.connect.errors.DataException.class, () -> {
            try (BufferAllocator alloc = new RootAllocator();
                 VectorSchemaRoot root = ArrowRows.build(alloc, arrow, Collections.<JsonNode>singletonList(big))) {
                assertEquals(0, root.getRowCount());
            }
        });
    }

    @Test
    void metadataStructIsAppendedWithTheConfiguredFields() {
        org.apache.kafka.connect.data.Schema value = SchemaBuilder.struct().field("id", I64).build();
        Schema s = ArrowTypes.schemaFor(value, Arrays.asList("cluster", "partition", "timestamp"));
        assertEquals(Arrays.asList("id", "_kafka"), names(s.getFields()));
        Field kafka = s.getFields().get(1);
        assertEquals(Arrays.asList("cluster", "partition", "timestamp"), names(kafka.getChildren()));
        assertEquals(ArrowTypes.INT32, kafka.getChildren().get(1).getType());
    }

    @Test
    void writerFillsEveryProbedTypeAndWritesNullsForMissingFields() {
        org.apache.kafka.connect.data.Schema struct = SchemaBuilder.struct().optional().field("a", I64).field("s", STR).build();
        org.apache.kafka.connect.data.Schema value = SchemaBuilder.struct()
                .field("id", I64).field("i", org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA)
                .field("d", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA).field("s", STR)
                .field("b", org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field("ts", Timestamp.builder().optional().build())
                .field("arr", SchemaBuilder.array(STR).optional().build())
                .field("st", struct).build();
        Schema arrow = ArrowTypes.schemaFor(value, null);
        ObjectNode r1 = ValueJson.MAPPER.createObjectNode();
        r1.put("id", 7L).put("i", 3).put("d", 2.5).put("s", "hi").put("b", true).put("ts", 1700000000000000L);
        r1.putArray("arr").add("x").add("y");
        r1.putObject("st").put("a", 9L).put("s", "in");
        ObjectNode r2 = ValueJson.MAPPER.createObjectNode();   // missing fields and a null struct
        r2.put("id", 8L).putNull("st");
        try (BufferAllocator alloc = new RootAllocator();
             VectorSchemaRoot root = ArrowRows.build(alloc, arrow, Arrays.<JsonNode>asList(r1, r2))) {
            assertEquals(2, root.getRowCount());
            assertEquals(7L, ((BigIntVector) root.getVector("id")).get(0));
            assertEquals(8L, ((BigIntVector) root.getVector("id")).get(1));
            assertEquals(3, ((IntVector) root.getVector("i")).get(0));
            assertEquals(2.5, ((Float8Vector) root.getVector("d")).get(0));
            assertEquals("hi", new String(((VarCharVector) root.getVector("s")).get(0), java.nio.charset.StandardCharsets.UTF_8));
            assertEquals(1, ((BitVector) root.getVector("b")).get(0));
            assertEquals(1700000000000000L, ((TimeStampVector) root.getVector("ts")).get(0));
            ListVector arr = (ListVector) root.getVector("arr");
            assertEquals(2, arr.getElementEndIndex(0) - arr.getElementStartIndex(0));
            assertTrue(root.getVector("i").isNull(1), "missing field is null");
            assertTrue(root.getVector("arr").isNull(1), "missing list is null");
            StructVector st = (StructVector) root.getVector("st");
            assertEquals(9L, ((BigIntVector) st.getChild("a")).get(0));
            assertTrue(st.isNull(1), "explicit null struct is null");
        }
    }

    @Test
    void writerRejectsAValueOfTheWrongJsonTypeInsteadOfCoercingIt() {
        org.apache.kafka.connect.data.Schema value = SchemaBuilder.struct().field("amount",
                org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA).build();
        Schema arrow = ArrowTypes.schemaFor(value, null);
        ObjectNode bad = ValueJson.MAPPER.createObjectNode().put("amount", "abc");
        try (BufferAllocator alloc = new RootAllocator()) {
            assertThrows(org.apache.kafka.connect.errors.DataException.class,
                    () -> ArrowRows.build(alloc, arrow, Collections.singletonList(bad)).close());
        }
    }

    @Test
    void fitsAcceptsSubsetsAndRejectsFieldsTheSchemaLacks() {
        org.apache.kafka.connect.data.Schema wide = SchemaBuilder.struct().field("id", I64).field("kind", STR)
                .field("score", org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA).build();
        org.apache.kafka.connect.data.Schema narrow = SchemaBuilder.struct().field("id", I64).field("kind", STR).build();
        Schema pinned = ArrowTypes.schemaFor(wide, null);
        ObjectNode withoutScore = ValueJson.MAPPER.createObjectNode().put("id", 1L).put("kind", "k");
        ObjectNode withScore = ValueJson.MAPPER.createObjectNode().put("id", 2L).put("kind", "k").put("score", 1.0);
        ObjectNode extra = ValueJson.MAPPER.createObjectNode().put("id", 3L).put("other", "x");
        assertTrue(ArrowRows.fits(withoutScore, pinned), "a row without a field is sent as null");
        assertTrue(ArrowRows.fits(withScore, pinned));
        assertFalse(ArrowRows.fits(extra, pinned), "a row with a field the schema lacks needs another stream");
        assertTrue(ArrowRows.fits(withoutScore, ArrowTypes.schemaFor(narrow, null)));
    }

    private static List<String> names(List<Field> fields) {
        java.util.List<String> out = new java.util.ArrayList<>();
        for (Field f : fields) {
            out.add(f.getName());
        }
        return out;
    }
}
