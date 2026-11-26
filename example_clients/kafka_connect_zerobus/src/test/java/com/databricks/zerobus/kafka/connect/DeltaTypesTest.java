package com.databricks.zerobus.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

class DeltaTypesTest {

    @Test
    void mapsPrimitiveAndNestedSchemas() {
        assertEquals("BIGINT", DeltaTypes.ddl(SchemaBuilder.int64().build()));
        assertEquals("INT", DeltaTypes.ddl(SchemaBuilder.int32().build()));
        assertEquals("ARRAY<STRING>", DeltaTypes.ddl(SchemaBuilder.array(SchemaBuilder.string().build()).build()));
        assertEquals("STRUCT<city: STRING, zip: INT>", DeltaTypes.ddl(SchemaBuilder.struct()
                .field("city", SchemaBuilder.string().build())
                .field("zip", SchemaBuilder.int32().build()).build()));
        assertEquals("DECIMAL(38,2)", DeltaTypes.ddl(Decimal.builder(2).build()));
    }

    @Test
    void comparesPrimitivesOnly() {
        assertTrue(DeltaTypes.matches("INT", "int"));
        assertTrue(DeltaTypes.matches("DECIMAL(38,2)", "decimal(38,2)"));
        assertFalse(DeltaTypes.matches("STRING", "int"));
        // complex types are not compared client-side; the server decides
        assertTrue(DeltaTypes.matches("STRUCT<a: STRING>", "struct<a:string,b:int>"));
    }
}
