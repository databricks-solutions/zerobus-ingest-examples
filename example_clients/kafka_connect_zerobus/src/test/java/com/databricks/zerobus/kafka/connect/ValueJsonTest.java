package com.databricks.zerobus.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.util.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;

class ValueJsonTest {

    @Test
    void timestampIsEpochMicrosAndDecimalIsText() {
        JsonNode ts = ValueJson.value(Timestamp.SCHEMA, new Date(1_700_000_000_123L));
        assertEquals(1_700_000_000_123_000L, ts.asLong());
        JsonNode dec = ValueJson.value(Decimal.builder(2).build(), new BigDecimal("123.45"));
        assertEquals("123.45", dec.asText());
    }

    @Test
    void bytesAreBase64AndStringsStayStrings() {
        assertEquals("aGk=", ValueJson.value(SchemaBuilder.bytes().build(), "hi".getBytes()).asText());
        assertEquals("\"plain\"", ValueJson.asJsonText(SchemaBuilder.string().build(), "plain"));
    }
}
