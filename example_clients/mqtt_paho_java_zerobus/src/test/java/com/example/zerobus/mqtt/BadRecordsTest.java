package com.example.zerobus.mqtt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BadRecordsTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void typeMismatchAndExtraFieldAreJsonButDoNotMatchTheSchema() throws Exception {
        Map<String, String> cases = BadRecords.cases();
        assertEquals(3, cases.size());

        JsonNode typeMismatch = JSON.readTree(cases.get("type mismatch"));
        assertTrue(typeMismatch.get("temp").isTextual(), "temp must be a string for a type mismatch");

        JsonNode extraField = JSON.readTree(cases.get("extra field"));
        assertTrue(extraField.get("temp").isInt());
        assertTrue(extraField.has("extra_field"), "the extra field must be present");
    }

    @Test
    void notJsonPayloadFailsToParse() {
        String payload = BadRecords.cases().get("not JSON");
        assertThrows(JsonProcessingException.class, () -> JSON.readTree(payload));
    }
}
