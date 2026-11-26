package com.example.zerobus.mqtt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TokenClientTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void authorizationDetailsScopeTheTokenToCatalogSchemaAndTable() throws Exception {
        JsonNode details = JSON.readTree(TokenClient.authorizationDetailsJson("main.default.air_quality"));

        assertEquals(3, details.size());
        assertEquals("main", details.get(0).get("object_full_path").asText());
        assertEquals("main.default", details.get(1).get("object_full_path").asText());
        assertEquals("main.default.air_quality", details.get(2).get("object_full_path").asText());
        assertEquals("TABLE", details.get(2).get("object_type").asText());
        assertEquals("SELECT", details.get(2).get("privileges").get(0).asText());
        assertEquals("MODIFY", details.get(2).get("privileges").get(1).asText());
    }

    @Test
    void formIsUrlEncoded() {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("scope", "all-apis");
        fields.put("resource", "api://databricks/workspaces/1/zerobusDirectWriteApi");
        assertEquals("scope=all-apis&resource=api%3A%2F%2Fdatabricks%2Fworkspaces%2F1%2FzerobusDirectWriteApi",
                TokenClient.form(fields));
    }
}
