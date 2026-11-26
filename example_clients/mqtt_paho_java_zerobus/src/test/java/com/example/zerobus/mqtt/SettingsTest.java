package com.example.zerobus.mqtt;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SettingsTest {

    private static Map<String, String> env(String... pairs) {
        Map<String, String> map = new java.util.HashMap<>(Map.of(
                "DATABRICKS_WORKSPACE_URL", "https://<workspace-url>/",
                "DATABRICKS_WORKSPACE_ID", "<workspace-id>",
                "DATABRICKS_CLIENT_ID", "<client-id>",
                "DATABRICKS_CLIENT_SECRET", "<client-secret>",
                "DATABRICKS_TABLE_NAME", "main.default.air_quality",
                "ZEROBUS_MQTT_HOST", "<workspace-id>.zerobus.<region>.example.com"));
        for (int i = 0; i < pairs.length; i += 2) {
            map.put(pairs[i], pairs[i + 1]);
        }
        return map;
    }

    @Test
    void loadsFromEnvironmentAndStripsTrailingSlash() throws Exception {
        Settings settings = Settings.load(null, env()::get);

        assertEquals("https://<workspace-url>", settings.workspaceUrl());
        assertEquals("main.default.air_quality", settings.tableName());
        assertEquals(20, settings.records());
    }

    @Test
    void secretIsMaskedInToString() throws Exception {
        Settings settings = Settings.load(null, env()::get);

        assertFalse(settings.toString().contains("<client-secret>"));
        assertEquals(true, settings.toString().contains("clientSecret=***"));
    }

    @Test
    void rejectsTwoPartTableName() {
        Map<String, String> env = env("DATABRICKS_TABLE_NAME", "default.air_quality");
        assertThrows(IllegalArgumentException.class, () -> Settings.load(null, env::get));
    }

    @Test
    void rejectsSchemeInMqttHost() {
        Map<String, String> env = env("ZEROBUS_MQTT_HOST", "https://<workspace-id>.zerobus.example.com");
        assertThrows(IllegalArgumentException.class, () -> Settings.load(null, env::get));
    }

    @Test
    void requiresClientSecretFromEnvironment() {
        Map<String, String> env = env();
        env.remove("DATABRICKS_CLIENT_SECRET");
        assertThrows(IllegalArgumentException.class, () -> Settings.load(null, env::get));
    }
}
