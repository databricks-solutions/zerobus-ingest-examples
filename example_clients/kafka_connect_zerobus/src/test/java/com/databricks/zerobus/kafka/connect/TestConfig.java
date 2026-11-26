package com.databricks.zerobus.kafka.connect;

import java.util.HashMap;
import java.util.Map;

/** Builds valid connector configurations for tests. Values are placeholders; nothing here reaches a network. */
final class TestConfig {

    private TestConfig() {}

    static Map<String, String> base() {
        Map<String, String> m = new HashMap<>();
        m.put("connector.class", "com.databricks.zerobus.kafka.connect.ZerobusSinkConnector");
        m.put("zerobus.endpoint", "https://0.zerobus.test.invalid");
        m.put("zerobus.workspace.url", "https://workspace.test.invalid");
        m.put("zerobus.client.id", "client-id-placeholder");
        m.put("zerobus.client.secret", "client-secret-placeholder");
        m.put("zerobus.metadata.enabled", "false");
        m.put("zerobus.kafka.bootstrap.servers", "127.0.0.1:9092");
        m.put("zerobus.table.service.warehouse.id", "warehouse-placeholder");
        m.put("zerobus.connector.name", "unit");
        m.put("zerobus.table.service.mode", "sharded");
        m.put("zerobus.arrow.open.threads", "0");  // unit tests open streams on the calling thread unless they say otherwise
        return m;
    }

    static Map<String, String> withTable(String table) {
        Map<String, String> m = base();
        m.put("zerobus.table", table);
        return m;
    }
}
