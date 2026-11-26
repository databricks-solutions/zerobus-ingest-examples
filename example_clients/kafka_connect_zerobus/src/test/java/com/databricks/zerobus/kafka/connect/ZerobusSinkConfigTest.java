package com.databricks.zerobus.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

/** Unit: configuration validation for the routing families, DDL options and naming rules. */
class ZerobusSinkConfigTest {

    @Test
    void singleTableIsValid() {
        ZerobusSinkConfig c = new ZerobusSinkConfig(TestConfig.withTable("main.s.t"));
        assertEquals("main.s.t", c.defaultTable());
        assertTrue(c.tableServiceSharded());
    }

    @Test
    void noRoutingFamilyIsRejected() {
        Map<String, String> m = TestConfig.base();
        assertThrows(ConfigException.class, () -> new ZerobusSinkConfig(m));
    }

    @Test
    void twoRoutingFamiliesAreRejected() {
        Map<String, String> m = TestConfig.withTable("main.s.t");
        m.put("zerobus.tables", "main.s.a,main.s.b");
        assertThrows(ConfigException.class, () -> new ZerobusSinkConfig(m));
    }

    @Test
    void badTableNameIsRejected() {
        assertThrows(ConfigException.class, () -> new ZerobusSinkConfig(TestConfig.withTable("main.s.bad-name")));
        assertThrows(ConfigException.class, () -> new ZerobusSinkConfig(TestConfig.withTable("t")));
    }

    @Test
    void topicMapRulesParseInOrder() {
        Map<String, String> m = TestConfig.base();
        m.put("zerobus.topic2table.map", "events_(.*)=>main.s.ev_$1; orders=>main.s.orders");
        ZerobusSinkConfig c = new ZerobusSinkConfig(m);
        assertEquals(2, c.topicToTableRules().size());
        assertEquals("events_(.*)", c.topicToTableRules().get(0).regex);
    }

    @Test
    void topicMapEntryWithoutArrowIsRejected() {
        Map<String, String> m = TestConfig.base();
        m.put("zerobus.topic2table.map", "orders main.s.orders");
        assertThrows(ConfigException.class, () -> new ZerobusSinkConfig(m));
    }

    @Test
    void staticFanOutNeedsRegexKeysForListedTablesOnly() {
        Map<String, String> m = TestConfig.base();
        m.put("zerobus.tables", "main.s.a,main.s.b");
        m.put("zerobus.tables.route.field", "kind");
        m.put("zerobus.tables.main.s.a.route.regex", "alpha");
        ZerobusSinkConfig c = new ZerobusSinkConfig(m);
        assertEquals(1, c.tableRouteRegexes().size());
        m.put("zerobus.tables.main.s.zzz.route.regex", "x");
        assertThrows(ConfigException.class, () -> new ZerobusSinkConfig(m));
    }

    @Test
    void dynamicRoutingNeedsFieldAndNamespace() {
        Map<String, String> m = TestConfig.base();
        m.put("zerobus.tables.dynamic.enabled", "true");
        assertThrows(ConfigException.class, () -> new ZerobusSinkConfig(m));
        m.put("zerobus.tables.route.field", "dest");
        assertThrows(ConfigException.class, () -> new ZerobusSinkConfig(m));
        m.put("zerobus.tables.dynamic.namespace", "main.s");
        assertTrue(new ZerobusSinkConfig(m).dynamic());
    }

    @Test
    void autoCreateNeedsTheTableService() {
        Map<String, String> m = TestConfig.withTable("main.s.t");
        m.put("zerobus.table.auto.create", "true");
        m.put("zerobus.table.service.mode", "off");
        assertThrows(ConfigException.class, () -> new ZerobusSinkConfig(m));
        m.put("zerobus.table.service.mode", "sharded");
        assertTrue(new ZerobusSinkConfig(m).autoCreate());
    }

    @Test
    void evolutionNeedsTypedModeAndWarehouse() {
        Map<String, String> m = TestConfig.withTable("main.s.t");
        m.put("zerobus.schema.evolution.enabled", "true");
        m.put("zerobus.mode", "envelope");
        assertThrows(ConfigException.class, () -> new ZerobusSinkConfig(m));
        m.put("zerobus.mode", "typed");
        m.remove("zerobus.table.service.warehouse.id");
        assertThrows(ConfigException.class, () -> new ZerobusSinkConfig(m));
    }

    @Test
    void deadLetterTableNameIsChecked() {
        Map<String, String> m = TestConfig.withTable("main.s.t");
        m.put("zerobus.dlq.table", "main.s.dlq table");
        assertThrows(ConfigException.class, () -> new ZerobusSinkConfig(m));
        m.put("zerobus.dlq.table", "main.s.kc2_dlq");
        assertEquals("main.s.kc2_dlq", new ZerobusSinkConfig(m).dlqTable());
        assertFalse(new ZerobusSinkConfig(TestConfig.withTable("main.s.t")).dlqTable() != null);
    }

    @Test
    void identifiersFollowZerobusRules() {
        assertTrue(ZerobusSinkConfig.validIdentifier("abc_123"));
        assertFalse(ZerobusSinkConfig.validIdentifier("a-b"));
        assertFalse(ZerobusSinkConfig.validIdentifier("a.b"));
        assertFalse(ZerobusSinkConfig.validIdentifier("é"));
    }
}
