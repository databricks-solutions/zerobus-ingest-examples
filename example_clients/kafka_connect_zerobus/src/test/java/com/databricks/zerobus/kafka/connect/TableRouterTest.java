package com.databricks.zerobus.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/** Unit: routing by topic regex (with group replacement), static fan-out by route regex, and dynamic routing. */
class TableRouterTest {

    private static final Schema S = SchemaBuilder.struct().field("kind", Schema.STRING_SCHEMA)
            .field("dest", Schema.OPTIONAL_STRING_SCHEMA).build();

    private static SinkRecord rec(String topic, String kind, String dest) {
        Struct v = new Struct(S).put("kind", kind).put("dest", dest);
        return new SinkRecord(topic, 0, null, null, S, v, 0L);
    }

    @Test
    void topicRegexGroupReplacementAndFirstMatchWins() {
        Map<String, String> m = TestConfig.base();
        m.put("zerobus.topic2table.map", "kc2_ev_(.*)=>main.s.ev_$1; kc2_.*=>main.s.other; orders=>main.s.orders");
        TableRouter r = new TableRouter(new ZerobusSinkConfig(m));
        assertEquals("main.s.ev_clicks", r.topicTable("kc2_ev_clicks"));
        assertEquals("main.s.other", r.topicTable("kc2_zzz"));
        assertEquals("main.s.other", r.topicTable("kc2_orders"), "rule order: the first full match wins");
    }

    @Test
    void regexMustMatchTheWholeTopic() {
        Map<String, String> m = TestConfig.base();
        m.put("zerobus.topic2table.map", "orders=>main.s.orders");
        TableRouter r = new TableRouter(new ZerobusSinkConfig(m));
        assertThrows(TableRouter.RouteException.class, () -> r.topicTable("orders_v2"));
    }

    @Test
    void defaultTableCatchesUnmatchedTopics() {
        Map<String, String> m = TestConfig.base();
        m.put("zerobus.topic2table.map", "orders=>main.s.orders");
        m.put("zerobus.table", "main.s.all");
        TableRouter r = new TableRouter(new ZerobusSinkConfig(m));
        assertEquals("main.s.all", r.topicTable("anything"));
    }

    @Test
    void singleTableRoutesEveryTopic() {
        TableRouter r = new TableRouter(new ZerobusSinkConfig(TestConfig.withTable("main.s.one")));
        assertEquals(Collections.singletonList("main.s.one"), r.route(rec("a", "x", null)));
        assertEquals(Collections.singletonList("main.s.one"), r.route(rec("b", "x", null)));
    }

    @Test
    void staticFanOutSendsOneRecordToEveryMatchingTable() {
        Map<String, String> m = TestConfig.base();
        m.put("zerobus.tables", "main.s.a,main.s.b,main.s.c");
        m.put("zerobus.tables.route.field", "kind");
        m.put("zerobus.tables.main.s.a.route.regex", "alpha|beta");
        m.put("zerobus.tables.main.s.b.route.regex", "alpha");
        m.put("zerobus.tables.main.s.c.route.regex", "gamma");
        TableRouter r = new TableRouter(new ZerobusSinkConfig(m));
        assertEquals(java.util.Arrays.asList("main.s.a", "main.s.b"), r.route(rec("t", "alpha", null)));
        assertEquals(Collections.singletonList("main.s.a"), r.route(rec("t", "beta", null)));
        assertThrows(TableRouter.RouteException.class, () -> r.route(rec("t", "delta", null)));
    }

    @Test
    void staticFanOutWithoutRouteFieldGoesEverywhere() {
        Map<String, String> m = TestConfig.base();
        m.put("zerobus.tables", "main.s.a,main.s.b");
        TableRouter r = new TableRouter(new ZerobusSinkConfig(m));
        assertEquals(java.util.Arrays.asList("main.s.a", "main.s.b"), r.route(rec("t", "x", null)));
    }

    @Test
    void missingRouteFieldIsAnError() {
        Map<String, String> m = TestConfig.base();
        m.put("zerobus.tables", "main.s.a");
        m.put("zerobus.tables.route.field", "dest");
        m.put("zerobus.tables.main.s.a.route.regex", ".*");
        TableRouter r = new TableRouter(new ZerobusSinkConfig(m));
        assertThrows(TableRouter.RouteException.class, () -> r.route(rec("t", "x", null)));
    }

    @Test
    void dynamicRoutingUsesTheFieldAsTableNameInsideTheNamespace() {
        Map<String, String> m = TestConfig.base();
        m.put("zerobus.tables.dynamic.enabled", "true");
        m.put("zerobus.tables.dynamic.namespace", "main.s");
        m.put("zerobus.tables.route.field", "dest");
        TableRouter r = new TableRouter(new ZerobusSinkConfig(m));
        assertEquals(Collections.singletonList("main.s.kc2_dyn_a"), r.route(rec("t", "x", "kc2_dyn_a")));
        assertThrows(TableRouter.RouteException.class, () -> r.route(rec("t", "x", "bad-name")));
        assertThrows(TableRouter.RouteException.class, () -> r.route(rec("t", "x", "main.other.t")));
        assertThrows(TableRouter.RouteException.class, () -> r.route(rec("t", "x", null)));
    }

    @Test
    void schemalessValuesReadTheRouteFieldFromAMap() {
        Map<String, String> m = TestConfig.base();
        m.put("zerobus.tables.dynamic.enabled", "true");
        m.put("zerobus.tables.dynamic.namespace", "main.s");
        m.put("zerobus.tables.route.field", "dest");
        TableRouter r = new TableRouter(new ZerobusSinkConfig(m));
        Map<String, Object> value = new HashMap<>();
        value.put("dest", "kc2_from_map");
        SinkRecord s = new SinkRecord("t", 0, null, null, null, value, 0L);
        assertEquals(Collections.singletonList("main.s.kc2_from_map"), r.route(s));
    }
}
