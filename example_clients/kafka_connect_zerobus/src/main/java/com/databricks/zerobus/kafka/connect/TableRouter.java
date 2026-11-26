package com.databricks.zerobus.kafka.connect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Decides which tables a record goes to. Three families, chosen once by the configuration:
 * topic rules (topic2table.map, with zerobus.table as the default), static fan-out (zerobus.tables,
 * one record to every table whose route regex matches its route field), and dynamic (the route
 * field names the table inside one allowed namespace). Pure: no I/O, so it is unit tested directly.
 */
final class TableRouter {

    /** A record that no rule can place. The task reports it (DLQ or fail) instead of dropping it. */
    static final class RouteException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        RouteException(String message) {
            super(message);
        }
    }

    private final List<ZerobusSinkConfig.TopicRule> topicRules;
    private final String defaultTable;
    private final List<String> staticTables;
    private final Map<String, Pattern> routeRegexes;
    private final String routeField;
    private final boolean dynamic;
    private final String namespace;

    TableRouter(ZerobusSinkConfig cfg) {
        this.topicRules = cfg.topicToTableRules();
        this.defaultTable = cfg.defaultTable();
        this.staticTables = Collections.unmodifiableList(new ArrayList<>(cfg.staticTables()));
        this.routeRegexes = cfg.tableRouteRegexes();
        this.routeField = cfg.routeField();
        this.dynamic = cfg.dynamic();
        this.namespace = cfg.dynamicNamespace();
    }

    /** Tables known before any record arrives (used for the startup snapshot). */
    List<String> knownTables() {
        if (!staticTables.isEmpty()) {
            return staticTables;
        }
        return defaultTable == null ? Collections.emptyList() : Collections.singletonList(defaultTable);
    }

    /** Target tables for a post-SMT record. Throws RouteException when no rule places it. */
    List<String> route(SinkRecord r) {
        if (dynamic) {
            String value = routeValue(r.value());
            if (value == null) {
                throw new RouteException("route field '" + routeField + "' is missing");
            }
            if (!ZerobusSinkConfig.validIdentifier(value)) {
                throw new RouteException("route value '" + value + "' is not a valid table name (ASCII letters, digits, underscores)");
            }
            return Collections.singletonList(namespace + "." + value);
        }
        if (!staticTables.isEmpty()) {
            if (routeField == null) {
                return staticTables;
            }
            String value = routeValue(r.value());
            if (value == null) {
                throw new RouteException("route field '" + routeField + "' is missing");
            }
            List<String> out = new ArrayList<>();
            for (Map.Entry<String, Pattern> e : routeRegexes.entrySet()) {
                if (e.getValue().matcher(value).matches()) {
                    out.add(e.getKey());
                }
            }
            if (out.isEmpty()) {
                throw new RouteException("no route.regex matches " + routeField + "='" + value + "'");
            }
            return out;
        }
        return Collections.singletonList(topicTable(r.topic()));
    }

    /** First rule whose regex matches the whole topic; otherwise the default table. */
    String topicTable(String topic) {
        for (ZerobusSinkConfig.TopicRule rule : topicRules) {
            Matcher m = rule.pattern.matcher(topic);
            if (m.matches()) {
                String table = m.replaceFirst(rule.template);
                if (!ZerobusSinkConfig.validTable(table)) {
                    throw new RouteException("topic " + topic + " maps to '" + table + "', not catalog.schema.table");
                }
                return table;
            }
        }
        if (defaultTable != null) {
            return defaultTable;
        }
        throw new RouteException("no table for topic " + topic + ": add a zerobus.topic2table.map rule or zerobus.table");
    }

    /** The route field's value, read from a Struct or a Map (top-level fields only). Null when absent. */
    String routeValue(Object value) {
        if (routeField == null || value == null) {
            return null;
        }
        Object v;
        if (value instanceof Struct) {
            Struct s = (Struct) value;
            if (s.schema().field(routeField) == null) {
                return null;
            }
            v = s.get(routeField);
        } else if (value instanceof Map) {
            v = ((Map<?, ?>) value).get(routeField);
        } else {
            return null;
        }
        return v == null ? null : v.toString();
    }
}
