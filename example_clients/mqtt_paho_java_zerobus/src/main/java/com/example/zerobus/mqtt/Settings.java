package com.example.zerobus.mqtt;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.function.Function;

/**
 * Settings for the simulator. Values come from a properties file and can be overridden by
 * environment variables. The service principal's client ID and secret come only from the
 * environment, so they are never written to disk.
 */
public record Settings(
        String workspaceUrl,
        String workspaceId,
        String clientId,
        String clientSecret,
        String tableName,
        String mqttHost,
        String deviceName,
        int records,
        long intervalMs,
        int reconnectEvery) {

    /**
     * Loads settings. {@code file} may be null. {@code env} is normally {@code System::getenv}.
     */
    public static Settings load(Path file, Function<String, String> env) throws IOException {
        Properties props = new Properties();
        if (file != null) {
            try (Reader reader = Files.newBufferedReader(file)) {
                props.load(reader);
            }
        }

        String workspaceUrl = stripTrailingSlash(pick(env, props, "DATABRICKS_WORKSPACE_URL", "workspace.url", true));
        String workspaceId = pick(env, props, "DATABRICKS_WORKSPACE_ID", "workspace.id", true);
        String clientId = required(env, "DATABRICKS_CLIENT_ID");
        String clientSecret = required(env, "DATABRICKS_CLIENT_SECRET");
        String tableName = pick(env, props, "DATABRICKS_TABLE_NAME", "table.name", true);
        String mqttHost = pick(env, props, "ZEROBUS_MQTT_HOST", "mqtt.host", true);
        String deviceName = props.getProperty("sensor.device", "sensor-1").trim();
        int records = intProperty(props, "sensor.records", 20);
        long intervalMs = longProperty(props, "sensor.interval.ms", 1000L);
        int reconnectEvery = intProperty(props, "sensor.reconnect.every", 0);

        validate(tableName, mqttHost, records, intervalMs, reconnectEvery);
        return new Settings(workspaceUrl, workspaceId, clientId, clientSecret, tableName, mqttHost,
                deviceName, records, intervalMs, reconnectEvery);
    }

    private static void validate(String tableName, String mqttHost, int records, long intervalMs, int reconnectEvery) {
        String[] parts = tableName.split("\\.", -1);
        if (parts.length != 3 || parts[0].isBlank() || parts[1].isBlank() || parts[2].isBlank()) {
            throw new IllegalArgumentException("Table name must be catalog.schema.table");
        }
        if (mqttHost.contains("/") || mqttHost.contains(":")) {
            throw new IllegalArgumentException("MQTT host must be a bare hostname, without https:// or a port");
        }
        if (records < 1) {
            throw new IllegalArgumentException("sensor.records must be at least 1");
        }
        if (intervalMs < 0 || reconnectEvery < 0) {
            throw new IllegalArgumentException("sensor.interval.ms and sensor.reconnect.every can't be negative");
        }
    }

    private static String pick(Function<String, String> env, Properties props, String envName, String key,
                               boolean required) {
        String value = env.apply(envName);
        if (value == null || value.isBlank()) {
            value = props.getProperty(key);
        }
        if (required && (value == null || value.isBlank())) {
            throw new IllegalArgumentException("Set " + envName + " or " + key);
        }
        return value == null ? null : value.trim();
    }

    private static String required(Function<String, String> env, String envName) {
        String value = env.apply(envName);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Set the " + envName + " environment variable");
        }
        return value.trim();
    }

    private static int intProperty(Properties props, String key, int fallback) {
        String value = props.getProperty(key);
        return value == null ? fallback : Integer.parseInt(value.trim());
    }

    private static long longProperty(Properties props, String key, long fallback) {
        String value = props.getProperty(key);
        return value == null ? fallback : Long.parseLong(value.trim());
    }

    private static String stripTrailingSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    /** Masks the secret so that logging a Settings object can't leak it. */
    @Override
    public String toString() {
        return "Settings[workspaceUrl=" + workspaceUrl + ", workspaceId=" + workspaceId
                + ", clientId=" + clientId + ", clientSecret=***, tableName=" + tableName
                + ", mqttHost=" + mqttHost + ", deviceName=" + deviceName + ", records=" + records
                + ", intervalMs=" + intervalMs + ", reconnectEvery=" + reconnectEvery + "]";
    }
}
