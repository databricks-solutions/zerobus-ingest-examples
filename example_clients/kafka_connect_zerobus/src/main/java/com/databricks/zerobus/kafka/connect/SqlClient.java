package com.databricks.zerobus.kafka.connect;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Minimal Databricks SQL client for the table service: OAuth client credentials, the SQL Statement
 * Execution API, and the DDL the table service needs. Tokens are never logged.
 */
public final class SqlClient implements TableCatalog {

    private final String workspaceUrl;
    private final String warehouseId;
    private final String clientId;
    private final String clientSecret;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
    private String token;
    private Instant tokenExpiry = Instant.EPOCH;

    public SqlClient(String workspaceUrl, String warehouseId, String clientId, String clientSecret) {
        this.workspaceUrl = stripSlash(workspaceUrl);
        this.warehouseId = warehouseId;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    private static String stripSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }

    private synchronized String token() throws IOException {
        if (token != null && Instant.now().isBefore(tokenExpiry)) {
            return token;
        }
        String basic = Base64.getEncoder().encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
        HttpRequest req = HttpRequest.newBuilder(URI.create(workspaceUrl + "/oidc/v1/token"))
                .header("Authorization", "Basic " + basic)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString("grant_type=client_credentials&scope=all-apis"))
                .timeout(Duration.ofSeconds(60)).build();
        JsonNode j = send(req, "token");
        token = j.path("access_token").asText();
        long ttl = j.path("expires_in").asLong(3600);
        tokenExpiry = Instant.now().plusSeconds(Math.max(60, ttl - 120));
        return token;
    }

    private JsonNode send(HttpRequest req, String what) throws IOException {
        try {
            HttpResponse<String> r = http.send(req, HttpResponse.BodyHandlers.ofString());
            JsonNode j = ValueJson.MAPPER.readTree(r.body().isEmpty() ? "{}" : r.body());
            if (r.statusCode() / 100 != 2) {
                throw new IOException(what + " failed: HTTP " + r.statusCode() + " "
                        + j.path("message").asText(j.path("error_description").asText(j.path("error").asText(""))));
            }
            return j;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(what + " interrupted", e);
        }
    }

    /** Runs one statement and waits for it. Returns the result JSON (status and result.data_array). */
    public JsonNode execute(String statement) throws IOException {
        String body = ValueJson.MAPPER.createObjectNode()
                .put("warehouse_id", warehouseId)
                .put("statement", statement)
                .put("wait_timeout", "30s")
                .put("on_wait_timeout", "CONTINUE")
                .put("disposition", "INLINE")
                .put("format", "JSON_ARRAY").toString();
        HttpRequest req = HttpRequest.newBuilder(URI.create(workspaceUrl + "/api/2.0/sql/statements"))
                .header("Authorization", "Bearer " + token())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofSeconds(60)).build();
        JsonNode j = send(req, "statement");
        long deadline = System.currentTimeMillis() + 300_000L;
        while (("PENDING".equals(state(j)) || "RUNNING".equals(state(j))) && System.currentTimeMillis() < deadline) {
            sleep(2000);
            HttpRequest poll = HttpRequest.newBuilder(URI.create(workspaceUrl + "/api/2.0/sql/statements/" + j.path("statement_id").asText()))
                    .header("Authorization", "Bearer " + token())
                    .timeout(Duration.ofSeconds(60)).GET().build();
            j = send(poll, "statement poll");
        }
        if (!"SUCCEEDED".equals(state(j))) {
            throw new IOException("statement " + state(j) + ": " + j.path("status").path("error").path("message").asText(""));
        }
        return j;
    }

    private static void sleep(long ms) throws IOException {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
        }
    }

    private static String state(JsonNode j) {
        return j.path("status").path("state").asText("");
    }

    @Override
    public Snapshot describe(String table) throws IOException {
        JsonNode j;
        try {
            j = execute("DESCRIBE TABLE " + table);
        } catch (IOException e) {
            String m = String.valueOf(e.getMessage());
            if (m.contains("TABLE_OR_VIEW_NOT_FOUND") || m.contains("cannot be found")) {
                return new Snapshot(false, -1L, new LinkedHashMap<>());
            }
            throw e;
        }
        Map<String, String> cols = new LinkedHashMap<>();
        for (JsonNode row : j.path("result").path("data_array")) {
            String name = row.get(0) == null ? "" : row.get(0).asText("");
            if (name.isEmpty() || name.startsWith("#")) {
                break;
            }
            cols.put(name, row.get(1).asText(""));
        }
        JsonNode h = execute("DESCRIBE HISTORY " + table + " LIMIT 1");
        long version = h.path("result").path("data_array").path(0).path(0).asLong(-1L);
        return new Snapshot(true, version, cols);
    }

    @Override
    public void createIfNotExists(String table, Map<String, String> columns) throws IOException {
        execute("CREATE TABLE IF NOT EXISTS " + table + " (" + ddl(columns) + ")");
    }

    @Override
    public void setRescueTag(String table, String column) throws IOException {
        execute("ALTER TABLE " + table + " ALTER COLUMN `" + column + "` SET TAGS ('zerobus-rescue')");
    }

    @Override
    public void addColumns(String table, Map<String, String> columns) throws IOException {
        execute("ALTER TABLE " + table + " ADD COLUMNS (" + ddl(columns) + ")");
    }

    private static String ddl(Map<String, String> columns) {
        StringBuilder b = new StringBuilder();
        for (Map.Entry<String, String> c : columns.entrySet()) {
            if (b.length() > 0) {
                b.append(", ");
            }
            b.append('`').append(c.getKey()).append('`').append(' ').append(c.getValue());
        }
        return b.toString();
    }
}
