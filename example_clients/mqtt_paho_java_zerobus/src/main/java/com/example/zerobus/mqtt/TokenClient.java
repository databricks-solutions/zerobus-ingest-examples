package com.example.zerobus.mqtt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Fetches a table-scoped OAuth token with the service principal client credentials flow.
 * Call {@link #fetchToken()} before every CONNECT. Tokens are short-lived, and a connection
 * keeps the token it was opened with.
 */
final class TokenClient {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Settings settings;
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    TokenClient(Settings settings) {
        this.settings = settings;
    }

    /** Returns a new access token. The token value must never be logged. */
    String fetchToken() throws IOException, InterruptedException {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("grant_type", "client_credentials");
        fields.put("scope", "all-apis");
        fields.put("resource", "api://databricks/workspaces/" + settings.workspaceId() + "/zerobusDirectWriteApi");
        fields.put("authorization_details", authorizationDetailsJson(settings.tableName()));

        String credentials = settings.clientId() + ":" + settings.clientSecret();
        HttpRequest request = HttpRequest.newBuilder(URI.create(settings.workspaceUrl() + "/oidc/v1/token"))
                .timeout(Duration.ofSeconds(30))
                .header("Authorization", "Basic " + Base64.getEncoder()
                        .encodeToString(credentials.getBytes(StandardCharsets.UTF_8)))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form(fields)))
                .build();

        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            // Print the status only. The body is not logged, so nothing sensitive reaches the output.
            throw new IOException("Token request failed with HTTP " + response.statusCode());
        }
        JsonNode accessToken = JSON.readTree(response.body()).get("access_token");
        if (accessToken == null || accessToken.asText().isBlank()) {
            throw new IOException("Token response has no access_token");
        }
        return accessToken.asText();
    }

    /**
     * The authorization_details for the catalog, schema and table. The table grant covers
     * SELECT and MODIFY, the two table privileges the Zerobus Ingest docs list.
     */
    static String authorizationDetailsJson(String tableName) {
        String[] parts = tableName.split("\\.");
        String catalog = parts[0];
        String schema = parts[1];

        List<Map<String, Object>> details = new ArrayList<>();
        details.add(privilege(List.of("USE CATALOG"), "CATALOG", catalog));
        details.add(privilege(List.of("USE SCHEMA"), "SCHEMA", catalog + "." + schema));
        details.add(privilege(List.of("SELECT", "MODIFY"), "TABLE", tableName));
        try {
            return JSON.writeValueAsString(details);
        } catch (Exception e) {
            throw new IllegalStateException("Could not build authorization_details", e);
        }
    }

    private static Map<String, Object> privilege(List<String> privileges, String objectType, String fullPath) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("type", "unity_catalog_privileges");
        entry.put("privileges", privileges);
        entry.put("object_type", objectType);
        entry.put("object_full_path", fullPath);
        return entry;
    }

    static String form(Map<String, String> fields) {
        return fields.entrySet().stream()
                .map(e -> URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8) + "="
                        + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));
    }
}
