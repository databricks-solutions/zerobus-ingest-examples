package com.example.zerobus.mqtt;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Payloads that don't match the table schema, in the order the simulator sends them with
 * {@code --bad-records}. With the rescue column, the first two are stored (the bad fields go to
 * {@code rescue}) and get a successful PUBACK. The third isn't JSON, so the endpoint answers with
 * PUBACK reason code 0x99 and stores nothing. Without the rescue column, a type mismatch or an
 * extra field closes the connection with DISCONNECT 0x83.
 */
final class BadRecords {

    private BadRecords() {
    }

    /** Case name to payload. The map keeps insertion order. */
    static Map<String, String> cases() {
        Map<String, String> cases = new LinkedHashMap<>();
        cases.put("type mismatch", "{\"device_name\":\"sensor-bad\",\"temp\":\"hot\",\"humidity\":50}");
        cases.put("extra field", "{\"device_name\":\"sensor-bad\",\"temp\":21,\"humidity\":50,\"extra_field\":\"hello\"}");
        cases.put("not JSON", "not json ((");
        return cases;
    }
}
