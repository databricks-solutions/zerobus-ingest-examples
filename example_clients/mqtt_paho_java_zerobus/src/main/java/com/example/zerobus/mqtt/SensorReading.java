package com.example.zerobus.mqtt;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Random;

/**
 * One reading from a simulated sensor. Each field maps to a column of the target table:
 * device_name STRING, temp INT, humidity INT.
 */
public record SensorReading(
        @JsonProperty("device_name") String deviceName,
        @JsonProperty("temp") int temp,
        @JsonProperty("humidity") int humidity) {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** A plausible reading: 18 to 26 degrees and 40 to 70 percent humidity. */
    static SensorReading random(String deviceName, Random random) {
        return new SensorReading(deviceName, 18 + random.nextInt(9), 40 + random.nextInt(31));
    }

    /** The record as one UTF-8 JSON object, the payload format Zerobus Ingest expects. */
    public String toJson() {
        try {
            return JSON.writeValueAsString(this);
        } catch (Exception e) {
            throw new IllegalStateException("Could not serialize sensor reading", e);
        }
    }
}
