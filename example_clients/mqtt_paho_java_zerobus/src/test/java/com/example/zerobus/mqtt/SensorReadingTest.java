package com.example.zerobus.mqtt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SensorReadingTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void recordHasExactlyTheTableColumnsWithIntegerValues() throws Exception {
        JsonNode node = JSON.readTree(new SensorReading("sensor-1", 22, 55).toJson());

        assertEquals(Set.of("device_name", "temp", "humidity"), fieldNames(node));
        assertEquals("sensor-1", node.get("device_name").asText());
        assertTrue(node.get("temp").isInt());
        assertTrue(node.get("humidity").isInt());
        assertEquals(22, node.get("temp").asInt());
        assertEquals(55, node.get("humidity").asInt());
    }

    @Test
    void randomReadingsStayInTheSimulatedRange() throws Exception {
        Random random = new Random(42);
        for (int i = 0; i < 200; i++) {
            SensorReading reading = SensorReading.random("sensor-1", random);
            assertTrue(reading.temp() >= 18 && reading.temp() <= 26, "temp " + reading.temp());
            assertTrue(reading.humidity() >= 40 && reading.humidity() <= 70, "humidity " + reading.humidity());
        }
    }

    private static Set<String> fieldNames(JsonNode node) {
        Set<String> names = new java.util.HashSet<>();
        Iterator<String> it = node.fieldNames();
        while (it.hasNext()) {
            names.add(it.next());
        }
        return names;
    }
}
