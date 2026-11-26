package com.example.zerobus.mqtt;

import org.eclipse.paho.mqttv5.common.MqttException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

/**
 * Publishes simulated sensor readings to a Zerobus Ingest table over MQTT v5.
 *
 * <pre>
 * java -jar target/sensor-simulator.jar [config.properties] [--bad-records]
 * </pre>
 *
 * <p>Each record is sent at QoS 1 and counts as stored only after its PUBACK. A record that isn't
 * acknowledged is retried on a new connection with a new token. A retry can create a duplicate.
 * With {@code --bad-records}, the simulator sends the payloads in {@link BadRecords} after the normal
 * records, on the same connection, then one valid record, and prints each result. The table needs the
 * rescue column (see the README).
 */
public final class SensorSimulator {

    private static final int MAX_ATTEMPTS = 3;

    private SensorSimulator() {
    }

    public static void main(String[] args) throws Exception {
        boolean badRecords = Arrays.asList(args).contains("--bad-records");
        Path configFile = Arrays.stream(args)
                .filter(arg -> !arg.startsWith("--"))
                .findFirst()
                .map(Path::of)
                .orElse(null);

        Settings settings = Settings.load(configFile, System::getenv);
        System.out.println("Loaded " + settings);

        TokenClient tokens = new TokenClient(settings);
        Random random = new Random();
        try (ZerobusMqttPublisher publisher = new ZerobusMqttPublisher(settings, tokens)) {
            publisher.connect();

            for (int i = 1; i <= settings.records(); i++) {
                if (settings.reconnectEvery() > 0 && i > 1 && (i - 1) % settings.reconnectEvery() == 0) {
                    System.out.println("Reconnecting before record " + i + " with a new token");
                    publisher.reconnect();
                }
                String json = SensorReading.random(settings.deviceName(), random).toJson();
                publishWithRetry(publisher, json, i);
                Thread.sleep(settings.intervalMs());
            }

            if (badRecords) {
                sendBadRecords(publisher, settings, random);
            }
        }
        System.out.println("Done");
    }

    /**
     * Sends each bad payload on the open connection, then one valid record. The valid record shows
     * whether the connection survived the bad ones.
     */
    private static void sendBadRecords(ZerobusMqttPublisher publisher, Settings settings, Random random)
            throws MqttException {
        for (Map.Entry<String, String> bad : BadRecords.cases().entrySet()) {
            try {
                publisher.publish(bad.getValue());
                System.out.println("Bad record (" + bad.getKey() + "): PUBACK reason code 0 (stored), connected="
                        + publisher.isConnected());
            } catch (MqttException e) {
                System.out.println("Bad record (" + bad.getKey() + "): not acknowledged, reason code "
                        + e.getReasonCode() + ", connected=" + publisher.isConnected());
            }
        }
        String valid = SensorReading.random(settings.deviceName(), random).toJson();
        try {
            publisher.publish(valid);
            System.out.println("Valid record after bad records: PUBACK reason code 0, connected="
                    + publisher.isConnected());
        } catch (MqttException e) {
            System.out.println("Valid record after bad records: not acknowledged, reason code "
                    + e.getReasonCode() + ", connected=" + publisher.isConnected());
        }
    }

    private static void publishWithRetry(ZerobusMqttPublisher publisher, String json, int record)
            throws IOException, InterruptedException, MqttException {
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                publisher.publish(json);
                System.out.println("Record " + record + " acknowledged (PUBACK)");
                return;
            } catch (MqttException e) {
                System.out.println("Record " + record + " not acknowledged on attempt " + attempt
                        + " (reason code " + e.getReasonCode() + ")");
                if (attempt == MAX_ATTEMPTS) {
                    throw e;
                }
                System.out.println("Reconnecting with a new token; this record may be a duplicate if it was stored");
                publisher.reconnect();
            }
        }
    }
}
