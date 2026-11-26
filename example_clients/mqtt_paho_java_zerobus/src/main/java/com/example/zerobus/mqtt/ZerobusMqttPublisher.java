package com.example.zerobus.mqtt;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.client.MqttToken;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

/**
 * One MQTT v5 connection that publishes JSON records to one Zerobus Ingest table.
 *
 * <p>The table and the OAuth token travel in the CONNECT User Properties. The token is fetched
 * on every {@link #connect()}, so {@link #reconnect()} always sends a fresh one. Paho's automatic
 * reconnect is off, because the connection options (and the token) must change at reconnect.
 */
final class ZerobusMqttPublisher implements AutoCloseable {

    private static final int MQTT_PORT = 8883;
    private static final long PUBACK_TIMEOUT_MS = 30_000;
    private static final int PUBACK_ERROR_THRESHOLD = 0x80;

    private final Settings settings;
    private final TokenClient tokens;
    private final MqttClient client;

    ZerobusMqttPublisher(Settings settings, TokenClient tokens) throws MqttException {
        this.settings = settings;
        this.tokens = tokens;
        String clientId = "sensor-simulator-" + UUID.randomUUID();
        this.client = new MqttClient("ssl://" + settings.mqttHost() + ":" + MQTT_PORT, clientId,
                new MemoryPersistence());
        this.client.setCallback(new ConnectionEvents());
        this.client.setTimeToWait(PUBACK_TIMEOUT_MS);
    }

    /** Opens a connection. Fetches a new token and sends it in the CONNECT properties. */
    void connect() throws IOException, InterruptedException, MqttException {
        String token = tokens.fetchToken();

        List<UserProperty> userProperties = new ArrayList<>();
        userProperties.add(new UserProperty("Authorization", "Bearer " + token));
        userProperties.add(new UserProperty("x-databricks-zerobus-table-name", settings.tableName()));

        MqttConnectionOptions options = new MqttConnectionOptions();
        options.setCleanStart(true);
        options.setKeepAliveInterval(60);
        options.setConnectionTimeout(10);
        options.setAutomaticReconnect(false);
        options.setUserProperties(userProperties);

        client.connect(options);
        System.out.println("Connected to " + settings.tableName() + " (token " + fingerprint(token) + ")");
    }

    /**
     * Reconnects with a new token. The caller runs this, because the server ends a connection after
     * a bounded lifetime and the old token can't be reused for a new CONNECT.
     */
    void reconnect() throws IOException, InterruptedException, MqttException {
        if (client.isConnected()) {
            client.disconnect();
        }
        connect();
    }

    /**
     * Publishes one JSON record at QoS 1 and waits for its PUBACK. Returns normally only when the
     * record is durable. Throws MqttException when the PUBACK is unsuccessful or never arrives.
     */
    void publish(String json) throws MqttException {
        MqttMessage message = new MqttMessage(json.getBytes(StandardCharsets.UTF_8));
        message.setQos(1);
        MqttToken token = client.getTopic(settings.tableName()).publish(message);
        token.waitForCompletion(PUBACK_TIMEOUT_MS);
        int[] reasonCodes = token.getReasonCodes();
        if (reasonCodes != null && reasonCodes.length > 0 && reasonCodes[0] >= PUBACK_ERROR_THRESHOLD) {
            throw new MqttException(reasonCodes[0]);
        }
    }

    boolean isConnected() {
        return client.isConnected();
    }

    /** Short token fingerprint for logs. It identifies a token without revealing it. */
    static String fingerprint(String token) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.UTF_8));
            return "sha256:" + HexFormat.of().formatHex(digest, 0, 4);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() throws MqttException {
        if (client.isConnected()) {
            client.disconnect();
        }
        client.close();
    }

    /** Logs connection events. It never prints CONNECT properties or tokens. */
    private static final class ConnectionEvents implements MqttCallback {
        @Override
        public void disconnected(MqttDisconnectResponse disconnectResponse) {
            System.out.println("Disconnected (reason code " + disconnectResponse.getReturnCode() + ")");
        }

        @Override
        public void mqttErrorOccurred(MqttException exception) {
            System.out.println("MQTT error (reason code " + exception.getReasonCode() + ")");
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
            // The simulator doesn't subscribe, so nothing arrives.
        }

        @Override
        public void deliveryComplete(IMqttToken token) {
            // Delivery is tracked through the token returned by publish().
        }

        @Override
        public void connectComplete(boolean reconnect, String serverURI) {
            // Logged by connect() after the call returns.
        }

        @Override
        public void authPacketArrived(int reasonCode, MqttProperties properties) {
            // Enhanced authentication isn't supported by Zerobus Ingest.
        }
    }
}
