package com.databricks.zerobus.kafka.connect;

import com.databricks.zerobus.AckCallback;
import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusJsonStream;
import com.databricks.zerobus.ZerobusSdk;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Adapts the Zerobus Java SDK to JsonSink. The only class that touches SDK stream types. */
final class SdkSinkFactory implements JsonSink.Factory {

    private static final Logger log = LoggerFactory.getLogger(SdkSinkFactory.class);

    private final ZerobusSdk sdk;
    private final String clientId;
    private final String clientSecret;
    private final int maxInflight;

    SdkSinkFactory(ZerobusSdk sdk, String clientId, String clientSecret, int maxInflight) {
        this.sdk = sdk;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.maxInflight = maxInflight;
    }

    @Override
    public JsonSink open(String table, JsonSink.AckListener listener) {
        try {
            ZerobusJsonStream s = sdk.streamBuilder()
                    .table(table)
                    .oauth(clientId, clientSecret)
                    .maxInflightRecords(maxInflight)
                    .ackCallback(new AckCallback() {
                        @Override
                        public void onAck(long upTo) {
                            listener.onAck(upTo);
                        }

                        @Override
                        public void onError(long offset, String message) {
                            listener.onError(offset, message);
                        }
                    })
                    .json()
                    .build()
                    .join();
            return new Sdk(s);
        } catch (CompletionException e) {
            throw new ConnectException("could not open a Zerobus stream for " + table + ": " + e.getCause(), e);
        }
    }

    private static final class Sdk implements JsonSink {
        private final ZerobusJsonStream s;

        Sdk(ZerobusJsonStream s) {
            this.s = s;
        }

        @Override
        public long ingest(String json) throws ZerobusException {
            return s.ingestRecordOffset(json);
        }

        @Override
        public void flush(Duration timeout) throws ZerobusException {
            s.flush();
        }

        @Override
        public boolean isClosed() {
            return s.isClosed();
        }

        @Override
        public void close() {
            try {
                if (!s.isClosed()) {
                    s.close();
                }
            } catch (ZerobusException e) {
                log.warn("closing a Zerobus stream failed: {}", e.getMessage());
            }
        }
    }
}
