package com.databricks.zerobus.kafka.connect;

import com.databricks.zerobus.ZerobusArrowStream;
import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusSdk;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Adapts the Zerobus Java SDK's Arrow stream to ArrowSink. */
final class SdkArrowSinkFactory implements ArrowSink.Factory {

    private static final Logger log = LoggerFactory.getLogger(SdkArrowSinkFactory.class);

    private final ZerobusSdk sdk;
    private final String clientId;
    private final String clientSecret;

    SdkArrowSinkFactory(ZerobusSdk sdk, String clientId, String clientSecret) {
        this.sdk = sdk;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    @Override
    public ArrowSink open(String table, Schema schema) {
        try {
            ZerobusArrowStream s = sdk.streamBuilder()
                    .table(table)
                    .oauth(clientId, clientSecret)
                    .arrow(schema)
                    .build()
                    .join();
            return new Sdk(s);
        } catch (CompletionException e) {
            throw new ConnectException("could not open an Arrow stream for " + table + ": " + e.getCause(), e);
        }
    }

    private static final class Sdk implements ArrowSink {
        private final ZerobusArrowStream s;

        Sdk(ZerobusArrowStream s) {
            this.s = s;
        }

        @Override
        public long ingest(VectorSchemaRoot batch) throws ZerobusException {
            return s.ingestBatch(batch).orElseThrow(() -> new ZerobusException("empty Arrow batch"));
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
                log.warn("closing an Arrow stream failed: {}", e.getMessage());
            }
        }
    }
}
