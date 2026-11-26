package com.databricks.zerobus.kafka.connect;

import com.databricks.zerobus.ZerobusException;
import java.time.Duration;

/**
 * One open JSON stream to one table, as the task uses it. The Zerobus SDK stream is wrapped so unit
 * tests can supply a fake that acknowledges synchronously.
 */
public interface JsonSink {

    /** Acknowledgement callbacks. Offsets are stream-local and restart at 0 on every stream. */
    interface AckListener {
        void onAck(long upTo);
        void onError(long offset, String message);
    }

    /** Opens sinks. Throws on failure to open. */
    interface Factory {
        JsonSink open(String table, AckListener listener);
    }

    /** Sends one JSON record and returns its stream-local offset. */
    long ingest(String json) throws ZerobusException;

    void flush(Duration timeout) throws ZerobusException;

    boolean isClosed();

    void close();
}
