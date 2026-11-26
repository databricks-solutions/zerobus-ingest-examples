package com.databricks.zerobus.kafka.connect;

import com.databricks.zerobus.ZerobusException;
import java.time.Duration;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * One open Arrow stream to one table, as the task uses it. A batch is one offset: every record in it is
 * acknowledged together. Arrow has no ack callback, so acknowledgement is learned from flush().
 */
public interface ArrowSink {

    /** Opens sinks under a fixed schema. Throws on failure to open. */
    interface Factory {
        ArrowSink open(String table, Schema schema);
    }

    /** Sends one batch (the SDK serialises it before returning) and returns the batch's stream offset. */
    long ingest(VectorSchemaRoot batch) throws ZerobusException;

    /** Returns once everything sent so far is acknowledged. Throws NonRetriableException on a rejection. */
    void flush(Duration timeout) throws ZerobusException;

    boolean isClosed();

    void close();
}
