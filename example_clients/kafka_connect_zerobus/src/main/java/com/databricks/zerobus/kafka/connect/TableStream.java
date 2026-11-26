package com.databricks.zerobus.kafka.connect;

import com.databricks.zerobus.NonRetriableException;
import com.databricks.zerobus.ZerobusException;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The stream for one (topic-partition, table) pair. It may be reopened over time; a stream keeps the
 * format and the schema it opened with. Entries are kept in send order and completed on acknowledgement,
 * which releases one unit of the record's ticket. A JSON stream acknowledges per record (callbacks). An
 * Arrow stream acknowledges per batch: every record of a batch shares the batch's offset, and acknowledgement
 * is learned from flush(). Callbacks from an earlier generation are ignored.
 */
final class TableStream {

    private static final Logger log = LoggerFactory.getLogger(TableStream.class);

    /** One record bound for this table. Shared by the fan-out targets of the same record. */
    static final class Pending {
        final SinkRecord record;
        final JsonNode row;
        final DeliveryTracker.Ticket ticket;
        private String json;

        Pending(SinkRecord record, JsonNode row, DeliveryTracker.Ticket ticket) {
            this.record = record;
            this.row = row;
            this.ticket = ticket;
        }

        /** The row as the JSON text sent on a JSON stream (built once, on first use). */
        String json() {
            if (json == null) {
                json = row.toString();
            }
            return json;
        }
    }

    /** A sent record awaiting acknowledgement, or a server rejection awaiting handling. */
    static final class Entry {
        final long sdkOffset;
        final Pending pending;
        boolean done;
        boolean failed;
        boolean reported;
        String error;

        Entry(long sdkOffset, Pending pending) {
            this.sdkOffset = sdkOffset;
            this.pending = pending;
        }
    }

    final TopicPartition tp;
    final String table;
    private final DeliveryTracker tracker;
    private final JsonSink.Factory jsonFactory;
    private final ArrowSink.Factory arrowFactory;
    private final Deque<Entry> inflight = new ArrayDeque<>();
    private final Map<Long, String> errors = new HashMap<>();
    private JsonSink json;
    private ArrowSink arrow;
    private Schema pinned;
    private int generation;
    private long ackedUpTo = -1L;
    private long lastBatchOffset = -1L;
    private long openedVersion = -1L;
    private long ackCount;

    TableStream(TopicPartition tp, String table, DeliveryTracker tracker, JsonSink.Factory jsonFactory,
                ArrowSink.Factory arrowFactory) {
        this.tp = tp;
        this.table = table;
        this.tracker = tracker;
        this.jsonFactory = jsonFactory;
        this.arrowFactory = arrowFactory;
    }

    synchronized boolean isOpen() {
        return (json != null && !json.isClosed()) || (arrow != null && !arrow.isClosed());
    }

    /** True when the stream (open or closed) was opened in Arrow format. */
    synchronized boolean isArrow() {
        return arrow != null;
    }

    /** The Arrow schema the stream pinned at open (null for JSON). */
    synchronized Schema schema() {
        return pinned;
    }

    synchronized long openedVersion() {
        return openedVersion;
    }

    synchronized long ackCount() {
        return ackCount;
    }

    /** Records sent on this stream and not yet acknowledged or released (rejected ones included until released). */
    synchronized int unackedCount() {
        return inflight.size();
    }

    /**
     * Opens a stream under the given schema version. arrowSchema null opens JSON, otherwise Arrow with that
     * schema. Never called with the monitor held (SDK call).
     */
    void open(long schemaVersion, Schema arrowSchema) {
        final int g;
        synchronized (this) {
            g = ++generation;
            ackedUpTo = -1L;
            lastBatchOffset = -1L;
            errors.clear();
        }
        if (arrowSchema != null) {
            if (arrowFactory == null) {
                throw new ConnectException("Arrow format selected for " + table + " but no Arrow sink is configured");
            }
            ArrowSink a = arrowFactory.open(table, arrowSchema);
            synchronized (this) {
                arrow = a;
                json = null;
                pinned = arrowSchema;
                openedVersion = schemaVersion;
            }
            return;
        }
        JsonSink s = jsonFactory.open(table, new JsonSink.AckListener() {
            @Override
            public void onAck(long upTo) {
                TableStream.this.onAck(g, upTo);
            }

            @Override
            public void onError(long offset, String message) {
                TableStream.this.onError(g, offset, message);
            }
        });
        synchronized (this) {
            json = s;
            arrow = null;
            pinned = null;
            openedVersion = schemaVersion;
        }
    }

    /**
     * Off the task thread (the open pool): flushes the current stream, so the records it acknowledges are not
     * taken back, then opens the replacement Arrow stream. Returns the new sink, which the task attaches with
     * takeUnackedAndClose() and attachArrow(). Holds no monitor across an SDK call.
     */
    ArrowSink flushAndOpenArrow(Duration flushTimeout, Schema arrowSchema) throws ZerobusException {
        flush(flushTimeout);
        if (arrowFactory == null) {
            throw new ConnectException("Arrow format selected for " + table + " but no Arrow sink is configured");
        }
        return arrowFactory.open(table, arrowSchema);
    }

    /**
     * Task thread, after takeUnackedAndClose(): makes the sink returned by flushAndOpenArrow the stream.
     * The generation was advanced by takeUnackedAndClose, so callbacks from the old stream are ignored.
     */
    void attachArrow(ArrowSink sink, long schemaVersion, Schema arrowSchema) {
        synchronized (this) {
            arrow = sink;
            json = null;
            pinned = arrowSchema;
            openedVersion = schemaVersion;
        }
    }

    /** Sends one record on a JSON stream. The entry is registered after the SDK accepts it. */
    void ingest(Pending p) throws ZerobusException {
        JsonSink s;
        synchronized (this) {
            s = json;
        }
        if (s == null) {
            throw new ZerobusException("no open JSON stream for " + table);
        }
        long off = s.ingest(p.json());
        synchronized (this) {
            Entry e = new Entry(off, p);
            inflight.addLast(e);
            String err = errors.remove(off);
            if (err != null) {
                e.failed = true;
                e.error = err;
            }
            releaseAcked();
        }
    }

    /**
     * Sends one Arrow batch. Every record of the batch gets an entry with the batch's offset, in order. The
     * caller builds and closes the root; the SDK has serialised it when this returns.
     */
    void ingestBatch(List<Pending> batch, VectorSchemaRoot root) throws ZerobusException {
        ArrowSink a;
        synchronized (this) {
            a = arrow;
        }
        if (a == null) {
            throw new ZerobusException("no open Arrow stream for " + table);
        }
        long off = a.ingest(root);
        synchronized (this) {
            for (Pending p : batch) {
                inflight.addLast(new Entry(off, p));
            }
            lastBatchOffset = off;
            releaseAcked();
        }
    }

    void flush(Duration timeout) throws ZerobusException {
        ArrowSink a;
        JsonSink j;
        long last;
        int g;
        synchronized (this) {
            a = arrow;
            j = json;
            last = lastBatchOffset;
            g = generation;
            if (inflight.isEmpty()) {
                return;  // nothing sent is unacknowledged: no server round trip (a commit flushes every stream)
            }
        }
        if (a != null) {
            if (a.isClosed()) {
                return;
            }
            try {
                a.flush(timeout);
            } catch (NonRetriableException e) {
                failUnacked(g, e.getMessage());  // Arrow reports a rejection by flush, not by callback
                throw e;
            }
            onAck(g, last);  // a successful flush acknowledges every batch sent so far
            return;
        }
        if (j != null && !j.isClosed()) {
            j.flush(timeout);
        }
    }

    /** Closes the stream without taking back its records (the caller decides what happens to them). */
    void close() {
        JsonSink oldJ;
        ArrowSink oldA;
        synchronized (this) {
            oldJ = json;
            oldA = arrow;
            json = null;
            arrow = null;
            pinned = null;
        }
        closeSinks(oldJ, oldA);
    }

    /**
     * Closes the stream and returns every record still in flight, in send order, so the caller can
     * send them again on a fresh stream. Acknowledged records are not returned.
     */
    List<Pending> takeUnackedAndClose() {
        List<Pending> out = new ArrayList<>();
        JsonSink oldJ;
        ArrowSink oldA;
        synchronized (this) {
            generation++;
            for (Entry e : inflight) {
                if (!e.done) {
                    out.add(e.pending);
                }
            }
            inflight.clear();
            errors.clear();
            ackedUpTo = -1L;
            lastBatchOffset = -1L;
            oldJ = json;
            oldA = arrow;
            json = null;
            arrow = null;
            pinned = null;
        }
        // Never call the SDK while holding the monitor: its ack thread needs it to deliver acks.
        closeSinks(oldJ, oldA);
        return out;
    }

    private static void closeSinks(JsonSink j, ArrowSink a) {
        if (j != null) {
            j.close();
        }
        if (a != null) {
            a.close();
        }
    }

    /** Server rejections not yet handed to the task. Marks them reported. */
    synchronized List<Entry> takeFailures() {
        List<Entry> out = new ArrayList<>();
        for (Entry e : inflight) {
            if (e.failed && !e.reported) {
                e.reported = true;
                out.add(e);
            }
        }
        return out;
    }

    /**
     * Removes a rejected entry. complete=true finishes its ticket unit as failed (tolerated, reported);
     * complete=false keeps the unit, because the record moved to another target (dead-letter table).
     */
    synchronized void releaseFailure(Entry e, boolean complete) {
        inflight.remove(e);
        if (!e.done) {
            e.done = true;
            if (complete) {
                tracker.done(e.pending.ticket, true);
            }
        }
        releaseAcked();
    }

    private void onAck(int g, long upTo) {
        synchronized (this) {
            if (g != generation) {
                return;
            }
            ackedUpTo = Math.max(ackedUpTo, upTo);
            releaseAcked();
        }
    }

    /** Marks every unacknowledged entry failed with one message (an Arrow flush rejection). */
    private void failUnacked(int g, String message) {
        synchronized (this) {
            if (g != generation) {
                return;
            }
            for (Entry e : inflight) {
                if (!e.done && !e.failed) {
                    e.failed = true;
                    e.error = message;
                }
            }
        }
    }

    private void onError(int g, long offset, String message) {
        synchronized (this) {
            if (g != generation) {
                return;
            }
            boolean found = false;
            for (Entry e : inflight) {
                if (e.sdkOffset == offset) {
                    e.failed = true;
                    e.error = message;
                    found = true;
                }
            }
            if (!found) {
                errors.put(offset, message);
            }
        }
        log.warn("zerobus rejected record at sdk offset {} for {}: {}", offset, table, message);
    }

    /** Completes acknowledged entries from the head. Stops at a rejected head until it is released. Must hold the monitor. */
    private void releaseAcked() {
        while (!inflight.isEmpty()) {
            Entry e = inflight.peekFirst();
            if (e.failed || e.sdkOffset > ackedUpTo) {
                break;
            }
            inflight.pollFirst();
            if (!e.done) {
                e.done = true;
                ackCount++;
                tracker.done(e.pending.ticket, false);
            }
        }
    }
}
