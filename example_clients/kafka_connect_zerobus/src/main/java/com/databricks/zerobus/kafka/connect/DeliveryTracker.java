package com.databricks.zerobus.kafka.connect;

import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Offset bookkeeping for one topic-partition. A record is a ticket with one outstanding unit per
 * target table (fan-out writes it to several). The committable offset advances only past tickets
 * whose units are all done, in Kafka order, so a record that is still in flight anywhere holds back
 * the commit. A tolerated failure counts as done (its row is in the dead-letter table or the Connect
 * error topic). Thread-safe: acknowledgements arrive on SDK threads.
 */
final class DeliveryTracker {

    static final class Ticket {
        final SinkRecord record;
        int outstanding;
        boolean failed;
        boolean reported;

        Ticket(SinkRecord record, int outstanding) {
            this.record = record;
            this.outstanding = outstanding;
        }
    }

    private final Deque<Ticket> tickets = new ArrayDeque<>();
    private long watermark = -1L;

    /** Opens a ticket for a record with the given number of target tables (0 completes it at once). */
    synchronized Ticket open(SinkRecord record, int targets) {
        Ticket t = new Ticket(record, targets);
        tickets.addLast(t);
        advance();
        return t;
    }

    /** One target of the ticket is finished (acknowledged, or failed and tolerated). */
    synchronized void done(Ticket t, boolean failed) {
        if (t.outstanding <= 0) {
            throw new IllegalStateException("ticket for offset " + t.record.kafkaOffset() + " has no outstanding targets");
        }
        t.outstanding--;
        if (failed) {
            t.failed = true;
        }
        advance();
    }

    /** Adds a target to an open ticket (a record that moves to the dead-letter table keeps its unit). */
    synchronized void addTarget(Ticket t) {
        t.outstanding++;
    }

    /** Next offset to commit, or -1 when nothing is complete yet. */
    synchronized long committableNext() {
        advance();
        return watermark < 0 ? -1L : watermark + 1;
    }

    synchronized int pending() {
        return tickets.size();
    }

    private void advance() {
        while (!tickets.isEmpty() && tickets.peekFirst().outstanding == 0) {
            watermark = tickets.pollFirst().record.kafkaOffset();
        }
    }
}
