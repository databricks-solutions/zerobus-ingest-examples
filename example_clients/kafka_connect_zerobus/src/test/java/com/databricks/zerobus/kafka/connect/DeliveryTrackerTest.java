package com.databricks.zerobus.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/** Unit: offset commit only past fully completed records, with fan-out and tolerated failures. */
class DeliveryTrackerTest {

    private static SinkRecord r(long offset) {
        return new SinkRecord("t", 0, null, null, null, "v", offset);
    }

    @Test
    void commitWaitsForTheHeadRecord() {
        DeliveryTracker t = new DeliveryTracker();
        DeliveryTracker.Ticket a = t.open(r(0), 1);
        DeliveryTracker.Ticket b = t.open(r(1), 1);
        t.done(b, false);
        assertEquals(-1, t.committableNext(), "offset 1 is done but offset 0 is not");
        t.done(a, false);
        assertEquals(2, t.committableNext());
    }

    @Test
    void fanOutNeedsEveryTargetBeforeCommit() {
        DeliveryTracker t = new DeliveryTracker();
        DeliveryTracker.Ticket a = t.open(r(5), 3);
        t.done(a, false);
        t.done(a, false);
        assertEquals(-1, t.committableNext(), "one of three targets still open");
        t.done(a, true);
        assertEquals(6, t.committableNext(), "a tolerated failure counts as done");
        assertEquals(true, a.failed);
    }

    @Test
    void zeroTargetRecordsAreCompleteAtOnce() {
        DeliveryTracker t = new DeliveryTracker();
        t.open(r(0), 0);
        t.open(r(1), 0);
        assertEquals(2, t.committableNext(), "ignored tombstones and unroutable records advance the offset");
        assertEquals(0, t.pending());
    }

    @Test
    void addingATargetKeepsTheTicketOpen() {
        DeliveryTracker t = new DeliveryTracker();
        DeliveryTracker.Ticket a = t.open(r(9), 1);
        t.addTarget(a);
        t.done(a, false);
        assertEquals(-1, t.committableNext());
        t.done(a, false);
        assertEquals(10, t.committableNext());
    }

    @Test
    void completingMoreThanOpenedIsAnError() {
        DeliveryTracker t = new DeliveryTracker();
        DeliveryTracker.Ticket a = t.open(r(0), 1);
        t.done(a, false);
        assertThrows(IllegalStateException.class, () -> t.done(a, false));
    }

    @Test
    void outOfOrderAcksCommitOnlyTheCompletePrefix() {
        DeliveryTracker t = new DeliveryTracker();
        DeliveryTracker.Ticket t0 = t.open(r(0), 1);
        DeliveryTracker.Ticket t1 = t.open(r(1), 1);
        DeliveryTracker.Ticket t2 = t.open(r(2), 1);
        t.done(t1, false);
        assertEquals(-1, t.committableNext(), "offset 1 is done but 0 is not: nothing commits past a gap");
        t.done(t2, false);
        assertEquals(-1, t.committableNext());
        t.done(t0, false);
        assertEquals(3, t.committableNext(), "the prefix 0..2 commits once 0 lands");
    }

    @Test
    void aRetriedTargetHoldsTheCommitUntilEveryTargetIsDone() {
        DeliveryTracker t = new DeliveryTracker();
        DeliveryTracker.Ticket fanOut = t.open(r(0), 2);
        t.done(fanOut, false);
        assertEquals(-1, t.committableNext(), "one of two targets is still outstanding");
        t.addTarget(fanOut);  // a retry or a move to the dead-letter table keeps the unit
        t.done(fanOut, true);
        t.done(fanOut, false);
        assertEquals(1, t.committableNext(), "a failed target that was tolerated or moved still counts as done");
    }

    @Test
    void aDoneWithoutAnOutstandingTargetIsAnError() {
        DeliveryTracker t = new DeliveryTracker();
        DeliveryTracker.Ticket only = t.open(r(0), 1);
        t.done(only, false);
        assertThrows(IllegalStateException.class, () -> t.done(only, false));
    }
}

