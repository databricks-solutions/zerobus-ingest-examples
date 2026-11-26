package com.databricks.zerobus.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** Unit: request coalescing across callers, observation by other workers, settlement, and bounded backoff. */
class RequestGateTest {

    @Test
    void oneCallerOfMany512IsAllowedToSend() {
        RequestGate g = new RequestGate(30000L, new Random(1));
        String key = RequestGate.changeKey("main.s.t", Arrays.asList("score"));
        int sent = 0;
        for (int i = 0; i < 512; i++) {
            if (g.tryRequest(key, "main.s.t", Arrays.asList("score"), 1000L)) {
                sent++;
            }
        }
        assertEquals(1, sent, "512 partitions asking for the same column send once inside the window");
    }

    @Test
    void keysAreCanonicalAndPerColumnSet() {
        assertEquals(RequestGate.changeKey("t", Arrays.asList("b", "a")), RequestGate.changeKey("t", Arrays.asList("a", "b")));
        assertFalse(RequestGate.changeKey("t", Arrays.asList("a")).equals(RequestGate.changeKey("t", Arrays.asList("a", "b"))));
    }

    @Test
    void anObservedRequestSuppressesOwnSend() {
        RequestGate g = new RequestGate(30000L, new Random(2));
        String key = RequestGate.changeKey("t", Arrays.asList("c"));
        g.observed(key, "t", Arrays.asList("c"), 0L);
        assertFalse(g.tryRequest(key, "t", Arrays.asList("c"), 1000L), "another worker already asked");
        assertTrue(g.tryRequest(key, "t", Arrays.asList("c"), RequestGate.BASE_MS * 10), "after the window a resend is allowed");
    }

    @Test
    void settledClearsSatisfiedKeysOnly() {
        RequestGate g = new RequestGate(30000L, new Random(3));
        String ca = RequestGate.changeKey("t", Arrays.asList("a"));
        String cb = RequestGate.changeKey("t", Arrays.asList("b"));
        g.tryRequest(ca, "t", Arrays.asList("a"), 0L);
        g.tryRequest(cb, "t", Arrays.asList("b"), 0L);
        g.settled("t", true, Arrays.asList("a", "x"));
        assertEquals(1, g.size());
        assertEquals(Collections.singletonList(cb), g.keys());
    }

    @Test
    void createIsSettledByExistence() {
        RequestGate g = new RequestGate(30000L, new Random(4));
        g.tryRequest(RequestGate.createKey("t"), "t", Collections.emptyList(), 0L);
        g.settled("t", false, Collections.emptyList());
        assertEquals(1, g.size());
        g.settled("t", true, Collections.emptyList());
        assertEquals(0, g.size());
    }

    @Test
    void backoffGrowsAndIsCappedWithJitter() {
        RequestGate g = new RequestGate(30000L, new Random(5));
        for (int attempt = 0; attempt < 20; attempt++) {
            long b = g.backoff(attempt);
            assertTrue(b >= RequestGate.BASE_MS / 2, "lower jitter bound: " + b);
            assertTrue(b <= 30000L * 3 / 2, "upper bound is the cap plus jitter: " + b);
        }
        assertTrue(g.backoff(5) > g.backoff(0) / 2);
    }
}
