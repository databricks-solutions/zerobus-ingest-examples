package com.databricks.zerobus.kafka.connect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * Coalesces table-service requests. One key (table, column set, kind) is in flight at a time:
 * a worker sends it, and every other worker that observes it on the control topic (the broadcast
 * reader sees all requests) waits instead of sending its own. A repeat is allowed only after an
 * exponential backoff with jitter, so a stalled owner sees a bounded trickle, not a storm.
 * Waiters do not poll the table service: they wait for SCHEMA_CHANGED, and settled() clears the key.
 */
final class RequestGate {

    static final long BASE_MS = 5000L;

    private static final class Entry {
        final String table;
        final String kind;
        final Set<String> columns;
        long allowedAt;
        int attempts;

        Entry(String table, String kind, Set<String> columns, long allowedAt) {
            this.table = table;
            this.kind = kind;
            this.columns = columns;
            this.allowedAt = allowedAt;
        }
    }

    private final long maxMs;
    private final Random random;
    private final Map<String, Entry> entries = new HashMap<>();

    RequestGate(long maxMs, Random random) {
        this.maxMs = maxMs;
        this.random = random;
    }

    static String changeKey(String table, Collection<String> columns) {
        return "CHANGE|" + table + "|" + String.join(",", new TreeSet<>(columns));
    }

    static String createKey(String table) {
        return "CREATE|" + table;
    }

    static String snapshotKey(String table) {
        return "SNAPSHOT|" + table;
    }

    /** True if the caller should send the request now. Sending starts the backoff window for the key. */
    synchronized boolean tryRequest(String key, String table, Collection<String> columns, long now) {
        Entry e = entries.get(key);
        if (e == null) {
            e = new Entry(table, kindOf(key), new TreeSet<>(columns), now + backoff(0));
            entries.put(key, e);
            return true;
        }
        if (now < e.allowedAt) {
            return false;
        }
        e.attempts++;
        e.allowedAt = now + backoff(e.attempts);
        return true;
    }

    /** A request for this key was seen on the control topic (sent by any worker): wait, do not resend. */
    synchronized void observed(String key, String table, Collection<String> columns, long now) {
        Entry e = entries.get(key);
        if (e == null) {
            entries.put(key, new Entry(table, kindOf(key), new TreeSet<>(columns), now + BASE_MS));
        } else {
            e.allowedAt = Math.max(e.allowedAt, now + BASE_MS);
        }
    }

    /** A SCHEMA_CHANGED arrived: drop the keys it satisfies. */
    synchronized void settled(String table, boolean exists, Collection<String> liveColumns) {
        Iterator<Map.Entry<String, Entry>> it = entries.entrySet().iterator();
        while (it.hasNext()) {
            Entry e = it.next().getValue();
            if (!e.table.equals(table)) {
                continue;
            }
            boolean done;
            switch (e.kind) {
                case "CHANGE": done = exists && liveColumns.containsAll(e.columns); break;
                case "CREATE": done = exists; break;
                default: done = true; break;
            }
            if (done) {
                it.remove();
            }
        }
    }

    synchronized int size() {
        return entries.size();
    }

    synchronized List<String> keys() {
        return new ArrayList<>(entries.keySet());
    }

    /** Backoff for attempt n: BASE * 2^n capped at maxMs, scaled by jitter in [0.5, 1.5). */
    long backoff(int attempt) {
        long raw = BASE_MS << Math.min(attempt, 10);
        long capped = Math.min(raw, maxMs);
        return (long) (capped * (0.5 + random.nextDouble()));
    }

    private static String kindOf(String key) {
        return key.substring(0, key.indexOf('|'));
    }
}
