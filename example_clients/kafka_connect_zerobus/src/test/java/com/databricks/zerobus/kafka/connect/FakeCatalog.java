package com.databricks.zerobus.kafka.connect;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/** In-memory catalog with the failure modes the table service must survive. */
final class FakeCatalog implements TableCatalog {

    static final class Table {
        boolean exists;
        long version = -1;
        final Map<String, String> columns = new LinkedHashMap<>();
        final Set<String> tagged = new TreeSet<>();
    }

    final Map<String, Table> tables = new HashMap<>();
    final AtomicInteger alters = new AtomicInteger();
    final AtomicInteger alterCalls = new AtomicInteger();
    final AtomicInteger creates = new AtomicInteger();
    /** Next N calls fail with a transient (timeout) error. */
    final AtomicInteger transientFailures = new AtomicInteger();
    /** Run just before the next ALTER, to simulate another owner winning the race. */
    Runnable beforeAlter;
    /** When set, the next ALTER fails with this (permanent) message. */
    String alterError;

    synchronized Table table(String name) {
        return tables.computeIfAbsent(name, k -> new Table());
    }

    synchronized void preset(String name, long version, Map<String, String> columns) {
        Table t = table(name);
        t.exists = true;
        t.version = version;
        t.columns.putAll(columns);
    }

    private void maybeTransient() throws IOException {
        if (transientFailures.getAndUpdate(v -> v > 0 ? v - 1 : 0) > 0) {
            throw new IOException("statement TIMEOUT: request timed out");
        }
    }

    @Override
    public synchronized Snapshot describe(String name) throws IOException {
        maybeTransient();
        Table t = tables.get(name);
        if (t == null || !t.exists) {
            return new Snapshot(false, -1L, new LinkedHashMap<>());
        }
        return new Snapshot(true, t.version, new LinkedHashMap<>(t.columns));
    }

    @Override
    public synchronized void createIfNotExists(String name, Map<String, String> columns) throws IOException {
        maybeTransient();
        Table t = table(name);
        if (!t.exists) {
            t.exists = true;
            t.version = 0;
            t.columns.putAll(columns);
            creates.incrementAndGet();
        }
    }

    @Override
    public synchronized void setRescueTag(String name, String column) throws IOException {
        maybeTransient();
        table(name).tagged.add(column);
    }

    @Override
    public void addColumns(String name, Map<String, String> columns) throws IOException {
        alterCalls.incrementAndGet();
        if (beforeAlter != null) {
            beforeAlter.run();
        }
        synchronized (this) {
            maybeTransient();
            if (alterError != null) {
                throw new IOException(alterError);
            }
            Table t = table(name);
            for (String c : columns.keySet()) {
                if (t.columns.containsKey(c)) {
                    throw new IOException("[FIELD_ALREADY_EXISTS] column " + c + " already exists");
                }
            }
            t.columns.putAll(columns);
            t.version++;
            alters.incrementAndGet();
        }
    }
}
