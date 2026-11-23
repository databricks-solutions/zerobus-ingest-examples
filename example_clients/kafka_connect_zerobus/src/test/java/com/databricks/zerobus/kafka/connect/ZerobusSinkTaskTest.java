package com.databricks.zerobus.kafka.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.zerobus.NonRetriableException;
import com.databricks.zerobus.ZerobusException;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.kafka.connect.errors.ConnectException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.function.Predicate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Test;

/**
 * Unit: the sink task with fakes for the SDK streams, the catalog and the control topic. The control
 * port is a loopback: requests go to a table-service owner, and its SCHEMA_CHANGED messages come back
 * to the task's broadcast handler, which is the protocol without Kafka.
 */
class ZerobusSinkTaskTest {

    /** Records what each table received; acknowledges synchronously, which the stream must handle. */
    static final class FakeSinks implements JsonSink.Factory {
        final Map<String, List<String>> sent = new HashMap<>();
        final Map<String, Integer> opens = new HashMap<>();
        /** Flush calls across all streams (iteration 5: the commit path flushes only streams with records in flight). */
        final java.util.concurrent.atomic.AtomicInteger flushes = new java.util.concurrent.atomic.AtomicInteger();
        /** When true, acknowledgements wait for the next flush, like the SDK (acked at flush, not at ingest). */
        volatile boolean holdAcks;

        @Override
        public synchronized JsonSink open(String table, JsonSink.AckListener listener) {
            opens.merge(table, 1, Integer::sum);
            List<Long> held = new ArrayList<>();
            return new JsonSink() {
                private long next;
                private boolean closed;

                @Override
                public long ingest(String json) {
                    long off;
                    synchronized (FakeSinks.this) {
                        off = next++;
                        sent.computeIfAbsent(table, k -> new ArrayList<>()).add(json);
                    }
                    if (holdAcks) {
                        synchronized (held) {
                            held.add(off);
                        }
                    } else {
                        listener.onAck(off);
                    }
                    return off;
                }

                @Override
                public void flush(java.time.Duration timeout) {
                    flushes.incrementAndGet();
                    List<Long> acks;
                    synchronized (held) {
                        acks = new ArrayList<>(held);
                        held.clear();
                    }
                    for (Long off : acks) {
                        listener.onAck(off);
                    }
                }

                @Override
                public boolean isClosed() {
                    return closed;
                }

                @Override
                public void close() {
                    closed = true;
                }
            };
        }

        synchronized List<String> rows(String table) {
            return sent.getOrDefault(table, Collections.emptyList());
        }
    }

    /** Holds every control message; the test pumps them to the owner and the broadcast handler. */
    static final class Loop implements ControlMessage.Port {
        final List<ControlMessage> out = new ArrayList<>();

        @Override
        public synchronized void send(ControlMessage m) {
            out.add(m);
        }

        synchronized List<ControlMessage> drain() {
            List<ControlMessage> copy = new ArrayList<>(out);
            out.clear();
            return copy;
        }
    }

    /** Stands in for SinkTaskContext: records pause and resume, and reports an empty assignment. */
    static final class Ctx {
        final Set<TopicPartition> paused = new HashSet<>();
        /** Poll timeouts requested through SinkTaskContext.timeout(ms). */
        final List<Long> timeouts = new ArrayList<>();
        final SinkTaskContext context;

        Ctx() {
            context = (SinkTaskContext) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {SinkTaskContext.class}, (proxy, method, args) -> {
                        if ("timeout".equals(method.getName()) && args != null) {
                            timeouts.add((Long) args[0]);
                        }
                        if (("pause".equals(method.getName()) || "resume".equals(method.getName())) && args != null) {
                            TopicPartition[] tps = (TopicPartition[]) args[0];
                            for (TopicPartition tp : tps) {
                                if ("pause".equals(method.getName())) {
                                    paused.add(tp);
                                } else {
                                    paused.remove(tp);
                                }
                            }
                        }
                        return null;
                    });
        }
    }

    private static final Schema ID_ONLY = SchemaBuilder.struct().name("rec1").field("id", Schema.INT64_SCHEMA)
            .field("kind", Schema.OPTIONAL_STRING_SCHEMA).build();
    private static final Schema WITH_SCORE = SchemaBuilder.struct().name("rec2").field("id", Schema.INT64_SCHEMA)
            .field("kind", Schema.OPTIONAL_STRING_SCHEMA).field("score", Schema.OPTIONAL_FLOAT64_SCHEMA).build();

    private static SinkRecord rec(String topic, int partition, long offset, Schema s, Struct v) {
        return new SinkRecord(topic, partition, null, null, s, v, offset);
    }

    private static Struct idOnly(long id, String kind) {
        return new Struct(ID_ONLY).put("id", id).put("kind", kind);
    }

    private static Struct withScore(long id, double score) {
        return new Struct(WITH_SCORE).put("id", id).put("kind", "k").put("score", score);
    }

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = TestConfig.base();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    /** Delivers requests to the owner and state back to the task until the loop is quiet. */
    private static void pump(ZerobusSinkTask task, TableService owner, Loop loop) {
        for (int round = 0; round < 20; round++) {
            List<ControlMessage> batch = loop.drain();
            if (batch.isEmpty()) {
                return;
            }
            task.onBroadcast(batch);
            List<ControlMessage> reqs = batch.stream().filter(ControlMessage::isRequest).collect(Collectors.toList());
            if (owner != null && !reqs.isEmpty()) {
                owner.handle(reqs);
            }
        }
    }

    @Test
    void staticFanOutWritesEachRecordToEveryMatchingTable() {
        FakeSinks sinks = new FakeSinks();
        ZerobusSinkTask task = new ZerobusSinkTask();
        task.initialize(new Ctx().context);
        task.configure(props("zerobus.tables", "main.s.a,main.s.b", "zerobus.tables.route.field", "kind",
                "zerobus.tables.main.s.a.route.regex", "alpha|beta", "zerobus.tables.main.s.b.route.regex", "alpha"),
                sinks, null, null);
        TopicPartition tp = new TopicPartition("src", 0);
        task.put(Arrays.asList(rec("src", 0, 0, ID_ONLY, idOnly(1, "alpha")), rec("src", 0, 1, ID_ONLY, idOnly(2, "beta"))));
        assertEquals(2, sinks.rows("main.s.a").size());
        assertEquals(1, sinks.rows("main.s.b").size(), "beta does not match table b's regex");
        task.preCommit(Collections.emptyMap());
        assertEquals(2, task.committableFor(tp), "both records are complete in every target");
    }

    @Test
    void oneConnectorForManyTopicsWithRegexTopicRouting() {
        FakeSinks sinks = new FakeSinks();
        ZerobusSinkTask task = new ZerobusSinkTask();
        task.initialize(new Ctx().context);
        task.configure(props("zerobus.topic2table.map", "kc2_(.*)=>main.s.$1"), sinks, null, null);
        task.put(Arrays.asList(rec("kc2_a", 0, 0, ID_ONLY, idOnly(1, "x")), rec("kc2_b", 0, 0, ID_ONLY, idOnly(2, "x"))));
        // a topic that appears after start matches the regex with no change to the config
        task.put(Collections.singletonList(rec("kc2_late", 0, 0, ID_ONLY, idOnly(3, "x"))));
        assertEquals(1, sinks.rows("main.s.a").size());
        assertEquals(1, sinks.rows("main.s.b").size());
        assertEquals(1, sinks.rows("main.s.late").size());
    }

    @Test
    void unroutableRecordGoesToTheDeadLetterTableAndTheOffsetStillCommits() {
        FakeSinks sinks = new FakeSinks();
        ZerobusSinkTask task = new ZerobusSinkTask();
        task.initialize(new Ctx().context);
        task.configure(props("zerobus.tables.dynamic.enabled", "true", "zerobus.tables.dynamic.namespace", "main.s",
                "zerobus.tables.route.field", "kind", "zerobus.dlq.table", "main.s.kc2_dlq"), sinks, null, null);
        TopicPartition tp = new TopicPartition("src", 0);
        task.put(Arrays.asList(rec("src", 0, 0, ID_ONLY, idOnly(1, "ok_t")), rec("src", 0, 1, ID_ONLY, idOnly(2, "bad-name"))));
        assertEquals(1, sinks.rows("main.s.ok_t").size());
        assertEquals(1, sinks.rows("main.s.kc2_dlq").size());
        assertTrue(sinks.rows("main.s.kc2_dlq").get(0).contains("not a valid table name"));
        task.preCommit(Collections.emptyMap());
        assertEquals(2, task.committableFor(tp));
    }

    @Test
    void aMissingDeadLetterTableWithAutoCreateOffFailsAtStartAndNamesTheTable() {
        // D74: without this check the dead-letter stream open fails with a misleading 401 ("invalid format").
        FakeCatalog cat = new FakeCatalog();
        ZerobusSinkTask task = new ZerobusSinkTask();
        task.initialize(new Ctx().context);
        ConnectException e = assertThrows(ConnectException.class, () -> task.configure(
                props("zerobus.table", "main.s.t", "zerobus.dlq.table", "main.s.kc5_dlq_missing"), new FakeSinks(), cat, null));
        assertTrue(e.getMessage().contains("main.s.kc5_dlq_missing"), e.getMessage());
        assertTrue(e.getMessage().contains("zerobus.table.auto.create"), e.getMessage());
    }

    @Test
    void aTransientErrorOnTheDeadLetterCheckIsRetriedAndThenPasses() {
        // Iteration 6 defect: a transient SQL error at start killed the task. The check now retries with backoff.
        long saved = ZerobusSinkTask.dlqCheckBackoffMs;
        ZerobusSinkTask.dlqCheckBackoffMs = 1L;
        try {
            FakeCatalog cat = new FakeCatalog();
            cat.preset("main.s.kc6_dlq_flaky", 0, TableColumns.DEAD_LETTER);
            cat.transientFailures.set(2);  // the first two describes fail, the third succeeds
            ZerobusSinkTask task = new ZerobusSinkTask();
            task.initialize(new Ctx().context);
            task.configure(props("zerobus.table", "main.s.t", "zerobus.dlq.table", "main.s.kc6_dlq_flaky"), new FakeSinks(), cat, null);
            assertEquals(0, cat.transientFailures.get(), "both failures were consumed by retries");
        } finally {
            ZerobusSinkTask.dlqCheckBackoffMs = saved;
        }
    }

    @Test
    void aDeadLetterCheckThatNeverSucceedsIsRetriableNotFatal() {
        long saved = ZerobusSinkTask.dlqCheckBackoffMs;
        ZerobusSinkTask.dlqCheckBackoffMs = 1L;
        try {
            FakeCatalog cat = new FakeCatalog();
            cat.preset("main.s.kc6_dlq_down", 0, TableColumns.DEAD_LETTER);
            cat.transientFailures.set(100);
            ZerobusSinkTask task = new ZerobusSinkTask();
            task.initialize(new Ctx().context);
            org.apache.kafka.connect.errors.RetriableException e = assertThrows(org.apache.kafka.connect.errors.RetriableException.class,
                    () -> task.configure(props("zerobus.table", "main.s.t", "zerobus.dlq.table", "main.s.kc6_dlq_down"),
                            new FakeSinks(), cat, null));
            assertTrue(e.getMessage().contains("main.s.kc6_dlq_down"), e.getMessage());
            assertTrue(e.getMessage().contains("5 attempts"), e.getMessage());
        } finally {
            ZerobusSinkTask.dlqCheckBackoffMs = saved;
        }
    }

    @Test
    void aDeadLetterTableMissingAColumnTheOptionsNeedFailsAtStart() {
        // Created before iteration 5: no value_original_bytes / value_truncated. The connector never alters it (D92).
        Map<String, String> old = new java.util.LinkedHashMap<>(TableColumns.DEAD_LETTER);
        old.remove("value_original_bytes");
        old.remove("value_truncated");
        FakeCatalog cat = new FakeCatalog();
        cat.preset("main.s.kc5_dlq_old", 0, old);
        ZerobusSinkTask task = new ZerobusSinkTask();
        task.initialize(new Ctx().context);
        ConnectException e = assertThrows(ConnectException.class, () -> task.configure(
                props("zerobus.table", "main.s.t", "zerobus.dlq.table", "main.s.kc5_dlq_old"), new FakeSinks(), cat, null));
        assertTrue(e.getMessage().contains("main.s.kc5_dlq_old"), e.getMessage());
        assertTrue(e.getMessage().contains("value_original_bytes"), e.getMessage());
    }

    private static SinkRecord bigRecord(long offset, String kind, int bodyLength) {
        Schema schema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA)
                .field("kind", Schema.STRING_SCHEMA).field("body", Schema.OPTIONAL_STRING_SCHEMA).build();
        Struct v = new Struct(schema).put("id", offset).put("kind", kind).put("body", "y".repeat(bodyLength));
        return new SinkRecord("src", 0, null, null, schema, v, offset);
    }

    @Test
    void anOversizeRecordIsDeadLetteredAsMetadataOnly() {
        FakeSinks sinks = new FakeSinks();
        ZerobusSinkTask task = new ZerobusSinkTask();
        task.initialize(new Ctx().context);
        task.configure(props("zerobus.tables.dynamic.enabled", "true", "zerobus.tables.dynamic.namespace", "main.s",
                "zerobus.tables.route.field", "kind", "zerobus.dlq.table", "main.s.dlq"), sinks, null, null);
        // The route value is not a valid table name, so the record is rejected; its body is over 9 MiB too.
        task.put(Arrays.asList(bigRecord(0, "bad-name", 10 * 1024 * 1024), bigRecord(1, "ok_t", 5)));
        assertEquals(1, sinks.rows("main.s.dlq").size(), "the oversize record lands as one metadata row");
        String row = sinks.rows("main.s.dlq").get(0);
        assertTrue(row.contains("\"value_truncated\":true"), row.substring(0, 200));
        assertTrue(row.contains("\"offset\":0"), "the coordinates are kept");
        assertTrue(row.contains("\"value\":null"), "the value is left out");
        task.preCommit(Collections.emptyMap());
        assertEquals(2, task.committableFor(new TopicPartition("src", 0)), "both records are handled and the offsets commit");
    }

    @Test
    void anOversizeRecordWithOversizeOnlyOffIsSkippedLoggedAndItsOffsetStillCommits() {
        FakeSinks sinks = new FakeSinks();
        ZerobusSinkTask task = new ZerobusSinkTask();
        task.initialize(new Ctx().context);
        task.configure(props("zerobus.tables.dynamic.enabled", "true", "zerobus.tables.dynamic.namespace", "main.s",
                "zerobus.tables.route.field", "kind", "zerobus.dlq.table", "main.s.dlq",
                "zerobus.dlq.oversize.metadata.only", "false"), sinks, null, null);
        task.put(Arrays.asList(bigRecord(0, "bad-name", 10 * 1024 * 1024), bigRecord(1, "ok_t", 5)));
        assertEquals(0, sinks.rows("main.s.dlq").size(), "nothing is written for the oversize record");
        assertEquals(1, task.oversizeSkipped());
        task.preCommit(Collections.emptyMap());
        assertEquals(2, task.committableFor(new TopicPartition("src", 0)), "a skipped record does not hold the commit");
    }

    @Test
    void replayAfterARevokeDeliversEveryRecordAtLeastOnce() {
        // Worker A takes offsets 0..2, commits, and loses the partition. Worker B replays from the committed offset.
        FakeSinks sinks = new FakeSinks();
        sinks.holdAcks = true;
        TopicPartition tp = new TopicPartition("src", 0);
        ZerobusSinkTask a = new ZerobusSinkTask();
        a.initialize(new Ctx().context);
        a.configure(props("zerobus.table", "main.s.t"), sinks, null, null);
        a.put(Arrays.asList(rec("src", 0, 0, ID_ONLY, idOnly(1, "x")), rec("src", 0, 1, ID_ONLY, idOnly(2, "x")),
                rec("src", 0, 2, ID_ONLY, idOnly(3, "x"))));
        a.preCommit(Collections.emptyMap());
        long committed = a.committableFor(tp);
        a.close(Collections.singletonList(tp));
        ZerobusSinkTask b = new ZerobusSinkTask();
        b.initialize(new Ctx().context);
        b.configure(props("zerobus.table", "main.s.t"), sinks, null, null);
        b.open(Collections.singletonList(tp));
        b.put(Collections.singletonList(rec("src", 0, committed, ID_ONLY, idOnly(4, "x"))));  // offset 3 (id 4)
        b.preCommit(Collections.emptyMap());
        assertEquals(4, b.committableFor(tp));
        assertCoverage(sinks, "main.s.t", 0, 3);
    }

    @Test
    void closeDuringInFlightWorkLosesNothingOnReplay() {
        FakeSinks sinks = new FakeSinks();
        sinks.holdAcks = true;
        TopicPartition tp = new TopicPartition("src", 0);
        ZerobusSinkTask a = new ZerobusSinkTask();
        a.initialize(new Ctx().context);
        a.configure(props("zerobus.table", "main.s.t"), sinks, null, null);
        a.put(Arrays.asList(rec("src", 0, 0, ID_ONLY, idOnly(1, "x")), rec("src", 0, 1, ID_ONLY, idOnly(2, "x"))));
        // No preCommit: the close happens with records in flight. Nothing is committed, so the replay starts at 0.
        a.close(Collections.singletonList(tp));
        ZerobusSinkTask b = new ZerobusSinkTask();
        b.initialize(new Ctx().context);
        b.configure(props("zerobus.table", "main.s.t"), sinks, null, null);
        b.open(Collections.singletonList(tp));
        b.put(Arrays.asList(rec("src", 0, 0, ID_ONLY, idOnly(1, "x")), rec("src", 0, 1, ID_ONLY, idOnly(2, "x"))));
        b.preCommit(Collections.emptyMap());
        assertEquals(2, b.committableFor(tp));
        assertCoverage(sinks, "main.s.t", 0, 1);
    }

    /** Every offset in [from, to] is in the table at least once (duplicates allowed). Counted by offset in the id. */
    private static void assertCoverage(FakeSinks sinks, String table, long from, long to) {
        assertFalse(sinks.rows(table).isEmpty(), "rows arrived");
        for (long off = from; off <= to; off++) {
            final long id = off + 1;  // ids are offset + 1 in these tests
            assertTrue(sinks.rows(table).stream().anyMatch(r -> r.contains("\"id\":" + id) || r.contains("\"id\": " + id)),
                    "offset " + off + " (id " + id + ") arrived");
        }
    }

    @Test
    void preCommitFlushesOnlyStreamsWithRecordsInFlight() {
        FakeSinks sinks = new FakeSinks();
        sinks.holdAcks = true;
        ZerobusSinkTask task = new ZerobusSinkTask();
        task.initialize(new Ctx().context);
        task.configure(props("zerobus.table", "main.s.t"), sinks, null, null);
        TopicPartition tp = new TopicPartition("src", 0);
        task.put(Arrays.asList(rec("src", 0, 0, ID_ONLY, idOnly(1, "a")), rec("src", 0, 1, ID_ONLY, idOnly(2, "b"))));
        task.preCommit(Collections.emptyMap());
        assertEquals(2, task.committableFor(tp), "the flush acknowledged the records in flight");
        int afterFirst = sinks.flushes.get();
        assertTrue(afterFirst >= 1, "a stream with records in flight is flushed before the commit");
        task.preCommit(Collections.emptyMap());
        task.preCommit(Collections.emptyMap());
        assertEquals(afterFirst, sinks.flushes.get(), "a stream with nothing in flight is not flushed (no round trip)");
        task.put(Collections.singletonList(rec("src", 0, 2, ID_ONLY, idOnly(3, "c"))));
        task.preCommit(Collections.emptyMap());
        assertEquals(3, task.committableFor(tp));
        assertTrue(sinks.flushes.get() > afterFirst, "the next record in flight is flushed");
    }

    @Test
    void aPendingSchemaChangeWaitsWithoutPausingUntilTheChangeLands() {
        FakeSinks sinks = new FakeSinks();
        FakeCatalog cat = new FakeCatalog();
        Map<String, String> existing = new java.util.LinkedHashMap<>();
        existing.put("id", "BIGINT");
        existing.put("kind", "STRING");
        cat.preset("main.s.kc2_ev", 0, existing);
        Loop loop = new Loop();
        TableService owner = new TableService(cat, null, loop::send);
        ZerobusSinkTask task = new ZerobusSinkTask();
        Ctx ctx = new Ctx();
        task.initialize(ctx.context);
        task.configure(props("zerobus.table", "main.s.kc2_ev", "zerobus.schema.evolution.enabled", "true",
                "zerobus.schema.change.max.buffered.records", "5"), sinks, cat, loop);
        pump(task, owner, loop);  // snapshot
        TopicPartition tp = new TopicPartition("src", 0);
        task.put(Collections.singletonList(rec("src", 0, 0, ID_ONLY, idOnly(0, "x"))));
        List<SinkRecord> four = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            four.add(rec("src", 0, i, WITH_SCORE, withScore(i, i * 1.5)));
        }
        task.put(four);
        assertFalse(ctx.paused.contains(tp), "waiting for a schema change does not pause the partition");
        assertEquals(4, task.pendingFor(tp, "main.s.kc2_ev"));
        List<SinkRecord> more = new ArrayList<>();
        for (int i = 5; i <= 12; i++) {
            more.add(rec("src", 0, i, WITH_SCORE, withScore(i, i * 1.5)));
        }
        task.put(more);
        assertTrue(ctx.paused.contains(tp), "a backlog over the cap pauses the partition");
        pump(task, owner, loop);  // ALTER applied, SCHEMA_CHANGED reaches the cache
        task.put(Collections.emptyList());  // the next put (or commit tick) drains the backlog
        assertFalse(ctx.paused.contains(tp), "resumed once the backlog drained");
        assertEquals(1, cat.alters.get(), "one ALTER for the whole backlog");
        assertEquals(13, sinks.rows("main.s.kc2_ev").size());
        task.preCommit(Collections.emptyMap());
        assertEquals(13, task.committableFor(tp));
    }

    @Test
    void revokedPartitionSendsNothingAndACrashedOwnersRecordsAreReplayedOnce() {
        FakeSinks oldSinks = new FakeSinks();
        FakeCatalog cat = new FakeCatalog();
        cat.preset("main.s.kc2_ev", 0, Collections.singletonMap("id", "BIGINT"));
        Loop loop = new Loop();
        TableService owner = new TableService(cat, null, loop::send);
        ZerobusSinkTask old = new ZerobusSinkTask();
        old.initialize(new Ctx().context);
        old.configure(props("zerobus.table", "main.s.kc2_ev", "zerobus.schema.evolution.enabled", "true"),
                oldSinks, cat, loop);
        pump(old, owner, loop);
        TopicPartition tp = new TopicPartition("src", 0);
        List<SinkRecord> recs = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            recs.add(rec("src", 0, i, WITH_SCORE, withScore(i, 1.0)));
        }
        old.put(recs);  // waiting for the change: nothing sent yet
        old.close(Collections.singletonList(tp));  // rebalance: the partition moves away
        assertEquals(0, oldSinks.rows("main.s.kc2_ev").size(), "the old owner sends nothing for a partition it lost");
        assertEquals(-1, old.committableFor(tp), "nothing is committed for the revoked partition");

        FakeSinks newSinks = new FakeSinks();
        ZerobusSinkTask replacement = new ZerobusSinkTask();
        replacement.initialize(new Ctx().context);
        replacement.configure(props("zerobus.table", "main.s.kc2_ev", "zerobus.schema.evolution.enabled", "true"),
                newSinks, cat, loop);
        pump(replacement, owner, loop);
        replacement.put(recs);  // replayed from the committed offset
        pump(replacement, owner, loop);
        replacement.put(Collections.emptyList());
        assertEquals(3, newSinks.rows("main.s.kc2_ev").size(), "each record is sent once, by the new owner");
        replacement.preCommit(Collections.emptyMap());
        assertEquals(3, replacement.committableFor(tp));
    }

    @Test
    void schemalessValuesNeedAnExistingTableWhenAutoCreateIsOff() {
        FakeSinks sinks = new FakeSinks();
        FakeCatalog cat = new FakeCatalog();
        Loop loop = new Loop();
        TableService owner = new TableService(cat, null, loop::send);
        ZerobusSinkTask task = new ZerobusSinkTask();
        task.initialize(new Ctx().context);
        cat.preset("main.s.kc2_dlq2", 0, TableColumns.DEAD_LETTER);  // the dead-letter check runs at start (D74)
        task.configure(props("zerobus.table", "main.s.kc2_missing", "zerobus.schema.evolution.enabled", "true",
                "zerobus.dlq.table", "main.s.kc2_dlq2"), sinks, cat, loop);
        pump(task, owner, loop);
        task.put(Collections.singletonList(rec("src", 0, 0, ID_ONLY, idOnly(1, "x"))));
        pump(task, owner, loop);
        task.put(Collections.emptyList());
        assertEquals(0, sinks.rows("main.s.kc2_missing").size());
        assertEquals(1, sinks.rows("main.s.kc2_dlq2").size(), "a missing table is a record failure, not a silent drop");
    }

    /**
     * Acknowledges in batches at flush, like the SDK: a batch that holds a poison record is rejected whole
     * (every offset in it reported with the same error) and the stream closes. The records of a rejected
     * batch are not stored, so the server-side view matches what a real stream would keep.
     */
    static final class BatchSinks implements JsonSink.Factory {
        final Map<String, List<String>> sent = new HashMap<>();
        final Predicate<String> poison;

        BatchSinks(Predicate<String> poison) {
            this.poison = poison;
        }

        @Override
        public synchronized JsonSink open(String table, JsonSink.AckListener listener) {
            return new JsonSink() {
                private long next;
                private boolean closed;
                private final List<String> buf = new ArrayList<>();
                private final List<Long> offs = new ArrayList<>();

                @Override
                public synchronized long ingest(String json) throws ZerobusException {
                    if (closed) {
                        throw new ZerobusException("stream is closed");
                    }
                    long off = next++;
                    buf.add(json);
                    offs.add(off);
                    return off;
                }

                @Override
                public synchronized void flush(Duration timeout) {
                    if (buf.isEmpty()) {
                        return;
                    }
                    if (buf.stream().anyMatch(poison)) {
                        closed = true;
                        for (long o : offs) {
                            listener.onError(o, "invalid digit found in string");
                        }
                    } else {
                        synchronized (BatchSinks.this) {
                            sent.computeIfAbsent(table, k -> new ArrayList<>()).addAll(buf);
                        }
                        listener.onAck(offs.get(offs.size() - 1));
                    }
                    buf.clear();
                    offs.clear();
                }

                @Override
                public synchronized boolean isClosed() {
                    return closed;
                }

                @Override
                public synchronized void close() {
                    closed = true;
                }
            };
        }

        synchronized List<String> rows(String table) {
            return sent.getOrDefault(table, Collections.emptyList());
        }
    }

    /** Rejects the poison record ids only; ids are unique in every test. */
    private static Predicate<String> poisonIds(long... ids) {
        return json -> {
            for (long id : ids) {
                if (json.contains("\"id\":" + id + ",")) {
                    return true;
                }
            }
            return false;
        };
    }

    private static List<SinkRecord> idRecords(long from, long to) {
        List<SinkRecord> out = new ArrayList<>();
        for (long id = from; id <= to; id++) {
            out.add(rec("src", 0, id - from, ID_ONLY, idOnly(id, "x")));
        }
        return out;
    }

    private static List<Long> ids(List<String> rows) {
        List<Long> out = new ArrayList<>();
        for (String r : rows) {
            int at = r.indexOf("\"id\":") + 5;
            out.add(Long.parseLong(r.substring(at, r.indexOf(',', at))));
        }
        return out;
    }

    private static ZerobusSinkTask deadLetterTask(BatchSinks sinks) {
        ZerobusSinkTask task = new ZerobusSinkTask();
        task.initialize(new Ctx().context);
        task.configure(props("zerobus.table", "main.s.t", "zerobus.dlq.table", "main.s.dlq"), sinks, null, null);
        return task;
    }

    @Test
    void oneBadRecordInABatchIsTheOnlyDeadLetterAndTheOthersArriveOnceInOrder() {
        BatchSinks sinks = new BatchSinks(poisonIds(3));
        ZerobusSinkTask task = deadLetterTask(sinks);
        TopicPartition tp = new TopicPartition("src", 0);
        task.put(idRecords(1, 5));
        task.preCommit(Collections.emptyMap());
        assertEquals(Arrays.asList(1L, 2L, 4L, 5L), ids(sinks.rows("main.s.t")), "valid records once, in order");
        assertEquals(1, sinks.rows("main.s.dlq").size(), "only the bad record is dead-lettered");
        assertTrue(sinks.rows("main.s.dlq").get(0).contains("rejected by Zerobus"));
        assertEquals(5, task.committableFor(tp), "offsets advance past every record, including the dead-lettered one");
        assertTrue(task.isolatedFor(tp, "main.s.t"), "isolation lasts until a clean run of acknowledgements");
    }

    @Test
    void badRecordsAtTheStartAndTheEndOfABatch() {
        BatchSinks sinks = new BatchSinks(poisonIds(1, 5));
        ZerobusSinkTask task = deadLetterTask(sinks);
        task.put(idRecords(1, 5));
        task.preCommit(Collections.emptyMap());
        assertEquals(Arrays.asList(2L, 3L, 4L), ids(sinks.rows("main.s.t")));
        assertEquals(2, sinks.rows("main.s.dlq").size());
        assertEquals(5, task.committableFor(new TopicPartition("src", 0)));
    }

    @Test
    void threeBadRecordsSpreadAcrossABatchAreEachDeadLetteredOnce() {
        BatchSinks sinks = new BatchSinks(poisonIds(2, 5, 9));
        ZerobusSinkTask task = deadLetterTask(sinks);
        task.put(idRecords(1, 10));
        task.preCommit(Collections.emptyMap());
        assertEquals(Arrays.asList(1L, 3L, 4L, 6L, 7L, 8L, 10L), ids(sinks.rows("main.s.t")));
        assertEquals(3, sinks.rows("main.s.dlq").size());
        assertEquals(10, task.committableFor(new TopicPartition("src", 0)));
    }

    @Test
    void isolationEndsAfterACleanRunAndPipelinedSendsResume() {
        BatchSinks sinks = new BatchSinks(poisonIds(2));
        ZerobusSinkTask task = deadLetterTask(sinks);
        TopicPartition tp = new TopicPartition("src", 0);
        task.put(idRecords(1, 110));
        task.preCommit(Collections.emptyMap());
        assertEquals(109, sinks.rows("main.s.t").size(), "every valid record delivered once");
        assertEquals(1, sinks.rows("main.s.dlq").size());
        assertFalse(task.isolatedFor(tp, "main.s.t"), "a clean run of ISOLATION_CLEAR_AFTER records ends isolation");
        assertEquals(110, task.committableFor(tp));
    }

    /**
     * Arrow stand-in. A batch is one offset; records are acknowledged at flush. A flush with a poison id in any
     * batch in flight rejects all of them (the SDK reports a rejection for the batch) and closes the stream.
     */
    static final class FakeArrow implements ArrowSink.Factory {
        final Map<String, List<Long>> sent = new HashMap<>();
        final List<Integer> ingests = new ArrayList<>();
        final List<String> opened = new ArrayList<>();
        /** Thread that opened each stream (the open pool or the task thread). */
        final List<String> openThreads = new ArrayList<>();
        final Set<Long> poison;

        FakeArrow(Long... poison) {
            this.poison = new HashSet<>(Arrays.asList(poison));
        }

        @Override
        public synchronized ArrowSink open(String table, org.apache.arrow.vector.types.pojo.Schema schema) {
            opened.add(table + ":" + schema.getFields().size());
            openThreads.add(Thread.currentThread().getName());
            return new ArrowSink() {
                private long next;
                private boolean closed;
                private final List<List<Long>> flight = new ArrayList<>();

                @Override
                public synchronized long ingest(VectorSchemaRoot batch) throws ZerobusException {
                    if (closed) {
                        throw new ZerobusException("stream is closed");
                    }
                    List<Long> ids = new ArrayList<>();
                    BigIntVector v = (BigIntVector) batch.getVector("id");
                    for (int i = 0; i < batch.getRowCount(); i++) {
                        ids.add(v.get(i));
                    }
                    flight.add(ids);
                    synchronized (FakeArrow.this) {
                        ingests.add(ids.size());
                    }
                    return next++;
                }

                @Override
                public synchronized void flush(Duration timeout) throws ZerobusException {
                    for (List<Long> b : flight) {
                        for (Long id : b) {
                            if (poison.contains(id)) {
                                closed = true;
                                flight.clear();
                                throw new NonRetriableException("invalid digit found in string");
                            }
                        }
                    }
                    synchronized (FakeArrow.this) {
                        for (List<Long> b : flight) {
                            sent.computeIfAbsent(table, k -> new ArrayList<>()).addAll(b);
                        }
                    }
                    flight.clear();
                }

                @Override
                public synchronized boolean isClosed() {
                    return closed;
                }

                @Override
                public synchronized void close() {
                    closed = true;
                }
            };
        }
    }

    private static ZerobusSinkTask arrowTask(FakeSinks json, FakeArrow arrow, String... kv) {
        ZerobusSinkTask task = new ZerobusSinkTask();
        task.initialize(new Ctx().context);
        task.configure(props(kv), json, arrow, null, null);
        return task;
    }

    @Test
    void arrowBatchesAreOneOffsetEachAndCommitFollowsTheFlush() {
        FakeSinks json = new FakeSinks();
        FakeArrow arrow = new FakeArrow();
        ZerobusSinkTask task = arrowTask(json, arrow, "zerobus.table", "main.s.t", "zerobus.arrow.batch.max.records", "2");
        TopicPartition tp = new TopicPartition("src", 0);
        task.put(idRecords(1, 5));
        assertEquals(Arrays.asList(2, 2, 1), arrow.ingests, "batches of at most 2 records, one offset each");
        assertEquals(Collections.singletonList("main.s.t:2"), arrow.opened, "one stream, pinned to the record's fields");
        task.preCommit(Collections.emptyMap());
        assertEquals(Arrays.asList(1L, 2L, 3L, 4L, 5L), arrow.sent.get("main.s.t"), "every record once, in order");
        assertEquals(0, json.rows("main.s.t").size(), "a flat typed table goes over Arrow, not JSON");
        assertEquals(5, task.committableFor(tp), "offsets commit after the flush acknowledged every batch");
    }

    @Test
    void formatAutoPicksArrowOnlyForTypedTablesWithAnArrowMapping() {
        FakeArrow flat = new FakeArrow();
        arrowTask(new FakeSinks(), flat, "zerobus.table", "main.s.t").put(idRecords(1, 2));
        assertEquals(1, flat.opened.size(), "flat typed: Arrow");

        FakeArrow rescue = new FakeArrow();
        FakeSinks rescueJson = new FakeSinks();
        arrowTask(rescueJson, rescue, "zerobus.table", "main.s.t", "zerobus.rescue.column.enabled", "true").put(idRecords(1, 2));
        assertEquals(0, rescue.opened.size(), "rescue column: JSON");
        assertEquals(2, rescueJson.rows("main.s.t").size());

        FakeArrow envelope = new FakeArrow();
        FakeSinks envelopeJson = new FakeSinks();
        arrowTask(envelopeJson, envelope, "zerobus.table", "main.s.t", "zerobus.mode", "envelope").put(idRecords(1, 1));
        assertEquals(0, envelope.opened.size(), "envelope: JSON");

        FakeArrow nested = new FakeArrow();
        org.apache.kafka.connect.data.Schema tagged = SchemaBuilder.struct().name("nested1").field("id", org.apache.kafka.connect.data.Schema.INT64_SCHEMA)
                .field("tags", SchemaBuilder.array(org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA).optional().build()).build();
        Struct v = new Struct(tagged).put("id", 1L).put("tags", Arrays.asList("a", "b"));
        arrowTask(new FakeSinks(), nested, "zerobus.table", "main.s.t").put(Collections.singletonList(rec("src", 0, 0, tagged, v)));
        assertEquals(1, nested.opened.size(), "nested struct and array of probed types: Arrow");

        FakeArrow schemaless = new FakeArrow();
        FakeSinks schemalessJson = new FakeSinks();
        arrowTask(schemalessJson, schemaless, "zerobus.table", "main.s.t").put(Collections.singletonList(
                new SinkRecord("src", 0, null, null, null, Collections.singletonMap("id", 1L), 0)));
        assertEquals(0, schemaless.opened.size(), "schemaless values: JSON");
        assertEquals(1, schemalessJson.rows("main.s.t").size());
    }

    @Test
    void formatArrowIsRefusedForRescueTables() {
        assertThrows(ConnectException.class, () -> arrowTask(new FakeSinks(), new FakeArrow(),
                "zerobus.table", "main.s.t", "zerobus.format", "arrow", "zerobus.rescue.column.enabled", "true"));
    }

    @Test
    void aRejectedArrowBatchIsRequeuedIsolatedAndOnlyTheBadRecordIsDeadLettered() {
        FakeSinks json = new FakeSinks();
        FakeArrow arrow = new FakeArrow(3L);
        ZerobusSinkTask task = arrowTask(json, arrow, "zerobus.table", "main.s.t", "zerobus.dlq.table", "main.s.dlq");
        TopicPartition tp = new TopicPartition("src", 0);
        task.put(idRecords(1, 5));
        task.preCommit(Collections.emptyMap());
        assertEquals(Arrays.asList(1L, 2L, 4L, 5L), arrow.sent.get("main.s.t"), "valid records once, in order");
        assertEquals(1, json.rows("main.s.dlq").size(), "only the bad record is dead-lettered");
        assertTrue(json.rows("main.s.dlq").get(0).contains("rejected by Zerobus"));
        assertEquals(5, task.committableFor(tp));
    }

    @Test
    void aRecordWithAFieldTheArrowStreamLacksReopensTheStream() {
        FakeArrow arrow = new FakeArrow();
        ZerobusSinkTask task = arrowTask(new FakeSinks(), arrow, "zerobus.table", "main.s.t");
        TopicPartition tp = new TopicPartition("src", 0);
        task.put(Arrays.asList(rec("src", 0, 0, ID_ONLY, idOnly(1, "x")), rec("src", 0, 1, WITH_SCORE, withScore(2, 1.5))));
        task.preCommit(Collections.emptyMap());
        assertEquals(2, arrow.opened.size(), "the second record's schema needs a new stream");
        assertEquals(Arrays.asList(1L, 2L), arrow.sent.get("main.s.t"));
        assertEquals(2, task.committableFor(tp));
    }

    @Test
    void anArrowStreamTheServerRefusesOpensAsJsonInAutoMode() {
        FakeSinks json = new FakeSinks();
        ArrowSink.Factory refusing = (table, schema) -> {
            throw new ConnectException("could not open an Arrow stream for " + table
                    + ": Arrow schema does not match the target table: Field 'amount' is incompatible");
        };
        ZerobusSinkTask task = new ZerobusSinkTask();
        task.initialize(new Ctx().context);
        task.configure(props("zerobus.table", "main.s.t"), json, refusing, null, null);
        TopicPartition tp = new TopicPartition("src", 0);
        task.put(idRecords(1, 3));
        task.preCommit(Collections.emptyMap());
        assertEquals(3, json.rows("main.s.t").size(), "the stream falls back to JSON, which judges each record");
        assertEquals(3, task.committableFor(tp));
    }

    /** Waits on the Arrow pool and returns once the stream has sent: put(empty) is the commit-tick style wake. */
    private static void settleUntilSent(ZerobusSinkTask task, FakeArrow arrow, String table, int expected) {
        long end = System.currentTimeMillis() + 15000L;
        while (arrow.sent.getOrDefault(table, Collections.emptyList()).size() < expected
                && System.currentTimeMillis() < end) {
            task.put(Collections.emptyList());
            try {
                Thread.sleep(5L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Test
    void anArrowReopenAfterASchemaChangeOpensOnThePoolAndTheQueueResumes() {
        FakeArrow arrow = new FakeArrow();
        FakeCatalog cat = new FakeCatalog();
        Map<String, String> existing = new java.util.LinkedHashMap<>();
        existing.put("id", "BIGINT");
        existing.put("kind", "STRING");
        cat.preset("main.s.t", 0, existing);
        Loop loop = new Loop();
        TableService owner = new TableService(cat, null, loop::send);
        ZerobusSinkTask task = new ZerobusSinkTask();
        task.initialize(new Ctx().context);
        task.configure(props("zerobus.table", "main.s.t", "zerobus.schema.evolution.enabled", "true",
                "zerobus.arrow.open.threads", "4"), new FakeSinks(), arrow, cat, loop);
        pump(task, owner, loop);  // snapshot
        TopicPartition tp = new TopicPartition("src", 0);
        task.put(Collections.singletonList(rec("src", 0, 0, ID_ONLY, idOnly(1, "x"))));
        settleUntilSent(task, arrow, "main.s.t", 1);
        task.put(Collections.singletonList(rec("src", 0, 1, WITH_SCORE, withScore(2, 1.5))));
        pump(task, owner, loop);  // ALTER applied, SCHEMA_CHANGED reaches the cache
        settleUntilSent(task, arrow, "main.s.t", 2);
        task.preCommit(Collections.emptyMap());
        assertEquals(Arrays.asList(1L, 2L), arrow.sent.get("main.s.t"), "both records once, in order");
        assertEquals(Arrays.asList("main.s.t:2", "main.s.t:3"), arrow.opened, "the reopen pins the new schema");
        for (String thread : arrow.openThreads) {
            assertTrue(thread.startsWith("zerobus-open-"), "every Arrow open runs on the pool, not the task thread: " + thread);
        }
        assertEquals(2, task.committableFor(tp));
    }

    @Test
    void aWaitingQueueAsksForAShortPollSoItIsCheckedWithoutRecords() {
        FakeCatalog cat = new FakeCatalog();
        Map<String, String> existing = new java.util.LinkedHashMap<>();
        existing.put("id", "BIGINT");
        existing.put("kind", "STRING");
        cat.preset("main.s.t", 0, existing);
        Loop loop = new Loop();
        TableService owner = new TableService(cat, null, loop::send);
        ZerobusSinkTask task = new ZerobusSinkTask();
        Ctx ctx = new Ctx();
        task.initialize(ctx.context);
        task.configure(props("zerobus.table", "main.s.t", "zerobus.schema.evolution.enabled", "true"),
                new FakeSinks(), null, cat, loop);
        pump(task, owner, loop);
        task.put(Collections.singletonList(rec("src", 0, 0, ID_ONLY, idOnly(1, "x"))));
        assertTrue(ctx.timeouts.isEmpty(), "nothing waits: no short poll is requested");
        task.put(Collections.singletonList(rec("src", 0, 1, WITH_SCORE, withScore(2, 1.5))));
        assertTrue(ctx.timeouts.contains(ZerobusSinkTask.WAKE_MS), "a waiting queue asks for a short poll");
        assertEquals(1, task.pendingFor(new TopicPartition("src", 0), "main.s.t"), "the record waits for the change");
        pump(task, owner, loop);
        ctx.timeouts.clear();
        task.put(Collections.emptyList());  // the poll that the wake returns: the wait is satisfied and the record goes
        assertEquals(0, task.pendingFor(new TopicPartition("src", 0), "main.s.t"));
        assertTrue(ctx.timeouts.isEmpty(), "no wait left: no short poll");
    }
}
