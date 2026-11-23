package com.databricks.zerobus.kafka.connect;

import com.databricks.zerobus.NonRetriableException;
import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusSdk;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink task. Each record is routed to one or more tables (fan-out), mapped once, and queued per
 * (topic-partition, table). A queue sends in order on its table's stream once the table's schema
 * allows it.
 *
 * Waiting. A queue that needs a schema change, a table or a schema-lag retry waits without pausing
 * the partition: records keep arriving and are queued behind it. The wait ends when the broadcast
 * shows the change, and is checked on every put() and every commit tick. A partition is paused only
 * when a queue holds zerobus.schema.change.max.buffered.records. The old design paused on the first
 * wait, and resumed only when a commit tick woke the task.
 *
 * Commit. A record's offset commits only after every target table has acknowledged it (or a
 * tolerated failure was moved to the dead-letter table or reported). Order on commit and revoke:
 * resume and send what is ready, flush, then compute the offsets. Revoked partitions drop their
 * unsent records; the new owner replays them from the committed offset.
 */
public class ZerobusSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(ZerobusSinkTask.class);
    private static final long LAG_RETRY_MS = 1500L;
    /** After a schema change, streams opened earlier are reopened once the change has settled. */
    private static final long SETTLE_MS = 3000L;
    /** Clean acknowledgements in isolation before a queue goes back to pipelined sends. */
    static final int ISOLATION_CLEAR_AFTER = 100;
    /** Upper bound on the flush-and-requeue rounds in one preCommit (isolation needs about two). */
    private static final int MAX_COMMIT_ROUNDS = 4;
    /** While any queue waits, the next poll returns after this long, so put() re-checks the waits (no records needed). */
    static final long WAKE_MS = 100L;
    /** Bound for waiting on an open that is abandoned during close. */
    private static final long ABANDON_OPEN_MS = 60000L;

    enum Kind { SCHEMA, TABLE, LAG }

    /** Why a queue is waiting. */
    static final class Wait {
        final Kind kind;
        final long since;
        final long retryAt;
        final Set<String> fields;
        final Map<String, String> proposal;

        Wait(Kind kind, long since, long retryAt, Set<String> fields, Map<String, String> proposal) {
            this.kind = kind;
            this.since = since;
            this.retryAt = retryAt;
            this.fields = fields;
            this.proposal = proposal;
        }
    }

    /** An Arrow stream being flushed and opened on the open pool. Attached on the task thread when done. */
    static final class Opening {
        final long version;
        final org.apache.arrow.vector.types.pojo.Schema schema;
        final Future<ArrowSink> future;

        Opening(long version, org.apache.arrow.vector.types.pojo.Schema schema, Future<ArrowSink> future) {
            this.version = version;
            this.schema = schema;
            this.future = future;
        }
    }

    /** Records bound for one table, in order. fixed = the dead-letter table (fixed schema, no diff). */
    static final class Queue {
        final String table;
        final boolean fixed;
        final TableStream stream;
        final Deque<TableStream.Pending> pending = new ArrayDeque<>();
        Wait wait;
        /** Set while the stream is being flushed and opened on the pool (Arrow). */
        Opening opening;
        /** Set when a wait is satisfied; cleared when the first record after it is sent (the pause ends there). */
        long waitedSince;
        String waitedDesc;
        long lagSince;
        long lagAckMark;
        /** Isolation: one record in flight at a time, so a rejection names one record. */
        boolean isolated;
        int isolatedRun;

        Queue(String table, boolean fixed, TableStream stream) {
            this.table = table;
            this.fixed = fixed;
            this.stream = stream;
        }
    }

    static final class Part {
        final TopicPartition tp;
        final DeliveryTracker tracker = new DeliveryTracker();
        final Map<String, Queue> queues = new LinkedHashMap<>();
        boolean paused;

        Part(TopicPartition tp) {
            this.tp = tp;
        }
    }

    /** One record cannot be delivered to one target. Handled per record (dead-letter, tolerate or fail). */
    static final class RecordFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;
        RecordFailure(String message) {
            super(message);
        }
    }

    private ZerobusSinkConfig cfg;
    private String worker;
    private TableRouter router;
    private RecordMapper mapper;
    private JsonSink.Factory sinks;
    private ArrowSink.Factory arrowSinks;
    private BufferAllocator allocator;
    private ExecutorService opener;
    private String format;
    private int arrowMaxRecords;
    private ZerobusSdk sdk;
    private final SchemaCache schemas = new SchemaCache();
    private RequestGate gate;
    private ControlMessage.Port control;
    private ControlChannel channel;
    private TableService tableService;
    private ErrantRecordReporter reporter;
    private boolean tolerate;
    private boolean typed;
    private boolean rescue;
    private boolean metadata;
    private boolean schemaAware;
    private boolean lagEnabled;
    private String dlqTable;
    /** Oversize records skipped (zerobus.dlq.oversize.metadata.only=false); tests read it. */
    private int oversizeSkipped;
    private String cluster;
    private final Map<TopicPartition, Part> parts = new HashMap<>();
    private volatile ConnectException fatal;
    private long enqueues;
    private long firstPutMs;
    private long lastLoggedCommit = -1L;

    public ZerobusSinkTask() {
    }

    @Override
    public String version() {
        return "0.6.0-iteration6";
    }

    @Override
    public void start(Map<String, String> props) {
        configure(props, null, null, null);
    }

    /**
     * Builds the task. Tests pass a sink factory, a catalog and a control port; null means "build from
     * configuration" (the SDK, the SQL warehouse and the Kafka control topic).
     */
    void configure(Map<String, String> props, JsonSink.Factory injectedSinks, TableCatalog injectedCatalog,
                   ControlMessage.Port injectedControl) {
        configure(props, injectedSinks, null, injectedCatalog, injectedControl);
    }

    /** With injectedArrow null, format=auto never picks Arrow (tests that do not use Arrow stay on JSON). */
    void configure(Map<String, String> props, JsonSink.Factory injectedSinks, ArrowSink.Factory injectedArrow,
                   TableCatalog injectedCatalog, ControlMessage.Port injectedControl) {
        cfg = new ZerobusSinkConfig(props);
        format = cfg.format();
        arrowMaxRecords = cfg.arrowBatchMaxRecords();
        worker = "task-" + cfg.taskIndex();
        tolerate = "all".equals(props.get("errors.tolerance"));
        typed = cfg.typed();
        rescue = cfg.rescue();
        metadata = cfg.metadata();
        dlqTable = cfg.dlqTable();
        router = new TableRouter(cfg);
        cluster = clusterName();
        mapper = typed
                ? new TypedRecordMapper(metadata, cfg.metadataFields(), cluster)
                : new EnvelopeRecordMapper(cfg.envelopeHeaders(), metadata, cfg.metadataFields(), cluster);
        schemaAware = cfg.evolution() || cfg.autoCreate();
        if (dlqTable != null && (injectedCatalog != null || injectedSinks == null)) {
            checkDeadLetterTable(injectedCatalog);  // tests that inject sinks without a catalog do not reach a warehouse
        }
        lagEnabled = typed && schemaAware;
        gate = new RequestGate(cfg.requestBackoffMaxMs(), new Random());
        if (context != null) {
            reporter = context.errantRecordReporter();
        }
        if (injectedSinks != null) {
            sinks = injectedSinks;
            arrowSinks = injectedArrow;
        } else {
            sdk = new ZerobusSdk(cfg.endpoint(), cfg.workspaceUrl());
            sinks = new SdkSinkFactory(sdk, cfg.clientId(), cfg.clientSecret(), cfg.maxInflight());
            arrowSinks = new SdkArrowSinkFactory(sdk, cfg.clientId(), cfg.clientSecret());
        }
        if ("arrow".equals(format) && (!typed || rescue)) {
            throw new ConnectException("zerobus.format=arrow needs typed mode without a rescue column");
        }
        if ("arrow".equals(format) && arrowSinks == null) {
            throw new ConnectException("zerobus.format=arrow but no Arrow sink is configured");
        }
        if (arrowSinks != null) {
            allocator = new RootAllocator();
        }
        if (arrowSinks != null && cfg.arrowOpenThreads() > 0) {
            opener = Executors.newFixedThreadPool(cfg.arrowOpenThreads(), namedThreads("zerobus-open-" + worker));
        }
        if (schemaAware && cfg.tableServiceSharded()) {
            TableCatalog catalog = injectedCatalog != null ? injectedCatalog
                    : new SqlClient(cfg.workspaceUrl(), cfg.warehouseId(), cfg.ddlClientId(), cfg.ddlClientSecret());
            tableService = new TableService(catalog, rescue ? cfg.rescueName() : null, m -> control.send(m));
            tableService.label(worker);
        }
        if (schemaAware) {
            if (injectedControl != null) {
                control = injectedControl;
            } else {
                channel = new ControlChannel(cfg.kafkaBootstrap(), cfg.controlTopic(),
                        "zerobus-tsvc-" + cfg.connectorName(), worker, cfg.controlSessionTimeoutMs(),
                        tableService != null ? tableService::handle : msgs -> { }, this::onBroadcast);
                control = channel;
                channel.start();
            }
            for (String t : router.knownTables()) {
                requestSnapshot(t);
            }
        }
        log.info("{} started: mode={} evolution={} auto-create={} rescue={} metadata={} dlq={} service={}", worker,
                cfg.typed() ? "typed" : "envelope", cfg.evolution(), cfg.autoCreate(), rescue, metadata,
                dlqTable == null ? "off" : dlqTable, tableService != null ? "sharded" : "off");
    }

    /**
     * At start (D74): with auto-create off, a missing dead-letter table is a clear error that names the table.
     * Without this check the dead-letter stream open fails with a misleading 401 ("invalid format"). When the
     * table exists without the iteration 5 columns, its rows are sent without them (a warning says so); the
     * connector never alters a dead-letter table. Needs the SQL warehouse (table service sharded); otherwise
     * the table is not checked and a warning names it.
     */
    private void checkDeadLetterTable(TableCatalog injected) {
        TableCatalog catalog = injected;
        if (catalog == null && cfg.tableServiceSharded() && cfg.warehouseId() != null && !cfg.warehouseId().isEmpty()) {
            catalog = new SqlClient(cfg.workspaceUrl(), cfg.warehouseId(), cfg.ddlClientId(), cfg.ddlClientSecret());
        }
        if (catalog == null) {
            if (!cfg.autoCreate()) {
                log.warn("{} zerobus.dlq.table {} is not checked at start: the check needs zerobus.table.service.mode=sharded and "
                        + "zerobus.table.service.warehouse.id. The table must exist before dead letters arrive.", worker, dlqTable);
            }
            return;
        }
        TableCatalog.Snapshot snap = describeDeadLetter(catalog);
        if (!snap.exists) {
            if (!cfg.autoCreate()) {
                throw new ConnectException("dead-letter table " + dlqTable + " does not exist and zerobus.table.auto.create is false: "
                        + "create the table, or set zerobus.table.auto.create=true so the connector creates it");
            }
            return;
        }
        // The table must have every column the enabled options write (iteration 6, D92). The connector never alters it.
        List<String> missing = new ArrayList<>();
        for (String c : TableColumns.deadLetter(cfg).keySet()) {
            if (!snap.columns.containsKey(c)) {
                missing.add(c);
            }
        }
        if (!missing.isEmpty()) {
            throw new ConnectException("dead-letter table " + dlqTable + " is missing column(s) " + missing
                    + " required by the zerobus.dlq.* options: add them with ALTER TABLE (types as in TableColumns.DEAD_LETTER), "
                    + "or create a new table");
        }
    }

    /** Attempts for the dead-letter describe at start (iteration 6): with backoff, then a retriable error. */
    static final int DLQ_CHECK_ATTEMPTS = 5;
    /** First backoff in ms; doubles each attempt (1.5, 3, 6, 12 s: about 22 s in all). Tests lower it. */
    static long dlqCheckBackoffMs = 1500L;

    private TableCatalog.Snapshot describeDeadLetter(TableCatalog catalog) {
        IOException last = null;
        for (int attempt = 1; attempt <= DLQ_CHECK_ATTEMPTS; attempt++) {
            try {
                return catalog.describe(dlqTable);
            } catch (IOException e) {
                last = e;
                log.warn("{} dead-letter table check {}/{} for {} failed: {}", worker, attempt, DLQ_CHECK_ATTEMPTS,
                        dlqTable, e.getMessage());
                if (attempt < DLQ_CHECK_ATTEMPTS) {
                    try {
                        Thread.sleep(dlqCheckBackoffMs << (attempt - 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RetriableException("interrupted while checking the dead-letter table " + dlqTable, ie);
                    }
                }
            }
        }
        throw new RetriableException("could not check the dead-letter table " + dlqTable + " after "
                + DLQ_CHECK_ATTEMPTS + " attempts: " + last.getMessage(), last);
    }

    private String clusterName() {
        if (!metadata) {
            return "";
        }
        String name = cfg.metadataClusterName();
        if (!name.isEmpty()) {
            return name;
        }
        Map<String, Object> props = Collections.singletonMap("bootstrap.servers", cfg.kafkaBootstrap());
        try (AdminClient admin = AdminClient.create(props)) {
            return admin.describeCluster().clusterId().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new ConnectException("could not read the Kafka cluster ID for _kafka.cluster: " + e.getMessage(), e);
        }
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            parts.computeIfAbsent(tp, Part::new);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (firstPutMs == 0L && !records.isEmpty()) {
            firstPutMs = System.currentTimeMillis();
            log.info("{} first put at {} ms (epoch), {} record(s)", worker, firstPutMs, records.size());
        }
        throwIfFatal();
        processFailures();
        for (SinkRecord r : records) {
            accept(r);
        }
        drainAll();
        scheduleWake();
        throwIfFatal();
    }

    /** Routes and maps one record, then queues it for each target table. */
    private void accept(SinkRecord r) {
        Part ps = parts.computeIfAbsent(new TopicPartition(r.originalTopic(), r.originalKafkaPartition()), Part::new);
        if (r.value() == null) {
            if (cfg.nullFails()) {
                throw new ConnectException("null value (tombstone) at " + ps.tp + "@" + r.kafkaOffset());
            }
            ps.tracker.open(r, 0);
            return;
        }
        List<String> targets;
        JsonNode row;
        try {
            targets = router.route(r);
            row = mapper.map(r);
        } catch (TableRouter.RouteException | DataException e) {
            reject(ps, r, e.getMessage());
            return;
        }
        DeliveryTracker.Ticket t = ps.tracker.open(r, targets.size());
        for (String table : targets) {
            enqueue(ps, table, false, new TableStream.Pending(r, row, t));
        }
    }

    /** A record that no rule could place or map: dead-letter table, or tolerated, or fail the task. */
    private void reject(Part ps, SinkRecord r, String reason) {
        ConnectException err = new ConnectException("Zerobus: " + reason);
        if (dlqTable != null) {
            DeliveryTracker.Ticket t = ps.tracker.open(r, 1);
            JsonNode row = deadLetterRow(r, reason);
            if (row != null) {
                enqueue(ps, dlqTable, true, new TableStream.Pending(r, row, t));
            } else {
                skipOversize(ps, r, reason);
                ps.tracker.done(t, true);
            }
            return;
        }
        if (!tolerate) {
            throw err;
        }
        if (reporter != null) {
            reporter.report(r, err);
        }
        ps.tracker.open(r, 0);
    }

    private JsonNode deadLetterRow(SinkRecord r, String reason) {
        return TableColumns.deadLetterRow(cfg, r, cluster, reason);
    }

    /** An oversize record is not written (zerobus.dlq.oversize.metadata.only=false). Logged; its offset still commits. */
    private void skipOversize(Part ps, SinkRecord r, String reason) {
        oversizeSkipped++;
        log.error("{} record not written to the dead-letter table: too large for Zerobus and {}=false ({}@{}; {})",
                worker, ZerobusSinkConfig.DLQ_OVERSIZE_METADATA_ONLY, ps.tp, r.kafkaOffset(), reason);
    }

    int oversizeSkipped() {
        return oversizeSkipped;
    }

    private void enqueue(Part ps, String table, boolean fixed, TableStream.Pending p) {
        enqueues++;
        Queue q = ps.queues.get(table);
        if (q == null) {
            q = new Queue(table, fixed, new TableStream(ps.tp, table, ps.tracker, sinks, arrowSinks));
            ps.queues.put(table, q);
        }
        q.pending.addLast(p);
        if (!ps.paused && q.pending.size() > cfg.bufferMax()) {
            context.pause(ps.tp);
            ps.paused = true;
            log.info("{} paused {}: {} records waiting for {}", worker, ps.tp, q.pending.size(), q.table);
        }
    }

    private void drainAll() {
        // A failed record is moved to the dead-letter queue during a pass, which may be new: repeat until nothing is added.
        long before;
        do {
            before = enqueues;
            for (Part ps : new ArrayList<>(parts.values())) {
                for (Queue q : new ArrayList<>(ps.queues.values())) {
                    drain(ps, q);
                }
            }
        } while (enqueues != before);
        resumeReady();
    }

    /** Sends a queue's head records in order until one must wait, or the queue is empty. */
    private void drain(Part ps, Queue q) {
        if (q.lagSince != 0 && q.stream.ackCount() > q.lagAckMark) {
            q.lagSince = 0;  // acknowledgements moved on: the lag window is over
        }
        while (!q.pending.isEmpty()) {
            if (q.isolated && q.stream.unackedCount() > 0) {
                // Isolation: the record in flight must be acknowledged (or rejected) before the next one is sent.
                settleIsolated(ps, q);
                if (q.stream.unackedCount() > 0) {
                    return;
                }
                continue;
            }
            TableStream.Pending p = q.pending.peekFirst();
            try {
                if (!ready(q, p)) {
                    return;
                }
                if (!ensureStream(ps, q, p, wantArrow(q, p))) {
                    return;  // requeued for schema lag; the head stays in the queue
                }
                if (q.pending.peekFirst() != p) {
                    continue;  // a reopen requeued unacknowledged records ahead of p: they go first
                }
                logWaitDone(ps, q, p);
                if (q.stream.isArrow()) {
                    if (!sendBatch(ps, q)) {
                        return;
                    }
                    continue;
                }
                if (!sendJson(q, p)) {
                    return;
                }
            } catch (RecordFailure f) {
                // p, not the head: a reopen may have requeued unacknowledged records ahead of p.
                q.pending.remove(p);
                failTarget(ps, q, p, f.getMessage());
                continue;
            }
            q.pending.pollFirst();
        }
    }

    /** Format for a record's stream: Arrow when the mode allows it and the record's types have an Arrow mapping. */
    private boolean wantArrow(Queue q, TableStream.Pending p) {
        if (arrowSinks == null || "json".equals(format) || q.fixed) {
            return false;  // the dead-letter table has a fixed schema and its rows are JSON objects
        }
        if (!typed || rescue || !(p.record.value() instanceof Struct)) {
            if ("arrow".equals(format)) {
                throw new RecordFailure("zerobus.format=arrow needs typed records without a rescue column");
            }
            return false;  // envelope, rescue and schemaless values stay on JSON
        }
        boolean ok = ArrowTypes.eligible(((Struct) p.record.value()).schema());
        if (!ok && "arrow".equals(format)) {
            throw new RecordFailure("zerobus.format=arrow: a column type of this record has no Arrow mapping");
        }
        return ok;
    }

    private Schema arrowSchemaFor(TableStream.Pending p) {
        return ArrowTypes.schemaFor(((Struct) p.record.value()).schema(), metadata ? cfg.metadataFields() : null);
    }

    /**
     * Makes the head's stream usable: reopens it when its schema or format no longer fits the head, then opens
     * one if none is open. Arrow opens (and Arrow reopens) run on the open pool when one is configured: the
     * queue waits for them and the task thread stays free for the other queues. Returns false when the head
     * was requeued for schema lag, or while an open is in flight.
     */
    private boolean ensureStream(Part ps, Queue q, TableStream.Pending p, boolean arrow) {
        if (q.opening != null) {
            return finishOpening(q);
        }
        SchemaCache.Snapshot snap = schemaAware ? schemas.get(q.table) : null;
        long version = snap == null ? 0L : snap.version;
        long now = System.currentTimeMillis();
        String why = null;
        if (q.stream.isOpen() && snap != null && q.stream.openedVersion() < snap.version
                && now - snap.appliedAtMs >= SETTLE_MS) {
            // A stream keeps the schema it opened with, so one opened earlier would route a new field to rescue
            // (or reject it) for its whole life. Flush, then take back anything unacknowledged and reopen.
            why = "opened under schema v" + q.stream.openedVersion() + ", now v" + snap.version;
        } else if (q.stream.isOpen() && q.stream.isArrow() && (!arrow || !ArrowRows.fits(p.row, q.stream.schema()))) {
            why = "the next record does not fit the Arrow schema of this stream";
        }
        if (why != null) {
            log.info("{} reopening stream for {}: {}", worker, ps.tp, why);
            if (arrow && opener != null) {
                startOpening(q, version, arrowSchemaFor(p));
                return false;
            }
            try {
                q.stream.flush(Duration.ofMillis(cfg.flushTimeoutMs()));
            } catch (ZerobusException e) {
                throw new RetriableException("could not flush before reopening the stream for " + q.table, e);
            }
            requeueFront(q, q.stream.takeUnackedAndClose());
        }
        if (!q.stream.isOpen()) {
            if (arrow && opener != null) {
                startOpening(q, version, arrowSchemaFor(p));
                return false;
            }
            try {
                q.stream.open(version, arrow ? arrowSchemaFor(p) : null);
            } catch (ConnectException e) {
                return openRefused(q, version, arrow, e);
            }
        }
        return true;
    }

    /** Submits the flush-and-open of an Arrow stream to the pool. The queue sends nothing until it is attached. */
    private void startOpening(Queue q, long version, Schema schema) {
        final long flushMs = cfg.flushTimeoutMs();
        final TableStream stream = q.stream;
        final String name = q.table;
        final TopicPartition tp = q.stream.tp;
        Callable<ArrowSink> job = () -> {
            long t0 = System.currentTimeMillis();
            ArrowSink sink = stream.flushAndOpenArrow(Duration.ofMillis(flushMs), schema);
            log.info("{} open job for {} took {} ms on {}", worker, tp, System.currentTimeMillis() - t0, Thread.currentThread().getName());
            return sink;
        };
        q.opening = new Opening(version, schema, opener.submit(job));
    }

    /**
     * Completes an open from the pool. Takes back what the old stream still holds (the flush inside the job
     * acknowledged what it could), then attaches the new Arrow stream. Returns false while the open runs.
     */
    private boolean finishOpening(Queue q) {
        Opening o = q.opening;
        if (!o.future.isDone()) {
            return false;
        }
        q.opening = null;
        requeueFront(q, q.stream.takeUnackedAndClose());
        ArrowSink sink;
        try {
            sink = o.future.get();
        } catch (ExecutionException e) {
            Throwable c = e.getCause();
            if (c instanceof ConnectException) {
                return openRefused(q, o.version, true, (ConnectException) c);
            }
            throw new RetriableException("could not open the Arrow stream for " + q.table + ": " + c, c);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetriableException("interrupted while opening the Arrow stream for " + q.table, e);
        }
        q.stream.attachArrow(sink, o.version, o.schema);
        return true;
    }

    /**
     * The server refused an open. An Arrow schema the table does not match is refused before any record is sent:
     * auto falls back to JSON for this stream (JSON judges each record), arrow dead-letters the head with the
     * server's text. A table the table service has just created may not be visible yet ("not found"): retry on
     * the lag schedule, bounded by the max wait, instead of killing the task. Returns true when the stream is open.
     */
    private boolean openRefused(Queue q, long version, boolean arrow, ConnectException e) {
        String m = e.getMessage() == null ? "" : e.getMessage();
        if (arrow && m.contains("does not match the target table")) {
            if ("arrow".equals(format)) {
                throw new RecordFailure("rejected by Zerobus: " + m);
            }
            log.warn("{} {} refused the Arrow schema of this record; this stream uses JSON: {}", worker,
                    q.table, m);
            q.stream.open(version, null);
            return true;
        }
        if (schemaAware && m.toLowerCase().contains("not found")) {
            return lagOrFail(q, "stream open", m);
        }
        throw e;
    }

    private static void requeueFront(Queue q, List<TableStream.Pending> back) {
        for (int i = back.size() - 1; i >= 0; i--) {
            q.pending.addFirst(back.get(i));
        }
    }

    /** The pause ends at the first record sent after a satisfied wait: log it with the time from the change. */
    private void logWaitDone(Part ps, Queue q, TableStream.Pending p) {
        if (q.waitedSince != 0L) {
            log.info("{} {} sent first record {} ms after {} on {}", worker, ps(p),
                    System.currentTimeMillis() - q.waitedSince, q.waitedDesc, q.table);
            q.waitedSince = 0L;
            q.waitedDesc = null;
        }
    }

    private void scheduleWake() {
        if (context == null) {
            return;
        }
        for (Part ps : parts.values()) {
            if (ps.paused) {
                context.timeout(WAKE_MS);
                return;
            }
            for (Queue q : ps.queues.values()) {
                if (!q.pending.isEmpty() || q.wait != null || q.opening != null) {
                    context.timeout(WAKE_MS);
                    return;
                }
            }
        }
    }

    private static ThreadFactory namedThreads(String prefix) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    /** Sends the head record on a JSON stream. Returns false when it was requeued. Throws RecordFailure. */
    private boolean sendJson(Queue q, TableStream.Pending p) {
        try {
            q.stream.ingest(p);
            return true;
        } catch (NonRetriableException e) {
            if (lagEnabled && isSchemaLag(e.getMessage())) {
                return lagOrFail(q, "rejected at send", e.getMessage());
            }
            if (!q.isolated && q.stream.unackedCount() > 0) {
                // Other records are in flight, so this rejection cannot be tied to this record alone.
                requeueAndIsolate(q, "synchronous rejection with records in flight");
                return false;
            }
            throw new RecordFailure("rejected by Zerobus: " + e.getMessage());
        } catch (ZerobusException e) {
            if (lagEnabled && !q.stream.isOpen()) {
                return lagOrFail(q, "stream closed by the server", e.getMessage());
            }
            throw new RetriableException("zerobus send failed for " + q.table + ": " + e.getMessage(), e);
        }
    }

    /** Index of the first row that cannot be converted on its own, or -1 when every row converts. Error path only. */
    private static int firstUnconvertible(Schema schema, List<JsonNode> rows) {
        for (int i = 0; i < rows.size(); i++) {
            try (BufferAllocator a = new RootAllocator();
                 VectorSchemaRoot one = ArrowRows.build(a, schema, Collections.singletonList(rows.get(i)))) {
                // converts alone
            } catch (DataException e) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Sends the ready records at the head as one Arrow batch (up to zerobus.arrow.batch.max.records; one record
     * while isolated). Records stay queued until the SDK accepts the batch, so a failed send leaves them in order.
     * Returns false when the batch was held back (requeued for lag, or isolated).
     */
    private boolean sendBatch(Part ps, Queue q) {
        Schema schema = q.stream.schema();
        int max = q.isolated ? 1 : arrowMaxRecords;
        List<TableStream.Pending> batch = new ArrayList<>();
        Iterator<TableStream.Pending> it = q.pending.iterator();
        while (it.hasNext() && batch.size() < max) {
            TableStream.Pending p = it.next();
            if (!batch.isEmpty()) {
                try {
                    if (!ready(q, p) || !wantArrow(q, p) || !ArrowRows.fits(p.row, schema)) {
                        break;
                    }
                } catch (RecordFailure f) {
                    break;  // handled when it reaches the head
                }
            }
            batch.add(p);
        }
        List<JsonNode> rows = new ArrayList<>(batch.size());
        for (TableStream.Pending p : batch) {
            rows.add(p.row);
        }
        VectorSchemaRoot root;
        try {
            root = ArrowRows.build(allocator, schema, rows);
        } catch (DataException e) {
            // A row that cannot be converted is a record error, found here before the server sees it. Rows before it
            // are sent as a shorter batch; the bad row goes to the failure path when it is the head.
            int bad = firstUnconvertible(schema, rows);
            if (bad <= 0) {
                q.pending.pollFirst();
                failTarget(ps, q, batch.get(0), e.getMessage());
                return true;
            }
            while (batch.size() > bad) {
                batch.remove(batch.size() - 1);
            }
            rows = rows.subList(0, bad);
            root = ArrowRows.build(allocator, schema, rows);
        }
        try {
            q.stream.ingestBatch(batch, root);
        } catch (NonRetriableException e) {
            if (lagEnabled && isSchemaLag(e.getMessage())) {
                return lagOrFail(q, "rejected at send", e.getMessage());
            }
            if (batch.size() == 1 && q.stream.unackedCount() == 0) {
                q.pending.pollFirst();
                failTarget(ps, q, batch.get(0), "rejected by Zerobus: " + e.getMessage());
                return true;
            }
            requeueAndIsolate(q, "synchronous rejection of a batch of " + batch.size());
            return false;
        } catch (ZerobusException e) {
            if (lagEnabled && !q.stream.isOpen()) {
                return lagOrFail(q, "stream closed by the server", e.getMessage());
            }
            throw new RetriableException("zerobus send failed for " + q.table + ": " + e.getMessage(), e);
        } finally {
            root.close();
        }
        for (int i = 0; i < batch.size(); i++) {
            q.pending.pollFirst();
        }
        return true;
    }

    private static String ps(TableStream.Pending p) {
        return "partition " + p.record.originalTopic() + "-" + p.record.originalKafkaPartition();
    }

    /** Whether the head may be sent now. Sets q.wait and returns false when it must wait. Throws RecordFailure. */
    private boolean ready(Queue q, TableStream.Pending p) {
        long now = System.currentTimeMillis();
        if (q.wait != null) {
            Wait w = q.wait;
            if (w.kind == Kind.LAG) {
                if (now < w.retryAt) {
                    return false;
                }
                q.wait = null;
            } else if (waitSatisfied(q, w)) {
                q.wait = null;
                q.waitedSince = w.since;
                q.waitedDesc = describe(w);
                log.info("{} {} waited {} ms on {} ({})", worker, ps(p), now - w.since, q.table, describe(w));
            } else if (now - w.since >= cfg.changeMaxWaitMs()) {
                q.wait = null;
                throw new RecordFailure(describe(w) + " on " + q.table + " not satisfied within " + cfg.changeMaxWaitMs() + " ms");
            } else {
                resend(q, w);
                return false;
            }
        }
        if (!schemaAware) {
            return true;
        }
        SchemaCache.Snapshot snap = schemas.get(q.table);
        if (snap == null) {
            requestSnapshot(q.table);
            q.wait = new Wait(Kind.SCHEMA, now, now, Collections.emptySet(), Collections.emptyMap());
            return false;
        }
        if (!snap.exists) {
            if (!cfg.autoCreate()) {
                throw new RecordFailure("table " + q.table + " does not exist and zerobus.table.auto.create is false");
            }
            Map<String, String> cols = createColumns(q, p);
            requestCreate(q.table, cols, q.table);
            q.wait = new Wait(Kind.TABLE, now, now, Collections.emptySet(), cols);
            return false;
        }
        if (q.fixed || !typed || !cfg.evolution() || !(p.record.value() instanceof Struct)) {
            return true;
        }
        return diff(q, p, snap, now);
    }

    private Map<String, String> createColumns(Queue q, TableStream.Pending p) {
        if (q.fixed) {
            return TableColumns.deadLetter(cfg);
        }
        try {
            return TableColumns.forRecord(cfg, p.record, typed);
        } catch (TableRouter.RouteException e) {
            throw new RecordFailure(e.getMessage());
        }
    }

    /** Compares the record with the cached table. Returns true when it can be sent now. */
    private boolean diff(Queue q, TableStream.Pending p, SchemaCache.Snapshot snap, long now) {
        Set<String> missing = new LinkedHashSet<>();
        Map<String, String> proposal = new LinkedHashMap<>();
        List<String> mismatches = new ArrayList<>();
        for (Field f : ((Struct) p.record.value()).schema().fields()) {
            String ddl = DeltaTypes.ddl(f.schema());
            String have = snap.columns.get(f.name());
            if (have == null) {
                missing.add(f.name());
                proposal.put(f.name(), ddl);
            } else if (!DeltaTypes.matches(ddl, have)) {
                mismatches.add(f.name() + " is " + have + " in the table, record has " + ddl);
            }
        }
        if (metadata && !snap.columns.containsKey("_kafka")) {
            missing.add("_kafka");
            proposal.put("_kafka", RecordMapper.Common.metadataDdl(cfg.metadataFields()));
        }
        if (!mismatches.isEmpty() && !rescue) {
            throw new RecordFailure("type change in " + q.table + ": " + String.join("; ", mismatches));
        }
        if (missing.isEmpty()) {
            return true;
        }
        requestChange(q.table, proposal);
        // With a rescue column, a missing data field is captured until its column exists, so the record goes on.
        // The _kafka struct is never rescued, so it always waits.
        boolean mustWait = !rescue || missing.contains("_kafka");
        if (mustWait) {
            q.wait = new Wait(Kind.SCHEMA, now, now, missing, proposal);
            return false;
        }
        return true;
    }

    private boolean waitSatisfied(Queue q, Wait w) {
        SchemaCache.Snapshot snap = schemas.get(q.table);
        if (w.kind == Kind.TABLE) {
            return snap != null && snap.exists;
        }
        return snap != null && snap.columns.keySet().containsAll(w.fields);
    }

    private String describe(Wait w) {
        switch (w.kind) {
            case TABLE: return "table creation";
            case SCHEMA: return w.fields.isEmpty() ? "schema snapshot" : "schema change " + w.fields;
            default: return "schema-lag retry";
        }
    }

    /** Repeats a request through the gate (it is sent only when the backoff allows). */
    private void resend(Queue q, Wait w) {
        switch (w.kind) {
            case SCHEMA:
                if (w.fields.isEmpty()) {
                    requestSnapshot(q.table);
                } else {
                    requestChange(q.table, w.proposal);
                }
                break;
            case TABLE:
                requestCreate(q.table, w.proposal, q.table);
                break;
            default:
                break;
        }
    }

    private boolean lagOrFail(Queue q, String why, String message) {
        if (lagExpired(q)) {
            throw new RecordFailure("schema lag on " + q.table + " not resolved within " + cfg.changeMaxWaitMs()
                    + " ms: " + message);
        }
        requeueForLag(q, why);
        return false;
    }

    private boolean lagExpired(Queue q) {
        return q.lagSince != 0 && System.currentTimeMillis() - q.lagSince >= cfg.changeMaxWaitMs();
    }

    /** Takes back a stream's unacknowledged records to the front of the queue and waits before resending. */
    private void requeueForLag(Queue q, String why) {
        List<TableStream.Pending> back = q.stream.takeUnackedAndClose();
        for (int i = back.size() - 1; i >= 0; i--) {
            q.pending.addFirst(back.get(i));
        }
        long now = System.currentTimeMillis();
        if (q.lagSince == 0) {
            q.lagSince = now;
        }
        q.lagAckMark = q.stream.ackCount();
        q.wait = new Wait(Kind.LAG, now, now + LAG_RETRY_MS, Collections.emptySet(), Collections.emptyMap());
        log.info("{} schema lag on {} ({}): requeued {} record(s), retry in {} ms", worker, q.table, why, back.size(),
                LAG_RETRY_MS);
    }

    static boolean isSchemaLag(String message) {
        return message != null && message.contains("unrecognized field name");
    }

    /** A record failed for one target: move that unit to the dead-letter table, tolerate it, or fail the task. */
    private void failTarget(Part ps, Queue q, TableStream.Pending p, String message) {
        ConnectException err = new ConnectException("Zerobus: " + message + " (table " + q.table + ", "
                + ps.tp + "@" + p.record.kafkaOffset() + ")");
        if (dlqTable != null && !q.fixed) {
            JsonNode row = deadLetterRow(p.record, err.getMessage());
            if (row != null) {
                enqueue(ps, dlqTable, true, new TableStream.Pending(p.record, row, p.ticket));
                return;
            }
            skipOversize(ps, p.record, err.getMessage());
            ps.tracker.done(p.ticket, true);
            return;
        }
        if (!tolerate) {
            fatal = err;
            return;
        }
        if (reporter != null && !p.ticket.reported) {
            reporter.report(p.record, err);
            p.ticket.reported = true;
        }
        ps.tracker.done(p.ticket, true);
    }

    /** Server rejections and closed streams that arrived since the last call. */
    private void processFailures() {
        for (Part ps : new ArrayList<>(parts.values())) {
            for (Queue q : new ArrayList<>(ps.queues.values())) {
                handleFailures(ps, q);
            }
        }
    }

    /**
     * Handles one queue's rejections. A rejection names one record only when that record is the only one
     * unacknowledged on the stream. Otherwise (a rejection that names several records, or a stream that
     * closed with records in flight) every unacknowledged record is requeued in order and the queue isolates:
     * one record in flight at a time, so the next rejection is exact. Returns true when anything was handled.
     */
    private boolean handleFailures(Part ps, Queue q) {
        List<TableStream.Entry> failed = q.stream.takeFailures();
        int unacked = q.stream.unackedCount();
        boolean closedWithWork = !q.stream.isOpen() && unacked > 0;
        if (failed.isEmpty() && !closedWithWork) {
            return false;
        }
        if (lagEnabled && !lagExpired(q)) {
            for (TableStream.Entry e : failed) {
                if (isSchemaLag(e.error)) {
                    requeueForLag(q, "rejected by the server");  // takes back every unacknowledged record
                    return true;
                }
            }
        }
        if (failed.size() == 1 && unacked == 1) {
            q.isolatedRun = 0;
            attribute(ps, q, failed.get(0));
            return true;
        }
        requeueAndIsolate(q, failed.isEmpty() ? "stream closed with records in flight"
                : "a rejection named " + failed.size() + " records");
        return true;
    }

    /** One record was rejected by itself: move it to the dead-letter table, tolerate it, or fail the task. */
    private void attribute(Part ps, Queue q, TableStream.Entry e) {
        String message = "rejected by Zerobus: " + e.error + " (table " + q.table + ", " + ps.tp
                + "@" + e.pending.record.kafkaOffset() + ")";
        if (dlqTable != null && !q.fixed) {
            q.stream.releaseFailure(e, false);
            JsonNode row = deadLetterRow(e.pending.record, message);
            if (row != null) {
                enqueue(ps, dlqTable, true, new TableStream.Pending(e.pending.record, row, e.pending.ticket));
                return;
            }
            skipOversize(ps, e.pending.record, message);
            ps.tracker.done(e.pending.ticket, true);
            return;
        }
        if (!tolerate) {
            fatal = new ConnectException(message);
            return;
        }
        if (reporter != null && !e.pending.ticket.reported) {
            reporter.report(e.pending.record, new ConnectException(message));
            e.pending.ticket.reported = true;
        }
        q.stream.releaseFailure(e, true);
    }

    /** Takes back a queue's unacknowledged records (in send order, to the front) and isolates the queue. */
    private void requeueAndIsolate(Queue q, String why) {
        List<TableStream.Pending> back = q.stream.takeUnackedAndClose();
        for (int i = back.size() - 1; i >= 0; i--) {
            q.pending.addFirst(back.get(i));
        }
        q.isolated = true;
        q.isolatedRun = 0;
        log.info("{} isolating {}: {} ({} record(s) requeued)", worker, q.table, why, back.size());
    }

    /**
     * Isolation: flushes the one record in flight (an SDK call, so no monitor is held), then handles its
     * outcome. A clean run of ISOLATION_CLEAR_AFTER acknowledgements returns the queue to pipelined sends.
     */
    private void settleIsolated(Part ps, Queue q) {
        try {
            q.stream.flush(Duration.ofMillis(cfg.flushTimeoutMs()));
        } catch (ZerobusException e) {
            log.warn("flush while isolating {} failed: {}", q.table, e.getMessage());
        }
        if (handleFailures(ps, q)) {
            return;
        }
        if (q.stream.unackedCount() == 0 && ++q.isolatedRun >= ISOLATION_CLEAR_AFTER) {
            q.isolated = false;
            q.isolatedRun = 0;
            log.info("{} {} left isolation after {} clean acknowledgements", worker, q.table, ISOLATION_CLEAR_AFTER);
        }
    }

    /** Resumes paused partitions whose queues have drained below half the cap. */
    private void resumeReady() {
        for (Part ps : parts.values()) {
            if (!ps.paused) {
                continue;
            }
            boolean drained = true;
            for (Queue q : ps.queues.values()) {
                if (q.pending.size() > cfg.bufferMax() / 2) {
                    drained = false;
                }
            }
            if (drained) {
                context.resume(ps.tp);
                ps.paused = false;
                log.info("{} resumed {} (backlog drained)", worker, ps.tp);
            }
        }
    }

    /** Requests go out through the gate, so one key is in flight at a time across all tasks. */
    private void requestSnapshot(String table) {
        if (control == null) {
            return;
        }
        if (gate.tryRequest(RequestGate.snapshotKey(table), table, Collections.emptySet(), System.currentTimeMillis())) {
            control.send(ControlMessage.snapshotRequest(table, worker));
        }
    }

    private void requestChange(String table, Map<String, String> proposal) {
        if (control == null || proposal.isEmpty()) {
            return;
        }
        if (gate.tryRequest(RequestGate.changeKey(table, proposal.keySet()), table, proposal.keySet(),
                System.currentTimeMillis())) {
            control.send(ControlMessage.changeRequest(table, worker, proposal));
            log.info("{} requested {} column(s) on {}: {}", worker, proposal.size(), table, proposal.keySet());
        }
    }

    private void requestCreate(String table, Map<String, String> cols, String forTable) {
        if (control == null) {
            return;
        }
        boolean rescueTable = rescue && !table.equals(dlqTable);
        if (gate.tryRequest(RequestGate.createKey(table), table, cols.keySet(), System.currentTimeMillis())) {
            control.send(ControlMessage.createRequest(table, worker, cols, rescueTable));
            log.info("{} requested creation of {} ({} column(s))", worker, table, cols.size());
        }
    }

    /** Broadcast reader callback (control thread): schema state and request observations. */
    void onBroadcast(List<ControlMessage> msgs) {
        long now = System.currentTimeMillis();
        for (ControlMessage m : msgs) {
            switch (m.type) {
                case ControlMessage.SCHEMA_CHANGED:
                    schemas.apply(m.table, m.exists, m.version, m.columns);
                    gate.settled(m.table, m.exists, m.columns.keySet());
                    break;
                case ControlMessage.CHANGE_REQUEST:
                    gate.observed(RequestGate.changeKey(m.table, m.columns.keySet()), m.table, m.columns.keySet(), now);
                    break;
                case ControlMessage.CREATE_REQUEST:
                    gate.observed(RequestGate.createKey(m.table), m.table, m.columns.keySet(), now);
                    break;
                case ControlMessage.SNAPSHOT_REQUEST:
                    gate.observed(RequestGate.snapshotKey(m.table), m.table, Collections.emptySet(), now);
                    break;
                default:
                    break;
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // Nothing is buffered outside the queues and the streams.
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // Rounds: handle rejections, send what is ready, flush. A rejection that requeues records (isolation)
        // needs another round to send them, so repeat while work remains, up to MAX_COMMIT_ROUNDS.
        for (int round = 0; round < MAX_COMMIT_ROUNDS; round++) {
            processFailures();
            drainAll();
            flushAll();
            if (!hasWork()) {
                break;
            }
        }
        processFailures();
        throwIfFatal();
        Map<TopicPartition, OffsetAndMetadata> out = new HashMap<>();
        long total = 0L;
        for (Part ps : parts.values()) {
            long next = ps.tracker.committableNext();
            if (next >= 0) {
                out.put(ps.tp, new OffsetAndMetadata(next));
                total += next;
            }
        }
        scheduleWake();
        if (!out.isEmpty() && total != lastLoggedCommit) {
            lastLoggedCommit = total;
            log.info("{} committed {} partition(s) at {} ms (epoch), committable offset sum {}", worker, out.size(),
                    System.currentTimeMillis(), total);
        }
        return out;
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            Part ps = parts.remove(tp);
            if (ps != null) {
                closeQueues(ps);
            }
        }
    }

    private void closeQueues(Part ps) {
        for (Queue q : ps.queues.values()) {
            if (q.opening != null) {
                try {
                    q.opening.future.get(ABANDON_OPEN_MS, TimeUnit.MILLISECONDS).close();
                } catch (Exception e) {
                    q.opening.future.cancel(true);
                    log.warn("abandoned an Arrow open for {}: {}", q.table, e.toString());
                }
                q.opening = null;
            }
            try {
                q.stream.flush(Duration.ofMillis(cfg.flushTimeoutMs()));
            } catch (ZerobusException e) {
                log.warn("flush on close failed for {}: {}", q.table, e.getMessage());
            }
            q.stream.close();
            // Unsent records are dropped here: the new owner reads them from the committed offset.
        }
    }

    @Override
    public void stop() {
        for (Part ps : parts.values()) {
            closeQueues(ps);
        }
        parts.clear();
        if (opener != null) {
            opener.shutdownNow();
            opener = null;
        }
        if (channel != null) {
            channel.close();
        }
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
        if (sdk != null) {
            sdk.close();
        }
    }

    private void throwIfFatal() {
        if (fatal != null) {
            throw fatal;
        }
    }

    private void flushAll() {
        for (Part ps : parts.values()) {
            for (Queue q : ps.queues.values()) {
                if (q.opening != null) {
                    continue;  // the pool is flushing this stream; a second flush from here would race it
                }
                if (q.stream.unackedCount() == 0) {
                    continue;  // nothing in flight: no acknowledgement to wait for, so no round trip (iteration 5, D84)
                }
                try {
                    q.stream.flush(Duration.ofMillis(cfg.flushTimeoutMs()));
                } catch (ZerobusException e) {
                    log.warn("flush before commit failed for {}: {}", q.table, e.getMessage());
                }
            }
        }
    }

    /**
     * True while a queue has records in flight (their outcome is not known yet) or records it can send now.
     * A queue waiting for a schema change or a retry is not work for another round: it is re-checked on the
     * next put or commit, and a round here would only flush the other streams again.
     */
    private boolean hasWork() {
        for (Part ps : parts.values()) {
            for (Queue q : ps.queues.values()) {
                if (q.stream.unackedCount() > 0 || (!q.pending.isEmpty() && q.wait == null && q.opening == null)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Test hooks: current state of a partition (package-private). */
    int pendingFor(TopicPartition tp, String table) {
        Part ps = parts.get(tp);
        if (ps == null || ps.queues.get(table) == null) {
            return 0;
        }
        return ps.queues.get(table).pending.size();
    }

    boolean isolatedFor(TopicPartition tp, String table) {
        Part ps = parts.get(tp);
        return ps != null && ps.queues.get(table) != null && ps.queues.get(table).isolated;
    }

    boolean pausedFor(TopicPartition tp) {
        Part ps = parts.get(tp);
        return ps != null && ps.paused;
    }

    int committableFor(TopicPartition tp) {
        Part ps = parts.get(tp);
        return ps == null ? -1 : (int) ps.tracker.committableNext();
    }

    TableService tableService() {
        return tableService;
    }

    RequestGate gate() {
        return gate;
    }
}
