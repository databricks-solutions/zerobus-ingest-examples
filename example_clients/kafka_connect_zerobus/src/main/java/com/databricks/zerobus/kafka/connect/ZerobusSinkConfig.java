package com.databricks.zerobus.kafka.connect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/** Typed view over the connector configuration. Routing rules are validated here, once, before any task starts. */
public final class ZerobusSinkConfig extends AbstractConfig {

    public static final String ENDPOINT = "zerobus.endpoint";
    public static final String WORKSPACE_URL = "zerobus.workspace.url";
    public static final String CLIENT_ID = "zerobus.client.id";
    public static final String CLIENT_SECRET = "zerobus.client.secret";
    /** Single target table, or the default for topics that match no topic2table rule. */
    public static final String TABLE = "zerobus.table";
    /** Ordered rules: topic-regex=>table-template, separated by ';'. $1.. refer to regex groups. */
    public static final String TOPIC2TABLE = "zerobus.topic2table.map";
    /** Static fan-out: the tables a record may be written to. */
    public static final String TABLES = "zerobus.tables";
    public static final String TABLES_ROUTE_FIELD = "zerobus.tables.route.field";
    /** Per-table route regex: zerobus.tables.<catalog.schema.table>.route.regex */
    public static final String TABLE_ROUTE_REGEX_PREFIX = "zerobus.tables.";
    public static final String TABLE_ROUTE_REGEX_SUFFIX = ".route.regex";
    public static final String DYNAMIC_ENABLED = "zerobus.tables.dynamic.enabled";
    /** The only catalog.schema that dynamic routing may write to. */
    public static final String DYNAMIC_NAMESPACE = "zerobus.tables.dynamic.namespace";
    public static final String AUTO_CREATE = "zerobus.table.auto.create";
    public static final String DLQ_TABLE = "zerobus.dlq.table";
    public static final String DLQ_INCLUDE_METADATA = "zerobus.dlq.include.metadata";
    public static final String DLQ_INCLUDE_KEY = "zerobus.dlq.include.key";
    public static final String DLQ_OVERSIZE_METADATA_ONLY = "zerobus.dlq.oversize.metadata.only";
    public static final String MODE = "zerobus.mode";
    public static final String RESCUE_ENABLED = "zerobus.rescue.column.enabled";
    public static final String RESCUE_NAME = "zerobus.rescue.column.name";
    public static final String EVOLUTION_ENABLED = "zerobus.schema.evolution.enabled";
    public static final String CHANGE_MAX_WAIT_MS = "zerobus.schema.change.max.wait.ms";
    public static final String BUFFER_MAX = "zerobus.schema.change.max.buffered.records";
    public static final String REQUEST_BACKOFF_MAX_MS = "zerobus.schema.request.backoff.max.ms";
    public static final String TABLE_SERVICE_MODE = "zerobus.table.service.mode";
    public static final String WAREHOUSE_ID = "zerobus.table.service.warehouse.id";
    public static final String DDL_CLIENT_ID = "zerobus.table.service.client.id";
    public static final String DDL_CLIENT_SECRET = "zerobus.table.service.client.secret";
    public static final String CONTROL_TOPIC = "zerobus.control.topic";
    public static final String CONTROL_SESSION_TIMEOUT_MS = "zerobus.control.session.timeout.ms";
    public static final String KAFKA_BOOTSTRAP = "zerobus.kafka.bootstrap.servers";
    public static final String METADATA_ENABLED = "zerobus.metadata.enabled";
    public static final String METADATA_FIELDS = "zerobus.metadata.fields";
    public static final String METADATA_CLUSTER_NAME = "zerobus.metadata.cluster.name";
    public static final String ENVELOPE_HEADERS_ENABLED = "zerobus.envelope.headers.enabled";
    public static final String MAX_INFLIGHT = "zerobus.max.inflight.records";
    public static final String FLUSH_TIMEOUT_MS = "zerobus.flush.timeout.ms";
    /** Stream format: auto (Arrow for typed tables it can map, else JSON), arrow, or json. */
    public static final String FORMAT = "zerobus.format";
    public static final String ARROW_BATCH_MAX_RECORDS = "zerobus.arrow.batch.max.records";
    /** Threads per task that flush and open Arrow streams for a reopen (0 = on the task thread). */
    public static final String ARROW_OPEN_THREADS = "zerobus.arrow.open.threads";
    public static final String FORMAT_AUTO = "auto";
    public static final String FORMAT_ARROW = "arrow";
    public static final String FORMAT_JSON = "json";
    public static final String ON_NULL_VALUES = "behavior.on.null.values";
    /** Internal: set by the connector per task (0..n-1) and per connector name (control group id). */
    public static final String TASK_INDEX = "zerobus.task.index";
    public static final String CONNECTOR_NAME = "zerobus.connector.name";

    public static final String MODE_TYPED = "typed";
    public static final String MODE_ENVELOPE = "envelope";
    public static final String SERVICE_SHARDED = "sharded";
    public static final String SERVICE_OFF = "off";
    public static final List<String> METADATA_FIELD_NAMES =
            Arrays.asList("cluster", "topic", "partition", "offset", "key", "timestamp");

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9_]+");
    private static final Pattern FQ_TABLE = Pattern.compile("[A-Za-z0-9_]+\\.[A-Za-z0-9_]+\\.[A-Za-z0-9_]+");
    private static final Pattern NAMESPACE = Pattern.compile("[A-Za-z0-9_]+\\.[A-Za-z0-9_]+");

    public static ConfigDef configDef() {
        return new ConfigDef()
                .define(ENDPOINT, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "Zerobus Ingest gRPC endpoint, https://<workspace-id>.zerobus.<region>.<domain>")
                .define(WORKSPACE_URL, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "Workspace URL used for OAuth and Unity Catalog calls, https://<workspace-url>")
                .define(CLIENT_ID, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "OAuth client ID of the service principal that writes the tables")
                .define(CLIENT_SECRET, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        "OAuth client secret of the service principal")
                .define(TABLE, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                        "Target table catalog.schema.table for every topic (or the default when topic2table.map has no match)")
                .define(TOPIC2TABLE, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                        "Topic routing rules: topic-regex=>catalog.schema.table_$1, separated by ';'. First full match wins")
                .define(TABLES, ConfigDef.Type.LIST, "", ConfigDef.Importance.HIGH,
                        "Static fan-out: comma-separated catalog.schema.table names a record may be written to")
                .define(TABLES_ROUTE_FIELD, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                        "Record field whose value is matched against each table's route.regex (or names the table when dynamic)")
                .define(DYNAMIC_ENABLED, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
                        "Dynamic routing: the route field's value is the table name, inside zerobus.tables.dynamic.namespace")
                .define(DYNAMIC_NAMESPACE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                        "catalog.schema that dynamic routing may write to (the only allowed namespace)")
                .define(AUTO_CREATE, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
                        "Create missing tables (CREATE TABLE IF NOT EXISTS) from the record schema, through the table service")
                .define(DLQ_TABLE, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                        "Optional dead-letter table catalog.schema.table. Empty disables it")
                .define(DLQ_INCLUDE_METADATA, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                        "Dead-letter rows carry the Kafka metadata: cluster, topic, partition, offset, timestamp, headers")
                .define(DLQ_INCLUDE_KEY, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                        "Dead-letter rows carry the record key (BINARY). Off by default to keep rows small")
                .define(DLQ_OVERSIZE_METADATA_ONLY, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                        "A record too large for Zerobus is dead-lettered as its metadata only (value omitted, value_truncated=true). "
                        + "Needs zerobus.dlq.include.metadata=true. False: such a record is logged and skipped, not written")
                .define(MODE, ConfigDef.Type.STRING, MODE_TYPED, ConfigDef.Importance.HIGH,
                        "typed (fields become columns) or envelope (fixed key/value/headers/timestamp columns)")
                .define(RESCUE_ENABLED, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
                        "Table has a rescue column (nullable VARIANT tagged zerobus-rescue); extra fields are captured server-side")
                .define(RESCUE_NAME, ConfigDef.Type.STRING, "_rescued_data", ConfigDef.Importance.LOW,
                        "Name of the rescue column (informational; the server routes by its tag)")
                .define(EVOLUTION_ENABLED, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
                        "Diff records against the cached table schema and request additive (ADD COLUMN) changes from the table service")
                .define(CHANGE_MAX_WAIT_MS, ConfigDef.Type.LONG, 30000L, ConfigDef.Importance.MEDIUM,
                        "Max time a queue waits for a schema change or table creation before the record fails")
                .define(BUFFER_MAX, ConfigDef.Type.INT, 10000, ConfigDef.Importance.LOW,
                        "Records a waiting target may hold before its partition is paused")
                .define(REQUEST_BACKOFF_MAX_MS, ConfigDef.Type.LONG, 30000L, ConfigDef.Importance.LOW,
                        "Cap for the jittered exponential backoff between repeated change requests for one key")
                .define(TABLE_SERVICE_MODE, ConfigDef.Type.STRING, SERVICE_SHARDED, ConfigDef.Importance.MEDIUM,
                        "sharded: every task owns the DDL of the tables hashed to its control-topic partitions. off: no DDL")
                .define(WAREHOUSE_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                        "SQL warehouse ID the table service runs DDL on (required for evolution or auto-create)")
                .define(DDL_CLIENT_ID, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW,
                        "Optional separate principal for DDL; defaults to zerobus.client.id")
                .define(DDL_CLIENT_SECRET, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.LOW,
                        "Secret for zerobus.table.service.client.id")
                .define(CONTROL_TOPIC, ConfigDef.Type.STRING, "zerobus-connect-control", ConfigDef.Importance.LOW,
                        "Kafka topic for table service requests and schema change notices. Give it at least as many partitions as tasks")
                .define(CONTROL_SESSION_TIMEOUT_MS, ConfigDef.Type.INT, 10000, ConfigDef.Importance.LOW,
                        "Session timeout of the control-topic consumer group; bounds how fast a dead owner's tables move")
                .define(KAFKA_BOOTSTRAP, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                        "Kafka bootstrap servers for the control topic and the cluster ID lookup")
                .define(METADATA_ENABLED, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                        "Add a _kafka STRUCT with Kafka record metadata")
                .define(METADATA_FIELDS, ConfigDef.Type.LIST, String.join(",", METADATA_FIELD_NAMES), ConfigDef.Importance.LOW,
                        "Subset of cluster,topic,partition,offset,key,timestamp to include in _kafka")
                .define(METADATA_CLUSTER_NAME, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                        "Cluster name for _kafka.cluster; empty means the Kafka cluster ID from zerobus.kafka.bootstrap.servers")
                .define(ENVELOPE_HEADERS_ENABLED, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.LOW,
                        "Envelope mode: include record_headers")
                .define(MAX_INFLIGHT, ConfigDef.Type.INT, 50000, ConfigDef.Importance.LOW,
                        "Max unacknowledged records per stream (SDK maxInflightRecords)")
                .define(FORMAT, ConfigDef.Type.STRING, FORMAT_AUTO,
                        ConfigDef.ValidString.in(FORMAT_AUTO, FORMAT_ARROW, FORMAT_JSON), ConfigDef.Importance.MEDIUM,
                        "Stream format: auto uses Arrow for typed tables with no rescue column whose types are all "
                        + " mapped (see ArrowTypes); arrow forces it (typed, no rescue); json always uses JSON.")
                .define(ARROW_BATCH_MAX_RECORDS, ConfigDef.Type.INT, 1000, ConfigDef.Range.between(1, 100000),
                        ConfigDef.Importance.LOW, "Maximum records in one Arrow batch (a batch is one offset).")
                .define(ARROW_OPEN_THREADS, ConfigDef.Type.INT, 8, ConfigDef.Range.between(0, 64),
                        ConfigDef.Importance.LOW, "Per task: threads that flush and open Arrow streams when a schema "
                        + "change or a fresh open needs one (0 = open on the task thread).")
                .define(FLUSH_TIMEOUT_MS, ConfigDef.Type.INT, 300000, ConfigDef.Importance.LOW,
                        "Timeout for flush on close, in ms")
                .define(ON_NULL_VALUES, ConfigDef.Type.STRING, "ignore", ConfigDef.Importance.LOW,
                        "Tombstones (null values): ignore or fail")
                .define(TASK_INDEX, ConfigDef.Type.INT, 0, ConfigDef.Importance.LOW,
                        "Internal: task index assigned by the connector")
                .define(CONNECTOR_NAME, ConfigDef.Type.STRING, "zerobus", ConfigDef.Importance.LOW,
                        "Internal: connector name, used for the control-topic group id");
    }

    public ZerobusSinkConfig(Map<String, String> props) {
        super(configDef(), props, false);
        validate();
    }

    private void validate() {
        if (!MODE_TYPED.equals(getString(MODE)) && !MODE_ENVELOPE.equals(getString(MODE))) {
            throw new ConfigException(MODE, getString(MODE), "must be typed or envelope");
        }
        if (!SERVICE_SHARDED.equals(getString(TABLE_SERVICE_MODE)) && !SERVICE_OFF.equals(getString(TABLE_SERVICE_MODE))) {
            throw new ConfigException(TABLE_SERVICE_MODE, getString(TABLE_SERVICE_MODE), "must be sharded or off");
        }
        if (!"ignore".equals(getString(ON_NULL_VALUES)) && !"fail".equals(getString(ON_NULL_VALUES))) {
            throw new ConfigException(ON_NULL_VALUES, getString(ON_NULL_VALUES), "must be ignore or fail");
        }
        validateRouting();
        if (getBoolean(EVOLUTION_ENABLED)) {
            if (MODE_ENVELOPE.equals(getString(MODE))) {
                throw new ConfigException(EVOLUTION_ENABLED, true, "schema evolution applies to typed mode only");
            }
            requireTableService("schema evolution");
        }
        if (getBoolean(AUTO_CREATE)) {
            requireTableService("auto-create");
        }
        if (!getString(DLQ_TABLE).isEmpty()) {
            checkTableName(DLQ_TABLE, getString(DLQ_TABLE));
            if (getBoolean(DLQ_OVERSIZE_METADATA_ONLY) && !getBoolean(DLQ_INCLUDE_METADATA)) {
                throw new ConfigException(DLQ_OVERSIZE_METADATA_ONLY, true,
                        "an oversize record is dead-lettered as its metadata, so " + DLQ_INCLUDE_METADATA
                                + " must be true; set " + DLQ_OVERSIZE_METADATA_ONLY + "=false to skip oversize records instead");
            }
        }
        if (getBoolean(METADATA_ENABLED) && getString(METADATA_CLUSTER_NAME).isEmpty()
                && getString(KAFKA_BOOTSTRAP).isEmpty()) {
            throw new ConfigException(METADATA_CLUSTER_NAME, "",
                    "set zerobus.metadata.cluster.name or zerobus.kafka.bootstrap.servers (cluster ID lookup)");
        }
        for (String f : getList(METADATA_FIELDS)) {
            if (!METADATA_FIELD_NAMES.contains(f)) {
                throw new ConfigException(METADATA_FIELDS, f, "unknown metadata field");
            }
        }
        if (getInt(BUFFER_MAX) < 1) {
            throw new ConfigException(BUFFER_MAX, getInt(BUFFER_MAX), "must be at least 1");
        }
    }

    private void requireTableService(String what) {
        if (SERVICE_OFF.equals(getString(TABLE_SERVICE_MODE))) {
            throw new ConfigException(TABLE_SERVICE_MODE, SERVICE_OFF, what + " needs zerobus.table.service.mode=sharded");
        }
        if (getString(WAREHOUSE_ID) == null || getString(WAREHOUSE_ID).isEmpty()) {
            throw new ConfigException(WAREHOUSE_ID, null, "required for " + what);
        }
        if (getString(KAFKA_BOOTSTRAP).isEmpty()) {
            throw new ConfigException(KAFKA_BOOTSTRAP, "", "required for " + what + " (control topic)");
        }
    }

    /** Exactly one routing family: (a) single table or topic map, (b) static fan-out, (c) dynamic. */
    private void validateRouting() {
        boolean topicFamily = getString(TABLE) != null || !getString(TOPIC2TABLE).trim().isEmpty();
        boolean fanOut = !getList(TABLES).isEmpty();
        boolean dynamic = getBoolean(DYNAMIC_ENABLED);
        int families = (topicFamily ? 1 : 0) + (fanOut ? 1 : 0) + (dynamic ? 1 : 0);
        if (families == 0) {
            throw new ConfigException(TABLE, null, "set zerobus.table, zerobus.topic2table.map, zerobus.tables or zerobus.tables.dynamic.enabled");
        }
        if (families > 1) {
            throw new ConfigException(TABLES, getList(TABLES), "choose one routing family: table/topic map, static fan-out (zerobus.tables) or dynamic");
        }
        if (getString(TABLE) != null) {
            checkTableName(TABLE, getString(TABLE));
        }
        topicToTableRules();  // validates syntax
        for (String t : getList(TABLES)) {
            checkTableName(TABLES, t);
        }
        for (String key : tableRouteRegexKeys()) {
            String table = key.substring(TABLE_ROUTE_REGEX_PREFIX.length(), key.length() - TABLE_ROUTE_REGEX_SUFFIX.length());
            if (!getList(TABLES).contains(table)) {
                throw new ConfigException(key, null, "route regex given for a table that is not in zerobus.tables");
            }
            compileAnchored(key, String.valueOf(originals().get(key)));
        }
        if (!tableRouteRegexKeys().isEmpty() && getString(TABLES_ROUTE_FIELD) == null) {
            // Allowed: fan-out to every listed table. A regex without a field has no effect, so say so.
            throw new ConfigException(TABLES_ROUTE_FIELD, null, "route.regex needs zerobus.tables.route.field");
        }
        if (dynamic) {
            String ns = getString(DYNAMIC_NAMESPACE);
            if (getString(TABLES_ROUTE_FIELD) == null) {
                throw new ConfigException(TABLES_ROUTE_FIELD, null, "dynamic routing needs zerobus.tables.route.field");
            }
            if (ns == null || !NAMESPACE.matcher(ns).matches()) {
                throw new ConfigException(DYNAMIC_NAMESPACE, ns, "must be catalog.schema (ASCII letters, digits, underscores)");
            }
        }
    }

    /** Keys zerobus.tables.<fq>.route.regex present in the raw config. */
    public List<String> tableRouteRegexKeys() {
        List<String> out = new ArrayList<>();
        for (String k : originals().keySet()) {
            if (k.startsWith(TABLE_ROUTE_REGEX_PREFIX) && k.endsWith(TABLE_ROUTE_REGEX_SUFFIX)
                    && k.length() > TABLE_ROUTE_REGEX_PREFIX.length() + TABLE_ROUTE_REGEX_SUFFIX.length()) {
                out.add(k);
            }
        }
        java.util.Collections.sort(out);
        return out;
    }

    private static Pattern compileAnchored(String key, String regex) {
        try {
            return Pattern.compile("^(?:" + regex + ")$");
        } catch (PatternSyntaxException e) {
            throw new ConfigException(key, regex, "not a valid regex: " + e.getDescription());
        }
    }

    /** One topic rule: a full-match regex and a table template with $n group references. */
    public static final class TopicRule {
        public final String regex;
        public final Pattern pattern;
        public final String template;

        TopicRule(String regex, String template) {
            this.regex = regex;
            this.pattern = compileAnchored(TOPIC2TABLE, regex);
            this.template = template;
        }
    }

    /** Parses zerobus.topic2table.map, in order. */
    public List<TopicRule> topicToTableRules() {
        List<TopicRule> out = new ArrayList<>();
        String raw = getString(TOPIC2TABLE);
        if (raw == null || raw.trim().isEmpty()) {
            return out;
        }
        for (String entry : raw.split(";")) {
            String e = entry.trim();
            if (e.isEmpty()) {
                continue;
            }
            int sep = e.indexOf("=>");
            if (sep <= 0 || sep + 2 >= e.length()) {
                throw new ConfigException(TOPIC2TABLE, raw, "entries are topic-regex=>catalog.schema.table (template may use $1)");
            }
            String regex = e.substring(0, sep).trim();
            String template = e.substring(sep + 2).trim();
            TopicRule rule = new TopicRule(regex, template);
            String sample = template.replaceAll("\\$\\d+", "x");
            checkTableName(TOPIC2TABLE, sample);
            out.add(rule);
        }
        return out;
    }

    public Map<String, Pattern> tableRouteRegexes() {
        Map<String, Pattern> out = new LinkedHashMap<>();
        for (String key : tableRouteRegexKeys()) {
            String table = key.substring(TABLE_ROUTE_REGEX_PREFIX.length(), key.length() - TABLE_ROUTE_REGEX_SUFFIX.length());
            out.put(table, compileAnchored(key, String.valueOf(originals().get(key))));
        }
        return out;
    }

    static void checkTableName(String key, String table) {
        if (!FQ_TABLE.matcher(table).matches()) {
            throw new ConfigException(key, table, "must be catalog.schema.table with ASCII letters, digits, underscores");
        }
    }

    public static boolean validIdentifier(String name) {
        return name != null && IDENTIFIER.matcher(name).matches();
    }

    public static boolean validTable(String fq) {
        return fq != null && FQ_TABLE.matcher(fq).matches();
    }

    public boolean typed() { return MODE_TYPED.equals(getString(MODE)); }
    public boolean evolution() { return getBoolean(EVOLUTION_ENABLED); }
    public boolean autoCreate() { return getBoolean(AUTO_CREATE); }
    public boolean dynamic() { return getBoolean(DYNAMIC_ENABLED); }
    public String dynamicNamespace() { return getString(DYNAMIC_NAMESPACE); }
    public String routeField() { return getString(TABLES_ROUTE_FIELD); }
    public List<String> staticTables() { return getList(TABLES); }
    public String topicMapRaw() { return getString(TOPIC2TABLE); }
    public String defaultTable() { return getString(TABLE); }
    public boolean dlqIncludeMetadata() { return getBoolean(DLQ_INCLUDE_METADATA); }
    public boolean dlqIncludeKey() { return getBoolean(DLQ_INCLUDE_KEY); }
    public boolean dlqOversizeMetadataOnly() { return getBoolean(DLQ_OVERSIZE_METADATA_ONLY); }

    public String dlqTable() {
        String v = getString(DLQ_TABLE);
        return v == null || v.isEmpty() ? null : v;
    }
    public boolean rescue() { return getBoolean(RESCUE_ENABLED); }
    public boolean metadata() { return getBoolean(METADATA_ENABLED); }
    public boolean tableServiceSharded() { return SERVICE_SHARDED.equals(getString(TABLE_SERVICE_MODE)); }
    public boolean nullFails() { return "fail".equals(getString(ON_NULL_VALUES)); }
    public String rescueName() { return getString(RESCUE_NAME); }
    public String endpoint() { return getString(ENDPOINT); }
    public String workspaceUrl() { return getString(WORKSPACE_URL); }
    public String clientId() { return getString(CLIENT_ID); }
    public String clientSecret() { return getPassword(CLIENT_SECRET).value(); }
    public String ddlClientId() {
        String v = getString(DDL_CLIENT_ID);
        return v == null || v.isEmpty() ? clientId() : v;
    }
    public String ddlClientSecret() {
        String v = getPassword(DDL_CLIENT_SECRET) == null ? null : getPassword(DDL_CLIENT_SECRET).value();
        return v == null || v.isEmpty() ? clientSecret() : v;
    }
    public String warehouseId() { return getString(WAREHOUSE_ID); }
    public String kafkaBootstrap() { return getString(KAFKA_BOOTSTRAP); }
    public String controlTopic() { return getString(CONTROL_TOPIC); }
    public int controlSessionTimeoutMs() { return getInt(CONTROL_SESSION_TIMEOUT_MS); }
    public int taskIndex() { return getInt(TASK_INDEX); }
    public String connectorName() { return getString(CONNECTOR_NAME); }
    public int bufferMax() { return getInt(BUFFER_MAX); }
    public long requestBackoffMaxMs() { return getLong(REQUEST_BACKOFF_MAX_MS); }
    public long changeMaxWaitMs() { return getLong(CHANGE_MAX_WAIT_MS); }
    public int maxInflight() { return getInt(MAX_INFLIGHT); }
    public int flushTimeoutMs() { return getInt(FLUSH_TIMEOUT_MS); }
    public String format() { return getString(FORMAT); }
    public int arrowBatchMaxRecords() { return getInt(ARROW_BATCH_MAX_RECORDS); }
    public int arrowOpenThreads() { return getInt(ARROW_OPEN_THREADS); }
    public boolean envelopeHeaders() { return getBoolean(ENVELOPE_HEADERS_ENABLED); }
    public String metadataClusterName() { return getString(METADATA_CLUSTER_NAME); }

    public List<String> metadataFields() {
        List<String> f = getList(METADATA_FIELDS);
        return f.isEmpty() ? METADATA_FIELD_NAMES : f;
    }

    /** Immutable copy of a map, for tests and tools. */
    public static Map<String, String> copyOf(Map<String, String> m) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(m));
    }
}
