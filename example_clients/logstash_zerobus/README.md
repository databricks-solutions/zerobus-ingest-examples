# Logstash output plugin for Zerobus Ingest

`logstash-output-zerobus` writes Logstash events to one Zerobus Ingest table through the REST insert endpoint. It gets a table-scoped OAuth token with the client-credentials grant, so the token can only write that table.

This is an example plugin in the Zerobus Ingest examples repository, not a supported Databricks product. It is tested against Logstash 9.5.6 and the Zerobus Ingest REST API, and against a mock server for failure cases.

## Overview

The token request for Zerobus Ingest needs two parameters that a standard Logstash output doesn't send: `resource`, which names the workspace, and `authorization_details`, which limits the token to one table. This plugin sends both.

The plugin handles these cases as follows:

- **One table per output.** The token asks for `USE CATALOG` and `USE SCHEMA`, and for `SELECT` and `MODIFY` on that table, with the `zerobuswrite` operation.
- **Token refresh.** The token is cached and refreshed 300 seconds before it expires by default (`token_refresh_margin`). A 401 on insert drops the cached token and retries.
- **Batches.** Each Logstash batch is sent in chunks of `batch_size` records. Each chunk is one request whose body is a JSON array, the form the REST docs show.
- **Retries.** Connection and timeout errors (connection refused, reset, or timed out, host unreachable, broken pipe, DNS lookup failure, open or read timeout, an unexpected end of stream, and a malformed HTTP response), 408, 425, 429, and 5xx are retried up to `max_retries` times, with exponential backoff (`retry_backoff` doubled each attempt, capped at 30 seconds).
- **Bad records.** Any 4xx for a chunk other than 401, 403, 408, 425, and 429 (for example 400) splits the chunk in halves until the bad record is alone. That record is tagged `_zerobusfailure` with `[@metadata][zerobus][failure]` set to `rejected` and the server's status and message in `[@metadata][zerobus][status]` and `[@metadata][zerobus][error]`. In the mock server tests (`spec/integration`), the other records in that chunk are written, and the pipeline keeps running.
- **Auth and retry exhaustion.** A 403 on insert, or a token request the token endpoint refuses (400, 401, or 403), is an auth failure. The events in that chunk are tagged with `[@metadata][zerobus][failure]` set to `auth`, and the chunk is not split. A transient failure that outlasts `max_retries` tags the events with `delivery`. The pipeline does not stop.

## Prerequisites

- Logstash 9.5.6, the version this plugin is tested with. The Logstash distribution's bundled JDK and JRuby are used. In these steps, `$LOGSTASH_HOME` is the unpacked distribution folder.
- A Databricks workspace with Zerobus Ingest enabled, and its workspace URL and Zerobus Ingest endpoint (`<workspace-id>.zerobus.<region>.cloud.databricks.com`). See [Get your workspace URL and Zerobus Ingest endpoint](https://docs.databricks.com/ingestion/zerobus-ingest#get-your-workspace-url-and-zerobus-ingest-endpoint).
- A service principal with a client ID and secret, with the `Workspace access` entitlement.
- A Unity Catalog table you can create or write, created with `sql/create_table.sql`. The service principal needs `USE CATALOG`, `USE SCHEMA`, `SELECT`, and `MODIFY` on the catalog, schema, and table.
- Permission to run `GRANT` on the table, schema, and catalog, so you can give the service principal those four privileges.
- SQL access to run the statements in Quick start steps 1 and 5, from a SQL warehouse or the SQL editor, with permission to create tables in the schema.
- A non-root user for every step. Logstash refuses to run as root.
- For the tests, also Docker (to run the mock server), `curl` (to check it), and `keytool`, which the Logstash distribution bundles in `$LOGSTASH_HOME/jdk/bin`.

## Configuration reference

| Option | Required | Default | Description |
|---|---|---|---|
| `workspace_url` | yes | none | Workspace URL, used for `/oidc/v1/token`. |
| `zerobus_endpoint` | yes | none | Zerobus Ingest endpoint host, with or without `https://`. |
| `workspace_id` | no | first DNS label of `zerobus_endpoint` | Workspace ID in the token resource. Set it when the endpoint doesn't start with the ID. |
| `table` | yes | none | `catalog.schema.table`. |
| `client_id` | yes | none | Service principal client ID. Use `${VAR}` to read it from the environment. |
| `client_secret` | yes | none | Service principal client secret (`password` type, not logged). Use `${VAR}` or the Logstash keystore. |
| `batch_size` | no | `500` | Records per request. |
| `max_retries` | no | `5` | Retries for each transient failure. |
| `retry_backoff` | no | `1.0` | Seconds before the first retry. The delay doubles each attempt, up to 30. |
| `token_refresh_margin` | no | `300` | Seconds before token expiry when the token is refreshed. |
| `timestamp_field` | no | `event_timestamp` | Column that receives `@timestamp` as ISO 8601 UTC. Set `timestamp_field` to `""` to omit `@timestamp`. |
| `open_timeout` | no | `10` | Connect timeout, seconds. |
| `read_timeout` | no | `60` | Read timeout, seconds. |

Logstash sets the batch size with [`pipeline.batch.size`](https://www.elastic.co/docs/reference/logstash/logstash-settings-file), the most events a worker collects before it runs the outputs, and `pipeline.batch.delay`, how long it waits for more events before it sends a smaller batch. Each batch reaches the output's `multi_receive` call. The plugin has no flush timer of its own.

## Quick start

1. Create the table. Replace the `<...>` values in `sql/create_table.sql` and run the file in Databricks SQL. The file creates the table, tags its `rescue` column, and ends with the `GRANT` statements for the service principal. Run the `GRANT` statements yourself with your service principal's client ID, so the service principal has the four privileges listed under Prerequisites. Wait about 7 minutes after the rescue tag before the first write. In the test run, records that used the rescue column landed about 7 minutes after the tag. Shorter waits were not tested.
2. Build the gem with the JRuby bundled in the Logstash distribution, from the plugin folder:

   ```bash
   $LOGSTASH_HOME/vendor/jruby/bin/jruby -S gem build logstash-output-zerobus.gemspec
   ```

3. Install the gem into Logstash with `logstash-plugin install --no-verify`, the command the test run used, from the plugin folder:

   ```bash
   $LOGSTASH_HOME/bin/logstash-plugin install --no-verify ./logstash-output-zerobus-0.1.0.gem
   $LOGSTASH_HOME/bin/logstash-plugin list --verbose logstash-output-zerobus
   ```

4. Set the credentials in the environment (or the keystore), copy `config.example.conf` to a file outside the repo, and fill in the placeholders:

   ```bash
   export DATABRICKS_CLIENT_ID=<client-id>
   export DATABRICKS_CLIENT_SECRET=<client-secret>
   $LOGSTASH_HOME/bin/logstash -f ./zerobus.conf
   ```

5. Check the table with `SELECT count(*) FROM <catalog>.<schema>.<table>`.

## Schema

Zerobus Ingest writes records to the table schema. The plugin sends each event as a JSON object:

- Every top-level event field, except `@version`, `@metadata`, and `@timestamp`.
- `@timestamp` as an ISO 8601 UTC string under `timestamp_field` (default `event_timestamp`).

In the mock server tests, the mock rejects a column named `@timestamp` with `must be a plain identifier`, so the plugin writes the timestamp under `timestamp_field`. The example config removes `host`, `event`, and `@version` in a `mutate` filter.

Fields that don't match the table schema are rejected with HTTP 400 unless the table has a `rescue VARIANT` column tagged `zerobus-rescue`. The tag must be on the rescue column itself. In the test run, a table-level tag did not keep unmatched records. With the tag on the column, the same records were written: an unknown field went to `rescue`, and a field with the wrong type left its column NULL and went to `rescue`. `sql/create_table.sql` creates the column this way, as a nullable `rescue VARIANT`. See the [rescue column](https://docs.databricks.com/ingestion/zerobus-rescue-column) and [schema management](https://docs.databricks.com/ingestion/zerobus-schema-management) docs. The plugin does not change the table schema.

## How to extend

- **Another table:** one output block per table. Each output gets its own token.
- **Another field mapping:** change `record_for` in `lib/logstash/outputs/zerobus.rb`.
- **Another transport:** `Client` only needs an `insert(records)` method that raises `RejectedError`, `TransientError`, or `AuthError`. Swap the `Transport` class to change how HTTP is done.
- **Other events:** the output only sees the event's fields. Use filters to shape them before this output.

## Tests

The specs run on the JRuby bundled with the Logstash 9.5.6 tarball, without Bundler. Set these variables first. `<logstash-home>` is the unpacked 9.5.6 folder, and `<gem-dir>` is any empty folder for the test gems:

```bash
export LOGSTASH_HOME=<logstash-home>
export GEM_HOME=<gem-dir>
export GEM_PATH=$GEM_HOME:$LOGSTASH_HOME/vendor/bundle/jruby/3.4.0
```

Install rspec 3.13.2, the version in the Logstash 9.5.6 Gemfile.lock, into `$GEM_HOME` once:

```bash
$LOGSTASH_HOME/vendor/jruby/bin/jruby -S gem install rspec -v 3.13.2 --install-dir $GEM_HOME --no-document
```

From the plugin folder, run the unit specs. `spec/run_specs.rb` loads Logstash core and rspec, adds the plugin's `lib` and `spec` folders to the load path, then runs rspec:

```bash
$LOGSTASH_HOME/vendor/jruby/bin/jruby spec/run_specs.rb spec/outputs
```

The unit specs in `spec/outputs/` cover the token request, the insert client (retries, and the 401, 403, and other 4xx responses), and the output (chunking, bisection, and tagging). They make no calls outside localhost.

The mock-server specs (`spec/integration/`) need a running Zerobus Ingest mock server. The [Zerobus Mock Server](../zerobus_mock_server/) in this repository is one. The mock is a test double, so these specs don't write to Databricks. Use the same `<catalog>.<schema>.<table>` placeholder as Quick start for the mock table, and replace it with the same value in the config and in `ZEROBUS_MOCK_TABLE`. Save this config as `<config-dir>/config.yaml`. It makes the mock listen on port 18443 and defines that table:

```yaml
server:
  host: 0.0.0.0
  rest_port: 18443
  grpc_port: 15051
  public_hosts:
    - localhost
tls:
  cert_dir: ./data/tls
tables:
  - name: <catalog>.<schema>.<table>
    columns:
      - {name: message, type: STRING, nullable: true}
      - {name: sequence, type: LONG, nullable: true}
      - {name: event_timestamp, type: STRING, nullable: true}
```

Build the mock image from its folder and start it with the config folder mounted at `/config`. The build installs Python packages from `PIP_INDEX_URL`, which defaults to `https://pypi.org/simple`. Pass `--build-arg PIP_INDEX_URL=<index-url>` if you need a mirror. The mock writes its CA to `<config-dir>/data/tls/ca.pem` on first start:

```bash
docker build --network host -t zerobus-mock-server:logstash-plugin <mock-folder>
docker run -d --name zerobus-mock --network host --user "$(id -u):$(id -g)" --entrypoint python -v <config-dir>:/config zerobus-mock-server:logstash-plugin -m zerobus_mock_server --config /config/config.yaml
curl -s --cacert <config-dir>/data/tls/ca.pem https://localhost:18443/_mock/health
```

JRuby's HTTP client uses the JVM trust store, not the file in `SSL_CERT_FILE`. Import the mock's CA into a PKCS12 truststore, then run the specs with it:

```bash
$LOGSTASH_HOME/jdk/bin/keytool -importcert -noprompt -alias zerobus-mock-ca -file <config-dir>/data/tls/ca.pem -keystore <truststore.p12> -storetype PKCS12 -storepass <password>
export ZEROBUS_MOCK_URL=https://localhost:18443
export ZEROBUS_MOCK_TABLE=<catalog>.<schema>.<table>
export JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=<truststore.p12> -Djavax.net.ssl.trustStorePassword=<password> -Djavax.net.ssl.trustStoreType=PKCS12"
$LOGSTASH_HOME/vendor/jruby/bin/jruby spec/run_specs.rb spec/integration
```

Stop the mock with `docker rm -f zerobus-mock` when you are done.

The spec folder has a test-only codec stand-in (`spec/support/logstash/codecs/plain.rb`). The tarball's `logstash-codec-plain` can't be activated outside Bundler. Activating it outside Bundler fails with `Could not find 'logstash-core-plugin-api'`. The output never uses a codec in these specs.
