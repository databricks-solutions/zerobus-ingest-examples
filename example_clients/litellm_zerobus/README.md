# LiteLLM Zerobus: LLM Gateway request logs into Delta with the built-in callback

This example logs every request that passes through the [LiteLLM](https://docs.litellm.ai/)
Gateway (the LiteLLM proxy server) to a Unity Catalog Delta table. It uses LiteLLM's built-in
[`zerobus` callback](https://docs.litellm.ai/docs/observability/zerobus) and Zerobus Ingest.

Each row holds one request: the model, status, token counts, cost, caller, timing, request tags,
and the prompt and response as `VARIANT`. Failed requests get a row too. The callback gets its
own OAuth token from a service principal, so there's no code to write and no collector or
message bus in between.

The app is a small FastAPI dice roller that asks the gateway to narrate each roll. The gateway's
two models answer locally with LiteLLM's `mock_response`, so the example needs no LLM provider
key. Swap them for your real models, and the callback logs them the same way.

The whole thing runs with `docker compose up`.

```
┌──────────────────────────┐
│   dice-app (FastAPI)     │  GET /rolldice → 1 chat completion
│   traffic: curl loop     │  every 10th call uses the failing model
└────────────┬─────────────┘
             │  OpenAI-compatible API :4000
             v
┌──────────────────────────┐
│      LiteLLM Gateway     │  models: dice-narrator, dice-broken (mock)
│  callbacks: ["zerobus"]  │  queues one row per request, sends every 5 s
└────────────┬─────────────┘
             │  REST insert, JSON batches, OAuth (service principal)
             v
┌──────────────────────────┐
│      Zerobus Ingest      │  serverless, no broker
└────────────┬─────────────┘
             v
┌──────────────────────────┐
│  Delta in Unity Catalog  │  litellm_requests
└──────────────────────────┘
```

---

## Overview

- **What you get:** one table of gateway requests, queryable next to the rest of your data:
  usage and cost per model, team or key, error rates, latency, and the request tags your apps set.
- **How it works:** the callback queues a row for each finished request, success or failure. Every
  `flush_interval` seconds (5 in this example), or sooner when the queue reaches `batch_size`
  rows, it posts the queue as a JSON batch to the Zerobus Ingest REST endpoint. Logging runs
  separately from the model call, so a logging error doesn't change the response.
- **Delivery:** the queue is in memory, and a retried batch can write a row more than one time.
  Deduplicate on `litellm_call_id` when you count requests.

## Prerequisites

- A Databricks workspace in a [region that supports Zerobus Ingest](https://docs.databricks.com/aws/en/ingestion/zerobus-limits).
- A service principal with an OAuth secret. See
  [Configure authentication](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest#service-principal).
- Docker with Docker Compose.
- Outbound HTTPS to your workspace and to the Zerobus Ingest endpoint.

## Quick start

### 1. Create the table and grant access

The callback writes a fixed set of columns, and LiteLLM generates the matching statement. Copy
`config.example.env` to `.env`, set `ZEROBUS_TABLE_NAME`, and print the statement for the
gateway's LiteLLM version:

```bash
cp config.example.env .env
./scripts/create_table.sh
```

Run the printed `CREATE TABLE` in Databricks SQL, then the three `GRANT` statements from
`scripts/create_table.sql` (replace `<service-principal-uuid>` with the service principal's
application ID). `scripts/create_table.sql` holds the full statement that LiteLLM 1.109.1 prints.

### 2. Configure

Fill in `.env`:

- `ZEROBUS_WORKSPACE_URL`: your workspace URL, `https://<workspace-url>`.
- `ZEROBUS_SERVER_ENDPOINT`: your Zerobus Ingest endpoint, for example
  `https://<workspace-id>.zerobus.<region>.cloud.databricks.com`. To find both values, see
  [Get your workspace URL and Zerobus Ingest endpoint](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest#get-your-workspace-url-and-zerobus-ingest-endpoint).
- `ZEROBUS_CLIENT_ID` and `ZEROBUS_CLIENT_SECRET`: the service principal's application ID and
  OAuth secret.
- `LITELLM_MASTER_KEY`: any string that starts with `sk-`. The app uses it to call the gateway.

### 3. Start the stack

```bash
docker compose up -d --build
```

This starts the gateway, the dice app and a traffic loop that rolls every 2 seconds as three
players.

### 4. Watch the data arrive

```bash
./tests/smoke_test.sh
```

The test sends one successful and one failing request through the app, waits for a flush, and
checks that the callback logged no error. Then query the table in Databricks SQL:

```sql
SELECT status, model_group, count(*) AS requests, sum(total_tokens) AS tokens
FROM <catalog>.<schema>.litellm_requests
GROUP BY ALL
ORDER BY ALL;
```

`tests/check_rows.sql` shows the smoke test's two rows, one `success` and one `failure`.

### 5. Stop

```bash
docker compose down
```

## Configuration reference

The callback reads its settings from `zerobus_params` in `gateway/config.yaml`, which takes
each value from `.env` with `os.environ/`:

| Setting | Variable in `.env` | Description |
|---|---|---|
| `workspace_url` | `ZEROBUS_WORKSPACE_URL` | Workspace URL. The callback gets its token from `<workspace-url>/oidc/v1/token`. |
| `server_endpoint` | `ZEROBUS_SERVER_ENDPOINT` | Zerobus Ingest endpoint, with `https://`. |
| `client_id` | `ZEROBUS_CLIENT_ID` | Service principal application ID. |
| `client_secret` | `ZEROBUS_CLIENT_SECRET` | Service principal OAuth secret. |
| `table_name` | `ZEROBUS_TABLE_NAME` | Target table, `<catalog>.<schema>.<table>`. |
| `batch_size` | (none) | Queue size that triggers an early send (100). It isn't a maximum batch size. |
| `flush_interval` | (none) | Seconds between sends: 5 here. LiteLLM's default is 10. |
| `turn_off_message_logging` | (none) | `true` writes `redacted-by-litellm` in place of the prompt and response text. |

`gateway/Dockerfile` pins `LITELLM_VERSION` (1.109.1). The callback is in LiteLLM 1.105.0 and
above. If you change the version, run `./scripts/create_table.sh` again and update the table to
match.

## Schema

`scripts/create_table.sql` has the full statement. The main columns:

| Column | Type | Content |
|---|---|---|
| `id`, `litellm_call_id` | STRING | Response ID and LiteLLM's call ID (use `litellm_call_id` to deduplicate) |
| `status` | STRING | `success` or `failure` |
| `model`, `model_group`, `custom_llm_provider` | STRING | Provider model, gateway model name, provider |
| `start_time`, `end_time`, `response_time` | TIMESTAMP, DOUBLE | Timing |
| `prompt_tokens`, `completion_tokens`, `total_tokens` | LONG | Token usage |
| `response_cost` | DOUBLE | Cost that LiteLLM computes for the call |
| `api_key_hash`, `team_id`, `user_id`, `end_user` | STRING | Caller |
| `request_tags` | VARIANT | Tags the app sends, here `player:<name>` and `app:dice-roller` |
| `messages`, `response` | VARIANT | Prompt and response (redacted with `turn_off_message_logging`) |
| `error_str`, `error_information` | STRING, VARIANT | Error details on failure rows |

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Gateway log shows `zerobus: dropping … rows, token request returned 401` with `invalid_authorization_details` | The table in `ZEROBUS_TABLE_NAME` doesn't exist, or the service principal lacks the grants | Check the table name, then run the grants in `scripts/create_table.sql` |
| Inserts fail with HTTP 400 and an empty body | The workspace ID or region in `ZEROBUS_SERVER_ENDPOINT` doesn't match `ZEROBUS_WORKSPACE_URL` | Copy both values from the workspace, as in step 2 |
| Inserts fail with a decoder or encoder error | The table doesn't match the columns that this LiteLLM version writes | Run `./scripts/create_table.sh` again and update the table |
| No rows appear | Rows are sent every `flush_interval` seconds | Wait a few seconds, then check `docker compose logs litellm` |

## How to extend

- **Use your real models:** replace the two `mock_response` models in `gateway/config.yaml`
  with your providers, keep `litellm_settings` as it is, and point your apps at the gateway.
- **Tag requests:** send `metadata.tags` in each request, as `app/app.py` does, then filter or
  group on `request_tags` in SQL.
- **Redact content:** set `turn_off_message_logging: true`. Other columns, such as `metadata`
  and `error_str`, are still written.
- **Deduplicate:** build a view that keeps one row per `litellm_call_id`, for example with
  `QUALIFY ROW_NUMBER() OVER (PARTITION BY litellm_call_id ORDER BY end_time) = 1`.
- **Run more replicas:** every gateway replica sends its own rows to the same table.

## File reference

| File | Purpose |
|---|---|
| `docker-compose.yml` | Runs the gateway, the dice app and a traffic loop |
| `gateway/Dockerfile`, `gateway/config.yaml` | LiteLLM Gateway 1.109.1 with two mock models and the `zerobus` callback |
| `app/app.py`, `app/requirements.txt`, `app/Dockerfile` | The FastAPI dice roller that calls the gateway |
| `config.example.env` | Template for `.env` (endpoint, workspace URL, service principal, table, gateway key) |
| `scripts/create_table.sh` | Prints the `CREATE TABLE` statement for the gateway's LiteLLM version |
| `scripts/create_table.sql` | The statement for LiteLLM 1.109.1, and the grants |
| `tests/smoke_test.sh`, `tests/check_rows.sql` | One success and one failure through the gateway, and the query that shows them |

## Reference

- [LiteLLM: Databricks Zerobus callback](https://docs.litellm.ai/docs/observability/zerobus)
- [LiteLLM proxy quick start](https://docs.litellm.ai/docs/proxy/quick_start)
- [LiteLLM mock responses](https://docs.litellm.ai/docs/completion/mock_requests)
- [Zerobus Ingest overview](https://docs.databricks.com/aws/en/ingestion/zerobus-overview)
- [Zerobus Ingest limits](https://docs.databricks.com/aws/en/ingestion/zerobus-limits)

## License

&copy; 2026 Databricks, Inc. All rights reserved. The source in this example is provided
subject to the [Databricks License](https://databricks.com/db-license-source).
