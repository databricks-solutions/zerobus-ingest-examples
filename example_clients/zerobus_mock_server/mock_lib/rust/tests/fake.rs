//! Behaviour tests for the Rust mock lib: the controls, the caller-visible outcomes, and reset.

use std::sync::Arc;

use zerobus_mock_lib::{FakeMock, FakeStream, FakeZerobusSdk, ZerobusError};

const TABLE: &str = "main.default.events";

async fn stream_for(mock: &Arc<FakeMock>) -> FakeStream {
    FakeZerobusSdk::new(Arc::clone(mock))
        .stream_builder()
        .table(TABLE)
        .oauth("id", "secret")
        .json()
        .build()
        .await
        .expect("create")
}

async fn create_error(mock: &Arc<FakeMock>) -> ZerobusError {
    FakeZerobusSdk::new(Arc::clone(mock))
        .stream_builder()
        .table(TABLE)
        .oauth("id", "secret")
        .json()
        .build()
        .await
        .err()
        .expect("create should fail")
}

fn msg(e: &ZerobusError) -> String {
    e.to_string()
}

#[tokio::test]
async fn records_are_recorded_in_order_with_offsets() {
    let mock = Arc::new(FakeMock::new());
    let s = stream_for(&mock).await;
    assert_eq!(s.ingest_record_offset(r#"{"n":0}"#).await.unwrap(), 0);
    assert_eq!(s.ingest_record_offset(r#"{"n":1}"#).await.unwrap(), 1);
    assert_eq!(s.ingest_records_offset([r#"{"n":2}"#, r#"{"n":3}"#]).await.unwrap(), Some(3));
    assert_eq!(s.ingest_records_offset(Vec::<String>::new()).await.unwrap(), None);
    s.flush().await.unwrap();
    assert_eq!(s.ingested().len(), 4);
    assert_eq!(s.stored(), s.ingested());
    assert!(s.get_unacked_records().await.unwrap().is_empty());
}

#[tokio::test]
async fn fail_next_after_skips_then_fails_once() {
    let mock = Arc::new(FakeMock::new());
    let s = stream_for(&mock).await;
    mock.fail_next("schema-mismatch", 1, 2).unwrap();
    s.ingest_record_offset("a").await.unwrap();
    s.ingest_record_offset("b").await.unwrap();
    let e = s.ingest_record_offset("c").await.unwrap_err();
    assert!(msg(&e).contains("does not fit"), "{e}");
    assert!(!e.is_retryable());
    assert_eq!(s.ingest_record_offset("d").await.unwrap(), 2, "one-shot: the next call succeeds");
    assert_eq!(s.ingested(), vec!["a", "b", "d"]);
}

#[tokio::test]
async fn record_too_large_raises_and_is_not_stored() {
    let mock = Arc::new(FakeMock::new());
    let s = stream_for(&mock).await;
    mock.fail_next("record-too-large", 1, 0).unwrap();
    let e = s.ingest_record_offset("big").await.unwrap_err();
    assert!(msg(&e).contains("maximum message size"), "{e}");
    assert!(s.stored().is_empty());
}

#[tokio::test]
async fn stream_abort_fatal_is_terminal_and_keeps_earlier_records() {
    let mock = Arc::new(FakeMock::new());
    let s = stream_for(&mock).await;
    s.ingest_record_offset("kept").await.unwrap();
    mock.fail_next("stream-abort-fatal", 1, 0).unwrap();
    let e = s.ingest_record_offset("x").await.unwrap_err();
    assert!(msg(&e).contains("terminal stream error"), "{e}");
    assert!(s.ingest_record_offset("after").await.is_err());
    assert!(s.flush().await.is_err());
    assert_eq!(s.stored(), vec!["kept"]);
    let fresh = stream_for(&mock).await;
    assert_eq!(fresh.ingest_record_offset("new").await.unwrap(), 0);
}

#[tokio::test]
async fn auth_failure_and_table_not_found_raise_at_create() {
    let mock = Arc::new(FakeMock::new());
    mock.fail_next("auth-failure", 1, 0).unwrap();
    let auth = create_error(&mock).await;
    assert!(msg(&auth).contains("invalid_client"), "{auth}");
    assert!(!auth.is_retryable());
    mock.fail_next("table-not-found", 1, 0).unwrap();
    assert!(msg(&create_error(&mock).await).contains("cannot be found"));
}

#[tokio::test]
async fn server_unavailable_holds_at_create_until_cleared() {
    let mock = Arc::new(FakeMock::new());
    mock.set_scenario("server-unavailable").unwrap();
    let e = create_error(&mock).await;
    assert!(msg(&e).contains("UNAVAILABLE"), "{e}");
    assert!(e.is_retryable(), "the SDK retries UNAVAILABLE");
    assert!(msg(&create_error(&mock).await).contains("UNAVAILABLE"), "still held");
    mock.clear_scenario();
    assert_eq!(stream_for(&mock).await.ingest_record_offset("ok").await.unwrap(), 0);
}

#[tokio::test]
async fn token_expiry_at_create_is_refreshed_and_mid_stream_is_terminal() {
    let mock = Arc::new(FakeMock::new());
    mock.expire_token(1);
    let s = stream_for(&mock).await; // create refreshes the token: success
    mock.expire_token(1);
    let e = s.ingest_record_offset("late").await.unwrap_err();
    assert!(msg(&e).contains("expired"), "{e}");
    assert!(s.ingest_record_offset("later").await.is_err());
    assert_eq!(stream_for(&mock).await.ingest_record_offset("new").await.unwrap(), 0);
}

#[tokio::test]
async fn no_acks_makes_flush_fail_until_cleared_and_keeps_records() {
    let mock = Arc::new(FakeMock::new());
    let s = stream_for(&mock).await;
    mock.set_scenario("no-acks").unwrap();
    s.ingest_record_offset("a").await.unwrap();
    let e = s.flush().await.unwrap_err();
    assert!(msg(&e).contains("timed out"), "{e}");
    assert_eq!(s.stored(), vec!["a"]);
    assert_eq!(s.get_unacked_records().await.unwrap(), vec!["a"]);
    mock.clear_scenario();
    s.flush().await.unwrap();
    assert!(s.get_unacked_records().await.unwrap().is_empty());
}

#[tokio::test]
async fn graceful_close_raises_nothing_and_stores_the_record() {
    let mock = Arc::new(FakeMock::new());
    let mut s = stream_for(&mock).await;
    mock.fail_next("graceful-close", 1, 0).unwrap();
    assert_eq!(s.ingest_record_offset("g").await.unwrap(), 0);
    s.flush().await.unwrap();
    s.close().await.unwrap();
    assert_eq!(s.stored(), vec!["g"]);
}

#[tokio::test]
async fn stream_abort_retryable_stores_the_record_twice_without_an_error() {
    let mock = Arc::new(FakeMock::new());
    let s = stream_for(&mock).await;
    mock.fail_next("stream-abort-retryable", 1, 1).unwrap();
    s.ingest_record_offset("once").await.unwrap();
    assert_eq!(s.ingest_record_offset("twice").await.unwrap(), 1);
    s.flush().await.unwrap();
    assert_eq!(s.stored(), vec!["once", "twice", "twice"]);
    assert_eq!(s.ingested(), vec!["once", "twice"]);
}

#[tokio::test]
async fn schema_change_while_open_raises_nothing_and_routes_rows_to_fallback() {
    let mock = Arc::new(FakeMock::new());
    let s = stream_for(&mock).await;
    s.ingest_record_offset("before").await.unwrap();
    mock.fail_next("schema-change-while-open", 1, 0).unwrap();
    s.ingest_record_offset("during").await.unwrap();
    s.flush().await.unwrap();
    assert_eq!(s.stored(), vec!["before"]);
    assert_eq!(s.fallback(), vec!["during"]);
}

#[tokio::test]
async fn reset_clears_budgets_held_scenario_and_expiry() {
    let mock = Arc::new(FakeMock::new());
    mock.fail_next("auth-failure", 1, 0).unwrap();
    mock.set_scenario("no-acks").unwrap();
    mock.expire_token(2);
    mock.reset();
    let s = stream_for(&mock).await;
    assert_eq!(s.ingest_record_offset("fine").await.unwrap(), 0);
    s.flush().await.unwrap();
}

#[test]
fn unknown_scenario_is_rejected_and_token_expired_cannot_be_held() {
    let mock = FakeMock::new();
    assert!(mock.fail_next("no-such", 1, 0).is_err());
    assert!(mock.set_scenario("token-expired").is_err());
}
