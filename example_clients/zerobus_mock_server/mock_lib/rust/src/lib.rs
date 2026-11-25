//! In-process fake of the Zerobus Rust SDK's `ZerobusSdk` and `ZerobusStream`, for unit tests.
//!
//! Nothing is sent anywhere. The SDK's methods are async, and so are these, so the same test code
//! works against either. The fake never awaits, so any executor (tokio, futures) will drive it.
//!
//! ```ignore
//! let mock = Arc::new(FakeMock::new());
//! let sdk = FakeZerobusSdk::new(mock.clone());
//! let stream = sdk.stream_builder().table("main.default.events").oauth("id", "secret").json().build().await?;
//! mock.fail_next("schema-mismatch", 1, 2)?;
//! ```
//!
//! The scenario table below is a copy of `../scenarios.json`. The unit test at the bottom checks the copy
//! against that file. Records are JSON strings: the fake does not model the SDK's proto and Arrow inputs.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

/// Result alias mirroring the SDK's `ZerobusResult`.
pub type ZerobusResult<T> = Result<T, ZerobusError>;

/// Offsets are `i64`, as in the SDK.
pub type OffsetId = i64;

const TOKEN_EXPIRED: &str = "token-expired";

/// gRPC status codes the fake uses. Mirrors the SDK's `GrpcCode` for these values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcCode {
    Unknown,
    InvalidArgument,
    Unauthenticated,
    NotFound,
    Unavailable,
}

/// A gRPC status. Displays as its message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcStatus {
    code: GrpcCode,
    message: String,
}

impl GrpcStatus {
    pub fn new(code: GrpcCode, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }

    pub fn code(&self) -> GrpcCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for GrpcStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

/// Errors from the fake. Mirrors the SDK's `ZerobusError` variants that the scenarios need, with the
/// SDK's Display texts. Local to this crate because the SDK crate pulls in tonic and friends.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ZerobusError {
    /// Failed to create stream (gRPC status from the server).
    CreateStreamError(GrpcStatus),
    /// Stream is closed (gRPC status from the server).
    StreamClosedError(GrpcStatus),
    /// The client provided an invalid argument.
    InvalidArgument(String),
    /// OAuth token fetch failed.
    TokenFetchError(String),
    /// The stream is in an invalid state for the requested operation.
    InvalidStateError(String),
}

impl ZerobusError {
    /// Whether the SDK's own recovery may retry this error. Mirrors the SDK's classification for these
    /// variants: InvalidArgument, Unauthenticated, PermissionDenied, OutOfRange, Unimplemented and NotFound
    /// are not retried.
    pub fn is_retryable(&self) -> bool {
        match self {
            ZerobusError::CreateStreamError(s) | ZerobusError::StreamClosedError(s) => !matches!(
                s.code(),
                GrpcCode::InvalidArgument | GrpcCode::Unauthenticated | GrpcCode::NotFound
            ),
            ZerobusError::InvalidArgument(_)
            | ZerobusError::TokenFetchError(_)
            | ZerobusError::InvalidStateError(_) => false,
        }
    }
}

impl fmt::Display for ZerobusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ZerobusError::CreateStreamError(s) => write!(f, "Failed to create stream: {s}."),
            ZerobusError::StreamClosedError(s) => write!(f, "Stream is closed: {s}"),
            ZerobusError::InvalidArgument(m) => write!(f, "Invalid argument: {m}."),
            ZerobusError::TokenFetchError(m) => write!(f, "Token fetch failed: {m}"),
            ZerobusError::InvalidStateError(m) => write!(f, "Stream is in invalid state: {m}"),
        }
    }
}

impl std::error::Error for ZerobusError {}

// Fields are read by the contract test; the fake itself uses only `text`.
#[allow(dead_code)]
struct Spec {
    /// The call that raises: "create", "ingest", "flush", or None.
    where_: Option<&'static str>,
    text: &'static str,
    retried_by_sdk: bool,
}

const SCENARIOS: &[(&str, Spec)] = &[
    ("auth-failure", Spec { where_: Some("create"), text: "invalid_client: token endpoint rejected the client", retried_by_sdk: false }),
    ("table-not-found", Spec { where_: Some("create"), text: "table cannot be found", retried_by_sdk: false }),
    ("schema-mismatch", Spec { where_: Some("ingest"), text: "record does not fit the table schema", retried_by_sdk: false }),
    ("record-too-large", Spec { where_: Some("ingest"), text: "record is over the maximum message size", retried_by_sdk: false }),
    ("server-unavailable", Spec { where_: Some("create"), text: "UNAVAILABLE: retries exhausted", retried_by_sdk: true }),
    ("stream-abort-retryable", Spec { where_: None, text: "", retried_by_sdk: true }),
    ("stream-abort-fatal", Spec { where_: Some("ingest"), text: "terminal stream error: INVALID_ARGUMENT", retried_by_sdk: false }),
    ("graceful-close", Spec { where_: None, text: "", retried_by_sdk: true }),
    ("no-acks", Spec { where_: Some("flush"), text: "flush timed out waiting for acknowledgements", retried_by_sdk: false }),
    ("schema-change-while-open", Spec { where_: None, text: "", retried_by_sdk: false }),
    (TOKEN_EXPIRED, Spec { where_: Some("ingest"), text: "token has expired", retried_by_sdk: false }),
];

fn spec(name: &str) -> Option<&'static Spec> {
    SCENARIOS.iter().find(|(n, _)| *n == name).map(|(_, s)| s)
}

/// The error a raising scenario produces.
fn failure(name: &str) -> ZerobusError {
    let text = spec(name).map(|s| s.text).unwrap_or_default().to_string();
    match name {
        "auth-failure" => ZerobusError::TokenFetchError(text),
        "table-not-found" => ZerobusError::CreateStreamError(GrpcStatus::new(GrpcCode::NotFound, text)),
        "server-unavailable" => ZerobusError::CreateStreamError(GrpcStatus::new(GrpcCode::Unavailable, text)),
        "schema-mismatch" | "record-too-large" => ZerobusError::InvalidArgument(text),
        "stream-abort-fatal" => ZerobusError::StreamClosedError(GrpcStatus::new(GrpcCode::InvalidArgument, text)),
        "no-acks" => ZerobusError::InvalidStateError(text),
        TOKEN_EXPIRED => ZerobusError::StreamClosedError(GrpcStatus::new(GrpcCode::Unauthenticated, text)),
        _ => ZerobusError::InvalidStateError(text),
    }
}

#[derive(Default)]
struct Budget {
    after: u32,
    left: u32,
}

#[derive(Default)]
struct MockState {
    budgets: HashMap<String, Budget>,
    held: Option<String>,
    expiries: u32,
}

/// Scenario controls. Same names as the Python lib and the server's control API. Share it with `Arc`.
#[derive(Default)]
pub struct FakeMock {
    state: Mutex<MockState>,
}

impl FakeMock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fails the next `count` matching calls after skipping `after` of them. Errors on an unknown scenario.
    pub fn fail_next(&self, scenario: &str, count: u32, after: u32) -> Result<(), String> {
        if spec(scenario).is_none() {
            return Err(format!("unknown scenario {scenario:?}"));
        }
        if count < 1 {
            return Err("count must be >= 1".to_string());
        }
        let mut st = self.state.lock().unwrap();
        if scenario == TOKEN_EXPIRED {
            if after > 0 {
                return Err("token-expired takes no after; use expire_token".to_string());
            }
            st.expiries += count;
            return Ok(());
        }
        st.budgets.insert(scenario.to_string(), Budget { after, left: count });
        Ok(())
    }

    /// Holds a scenario: every matching call fails until `clear_scenario`.
    pub fn set_scenario(&self, scenario: &str) -> Result<(), String> {
        if spec(scenario).is_none() {
            return Err(format!("unknown scenario {scenario:?}"));
        }
        if scenario == TOKEN_EXPIRED {
            return Err("token-expired is one-shot: use fail_next or expire_token".to_string());
        }
        self.state.lock().unwrap().held = Some(scenario.to_string());
        Ok(())
    }

    pub fn clear_scenario(&self) {
        self.state.lock().unwrap().held = None;
    }

    /// The next `count` authenticated calls find their token expired (create refreshes it; an ingest fails).
    pub fn expire_token(&self, count: u32) {
        self.state.lock().unwrap().expiries += count;
    }

    /// Clears fail-next budgets, the held scenario and the expiry budget.
    pub fn reset(&self) {
        *self.state.lock().unwrap() = MockState::default();
    }

    fn is_held(&self, scenario: &str) -> bool {
        self.state.lock().unwrap().held.as_deref() == Some(scenario)
    }

    /// True when a fail-next budget fires on this matching call.
    fn take(&self, scenario: &str) -> bool {
        let mut st = self.state.lock().unwrap();
        let Some(b) = st.budgets.get_mut(scenario) else { return false };
        if b.after > 0 {
            b.after -= 1;
            return false;
        }
        if b.left == 0 {
            return false;
        }
        b.left -= 1;
        true
    }

    /// A held scenario fires on every matching call; otherwise a fail-next budget may.
    fn fire(&self, scenario: &str) -> bool {
        self.is_held(scenario) || self.take(scenario)
    }

    fn take_expiry(&self) -> bool {
        let mut st = self.state.lock().unwrap();
        if st.expiries > 0 {
            st.expiries -= 1;
            return true;
        }
        false
    }
}

/// The fake of `ZerobusSdk`.
pub struct FakeZerobusSdk {
    mock: Arc<FakeMock>,
}

impl FakeZerobusSdk {
    pub fn new(mock: Arc<FakeMock>) -> Self {
        Self { mock }
    }

    pub fn mock(&self) -> &Arc<FakeMock> {
        &self.mock
    }

    /// Starts a stream builder, as `ZerobusSdk::stream_builder` does.
    pub fn stream_builder(&self) -> FakeStreamBuilder<'_> {
        FakeStreamBuilder { sdk: self, table: String::new() }
    }
}

/// The fake of `StreamBuilder`. Only the table name matters to the fake; auth and format are accepted and ignored.
pub struct FakeStreamBuilder<'a> {
    sdk: &'a FakeZerobusSdk,
    table: String,
}

impl FakeStreamBuilder<'_> {
    pub fn table(mut self, table_name: impl Into<String>) -> Self {
        self.table = table_name.into();
        self
    }

    pub fn oauth(self, _client_id: impl Into<String>, _client_secret: impl Into<String>) -> Self {
        self
    }

    pub fn json(self) -> Self {
        self
    }

    /// Creates the stream. A create-time failure is returned as `Err`, as the SDK does.
    pub async fn build(self) -> ZerobusResult<FakeStream> {
        let mock = &self.sdk.mock;
        for name in ["auth-failure", "table-not-found", "server-unavailable"] {
            if mock.fire(name) {
                return Err(failure(name));
            }
        }
        // An expired token is refreshed during create, so the caller sees success.
        mock.take_expiry();
        Ok(FakeStream::new(Arc::clone(mock), self.table))
    }
}

#[derive(Default)]
struct StreamState {
    accepted: Vec<(OffsetId, String)>,
    stored: Vec<String>,
    fallback: Vec<String>,
    next_offset: OffsetId,
    acked_through: OffsetId,
    terminal: Option<&'static str>,
    closed: bool,
}

/// The fake of `ZerobusStream`. `ingested()` is what the stream accepted; `stored()` is what landed in the
/// table, so a retried abort shows up twice; `fallback()` holds rows routed away by schema-change-while-open.
pub struct FakeStream {
    mock: Arc<FakeMock>,
    table: String,
    state: Mutex<StreamState>,
}

impl FakeStream {
    fn new(mock: Arc<FakeMock>, table: String) -> Self {
        let state = StreamState { acked_through: -1, ..Default::default() };
        Self { mock, table, state: Mutex::new(state) }
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    /// Queues one record and returns its offset.
    pub async fn ingest_record_offset(&self, payload: impl Into<String>) -> ZerobusResult<OffsetId> {
        let payload = payload.into();
        let mut st = self.state.lock().unwrap();
        if st.closed {
            return Err(ZerobusError::InvalidStateError("stream is closed".to_string()));
        }
        if let Some(name) = st.terminal {
            return Err(failure(name));
        }
        if self.mock.take_expiry() {
            st.terminal = Some(TOKEN_EXPIRED);
            return Err(failure(TOKEN_EXPIRED));
        }
        for name in ["schema-mismatch", "record-too-large"] {
            if self.mock.fire(name) {
                return Err(failure(name));
            }
        }
        if self.mock.fire("stream-abort-fatal") {
            st.terminal = Some("stream-abort-fatal");
            return Err(failure("stream-abort-fatal"));
        }
        let offset = st.next_offset;
        st.next_offset += 1;
        st.accepted.push((offset, payload.clone()));
        if !self.mock.is_held("no-acks") {
            st.acked_through = offset;
        }
        if self.mock.fire("schema-change-while-open") {
            st.fallback.push(payload);
            return Ok(offset);
        }
        st.stored.push(payload.clone());
        if self.mock.fire("stream-abort-retryable") {
            st.stored.push(payload); // the aborted copy was stored unacked; the SDK's resend stores it again
        }
        Ok(offset)
    }

    /// Queues a batch. Returns the last offset, or `None` for an empty batch.
    pub async fn ingest_records_offset<I, T>(&self, payloads: I) -> ZerobusResult<Option<OffsetId>>
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let mut last = None;
        for p in payloads {
            last = Some(self.ingest_record_offset(p).await?);
        }
        Ok(last)
    }

    /// Returns once `offset` is acknowledged, or an error if acks are withheld.
    pub async fn wait_for_offset(&self, offset: OffsetId) -> ZerobusResult<()> {
        let mut st = self.state.lock().unwrap();
        if let Some(name) = st.terminal {
            return Err(failure(name));
        }
        if self.mock.take("no-acks") || (self.mock.is_held("no-acks") && offset > st.acked_through) {
            return Err(failure("no-acks"));
        }
        st.acked_through = st.acked_through.max(offset);
        Ok(())
    }

    /// Waits until everything queued so far is acknowledged.
    pub async fn flush(&self) -> ZerobusResult<()> {
        let mut st = self.state.lock().unwrap();
        if let Some(name) = st.terminal {
            return Err(failure(name));
        }
        if self.mock.take("no-acks") {
            return Err(failure("no-acks"));
        }
        if self.mock.is_held("no-acks") {
            if st.acked_through < st.next_offset - 1 {
                return Err(failure("no-acks"));
            }
            return Ok(());
        }
        st.acked_through = st.next_offset - 1;
        Ok(())
    }

    /// The accepted records that are not acknowledged, in order.
    pub async fn get_unacked_records(&self) -> ZerobusResult<Vec<String>> {
        let st = self.state.lock().unwrap();
        Ok(st.accepted.iter().filter(|(o, _)| *o > st.acked_through).map(|(_, p)| p.clone()).collect())
    }

    /// Marks the stream closed. Later ingests fail.
    pub async fn close(&mut self) -> ZerobusResult<()> {
        self.state.lock().unwrap().closed = true;
        Ok(())
    }

    pub fn ingested(&self) -> Vec<String> {
        self.state.lock().unwrap().accepted.iter().map(|(_, p)| p.clone()).collect()
    }

    pub fn stored(&self) -> Vec<String> {
        self.state.lock().unwrap().stored.clone()
    }

    pub fn fallback(&self) -> Vec<String> {
        self.state.lock().unwrap().fallback.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The table matches ../scenarios.json (this test runs in the crate root, /zbmock/rust in the image).
    #[test]
    fn scenario_table_matches_scenarios_json() {
        let raw = std::fs::read_to_string("../scenarios.json").expect("scenarios.json");
        let v: serde_json::Value = serde_json::from_str(&raw).expect("valid JSON");
        let contract = v["scenarios"].as_object().expect("scenarios object");
        let mut have: Vec<&str> = SCENARIOS.iter().map(|(n, _)| *n).collect();
        let mut want: Vec<&str> = contract.keys().map(String::as_str).collect();
        have.sort_unstable();
        want.sort_unstable();
        assert_eq!(have, want, "scenario names differ");
        for (name, c) in contract {
            let s = spec(name).unwrap();
            assert_eq!(s.where_, c["where"].as_str(), "{name} where");
            assert_eq!(s.retried_by_sdk, c["retried_by_sdk"].as_bool().unwrap(), "{name} retried");
            if let Some(msg) = c["message"].as_str() {
                assert!(s.text.contains(msg), "{name}: {:?} lacks the contract message {msg:?}", s.text);
                // The error the fake raises is classified the way the contract says.
                assert_eq!(failure(name).is_retryable(), s.retried_by_sdk, "{name} is_retryable");
            }
        }
    }
}
