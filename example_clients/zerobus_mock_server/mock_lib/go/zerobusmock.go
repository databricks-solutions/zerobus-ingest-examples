// Package zerobusmock is an UNOFFICIAL in-process fake of the Zerobus Go SDK's ZerobusSdk and ZerobusStream,
// for unit tests. Not affiliated with Databricks. Nothing is sent anywhere. The real SDK
// (module github.com/databricks/zerobus-sdk/go) needs cgo and a native library, so this package defines
// ZerobusError itself, with the SDK's fields and methods.
//
//	mock := zerobusmock.NewFakeMock()
//	sdk := zerobusmock.NewFakeZerobusSdk(mock)
//	stream, err := sdk.CreateStream(zerobusmock.TableProperties{TableName: "main.default.events"}, "id", "secret", nil)
//	mock.FailNext("schema-mismatch", 1, 2)
//
// The scenario table is a copy of ../scenarios.json. zerobusmock_test.go checks the copy against that file.
package zerobusmock

import (
	"fmt"
	"sync"
)

// ContractVersion is the scenarios.json contract_version this copy embeds.
const ContractVersion = 1

const tokenExpired = "token-expired"

type spec struct {
	where     string // "create", "ingest", "flush" or "" (no error)
	text      string
	retriedBy bool // the SDK retries this one
}

var scenarios = map[string]spec{
	"auth-failure":             {"create", "invalid_client: token endpoint rejected the client", false},
	"table-not-found":          {"create", "table cannot be found", false},
	"schema-mismatch":          {"ingest", "record does not fit the table schema", false},
	"record-too-large":         {"ingest", "record is over the maximum message size", false},
	"server-unavailable":       {"create", "UNAVAILABLE: retries exhausted", true},
	"stream-abort-retryable":   {"", "", true},
	"stream-abort-fatal":       {"ingest", "terminal stream error: INVALID_ARGUMENT", false},
	"graceful-close":           {"", "", true},
	"no-acks":                  {"flush", "flush timed out waiting for acknowledgements", false},
	"schema-change-while-open": {"", "", false},
	tokenExpired:               {"ingest", "token has expired", false},
}

// ZerobusError has the same fields and methods as the SDK's ZerobusError.
type ZerobusError struct {
	Message     string
	IsRetryable bool
}

func (e *ZerobusError) Error() string {
	if e.IsRetryable {
		return fmt.Sprintf("ZerobusError (retryable): %s", e.Message)
	}
	return fmt.Sprintf("ZerobusError: %s", e.Message)
}

// Retryable returns whether this error can be retried.
func (e *ZerobusError) Retryable() bool { return e.IsRetryable }

func failure(name string) *ZerobusError {
	s := scenarios[name]
	return &ZerobusError{Message: s.text, IsRetryable: s.retriedBy}
}

// TableProperties mirrors the SDK's TableProperties. The fake ignores DescriptorProto.
type TableProperties struct {
	TableName       string
	DescriptorProto []byte
}

// StreamConfigurationOptions mirrors the SDK's options struct. The fake ignores it.
type StreamConfigurationOptions struct {
	MaxInflightRequests uint64
	Recovery            bool
}

type budget struct{ after, left int }

// FakeMock holds the scenario controls. Same names as the Python lib and the server's control API.
type FakeMock struct {
	mu      sync.Mutex
	budgets map[string]*budget
	held    string
	expiry  int
}

func NewFakeMock() *FakeMock { return &FakeMock{budgets: map[string]*budget{}} }

// FailNext makes the next count matching calls fail, after skipping after of them.
func (m *FakeMock) FailNext(scenario string, count, after int) error {
	if _, ok := scenarios[scenario]; !ok {
		return fmt.Errorf("unknown scenario %q", scenario)
	}
	if count < 1 || after < 0 {
		return fmt.Errorf("count must be >= 1 and after >= 0")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if scenario == tokenExpired {
		if after > 0 {
			return fmt.Errorf("token-expired takes no after; use ExpireToken")
		}
		m.expiry += count
		return nil
	}
	m.budgets[scenario] = &budget{after: after, left: count}
	return nil
}

// SetScenario holds a scenario: every matching call fails until ClearScenario.
func (m *FakeMock) SetScenario(scenario string) error {
	if _, ok := scenarios[scenario]; !ok {
		return fmt.Errorf("unknown scenario %q", scenario)
	}
	if scenario == tokenExpired {
		return fmt.Errorf("token-expired is one-shot: use FailNext or ExpireToken")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.held = scenario
	return nil
}

func (m *FakeMock) ClearScenario() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.held = ""
}

// ExpireToken makes the next count authenticated calls find their token expired.
func (m *FakeMock) ExpireToken(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expiry += count
}

// Reset clears fail-next budgets, the held scenario and the expiry budget.
func (m *FakeMock) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.budgets = map[string]*budget{}
	m.held = ""
	m.expiry = 0
}

func (m *FakeMock) isHeld(scenario string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.held == scenario
}

// take reports whether a fail-next budget fires on this matching call.
func (m *FakeMock) take(scenario string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	b := m.budgets[scenario]
	if b == nil {
		return false
	}
	if b.after > 0 {
		b.after--
		return false
	}
	if b.left <= 0 {
		return false
	}
	b.left--
	return true
}

// fire: a held scenario fires on every matching call; otherwise a fail-next budget may.
func (m *FakeMock) fire(scenario string) bool {
	return m.isHeld(scenario) || m.take(scenario)
}

func (m *FakeMock) takeExpiry() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.expiry > 0 {
		m.expiry--
		return true
	}
	return false
}

// FakeZerobusSdk is the fake of ZerobusSdk.
type FakeZerobusSdk struct{ mock *FakeMock }

// NewFakeZerobusSdk returns a fake SDK driven by mock. A nil mock gets a fresh one.
func NewFakeZerobusSdk(mock *FakeMock) *FakeZerobusSdk {
	if mock == nil {
		mock = NewFakeMock()
	}
	return &FakeZerobusSdk{mock: mock}
}

// Mock returns the controls for this SDK.
func (s *FakeZerobusSdk) Mock() *FakeMock { return s.mock }

// CreateStream mirrors ZerobusSdk.CreateStream. An expired token is refreshed here, so the caller sees success.
func (s *FakeZerobusSdk) CreateStream(props TableProperties, clientID, clientSecret string, options *StreamConfigurationOptions) (*FakeStream, error) {
	for _, name := range []string{"auth-failure", "table-not-found", "server-unavailable"} {
		if s.mock.fire(name) {
			return nil, failure(name)
		}
	}
	s.mock.takeExpiry()
	return &FakeStream{mock: s.mock, table: props.TableName, ackedThrough: -1}, nil
}

// FakeStream is the fake of ZerobusStream.
//
// Ingested returns every record the stream accepted (it returned an offset); Stored returns what landed in
// the table, so a retried abort shows up twice; Fallback returns rows routed away from the table by
// schema-change-while-open. A record is acked when it is ingested, unless no-acks is held.
type FakeStream struct {
	mu           sync.Mutex
	mock         *FakeMock
	table        string
	accepted     []offsetRecord
	stored       []interface{}
	fallback     []interface{}
	nextOffset   int64
	ackedThrough int64 // -1 before any ack
	terminal     string
	closed       bool
}

type offsetRecord struct {
	offset  int64
	payload interface{}
}

// IngestRecordOffset queues one record and returns its offset.
func (st *FakeStream) IngestRecordOffset(payload interface{}) (int64, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.closed {
		return -1, &ZerobusError{Message: "Stream has been closed", IsRetryable: false}
	}
	if err := st.checkTerminal(); err != nil {
		return -1, err
	}
	if st.mock.takeExpiry() {
		st.terminal = tokenExpired
		return -1, failure(tokenExpired)
	}
	if st.mock.fire("schema-mismatch") {
		return -1, failure("schema-mismatch")
	}
	if st.mock.fire("record-too-large") {
		return -1, failure("record-too-large")
	}
	if st.mock.fire("stream-abort-fatal") {
		st.terminal = "stream-abort-fatal"
		return -1, failure("stream-abort-fatal")
	}
	offset := st.nextOffset
	st.nextOffset++
	st.accepted = append(st.accepted, offsetRecord{offset, payload})
	if !st.mock.isHeld("no-acks") {
		st.ackedThrough = offset
	}
	if st.mock.fire("schema-change-while-open") {
		st.fallback = append(st.fallback, payload)
		return offset, nil
	}
	st.stored = append(st.stored, payload)
	if st.mock.fire("stream-abort-retryable") {
		st.stored = append(st.stored, payload) // the aborted copy was stored unacked; the SDK's resend stores it again
	}
	return offset, nil
}

// IngestRecordsOffset ingests each record in order and returns the last offset, or -1 for an empty batch (as the SDK does).
func (st *FakeStream) IngestRecordsOffset(records []interface{}) (int64, error) {
	last := int64(-1)
	for _, r := range records {
		off, err := st.IngestRecordOffset(r)
		if err != nil {
			return -1, err
		}
		last = off
	}
	return last, nil
}

// WaitForOffset returns once offset is acknowledged, or an error if acks are withheld.
func (st *FakeStream) WaitForOffset(offset int64) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if err := st.checkTerminal(); err != nil {
		return err
	}
	if st.mock.take("no-acks") || (st.mock.isHeld("no-acks") && offset > st.ackedThrough) {
		return failure("no-acks")
	}
	if offset > st.ackedThrough {
		st.ackedThrough = offset
	}
	return nil
}

// Flush waits until everything queued so far is acknowledged.
func (st *FakeStream) Flush() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if err := st.checkTerminal(); err != nil {
		return err
	}
	if st.mock.take("no-acks") {
		return failure("no-acks")
	}
	if st.mock.isHeld("no-acks") {
		if st.ackedThrough < st.nextOffset-1 {
			return failure("no-acks")
		}
		return nil
	}
	st.ackedThrough = st.nextOffset - 1
	return nil
}

// GetUnackedRecords returns the accepted records that are not acknowledged, in order.
func (st *FakeStream) GetUnackedRecords() ([]interface{}, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	var out []interface{}
	for _, r := range st.accepted {
		if r.offset > st.ackedThrough {
			out = append(out, r.payload)
		}
	}
	return out, nil
}

// Close marks the stream closed. Later ingests fail.
func (st *FakeStream) Close() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.closed = true
	return nil
}

// Ingested returns the records the stream accepted, in order.
func (st *FakeStream) Ingested() []interface{} {
	st.mu.Lock()
	defer st.mu.Unlock()
	out := make([]interface{}, 0, len(st.accepted))
	for _, r := range st.accepted {
		out = append(out, r.payload)
	}
	return out
}

// Stored returns what landed in the table, in order (a duplicate appears twice).
func (st *FakeStream) Stored() []interface{} {
	st.mu.Lock()
	defer st.mu.Unlock()
	return append([]interface{}(nil), st.stored...)
}

// Fallback returns rows routed to the fallback bucket by schema-change-while-open.
func (st *FakeStream) Fallback() []interface{} {
	st.mu.Lock()
	defer st.mu.Unlock()
	return append([]interface{}(nil), st.fallback...)
}

func (st *FakeStream) checkTerminal() error {
	if st.terminal != "" {
		return failure(st.terminal)
	}
	return nil
}
