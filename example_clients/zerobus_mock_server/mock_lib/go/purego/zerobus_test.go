// UNOFFICIAL: unit tests of the pure-Go adapter of the community Go fake. Not affiliated with Databricks.
package zerobus

import (
	"context"
	"errors"
	"strings"
	"testing"

	"zerobusmock"
)

const table = "main.default.events"

// The method signatures of github.com/databricks/zerobus-sdk/purego/zerobus (commit 857391c), as interfaces.
// Compiling this file fails if the adapter's shape drifts from them.
var (
	_ interface {
		CreateStream(context.Context, string, string, string, ...StreamOption) (*Stream, error)
		Close() error
	} = (*SDK)(nil)
	_ interface {
		IngestRecordOffset([]byte) (int64, error)
		IngestRecordsOffset([][]byte) (int64, error)
		WaitForOffset(int64) error
		Flush() error
		GetUnackedRecords() ([][]byte, error)
		Close() error
		IsClosed() bool
	} = (*Stream)(nil)
)

func sdkFor(t *testing.T, mock *zerobusmock.FakeMock) *SDK {
	t.Helper()
	sdk, err := New("https://zerobus.example", "https://workspace.example", WithMock(mock))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return sdk
}

func streamFor(t *testing.T, mock *zerobusmock.FakeMock) *Stream {
	t.Helper()
	st, err := sdkFor(t, mock).CreateStream(context.Background(), table, "id", "secret", WithJSON())
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return st
}

// mustErr asserts err wraps an *Error whose message contains want, and returns it.
func mustErr(t *testing.T, err error, want string) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("want *Error containing %q, got %v", want, err)
	}
	if !strings.Contains(e.Error(), want) {
		t.Fatalf("error %q lacks %q", e.Error(), want)
	}
	return e
}

func TestDefaultStreamIsProtoAndNeedsADescriptor(t *testing.T) {
	sdk := sdkFor(t, zerobusmock.NewFakeMock())
	_, err := sdk.CreateStream(context.Background(), table, "id", "secret")
	e := mustErr(t, err, "WithProto requires a non-empty descriptor proto")
	if e.Op != "CreateStream" || e.Retryable() {
		t.Fatalf("op %q retryable %v", e.Op, e.Retryable())
	}
	_, err = sdk.CreateStream(context.Background(), table, "id", "secret", WithProto(nil))
	mustErr(t, err, "non-empty descriptor")
	if _, err := sdk.CreateStream(context.Background(), table, "id", "secret", WithProto([]byte{0x0a})); err != nil {
		t.Fatalf("WithProto with a descriptor: %v", err)
	}
	if _, err := sdk.CreateStream(context.Background(), table, "id", "secret", WithJSON()); err != nil {
		t.Fatalf("WithJSON: %v", err)
	}
}

func TestIngestAndFlushTakeBytes(t *testing.T) {
	st := streamFor(t, zerobusmock.NewFakeMock())
	if off, err := st.IngestRecordOffset([]byte(`{"n":0}`)); err != nil || off != 0 {
		t.Fatalf("first: %d %v", off, err)
	}
	if off, err := st.IngestRecordOffset([]byte(`{"n":1}`)); err != nil || off != 1 {
		t.Fatalf("second: %d %v", off, err)
	}
	if last, err := st.IngestRecordsOffset([][]byte{[]byte("two"), []byte("three")}); err != nil || last != 3 {
		t.Fatalf("batch: %d %v", last, err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := st.Ingested(); len(got) != 4 || string(got[2]) != "two" {
		t.Fatalf("ingested %q", got)
	}
	if unacked, err := st.GetUnackedRecords(); err != nil || len(unacked) != 0 {
		t.Fatalf("unacked after flush: %q %v", unacked, err)
	}
}

func TestRetryableFollowsTheScenarioTable(t *testing.T) {
	mock := zerobusmock.NewFakeMock()
	sdk := sdkFor(t, mock)
	mock.FailNext("auth-failure", 1, 0)
	_, err := sdk.CreateStream(context.Background(), table, "id", "secret", WithJSON())
	mustErr(t, err, "invalid_client")
	if Retryable(err) {
		t.Fatal("auth failure must not be retryable")
	}
	mock.SetScenario("server-unavailable")
	_, err = sdk.CreateStream(context.Background(), table, "id", "secret", WithJSON())
	mustErr(t, err, "UNAVAILABLE")
	if !Retryable(err) {
		t.Fatal("UNAVAILABLE is retried by the SDK, so it is retryable")
	}
}

func TestSchemaMismatchIsOneShotAndNotRetryable(t *testing.T) {
	mock := zerobusmock.NewFakeMock()
	st := streamFor(t, mock)
	mock.FailNext("schema-mismatch", 1, 0)
	off, err := st.IngestRecordOffset([]byte("bad"))
	e := mustErr(t, err, "does not fit")
	if off != -1 || e.Op != "IngestRecordOffset" || e.Retryable() {
		t.Fatalf("offset %d op %q retryable %v", off, e.Op, e.Retryable())
	}
	if off, err := st.IngestRecordOffset([]byte("good")); err != nil || off != 0 {
		t.Fatalf("one-shot: next call should succeed, got %d %v", off, err)
	}
	if got := st.Stored(); len(got) != 1 || string(got[0]) != "good" {
		t.Fatalf("stored %q", got)
	}
}

func TestNoAcksFailsFlushAndKeepsUnackedRecords(t *testing.T) {
	mock := zerobusmock.NewFakeMock()
	st := streamFor(t, mock)
	mock.SetScenario("no-acks")
	st.IngestRecordOffset([]byte("a"))
	mustErr(t, st.Flush(), "timed out")
	unacked, err := st.GetUnackedRecords()
	if err != nil || len(unacked) != 1 || string(unacked[0]) != "a" {
		t.Fatalf("unacked %q %v", unacked, err)
	}
	mock.ClearScenario()
	if err := st.Flush(); err != nil {
		t.Fatalf("flush after clear: %v", err)
	}
}

func TestStreamAbortFatalIsTerminal(t *testing.T) {
	mock := zerobusmock.NewFakeMock()
	st := streamFor(t, mock)
	st.IngestRecordOffset([]byte("kept"))
	mock.FailNext("stream-abort-fatal", 1, 0)
	_, err := st.IngestRecordOffset([]byte("x"))
	mustErr(t, err, "terminal stream error")
	_, err = st.IngestRecordOffset([]byte("after"))
	mustErr(t, err, "terminal stream error")
	mustErr(t, st.Flush(), "terminal stream error")
	if got := st.Stored(); len(got) != 1 {
		t.Fatalf("stored %q", got)
	}
}

func TestStreamAbortRetryableStoresTheRecordTwice(t *testing.T) {
	mock := zerobusmock.NewFakeMock()
	st := streamFor(t, mock)
	mock.FailNext("stream-abort-retryable", 1, 0)
	if _, err := st.IngestRecordOffset([]byte("twice")); err != nil {
		t.Fatalf("no error expected: %v", err)
	}
	if got := st.Stored(); len(got) != 2 || string(got[0]) != "twice" || string(got[1]) != "twice" {
		t.Fatalf("stored %q", got)
	}
}

func TestCloseRejectsLaterIngestAndCreate(t *testing.T) {
	mock := zerobusmock.NewFakeMock()
	sdk := sdkFor(t, mock)
	st, err := sdk.CreateStream(context.Background(), table, "id", "secret", WithJSON())
	if err != nil {
		t.Fatal(err)
	}
	if st.IsClosed() {
		t.Fatal("new stream reports closed")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if !st.IsClosed() {
		t.Fatal("closed stream reports open")
	}
	_, err = st.IngestRecordOffset([]byte("late"))
	mustErr(t, err, "Stream has been closed")

	if err := sdk.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = sdk.CreateStream(context.Background(), table, "id", "secret", WithJSON())
	mustErr(t, err, "SDK is closed")
}

func TestCanceledContextRejectsCreate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := sdkFor(t, zerobusmock.NewFakeMock()).CreateStream(ctx, table, "id", "secret", WithJSON())
	e := mustErr(t, err, "context canceled")
	if !errors.Is(e, context.Canceled) {
		t.Fatalf("want context.Canceled in the chain, got %v", err)
	}
}

func TestNewRequiresBothEndpoints(t *testing.T) {
	if _, err := New("", "https://workspace.example"); err == nil {
		t.Fatal("empty Zerobus endpoint accepted")
	}
	if _, err := New("https://zerobus.example", "  "); err == nil {
		t.Fatal("blank Unity Catalog endpoint accepted")
	}
}

func TestSharedMockControlsTheSDK(t *testing.T) {
	mock := zerobusmock.NewFakeMock()
	sdk := sdkFor(t, mock)
	if sdk.Mock() != mock {
		t.Fatal("Mock() should return the shared controls")
	}
}
