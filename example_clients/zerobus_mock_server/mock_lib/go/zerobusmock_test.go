package zerobusmock

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

const table = "main.default.events"

func streamFor(t *testing.T, mock *FakeMock) *FakeStream {
	t.Helper()
	if mock == nil {
		mock = NewFakeMock()
	}
	st, err := NewFakeZerobusSdk(mock).CreateStream(TableProperties{TableName: table}, "id", "secret", nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return st
}

// mustErr asserts err is a ZerobusError whose message contains want.
func mustErr(t *testing.T, err error, want string) *ZerobusError {
	t.Helper()
	var ze *ZerobusError
	if !errors.As(err, &ze) {
		t.Fatalf("want *ZerobusError containing %q, got %v", want, err)
	}
	if !strings.Contains(ze.Message, want) {
		t.Fatalf("message %q lacks %q", ze.Message, want)
	}
	return ze
}

func TestScenarioTableMatchesScenariosJSON(t *testing.T) {
	raw, err := os.ReadFile("../scenarios.json")
	if err != nil {
		t.Fatal(err)
	}
	var contract struct {
		Scenarios map[string]struct {
			Where        *string `json:"where"`
			Message      *string `json:"message"`
			RetriedBySDK bool    `json:"retried_by_sdk"`
		} `json:"scenarios"`
	}
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatal(err)
	}
	var have, want []string
	for name := range scenarios {
		have = append(have, name)
	}
	for name := range contract.Scenarios {
		want = append(want, name)
	}
	sort.Strings(have)
	sort.Strings(want)
	if !reflect.DeepEqual(have, want) {
		t.Fatalf("scenario names differ: lib %v, contract %v", have, want)
	}
	for name, c := range contract.Scenarios {
		s := scenarios[name]
		wantWhere := ""
		if c.Where != nil {
			wantWhere = *c.Where
		}
		if s.where != wantWhere {
			t.Errorf("%s: where %q, contract %q", name, s.where, wantWhere)
		}
		if s.retriedBy != c.RetriedBySDK {
			t.Errorf("%s: retried %v, contract %v", name, s.retriedBy, c.RetriedBySDK)
		}
		if c.Message != nil && !strings.Contains(s.text, *c.Message) {
			t.Errorf("%s: %q lacks the contract message %q", name, s.text, *c.Message)
		}
	}
}

func TestRecordsAreRecordedInOrderWithOffsets(t *testing.T) {
	st := streamFor(t, nil)
	for i, want := range []int64{0, 1} {
		got, err := st.IngestRecordOffset(map[string]int{"n": i})
		if err != nil || got != want {
			t.Fatalf("ingest %d: offset %d err %v, want %d", i, got, err, want)
		}
	}
	if last, err := st.IngestRecordsOffset([]interface{}{"two", "three"}); err != nil || last != 3 {
		t.Fatalf("batch: %d %v", last, err)
	}
	if last, _ := st.IngestRecordsOffset(nil); last != -1 {
		t.Fatalf("empty batch should return -1, got %d", last)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := st.Ingested(); len(got) != 4 || got[2] != "two" {
		t.Fatalf("ingested %v", got)
	}
	if !reflect.DeepEqual(st.Stored(), st.Ingested()) {
		t.Fatalf("stored %v != ingested %v", st.Stored(), st.Ingested())
	}
	if unacked, _ := st.GetUnackedRecords(); len(unacked) != 0 {
		t.Fatalf("unacked after flush: %v", unacked)
	}
}

func TestFailNextAfterSkipsThenFailsOnce(t *testing.T) {
	mock := NewFakeMock()
	st := streamFor(t, mock)
	if err := mock.FailNext("schema-mismatch", 1, 2); err != nil {
		t.Fatal(err)
	}
	st.IngestRecordOffset("a")
	st.IngestRecordOffset("b")
	_, err := st.IngestRecordOffset("c")
	mustErr(t, err, "does not fit")
	if off, err := st.IngestRecordOffset("d"); err != nil || off != 2 {
		t.Fatalf("one-shot: next call should succeed, got %d %v", off, err)
	}
	if got := st.Ingested(); !reflect.DeepEqual(got, []interface{}{"a", "b", "d"}) {
		t.Fatalf("ingested %v", got)
	}
}

func TestRecordTooLargeRaisesAndIsNotStored(t *testing.T) {
	mock := NewFakeMock()
	st := streamFor(t, mock)
	mock.FailNext("record-too-large", 1, 0)
	_, err := st.IngestRecordOffset("big")
	mustErr(t, err, "maximum message size")
	if len(st.Stored()) != 0 {
		t.Fatalf("stored %v", st.Stored())
	}
}

func TestStreamAbortFatalIsTerminalAndKeepsEarlierRecords(t *testing.T) {
	mock := NewFakeMock()
	st := streamFor(t, mock)
	st.IngestRecordOffset("kept")
	mock.FailNext("stream-abort-fatal", 1, 0)
	_, err := st.IngestRecordOffset("x")
	mustErr(t, err, "terminal stream error")
	_, err = st.IngestRecordOffset("after")
	mustErr(t, err, "terminal stream error")
	mustErr(t, st.Flush(), "terminal stream error")
	if got := st.Stored(); !reflect.DeepEqual(got, []interface{}{"kept"}) {
		t.Fatalf("stored %v", got)
	}
	if off, err := streamFor(t, mock).IngestRecordOffset("new stream"); err != nil || off != 0 {
		t.Fatalf("a new stream should work: %d %v", off, err)
	}
}

func TestAuthFailureAndTableNotFoundRejectAtCreate(t *testing.T) {
	mock := NewFakeMock()
	sdk := NewFakeZerobusSdk(mock)
	mock.FailNext("auth-failure", 1, 0)
	_, err := sdk.CreateStream(TableProperties{TableName: table}, "id", "secret", nil)
	ze := mustErr(t, err, "invalid_client")
	if ze.Retryable() {
		t.Fatal("auth failure must not be retryable")
	}
	mock.FailNext("table-not-found", 1, 0)
	_, err = sdk.CreateStream(TableProperties{TableName: table}, "id", "secret", nil)
	mustErr(t, err, "cannot be found")
}

func TestServerUnavailableHoldsAtCreateUntilCleared(t *testing.T) {
	mock := NewFakeMock()
	sdk := NewFakeZerobusSdk(mock)
	if err := mock.SetScenario("server-unavailable"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		_, err := sdk.CreateStream(TableProperties{TableName: table}, "id", "secret", nil)
		ze := mustErr(t, err, "UNAVAILABLE")
		if !ze.Retryable() {
			t.Fatal("UNAVAILABLE is retried by the SDK, so it is retryable")
		}
	}
	mock.ClearScenario()
	if off, err := streamFor(t, mock).IngestRecordOffset("ok"); err != nil || off != 0 {
		t.Fatalf("after clear: %d %v", off, err)
	}
}

func TestTokenExpiryAtCreateIsRefreshedAndMidStreamIsTerminal(t *testing.T) {
	mock := NewFakeMock()
	mock.ExpireToken(1)
	st := streamFor(t, mock) // create refreshes the token: success
	mock.ExpireToken(1)
	_, err := st.IngestRecordOffset("late")
	mustErr(t, err, "expired")
	_, err = st.IngestRecordOffset("later")
	mustErr(t, err, "expired")
	if off, err := streamFor(t, mock).IngestRecordOffset("new"); err != nil || off != 0 {
		t.Fatalf("a new stream should work: %d %v", off, err)
	}
}

func TestNoAcksMakesFlushFailUntilClearedAndKeepsRecords(t *testing.T) {
	mock := NewFakeMock()
	st := streamFor(t, mock)
	mock.SetScenario("no-acks")
	st.IngestRecordOffset("a")
	mustErr(t, st.Flush(), "timed out")
	if !reflect.DeepEqual(st.Stored(), []interface{}{"a"}) {
		t.Fatalf("stored %v", st.Stored())
	}
	if unacked, _ := st.GetUnackedRecords(); !reflect.DeepEqual(unacked, []interface{}{"a"}) {
		t.Fatalf("unacked %v", unacked)
	}
	mock.ClearScenario()
	if err := st.Flush(); err != nil {
		t.Fatalf("flush after clear: %v", err)
	}
	if unacked, _ := st.GetUnackedRecords(); len(unacked) != 0 {
		t.Fatalf("unacked after clear+flush: %v", unacked)
	}
}

func TestGracefulCloseRaisesNothingAndStoresTheRecord(t *testing.T) {
	mock := NewFakeMock()
	st := streamFor(t, mock)
	mock.FailNext("graceful-close", 1, 0)
	if off, err := st.IngestRecordOffset("g"); err != nil || off != 0 {
		t.Fatalf("graceful close: %d %v", off, err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(st.Stored(), []interface{}{"g"}) {
		t.Fatalf("stored %v", st.Stored())
	}
}

func TestStreamAbortRetryableStoresTheRecordTwice(t *testing.T) {
	mock := NewFakeMock()
	st := streamFor(t, mock)
	mock.FailNext("stream-abort-retryable", 1, 1)
	st.IngestRecordOffset("once")
	if off, err := st.IngestRecordOffset("twice"); err != nil || off != 1 {
		t.Fatalf("no error expected: %d %v", off, err)
	}
	st.Flush()
	if got := st.Stored(); !reflect.DeepEqual(got, []interface{}{"once", "twice", "twice"}) {
		t.Fatalf("stored %v", got)
	}
	if got := st.Ingested(); !reflect.DeepEqual(got, []interface{}{"once", "twice"}) {
		t.Fatalf("ingested %v", got)
	}
}

func TestSchemaChangeWhileOpenRaisesNothingAndRoutesRowsToFallback(t *testing.T) {
	mock := NewFakeMock()
	st := streamFor(t, mock)
	st.IngestRecordOffset("before")
	mock.FailNext("schema-change-while-open", 1, 0)
	if _, err := st.IngestRecordOffset("during"); err != nil {
		t.Fatalf("no error expected: %v", err)
	}
	st.Flush()
	if !reflect.DeepEqual(st.Stored(), []interface{}{"before"}) {
		t.Fatalf("stored %v", st.Stored())
	}
	if !reflect.DeepEqual(st.Fallback(), []interface{}{"during"}) {
		t.Fatalf("fallback %v", st.Fallback())
	}
}

func TestResetClearsEverything(t *testing.T) {
	mock := NewFakeMock()
	mock.FailNext("auth-failure", 1, 0)
	mock.SetScenario("no-acks")
	mock.ExpireToken(2)
	mock.Reset()
	st := streamFor(t, mock)
	if off, err := st.IngestRecordOffset("fine"); err != nil || off != 0 {
		t.Fatalf("after reset: %d %v", off, err)
	}
	if err := st.Flush(); err != nil {
		t.Fatalf("flush after reset: %v", err)
	}
}

func TestUnknownScenarioIsRejectedAndTokenExpiredCannotBeHeld(t *testing.T) {
	mock := NewFakeMock()
	if err := mock.FailNext("no-such", 1, 0); err == nil {
		t.Fatal("unknown scenario accepted")
	}
	if err := mock.SetScenario("token-expired"); err == nil {
		t.Fatal("token-expired accepted as a held scenario")
	}
}
