// Package zerobus is an UNOFFICIAL in-process fake of the pure-Go Zerobus SDK's API
// (github.com/databricks/zerobus-sdk/purego/zerobus): New, SDK.CreateStream, the Stream ingest and flush
// methods, and Error. Not affiliated with Databricks. Nothing is sent anywhere.
//
// The cgo fake in the parent package (zerobusmock) has the same scenario table and controls. This package
// wraps it, so both fakes share one behavior. The pure-Go SDK's method names and signatures differ from the
// cgo SDK's (context arguments, []byte payloads, StreamOption values, proto by default), so this adapter
// exposes the pure-Go shape. Import path: zerobusmock/purego.
//
//	mock := zerobusmock.NewFakeMock()
//	sdk, _ := zerobus.New("https://zerobus.example", "https://workspace.example", zerobus.WithMock(mock))
//	stream, err := sdk.CreateStream(ctx, "main.default.events", "id", "secret", zerobus.WithJSON())
//	mock.FailNext("schema-mismatch", 1, 2)
//
// Simplifications: New checks only that both endpoints are non-empty. Payload bytes are not validated
// as JSON or protobuf. WithMock, Ingested, Stored and Fallback are fake-only.
package zerobus

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"zerobusmock"
)

// Error is the error type the pure-Go SDK returns from SDK and Stream calls. It has the same fields and methods.
type Error struct {
	// Op is the SDK operation that failed (e.g. "CreateStream", "Flush").
	Op string
	// cause is the underlying error.
	cause error
	// retryable records whether the failure is transient.
	retryable bool
}

func (e *Error) Error() string {
	if e.Op == "" {
		return e.cause.Error()
	}
	return "zerobus: " + e.Op + ": " + e.cause.Error()
}

// Unwrap exposes the underlying cause for errors.Is and errors.As.
func (e *Error) Unwrap() error { return e.cause }

// Retryable reports whether the failure is transient.
func (e *Error) Retryable() bool { return e.retryable }

// Retryable reports whether err (or any error it wraps) is transient. Same contract as the SDK's package function.
func Retryable(err error) bool {
	if err == nil {
		return false
	}
	var e *Error
	if errors.As(err, &e) {
		return e.retryable
	}
	var ze *zerobusmock.ZerobusError
	if errors.As(err, &ze) {
		return ze.IsRetryable
	}
	return false
}

// wrapErr tags err with the operation name, keeping an error that is already an *Error.
func wrapErr(op string, err error) error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		if e.Op == "" {
			e.Op = op
		}
		return err
	}
	return &Error{Op: op, cause: err, retryable: Retryable(err)}
}

type sdkConfig struct {
	mock *zerobusmock.FakeMock
}

// Option configures New.
type Option func(*sdkConfig)

// WithMock shares the scenario controls with the test. Without it, New makes a fresh mock. Fake only.
func WithMock(mock *zerobusmock.FakeMock) Option {
	return func(c *sdkConfig) { c.mock = mock }
}

type recordType int

const (
	recordTypeProto recordType = iota // the pure-Go SDK's default
	recordTypeJSON
)

type streamConfig struct {
	recordType recordType
	descriptor []byte
}

// StreamOption configures CreateStream.
type StreamOption func(*streamConfig)

// WithJSON makes the stream a JSON stream.
func WithJSON() StreamOption {
	return func(c *streamConfig) { c.recordType = recordTypeJSON }
}

// WithProto makes the stream a protobuf stream with the given descriptor. An empty descriptor is rejected at CreateStream.
func WithProto(descriptorProto []byte) StreamOption {
	return func(c *streamConfig) {
		c.recordType = recordTypeProto
		c.descriptor = descriptorProto
	}
}

// SDK is the fake of the pure-Go SDK's client.
type SDK struct {
	fake   *zerobusmock.FakeZerobusSdk
	closed bool
}

// New returns a fake SDK. Both endpoints must be non-empty.
func New(zerobusEndpoint, ucEndpoint string, opts ...Option) (*SDK, error) {
	if strings.TrimSpace(zerobusEndpoint) == "" || strings.TrimSpace(ucEndpoint) == "" {
		return nil, &Error{Op: "New", cause: errors.New("zerobus and Unity Catalog endpoints are required"), retryable: false}
	}
	cfg := sdkConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &SDK{fake: zerobusmock.NewFakeZerobusSdk(cfg.mock)}, nil
}

// Mock returns the scenario controls for this SDK.
func (s *SDK) Mock() *zerobusmock.FakeMock { return s.fake.Mock() }

// Close marks the SDK closed. Later CreateStream calls fail.
func (s *SDK) Close() error {
	s.closed = true
	return nil
}

// CreateStream mirrors the pure-Go SDK. It rejects a canceled context, an empty table name, and a stream that
// is proto (the default) without a descriptor, as validateStreamArgs does in the SDK.
func (s *SDK) CreateStream(ctx context.Context, tableName, clientID, clientSecret string, opts ...StreamOption) (*Stream, error) {
	if s.closed {
		return nil, &Error{Op: "CreateStream", cause: errors.New("SDK is closed"), retryable: false}
	}
	cfg := streamConfig{recordType: recordTypeProto}
	for _, opt := range opts {
		opt(&cfg)
	}
	if strings.TrimSpace(tableName) == "" {
		return nil, &Error{Op: "CreateStream", cause: errors.New("table name is required"), retryable: false}
	}
	if cfg.recordType == recordTypeProto && len(cfg.descriptor) == 0 {
		return nil, &Error{Op: "CreateStream", cause: errors.New("WithProto requires a non-empty descriptor proto"), retryable: false}
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, &Error{Op: "CreateStream", cause: err, retryable: false}
		}
	}
	inner, err := s.fake.CreateStream(zerobusmock.TableProperties{TableName: tableName, DescriptorProto: cfg.descriptor}, clientID, clientSecret, nil)
	if err != nil {
		return nil, wrapErr("CreateStream", err)
	}
	return &Stream{inner: inner, recordType: cfg.recordType}, nil
}

// Stream is the fake of the pure-Go SDK's stream.
type Stream struct {
	inner      *zerobusmock.FakeStream
	recordType recordType
	closed     bool
}

// IngestRecordOffset queues one record and returns its offset. It returns -1 on error.
func (s *Stream) IngestRecordOffset(record []byte) (int64, error) {
	off, err := s.inner.IngestRecordOffset(record)
	return off, wrapErr("IngestRecordOffset", err)
}

// IngestRecordsOffset queues each record in order and returns the last offset.
func (s *Stream) IngestRecordsOffset(records [][]byte) (int64, error) {
	batch := make([]interface{}, 0, len(records))
	for _, r := range records {
		batch = append(batch, r)
	}
	off, err := s.inner.IngestRecordsOffset(batch)
	return off, wrapErr("IngestRecordsOffset", err)
}

// WaitForOffset returns once offset is acknowledged.
func (s *Stream) WaitForOffset(offset int64) error {
	return wrapErr("WaitForOffset", s.inner.WaitForOffset(offset))
}

// Flush waits until everything queued so far is acknowledged.
func (s *Stream) Flush() error {
	return wrapErr("Flush", s.inner.Flush())
}

// GetUnackedRecords returns the accepted records that are not acknowledged, in order.
func (s *Stream) GetUnackedRecords() ([][]byte, error) {
	recs, err := s.inner.GetUnackedRecords()
	if err != nil {
		return nil, wrapErr("GetUnackedRecords", err)
	}
	return toBytes(recs), nil
}

// Close closes the stream. Later ingests fail.
func (s *Stream) Close() error {
	s.closed = true
	return wrapErr("Close", s.inner.Close())
}

// IsClosed reports whether Close has been called.
func (s *Stream) IsClosed() bool { return s.closed }

// Ingested returns the records the stream accepted, in order. Fake only.
func (s *Stream) Ingested() [][]byte { return toBytes(s.inner.Ingested()) }

// Stored returns what landed in the table, in order. A retried abort shows up twice. Fake only.
func (s *Stream) Stored() [][]byte { return toBytes(s.inner.Stored()) }

// Fallback returns rows routed to the fallback bucket by schema-change-while-open. Fake only.
func (s *Stream) Fallback() [][]byte { return toBytes(s.inner.Fallback()) }

func toBytes(xs []interface{}) [][]byte {
	out := make([][]byte, 0, len(xs))
	for _, x := range xs {
		b, ok := x.([]byte)
		if !ok {
			panic(fmt.Sprintf("zerobusmock/purego: payload is %T, want []byte", x))
		}
		out = append(out, b)
	}
	return out
}
