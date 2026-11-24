// SDK matrix client, Go with cgo (github.com/databricks/zerobus-sdk/go: a cgo binding over the Rust core).
// Usage: client <format> <scenario> --table T --endpoint E --uc U --client-id ID --client-secret SEC
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	zerobus "github.com/databricks/zerobus-sdk/go"
)

const (
	n          = 20
	mismatchAt = 5
)

func opt(args []string, key string) string {
	for i := 0; i+1 < len(args); i += 2 {
		if args[i] == "--"+key {
			return args[i+1]
		}
	}
	return ""
}

func varint(v uint64, out []byte) []byte {
	for v >= 0x80 {
		out = append(out, byte(v&0x7f)|0x80)
		v >>= 7
	}
	return append(out, byte(v))
}

func strField(field int, s string, out []byte) []byte {
	out = append(out, byte(field<<3|2))
	out = varint(uint64(len(s)), out)
	return append(out, s...)
}

func intField(field int, v uint64, out []byte) []byte {
	out = append(out, byte(field<<3|0))
	return varint(v, out)
}

// fieldDescriptor is a FieldDescriptorProto: name=1, number=3, label=4 (1 = optional), type=5.
func fieldDescriptor(name string, number, typ int) []byte {
	var f []byte
	f = strField(1, name, f)
	f = intField(3, uint64(number), f)
	f = intField(4, 1, f)
	return intField(5, uint64(typ), f)
}

// descriptorProto is a DescriptorProto (the message): name=1, field=2.
func descriptorProto(mismatch bool) []byte {
	const kString, kInt32, kInt64 = 9, 5, 3
	var d []byte
	d = strField(1, "SdkMatrix", d)
	add := func(f []byte) {
		d = append(d, byte(2<<3|2))
		d = varint(uint64(len(f)), d)
		d = append(d, f...)
	}
	add(fieldDescriptor("device_name", 1, kString))
	add(fieldDescriptor("temp", 2, kInt32))
	add(fieldDescriptor("humidity", 3, kInt64))
	if mismatch {
		add(fieldDescriptor("region", 4, kString))
	}
	return d
}

func protoRecord(i int) []byte {
	var r []byte
	r = strField(1, fmt.Sprintf("m-%d", i), r)
	r = intField(2, uint64(i), r)
	return intField(3, 2, r)
}

func run(format string, mismatch bool, args []string) (int, error) {
	sdk, err := zerobus.NewZerobusSdk(opt(args, "endpoint"), opt(args, "uc"))
	if err != nil {
		return 0, err
	}
	table, id, secret := opt(args, "table"), opt(args, "client-id"), opt(args, "client-secret")
	acked := 0
	switch format {
	case "json", "proto":
		props := zerobus.TableProperties{TableName: table}
		opts := zerobus.DefaultStreamConfigurationOptions()
		if format == "json" {
			opts.RecordType = zerobus.RecordTypeJson
		} else {
			props.DescriptorProto = descriptorProto(mismatch)
			opts.RecordType = zerobus.RecordTypeProto
		}
		stream, err := sdk.CreateStream(props, id, secret, opts)
		if err != nil {
			return 0, err
		}
		for i := 0; i < n; i++ {
			var rec interface{}
			if format == "json" {
				extra := ""
				if mismatch && i == mismatchAt {
					extra = `,"region":"eu"`
				}
				rec = fmt.Sprintf(`{"device_name":"m-%d","temp":%d,"humidity":2%s}`, i, i, extra)
			} else {
				rec = protoRecord(i)
			}
			if _, err := stream.IngestRecordOffset(rec); err != nil {
				if ferr := stream.Flush(); ferr != nil {
					return acked, ferr
				}
				return acked, err
			}
			acked++
		}
		if err := stream.Flush(); err != nil {
			return acked, err
		}
		return acked, stream.Close()
	case "arrow":
		dir := opt(args, "fixtures")
		if dir == "" {
			dir = "/cache/fixtures"
		}
		name := "schema.bin"
		if mismatch {
			name = "schema_mismatch.bin"
		}
		schema, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return 0, err
		}
		stream, err := sdk.CreateArrowStream(table, schema, id, secret, zerobus.DefaultArrowStreamConfigurationOptions())
		if err != nil {
			return 0, err
		}
		for k := 0; k < n/4; k++ {
			batch, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("batch_%d.bin", k)))
			if err != nil {
				return acked, err
			}
			if _, err := stream.IngestBatch(batch); err != nil {
				return acked, err
			}
			acked += 4
		}
		if err := stream.Flush(); err != nil {
			return acked, err
		}
		return acked, stream.Close()
	}
	return 0, fmt.Errorf("format %s is not supported by the cgo Go SDK", format)
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: client <format> <scenario> --table ...")
		os.Exit(2)
	}
	acked, err := run(os.Args[1], os.Args[2] == "mismatch", os.Args[3:])
	res := map[string]any{"ok": err == nil, "error": "", "acked": acked}
	if err != nil {
		res["error"] = err.Error()
	}
	b, _ := json.Marshal(res)
	fmt.Printf("ZB_RESULT %s\n", b)
}
