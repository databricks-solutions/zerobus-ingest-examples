// SDK matrix client, pure Go (github.com/databricks/zerobus-sdk/purego, built from the monorepo source).
// Usage: client <format> <scenario> --table T --endpoint E --uc U --client-id ID --client-secret SEC
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/databricks/zerobus-sdk/purego/zerobus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
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

func field(name string, num int32, typ descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(num),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		Type:   typ.Enum(),
	}
}

// descriptor builds the message descriptor. The SDK takes it as a serialized DescriptorProto.
func descriptor(mismatch bool) (*descriptorpb.DescriptorProto, []byte, error) {
	d := &descriptorpb.DescriptorProto{
		Name: proto.String("SdkMatrix"),
		Field: []*descriptorpb.FieldDescriptorProto{
			field("device_name", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING),
			field("temp", 2, descriptorpb.FieldDescriptorProto_TYPE_INT32),
			field("humidity", 3, descriptorpb.FieldDescriptorProto_TYPE_INT64),
		},
	}
	if mismatch {
		d.Field = append(d.Field, field("region", 4, descriptorpb.FieldDescriptorProto_TYPE_STRING))
	}
	raw, err := proto.Marshal(d)
	return d, raw, err
}

func protoRecord(d *descriptorpb.DescriptorProto, i int) ([]byte, error) {
	file := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("sdk_matrix.proto"),
		Package:     proto.String("zbmatrix"),
		Syntax:      proto.String("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{d},
	}
	fd, err := protodesc.NewFile(file, nil)
	if err != nil {
		return nil, err
	}
	md := fd.Messages().ByName("SdkMatrix")
	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("device_name"), protoreflect.ValueOfString(fmt.Sprintf("m-%d", i)))
	msg.Set(md.Fields().ByName("temp"), protoreflect.ValueOfInt32(int32(i)))
	msg.Set(md.Fields().ByName("humidity"), protoreflect.ValueOfInt64(2))
	return proto.Marshal(msg)
}

func run(format, scenario string, args []string) (int, error) {
	ctx := context.Background()
	mismatch := scenario == "mismatch"
	sdk, err := zerobus.New(opt(args, "endpoint"), opt(args, "uc"))
	if err != nil {
		return 0, err
	}
	defer sdk.Close()
	table, id, secret := opt(args, "table"), opt(args, "client-id"), opt(args, "client-secret")

	var stream *zerobus.Stream
	var desc *descriptorpb.DescriptorProto
	switch format {
	case "json":
		stream, err = sdk.CreateStream(ctx, table, id, secret, zerobus.WithJSON())
	case "proto":
		var raw []byte
		desc, raw, err = descriptor(mismatch)
		if err != nil {
			return 0, err
		}
		stream, err = sdk.CreateStream(ctx, table, id, secret, zerobus.WithProto(raw))
	default:
		return 0, fmt.Errorf("format %s is not supported by the pure-Go SDK", format)
	}
	if err != nil {
		return 0, err
	}
	acked := 0
	for i := 0; i < n; i++ {
		var rec []byte
		if format == "json" {
			extra := ""
			if mismatch && i == mismatchAt {
				extra = `,"region":"eu"`
			}
			rec = []byte(fmt.Sprintf(`{"device_name":"m-%d","temp":%d,"humidity":2%s}`, i, i, extra))
		} else {
			rec, err = protoRecord(desc, i)
			if err != nil {
				return acked, err
			}
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
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: client <format> <scenario> --table ... ")
		os.Exit(2)
	}
	format, scenario := os.Args[1], os.Args[2]
	acked, err := run(format, scenario, os.Args[3:])
	res := map[string]any{"ok": err == nil, "error": "", "acked": acked}
	if err != nil {
		res["error"] = fmt.Sprintf("%T: %v", err, err)
	}
	b, _ := json.Marshal(res)
	fmt.Printf("ZB_RESULT %s\n", b)
}
