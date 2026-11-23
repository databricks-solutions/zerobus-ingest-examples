// SDK matrix client, C++ (zerobus C++ SDK from the monorepo's cpp/, over the Rust core FFI).
// Usage: client <format> <scenario> --table T --endpoint E --uc U --client-id ID --client-secret SEC
// Prints: ZB_RESULT {"ok": bool, "error": str, "acked": int}
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "zerobus/sdk.hpp"

namespace {

constexpr int kN = 20;
constexpr int kMismatchAt = 5;

std::map<std::string, std::string> parse_opts(int argc, char** argv, int from) {
  std::map<std::string, std::string> opts;
  for (int i = from; i + 1 < argc; i += 2) opts[std::string(argv[i]).substr(2)] = argv[i + 1];
  return opts;
}

void put_varint(std::vector<std::uint8_t>& out, std::uint64_t v) {
  while (v >= 0x80) {
    out.push_back(static_cast<std::uint8_t>((v & 0x7f) | 0x80));
    v >>= 7;
  }
  out.push_back(static_cast<std::uint8_t>(v));
}

void put_str(std::vector<std::uint8_t>& out, int field, const std::string& s) {
  out.push_back(static_cast<std::uint8_t>((field << 3) | 2));
  put_varint(out, s.size());
  out.insert(out.end(), s.begin(), s.end());
}

void put_int(std::vector<std::uint8_t>& out, int field, std::uint64_t v) {
  out.push_back(static_cast<std::uint8_t>((field << 3) | 0));
  put_varint(out, v);
}

std::vector<std::uint8_t> field_descriptor(const std::string& name, int number, int type) {
  // FieldDescriptorProto: name=1, number=3, label=4 (1 = optional), type=5
  std::vector<std::uint8_t> f;
  put_str(f, 1, name);
  put_int(f, 3, number);
  put_int(f, 4, 1);
  put_int(f, 5, type);
  return f;
}

std::vector<std::uint8_t> proto_descriptor(bool mismatch) {
  // DescriptorProto: name=1, field=2 (repeated)
  constexpr int kString = 9, kInt32 = 5, kInt64 = 3;
  std::vector<std::uint8_t> d;
  put_str(d, 1, "SdkMatrix");
  auto add = [&d](const std::vector<std::uint8_t>& f) {
    d.push_back(static_cast<std::uint8_t>((2 << 3) | 2));
    put_varint(d, f.size());
    d.insert(d.end(), f.begin(), f.end());
  };
  add(field_descriptor("device_name", 1, kString));
  add(field_descriptor("temp", 2, kInt32));
  add(field_descriptor("humidity", 3, kInt64));
  if (mismatch) add(field_descriptor("region", 4, kString));
  return d;
}

std::vector<std::uint8_t> proto_record(int i) {
  std::vector<std::uint8_t> r;
  put_str(r, 1, "m-" + std::to_string(i));
  put_int(r, 2, static_cast<std::uint64_t>(i));
  put_int(r, 3, 2);
  return r;
}

std::string json_record(int i, bool mismatch) {
  std::string extra = (mismatch && i == kMismatchAt) ? ",\"region\":\"eu\"" : "";
  return "{\"device_name\":\"m-" + std::to_string(i) + "\",\"temp\":" + std::to_string(i) +
         ",\"humidity\":2" + extra + "}";
}

std::vector<std::uint8_t> read_file(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) throw std::runtime_error("cannot read fixture " + path);
  return std::vector<std::uint8_t>(std::istreambuf_iterator<char>(in), {});
}

int run(const std::string& format, bool mismatch, const std::map<std::string, std::string>& o) {
  auto sdk = zerobus::SdkBuilder()
                 .endpoint(o.at("endpoint"))
                 .unity_catalog_url(o.at("uc"))
                 .build();
  const std::string& table = o.at("table");
  const std::string& id = o.at("client-id");
  const std::string& secret = o.at("client-secret");
  int acked = 0;
  if (format == "json") {
    zerobus::TableProperties props{table, {}};
    zerobus::StreamOptions opts;
    opts.record_type = zerobus::RecordType::Json;  // StreamOptions defaults to Proto
    auto stream = sdk.create_stream(props, id, secret, opts);
    for (int i = 0; i < kN; ++i) {
      stream.ingest_json_record(json_record(i, mismatch));
      ++acked;
    }
    stream.flush();
    stream.close();
  } else if (format == "proto") {
    zerobus::TableProperties props{table, proto_descriptor(mismatch)};
    auto stream = sdk.create_stream(props, id, secret);
    for (int i = 0; i < kN; ++i) {
      stream.ingest_proto_record(proto_record(i));
      ++acked;
    }
    stream.flush();
    stream.close();
  } else if (format == "arrow") {
    const std::string dir = o.count("fixtures") ? o.at("fixtures") : std::string("/cache/fixtures");
    auto schema = read_file(dir + (mismatch ? "/schema_mismatch.bin" : "/schema.bin"));
    auto stream = sdk.create_arrow_stream(table, schema, id, secret);
    for (int k = 0; k < kN / 4; ++k) {
      stream.ingest_batch(read_file(dir + "/batch_" + std::to_string(k) + ".bin"));
      acked += 4;
    }
    stream.flush();
    stream.close();
  } else {
    throw std::runtime_error("format " + format + " is not supported by the C++ SDK");
  }
  return acked;
}

std::string escape(const std::string& s) {
  std::string out;
  for (char c : s) {
    if (c == '"' || c == '\\') out += '\\';
    out += (c == '\n') ? ' ' : c;
  }
  return out;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 3) {
    std::fprintf(stderr, "usage: client <format> <scenario> --table ...\n");
    return 2;
  }
  const std::string format = argv[1];
  const std::string scenario = argv[2];
  auto opts = parse_opts(argc, argv, 3);
  bool ok = false;
  std::string error;
  int acked = 0;
  try {
    acked = run(format, scenario == "mismatch", opts);
    ok = true;
  } catch (const std::exception& e) {
    error = e.what();
  }
  std::printf("ZB_RESULT {\"ok\": %s, \"error\": \"%s\", \"acked\": %d}\n", ok ? "true" : "false",
              escape(error).c_str(), acked);
  return 0;
}
