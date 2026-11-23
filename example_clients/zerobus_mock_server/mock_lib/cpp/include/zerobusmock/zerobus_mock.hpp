// UNOFFICIAL: a community test fake of the Zerobus C++ SDK (zerobus::Sdk and zerobus::Stream), for unit tests.
// Not affiliated with Databricks. Nothing is sent anywhere. Version 0.1.0 (see CHANGELOG.md).
//
//   auto mock = std::make_shared<zerobusmock::FakeMock>();
//   zerobusmock::FakeSdk sdk(mock);
//   auto stream = sdk.create_stream({"main.default.events", {}}, "id", "secret");
//   mock->fail_next("schema-mismatch", 1, 2);
//
// Payloads are JSON strings (ingest_json_record). Proto and Arrow inputs, StreamOptions and the header
// provider overloads are not modeled. The scenario table is a copy of ../scenarios.json; the unit tests check it.
#ifndef ZEROBUS_MOCK_HPP
#define ZEROBUS_MOCK_HPP

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace zerobusmock {

// The scenarios.json contract_version this copy embeds.
inline constexpr int kContractVersion = 1;

// Same name and shape as the SDK's exception: a message plus a retryability flag.
class ZerobusException : public std::runtime_error {
 public:
  ZerobusException(std::string message, bool is_retryable)
      : std::runtime_error(std::move(message)), is_retryable_(is_retryable) {}

  bool is_retryable() const noexcept { return is_retryable_; }

 private:
  bool is_retryable_;
};

// One row of the scenario table. where is "create", "ingest", "flush", or "" when the caller sees no error.
struct ScenarioSpec {
  std::string where;
  std::string text;
  bool retried_by_sdk;
};

// The embedded copy of scenarios.json, keyed by scenario name.
const std::map<std::string, ScenarioSpec>& scenario_table();

// Same fields as the SDK's TableProperties. The fake ignores descriptor_proto.
struct TableProperties {
  std::string table_name;
  std::vector<std::uint8_t> descriptor_proto;
};

class FakeSdk;
class FakeStream;

// The scenario controls, shared by the SDK fake and its streams.
class FakeMock {
 public:
  FakeMock() = default;
  FakeMock(const FakeMock&) = delete;
  FakeMock& operator=(const FakeMock&) = delete;

  // After skipping `after` matching calls, the next `count` fail. One-shot.
  void fail_next(const std::string& scenario, int count, int after = 0);
  // Every matching call fails until clear_scenario().
  void set_scenario(const std::string& scenario);
  void clear_scenario();
  // The next `count` authenticated calls find the token expired.
  void expire_token(int count);
  // Clears fail-next budgets, the held scenario and the expiry budget.
  void reset();

 private:
  friend class FakeSdk;
  friend class FakeStream;

  struct Budget {
    int after;
    int left;
  };

  bool is_held(const std::string& scenario);
  bool take(const std::string& scenario);
  bool fire(const std::string& scenario);  // a held scenario fires on every call; else a budget may
  bool take_expiry();

  std::mutex mu_;
  std::map<std::string, Budget> budgets_;
  std::string held_;
  int expiry_ = 0;
};

// The fake of zerobus::Stream. Move-only, like the SDK's stream.
class FakeStream {
 public:
  ~FakeStream();
  FakeStream(FakeStream&& other) noexcept;
  FakeStream& operator=(FakeStream&& other) noexcept;
  FakeStream(const FakeStream&) = delete;
  FakeStream& operator=(const FakeStream&) = delete;

  // Returns the record's offset. Throws ZerobusException on failure.
  std::int64_t ingest_json_record(const std::string& json);
  // Ingests each record in order and returns the last offset, or -1 for an empty batch.
  std::int64_t ingest_json_records(const std::vector<std::string>& records);
  void wait_for_offset(std::int64_t offset);
  void flush();
  // The accepted JSON payloads that are not acknowledged, in order.
  std::vector<std::string> get_unacked_records();
  void close();
  bool is_closed() const;

  // Records the stream accepted (it returned an offset).
  std::vector<std::string> ingested() const;
  // What landed in the table; a retried abort shows up twice.
  std::vector<std::string> stored() const;
  // Rows routed to the fallback bucket by schema-change-while-open.
  std::vector<std::string> fallback() const;

 private:
  friend class FakeSdk;
  struct State;
  FakeStream(std::shared_ptr<FakeMock> mock, std::string table);

  std::unique_ptr<State> state_;
};

// The fake of zerobus::Sdk. create_stream mirrors Sdk::create_stream without StreamOptions.
class FakeSdk {
 public:
  explicit FakeSdk(std::shared_ptr<FakeMock> mock = nullptr);

  FakeStream create_stream(const TableProperties& table, const std::string& client_id,
                           const std::string& client_secret);

  std::shared_ptr<FakeMock> mock() const { return mock_; }

 private:
  std::shared_ptr<FakeMock> mock_;
};

}  // namespace zerobusmock

#endif  // ZEROBUS_MOCK_HPP
