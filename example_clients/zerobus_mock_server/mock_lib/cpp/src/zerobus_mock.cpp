// UNOFFICIAL: a community test fake. Not affiliated with Databricks. See include/zerobusmock/zerobus_mock.hpp.
#include "zerobusmock/zerobus_mock.hpp"

#include <initializer_list>
#include <utility>

namespace zerobusmock {
namespace {

const char* const kTokenExpired = "token-expired";

void require_known(const std::string& scenario) {
  if (scenario_table().count(scenario) == 0) {
    throw std::invalid_argument("unknown scenario \"" + scenario + "\"");
  }
}

// Throws the failure a scenario produces, as the SDK would raise it.
[[noreturn]] void throw_failure(const std::string& name) {
  const ScenarioSpec& spec = scenario_table().at(name);
  throw ZerobusException(spec.text, spec.retried_by_sdk);
}

}  // namespace

const std::map<std::string, ScenarioSpec>& scenario_table() {
  static const std::map<std::string, ScenarioSpec> table = {
      {"auth-failure", {"create", "invalid_client: token endpoint rejected the client", false}},
      {"table-not-found", {"create", "table cannot be found", false}},
      {"schema-mismatch", {"ingest", "record does not fit the table schema", false}},
      {"record-too-large", {"ingest", "record is over the maximum message size", false}},
      {"server-unavailable", {"create", "UNAVAILABLE: retries exhausted", true}},
      {"stream-abort-retryable", {"", "", true}},
      {"stream-abort-fatal", {"ingest", "terminal stream error: INVALID_ARGUMENT", false}},
      {"graceful-close", {"", "", true}},
      {"no-acks", {"flush", "flush timed out waiting for acknowledgements", false}},
      {"schema-change-while-open", {"", "", false}},
      {"token-expired", {"ingest", "token has expired", false}},
  };
  return table;
}

// ---------------------------------------------------------------- FakeMock

void FakeMock::fail_next(const std::string& scenario, int count, int after) {
  require_known(scenario);
  if (count < 1 || after < 0) {
    throw std::invalid_argument("count must be >= 1 and after >= 0");
  }
  std::lock_guard<std::mutex> lock(mu_);
  if (scenario == kTokenExpired) {
    if (after > 0) {
      throw std::invalid_argument("token-expired takes no after; use expire_token");
    }
    expiry_ += count;
    return;
  }
  budgets_[scenario] = Budget{after, count};
}

void FakeMock::set_scenario(const std::string& scenario) {
  require_known(scenario);
  if (scenario == kTokenExpired) {
    throw std::invalid_argument("token-expired is one-shot: use fail_next or expire_token");
  }
  std::lock_guard<std::mutex> lock(mu_);
  held_ = scenario;
}

void FakeMock::clear_scenario() {
  std::lock_guard<std::mutex> lock(mu_);
  held_.clear();
}

void FakeMock::expire_token(int count) {
  std::lock_guard<std::mutex> lock(mu_);
  expiry_ += count;
}

void FakeMock::reset() {
  std::lock_guard<std::mutex> lock(mu_);
  budgets_.clear();
  held_.clear();
  expiry_ = 0;
}

bool FakeMock::is_held(const std::string& scenario) {
  std::lock_guard<std::mutex> lock(mu_);
  return held_ == scenario;
}

bool FakeMock::take(const std::string& scenario) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = budgets_.find(scenario);
  if (it == budgets_.end()) return false;
  Budget& b = it->second;
  if (b.after > 0) {
    --b.after;
    return false;
  }
  if (b.left <= 0) return false;
  --b.left;
  return true;
}

bool FakeMock::fire(const std::string& scenario) { return is_held(scenario) || take(scenario); }

bool FakeMock::take_expiry() {
  std::lock_guard<std::mutex> lock(mu_);
  if (expiry_ > 0) {
    --expiry_;
    return true;
  }
  return false;
}

// ---------------------------------------------------------------- FakeSdk

FakeSdk::FakeSdk(std::shared_ptr<FakeMock> mock)
    : mock_(mock ? std::move(mock) : std::make_shared<FakeMock>()) {}

FakeStream FakeSdk::create_stream(const TableProperties& table, const std::string& /*client_id*/,
                                  const std::string& /*client_secret*/) {
  for (const char* name : {"auth-failure", "table-not-found", "server-unavailable"}) {
    if (mock_->fire(name)) throw_failure(name);
  }
  // Create refreshes an expired token, so the caller sees success.
  mock_->take_expiry();
  return FakeStream(mock_, table.table_name);
}

// ---------------------------------------------------------------- FakeStream

struct FakeStream::State {
  struct Accepted {
    std::int64_t offset;
    std::string payload;
  };

  std::shared_ptr<FakeMock> mock;
  std::string table;
  mutable std::mutex mu;
  std::vector<Accepted> accepted;
  std::vector<std::string> stored;
  std::vector<std::string> fallback;
  std::int64_t next_offset = 0;
  std::int64_t acked_through = -1;  // -1 before any ack
  std::string terminal;             // a scenario name once the stream has failed for good
  bool closed = false;

  // Raises the terminal failure, if any. Caller holds mu.
  void check_terminal() const {
    if (!terminal.empty()) throw_failure(terminal);
  }
};

FakeStream::FakeStream(std::shared_ptr<FakeMock> mock, std::string table)
    : state_(std::make_unique<State>()) {
  state_->mock = std::move(mock);
  state_->table = std::move(table);
}

FakeStream::~FakeStream() = default;
FakeStream::FakeStream(FakeStream&& other) noexcept = default;
FakeStream& FakeStream::operator=(FakeStream&& other) noexcept = default;

std::int64_t FakeStream::ingest_json_record(const std::string& json) {
  State& st = *state_;
  std::lock_guard<std::mutex> lock(st.mu);
  if (st.closed) throw ZerobusException("Stream has been closed", false);
  st.check_terminal();
  if (st.mock->take_expiry()) {
    st.terminal = kTokenExpired;
    throw_failure(kTokenExpired);
  }
  if (st.mock->fire("schema-mismatch")) throw_failure("schema-mismatch");
  if (st.mock->fire("record-too-large")) throw_failure("record-too-large");
  if (st.mock->fire("stream-abort-fatal")) {
    st.terminal = "stream-abort-fatal";
    throw_failure("stream-abort-fatal");
  }

  const std::int64_t offset = st.next_offset++;
  st.accepted.push_back({offset, json});
  if (!st.mock->is_held("no-acks")) st.acked_through = offset;

  if (st.mock->fire("schema-change-while-open")) {
    st.fallback.push_back(json);
    return offset;
  }
  st.stored.push_back(json);
  if (st.mock->fire("stream-abort-retryable")) {
    st.stored.push_back(json);  // the aborted copy was stored unacked; the SDK's resend stores it again
  }
  return offset;
}

std::int64_t FakeStream::ingest_json_records(const std::vector<std::string>& records) {
  std::int64_t last = -1;
  for (const std::string& r : records) last = ingest_json_record(r);
  return last;
}

void FakeStream::wait_for_offset(std::int64_t offset) {
  State& st = *state_;
  std::lock_guard<std::mutex> lock(st.mu);
  st.check_terminal();
  if (st.mock->take("no-acks") || (st.mock->is_held("no-acks") && offset > st.acked_through)) {
    throw_failure("no-acks");
  }
  if (offset > st.acked_through) st.acked_through = offset;
}

void FakeStream::flush() {
  State& st = *state_;
  std::lock_guard<std::mutex> lock(st.mu);
  st.check_terminal();
  if (st.mock->take("no-acks")) throw_failure("no-acks");
  if (st.mock->is_held("no-acks")) {
    if (st.acked_through < st.next_offset - 1) throw_failure("no-acks");
    return;
  }
  st.acked_through = st.next_offset - 1;
}

std::vector<std::string> FakeStream::get_unacked_records() {
  State& st = *state_;
  std::lock_guard<std::mutex> lock(st.mu);
  std::vector<std::string> out;
  for (const State::Accepted& r : st.accepted) {
    if (r.offset > st.acked_through) out.push_back(r.payload);
  }
  return out;
}

void FakeStream::close() {
  std::lock_guard<std::mutex> lock(state_->mu);
  state_->closed = true;
}

bool FakeStream::is_closed() const {
  std::lock_guard<std::mutex> lock(state_->mu);
  return state_->closed;
}

std::vector<std::string> FakeStream::ingested() const {
  std::lock_guard<std::mutex> lock(state_->mu);
  std::vector<std::string> out;
  for (const State::Accepted& r : state_->accepted) out.push_back(r.payload);
  return out;
}

std::vector<std::string> FakeStream::stored() const {
  std::lock_guard<std::mutex> lock(state_->mu);
  return state_->stored;
}

std::vector<std::string> FakeStream::fallback() const {
  std::lock_guard<std::mutex> lock(state_->mu);
  return state_->fallback;
}

}  // namespace zerobusmock
