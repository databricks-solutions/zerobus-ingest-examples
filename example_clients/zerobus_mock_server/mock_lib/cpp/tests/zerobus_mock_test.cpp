// UNOFFICIAL: unit tests of the community C++ fake. Not affiliated with Databricks.
// ZBMOCK_SCENARIOS_JSON is set by CMake to ../scenarios.json (mock_lib/scenarios.json).
#include <fstream>
#include <functional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "zerobusmock/zerobus_mock.hpp"

namespace zm = zerobusmock;

namespace {

const char* const kTable = "main.default.events";

zm::FakeStream stream_for(std::shared_ptr<zm::FakeMock> mock) {
  zm::FakeSdk sdk(std::move(mock));
  return sdk.create_stream({kTable, {}}, "id", "secret");
}

// True when f throws a ZerobusException whose message contains want. Reports the retryable flag.
bool fails_with(const std::function<void()>& f, const std::string& want, bool* retryable = nullptr) {
  try {
    f();
  } catch (const zm::ZerobusException& e) {
    if (retryable) *retryable = e.is_retryable();
    return std::string(e.what()).find(want) != std::string::npos;
  }
  return false;
}

std::string slurp(const std::string& path) {
  std::ifstream in(path);
  std::stringstream ss;
  ss << in.rdbuf();
  return ss.str();
}

}  // namespace

TEST(ZerobusMock, ScenarioTableMatchesScenariosJson) {
  const auto contract = nlohmann::json::parse(slurp(ZBMOCK_SCENARIOS_JSON));
  ASSERT_EQ(zm::kContractVersion, contract.at("contract_version").get<int>());

  const auto& table = zm::scenario_table();
  std::set<std::string> have, want;
  for (const auto& [name, spec] : table) have.insert(name);
  for (const auto& [name, spec] : contract.at("scenarios").items()) want.insert(name);
  EXPECT_EQ(want, have);

  for (const auto& [name, c] : contract.at("scenarios").items()) {
    const auto it = table.find(name);
    ASSERT_NE(it, table.end()) << name;
    const zm::ScenarioSpec& spec = it->second;
    const std::string where = c.at("where").is_null() ? "" : c.at("where").get<std::string>();
    EXPECT_EQ(where, spec.where) << name;
    EXPECT_EQ(c.at("retried_by_sdk").get<bool>(), spec.retried_by_sdk) << name;
    if (!c.at("message").is_null()) {
      EXPECT_NE(spec.text.find(c.at("message").get<std::string>()), std::string::npos)
          << name << ": \"" << spec.text << "\" lacks the contract message";
    }
  }
}

TEST(ZerobusMock, RecordsAreRecordedInOrderWithOffsets) {
  auto st = stream_for(std::make_shared<zm::FakeMock>());
  EXPECT_EQ(0, st.ingest_json_record("{\"n\":0}"));
  EXPECT_EQ(1, st.ingest_json_record("{\"n\":1}"));
  EXPECT_EQ(3, st.ingest_json_records({"two", "three"}));
  EXPECT_EQ(-1, st.ingest_json_records({}));
  st.flush();
  EXPECT_EQ(4u, st.ingested().size());
  EXPECT_EQ("two", st.ingested()[2]);
  EXPECT_EQ(st.ingested(), st.stored());
  EXPECT_TRUE(st.get_unacked_records().empty());
}

TEST(ZerobusMock, FailNextAfterSkipsThenFailsOnce) {
  auto mock = std::make_shared<zm::FakeMock>();
  auto st = stream_for(mock);
  mock->fail_next("schema-mismatch", 1, 2);
  st.ingest_json_record("a");
  st.ingest_json_record("b");
  bool retryable = true;
  EXPECT_TRUE(fails_with([&] { st.ingest_json_record("c"); }, "does not fit", &retryable));
  EXPECT_FALSE(retryable);
  EXPECT_EQ(2, st.ingest_json_record("d")) << "one-shot: the next call succeeds";
  EXPECT_EQ((std::vector<std::string>{"a", "b", "d"}), st.ingested());
}

TEST(ZerobusMock, RecordTooLargeRaisesAndIsNotStored) {
  auto mock = std::make_shared<zm::FakeMock>();
  auto st = stream_for(mock);
  mock->fail_next("record-too-large", 1, 0);
  EXPECT_TRUE(fails_with([&] { st.ingest_json_record("big"); }, "maximum message size"));
  EXPECT_TRUE(st.stored().empty());
}

TEST(ZerobusMock, StreamAbortFatalIsTerminalAndKeepsEarlierRecords) {
  auto mock = std::make_shared<zm::FakeMock>();
  auto st = stream_for(mock);
  st.ingest_json_record("kept");
  mock->fail_next("stream-abort-fatal", 1, 0);
  EXPECT_TRUE(fails_with([&] { st.ingest_json_record("x"); }, "terminal stream error"));
  EXPECT_TRUE(fails_with([&] { st.ingest_json_record("after"); }, "terminal stream error"));
  EXPECT_TRUE(fails_with([&] { st.flush(); }, "terminal stream error"));
  EXPECT_EQ((std::vector<std::string>{"kept"}), st.stored());
  EXPECT_EQ(0, stream_for(mock).ingest_json_record("new stream"));
}

TEST(ZerobusMock, AuthFailureAndTableNotFoundRejectAtCreate) {
  auto mock = std::make_shared<zm::FakeMock>();
  zm::FakeSdk sdk(mock);
  bool retryable = true;
  mock->fail_next("auth-failure", 1, 0);
  EXPECT_TRUE(fails_with([&] { sdk.create_stream({kTable, {}}, "id", "secret"); }, "invalid_client", &retryable));
  EXPECT_FALSE(retryable);
  mock->fail_next("table-not-found", 1, 0);
  EXPECT_TRUE(fails_with([&] { sdk.create_stream({kTable, {}}, "id", "secret"); }, "cannot be found"));
}

TEST(ZerobusMock, ServerUnavailableHoldsAtCreateUntilCleared) {
  auto mock = std::make_shared<zm::FakeMock>();
  zm::FakeSdk sdk(mock);
  mock->set_scenario("server-unavailable");
  for (int i = 0; i < 2; ++i) {
    bool retryable = false;
    EXPECT_TRUE(fails_with([&] { sdk.create_stream({kTable, {}}, "id", "secret"); }, "UNAVAILABLE", &retryable));
    EXPECT_TRUE(retryable) << "UNAVAILABLE is retried by the SDK, so it is retryable";
  }
  mock->clear_scenario();
  EXPECT_EQ(0, stream_for(mock).ingest_json_record("ok"));
}

TEST(ZerobusMock, TokenExpiryAtCreateIsRefreshedAndMidStreamIsTerminal) {
  auto mock = std::make_shared<zm::FakeMock>();
  mock->expire_token(1);
  auto st = stream_for(mock);  // create refreshes the token: success
  mock->expire_token(1);
  EXPECT_TRUE(fails_with([&] { st.ingest_json_record("late"); }, "expired"));
  EXPECT_TRUE(fails_with([&] { st.ingest_json_record("later"); }, "expired"));
  EXPECT_EQ(0, stream_for(mock).ingest_json_record("new"));
}

TEST(ZerobusMock, NoAcksMakesFlushFailUntilClearedAndKeepsRecords) {
  auto mock = std::make_shared<zm::FakeMock>();
  auto st = stream_for(mock);
  mock->set_scenario("no-acks");
  st.ingest_json_record("a");
  EXPECT_TRUE(fails_with([&] { st.flush(); }, "timed out"));
  EXPECT_EQ((std::vector<std::string>{"a"}), st.stored());
  EXPECT_EQ((std::vector<std::string>{"a"}), st.get_unacked_records());
  mock->clear_scenario();
  EXPECT_NO_THROW(st.flush());
  EXPECT_TRUE(st.get_unacked_records().empty());
}

TEST(ZerobusMock, GracefulCloseRaisesNothingAndStoresTheRecord) {
  auto mock = std::make_shared<zm::FakeMock>();
  auto st = stream_for(mock);
  mock->fail_next("graceful-close", 1, 0);
  EXPECT_EQ(0, st.ingest_json_record("g"));
  EXPECT_NO_THROW(st.flush());
  EXPECT_NO_THROW(st.close());
  EXPECT_EQ((std::vector<std::string>{"g"}), st.stored());
}

TEST(ZerobusMock, StreamAbortRetryableStoresTheRecordTwice) {
  auto mock = std::make_shared<zm::FakeMock>();
  auto st = stream_for(mock);
  mock->fail_next("stream-abort-retryable", 1, 1);
  st.ingest_json_record("once");
  EXPECT_EQ(1, st.ingest_json_record("twice")) << "no error expected";
  st.flush();
  EXPECT_EQ((std::vector<std::string>{"once", "twice", "twice"}), st.stored());
  EXPECT_EQ((std::vector<std::string>{"once", "twice"}), st.ingested());
}

TEST(ZerobusMock, SchemaChangeWhileOpenRaisesNothingAndRoutesRowsToFallback) {
  auto mock = std::make_shared<zm::FakeMock>();
  auto st = stream_for(mock);
  st.ingest_json_record("before");
  mock->fail_next("schema-change-while-open", 1, 0);
  EXPECT_NO_THROW(st.ingest_json_record("during"));
  st.flush();
  EXPECT_EQ((std::vector<std::string>{"before"}), st.stored());
  EXPECT_EQ((std::vector<std::string>{"during"}), st.fallback());
}

TEST(ZerobusMock, ResetClearsEverything) {
  auto mock = std::make_shared<zm::FakeMock>();
  mock->fail_next("auth-failure", 1, 0);
  mock->set_scenario("no-acks");
  mock->expire_token(2);
  mock->reset();
  auto st = stream_for(mock);
  EXPECT_EQ(0, st.ingest_json_record("fine"));
  EXPECT_NO_THROW(st.flush());
}

TEST(ZerobusMock, UnknownScenarioIsRejectedAndTokenExpiredCannotBeHeld) {
  zm::FakeMock mock;
  EXPECT_THROW(mock.fail_next("no-such", 1, 0), std::invalid_argument);
  EXPECT_THROW(mock.set_scenario("token-expired"), std::invalid_argument);
}

TEST(ZerobusMock, ClosedStreamRejectsIngest) {
  auto st = stream_for(std::make_shared<zm::FakeMock>());
  EXPECT_FALSE(st.is_closed());
  st.close();
  EXPECT_TRUE(st.is_closed());
  EXPECT_TRUE(fails_with([&] { st.ingest_json_record("late"); }, "Stream has been closed"));
}
