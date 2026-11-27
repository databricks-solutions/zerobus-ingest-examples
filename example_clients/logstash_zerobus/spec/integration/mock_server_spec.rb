# encoding: utf-8
# Failure tests against the Zerobus Mock Server (example_clients/zerobus_mock_server, a test double).
# Runs only when ZEROBUS_MOCK_URL is set, e.g. https://localhost:18443. The JVM must trust the
# mock's CA (see the Tests section of the README).
require "spec_helper"
require "json"
require "net/http"
require "logstash/codecs/base"
require "logstash/event"
require "logstash/outputs/zerobus"

MOCK_URL = ENV["ZEROBUS_MOCK_URL"]
# The table named in the mock config (README, Tests section): catalog.schema.table.
MOCK_TABLE = ENV["ZEROBUS_MOCK_TABLE"]
raise ArgumentError, "set ZEROBUS_MOCK_TABLE to the table named in the mock config" if MOCK_URL && MOCK_TABLE.nil?

def mock_call(method, path, body = nil)
  uri = URI("#{MOCK_URL}#{path}")
  http = Net::HTTP.new(uri.host, uri.port)
  http.use_ssl = true
  request = (method == :get ? Net::HTTP::Get : method == :delete ? Net::HTTP::Delete : Net::HTTP::Post).new(uri)
  if body
    request["Content-Type"] = "application/json"
    request.body = JSON.generate(body)
  end
  response = http.request(request)
  raise "mock #{method} #{path} -> HTTP #{response.code}: #{response.body}" unless response.code.to_i < 300
  response.body.to_s.empty? ? {} : JSON.parse(response.body)
end

RSpec.describe "logstash-output-zerobus against the Zerobus mock server", if: MOCK_URL do
  let(:plugin) do
    p = LogStash::Outputs::Zerobus.new(
      "workspace_url" => MOCK_URL, "zerobus_endpoint" => MOCK_URL, "workspace_id" => "42",
      "table" => MOCK_TABLE, "client_id" => "mock-client", "client_secret" => "mock-secret",
      "retry_backoff" => 0.01, "max_retries" => 3
    )
    p.register
    p
  end

  def event(sequence, message = "hello")
    LogStash::Event.new("message" => message, "sequence" => sequence)
  end

  def stored_sequences
    rows = mock_call(:get, "/_mock/tables/#{MOCK_TABLE}/rows?destination=table&limit=1000")
    (rows["rows"] || []).map { |r| r["values"]["sequence"] }
  end

  before do
    mock_call(:post, "/_mock/reset", { "tables" => [MOCK_TABLE], "schema" => false })
  end

  after do
    mock_call(:delete, "/_mock/scenario") rescue nil
  end

  it "writes a clean batch through the real token and insert path" do
    plugin.multi_receive([event(1), event(2), event(3)])
    expect(stored_sequences).to contain_exactly(1, 2, 3)
  end

  it "tags every event when the token endpoint refuses the client (token failure)" do
    mock_call(:post, "/_mock/scenario", { "scenario" => "auth-failure", "table" => "*" })
    events = [event(1), event(2)]
    plugin.multi_receive(events)
    events.each do |e|
      expect(e.get("[@metadata][zerobus][failure]")).to eq("auth")
      expect(e.get("tags")).to include("_zerobusfailure")
    end
    expect(stored_sequences).to be_empty
  end

  it "retries a 5xx from the server and then writes every record" do
    mock_call(:post, "/_mock/fail-next", { "scenario" => "server-unavailable", "count" => 2, "table" => "*" })
    events = [event(1), event(2), event(3)]
    plugin.multi_receive(events)
    expect(events.map { |e| e.get("tags") }.compact).to be_empty
    expect(stored_sequences).to contain_exactly(1, 2, 3)
  end

  it "keeps going after a 5xx that outlasts the retries: the events are tagged, the pipeline does not raise" do
    mock_call(:post, "/_mock/scenario", { "scenario" => "server-unavailable", "table" => "*" })
    e = event(1)
    expect { plugin.multi_receive([e]) }.not_to raise_error
    expect(e.get("[@metadata][zerobus][failure]")).to eq("delivery")
  end

  it "isolates a bad record: the good records are written and the bad one is tagged" do
    good_a = event(1)
    bad = LogStash::Event.new("message" => "bad", "sequence" => "not-a-number")
    good_b = event(3)
    plugin.multi_receive([good_a, bad, good_b])
    expect(stored_sequences).to contain_exactly(1, 3)
    expect(bad.get("tags")).to include("_zerobusfailure")
    expect(bad.get("[@metadata][zerobus][failure]")).to eq("rejected")
    expect(bad.get("[@metadata][zerobus][status]")).to be_between(400, 499)
    expect(good_a.get("tags")).to be_nil
  end

  it "refreshes an expired token on the next insert without failing the batch" do
    mock_call(:post, "/_mock/expire-token", { "count" => 1 })
    events = [event(1), event(2)]
    plugin.multi_receive(events)
    expect(events.map { |e| e.get("tags") }.compact).to be_empty
    expect(stored_sequences).to contain_exactly(1, 2)
  end
end
