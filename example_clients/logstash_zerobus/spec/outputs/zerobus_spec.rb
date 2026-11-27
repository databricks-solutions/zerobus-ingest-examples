# encoding: utf-8
require "spec_helper"
require "logstash/codecs/base"
require "logstash/event"
require "logstash/outputs/zerobus"

# A client double: records each chunk; rejects any chunk that contains a record with "bad" set.
class ChunkClient
  attr_reader :chunks

  def initialize(fail_auth: false, fail_transient: false)
    @chunks = []
    @fail_auth = fail_auth
    @fail_transient = fail_transient
  end

  def insert(records)
    raise LogStash::Outputs::ZerobusRest::AuthError.new("refused", 401) if @fail_auth
    raise LogStash::Outputs::ZerobusRest::TransientError.new("down", 503) if @fail_transient
    if records.any? { |r| r["bad"] }
      raise LogStash::Outputs::ZerobusRest::RejectedError.new("insert rejected (HTTP 400): field mismatch", 400)
    end
    @chunks << records
    nil
  end

  def close; end
end

RSpec.describe LogStash::Outputs::Zerobus do
  let(:base) do
    { "workspace_url" => "https://ws.example.com", "zerobus_endpoint" => "42.zerobus.example.com",
      "table" => "main.sales.orders", "client_id" => "cid", "client_secret" => "sec",
      "batch_size" => 3 }
  end

  def build(overrides = {})
    plugin = described_class.new(base.merge(overrides))
    plugin.register
    plugin
  end

  def install_client(plugin, client)
    plugin.instance_variable_set(:@client, client)
    client
  end

  def event(hash = {})
    LogStash::Event.new({ "message" => "m" }.merge(hash))
  end

  it "derives the workspace id from the endpoint's first label and keeps the secret masked" do
    plugin = build
    expect(plugin.instance_variable_get(:@workspace_id)).to eq("42")
    expect(plugin.client_secret.to_s).to eq("<password>")
  end

  it "rejects a table name that is not catalog.schema.table" do
    expect { build("table" => "orders") }.to raise_error(LogStash::ConfigurationError, /catalog.schema.table/)
  end

  it "sends a Logstash batch in chunks of batch_size" do
    plugin = build
    client = install_client(plugin, ChunkClient.new)
    plugin.multi_receive((1..7).map { |i| event("n" => i) })
    expect(client.chunks.map(&:size)).to eq([3, 3, 1])
  end

  it "drops @version, writes the timestamp as ISO 8601 UTC under timestamp_field, and leaves @metadata out" do
    plugin = build
    client = install_client(plugin, ChunkClient.new)
    e = event("n" => 1)
    e.set("[@metadata][secret_note]", "x")
    plugin.multi_receive([e])
    record = client.chunks.first.first
    expect(record).not_to have_key("@version")
    expect(record).not_to have_key("@timestamp")
    expect(record).not_to have_key("@metadata")
    expect(record["event_timestamp"]).to match(/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\z/)
    expect(record["message"]).to eq("m")
  end

  it "omits the timestamp when timestamp_field is empty" do
    plugin = build("timestamp_field" => "")
    client = install_client(plugin, ChunkClient.new)
    plugin.multi_receive([event("n" => 1)])
    expect(client.chunks.first.first.keys).not_to include("event_timestamp", "@timestamp")
  end

  it "isolates a bad record: the rest of the chunk is written and the bad one is tagged" do
    plugin = build
    client = install_client(plugin, ChunkClient.new)
    good1 = event("n" => 1)
    bad = event("n" => 2, "bad" => true)
    good2 = event("n" => 3)
    plugin.multi_receive([good1, bad, good2])
    written = client.chunks.flatten.map { |r| r["n"] }
    expect(written).to contain_exactly(1, 3)
    expect(bad.get("tags")).to include("_zerobusfailure")
    expect(bad.get("[@metadata][zerobus][failure]")).to eq("rejected")
    expect(bad.get("[@metadata][zerobus][status]")).to eq(400)
    expect(good1.get("tags")).to be_nil
  end

  it "tags every event of a batch when the token is refused, and does not raise" do
    plugin = build
    install_client(plugin, ChunkClient.new(fail_auth: true))
    events = [event, event, event]
    expect { plugin.multi_receive(events) }.not_to raise_error
    events.each do |e|
      expect(e.get("tags")).to include("_zerobusfailure")
      expect(e.get("[@metadata][zerobus][failure]")).to eq("auth")
    end
  end

  it "tags events as delivery failures when retries are exhausted" do
    plugin = build
    install_client(plugin, ChunkClient.new(fail_transient: true))
    e = event
    plugin.multi_receive([e])
    expect(e.get("[@metadata][zerobus][failure]")).to eq("delivery")
  end
end
