# encoding: utf-8
require "spec_helper"
require "json"

RSpec.describe LogStash::Outputs::ZerobusRest::Protocol do
  it "requests one table, USE CATALOG, USE SCHEMA and SELECT+MODIFY with zerobuswrite" do
    details = JSON.parse(described_class.authorization_details("main.sales.orders"))
    expect(details.map { |d| d["object_type"] }).to eq(%w[CATALOG SCHEMA TABLE])
    expect(details[2]).to include("object_full_path" => "main.sales.orders",
                                  "privileges" => %w[SELECT MODIFY],
                                  "operations" => ["zerobuswrite"])
  end

  it "builds the resource from the workspace id" do
    expect(described_class.resource("123")).to eq("api://databricks/workspaces/123/zerobusDirectWriteApi")
  end
end

# Scripted transport: returns queued [status, body] answers and records every request.
class FakeTransport
  attr_reader :calls

  def initialize(*answers)
    @answers = answers
    @calls = []
  end

  def post(uri, headers:, body:)
    @calls << { uri: uri.to_s, headers: headers, body: body }
    answer = @answers.shift or raise "no scripted answer left for #{uri}"
    answer = answer.call(@calls.last) if answer.respond_to?(:call)
    answer
  end
end

TOKEN_OK = [200, '{"access_token":"tok-1","expires_in":3600}'].freeze

RSpec.describe LogStash::Outputs::ZerobusRest::TokenProvider do
  let(:now) { [0.0] }
  let(:clock) { -> { now[0] } }
  let(:args) do
    { workspace_url: "https://ws.example.com/", workspace_id: "42", table: "main.sales.orders",
      client_id: "cid", client_secret: "sec", refresh_margin: 300, clock: clock }
  end

  it "fetches a client-credentials token with basic auth and the table-scoped form fields" do
    transport = FakeTransport.new(TOKEN_OK)
    token = described_class.new(transport: transport, **args).token
    expect(token).to eq("tok-1")
    call = transport.calls.first
    expect(call[:uri]).to eq("https://ws.example.com/oidc/v1/token")
    expect(call[:headers]["Authorization"]).to start_with("Basic ")
    expect(call[:body]).to include("grant_type=client_credentials", "scope=all-apis")
    expect(call[:body]).to include("zerobusDirectWriteApi")
  end

  it "reuses the cached token until the refresh margin, then refreshes" do
    transport = FakeTransport.new(TOKEN_OK, [200, '{"access_token":"tok-2","expires_in":3600}'])
    provider = described_class.new(transport: transport, **args)
    expect(provider.token).to eq("tok-1")
    now[0] = 3000.0
    expect(provider.token).to eq("tok-1")
    now[0] = 3300.0 # within 300 s of expiry
    expect(provider.token).to eq("tok-2")
    expect(transport.calls.size).to eq(2)
  end

  it "maps a refused client to AuthError" do
    transport = FakeTransport.new([401, '{"error":"invalid_client"}'])
    expect { described_class.new(transport: transport, **args).token }
      .to raise_error(LogStash::Outputs::ZerobusRest::AuthError, /refused the client \(HTTP 401\)/)
  end

  it "maps a 503 from the token endpoint to TransientError" do
    transport = FakeTransport.new([503, "busy"])
    expect { described_class.new(transport: transport, **args).token }
      .to raise_error(LogStash::Outputs::ZerobusRest::TransientError)
  end

  it "never puts the client secret in an error message" do
    transport = FakeTransport.new([401, "nope"])
    expect { described_class.new(transport: transport, **args).token }
      .to raise_error { |e| expect(e.message).not_to include("sec") }
  end
end

RSpec.describe LogStash::Outputs::ZerobusRest::Client do
  let(:endpoint) { "https://42.zerobus.example.com" }
  let(:sleeps) { [] }
  let(:tokens) do
    provider = Object.new
    def provider.token; @n = (@n || 0) + 1; "tok-#{@n}"; end
    def provider.invalidate; @invalidated = (@invalidated || 0) + 1; end
    def provider.invalidated; @invalidated || 0; end
    provider
  end

  def client(transport, max_retries: 3)
    described_class.new(endpoint: endpoint, table: "main.sales.orders", token_provider: tokens,
                        transport: transport, max_retries: max_retries, retry_backoff: 0.5,
                        sleeper: ->(s) { sleeps << s })
  end

  it "posts a JSON array to the table insert path with a bearer token" do
    transport = FakeTransport.new([200, "{}"])
    client(transport).insert([{ "a" => 1 }, { "a" => 2 }])
    call = transport.calls.first
    expect(call[:uri]).to eq("#{endpoint}/zerobus/v1/tables/main.sales.orders/insert")
    expect(call[:headers]).to include("Authorization" => "Bearer tok-1", "Content-Type" => "application/json")
    expect(JSON.parse(call[:body])).to eq([{ "a" => 1 }, { "a" => 2 }])
  end

  it "retries 5xx with exponential backoff and then succeeds" do
    transport = FakeTransport.new([503, "down"], [500, "down"], [200, "{}"])
    expect { client(transport).insert([{ "a" => 1 }]) }.not_to raise_error
    expect(sleeps).to eq([0.5, 1.0])
    expect(transport.calls.size).to eq(3)
  end

  it "raises TransientError after max_retries" do
    transport = FakeTransport.new([503, "a"], [503, "b"], [503, "c"])
    expect { client(transport, max_retries: 2).insert([{}]) }
      .to raise_error(LogStash::Outputs::ZerobusRest::TransientError)
    expect(transport.calls.size).to eq(3)
  end

  it "drops the token on 401, then retries with a new one" do
    transport = FakeTransport.new([401, "expired"], [200, "{}"])
    client(transport).insert([{}])
    expect(tokens.invalidated).to eq(1)
    expect(transport.calls.last[:headers]["Authorization"]).to eq("Bearer tok-2")
  end

  it "raises RejectedError for a 4xx without retrying" do
    transport = FakeTransport.new([400, "field mismatch"])
    expect { client(transport).insert([{}]) }.to raise_error(LogStash::Outputs::ZerobusRest::RejectedError, /HTTP 400/)
    expect(transport.calls.size).to eq(1)
    expect(sleeps).to be_empty
  end

  it "treats 403 on insert as AuthError" do
    transport = FakeTransport.new([403, "no privilege"])
    expect { client(transport).insert([{}]) }.to raise_error(LogStash::Outputs::ZerobusRest::AuthError)
  end

  it "maps a refused connection to TransientError" do
    transport = LogStash::Outputs::ZerobusRest::Transport.new(open_timeout: 2, read_timeout: 2)
    expect { transport.post(URI("http://127.0.0.1:1/x"), headers: {}, body: "{}") }
      .to raise_error(LogStash::Outputs::ZerobusRest::TransientError, /network error/)
  end
end
