# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/outputs/zerobus/client"

# Sends events to one Zerobus Ingest table over the REST insert endpoint.
#
#   output {
#     zerobus {
#       workspace_url   => "${DATABRICKS_WORKSPACE_URL}"
#       zerobus_endpoint => "${ZEROBUS_ENDPOINT}"
#       table           => "catalog.schema.table"
#       client_id       => "${DATABRICKS_CLIENT_ID}"
#       client_secret   => "${DATABRICKS_CLIENT_SECRET}"
#     }
#   }
#
# Each Logstash batch is sent in chunks of `batch_size` records. A chunk is one JSON array.
# Transient failures are retried with exponential backoff. A 4xx for a chunk splits it in halves
# until the bad record is alone; that record is tagged `_zerobusfailure` and the pipeline keeps
# running. Put a `rescue` VARIANT column on the table (tagged `zerobus-rescue`) so fields that
# don't fit the schema are kept instead of rejected.
class LogStash::Outputs::Zerobus < LogStash::Outputs::Base
  config_name "zerobus"
  concurrency :single

  FAILURE_TAG = "_zerobusfailure".freeze

  # Base URL of the Zerobus endpoint, for example https://<workspace-id>.zerobus.<region>.cloud.databricks.com
  config :workspace_url, :validate => :string, :required => true
  config :zerobus_endpoint, :validate => :string, :required => true
  # Defaults to the first DNS label of zerobus_endpoint, which names the workspace.
  config :workspace_id, :validate => :string
  # catalog.schema.table, the one table this output writes to.
  config :table, :validate => :string, :required => true
  config :client_id, :validate => :string, :required => true
  config :client_secret, :validate => :password, :required => true

  # Column name for the event timestamp (ISO 8601, UTC). The mock server rejected a column named
  # @timestamp in the test run, so @timestamp is written under this name. Set to "" to leave it out.
  config :timestamp_field, :validate => :string, :default => "event_timestamp"

  config :batch_size, :validate => :number, :default => 500
  config :max_retries, :validate => :number, :default => 5
  config :retry_backoff, :validate => :number, :default => 1.0
  config :token_refresh_margin, :validate => :number, :default => 300
  config :open_timeout, :validate => :number, :default => 10
  config :read_timeout, :validate => :number, :default => 60

  def register
    @table = @table.to_s
    parts = @table.split(".")
    unless parts.length == 3 && parts.none?(&:empty?)
      raise LogStash::ConfigurationError, "table must be catalog.schema.table, got #{@table.inspect}"
    end
    @batch_size = [@batch_size.to_i, 1].max
    @endpoint = normalise_endpoint(@zerobus_endpoint)
    @workspace_id ||= @endpoint[%r{\Ahttps?://([^.]+)\.}, 1] || raise(LogStash::ConfigurationError, "cannot derive workspace_id from zerobus_endpoint; set workspace_id")

    transport = LogStash::Outputs::ZerobusRest::Transport.new(open_timeout: @open_timeout, read_timeout: @read_timeout)
    tokens = LogStash::Outputs::ZerobusRest::TokenProvider.new(
      workspace_url: @workspace_url,
      workspace_id: @workspace_id,
      table: @table,
      client_id: @client_id,
      client_secret: @client_secret.value,
      transport: transport,
      refresh_margin: @token_refresh_margin
    )
    @client = LogStash::Outputs::ZerobusRest::Client.new(
      endpoint: @endpoint,
      table: @table,
      token_provider: tokens,
      transport: transport,
      max_retries: @max_retries.to_i,
      retry_backoff: @retry_backoff.to_f
    )
  end

  def multi_receive(events)
    events.each_slice(@batch_size) { |chunk| send_chunk(chunk) }
  end

  def close
    @client.close if @client.respond_to?(:close)
  end

  private

  def normalise_endpoint(value)
    url = value.to_s.strip
    url = "https://#{url}" unless url.start_with?("http://", "https://")
    url.chomp("/")
  end

  def send_chunk(events)
    @client.insert(events.map { |event| record_for(event) })
    @logger.debug("Zerobus accepted chunk", :table => @table, :records => events.size)
  rescue LogStash::Outputs::ZerobusRest::RejectedError => e
    if events.size > 1
      half = events.size / 2
      send_chunk(events[0...half])
      send_chunk(events[half..-1])
    else
      fail_event(events.first, "rejected", e)
    end
  rescue LogStash::Outputs::ZerobusRest::Error => e
    # Auth failure or retries exhausted: none of these events reached the table.
    kind = e.is_a?(LogStash::Outputs::ZerobusRest::AuthError) ? "auth" : "delivery"
    events.each { |event| fail_event(event, kind, e) }
  end

  # The event as one JSON object. @version and @timestamp are not sent as they are: the timestamp
  # goes out as ISO 8601 UTC under `timestamp_field`. @metadata is never part of the record.
  def record_for(event)
    record = event.to_hash
    record.delete("@version")
    timestamp = record.delete("@timestamp")
    unless @timestamp_field.to_s.empty? || timestamp.nil?
      record[@timestamp_field] = event.get("@timestamp").time.utc.iso8601(3)
    end
    record
  end

  def fail_event(event, kind, error)
    event.tag(FAILURE_TAG)
    event.set("[@metadata][zerobus][failure]", kind)
    event.set("[@metadata][zerobus][status]", error.status) if error.status
    event.set("[@metadata][zerobus][error]", error.message[0, 300])
    @logger.warn("Zerobus did not write a record", :table => @table, :failure => kind,
                 :status => error.status, :error => error.message[0, 300])
  end
end
