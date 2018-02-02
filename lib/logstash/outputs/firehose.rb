# encoding: utf-8
require "aws-sdk"
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "stud/temporary"
require "stud/task"
require "socket" # for Socket.gethostname
require "thread"
require "tmpdir"
require "fileutils"

# INFORMATION:
#
# This plugin sends logstash events to Amazon Kinesis Firehose.
# To use it you need to have the proper write permissions and a valid Firehose stream.
# Make sure you have permissions to put records into Firehose stream.
# Also be sure to run logstash as super user to establish a connection.
#
# AWS SDK, Firehose client: http://docs.aws.amazon.com/sdkforruby/api/Aws/Firehose/Client.html
#
# #### Usage:
# This is an example of logstash config:
# [source,ruby]
# output {
#   firehose {
#     access_key_id => "AWS ACCESS KEY"       (required)
#     secret_access_key => "AWS SECRET KEY"   (required)
#     region => "us-east-1"                   (required)
#     stream => "firehose-stream-name"        (required)
#     codec => plain {
#       format: "%{message}"
#     }                                       (optional, default: plain)
#     aws_credentials_file => "/path/file"    (optional, default: none)
#     proxy_uri => "proxy URI"                (optional, default: none)
#   }
# }
#

class LogStash::Outputs::Firehose < LogStash::Outputs::Base
  include LogStash::PluginMixins::AwsConfig::V2

  TEMPFILE_EXTENSION = "txt"
  FIREHOSE_STREAM_VALID_CHARACTERS = /[\w\-]/

  # These are hard limits
  FIREHOSE_PUT_BATCH_SIZE_LIMIT = 4_000_000 # 4MB
  FIREHOSE_PUT_BATCH_RECORD_LIMIT = 500
  FIREHOSE_PUT_RECORD_SIZE_LIMIT = 1_000_000 # 1_000 KB

  # make properties visible for tests
  attr_accessor :stream
  attr_accessor :codec

  concurrency :single

  config_name "firehose"

  # Output coder
  default :codec, "plain"

  # Firehose stream info
  config :region, :validate => :string, :default => "us-east-1"
  config :stream, :validate => :string

  #
  # Register plugin
  public
  def register
    # Validate stream name
    if @stream.nil? || @stream.empty?
      @logger.error("Firehose: stream name is empty", :stream => @stream)
      raise LogStash::ConfigurationError, "Firehose: stream name is empty"
    end
    if @stream && @stream !~ FIREHOSE_STREAM_VALID_CHARACTERS
      @logger.error("Firehose: stream name contains invalid characters", :stream => @stream, :allowed => FIREHOSE_STREAM_VALID_CHARACTERS)
      raise LogStash::ConfigurationError, "Firehose: stream name contains invalid characters"
    end

    # Register coder: comma separated line -> SPECIFIED_CODEC_FMT, call handler after to deliver encoded data to Firehose
    @event_buffer_lock = Mutex.new
    @event_buffer = Array.new
    @codec.on_event do |event, encoded_event|
      @logger.debug("Event info", :event => event, :encoded_event => encoded_event)
      @event_buffer_lock.synchronize do
        @event_buffer.push(encoded_event)
      end
    end
  end

  #
  # On event received handler: just wrap as JSON and pass it to handle_event method
  def receive(event)
    @codec.encode(event)

    handle_event
  end # def event

  def multi_receive(events)
    events.each do |event|
      @codec.encode(event)
    end

    handle_events
  end # def multi_receive

  # Evaluate AWS endpoint for Firehose based on specified @region option
  def aws_service_endpoint(region)
    return {
        :region => region,
        :endpoint => "https://firehose.#{region}.amazonaws.com"
    }
  end

  private

  # Build AWS Firehose client
  def aws_firehose_client
    @firehose ||= Aws::Firehose::Client.new(aws_full_options)
  end

  # Build and return AWS client options map
  def aws_full_options
    aws_options_hash
  end

  def oversized_event(event)
    event.bytesize > FIREHOSE_PUT_RECORD_SIZE_LIMIT
  end

  def handle_event
    encoded_event = @event_buffer.pop
    @logger.debug "Pushing encoded event: #{encoded_event}"

    if oversized_event(encoded_event)
      # Drop it to the floor
      @logger.error "Event is too big for Firehose: #{encoded_event.bytesize}"
      return
    end

    begin
      aws_firehose_client.put_record({
        delivery_stream_name: @stream,
        record: {
            data: encoded_event
        }
      })
    rescue Aws::Firehose::Errors::ResourceNotFoundException => error
      # Firehose stream not found
      @logger.error "Firehose: AWS resource error", :error => error
      raise LogStash::Error, "Firehose: AWS resource not found error: #{error}"
    rescue Aws::Firehose::Errors::ServiceError => error
      # TODO Retry policy
      # Permanently failing events can be pushed to a DLQ by Logstash
      @logger.error "Firehose: AWS delivery error", :error => error
      @logger.info "Failed to deliver event: #{encoded_event}"
    end
  end

  def array_size(array)
    array.collect(&:bytesize).inject(0, :+)
  end

  def remove_oversized_events(events)
    undersized_events = events.reject { |event| oversized_event(event) }
    oversized_events = events - undersized_events
    if oversized_events.length > 0
      @logger.error "#{oversized_events.length} events are too big for Firehose, they will not be sent"
    end
    undersized_events
  end

  def handle_events
    @logger.debug "Pushing encoded events"

    begin
      rounds = (@event_buffer.length / FIREHOSE_PUT_BATCH_RECORD_LIMIT.to_f).ceil
      rounds.times do
        events = []
        @event_buffer_lock.synchronize do
          events = @event_buffer.slice!(0, FIREHOSE_PUT_BATCH_RECORD_LIMIT)
        end
        break if events.nil?

        events = remove_oversized_events(events)
        break if events.empty?

        put_in_batches(events)
      end
    rescue Aws::Firehose::Errors::ResourceNotFoundException => error
      # Firehose stream not found
      @logger.error "Firehose: AWS resource error", :error => error
      raise LogStash::Error, "Firehose: AWS resource not found error: #{error}"
    rescue Aws::Firehose::Errors::ServiceError => error
      @logger.error "Firehose: AWS delivery error", :error => error
    end
  end

  def put_in_batches(events)
    if array_size(events) > FIREHOSE_PUT_BATCH_SIZE_LIMIT
      while events.length > 0
        event_chunk = []
        while events.length > 0 && (array_size(event_chunk) + events.last.bytesize) < FIREHOSE_PUT_BATCH_SIZE_LIMIT
          event_chunk << events.pop
        end
        put_batch(event_chunk)
      end
    else
      put_batch(events)
    end
  end

  def put_batch(events)
    aws_firehose_client.put_record_batch({
      delivery_stream_name: @stream,
      records: events.map { |e| {data: e} }
    })
  end

end # class LogStash::Outputs::Firehose
