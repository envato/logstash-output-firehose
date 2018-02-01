# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/firehose"
require "logstash/codecs/line"
require "logstash/codecs/json_lines"
require "logstash/event"
require "aws-sdk"
require "timecop"

describe LogStash::Outputs::Firehose do
  dataStr = "123,someValue,1234567890"

  let(:sample_event) { LogStash::Event.new("message" => dataStr) }
  let(:time_now) { Time.now }
  let(:expected_event) { "#{time_now.strftime("%FT%H:%M:%S.%3NZ")} %{host} 123,someValue,1234567890" }
  let(:firehose_double) { instance_double(Aws::Firehose::Client) }
  let(:stream_name) { "aws-test-stream" }
  subject { LogStash::Outputs::Firehose.new({"codec" => "plain"}) }

  before do
    Thread.abort_on_exception = true

    # Setup Firehose client
    subject.stream = stream_name
    subject.register

    allow(Aws::Firehose::Client).to receive(:new).and_return(firehose_double)
    allow(firehose_double).to receive(:put_record)
    allow(firehose_double).to receive(:put_record_batch)
  end

  describe "receive one message" do
    it "returns same string" do
      expect(firehose_double).to receive(:put_record).with({
        delivery_stream_name: stream_name,
        record: {
            data: expected_event
        }
      })
      Timecop.freeze(time_now) do
        subject.receive(sample_event)
      end
    end
  end

  describe "receive multiple messages" do
    let(:sample_event_1) { LogStash::Event.new("message" => "abc") }
    let(:sample_event_2) { LogStash::Event.new("message" => "def") }
    let(:sample_event_3) { LogStash::Event.new("message" => "ghi") }
    let(:time_now) { Time.now }
    let(:expected_event_1) { "#{time_now.strftime("%FT%H:%M:%S.%3NZ")} %{host} abc" }
    let(:expected_event_2) { "#{time_now.strftime("%FT%H:%M:%S.%3NZ")} %{host} def" }
    let(:expected_event_3) { "#{time_now.strftime("%FT%H:%M:%S.%3NZ")} %{host} ghi" }
    it "returns same string" do
      expect(firehose_double).to receive(:put_record_batch).with({
        delivery_stream_name: stream_name,
        records: [
          {
            data: expected_event
          },
          {
            data: expected_event
          },
          {
            data: expected_event
          },
        ]
      })
      Timecop.freeze(time_now) do
        subject.multi_receive([sample_event, sample_event, sample_event])
      end
    end

    it "sends each message once" do
      expect(firehose_double).to receive(:put_record_batch).with({
        delivery_stream_name: stream_name,
        records: [
          {
            data: expected_event_1
          },
          {
            data: expected_event_2
          },
        ]
      }).once
      expect(firehose_double).to receive(:put_record_batch).with({
        delivery_stream_name: stream_name,
        records: [
          {
            data: expected_event_3
          },
        ]
      }).once
      Timecop.freeze(time_now) do
        subject.multi_receive([sample_event_1, sample_event_2])
        subject.multi_receive([sample_event_3])
        subject.multi_receive([])
      end
    end

    it "doesn't crash if no events are sent" do
      Thread.new { subject.multi_receive(Array.new(499, sample_event_1)) }
      Thread.new { subject.multi_receive([sample_event_1, sample_event_2]) }
      expect { subject.multi_receive([sample_event_1, sample_event_2]) }.not_to raise_exception
    end
  end
end
