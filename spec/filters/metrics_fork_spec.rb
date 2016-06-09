# encoding: utf-8
require "spec_helper"
require "logstash/filters/metrics_fork"

describe LogStash::Filters::MetricsFork do
  describe "test with full prefix and field" do
    config <<-CONFIG
      filter {
        metrics_fork {
          prefix => "status_code."
          regex => "\\d+"
          relevant_metric => "count"
          name => "http_status_code"
        }
      }
    CONFIG
  
    sample("status_code.200" => {"count" => 24}, "status_code.400" => {"count" => 17}) do
      insist { subject.length } == 2
      insist { subject[0]["http_status_code"] } == 24
      insist { subject[0]["metric"] } == "200"
      insist { subject[0]["tags"] }.nil?
      insist { subject[1]["http_status_code"] } == 17
      insist { subject[1]["metric"] } == "400"
      insist { subject[1]["tags"] }.nil?
    end
  end
  
  describe "test with short prefix" do
    config <<-CONFIG
      filter {
        metrics_fork {
          prefix => "stat"
          regex => "\\d+"
          relevant_metric => "rate_1m"
          name => "http_status_code"
        }
      }
    CONFIG
  
    sample("status_code.200" => {"count" => 24, "rate_1m" => 15}) do
      insist { subject["http_status_code"] } == 15
      insist { subject["metric"] } == "200"
      insist { subject["tags"] }.nil?
    end
  end

  describe "dont drop original" do
    config <<-CONFIG
      filter {
        metrics_fork {
          prefix => "stat"
          regex => "\\d+"
          name => "http_status_code"
          drop_original_event => false
        }
      }
    CONFIG
  
    sample("status_code.200" => {"count" => 24, "rate_1m" => 15}) do
      insist { subject.length } == 2
      insist { subject[0]["tags"] }.nil?
      insist { subject[1]["tags"] }.nil?
    end
  end

  describe "unescaped regex" do
    config <<-CONFIG
      filter {
        metrics_fork {
          prefix => "stat"
          regex => ""
          name => "http_status_code"
        }
      }
    CONFIG
  
    sample("status_code.200" => {"count" => 24, "rate_1m" => 15}) do
      insist { subject["tags"] }.include?("_regexpparsefailure")

    end
  end

  describe "named tags" do
    config <<-CONFIG
      filter {
        metrics_fork {
          prefix => "stat"
          regex => "(?<=status_code\\.)[\\d]+"
          name => "responses"
          tag_key => "status_code"
        }
      }
    CONFIG
  
    sample("status_code.200" => {"count" => 24, "rate_1m" => 15}) do
      insist { subject["tags"] }.nil?
      insist { subject["responses"] } == 24
      insist { subject["status_code"] } == "200"

    end
  end
end

