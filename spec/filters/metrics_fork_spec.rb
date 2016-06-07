# encoding: utf-8
require "spec_helper"
require "logstash/filters/metrics_fork"

describe LogStash::Filters::MetricsFork do
  describe "test with prefix and field" do
    config <<-CONFIG
      filter {
        metrics_fork {
          prefix => "status_code."
          field => "count"
        }
      }
    CONFIG
  
    sample("status_code.200"=>{"count"=>24}, "status_code.400"=>{"count"=>17}) do
      insist { subject.length } == 2
      insist { subject[0].get("status_code.") } == 24
      insist { subject[0].get("metric") } == "200"
      insist { subject[1].get("status_code.") } == 17
      insist { subject[1].get("metric") } == "400"
    end
  end
  
  describe "test with prefix, field and override name" do
    config <<-CONFIG
      filter {
        metrics_fork {
          prefix => "status_code."
          field => "count"
          name => "http_status_code"
        }
      }
    CONFIG
  
    sample("status_code.200"=>{"count"=>24}, "status_code.400"=>{"count"=>17}) do
      insist { subject.length } == 2
      insist { subject[0].get("http_status_code") } == 24
      insist { subject[0].get("metric") } == "200"
      insist { subject[1].get("http_status_code") } == 17
      insist { subject[1].get("metric") } == "400"
    end
  end
  
  describe "test with short prefix" do
    config <<-CONFIG
      filter {
        metrics_fork {
          prefix => "stat"
          field => "rate_1m"
        }
      }
    CONFIG
  
    sample("status_code.200"=>{"count"=>24, "rate_1m"=>15}) do
      insist { subject.get("stat") } == 15
      insist { subject.get("metric") } == "us_code.200"
    end
  end
end
