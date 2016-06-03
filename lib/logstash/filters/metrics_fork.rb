# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# A filter that accepts the grouped metrics from teh metrics filter, which reformats
# the event and emits individual events formatted to be later inserted into InfluxDB
class LogStash::Filters::MetricsFork < LogStash::Filters::Base
  config_name "metrics_fork"

  # The field containing the metric to use as the value. This is usually either count or
  # one of the rate metrics (f.ex: rate_1m)
  config :metric, :validate => :string, :default => "count", :required => true
  
  # The prefix to match in the event for the metric fields 
  config :prefix, :validate => :string

  public
  def register
    # Add instance variables
  end # def register

  public
  def filter(event)

    return unless filter?(event)

    # extract the metrics and the value we're interested in. if @prefix=>"status_code" and
    # @metric=>"count" this:
    # {"status_code.200"=>{"count"=>24, ...}, "status_code.400"=>{"count"=>17, ...}}
    # becomes:
    # {"status_code.200"=>24, "status_code.400"=>17}
    relevant_fields = {}
    event.each { |key,value|
      if key.start_with?(@prefix) 
        metric_value = event[key][@metric]
        relevant_fields[key] = metric_value
      end
    }
    
    # remove the original metrics from the event
    relevant_fields.keys.each { |key|
      event.remove(key)
    }
    
    # clone the event, reformat the metric and emit the new event
    relevant_fields.each { |key,value|
      fork = event.clone
      # this gives us a new event with {'status_code'=>24, 'metric'=>'200'} 
      fork.set(@prefix, value)
      metric_name = key[@prefix.length..-1]
      
      fork.set('metric', metric_name)
      yield fork
    }

    # cancel the triggering event
    event.cancel
    
  end # def filter
end # class LogStash::Filters::Example
