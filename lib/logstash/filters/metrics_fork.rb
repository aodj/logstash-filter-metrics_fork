# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# A filter that accepts the grouped metrics from teh metrics filter, which reformats
# the event and emits individual events formatted to be later inserted into InfluxDB
class LogStash::Filters::MetricsFork < LogStash::Filters::Base
  config_name "metrics_fork"

  # The prefix to match in the event for the metric fields. Everything after the prefix
  # in the field name is considered the name of the extracted metric 
  config :prefix, :validate => :string

  # The field containing the metric to use as the value. This is usually either count or
  # one of the rate metrics (f.ex: rate_1m)
  config :field, :validate => :string, :default => "count", :required => true
  
  # The name to use for the metric. if this isn't set, the filter will use the @prefix 
  # value instead
  config :name, :validate => :string
  
  public
  def register
    # Add instance variables
  end # def register

  public
  def filter(event)

    return unless filter?(event)

    # extract the metrics and the value we're interested in. if @prefix=>"status_code" and
    # @field=>"count" this:
    # {"status_code.200"=>{"count"=>24, ...}, "status_code.400"=>{"count"=>17, ...}}
    # becomes:
    # {"status_code.200"=>24, "status_code.400"=>17}
    relevant_fields = {}
    event.to_hash.each do |key,value|
      if key.start_with?(@prefix) 
        metric_value = event.get("[%s][%s]" % [key, @field])
        relevant_fields[key] = metric_value
      end
    end
    
    # remove the original metrics from the event
    relevant_fields.keys.each do |key|
      event.remove(key)
    end
    
    # clone the event, reformat the metric and emit the new event
    relevant_fields.each do |key,value|
      fork = event.clone
      
      # this gives us a new event with {'status_code'=>24, 'metric'=>'200'} 
      if @name.nil?
        fork.set(@prefix, value) # {'status_code'=>24}
      else
        fork.set(@name, value) # {'status_code'=>24}
      end
      fork.set('metric', key[@prefix.length..-1]) # {'metric'=>200}
      yield fork
    end

    # cancel the triggering event
    event.cancel
    
  end # def filter
end # class LogStash::Filters::Example
