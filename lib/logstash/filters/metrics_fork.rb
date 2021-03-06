# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# A filter that accepts the grouped metrics from teh metrics filter, which reformats
# the event and emits individual events formatted to be later inserted into InfluxDB
class LogStash::Filters::MetricsFork < LogStash::Filters::Base
  config_name "metrics_fork"

  # The prefix to match in the event for the metric fields. Everything after the prefix
  # in the field name is considered the name of the extracted metric 
  config :prefix, :validate => :string, :required => true

  # A regular expression used to match the portion of the field name that contains the metric.
  # If the field name is "status_code.200" then a regex of `\d+` would yield `200` as the metric name.
  # This is useful for subsequently sending to the InfluxDB output plugin as a tag key
  config :regex, :validate => :string, :required => true

  # The field within the metric arrya containing the value we want to use as a metric.
  # This is usually either count or one of the rate metrics (f.ex: rate_1m)
  config :relevant_metric, :validate => :string, :default => "count", :required => true
  
  # The name of the metric being recorded. Defaults to `metric`. This is useful when combined with
  # the InfluxDB output to specify timeseries tags
  config :tag_key, :validate => :string, :default => "metric", :required => false

  # The name to use for the metric.
  config :name, :validate => :string, :required => true
  
  # Flag indicating whether the original event should be dropped or not.
  config :drop_original_event, :validate => :boolean, :default => true

  REGEXPARSEFAILURE = "_regexpparsefailure"

  public
  def register
    # Add instance variables
  end # def register

  public
  def filter(event)

    return unless filter?(event)

    # extract the metrics and the value we're interested in. if :@prefix=>"status_code" and
    # :@relevant_metric=>"count" this:
    # {"status_code.200" => {"count" => 24, ...}, "status_code.400" => {"count" => 17, ...}}
    # becomes:
    # {"status_code.200" => 24, "status_code.400" => 17}
    relevant_fields = {}
    event.to_hash.keys.each do |key|
      if key.start_with?(@prefix) 
        metric_value = event[key][@relevant_metric]
        relevant_fields[key] = metric_value
      end
    end
    
    # remove the original metrics from the event
    relevant_fields.keys.each do |key|
      event.remove(key)
    end
    
    regex = Regexp.new @regex
    # clone the event, reformat the metric and emit the new event
    relevant_fields.each do |key,value|
      fork = event.clone
      
      # this gives us a new event with {:@name => 24, :@tag_key => '200'} 
      fork[@name] = value # {:@name => 24}
      
      metric = key[regex]
      if metric.nil? or metric.empty?
        event.tag(REGEXPARSEFAILURE)
        @logger.warn("No metric matched with the given regex: #{@regex}")
        return
      end

      fork[@tag_key] = metric # {:@tag_key => '200'}
      yield fork
    end
    
    if @drop_original_event
      # cancel the triggering event
      event.cancel
    end
    
  end # def filter
end # class LogStash::Filters::Example
