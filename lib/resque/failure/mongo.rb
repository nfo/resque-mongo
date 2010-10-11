require 'set'

module Resque
  module Failure
    # A Failure backend that stores exceptions in Mongo. Very simple but
    # works out of the box, along with support in the Resque web app.
    class Mongo < Base
      def save
        data = {
          :failed_at => Time.now.strftime("%Y/%m/%d %H:%M:%S"),
          :payload   => payload,
          :exception => exception.class.to_s,
          :error     => exception.to_s,
          :backtrace => Array(exception.backtrace),
          :worker    => worker.to_s,
          :queue     => queue
        }
        Resque.mongo_failures << data
      end

      def self.count
        Resque.mongo_failures.count
      end

      def self.all(start = 0, count = 1)
        start, count = [start, count].map { |n| Integer(n) }
        all_failures = Resque.mongo_failures.find().sort([:natural, :desc]).skip(start).limit(count).to_a
        all_failures.size == 1 ? all_failures.first : all_failures
      end
      
      # Looks for failed jobs who match a particular search string
      def self.search_results(query, start = 0, count = 1)
        # If the search query is nil or empty, return an empty array
        return [] if query.nil? || query.empty?
        
        start, count = [start, count].map { |n| Integer(n) }
        set_results = Set.new
        
        # For each search term, retrieve the failed jobs that contain at least one relevant field matching the regexp defined by that search term
        query.split.each do |term|
          partial_results = Resque.mongo_failures.find(
            {"$or" => [ 
              {"exception" => /#{term}/i},
              {"error" => /#{term}/i},
              {"payload.class" => /#{term}/i},
              {"payload.args" => /#{term}/i},
              {"worker" => /#{term}/i},
              {"queue" => /#{term}/i} 
            ] },
            {:fields => {"backtrace" =>0, "failed_at" => 0}}
          ).sort([:natural, :desc]).to_a
            
          # If the set was empty, merge the first results, else intersect it with the current results
          if set_results.empty?
            set_results.merge(partial_results)
          else
            set_results = set_results & partial_results
          end
        end
        
        # search_res will be an array containing 'count' values, starting with 'start', sorted in descending order
        search_results = set_results.to_a[start, count]
        
        search_results || []
      end

      def self.clear
        Resque.mongo_failures.remove
      end

      def self.requeue(index)
        item = all(index)
        item['retried_at'] = Time.now.strftime("%Y/%m/%d %H:%M:%S")
        Resque.mongo_failures.update({:_id => item['_id']}, item)
        Job.create(item['queue'], item['payload']['class'], *item['payload']['args'])
      end
    end
  end
end