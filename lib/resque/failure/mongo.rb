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
      def self.search_results(start = 0, count = 1, squery)
        start, count = [start, count].map { |n| Integer(n) }
        search_res = [];
        
        if squery.nil? || squery.empty?
          return search_res
        end
        
        Resque.mongo_failures.find({}, {:fields => {"backtrace" => 0, "failed_at" => 0}}).sort([:natural, :desc]).skip(start).limit(count).to_a.each do |failure|
            failure.each_key do |key|
              match = false
              squery.split.each do |term|
                p term
                if failure[key].to_s =~ /#{term}/i
                  match = true
                  break
                end
              end
              if match
                search_res << failure
                break
              end
            end
        end
        
        search_res.size == 1 ? search_res.first : search_res
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
