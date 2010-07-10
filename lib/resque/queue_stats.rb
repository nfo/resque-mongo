module Resque
  class QueueStats
    include Helpers
    extend Helpers
   
    def initialize(queue)
      @queue = queue
      queue_doc = mongo_queues.find({ :queue => queue.to_s }, :limit => 1).to_a[0]
      ### insert a new document if we don't know anyting about this queue
      if queue_doc
        @size = queue_doc['count']
      else
        @size = 0
        mongo_queues.insert({ :queue => queue.to_s, :count => 0 }) unless queue_doc
      end
    end

    def add_job(count=1)
      mongo_queues.update({ :queue => @queue.to_s }, { '$inc' => { :count => count } })
      @size += count
    end

    def remove_job(count=1)
      add_job(-count)
    end

    def size
      @size
    end 

    def self.list
      list = mongo_queues.distinct(:queue).to_a
      list
    end

    def self.remove(queue)
      mongo.remove({:queue => queue.to_s})
      mongo_queues.remove({:queue => queue.to_s})
    end
  
    def self.add_job(queue,count=1)
      QueueStats.new(queue).add_job(count)
    end
  
    def self.remove_job(queue, count = 1)
      self.add_job(queue,-count)
    end
     
    def self.size(queue)
      QueueStats.new(queue).size
    end
  end
end