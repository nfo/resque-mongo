require 'mongo'

begin
  require 'yajl'
rescue LoadError
  require 'json'
end

require 'resque/version'

require 'resque/errors'

require 'resque/failure'
require 'resque/failure/base'

require 'resque/helpers'
require 'resque/stat'
require 'resque/job'
require 'resque/worker'
require 'resque/plugin'
require 'resque/queue_stats'

module Resque
  include Helpers
  extend self

  attr_accessor :verbose
  attr_accessor :very_verbose
  
  # Accepts  'hostname' or 'hostname:port' or 'hostname:port/db' strings
  # or a Mongo::DB object.
  def mongo=(server)
    @verbose = ENV['LOGGING']||ENV['VERBOSE']
    @very_verbose = ENV['VVERBOSE']
    
    @con.close if @con
    
    case server
    when String
      match = server.match(/([^:]+):?(\d*)\/?(\w*)/) # http://rubular.com/r/G6O8qe0DJ5
      host = match[1]
      port = match[2].nil? || match[2] == '' ? '27017' : match[2]
      db = match[3].nil? || match[3] == '' ? 'monque' : match[3]
      
      log "Initializing connection to #{host}:#{port}"
      @con = Mongo::Connection.new(host, port)
      @db = @con.db(db)
    when Mongo::DB
      @con = server.connection
      @db = server
    else
      raise "I don't know what to do with #{server.inspect}" unless server.is_a?(String) || server.is_a?(Mongo::Connection)
    end
    
    @mongo = @db.collection('monque')
    @workers = @db.collection('workers')
    @failures = @db.collection('failures')
    @stats = @db.collection('stats')
    @queues = @db.collection('queues')
    log "Creating/updating indexes"
    add_indexes
  end


  # Returns the current Mongo connection. If none has been created, will
  # create a new one.
  def mongo
    return @mongo if @mongo
    self.mongo = ENV['MONGO']||'localhost:27017'
    self.mongo
  end

  def mongo_workers
    return @workers if @workers
    self.mongo = ENV['MONGO']||'localhost:27017'
    @workers
  end

  def mongo_failures
    return @failures if @failures
    self.mongo = ENV['MONGO']||'localhost:27017'
    @failures
  end

  def mongo_stats
    return @stats if @stats
    self.mongo = ENV['MONGO']||'localhost:27017'
    @stats
  end
  
  def mongo_queues
    return @queues if @queues
    self.mongo = ENV['MONGO']||'localhost:27017'
    @queues
  end
  
  # The `before_first_fork` hook will be run in the **parent** process
  # only once, before forking to run the first job. Be careful- any
  # changes you make will be permanent for the lifespan of the
  # worker.
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def before_first_fork(&block)
    block ? (@before_first_fork = block) : @before_first_fork
  end

  # Set a proc that will be called in the parent process before the
  # worker forks for the first time.
  def before_first_fork=(before_first_fork)
    @before_first_fork = before_first_fork
  end

  # The `before_fork` hook will be run in the **parent** process
  # before every job, so be careful- any changes you make will be
  # permanent for the lifespan of the worker.
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def before_fork(&block)
    block ? (@before_fork = block) : @before_fork
  end

  # Set the before_fork proc.
  def before_fork=(before_fork)
    @before_fork = before_fork
  end

  # The `after_fork` hook will be run in the child process and is passed
  # the current job. Any changes you make, therefore, will only live as
  # long as the job currently being processed.
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def after_fork(&block)
    block ? (@after_fork = block) : @after_fork
  end

  # Set the after_fork proc.
  def after_fork=(after_fork)
    @after_fork = after_fork
  end

  def to_s
    "Resque Client connected to #{@con.primary[0]}:#{@con.primary[1]}/#{@db.name}/#{@mongo.name}"
  end


  def add_indexes
    @mongo.create_index([[:queue,1],[:date, 1]])
    @mongo.create_index :queue
    @workers.create_index :worker
    @stats.create_index :stat
    @queues.create_index(:queue,:unique => 1)
    @failures.create_index :queue
  end

  # If 'inline' is true Resque will call #perform method inline
  # without queuing it into Redis and without any Resque callbacks.
  # The 'inline' is false Resque jobs will be put in queue regularly.
  def inline?
    @inline
  end
  alias_method :inline, :inline?

  def inline=(inline)
    @inline = inline
  end

  #
  # queue manipulation
  #

  # Pushes a job onto a queue. Queue name should be a string and the
  # item should be any JSON-able Ruby object.
  def push(queue, item)
    watch_queue(queue)
    mongo << { :queue => queue.to_s, :item => item , :date => Time.now }
    QueueStats.add_job(queue)
  end

  # Pops a job off a queue. Queue name should be a string.
  #
  # Returns a Ruby object.
  def pop(queue)
    doc = mongo.find_and_modify( :query => { :queue => queue.to_s },
                                 :sort => [[:date, 1]],
                                 :remove => true )
    QueueStats.remove_job(queue)
    doc['item']
  rescue Mongo::OperationFailure => e
    return nil if e.message =~ /No matching object/
    raise e
  end

  # Returns an integer representing the size of a queue.
  # Queue name should be a string.
  def size(queue)
    queue_stats = QueueStats.new(queue)
    queue_stats.size
  end

  # Returns an array of items currently queued. Queue name should be
  # a string.
  #
  # start and count should be integer and can be used for pagination.
  # start is the item to begin, count is how many items to return.
  #
  # To get the 3rd page of a 30 item, paginatied list one would use:
  #   Resque.peek('my_list', 59, 30)
  def peek(queue, start = 0, count = 1)
    start, count = [start, count].map { |n| Integer(n) }
    res = mongo.find(:queue => queue).sort([:date, 1]).skip(start).limit(count).to_a  
    res.collect! { |doc| doc['item'] }
    if count == 1
      return nil if res.empty?
      res.first
    else
      return [] if res.empty?
      res
    end
  end

  # Returns an array of all known Resque queues as strings,
  # filtered by the given names or prefixes.
  def queues(names = nil)
    QueueStats.list(names)
  end
  
  # Given a queue name, completely deletes the queue.
  def remove_queue(queue)
    log "removing #{queue}"
    mongo.remove({:queue => queue.to_s})
    QueueStats.remove(queue)
  end

  # Used internally to keep track of which queues we've created.
  # Don't call this directly.
  def watch_queue(queue)
#    redis.sadd(:queues, queue.to_s)
  end


  #
  # job shortcuts
  #

  # This method can be used to conveniently add a job to a queue.
  # It assumes the class you're passing it is a real Ruby class (not
  # a string or reference) which either:
  #
  #   a) has a @queue ivar set
  #   b) responds to `queue`
  #
  # If either of those conditions are met, it will use the value obtained
  # from performing one of the above operations to determine the queue.
  #
  # If no queue can be inferred this method will raise a `Resque::NoQueueError`
  #
  # This method is considered part of the `stable` API.
  def enqueue(klass, *args)
    Job.create(queue_from_class(klass), klass, *args)

    Plugin.after_enqueue_hooks(klass).each do |hook|
      klass.send(hook, *args)
    end
  end

  # This method can be used to conveniently remove a job from a queue.
  # It assumes the class you're passing it is a real Ruby class (not
  # a string or reference) which either:
  #
  #   a) has a @queue ivar set
  #   b) responds to `queue`
  #
  # If either of those conditions are met, it will use the value obtained
  # from performing one of the above operations to determine the queue.
  #
  # If no queue can be inferred this method will raise a `Resque::NoQueueError`
  #
  # If no args are given, this method will dequeue *all* jobs matching
  # the provided class. See `Resque::Job.destroy` for more
  # information.
  #
  # Returns the number of jobs destroyed.
  #
  # Example:
  #
  #   # Removes all jobs of class `UpdateNetworkGraph`
  #   Resque.dequeue(GitHub::Jobs::UpdateNetworkGraph)
  #
  #   # Removes all jobs of class `UpdateNetworkGraph` with matching args.
  #   Resque.dequeue(GitHub::Jobs::UpdateNetworkGraph, 'repo:135325')
  #
  # This method is considered part of the `stable` API.
  def dequeue(klass, *args)
    Job.destroy(queue_from_class(klass), klass, *args)
  end

  # Given a class, try to extrapolate an appropriate queue based on a
  # class instance variable or `queue` method.
  def queue_from_class(klass)
    klass.instance_variable_get(:@queue) ||
      (klass.respond_to?(:queue) and klass.queue)
  end

  # This method will return a `Resque::Job` object or a non-true value
  # depending on whether a job can be obtained. You should pass it the
  # precise name of a queue: case matters.
  #
  # This method is considered part of the `stable` API.
  def reserve(queue)
    Job.reserve(queue)
  end

  # Validates if the given klass could be a valid Resque job
  #
  # If no queue can be inferred this method will raise a `Resque::NoQueueError`
  #
  # If given klass is nil this method will raise a `Resque::NoClassError`
  def validate(klass, queue = nil)
    queue ||= queue_from_class(klass)

    if !queue
      raise NoQueueError.new("Jobs must be placed onto a queue.")
    end

    if klass.to_s.empty?
      raise NoClassError.new("Jobs must be given a class.")
    end
  end


  #
  # worker shortcuts
  #

  # A shortcut to Worker.all
  def workers
    Worker.all
  end

  # A shortcut to Worker.working
  def working
    Worker.working
  end

  # A shortcut to unregister_worker
  # useful for command line tool
  def remove_worker(worker_id)
    worker = Resque::Worker.find(worker_id)
    worker.unregister_worker
  end

  #
  # stats
  #

  # Returns a hash, similar to redis-rb's #info, of interesting stats.
  def info
    return {
      :pending   => queues.inject(0) { |m,k| m + size(k) },
      :processed => Stat[:processed],
      :queues    => queues.size,
      :workers   => workers.size.to_i,
      :working   => working.size,
      :failed    => Stat[:failed],
      :servers   => "#{@con.primary[0]}:#{@con.primary[1]}/#{@db.name}/#{@mongo.name}",
      :environment  => ENV['RAILS_ENV'] || ENV['RACK_ENV'] || 'development',
    }
  end

  # Returns an array of all known Resque keys in Redis. Redis' KEYS operation
  # is O(N) for the keyspace, so be careful - this can be slow for big databases.
  def keys
    queues
  end
  
  
  # Log a message to STDOUT if we are verbose or very_verbose.
  def log(message)
    if verbose
      puts "*** #{message}"
    elsif very_verbose
      time = Time.now.strftime('%I:%M:%S %Y-%m-%d')
      puts "** [#{time}] #$$: #{message}"
    end
  end

end
