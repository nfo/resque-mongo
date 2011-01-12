module Resque
  module Failure
    # A Failure backend that uses multiple backends
    # delegates all queries to the first backend
    class Multiple < Base

      class << self
        attr_accessor :classes
      end

      def self.configure
        yield self
        Resque::Failure.backend = self
      end

      def initialize(*args)
        super
        @backends = self.class.classes.map {|klass| klass.new(*args)}
      end

      def save
        @backends.each(&:save)
      end

      # The number of failures.
      def self.count
        classes.first.count
      end

      # Number of result of a failures search.
      def self.search_count
        classes.first.search_count if classes.first.respond_to?(:search_count)
      end

      # Returns a paginated array of failure objects.
      def self.all(start = 0, count = 1)
        classes.first.all(start,count)
      end

      # The results of a failures search.
      def self.search_results(query, start = 0, count = 1)
        classes.first.search_results(query, start, count) if classes.first.respond_to?(:search_results)
      end

      # A URL where someone can go to view failures.
      def self.url
        classes.first.url
      end

      # Clear all failure objects
      def self.clear
        classes.first.clear
      end

      def self.requeue(*args)
        classes.first.requeue(*args)
      end
    end
  end
end