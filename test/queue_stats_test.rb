require File.expand_path(File.dirname(__FILE__)) + '/test_helper'

context "Queue Statistics" do  
  setup do
    Resque.flushall
  end
  
  test "Creating a queue named 'test' and check it exists in the queues list" do
    queue_stats = Resque::QueueStats.new(:test)
    list = Resque::QueueStats.list
    assert list.include?('test')
  end
  
  test "Creating queues and check it can be filtered in the queues list" do
    queue_stats = Resque::QueueStats.new(:abc)
    queue_stats = Resque::QueueStats.new(:abd)
    queue_stats = Resque::QueueStats.new(:xyz)
    
    list = Resque::QueueStats.list('ab*')
    assert list.include?('abc')
    assert list.include?('abd')
    assert !list.include?('xyz')
    
    list = Resque::QueueStats.list(['x*', 'abc'])
    assert list.include?('abc')
    assert !list.include?('abd')
    assert list.include?('xyz')
  end
  
  test "We can add an remove queues" do
    test_1 = Resque::QueueStats.new(:test1)
    list = Resque::QueueStats.list
    assert list.include?('test1')
    Resque::QueueStats.remove(:test1)
    list = Resque::QueueStats.list
    assert !list.include?('test1')
  end
  
  test "both add_job, remove_job and work" do
    ## test class methods 
    Resque::QueueStats.add_job(:test)
    assert_equal 1,Resque::QueueStats.size(:test)
    Resque::QueueStats.remove_job(:test)
    assert_equal 0,Resque::QueueStats.size(:test)
  end
  
  
  test "job counter in QueueStats works" do
    assert Resque::Job.create(:stats, 'SomeJob', 20, '/tmp')
    ## same for the class method version
    assert_equal 1,Resque::QueueStats.size(:stats)
    assert Resque::Job.create(:stats, 'SomeJob', 22, '/tmp')
    assert_equal 2,Resque::QueueStats.size(:stats)
    job = Resque.pop(:stats)
    assert_equal 1,Resque::QueueStats.size(:stats)
  end
end