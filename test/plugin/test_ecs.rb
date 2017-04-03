require 'helper'

class ECSFilterTest < Test::Unit::TestCase
  def setup
    unless defined?(Fluent::Filter)
      omit('Fluent::Filter is not defined. Use fluentd 0.12 or later.')
    end

    Fluent::Test.setup
  end

  # config_param :ratio, :float
  # config_param :key_names, :string, :default => nil
  # config_param :key_pattern, :string, :default => nil
  # config_param :floor, :bool, :default => false
  # config_param :remove_prefix, :string, :default => nil
  # config_param :add_prefix, :string, :default => nil

  CONFIG = %[]
  CONFIG2 = %[
    cache_size 2000
    cache_ttl 300
  ]
  CONFIG3 = %[
    get_container_id_tag true
    container_id_attr container_id
  ]
  CONFIG4 = %[
    task_family_prepend foo-
  ]
  CONFIG5 = %[
    container_id_attr docker.id
  ]

  def create_driver(conf = CONFIG, tag = 'test')
    Fluent::Test::FilterTestDriver.new(Fluent::ECSFilter, tag).configure(conf)
  end

  def setup_docker_stub(file, docker_api_url)
    stub_request(:get, docker_api_url)
      .to_return(status: 200, body: file, headers: {})
  end

  def setup_ecs_container(container_id, file_name)
    docker_api_url = "http://tcp//example.com:5422/v1.16/containers/#{container_id}/json"
    file = File.open("test/containers/#{file_name}.json", 'rb')
    setup_docker_stub(file, docker_api_url)
  end


  def test_ecs_filter
    setup_ecs_container('foobar123', 'ecs')

    d1 = create_driver(CONFIG, 'docker.foobar123')
    d1.run do
      d1.filter('log' => 'Hello World 1')
    end
    filtered = d1.filtered_as_array

    task_id = 'fdb86b4f-b919-4a65-89eb-4b3761eb8952'

    log_entry = filtered[0][2]

    assert_equal 'unifi-video', log_entry['task_family']
    assert_equal '9', log_entry['task_version']
    assert_equal task_id, log_entry['task_id']
  end

  def test_task_family_prepend
    setup_ecs_container('foobar123', 'ecs')

    d1 = create_driver(CONFIG4, 'docker.foobar123')
    d1.run do
      d1.filter('log' => 'Hello World 1')
    end
    filtered = d1.filtered_as_array

    log_entry = filtered[0][2]

    assert_equal 'foo-unifi-video', log_entry['task_family']
  end

  def test_nested_container_id
    setup_ecs_container('foobar1234', 'get_from_record')

    d1 = create_driver(CONFIG5)
    d1.run do
      d1.filter('log' => 'Hello World 1', 'docker': {
                'id': 'foobar1234',
                'name': 'k8s_fabric8-console-container.efbd6e64_fabric8-console-controller-9knhj_default_8ae2f621-f360-11e4-8d12-54ee7527188d_7ec9aa3e',
                'container_hostname': 'fabric8-console-controller-9knhj',
                'image': 'fabric8/hawtio-kubernetes:latest',
                'image_id': 'b2bd1a24a68356b2f30128e6e28e672c1ef92df0d9ec01ec0c7faea5d77d2303',
                'labels': {}
                })
    end
    filtered = d1.filtered_as_array

    log_entry = filtered[0][2]
    puts "LOG ENTRY #{log_entry}"
    assert_equal 'unifi-video', log_entry['task_family']
  end

  def test_container_cache
    setup_ecs_container('foobar123', 'ecs')

    d1 = create_driver(CONFIG, 'docker.foobar123')
    d1.run do
      1000.times do
        d1.filter('log' => 'Hello World 4')
      end
    end
    docker_api_url = 'http://tcp//example.com:5422/v1.16/containers/foobar123/json'

    assert_equal 1000, d1.filtered_as_array.length
    assert_requested(:get, docker_api_url, times: 2)
  end

  def test_container_cache_expiration
    setup_ecs_container('foobar123', 'ecs')

    d1 = create_driver(CONFIG2, 'docker.foobar123')
    d1.run do
      d1.filter('log' => 'Hello World 4')
    end

    Timecop.travel(Time.now + 10 * 60)

    d1.run do
      d1.filter('log' => 'Hello World 4')
    end

    Timecop.return

    docker_api_url = 'http://tcp//example.com:5422/v1.16/containers/foobar123/json'

    assert_requested(:get, docker_api_url, times: 4)
  end

  def test_merge_json
    setup_ecs_container('foobar123', 'ecs')

    d1 = create_driver(CONFIG, 'docker.foobar123')
    d1.run do
      d1.filter('log' => '{"test_key":"Hello World"}')
    end
    filtered = d1.filtered_as_array
    log_entry = filtered[0][2]

    assert_equal 'Hello World', log_entry['test_key']
  end

  def test_bad_merge_json
    setup_ecs_container('foobar123', 'ecs')
    bad_json1 = '{"test_key":"Hello World"'
    bad_json2 = '{"test_key":"Hello World", "badnews"}'

    d1 = create_driver(CONFIG, 'docker.foobar123')
    d1.run do
      d1.filter('log' => bad_json1)
      d1.filter('log' => bad_json2)
    end
    filtered = d1.filtered_as_array

    assert_equal bad_json1, filtered[0][2]['log']
    assert_equal bad_json2, filtered[1][2]['log']
  end

end
