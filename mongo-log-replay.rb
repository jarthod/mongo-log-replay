#!/usr/bin/env ruby

require 'bundler/inline'
gemfile do
  source 'https://rubygems.org'
  gem 'mongo'
  gem 'concurrent-ruby'
end

stop = false
trap('INT') { stop = true }

pool = Concurrent::FixedThreadPool.new(4, max_queue: 1000)
timings = Concurrent::Array.new
dry_run = false
reads = !dry_run
writes = !dry_run

class MongoMonitor
  def initialize timings; @timings = timings; end
  def started(_); end
  def succeeded(event); @timings << event.duration * 1000; end
  def failed(event); succeeded(event); end
end
Mongo::Monitoring::Global.subscribe(Mongo::Monitoring::COMMAND, MongoMonitor.new(timings))
Mongo::Logger.logger = Logger.new(STDOUT)
Mongo::Logger.logger.level = Logger::Severity::ERROR
mongo = Mongo::Client.new([ARGV.first || 'localhost'], database: 'mongo-log-replay', truncate_logs: false, connect: :direct, max_pool_size: 60)

lines = err = queries = counts = aggregates = updates = inserts = deletes = 0
inserted_doc = Concurrent::Hash.new {|h, k| h[k] = Concurrent::Array.new}
missing_topologies = Set.new
start = Time.now
time = nil
STDIN.each_line do |line|
  # needed to cleanup invalid chars
  # line = line.encode('UTF-8', invalid: :replace, undef: :replace)
  if line =~ /STARTED \| (.+)$/
    time = line[0, 15]
    str = $1
    str.gsub!(/\w+, \d+ \w+ \d+ \d+:\d+:\d+ \w+ \+?\d+:\d+/) {|m| "Time.parse(\"#{m}\").utc"}
    pool.post {
      begin
        args = eval(str)
        if col = args['find']
          query = mongo[col].find(args['filter'], projection: args['projection'])
          query = query.limit(args['limit']) if args['limit']
          query = query.sort(args['sort']) if args['sort']
          query = query.skip(args['skip']) if args['skip']
          query.each {} if reads
          queries += 1
        elsif col = args['distinct']
          mongo[col].find(args['query']).distinct(args['key']) if reads
          queries += 1
        elsif col = args['count']
          query = mongo[col].find(args['query'])
          query.count(hint: args['hint']) if reads
          counts += 1
        elsif col = args['aggregate']
          query = mongo[col].aggregate(args['pipeline'])
          query.each {} if reads
          aggregates += 1
        elsif group = args['group']
          mongo.command(group: group) if reads
          aggregates += 1
        elsif col = args['findandmodify']
          mongo[col].find_one_and_update(args['query'], args['update']) if writes
          updates += 1
        elsif col = args['update']
          data = args['updates'][0]
          if data['multi'] == false
            mongo[col].update_one(data['q'], data['u']) if writes
          else
            mongo[col].update_many(data['q'], data['u']) if writes
          end
          updates += 1
        elsif col = args['insert']
          inserted_doc[col].concat(args['documents'].map {|d| d['_id']})
          mongo[col].insert_many(args['documents']) if writes
          inserts += 1
        elsif col = args['delete']
          data = args['deletes'][0]
          if data['limit'] == 1
            mongo[col].delete_one(data['q']) if writes
          else
            mongo[col].delete_many(data['q']) if writes
          end
          deletes += 1
        elsif args['getMore']
        else
          missing_topologies << args.keys
        end
      rescue SyntaxError => e
        err += 1
      rescue => e
        # p e
        # puts e.backtrace.join("\n")
        err += 1
      end
    }
  end
  lines += 1
  print "\rparsed %8d lines (%3d err) : %7d queries, %6d counts, %5d aggregates, %5d inserts, %5d updates, %5d deletes — %s (queue: %3d)" % [lines, err, queries, counts, aggregates, inserts, updates, deletes, time, pool.queue_length] if lines % 10 == 0
  break if stop #or lines >= 10_000
  sleep 0.1 while pool.queue_length > 900
end
pool.shutdown
while pool.queue_length > 0
  sleep 0.1
  print "\rparsed %8d lines (%3d err) : %7d queries, %6d counts, %5d aggregates, %5d inserts, %5d updates, %5d deletes — %s (queue: %3d)" % [lines, err, queries, counts, aggregates, inserts, updates, deletes, time, pool.queue_length]
end
pool.wait_for_termination

clock_time = Time.now - start
timings.sort!

puts
puts "              clock time: %.3f sec" % (clock_time)
puts "    total requests count: #{timings.size}"
if timings.size > 0
  puts " total requests duration: %.1f sec" % (timings.reduce(:+) / 1000)
  mean = timings.reduce(:+) / timings.size
  puts "  mean requests duration: %.1f ms" % mean
  puts "median requests duration: %.1f ms" % timings[timings.size / 2]
  puts " 90th%% requests duration: %.1f ms" % timings[(timings.size*0.9).floor]
  puts " 99th%% requests duration: %.1f ms" % timings[(timings.size*0.99).floor]
  stdev = Math.sqrt(timings.inject(0) {|acc, i| acc + (i-mean)**2 } / timings.size)
  puts " stdev requests duration: %.1f ms" % stdev
  puts "       requests / second: %.1f" % (timings.size / clock_time)
end
puts
inserted_doc.each do |col, docs|
  print "removing #{docs.count} documents inserted in #{col} →"
  res = mongo[col].delete_many(_id: {'$in' => docs}).first['n']
  puts " DONE: #{res} removed."
end

# p missing_topologies unless missing_topologies.empty?
# [x] ["aggregate", "pipeline", "cursor"]
# [x] ["count", "query"]
# [x] ["count", "query", "hint"]
# [x] ["getMore", "collection"]
# [x] ["find", "filter"]
# [x] ["find", "filter", "sort", "projection", "limit"]
# [x] ["find", "filter", "sort", "limit", "projection"]
# [x] ["update", "updates", "ordered"]
# [x] ["findandmodify", "query", "update", "sort", "new", "bypassDocumentValidation"]
# [x] ["insert", "documents", "ordered"]
# [x] ["delete", "deletes", "ordered"]
# [x] ["distinct", "key", "query"]
# [x] ["group"]
# [ ] ["listIndexes", "cursor"]