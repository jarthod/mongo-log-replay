require "mongo"
require "./json_patch"

stop = false
Signal::INT.trap { stop = true }

timings = [] of Float64
dry_run = false
reads = !dry_run
writes = !dry_run

client = Mongo::Client.new "mongodb://localhost"
mongo = client["mongo-log-replay"]

lines = err = queries = counts = aggregates = updates = inserts = deletes = 0
inserted_doc = Hash(String, Array(String)).new { |h, k| h[k] = [] of String }
missing_topologies = Set(Array(String)).new
start = Time.now
time = nil
STDIN.each_line do |line|
  if line =~ /STARTED \| (.+)$/
    time = line[0, 15]
    str = $1
    # str = str.gsub(/\w+, \d+ \w+ \d+ \d+:\d+:\d+ \w+ \+?\d+:\d+/) { |m| "Time.parse(\"#{m}\").utc" }
    begin
      timings << Time.measure {
        # puts str
        json = str.gsub(/(?<==>|\[|,)\s?:\"?([^\s,\"\]\}]+)\"?/, "\"\\1\"")               # Replace symbols with string
          .gsub(/<BSON::Binary:0x\w+ type=generic data=0x(\w+)\.\.\.>/, "\"BINARY:\\1\"") # Replace binary with string
          .gsub(/BSON::ObjectId\('([0-9a-f]+)'\)/, "\"\\1\"")                             # replace object id with string
          .gsub("=>", ":").gsub("nil", "null")
        args = JSON.parse(json).as_h
        args.delete("$readPreference")
        if col = args["find"]?
          # p args
          query = args["filter"].to_bson.as(BSON)
          projection = args["projection"]?.try(&.to_bson.as(BSON)) || BSON.new
          skip = args["skip"]?.try(&.as_i) || 0
          limit = args["limit"]?.try(&.as_i) || 0
          if sort = args["sort"]?
            query = {"$query" => query, "$orderby" => sort.to_bson.as(BSON)}
          end
          # if args["hint"]? → add to hash with $hint
          mongo[col.as_s].find(query, projection, LibMongoC::QueryFlags::NONE, skip, limit) { } if reads
          queries += 1
        elsif col = args["distinct"]?
          # TODO: support $in (args = {"distinct" => "users", "key" => "email", "query" => {"_id" => {"$in" => [1]}}})
          command = args.to_bson.as(BSON)
          mongo[col.as_s].command(command).first if reads
          queries += 1
        elsif col = args["count"]?
          # TODO add hint like find (args["hint"])
          mongo[col.as_s].count(args["query"].as_h) if reads
          counts += 1
        elsif col = args["aggregate"]?
          mongo[col.as_s].aggregate(args["pipeline"].as_a) { } if reads
          aggregates += 1
          # elsif group = args["group"]?
          # p args
          # mongo[col.as_s].command(group: group) # if reads
          # aggregates += 1
        elsif col = args["findAndModify"]?
          mongo[col.as_s].find_and_modify(args["query"].to_bson, args["update"].to_bson) if writes
          updates += 1
        elsif col = args["update"]?
          data = args["updates"][0]
          if data["multi"] == false
            mongo[col.as_s].update(data["q"], data["u"]) if writes
          else
            mongo[col.as_s].update(data["q"], data["u"], LibMongoC::UpdateFlags::MULTI_UPDATE) if writes
          end
          updates += 1
        elsif col = args["insert"]?
          docs = args["documents"].as_a
          # mongo[col.as_s].remove({"_id" => {"$in" => docs.map { |d| d["_id"].as_s }}})
          inserted_doc[col.as_s].concat(docs.map { |d| d["_id"].as_s })
          mongo[col.as_s].insert_bulk(docs.map(&.to_bson)) if writes
          inserts += 1
        elsif col = args["delete"]?
          data = args["deletes"][0]
          if data["limit"] == 1
            mongo[col.as_s].remove(data["q"], LibMongoC::RemoveFlags::SINGLE_REMOVE) if writes
          else
            mongo[col.as_s].remove(data["q"]) if writes
          end
          deletes += 1
          # elsif args["getMore"]
        else
          missing_topologies << args.keys
        end
      }.total_milliseconds
    rescue e : JSON::ParseException
      # p e
      # puts str
      # puts json
      err += 1
    end
  end
  lines += 1
  print "\rparsed %8d lines (%3d err) : %7d queries, %6d counts, %5d aggregates, %5d inserts, %5d updates, %5d deletes — %s" % [lines, err, queries, counts, aggregates, inserts, updates, deletes, time] if lines % 100 == 0
  break if stop
end

print "\rparsed %8d lines (%3d err) : %7d queries, %6d counts, %5d aggregates, %5d inserts, %5d updates, %5d deletes — %s" % [lines, err, queries, counts, aggregates, inserts, updates, deletes, time]

clock_time = Time.now - start
timings.sort!

puts
puts "              clock time: %.3f sec" % (clock_time.to_f)
puts "    total requests count: #{timings.size}"
if timings.size > 0
  puts " total requests duration: %.1f sec" % (timings.sum / 1000)
  mean = timings.sum / timings.size
  puts "  mean requests duration: %.2f ms" % mean
  puts "median requests duration: %.2f ms" % timings[timings.size / 2]
  puts " 90th%% requests duration: %.2f ms" % timings[(timings.size*0.9).floor.to_i]
  puts " 99th%% requests duration: %.2f ms" % timings[(timings.size*0.99).floor.to_i]
  puts "       requests / second: %.1f" % (timings.size / clock_time.to_f)
end
# puts
inserted_doc.each do |col, docs|
  puts "removing #{docs.size} documents inserted in #{col}"
  mongo[col].remove({"_id" => {"$in" => docs}}) if writes
end

# p missing_topologies unless missing_topologies.empty?
