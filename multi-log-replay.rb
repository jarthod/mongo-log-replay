#!/usr/bin/env ruby
# https://github.com/jarthod/mongo-log-replay

# Run mongo log replay N times in different processes and distribute the
# log lines to all of them. Each query will only be executed once.

# usage: cat ruby-application.log | ./multi-log-replay.rb mongodb://localhost

childs = []
N = 20
puts "Starting #{N} workers"
N.times do |i|
  p_read, p_write = IO.pipe
  pid = spawn({"CLUSTER" => "1"}, "ruby -E utf-8 mongo-log-replay.rb #{ARGV.first}", in: p_read)
  childs << {
    pid: pid,
    stdin: p_write
  }
end

STDIN.each_line.with_index do |line, i|
  childs[i%N][:stdin].puts(line)
end

puts "Finishing"
childs.each do |child|
  child[:stdin].close
  Process.waitpid(child[:pid])
end

