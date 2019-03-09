# mongo-log-replay

The goal of this tool is to be able to read log lines produced by the ruby mongo driver, looking like:
```
MONGODB | localhost:27017 | database.find | STARTED | {"find"=>"users", "filter"=>{"email"=>"test@domain.com"}, "projection"=>{"_id"=>1}, "limit"=>1, "$readPreference"=>{"mode"=>"primary"}}
MONGODB | localhost:27017 | database.find | STARTED | {"find"=>"users", "filter"=>{"api_key"=>"ABCDEFGHIJKL"}, "projection"=>{"_id"=>1}, "limit"=>1, "$readPreference"=>{"mode"=>"primary"}}
MONGODB | localhost:27017 | database.insert | STARTED | {"insert"=>"users", "ordered"=>true, "documents"=>[{"_id"=>BSON::ObjectId('5c7bd7c3b2c79a79954c5496'), "time_zone"=>"Hong Kong", "credits"=>10, "favicon_fetcher"=>true, "email"=>"test@domain.com", "login"=>"test", "api_key"=>"ABCDEFGHIJKL", "updated_at"=>"2019-03-03T13:33:55.160Z", "created_at"=>"2019-03-03T13:33:55.160Z"}]}
```
From a big log file and replay them as fast as possible on another mongo instance / database for performance testing.

Example output:
```
> ./mongo-log-replay < test.log
parsed   279300 lines (10238 err) :  119632 queries,   2410 counts, 22967 aggregates, 29973 inserts, 12169 updates, 81893 deletes
              clock time: 69.425 sec
    total requests count: 269089
 total requests duration: 65.3 sec
  mean requests duration: 0.2 ms
median requests duration: 0.1 ms
 90th% requests duration: 0.4 ms
 99th% requests duration: 0.3 ms
       requests / second: 3875.9
removing 8718 documents inserted in users
removing 9296 documents inserted in …
```

I wrote a first version in ruby (`mongo-log-replay.rb`) which works decently but is very limited by ruby performance. I then tried to write a crystal version (`mongo-log-replay.cr`) which runs much faster of course but is less good at parsing the hash format and less acurate.

## Installation

Ruby:
Make sure you have `ruby` and `bundler` installed (tested on `2.4.1`)

Crystal:
Make sure you have `cystal` and `shard` installed (tested on `0.27.2`)
```
shard install
crystal build mongo-log-replay.cr
```
For some reasons building with `--release` leads to random crashes

## Usage

Ruby:
```sh
./mongo-log-replay.rb < logfile
# or using pipes:
cat logfile | head -1000 | ./mongo-log-replay.rb
```

Crystal:
```sh
./mongo-log-replay < logfile
# or using pipes:
cat logfile | head -1000 | ./mongo-log-replay
```

## Contributing

1. Fork it (<https://github.com/jarthod/mongo-log-replay/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Adrien Rey-Jarthon](https://github.com/jarthod) - creator and maintainer
