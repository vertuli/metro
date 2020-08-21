# metro

Getting pings from http://api.metro.net and using Spark's structured streaming to create a history of vehicle paths.

## pings
- `mill metro.pings.run` - gets pings from `api.metro.net` for the agencies `lametro` and `lametro-rail` and writes them as JSON lines to `data/pings/`, partitioned by `agency_id` and `localdate`.
- `mill metro.pings.test` - runs pings tests

## paths
- `mill metro.paths.test` - runs paths tests

