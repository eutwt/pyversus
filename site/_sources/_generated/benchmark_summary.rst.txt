These benchmarks show time spent and peak memory observed while running `versus.compare()` with the default `materialize="all"` setting.

The benchmark dataset starts from the Python `nycflights13` `weather` table. Each benchmark size builds a deterministic sample with replacement from that table using a synthetic `benchmark_id` key. The left and right inputs each keep 95% of those keys, and 5% of the sampled keys that remain on both sides are mutated in 4 columns (`temp`, `dewp`, `humid`, `wind_dir`).

Synthetic benchmark sizes: 250k, 1M, 2M, 5M, 10M, 20M.

Each point is the median of 3 fresh-process runs with 30 seconds of cooldown between runs. Only the `compare()` call is timed; input generation happens before timing starts.

Benchmarks were run on MacBook Air with Apple M5 and 32 GB.

This combined view keeps the stored DuckDB-backed and PySpark-backed benchmark runs behind the same `versus.compare()` entry point.

The stored PySpark runs used `local[*]`, parallelism 10, 10 GB driver memory.

Methods:

- `versus.compare()` on pandas DataFrames already held in memory.
- `versus.compare()` on DuckDB relations backed by parquet files.
- `versus.compare()` on PySpark DataFrames cached before timing starts.
- `versus.compare()` on PySpark DataFrames read from parquet without caching.

Hover a point to see the exact value.
