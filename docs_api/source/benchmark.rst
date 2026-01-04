Benchmark
=========

These benchmarks show time spent and memory used by `versus.compare()`. The data
comes from the Python `nycflights13` package (the `weather` table). Each size is
sampled with replacement from the original table and keeps 95% of rows on each
side. In 4 of the 15 columns (`temp`, `dewp`, `humid`, `wind_dir`), 5% of values
differ between the two inputs. For the parquet scan case, the sampled tables are
written to parquet before running `compare()`.

Row sizes: 250k, 1M, 2M, 5M, 10M, 20M.

Benchmarks were run on a 2020 13-inch MacBook Pro (2.3 GHz quad-core Intel Core i7, 32 GB RAM).

Methods:

- `py_in_memory`: `versus.compare()` on pandas DataFrames.
- `py_parquet_scan`: `versus.compare()` on DuckDB relations scanning parquet.

Hover a point to see the exact value.

.. raw:: html

   <div class="benchmark-charts">
    <div class="benchmark-chart" data-benchmark-chart="time" data-title="Benchmark time by input size" data-y-label="Median time"></div>
    <div class="benchmark-chart" data-benchmark-chart="memory" data-title="Benchmark memory by input size" data-y-label="Peak memory"></div>
   </div>
   <script>
   window.PYVERSUS_BENCHMARK_DATA = {
     rows: [250000, 1000000, 2000000, 5000000, 10000000, 20000000],
     series: [
       {
         id: "py_in_memory",
         label: "versus (in-memory)",
         time: [0.2018, 0.5649, 1.0123, 2.2891, 4.5795, 13.1619],
         memory: [130.5, 494.8, 820.8, 1928.8, 3729.9, 7428.8]
       },
       {
         id: "py_parquet_scan",
         label: "versus (parquet scan)",
         time: [0.1881, 0.3918, 0.7402, 1.5392, 3.3379, 6.7015],
         memory: [91.3, 282.5, 377.8, 882.2, 1623.9, 3104.3]
       }
     ]
   };
   </script>
   <script src="_static/js/benchmark_charts.js" defer></script>
