window.PYVERSUS_BENCHMARK_DATA = {
  "rows": [
    250000,
    1000000,
    2000000,
    5000000,
    10000000,
    20000000
  ],
  "series": [
    {
      "id": "py_pandas_dataframe",
      "label": "versus (pandas dataframe)",
      "time": [
        0.1433,
        0.3922,
        0.6624,
        1.6926,
        4.0714,
        16.6313
      ],
      "memory": [
        507.0,
        1401.0,
        2479.9,
        6207.4,
        9311.3,
        11743.0
      ]
    },
    {
      "id": "py_parquet_scan",
      "label": "versus (parquet scan)",
      "time": [
        0.1218,
        0.2246,
        0.3891,
        0.7366,
        2.0208,
        4.2365
      ],
      "memory": [
        371.4,
        577.5,
        749.5,
        1194.8,
        2054.5,
        3767.2
      ]
    }
  ]
};
