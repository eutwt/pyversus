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
    },
    {
      "id": "py_pyspark_dataframe",
      "label": "versus (pyspark cached dataframe)",
      "time": [
        7.0906,
        11.0228,
        14.73,
        24.4264,
        37.9726,
        64.411
      ],
      "memory": [
        2034.4,
        2850.9,
        3559.8,
        5814.7,
        9697.8,
        10525.7
      ]
    },
    {
      "id": "py_pyspark_parquet_scan",
      "label": "versus (pyspark parquet scan)",
      "time": [
        9.6252,
        14.4082,
        18.6702,
        30.7104,
        39.2619,
        66.8004
      ],
      "memory": [
        1985.2,
        2536.2,
        2922.2,
        6971.6,
        9947.5,
        10725.4
      ]
    }
  ]
};
