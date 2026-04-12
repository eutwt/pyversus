import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1] / "python"
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("pyversus2_tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
    )
    if sys.platform == "win32":
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
        builder = (
            builder.config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.python.worker.reuse", "false")
        )

    try:
        session = builder.getOrCreate()
    except Exception as exc:  # pragma: no cover - depends on local Java runtime
        pytest.skip(f"PySpark is unavailable in this environment: {exc}")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
