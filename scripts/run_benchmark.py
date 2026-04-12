from __future__ import annotations

import argparse
import copy
import importlib.metadata as metadata
import json
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import suppress
from pathlib import Path
from typing import Dict, List

import duckdb
import nycflights13
import pandas
import psutil

from versus import compare

ROW_SIZES = [250_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000, 20_000_000]
WEATHER_COLUMNS = [
    "origin",
    "year",
    "month",
    "day",
    "hour",
    "temp",
    "dewp",
    "humid",
    "wind_dir",
    "wind_speed",
    "wind_gust",
    "precip",
    "pressure",
    "visib",
    "time_hour",
]
ALL_COLUMNS = ["benchmark_id"] + WEATHER_COLUMNS
DIFF_COLUMNS = ["temp", "dewp", "humid", "wind_dir"]
SAMPLE_MULTIPLIER = 7919
SAMPLE_OFFSET = 104_729
BENCHMARK_KEY = "benchmark_id"
KEEP_MODULUS = 20
DROP_LEFT_SLOT = 0
DROP_RIGHT_SLOT = 1
MUTATE_SLOT = 2
SERIES = [
    {
        "id": "py_pandas_dataframe",
        "label": "versus (pandas dataframe)",
        "mode": "pandas_dataframe",
        "description": (
            "`versus.compare()` on pandas DataFrames already held in memory."
        ),
    },
    {
        "id": "py_parquet_scan",
        "label": "versus (parquet scan)",
        "mode": "parquet_scan",
        "description": (
            "`versus.compare()` on DuckDB relations backed by parquet files."
        ),
    },
]
PYSPARK_RESULTS_PATH = Path(
    "docs_api/source/_generated/benchmark_results_pyspark.json"
)
DUCKDB_RESULTS_PATH = Path(
    "docs_api/source/_generated/benchmark_results_duckdb.json"
)
PYSPARK_SERIES_OVERRIDES = {
    "pyspark_dataframe": {
        "id": "py_pyspark_dataframe",
        "label": "versus (pyspark cached dataframe)",
        "description": (
            "`versus.compare()` on PySpark DataFrames cached before timing starts."
        ),
    },
    "pyspark_parquet_scan": {
        "id": "py_pyspark_parquet_scan",
        "label": "versus (pyspark parquet scan)",
        "description": (
            "`versus.compare()` on PySpark DataFrames read from parquet "
            "without caching."
        ),
    },
}


class ProcessTreeSampler:
    def __init__(self, interval_seconds: float = 0.05) -> None:
        self.interval_seconds = interval_seconds
        self._root = psutil.Process()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self.peak_rss_bytes = 0

    def start(self) -> None:
        self._sample_once()
        self._thread.start()

    def stop(self) -> int:
        self._stop.set()
        self._thread.join()
        self._sample_once()
        out = self.peak_rss_bytes
        return out

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self._sample_once()

    def _sample_once(self) -> None:
        total = 0
        child_processes = []
        with suppress(
            psutil.NoSuchProcess,
            psutil.AccessDenied,
            PermissionError,
        ):
            child_processes = self._root.children(recursive=True)
        processes = [self._root] + child_processes
        for process in processes:
            with suppress(
                psutil.NoSuchProcess,
                psutil.AccessDenied,
                PermissionError,
            ):
                total += process.memory_info().rss
        self.peak_rss_bytes = max(self.peak_rss_bytes, total)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the pyversus benchmark and regenerate docs assets."
    )
    parser.add_argument(
        "--rows",
        nargs="+",
        type=int,
        default=ROW_SIZES,
        help="Benchmark row sizes to run.",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=3,
        help="Fresh-process repeats per method and row size.",
    )
    parser.add_argument(
        "--cooldown-seconds",
        type=float,
        default=30.0,
        help="Seconds to wait between measured runs.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("docs_api/source/_generated/benchmark_results.json"),
        help="Where to write the raw benchmark results JSON.",
    )
    parser.add_argument(
        "--output-summary",
        type=Path,
        default=Path("docs_api/source/_generated/benchmark_summary.rst"),
        help="Where to write the generated benchmark summary include.",
    )
    parser.add_argument(
        "--output-js",
        type=Path,
        default=Path("docs_api/source/_static/js/benchmark_data.js"),
        help="Where to write the generated benchmark chart data.",
    )
    parser.add_argument(
        "--reuse-existing",
        action="store_true",
        help=(
            "Reuse the benchmark JSON already stored at --output-json instead "
            "of rerunning the benchmark workers."
        ),
    )
    parser.add_argument(
        "--duckdb-results-json",
        type=Path,
        default=DUCKDB_RESULTS_PATH,
        help=(
            "Stored DuckDB benchmark results to reuse with --reuse-existing "
            "before merging any PySpark series."
        ),
    )
    parser.add_argument(
        "--pyspark-results-json",
        type=Path,
        default=PYSPARK_RESULTS_PATH,
        help=(
            "Stored PySpark benchmark results to merge into the generated "
            "docs assets."
        ),
    )
    parser.add_argument(
        "--worker",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--mode", choices=[item["mode"] for item in SERIES])
    parser.add_argument("--rows-single", type=int)
    out = parser.parse_args()
    return out


def load_base_weather() -> pandas.DataFrame:
    weather = nycflights13.weather.copy()
    weather.insert(0, "__source_row", range(len(weather)))
    out = weather
    return out


def sql_literal(path: Path) -> str:
    out = str(path).replace("'", "''")
    return out


def sampled_sql(base_count: int, rows: int) -> str:
    selected_columns = ", ".join(f"base.{column}" for column in WEATHER_COLUMNS)
    out = f"""
WITH benchmark_ids AS (
    SELECT
        range AS benchmark_id,
        ((range * {SAMPLE_MULTIPLIER}) + {SAMPLE_OFFSET}) % {base_count}
            AS source_row
    FROM range({rows})
)
SELECT
    benchmark_id,
    {selected_columns}
FROM benchmark_ids
JOIN weather_base AS base
    ON base.__source_row = benchmark_ids.source_row
"""
    return out


def left_sql() -> str:
    column_list = ", ".join(ALL_COLUMNS)
    out = f"""
SELECT
    {column_list}
FROM benchmark_sampled
WHERE benchmark_id % {KEEP_MODULUS} <> {DROP_LEFT_SLOT}
"""
    return out


def right_sql() -> str:
    unchanged_columns = [
        "benchmark_id",
        "origin",
        "year",
        "month",
        "day",
        "hour",
    ]
    tail_columns = [
        "wind_speed",
        "wind_gust",
        "precip",
        "pressure",
        "visib",
        "time_hour",
    ]
    select_parts = unchanged_columns + [
        f"""
CASE
    WHEN benchmark_id % {KEEP_MODULUS} = {MUTATE_SLOT}
        THEN temp + 1.5
    ELSE temp
END AS temp
""".strip(),
        f"""
CASE
    WHEN benchmark_id % {KEEP_MODULUS} = {MUTATE_SLOT}
        THEN dewp - 1.25
    ELSE dewp
END AS dewp
""".strip(),
        f"""
CASE
    WHEN benchmark_id % {KEEP_MODULUS} = {MUTATE_SLOT}
        THEN CASE
            WHEN humid IS NULL THEN 7.0
            ELSE LEAST(humid + 7.0, 100.0)
        END
    ELSE humid
END AS humid
""".strip(),
        f"""
CASE
    WHEN benchmark_id % {KEEP_MODULUS} = {MUTATE_SLOT}
        THEN CASE
            WHEN wind_dir IS NULL THEN 15.0
            ELSE MOD(wind_dir + 15.0, 360.0)
        END
    ELSE wind_dir
END AS wind_dir
""".strip(),
    ] + tail_columns
    out = f"""
SELECT
    {", ".join(select_parts)}
FROM benchmark_sampled
WHERE benchmark_id % {KEEP_MODULUS} <> {DROP_RIGHT_SLOT}
"""
    return out


def prepare_views(conn: duckdb.DuckDBPyConnection, rows: int) -> None:
    base_weather = load_base_weather()
    conn.register("weather_base_df", base_weather)
    conn.execute(
        """
CREATE OR REPLACE TEMP VIEW weather_base AS
SELECT *
FROM weather_base_df
"""
    )
    conn.execute(
        "CREATE OR REPLACE TEMP VIEW benchmark_sampled AS "
        + sampled_sql(len(base_weather), rows)
    )


def dropped_count(rows: int, slot: int) -> int:
    if rows <= slot:
        return 0
    out = ((rows - 1 - slot) // KEEP_MODULUS) + 1
    return out


def kept_count(rows: int, slot: int) -> int:
    out = rows - dropped_count(rows, slot)
    return out


def run_worker(mode: str, rows: int) -> Dict[str, float]:
    if mode == "pandas_dataframe":
        out = run_pandas_worker(rows)
        return out
    out = run_parquet_worker(rows)
    return out


def run_pandas_worker(rows: int) -> Dict[str, float]:
    build_conn = duckdb.connect()
    try:
        prepare_views(build_conn, rows)
        left_df = build_conn.sql(left_sql()).df()
        right_df = build_conn.sql(right_sql()).df()
    finally:
        build_conn.close()

    compare_conn = duckdb.connect()
    comparison = None
    sampler = ProcessTreeSampler()
    sampler.start()
    try:
        started = time.perf_counter()
        comparison = compare(left_df, right_df, by=[BENCHMARK_KEY], con=compare_conn)
        elapsed_seconds = time.perf_counter() - started
    finally:
        peak_rss_bytes = sampler.stop()
        if comparison is not None:
            comparison.close()
        compare_conn.close()

    out = {
        "elapsed_seconds": elapsed_seconds,
        "peak_memory_mb": peak_rss_bytes / (1024 * 1024),
        "left_rows": kept_count(rows, DROP_LEFT_SLOT),
        "right_rows": kept_count(rows, DROP_RIGHT_SLOT),
    }
    return out


def run_parquet_worker(rows: int) -> Dict[str, float]:
    with tempfile.TemporaryDirectory(prefix="pyversus-benchmark-") as temp_dir:
        temp_root = Path(temp_dir)
        left_path = temp_root / "left.parquet"
        right_path = temp_root / "right.parquet"

        build_conn = duckdb.connect()
        try:
            prepare_views(build_conn, rows)
            build_conn.execute(
                "COPY (" + left_sql() + ") TO '"
                + sql_literal(left_path)
                + "' (FORMAT PARQUET)"
            )
            build_conn.execute(
                "COPY (" + right_sql() + ") TO '"
                + sql_literal(right_path)
                + "' (FORMAT PARQUET)"
            )
        finally:
            build_conn.close()

        compare_conn = duckdb.connect()
        comparison = None
        left_rel = compare_conn.read_parquet(str(left_path))
        right_rel = compare_conn.read_parquet(str(right_path))
        sampler = ProcessTreeSampler()
        sampler.start()
        try:
            started = time.perf_counter()
            comparison = compare(
                left_rel,
                right_rel,
                by=[BENCHMARK_KEY],
                con=compare_conn,
            )
            elapsed_seconds = time.perf_counter() - started
        finally:
            peak_rss_bytes = sampler.stop()
            if comparison is not None:
                comparison.close()
            compare_conn.close()

    out = {
        "elapsed_seconds": elapsed_seconds,
        "peak_memory_mb": peak_rss_bytes / (1024 * 1024),
        "left_rows": kept_count(rows, DROP_LEFT_SLOT),
        "right_rows": kept_count(rows, DROP_RIGHT_SLOT),
    }
    return out


def machine_details() -> Dict[str, str]:
    details = {
        "model_name": "Unknown machine",
        "model_identifier": "unknown",
        "chip": "unknown",
        "cores": str(psutil.cpu_count(logical=False) or psutil.cpu_count() or "?"),
        "memory": f"{round(psutil.virtual_memory().total / (1024 ** 3))} GB",
        "system_version": sys.platform,
    }
    if sys.platform != "darwin":
        return details
    with suppress(Exception):
        output = subprocess.check_output(
            ["/usr/sbin/system_profiler", "SPHardwareDataType", "SPSoftwareDataType"],
            text=True,
        )
        for line in output.splitlines():
            stripped = line.strip()
            if ": " not in stripped:
                continue
            key, value = stripped.split(": ", 1)
            if key == "Model Name":
                details["model_name"] = value
            elif key == "Model Identifier":
                details["model_identifier"] = value
            elif key == "Chip":
                details["chip"] = value
            elif key == "Total Number of Cores":
                details["cores"] = value
            elif key == "Memory":
                details["memory"] = value
            elif key == "System Version":
                details["system_version"] = value
    out = details
    return out


def benchmark_versions() -> Dict[str, str]:
    out = {
        "python": sys.version.split()[0],
        "pyversus2": metadata.version("pyversus2"),
        "duckdb": metadata.version("duckdb"),
        "nycflights13": metadata.version("nycflights13"),
        "pandas": metadata.version("pandas"),
        "psutil": metadata.version("psutil"),
        "setuptools": metadata.version("setuptools"),
    }
    return out


def git_head(repo_root: Path) -> str:
    out = subprocess.check_output(
        ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
        text=True,
    ).strip()
    return out


def format_rows(value: int) -> str:
    if value >= 1_000_000:
        number = value / 1_000_000
        suffix = "M"
    elif value >= 1_000:
        number = value / 1_000
        suffix = "k"
    else:
        out = str(value)
        return out
    if number.is_integer():
        out = f"{int(number)}{suffix}"
        return out
    out = f"{number:.1f}{suffix}"
    return out


def format_seconds(seconds: float) -> str:
    if seconds.is_integer():
        out = str(int(seconds))
        return out
    out = f"{seconds:g}"
    return out


def sleep_for_cooldown(seconds: float) -> None:
    if seconds <= 0:
        return
    print(
        f"[benchmark] Cooling down for {format_seconds(seconds)}s "
        "before the next run...",
        file=sys.stderr,
        flush=True,
    )
    time.sleep(seconds)


def build_results_template(
    args: argparse.Namespace,
    repo_root: Path,
) -> Dict[str, object]:
    base_weather = load_base_weather()
    series_results = [
        {
            "id": item["id"],
            "label": item["label"],
            "mode": item["mode"],
            "description": item["description"],
            "points": [{"rows": rows, "measurements": []} for rows in args.rows],
        }
        for item in SERIES
    ]
    out = {
        "metadata": {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "repo": "pyversus",
            "git_head": git_head(repo_root),
            "dataset": {
                "name": "nycflights13.weather",
                "rows": len(base_weather),
                "columns": WEATHER_COLUMNS,
                "benchmark_key": BENCHMARK_KEY,
                "sample_multiplier": SAMPLE_MULTIPLIER,
                "sample_offset": SAMPLE_OFFSET,
                "keep_modulus": KEEP_MODULUS,
                "drop_left_slot": DROP_LEFT_SLOT,
                "drop_right_slot": DROP_RIGHT_SLOT,
                "mutate_slot": MUTATE_SLOT,
                "sampling": (
                    "deterministic sampling with replacement from the weather "
                    "table via a synthetic benchmark key and fixed formulas"
                ),
                "left_keep_fraction": 0.95,
                "right_keep_fraction": 0.95,
                "diff_columns": DIFF_COLUMNS,
                "diff_fraction": 0.05,
            },
            "machine": machine_details(),
            "versions": benchmark_versions(),
            "settings": {
                "rows": args.rows,
                "repeats": args.repeats,
                "cooldown_seconds": args.cooldown_seconds,
                "series_order": "alternating per repeat",
                "timed_scope": "compare() only",
            },
            "rerun_command": (
                "env UV_CACHE_DIR=.uv_cache uv run --extra benchmark "
                "python scripts/run_benchmark.py --repeats "
                f"{args.repeats} --cooldown-seconds "
                f"{format_seconds(args.cooldown_seconds)}"
            ),
        },
        "series": series_results,
    }
    return out


def series_order(repeat_index: int) -> List[Dict[str, str]]:
    if repeat_index % 2 == 0:
        out = list(SERIES)
        return out
    out = list(reversed(SERIES))
    return out


def point_lookup(results: Dict[str, object]) -> Dict[tuple, Dict[str, object]]:
    lookup = {}
    for series_result in results["series"]:
        for point in series_result["points"]:
            lookup[(series_result["id"], point["rows"])] = point
    out = lookup
    return out


def write_partial_results(output_json: Path, results: Dict[str, object]) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(results, indent=2) + "\n")


def run_worker_subprocess(
    repo_root: Path,
    mode: str,
    rows: int,
) -> Dict[str, object]:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--mode",
        mode,
        "--rows-single",
        str(rows),
    ]
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            cwd=repo_root,
        )
    except subprocess.CalledProcessError as error:
        if error.stdout:
            print(error.stdout, file=sys.stderr, end="")
        if error.stderr:
            print(error.stderr, file=sys.stderr, end="")
        raise
    out = json.loads(completed.stdout)
    return out


def collect_measurements(
    args: argparse.Namespace,
    repo_root: Path,
) -> Dict[str, object]:
    results = build_results_template(args, repo_root)
    points = point_lookup(results)
    run_index = 0
    for rows in args.rows:
        for repeat in range(args.repeats):
            for series in series_order(repeat):
                if run_index:
                    sleep_for_cooldown(args.cooldown_seconds)
                print(
                    (
                        f"[benchmark] Running {series['label']} at {rows:,} rows "
                        f"(repeat {repeat + 1}/{args.repeats})"
                    ),
                    file=sys.stderr,
                    flush=True,
                )
                payload = run_worker_subprocess(repo_root, series["mode"], rows)
                payload["repeat"] = repeat + 1
                payload["run_index"] = run_index + 1
                points[(series["id"], rows)]["measurements"].append(payload)
                run_index += 1
                write_partial_results(args.output_json, results)
    out = results
    return out


def finalize_results(results: Dict[str, object]) -> Dict[str, object]:
    for series_result in results["series"]:
        for point in series_result["points"]:
            measurements = point["measurements"]
            point["median_elapsed_seconds"] = statistics.median(
                item["elapsed_seconds"] for item in measurements
            )
            point["median_peak_memory_mb"] = statistics.median(
                item["peak_memory_mb"] for item in measurements
            )
    out = results
    return out


def ensure_matching_rows(
    base_results: Dict[str, object],
    pyspark_results: Dict[str, object],
) -> None:
    base_rows = base_results["metadata"]["settings"]["rows"]
    spark_rows = pyspark_results["metadata"]["settings"]["rows"]
    if base_rows != spark_rows:
        raise ValueError(
            "Stored PySpark benchmark rows do not match the DuckDB benchmark "
            "rows in this repo."
        )


def source_run_summary(
    metadata_block: Dict[str, object],
    *,
    backend: str,
) -> Dict[str, object]:
    out = {
        "backend": backend,
        "repo": metadata_block.get("repo"),
        "git_head": metadata_block.get("git_head"),
        "generated_at": metadata_block.get("generated_at"),
        "machine": metadata_block.get("machine"),
        "versions": metadata_block.get("versions"),
        "settings": metadata_block.get("settings"),
    }
    return out


def merge_pyspark_results(
    results: Dict[str, object],
    pyspark_results_json: Path,
    repo_root: Path,
) -> Dict[str, object]:
    metadata_block = results["metadata"]
    original_metadata = copy.deepcopy(metadata_block)
    spark_results = None
    if pyspark_results_json.exists():
        spark_results = json.loads(pyspark_results_json.read_text())
        ensure_matching_rows(results, spark_results)
    spark_series_ids = set(PYSPARK_SERIES_OVERRIDES)
    spark_series_ids.update(
        item["id"] for item in PYSPARK_SERIES_OVERRIDES.values()
    )
    results["series"] = [
        item for item in results["series"] if item["id"] not in spark_series_ids
    ]
    metadata_block["repo"] = "pyversus2"
    metadata_block["git_head"] = git_head(repo_root)
    metadata_block["generated_at"] = time.strftime(
        "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
    )
    source_runs = [source_run_summary(original_metadata, backend="duckdb")]
    if spark_results is None:
        metadata_block["source_runs"] = source_runs
        return results
    for item in spark_results["series"]:
        merged_item = copy.deepcopy(item)
        overrides = PYSPARK_SERIES_OVERRIDES.get(merged_item["id"], {})
        merged_item.update(overrides)
        results["series"].append(merged_item)
    source_runs.append(
        source_run_summary(spark_results["metadata"], backend="pyspark")
    )
    metadata_block["source_runs"] = source_runs
    return results


def summary_lines(results: Dict[str, object]) -> List[str]:
    metadata_block = results["metadata"]
    dataset = metadata_block["dataset"]
    machine = metadata_block["machine"]
    settings = metadata_block["settings"]
    source_runs = metadata_block.get("source_runs", [])
    spark_source = next(
        (item for item in source_runs if item.get("backend") == "pyspark"),
        None,
    )
    lines = [
        (
            "These benchmarks show time spent and peak memory observed while "
            "running `versus.compare()` with the default "
            "`materialize=\"all\"` setting."
        ),
        "",
        (
            "The benchmark dataset starts from the Python `nycflights13` "
            "`weather` table. Each benchmark size builds a deterministic "
            "sample with replacement from that table using a synthetic "
            f"`{dataset['benchmark_key']}` key. The left and right inputs each "
            "keep 95% of those keys, and 5% of the sampled keys that remain "
            "on both sides are mutated in 4 columns (`temp`, `dewp`, "
            "`humid`, `wind_dir`)."
        ),
        "",
        (
            "Synthetic benchmark sizes: "
            + ", ".join(format_rows(value) for value in settings["rows"])
            + "."
        ),
        "",
        (
            "Each point is the median of "
            f"{settings['repeats']} fresh-process runs with "
            f"{format_seconds(settings['cooldown_seconds'])} seconds of "
            "cooldown between runs. Only the `compare()` call is timed; "
            "input generation happens before timing starts."
        ),
        "",
        (
            "Benchmarks were run on "
            f"{machine['model_name']} with {machine['chip']} and "
            f"{machine['memory']}."
        ),
    ]
    if spark_source is not None:
        spark_settings = spark_source.get("settings", {})
        lines.extend(
            [
                "",
                (
                    "This combined view keeps the stored DuckDB-backed and "
                    "PySpark-backed benchmark runs that pyversus2 now unifies "
                    "behind the same `versus.compare()` entry point."
                ),
            ]
        )
        spark_bits = []
        spark_master = spark_settings.get("spark_master")
        if spark_master:
            spark_bits.append(f"`{spark_master}`")
        spark_parallelism = spark_settings.get("spark_parallelism")
        if spark_parallelism:
            spark_bits.append(f"parallelism {spark_parallelism}")
        spark_driver_memory_gb = spark_settings.get("spark_driver_memory_gb")
        if spark_driver_memory_gb:
            spark_bits.append(f"{spark_driver_memory_gb} GB driver memory")
        if spark_bits:
            lines.extend(
                [
                    "",
                    "The stored PySpark runs used "
                    + ", ".join(spark_bits[:-1] + [spark_bits[-1]])
                    + ".",
                ]
            )
    lines.extend(
        [
            "",
            "Methods:",
            "",
        ]
    )
    lines.extend(f"- {series['description']}" for series in results["series"])
    lines.extend(
        [
            "",
            "Hover a point to see the exact value.",
        ]
    )
    out = lines
    return out


def chart_payload(results: Dict[str, object]) -> Dict[str, object]:
    rows = results["metadata"]["settings"]["rows"]
    series = [
        {
            "id": item["id"],
            "label": item["label"],
            "time": [
                round(point["median_elapsed_seconds"], 4)
                for point in item["points"]
            ],
            "memory": [
                round(point["median_peak_memory_mb"], 1)
                for point in item["points"]
            ],
        }
        for item in results["series"]
    ]
    out = {"rows": rows, "series": series}
    return out


def write_outputs(
    output_json: Path,
    output_summary: Path,
    output_js: Path,
    results: Dict[str, object],
) -> None:
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_summary.parent.mkdir(parents=True, exist_ok=True)
    output_js.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(results, indent=2) + "\n")
    output_summary.write_text("\n".join(summary_lines(results)) + "\n")
    payload = chart_payload(results)
    output_js.write_text(
        "window.PYVERSUS_BENCHMARK_DATA = "
        + json.dumps(payload, indent=2)
        + ";\n"
    )


def print_worker_payload(payload: Dict[str, float]) -> None:
    print(json.dumps(payload))


def main() -> None:
    args = parse_args()
    if args.worker:
        payload = run_worker(args.mode, args.rows_single)
        print_worker_payload(payload)
        return

    repo_root = Path(__file__).resolve().parents[1]
    if args.reuse_existing:
        source_json = args.duckdb_results_json
        if not source_json.exists():
            source_json = args.output_json
        results = json.loads(source_json.read_text())
    else:
        results = collect_measurements(args, repo_root)
        results = finalize_results(results)
    results = merge_pyspark_results(
        results,
        args.pyspark_results_json,
        repo_root,
    )
    write_outputs(args.output_json, args.output_summary, args.output_js, results)
    print(
        (
            "[benchmark] Wrote benchmark results to "
            f"{args.output_json}, {args.output_summary}, and {args.output_js}"
        ),
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
