#!/usr/bin/env python3
"""
Partition tuning: vary spark.sql.shuffle.partitions (2, 4, 8, 16) and time
WordFrequency (DataFrame vs Spark SQL) on the full corpus glob.

Writes results/benchmark_partitions.csv
"""

import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import SparkSession

from src.benchmarking.corpus_paths import list_sorted_corpus_files, project_root
from src.benchmarking.utils import save_results, time_function
from src.dataframe_api.word_frequency import run_word_frequency
from src.spark_sql.word_frequency_sql import run_word_frequency_sql


def _wf_summary_df(r) -> str:
    row = r.collect()[0]
    return f"Top word: {row['words']} ({row['count']})"


def _wf_summary_sql(r) -> str:
    row = r.collect()[0]
    return f"Top word: {row['word']} ({row['count']})"


def main():
    corpus_dir = os.path.join(project_root(), "data", "corpus")
    paths = list_sorted_corpus_files(corpus_dir)
    if not paths:
        print(
            "No .txt files under data/corpus/. Run: python data/download_books.py",
            file=sys.stderr,
        )
        sys.exit(1)

    spark = SparkSession.builder.appName("BenchmarkPartitions").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    analyses = [
        ("WordFrequency", "DataFrame", run_word_frequency, _wf_summary_df),
        ("WordFrequency", "SparkSQL", run_word_frequency_sql, _wf_summary_sql),
    ]

    partitions = (2, 4, 8, 16)
    records = []
    path_label = ";".join(paths)

    for n in partitions:
        spark.conf.set("spark.sql.shuffle.partitions", str(n))
        print(f"\n{'=' * 70}\nshuffle.partitions = {n}\n{'=' * 70}")

        for name, api, func, summarizer in analyses:
            label = f"{name} ({api})"
            print(f"\nRunning {label}...")
            result, elapsed = time_function(func, spark, paths)
            summary = summarizer(result)
            print(f"  {summary}\n  Time: {elapsed:.3f}s")
            records.append(
                {
                    "shuffle_partitions": n,
                    "analysis": name,
                    "api_type": api,
                    "elapsed_seconds": round(elapsed, 4),
                    "num_files": len(paths),
                    "corpus_path": path_label[:500]
                    + ("..." if len(path_label) > 500 else ""),
                    "result_summary": summary,
                }
            )

    spark.stop()

    out = os.path.join(project_root(), "results", "benchmark_partitions.csv")
    save_results(records, out)
    print("\nPartition benchmark complete. Plot: python -m src.benchmarking.plot_results")


if __name__ == "__main__":
    main()
