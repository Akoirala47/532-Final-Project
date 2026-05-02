#!/usr/bin/env python3
"""
Multi-size API benchmark: DataFrame vs Spark SQL across small / medium / large
subsets of data/corpus/*.txt (same analyses as benchmark_runner + TF-IDF).

Writes results/benchmark_scaling.csv and can plot via plot_results.py.
"""

import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import SparkSession

from src.benchmarking.corpus_paths import project_root, subsets_by_book_count
from src.benchmarking.utils import save_results, time_function
from src.dataframe_api.word_count import run_word_count
from src.dataframe_api.word_frequency import run_word_frequency
from src.dataframe_api.word_pairs import run_word_pairs
from src.dataframe_api.tfidf import run_tfidf
from src.spark_sql.word_count_sql import run_word_count_sql
from src.spark_sql.word_frequency_sql import run_word_frequency_sql
from src.spark_sql.word_pairs_sql import run_word_pairs_sql
from src.spark_sql.tfidf_sql import run_tfidf_sql


def _wc_summary(r: int) -> str:
    return f"Total words: {r:,}"


def _wf_summary_df(r) -> str:
    row = r.collect()[0]
    return f"Top word: {row['words']} ({row['count']})"


def _wf_summary_sql(r) -> str:
    row = r.collect()[0]
    return f"Top word: {row['word']} ({row['count']})"


def _wp_summary_df(r) -> str:
    row = r.collect()[0]
    return f"Top pair: ({row['word1']}, {row['word2']}) ({row['count']})"


def _wp_summary_sql(r) -> str:
    row = r.collect()[0]
    return f"Top pair: ({row['word1']}, {row['word2']}) ({row['count']})"


def _tfidf_summary(r) -> str:
    row = r.collect()[0]
    return f"Top term: {row['word']} ({row['max_tfidf']:.6f})"


def main():
    corpus_dir = os.path.join(project_root(), "data", "corpus")
    subsets = subsets_by_book_count(corpus_dir, sizes=(5, 12, 20))

    if not any(subsets.values()):
        print(
            "No .txt files under data/corpus/. Run: python data/download_books.py",
            file=sys.stderr,
        )
        sys.exit(1)

    spark = SparkSession.builder.appName("BenchmarkScaling").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    analyses = [
        ("WordCount", "DataFrame", run_word_count, _wc_summary),
        ("WordCount", "SparkSQL", run_word_count_sql, _wc_summary),
        ("WordFrequency", "DataFrame", run_word_frequency, _wf_summary_df),
        ("WordFrequency", "SparkSQL", run_word_frequency_sql, _wf_summary_sql),
        ("WordPairs", "DataFrame", run_word_pairs, _wp_summary_df),
        ("WordPairs", "SparkSQL", run_word_pairs_sql, _wp_summary_sql),
        ("TFIDF", "DataFrame", run_tfidf, _tfidf_summary),
        ("TFIDF", "SparkSQL", run_tfidf_sql, _tfidf_summary),
    ]

    records = []

    for dataset_size, paths in subsets.items():
        if not paths:
            print(f"Skipping dataset_size={dataset_size!r}: empty path list")
            continue

        path_label = ";".join(paths)
        n_files = len(paths)
        print(f"\n{'=' * 70}\nDataset {dataset_size} ({n_files} files)\n{'=' * 70}")

        for name, api, func, summarizer in analyses:
            label = f"{name} ({api})"
            print(f"\nRunning {label}...")
            result, elapsed = time_function(func, spark, paths)
            summary = summarizer(result)
            print(f"  {summary}\n  Time: {elapsed:.3f}s")
            records.append(
                {
                    "dataset_size": dataset_size,
                    "num_files": n_files,
                    "analysis": name,
                    "api_type": api,
                    "elapsed_seconds": round(elapsed, 4),
                    "corpus_path": path_label[:500]
                    + ("..." if len(path_label) > 500 else ""),
                    "result_summary": summary,
                }
            )

    spark.stop()

    out = os.path.join(project_root(), "results", "benchmark_scaling.csv")
    save_results(records, out)
    print("\nScaling benchmark complete. Generate charts: python -m src.benchmarking.plot_results")


if __name__ == "__main__":
    main()
