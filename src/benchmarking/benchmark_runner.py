#!/usr/bin/env python3
"""
Benchmark Runner

Runs all six analyses (3 DataFrame API + 3 Spark SQL) on the corpus,
records execution times, and saves results to CSV.
"""

import os
import sys

# Add project root to path so we can import src modules
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import SparkSession
from src.benchmarking.utils import time_function, save_results

# Import analysis functions
from src.dataframe_api.word_count import run_word_count
from src.dataframe_api.word_frequency import run_word_frequency
from src.dataframe_api.word_pairs import run_word_pairs
from src.spark_sql.word_count_sql import run_word_count_sql
from src.spark_sql.word_frequency_sql import run_word_frequency_sql
from src.spark_sql.word_pairs_sql import run_word_pairs_sql


def main():
    corpus_path = os.path.join(PROJECT_ROOT, "data", "corpus", "*.txt")

    # Allow override via command line
    if len(sys.argv) > 1:
        corpus_path = sys.argv[1]

    print(f"Corpus path: {corpus_path}")
    print(f"{'='*70}")

    # Create a shared SparkSession
    spark = SparkSession.builder \
        .appName("BenchmarkRunner") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Define all analyses to benchmark
    analyses = [
        {
            "name": "WordCount",
            "api": "DataFrame",
            "func": run_word_count,
            "format_result": lambda r: f"Total words: {r:,}",
        },
        {
            "name": "WordCount",
            "api": "SparkSQL",
            "func": run_word_count_sql,
            "format_result": lambda r: f"Total words: {r:,}",
        },
        {
            "name": "WordFrequency",
            "api": "DataFrame",
            "func": run_word_frequency,
            "format_result": lambda r: f"Top word: {r.collect()[0]['words']} ({r.collect()[0]['count']})",
        },
        {
            "name": "WordFrequency",
            "api": "SparkSQL",
            "func": run_word_frequency_sql,
            "format_result": lambda r: f"Top word: {r.collect()[0]['word']} ({r.collect()[0]['count']})",
        },
        {
            "name": "WordPairs",
            "api": "DataFrame",
            "func": run_word_pairs,
            "format_result": lambda r: f"Top pair: ({r.collect()[0]['word1']}, {r.collect()[0]['word2']}) ({r.collect()[0]['count']})",
        },
        {
            "name": "WordPairs",
            "api": "SparkSQL",
            "func": run_word_pairs_sql,
            "format_result": lambda r: f"Top pair: ({r.collect()[0]['word1']}, {r.collect()[0]['word2']}) ({r.collect()[0]['count']})",
        },
    ]

    records = []

    for analysis in analyses:
        label = f"{analysis['name']} ({analysis['api']})"
        print(f"\nRunning {label}...")

        result, elapsed = time_function(analysis["func"], spark, corpus_path)
        summary = analysis["format_result"](result)

        print(f"  Result:  {summary}")
        print(f"  Time:    {elapsed:.3f}s")

        records.append({
            "analysis": analysis["name"],
            "api_type": analysis["api"],
            "elapsed_seconds": round(elapsed, 4),
            "corpus_path": corpus_path,
            "result_summary": summary,
        })

    # Print summary table
    print(f"\n{'='*70}")
    print(f"{'Analysis':<20} {'API':<12} {'Time (s)':<12} {'Result'}")
    print(f"{'-'*70}")
    for r in records:
        print(f"{r['analysis']:<20} {r['api_type']:<12} {r['elapsed_seconds']:<12.4f} {r['result_summary']}")
    print(f"{'='*70}")

    # Save to CSV
    results_path = os.path.join(PROJECT_ROOT, "results", "benchmark_results.csv")
    save_results(records, results_path)

    spark.stop()
    print("\nDone!")


if __name__ == "__main__":
    main()
