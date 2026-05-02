#!/usr/bin/env python3
"""
Word Count — DataFrame API

Counts the total number of words across all text files in the corpus.
Adapted from spark-hw-reference/WordCount.py to work on the full corpus.
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, explode

from src.readers import read_lines


def run_word_count(spark: SparkSession, input_path) -> int:
    """Run word count and return the total number of words."""
    df = read_lines(spark, input_path)

    words_df = df.select(
        explode(split(col("value"), " ")).alias("words")
    ).filter(
        col("words") != ""
    )

    total = words_df.count()
    return total


def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "..", "..", "data", "corpus", "*.txt"
    )

    spark = SparkSession.builder.appName("WordCount_DataFrame").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    total = run_word_count(spark, input_path)
    print(f"\n{'='*50}")
    print(f"Total words (DataFrame API): {total:,}")
    print(f"{'='*50}\n")

    spark.stop()


if __name__ == "__main__":
    main()
