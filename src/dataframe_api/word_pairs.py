#!/usr/bin/env python3
"""
Word Pairs — DataFrame API

Finds the top 20 most frequent word pairs (on the same line) across the corpus.
Adapted from spark-hw-reference/WordPairs.py to work on the full corpus.
"""

import sys
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    split, col, posexplode, desc, trim, monotonically_increasing_id,
)

from src.readers import read_lines


def run_word_pairs(spark: SparkSession, input_path) -> DataFrame:
    """Run word pairs analysis and return the top-20 DataFrame."""
    df = read_lines(spark, input_path).withColumn(
        "line_id", monotonically_increasing_id()
    )

    words_df = df.select(
        "line_id",
        split(trim(col("value")), " ").alias("words"),
    )

    left_df = words_df.select(
        "line_id", posexplode(col("words")).alias("pos1", "word1")
    )
    right_df = words_df.select(
        "line_id", posexplode(col("words")).alias("pos2", "word2")
    )

    pairs_df = (
        left_df.join(right_df, "line_id")
        .filter(col("pos1") <= col("pos2"))
        .filter(col("word1") != "")
        .filter(col("word2") != "")
        .groupBy("word1", "word2")
        .count()
        .orderBy(desc("count"))
        .limit(20)
    )

    return pairs_df


def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "..", "..", "data", "corpus", "*.txt"
    )

    spark = SparkSession.builder.appName("WordPairs_DataFrame").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    pairs_df = run_word_pairs(spark, input_path)

    print(f"\n{'='*50}")
    print("Top 20 Word Pairs (DataFrame API):")
    print(f"{'='*50}")
    pairs_df.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
