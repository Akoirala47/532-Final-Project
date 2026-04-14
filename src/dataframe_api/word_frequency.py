#!/usr/bin/env python3
"""
Word Frequency — DataFrame API

Finds the top 20 most frequent words across the corpus.
Adapted from spark-hw-reference/WordFrequency.py to work on the full corpus.
"""

import sys
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import split, col, explode, desc


def run_word_frequency(spark: SparkSession, input_path: str) -> DataFrame:
    """Run word frequency and return the top-20 DataFrame."""
    df = spark.read.text(input_path)

    words_df = df.select(
        explode(split(col("value"), " ")).alias("words")
    ).filter(
        col("words") != ""
    )

    freq_df = words_df.groupBy("words").count().orderBy(desc("count")).limit(20)
    return freq_df


def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "..", "..", "data", "corpus", "*.txt"
    )

    spark = SparkSession.builder.appName("WordFrequency_DataFrame").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    freq_df = run_word_frequency(spark, input_path)

    print(f"\n{'='*50}")
    print("Top 20 Words (DataFrame API):")
    print(f"{'='*50}")
    freq_df.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
