#!/usr/bin/env python3
"""
Word Count — Spark SQL

Counts the total number of words across the corpus using pure Spark SQL.
"""

import sys
import os
from pyspark.sql import SparkSession

from src.readers import read_lines


def run_word_count_sql(spark: SparkSession, input_path) -> int:
    """Run word count via Spark SQL and return the total number of words."""
    df = read_lines(spark, input_path)
    df.createOrReplaceTempView("corpus")

    result = spark.sql("""
        SELECT COUNT(*) AS total_words
        FROM (
            SELECT explode(split(value, ' ')) AS word
            FROM corpus
        ) t
        WHERE word != ''
    """)

    total = result.collect()[0]["total_words"]
    return total


def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "..", "..", "data", "corpus", "*.txt"
    )

    spark = SparkSession.builder.appName("WordCount_SQL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    total = run_word_count_sql(spark, input_path)
    print(f"\n{'='*50}")
    print(f"Total words (Spark SQL): {total:,}")
    print(f"{'='*50}\n")

    spark.stop()


if __name__ == "__main__":
    main()
