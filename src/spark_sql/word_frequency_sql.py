#!/usr/bin/env python3
"""
Word Frequency — Spark SQL

Finds the top 20 most frequent words across the corpus using pure Spark SQL.
"""

import sys
import os
from pyspark.sql import SparkSession, DataFrame


def run_word_frequency_sql(spark: SparkSession, input_path: str) -> DataFrame:
    """Run word frequency via Spark SQL and return the top-20 DataFrame."""
    df = spark.read.text(input_path)
    df.createOrReplaceTempView("corpus")

    freq_df = spark.sql("""
        SELECT word, COUNT(*) AS count
        FROM (
            SELECT explode(split(value, ' ')) AS word
            FROM corpus
        ) t
        WHERE word != ''
        GROUP BY word
        ORDER BY count DESC
        LIMIT 20
    """)

    return freq_df


def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "..", "..", "data", "corpus", "*.txt"
    )

    spark = SparkSession.builder.appName("WordFrequency_SQL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    freq_df = run_word_frequency_sql(spark, input_path)

    print(f"\n{'='*50}")
    print("Top 20 Words (Spark SQL):")
    print(f"{'='*50}")
    freq_df.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
