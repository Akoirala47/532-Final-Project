#!/usr/bin/env python3
"""
Word Pairs — Spark SQL

Finds the top 20 most frequent word pairs (on the same line) using pure Spark SQL.
"""

import sys
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import monotonically_increasing_id


def run_word_pairs_sql(spark: SparkSession, input_path: str) -> DataFrame:
    """Run word pairs via Spark SQL and return the top-20 DataFrame."""
    df = spark.read.text(input_path).withColumn(
        "line_id", monotonically_increasing_id()
    )
    df.createOrReplaceTempView("corpus_lines")

    # Step 1: Create a view with words and their positions
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW words_with_pos AS
        SELECT line_id, pos, word
        FROM corpus_lines
        LATERAL VIEW posexplode(split(trim(value), ' ')) t AS pos, word
    """)

    # Step 2: Self-join to form pairs, filter, aggregate, and sort
    pairs_df = spark.sql("""
        SELECT w1.word AS word1, w2.word AS word2, COUNT(*) AS count
        FROM words_with_pos w1
        JOIN words_with_pos w2 ON w1.line_id = w2.line_id
        WHERE w1.pos <= w2.pos
          AND w1.word != ''
          AND w2.word != ''
        GROUP BY w1.word, w2.word
        ORDER BY count DESC
        LIMIT 20
    """)

    return pairs_df


def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(__file__), "..", "..", "data", "corpus", "*.txt"
    )

    spark = SparkSession.builder.appName("WordPairs_SQL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    pairs_df = run_word_pairs_sql(spark, input_path)

    print(f"\n{'='*50}")
    print("Top 20 Word Pairs (Spark SQL):")
    print(f"{'='*50}")
    pairs_df.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
