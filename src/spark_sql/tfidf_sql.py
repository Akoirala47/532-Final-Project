#!/usr/bin/env python3
"""
TF-IDF — Spark SQL

Mirrors the DataFrame implementation using temp views and spark.sql().
"""

import os
import sys

from pyspark.sql import SparkSession, DataFrame

from src.readers import read_wholetext_documents


def run_tfidf_sql(spark: SparkSession, input_paths) -> DataFrame:
    """Return top 20 words by max TF-IDF (natural log IDF, smoothed)."""
    raw = read_wholetext_documents(spark, input_paths)
    raw.createOrReplaceTempView("docs_raw")

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW docs AS
        SELECT input_file_name() AS doc_id, value AS text
        FROM docs_raw
        """
    )
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW words AS
        SELECT doc_id, word
        FROM docs
        LATERAL VIEW explode(split(text, ' ')) ex AS word
        WHERE word != ''
        """
    )
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW tf AS
        SELECT doc_id, word, COUNT(*) AS tf
        FROM words
        GROUP BY doc_id, word
        """
    )
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW doc_len AS
        SELECT doc_id, COUNT(*) AS doc_len
        FROM words
        GROUP BY doc_id
        """
    )
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW tf_norm AS
        SELECT t.doc_id, t.word, t.tf / d.doc_len AS tf_weight
        FROM tf t
        JOIN doc_len d ON t.doc_id = d.doc_id
        """
    )
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW word_df AS
        SELECT word, COUNT(DISTINCT doc_id) AS df
        FROM words
        GROUP BY word
        """
    )

    n_docs = spark.sql(
        "SELECT COUNT(DISTINCT doc_id) AS n FROM docs"
    ).collect()[0]["n"]

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW idf_tbl AS
        SELECT word, df, ln(({int(n_docs)} + 1) / (df + 1)) AS idf
        FROM word_df
        """
    )

    return spark.sql(
        """
        SELECT t.word, MAX(t.tf_weight * i.idf) AS max_tfidf
        FROM tf_norm t
        JOIN idf_tbl i ON t.word = i.word
        GROUP BY t.word
        ORDER BY max_tfidf DESC
        LIMIT 20
        """
    )


def main():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    default = os.path.join(root, "data", "corpus", "*.txt")
    input_path = sys.argv[1] if len(sys.argv) > 1 else default

    spark = SparkSession.builder.appName("TFIDF_SQL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    top_df = run_tfidf_sql(spark, input_path)
    print("\nTop 20 terms by max TF-IDF (Spark SQL):")
    top_df.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
