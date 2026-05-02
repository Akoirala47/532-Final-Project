#!/usr/bin/env python3
"""
TF-IDF — DataFrame API

Document = one Project Gutenberg text file (whole-file read). Ranks terms by
max TF-IDF across documents (good discriminative terms for this corpus).
"""

import os
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    countDistinct,
    desc,
    explode,
    input_file_name,
    lit,
    log,
    max as max_,
    split,
)
from pyspark.sql.functions import count as spark_count

from src.readers import read_wholetext_documents


def run_tfidf(spark: SparkSession, input_paths) -> DataFrame:
    """Return top 20 words by max TF-IDF (natural log IDF, smoothed)."""
    docs = read_wholetext_documents(spark, input_paths).select(
        input_file_name().alias("doc_id"),
        col("value").alias("text"),
    )

    words = docs.select(
        "doc_id",
        explode(split(col("text"), " ")).alias("word"),
    ).filter(col("word") != "")

    tf = words.groupBy("doc_id", "word").agg(spark_count("*").alias("tf"))
    doc_len = words.groupBy("doc_id").agg(spark_count("*").alias("doc_len"))
    tf_norm = tf.join(doc_len, "doc_id").withColumn(
        "tf_weight", col("tf") / col("doc_len")
    )

    word_df = words.groupBy("word").agg(countDistinct("doc_id").alias("df"))
    n_docs = docs.select(countDistinct("doc_id").alias("n")).collect()[0]["n"]
    idf_tbl = word_df.withColumn(
        "idf",
        log(lit(n_docs + 1) / (col("df") + lit(1))),
    )

    scored = tf_norm.join(idf_tbl, "word").withColumn(
        "tfidf", col("tf_weight") * col("idf")
    )

    return (
        scored.groupBy("word")
        .agg(max_("tfidf").alias("max_tfidf"))
        .orderBy(desc("max_tfidf"))
        .limit(20)
    )


def main():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    default = os.path.join(root, "data", "corpus", "*.txt")
    input_path = sys.argv[1] if len(sys.argv) > 1 else default

    spark = SparkSession.builder.appName("TFIDF_DataFrame").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    top_df = run_tfidf(spark, input_path)
    print("\nTop 20 terms by max TF-IDF (DataFrame API):")
    top_df.show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
