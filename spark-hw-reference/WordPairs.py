from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, posexplode, desc, trim, monotonically_increasing_id

#create session
spark = SparkSession.builder.appName("WordPairs").getOrCreate() 

#read file and give them increasing id
df = spark.read.text("hamlet.txt").withColumn("line_id", monotonically_increasing_id())

words_df = df.select(
    "line_id",
    split(trim(col("value")), " ").alias("words"),
)

#making two dataframe to combine later
left_df = words_df.select("line_id", posexplode(col("words")).alias("pos1", "word1"))
right_df = words_df.select("line_id", posexplode(col("words")).alias("pos2", "word2"))

#join words from the same line to form pairs, keep unique pairs (pos1 <= pos2), filter out blanks
#order by descending count and show top 20
left_df.join(right_df, "line_id") \
.filter(col("pos1") <= col("pos2")).filter(col("word1") != "").filter(col("word2") != "") \
.groupBy("word1", "word2").count().orderBy(desc("count")).show(20, truncate=False)

spark.stop()