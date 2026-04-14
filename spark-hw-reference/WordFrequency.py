from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, explode, desc

spark = SparkSession.builder.appName("WordFrequency").getOrCreate() #creating session

df = spark.read.text("hamlet.txt") #reading text

#split the col named values by spaces into an array
#make a new df with rows called word where each row is a word from the arrays
#ignore blank words
words_df = df.select(explode(split(col("value"), " ")).alias("words")
).filter(
    col("words") != ""
)

freq_df = words_df.groupBy("words").count() #make a new df grouped by the frequency of the words

freq_df.orderBy(desc("count")).limit(20).show() #sort by descending count and show the top 20 res

spark.stop()