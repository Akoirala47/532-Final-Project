from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, explode

spark = SparkSession.builder.appName("WordCount").getOrCreate() #creating session

df = spark.read.text("hamlet.txt") #reading text

#split the col named values by spaces into an array
#make a new df with rows called word where each row is a word from the arrays
#ignore blank words
words_df = df.select(explode(split(col("value"), " ")).alias("words")
).filter(
    col("words") != ""
)

print("Total words: ", words_df.count()) #print number of rows

spark.stop() #stopping session