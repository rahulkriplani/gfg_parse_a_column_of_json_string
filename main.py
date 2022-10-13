from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

if __name__ == "__main__":

    spark = SparkSession.builder.appName('Parse a column of json strings').getOrCreate()

    df = spark.read.load('./data/movie_input.csv', header=True, format="csv")
    df.show(truncate=False)

    schema = T.StructType(
    [
        T.StructField('title', T.StringType(), True),
        T.StructField('rating', T.StringType(), True),
        T.StructField('releaseYear', T.StringType(), True),
        T.StructField('genre', T.StringType(), True)
    ]
    )
    parsed_df = df.withColumn("movie", F.from_json("movie", schema)).select(F.col('id'), F.col("movie.*"))
    parsed_df.show(truncate=False)