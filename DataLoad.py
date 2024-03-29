from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, TimestampType

spark=SparkSession \
    .builder \
    .appName("data_to_mongodb") \
    .master("local") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/") \
    .config("spark.executor.memory", "6g") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()
schema = StructType([
    StructField("asin", StringType(), True),
    StructField("overall", DoubleType(), True),
    StructField("reviewText", StringType(), True),
    StructField("reviewerID", StringType(), True),
    StructField("reviewerName", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("unixReviewTime", DoubleType(), True),
    StructField("verified", BooleanType(), True)
])
dataframe = spark.read.format("json").schema(schema).load("D:\All_Amazon_Review_Folder\All_Amazon_Review.json")

dataframe.select("asin", "reviewerID", "reviewerName", "reviewText", "summary", "overall", "unixReviewTime","verified").write \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", "mongodb://localhost:27017/amazon.data") \
    .option("partitioner", "MongoSinglePartitioner") \
    .option("partitionkey", "asin") \
    .save()
    
spark.stop()
