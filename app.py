from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read GCS File") \
    .config('spark.jars.packages', 'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.1.4') \
    .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
    .config('spark.hadoop.google.cloud.auth.service.account.enable', 'true') \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.0") \
    .config('spark.hadoop.google.cloud.auth.service.account.json.keyfile', 'aa-study-9a5f15fde139.json') \
    .getOrCreate()
    
    
# Leer el archivo CSV desde Cloud Storage
df = spark.read.format("csv") \
    .option("header", "true") \
    .load("gs://dpineda-poc/source/iris.csv")
    
df = df.withColumnRenamed("sepal.length", "sepal_length")
df = df.withColumnRenamed("sepal.width", "sepal_width")
df = df.withColumnRenamed("petal.length", "petal_length")   
df = df.withColumnRenamed("petal.width", "petal_width")   

df.write.format("avro") \
    .save("gs://dpineda-poc/destiny/iris.avro")


spark.stop