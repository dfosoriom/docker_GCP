
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from google.cloud import bigquery


spark = SparkSession.builder \
    .appName("fire_incidents") \
    .config('spark.jars.packages', 'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.1.4') \
    .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
    .config('spark.hadoop.google.cloud.auth.service.account.enable', 'true') \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.0") \
    .config('spark.hadoop.google.cloud.auth.service.account.json.keyfile', 'aa-study-7975facd84cd.json') \
    .getOrCreate()
    

data_uri = "https://data.sfgov.org/api/views/wr8u-xric/rows.csv?accessType=DOWNLOAD"
spark.sparkContext.addFile(data_uri)
df = spark.read.csv(SparkFiles.get("rows.csv"), header=True, inferSchema= True)

#renombrar columnas por que avro no lo lee 
def rename_columns(df):
    
    columns = df.columns
    
    
    for column_name in columns:
        if ' ' in column_name:
            new_column_name = column_name.replace(' ', '_')
            df = df.withColumnRenamed(column_name, new_column_name)
    
    
    return df


df = rename_columns(df)
# escribir en el destino 
df.write.format("avro") \
    .save("gs://dpineda-poc/destiny/fire_incidents2.avro")
    
    
    
spark.stop()

