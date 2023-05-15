
#Script para crear tabla en BigQuery y dale esquema a la tabla basado en los datos cargados 

from pyspark.sql import SparkSession
from pyspark import SparkFiles
from google.cloud import bigquery




import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/app/aa-study-7975facd84cd.json"

# Configuración de la sesión de Spark
spark = SparkSession.builder \
    .appName("fire_incidents2") \
    .config('spark.jars.packages', 'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.1.4') \
    .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
    .config('spark.hadoop.google.cloud.auth.service.account.enable', 'true') \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.0") \
    .config('spark.hadoop.google.cloud.auth.service.account.json.keyfile', 'aa-study-7975facd84cd.json') \
    .getOrCreate()

# Carga de datos desde GCS a un DataFrame de Spark
spark.sparkContext.addFile("gs://dpineda-poc/destiny/fire_incidents2.avro", recursive=True)
df = spark.read.format("avro").load(SparkFiles.get("fire_incidents2.avro"))

# Configuración de la tabla de BigQuery
table_id = "dpineda_poc.stg_fire_incidents2"
project_id = "aa-study"
dataset_id = "dpineda_poc"

# Crea la tabla si no existe y dare un esquema
   
bigquery_client = bigquery.Client(project='aa-study')
dataset_ref = bigquery_client.dataset('dpineda_poc')

dataset_ref.location = 'US'  

table_ref = dataset_ref.table('stg_fire_incidents2')
schema = [
    bigquery.SchemaField('incident_number', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('exposure_number', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('latitude', 'FLOAT'),
    bigquery.SchemaField('longitude', 'FLOAT'),
    bigquery.SchemaField('incident_date', 'TIMESTAMP'),
    bigquery.SchemaField('fire_district', 'STRING'),
    bigquery.SchemaField('incident_type', 'STRING'),
    bigquery.SchemaField('property_use', 'STRING'),
    bigquery.SchemaField('street_address', 'STRING'),
    bigquery.SchemaField('city', 'STRING'),
    bigquery.SchemaField('state', 'STRING'),
    bigquery.SchemaField('zip', 'STRING'),
    bigquery.SchemaField('year', 'INTEGER'),
    bigquery.SchemaField('month', 'INTEGER'),
    bigquery.SchemaField('day', 'INTEGER')
]
table = bigquery.Table(table_ref, schema=schema)
table = bigquery_client.create_table(table)   
    
    

print("la tabla de ha creado correctamente stg_fire_incidents2 en BigQuery.")

spark.stop()