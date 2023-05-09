
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery import TimePartitioning
from google.cloud.bigquery._pandas_helpers import dataframe_to_bq_schema


# Configurar credenciales de BigQuery
client = bigquery.Client.from_service_account_json("aa-study-7975facd84cd.json")

spark = SparkSession.builder \
    .appName("fire_incidents_2") \
    .config('spark.jars.packages', 'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.1.4') \
    .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
    .config('spark.hadoop.google.cloud.auth.service.account.enable', 'true') \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.0") \
    .config('spark.hadoop.google.cloud.auth.service.account.json.keyfile', 'aa-study-7975facd84cd.json') \
    .getOrCreate()
    
    
    
    
    
# Leer datos desde el archivo avro en GCS
df = spark.read.format("avro").load("gs://dpineda-poc/destiny/fire_incidents.avro")

# Convertir el DataFrame de PySpark en un DataFrame de Pandas
pandas_df = df.toPandas()

# Generar el esquema de BigQuery a partir del DataFrame de Pandas
schema = _pandas_helpers.dataframe_to_bq_schema(pandas_df)

# Enviar datos a BigQuery
dataset_id = 'dpineda_poc'
table_id = 'stg_fire_incidents'
table_ref = client.dataset(dataset_id).table(table_id)

job_config = bigquery.LoadJobConfig(
    autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=schema
)

job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
job.result()

print(f"Los datos se han cargado en la tabla {table_id} en BigQuery.")    

spark.stop()   