
#Script para crear nueva tabla y escribir datos sobre ella ( ya escribe datos pero se toma Datos dummy se debe verificar problemas con permisos de almacenamiento)



import os
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery import SourceFormat

# Variable entorno para utenticación
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "aa-study-7975facd84cd.json"


project_id = "aa-study"
dataset_id = "dpineda_poc"


client = bigquery.Client(project=project_id)
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table('stg_dummy')

# Definir configuración de trabajo 
job_config = LoadJobConfig()
job_config.source_format = SourceFormat.AVRO

# Crear la tabla 
table = bigquery.Table(table_ref)
table = client.create_table(table)

# Carlga los datos desde GCP 
uri = "gs://cloud-samples-data/bigquery/us-states/us-states.avro" # reemplazar por  "gs://dpineda-poc/destiny/fire_incidents2.avro" cuando se arreglen los permisos
load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)

print(f"Starting job {load_job.job_id}")
load_job.result()  
print(f"Job {load_job.job_id} completed")

# Verificar cargue de datos 
table = client.get_table(table)
print(f"Loaded {table.num_rows} rows to {table.table_id}")