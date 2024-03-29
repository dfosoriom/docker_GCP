
FROM python:3.9

RUN apt-get update && apt-get install -y openjdk-11-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:${SPARK_HOME}/bin

# Descargar e instalar Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz && \
    tar -xzf spark-3.2.0-bin-hadoop3.2.tgz && \
    mv spark-3.2.0-bin-hadoop3.2 ${SPARK_HOME} && \
    rm spark-3.2.0-bin-hadoop3.2.tgz

# Descargar e instalar las bibliotecas de GCS 
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.0/hadoop-auth-3.3.0.jar -P ${SPARK_HOME}/jars/ && \
    wget https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.1.4/gcs-connector-hadoop3-2.1.4-shaded.jar -P ${SPARK_HOME}/jars/

# Instalar las bibliotecas de Python necesarias
RUN pip3 install pyspark==3.2.0 google-cloud-storage avro==1.10.2 google-cloud-bigquery pyarrow pandas==1.4.0



WORKDIR /app
COPY app.py .
COPY app2.py .
COPY app3.py .

COPY aa-study-7975facd84cd.json /app/aa-study-7975facd84cd.json

CMD ["app.py","app2.py","app3.py"]




