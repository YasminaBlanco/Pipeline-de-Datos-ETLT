from pyspark.sql import SparkSession
# Se añade from_utc_timestamp y se deja date_format, col y from_json
from pyspark.sql.functions import from_json, col, date_format, from_utc_timestamp 
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, StructField
import datetime
import os
import sys

# ------------------------------
# Configuración de Spark
# ------------------------------
# NOTA: Las dependencias de Kafka y Hadoop-AWS se pasan a través del comando spark-submit
spark = (
    SparkSession.builder
    .appName("OpenWeatherConsumerRAW")
    # Configuración del sistema de archivos S3A (requiere las JARs de Hadoop-AWS)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Usa DefaultAWSCredentialsProviderChain para leer credenciales desde variables de entorno de AWS
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .getOrCreate()
)

# Aumentar la verbosidad de los logs para ayudar en el diagnóstico inicial
spark.sparkContext.setLogLevel("WARN")
print(f"--- Spark Session creada. App Name: {spark.sparkContext.appName} ---")

# ------------------------------
# Esquema JSON de Kafka
# ------------------------------
# Importante: El esquema DEBE coincidir con la estructura del JSON enviado por producer.py
schema = StructType([
    StructField("source", StringType()),
    StructField("city", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("ts", LongType()),
    StructField("datetime_utc", StringType()), # Formato ISO 8601 string (e.g., "2025-10-19T14:45:00Z")
    StructField("temp_c", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("wind_speed", DoubleType())
])


# ------------------------------
# Leer datos desde Kafka
# ------------------------------
BROKER = "172.31.38.86:9092"
TOPIC = "openweather_topic" # CORREGIDO a guion bajo

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
.load()

# Parsear JSON, seleccionar columnas y crear particiones
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") 

# --- LÓGICA DE PARTICIONADO DIARIO ---
# 1. Convierte el string datetime_utc a un tipo timestamp. 
# 2. Usa from_utc_timestamp para interpretar el string correctamente como UTC.
# 3. Usa date_format para extraer el año, mes y día para el particionado.
df_partitioned = df_parsed \
    .withColumn(
        "ts_utc", 
        from_utc_timestamp(col("datetime_utc"), "UTC")
    ) \
    .withColumn("year", date_format(col("ts_utc"), "yyyy")) \
    .withColumn("month", date_format(col("ts_utc"), "MM")) \
    .withColumn("day", date_format(col("ts_utc"), "dd")) \
    .drop("ts_utc") # La columna temporal ya no es necesaria

# ----------------------------------------------------
# CORRECCIÓN DE ARCHIVOS PEQUEÑOS: COALESCE(1)
# Esto asegura que cada micro-batch (cada 24 horas) 
# escriba UN SOLO archivo.
# ----------------------------------------------------
df_coalesced = df_partitioned.coalesce(1)


# ------------------------------
# Escritura en S3 raw
# ------------------------------
# NOTA: Asegúrate de que este bucket y path existan y tengas permisos.
output_path = "s3a://infraestructura-datos-raw/raw/openweather-api/PatagoniaopenWeather/"

# Definir el DataStreamWriter
# Usamos df_coalesced en lugar de df_partitioned
writer = df_coalesced.writeStream \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", output_path + "_checkpoints/") \
    .partitionBy("year", "month", "day") \
    .outputMode("append") # append es la única opción válida para sink basados en archivos sin agregación

# Iniciar la consulta de manera continua cada 86400 segundos (24 horas)
query = writer.trigger(processingTime="86400 seconds").start()

print(f"--- Spark Structured Streaming iniciado. Escribiendo en S3: {output_path} ---")

# Esperar la terminación de la consulta (hasta que se detenga manualmente)
query.awaitTermination()