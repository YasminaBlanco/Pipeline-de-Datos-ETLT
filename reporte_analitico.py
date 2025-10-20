import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, avg, max, min, 
    date_format, dayofmonth, month, year, 
    pow, coalesce, round, concat_ws, sum,
    current_timestamp, to_date
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# --- 1. CONFIGURACIÓN Y MANEJO DE VARIABLES DE ENTORNO ---

def get_required_env(var_name):
    """Obtiene la variable de entorno o termina la ejecución si no se encuentra."""
    value = os.environ.get(var_name)
    if not value:
        print(f"FATAL ERROR: La variable de entorno requerida '{var_name}' no está configurada o es nula.", file=sys.stderr)
        sys.exit(1) 
    return value

try:
    # Obtener variables de entorno
    BUCKET_NAME_GOLD = get_required_env("BUCKET_NAME_GOLD")
    AWS_ACCESS_KEY_ID = get_required_env("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = get_required_env("AWS_SECRET_ACCESS_KEY")
    
except Exception:
    sys.exit(1)

# Rutas de entrada (capa GOLD)
INPUT_PATH_DAILY = f"s3a://{BUCKET_NAME_GOLD}/gold/resumen_clima_diario/"
INPUT_PATH_PATTERNS = f"s3a://{BUCKET_NAME_GOLD}/gold/patrones_horarios/"

print(f"--- INICIO GENERACIÓN DE REPORTE ANALÍTICO (Capa GOLD) ---")
print(f"✅ Leyendo datos analíticos desde GOLD: {INPUT_PATH_DAILY} y {INPUT_PATH_PATTERNS}")

# --- 2. CONFIGURACIÓN DE LA SESIÓN SPARK ---

spark = (
    SparkSession.builder.appName("GoldReportGeneration")
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Función para leer datos y manejar el vacío
def safe_read_parquet(path):
    try:
        df = spark.read.parquet(path)
        if df.count() == 0:
            print(f"❌ ERROR: La tabla en '{path}' está vacía.", file=sys.stderr)
            sys.exit(1)
        return df
    except Exception as e:
        print(f"❌ ERROR: No se pudo leer la tabla en '{path}'. Detalle: {e}", file=sys.stderr)
        sys.exit(1)

# --- 3. LECTURA DE DATOS GOLD ---

try:
    df_daily = safe_read_parquet(INPUT_PATH_DAILY)
    df_patterns = safe_read_parquet(INPUT_PATH_PATTERNS)

    print(f"Total de registros diarios leídos: {df_daily.count()}")
    print(f"Total de registros de patrones leídos: {df_patterns.count()}")

    # --- 4. RESPUESTAS CLAVE DE ANALÍTICA ---

    # 4.1. Pregunta 1: Ciudad y Día con Mayor y Menor Potencial Eólico y Solar
    
    print("\n--- 4.1. Días de Mayor y Menor Potencial General (Eólico y Solar) ---")
    
    # Mayor Potencial Eólico
    max_eolico = df_daily.orderBy(col("max_potencial_eolico_diario").desc()).limit(1).select("ciudad", "fecha_solo", col("max_potencial_eolico_diario").alias("Potencial_Eolico_Maximo")).collect()[0]
    
    # Mayor Potencial Solar
    max_solar = df_daily.orderBy(col("max_potencial_solar_diario").desc()).limit(1).select("ciudad", "fecha_solo", col("max_potencial_solar_diario").alias("Potencial_Solar_Maximo")).collect()[0]

    # Menor Potencial Eólico (Promedio diario)
    min_eolico_prom = df_daily.orderBy(col("promedio_potencial_eolico_diario").asc()).limit(1).select("ciudad", "fecha_solo", col("promedio_potencial_eolico_diario").alias("Potencial_Eolico_Minimo_Prom")).collect()[0]

    # Menor Potencial Solar (Promedio diario)
    min_solar_prom = df_daily.orderBy(col("promedio_potencial_solar_diario").asc()).limit(1).select("ciudad", "fecha_solo", col("promedio_potencial_solar_diario").alias("Potencial_Solar_Minimo_Prom")).collect()[0]

    # 4.2. Preguntas 2 y 3: Patrones Horarios (Variación del Potencial Eólico y Solar)
    
    print("\n--- 4.2. Patrones Horarios de Potencial (Hora pico) ---")
    
    # Hora promedio MÁXIMA de Potencial Eólico
    patron_eolico_max = df_patterns.groupBy("hora_solo").agg(
        round(avg("promedio_eolico_por_hora_mes"), 2).alias("promedio_global_eolico")
    ).orderBy(col("promedio_global_eolico").desc()).limit(1).collect()[0]
    
    # Hora promedio MÁXIMA de Potencial Solar
    patron_solar_max = df_patterns.groupBy("hora_solo").agg(
        round(avg("promedio_solar_por_hora_mes"), 2).alias("promedio_global_solar")
    ).orderBy(col("promedio_global_solar").desc()).limit(1).collect()[0]

    
    # 4.3. Pregunta 4: Correlación Lluvia / Nubes con Potencial Solar
    
    print("\n--- 4.3. Análisis de Correlación: Días de Alta Lluvia vs. Potencial Solar ---")
    
    # Identificar el día con mayor precipitación
    dia_mas_lluvioso = df_daily.orderBy(col("total_precipitacion_diaria_mm").desc()).limit(1).collect()[0]
    
    # Obtener el potencial solar promedio de ese día
    potencial_solar_en_lluvia = dia_mas_lluvioso["promedio_potencial_solar_diario"]
    lluvia_total = dia_mas_lluvioso["total_precipitacion_diaria_mm"]
    ciudad_lluvia = dia_mas_lluvioso["ciudad"]
    fecha_lluvia = dia_mas_lluvioso["fecha_solo"]


    # 4.4. Pregunta 5, 6, 7: Condiciones Extremas y Promedios (Riohacha y Patagonia)
    
    print("\n--- 4.4. Resumen de Condiciones Climáticas Extremas ---")
    
    # Ciudad más caliente (Máx Temp Absoluta)
    ciudad_max_temp = df_daily.orderBy(col("max_temperatura_registro_c").desc()).limit(1).select("ciudad", "fecha_solo", col("max_temperatura_registro_c").alias("Temperatura_Absoluta_Max")).collect()[0]
    
    # Ciudad más fría (Mín Temp Absoluta)
    ciudad_min_temp = df_daily.orderBy(col("min_temperatura_registro_c").asc()).limit(1).select("ciudad", "fecha_solo", col("min_temperatura_registro_c").alias("Temperatura_Absoluta_Min")).collect()[0]

    # Ciudad con mayor promedio de humedad
    ciudad_max_humedad = df_daily.groupBy("ciudad").agg(
        round(avg("promedio_humedad_diaria"), 2).alias("promedio_humedad_general")
    ).orderBy(col("promedio_humedad_general").desc()).limit(1).collect()[0]


    # --- 5. GENERACIÓN DEL REPORTE FINAL ---
    
    reporte = f"""
# Reporte Analítico de Potencial Energético

## 1. Resumen de Potenciales Extremos (Tabla 'resumen_clima_diario')

Este análisis identifica los días con el potencial energético más alto y más bajo, crucial para la planificación de infraestructura.

| Potencial | Ciudad | Fecha | Valor Máximo/Mínimo (Índice) |
| :--- | :--- | :--- | :--- |
| **Eólico Máximo** | {max_eolico["ciudad"]} | {max_eolico["fecha_solo"]} | **{max_eolico["Potencial_Eolico_Maximo"]}** |
| **Solar Máximo** | {max_solar["ciudad"]} | {max_solar["fecha_solo"]} | **{max_solar["Potencial_Solar_Maximo"]}** |
| Eólico Mínimo (Promedio) | {min_eolico_prom["ciudad"]} | {min_eolico_prom["fecha_solo"]} | {min_eolico_prom["Potencial_Eolico_Minimo_Prom"]} |
| Solar Mínimo (Promedio) | {min_solar_prom["ciudad"]} | {min_solar_prom["fecha_solo"]} | {min_solar_prom["Potencial_Solar_Minimo_Prom"]} |

## 2. Patrones de Generación Horaria (Tabla 'patrones_horarios')

El análisis de los patrones horarios revela las ventanas óptimas de generación para ambas fuentes, promediadas a través del periodo de estudio.

* **Pregunta 2 (Eólico):** La hora promedio con el **mayor potencial eólico** a nivel general es la hora **{patron_eolico_max["hora_solo"]}:00 (Promedio: {patron_eolico_max["promedio_global_eolico"]} WPI)**. Esto indica las horas pico de viento para la generación.

* **Pregunta 3 (Solar):** La hora promedio con el **mayor potencial solar** a nivel general es la hora **{patron_solar_max["hora_solo"]}:00 (Promedio: {patron_solar_max["promedio_global_solar"]} SPI)**. Esta es la franja óptima de radiación solar.

## 3. Correlaciones Climáticas y Extremos

Estos datos ayudan a evaluar la resiliencia de la inversión frente a condiciones meteorológicas adversas y extremas.

* **Pregunta 4 (Impacto de la Lluvia/Nubes):**
    * El día con la mayor precipitación registrada fue el **{fecha_lluvia}** en **{ciudad_lluvia}**, con un total de **{lluvia_total} mm** de lluvia.
    * En ese mismo día, el potencial solar promedio fue de **{potencial_solar_en_lluvia} SPI**. Este valor (junto con el promedio general) se usa para cuantificar directamente la atenuación del potencial solar debido a la cobertura de nubes y la lluvia.

* **Pregunta 5 (Condición más Caliente):** La ciudad que registró la **temperatura máxima absoluta** en el periodo fue **{ciudad_max_temp["ciudad"]}** el **{ciudad_max_temp["fecha_solo"]}** con **{ciudad_max_temp["Temperatura_Absoluta_Max"]} °C**.

* **Pregunta 6 (Condición más Fría):** La ciudad que registró la **temperatura mínima absoluta** en el periodo fue **{ciudad_min_temp["ciudad"]}** el **{ciudad_min_temp["fecha_solo"]}** con **{ciudad_min_temp["Temperatura_Absoluta_Min"]} °C**.

* **Pregunta 7 (Mayor Humedad):** La ciudad con el **promedio de humedad diaria más alto** durante el periodo de estudio es **{ciudad_max_humedad["ciudad"]}** con un promedio de **{ciudad_max_humedad["promedio_humedad_general"]}%**.

## Conclusiones Clave
La ciudad de **{max_solar["ciudad"]}** muestra la mayor ventana de oportunidad solar y la ciudad de **{max_eolico["ciudad"]}** la mayor de potencial eólico. Los patrones horarios confirman que la generación es altamente dependiente de las horas centrales del día, lo que requiere considerar el almacenamiento de energía para maximizar la eficiencia fuera de las horas pico.

"""
    print("\n" + "="*80)
    print("REPORTE ANALÍTICO GENERADO:")
    print("="*80)
    print(reporte)
    print("="*80)
    
except Exception as e:
    print(f"❌ ERROR: Ocurrió un error grave durante la generación del reporte. Detalle: {e}", file=sys.stderr)
    sys.exit(1)

finally:
    spark.stop()
    print("Sesión Spark detenida.")