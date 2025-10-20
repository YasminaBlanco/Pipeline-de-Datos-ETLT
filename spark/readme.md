# Módulo Spark

Este módulo contiene el núcleo de la lógica ETL (Extract, Transform, Load) y el entorno de procesamiento distribuido (Apache Spark). Es donde los datos se limpian, enriquecen y transforman de la Capa RAW a las capas Silver y Gold.

## 📁 Contenido

* [capa_silver.py](capa_silver.py): Script ETL para transformar datos **RAW** (JSON/Parquet crudos) en la capa **Silver** (datos limpios, aplanados y estandarizados en S3).
* [capa_gold.py](capa_gold.py): Script ETL para transformar datos **Silver** en la capa **Gold** (datos agregados, enriquecidos con índices de potencial energético y listos para el análisis de negocio).
* [Dockerfile](Dockerfile): Define el entorno del contenedor Spark, incluyendo Python, PySpark, y todas las dependencias necesarias.
* [docker-compose.yaml](docker-compose.yaml)
* `.env`: Variables de entorno para Spark (ej: memoria, núcleos, credenciales S3).
* [reporte.ipynb](reporte.ipynb): Script de Jupyter Notebook para generar reportes de resultados.

## ⚙️ Flujo ETL

### 1. Capa Silver (`capa_silver.py`)

**Función:** Limpieza y Estandarización.
* Lee datos de múltiples fuentes y formatos (ej: JSON histórico, Parquet reciente) desde S3 (Bucket RAW).
* Desanida las estructuras complejas (e.g., `main`, `weather`, `coord`).
* Aplica transformaciones de tipo de dato y validación de calidad.
* Escribe los resultados en S3 (Bucket SILVER) en formato Parquet particionado.

### 2. Capa Gold (`capa_gold.py`)

**Función:** Agregación y Análisis.
* Lee los datos limpios de S3 (Bucket SILVER).
* **Enriquecimiento:** Calcula índices de potencial energético (WPI y SPI).
* **Agregación:** Crea dos tablas analíticas principales:
    1.  **Resumen Diario:** Promedios y extremos por día (clave para reporting).
    2.  **Patrones Horarios/Mensuales:** Patrones de potencial energético a lo largo del tiempo (clave para la optimización de la red).
* Escribe los resultados en S3 (Bucket GOLD) en formato Parquet particionado.

## Ejecución de Pruebas

Estos scripts son ejecutados por Airflow, pero puedes probarlos directamente en el contenedor de Spark:

1.  **Ejecutar ETL Silver:**
    ```bash
    docker-compose exec spark-server python3 /app/capa_silver.py
    ```

2.  **Ejecutar ETL Gold:**
    ```bash
    docker-compose exec spark-server python3 /app/capa_gold.py