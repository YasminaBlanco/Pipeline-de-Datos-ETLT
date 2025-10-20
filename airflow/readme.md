# Airflow

Este módulo contiene la configuración del orquestador Apache Airflow, responsable de programar, ejecutar y monitorear los flujos de trabajo ETLT (DAGs).

## 📁 Contenido

* `dags/`: Contiene los archivos DAG de Python.
    * [spark_etl_dag.py](dags/spark_etl_dag.py): El flujo de trabajo principal que orquesta la ejecución de los scripts de Spark ETL (capa Silver y Gold) a través de un operador SSH o similar.
* `.env`: Variables de entorno específicas para Airflow (usuario/contraseña de la UI).
* `docker-compose.yaml` (Raíz): Define el servicio `airflow-webserver` y `airflow-scheduler`.

## ⚙️ Flujo de Trabajo Principal (`spark_etl_dag.py`)

Este DAG es el responsable de lanzar los trabajos pesados de transformación de datos de una capa a otra.

1.  **Verificar** la conectividad con los servicios Spark.
2.  **Ejecutar el ETL Silver:** Lanza el script `capa_silver.py` en el contenedor de Spark.
3.  **Ejecutar el ETL Gold:** Lanza el script `capa_gold.py` en el contenedor de Spark (depende del éxito del Silver).

### Dependencias y Conexiones

Este DAG requiere una conexión llamada `spark-connection` (típicamente de tipo SSH si el motor Spark está en otro contenedor o servidor) para enviar comandos de ejecución a la máquina donde reside el código Spark.

### Uso en Producción

Tras iniciar Docker Compose, accede a la UI de Airflow (`http://<IP_PÚBLICA>:8080`), busca el DAG `spark_etl_final` y actívalo. Se ejecutará automáticamente en el horario programado o puede ser lanzado manualmente.

