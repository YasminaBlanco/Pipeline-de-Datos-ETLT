from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

# --- CONFIGURACIÓN CLAVE ---
# ID de la conexión SSH que ya configuraste
# CAMBIADO: Debe coincidir con el ID que usaste en la UI de Airflow.
SSH_CONN_ID = "spark-connection"

# La ruta ABSOLUTA del directorio 'spark-project' en tu instancia Spark-EC2.
SPARK_PROJECT_PATH = "/home/ubuntu/spark-project"
# --------------------------

with DAG(
    dag_id='spark_etl_final',
    start_date=datetime(2025, 1, 1),
    schedule_interval=timedelta(hours=12), # Ejecutar dos veces al día
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'ssh_conn_id': SSH_CONN_ID, # Usa el nuevo ID
    }
) as dag:
    
    # 1. Tarea para ejecutar la ETL de la Capa Silver
    run_capa_silver = SSHOperator(
        task_id='run_capa_silver_etl',
        # Comando para navegar y ejecutar capa_silver.py dentro del contenedor
        command=f"cd {SPARK_PROJECT_PATH} && sudo docker-compose exec -T spark-server python3 /app/capa_silver.py",
        dag=dag,
    )

    # 2. Tarea para ejecutar la ETL de la Capa Gold (Depende de Silver)
    run_capa_gold = SSHOperator(
        task_id='run_capa_gold_etl',
        # Comando para navegar y ejecutar capa_gold.py dentro del contenedor
        command=f"cd {SPARK_PROJECT_PATH} && sudo docker-compose exec -T spark-server python3 /app/capa_gold.py",
        dag=dag,
    )

    run_capa_silver >> run_capa_gold
