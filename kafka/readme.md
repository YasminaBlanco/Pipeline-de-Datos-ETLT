# ⚡ Módulo Kafka

Este módulo se encarga del flujo de datos en tiempo real. Utiliza Apache Kafka como bus de mensajes para desacoplar la ingesta de datos (Productor) del procesamiento inicial (Consumidor/Spark Streaming).

## 📁 Contenido

* [producer.py](producer.py): Script de Python que simula la ingesta de datos climáticos (ej: llamando a una API externa) y los envía al topic `openweather_topic`.
* [consumer.py](consumer.py): Script de Python que lee mensajes del topic de Kafka. En una arquitectura de streaming, este rol puede ser reemplazado por Spark Streaming o un proceso ETL.
* `server.properties`: Archivo de configuración para el broker de Kafka (puertos, logs).

## ⚙️ Configuración y Funcionamiento

* Crearse una cuenta en https://openweathermap.org/api para obtener una clave de API.

### Tópico Principal

* **Nombre:** `openweather_topic`
* **Función:** Recibir el payload JSON crudo directamente desde la fuente de datos.
    https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric&lang=es"

### Ejecución de Scripts

Para simular la ingesta y el consumo de datos:

Instala las dependencias de Kafka
# Instala las dependencias de Kafka.
pip install kafka-python requests python-dotenv:

1.  **Ejecutar el Productor:**
    Ejecuta este script dentro del contenedor de `kafka` para empezar a enviar datos al topic.
    ```bash
    docker-compose exec kafka python3 /app/producer.py
    ```

2.  **Ejecutar el Consumidor:**
    Ejecuta este script para verificar que los datos se están recibiendo correctamente en el otro extremo.
    ```bash
    docker-compose exec kafka python3 /app/consumer.py
    ```

3.  **Verificar Buckets S3:**
    Accede a la consola de S3 y verifica que los datos se han escrito en los Buckets S3 correspondientes.
