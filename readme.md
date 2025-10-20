#  Diseño e Implementación de un Pipeline de Datos ETLT

Este proyecto implementa una arquitectura de procesamiento de datos escalable (Data Lake) utilizando Apache Spark, Apache Kafka, y Apache Airflow. El objetivo es procesar flujos de datos climáticos en tiempo real y por lotes, transformándolos de la capa RAW a la capa GOLD para generar insights de potencial energético (eólico y solar).

## 🗂️ Estructura del Proyecto

El repositorio se organiza en módulos de tecnología:

* `airflow/`: Contiene el orquestador (DAGs, configuración).
* `kafka/`: Contiene los scripts de productor y consumidor para la ingesta de datos en tiempo real.
* `spark/`: Contiene la lógica ETL de transformación (capa Silver y Gold).

## ⚙️ Guía de Instalación y Ejecución (Paso a Paso)

Sigue estos pasos para poner en marcha toda la arquitectura en una máquina virtual o servidor dedicado

Debes crearte una cuenta en [cloud.air](https://cloud.airbyte.com/) para la ingesta de datos de Riohacha.
* Configurar la conexión de Airbyte con tu cuenta de AWS.*
* Configurar la conexión de Airbyte con tu cuenta de OpenWeatherMap.*
    * Crear el builder de datos de OpenWeatherMap.
    * Crear el destino de datos de Riohacha (Bucket RAW).
    * Crear la conexión de Airbyte con tu cuenta de AWS. (Considerar que se debe dar el permiso por medio de IAM para que Airbyte pueda acceder a tu cuenta de AWS y escribir en tu Bucket S3).


### Paso 1: Configuración del Servidor (Instancia AWS EC2)

Recomendamos usar una instancia con al menos **8GB de RAM** y **2 vCPUs** para asegurar que todos los servicios de Spark, Kafka y Airflow se ejecuten sin problemas de memoria.

Debes crear  tres instancias EC2 para ejecutar toda la arquitectura:
1. - Spark
2. - Kafka
3. - Airflow

1.  **Crear una instancia AWS EC2:** Accede a la página de instancias en la consola de AWS y crea una nueva instancia EC2 con la siguiente configuración:
    * Nombre de instancia: Puede ser un nombre relevante para identificar la instancia.
    * Imagen: Ubuntu Server 24.04 LTS (HVM).
    * Tipo de instancia: M7i-flex.large.
    * Tamaño de disco: 30 GB.
    En detalles avanzados
    * Perfiles de IAM: elegir el perfil para la instancia.

Descargar el archivo de par de clave .pem y guardarlo en donde puedas acceder.

2.  **Conexión SSH:** Conéctate a tu instancia usando SSH.
    ```bash
    ssh -i tu-clave.pem usuario@ip-publica
    ```

3.  **Actualizar el Sistema:**
    ```bash
    sudo apt update && sudo apt upgrade -y
    ```

### Paso 2: Instalación de Docker y Docker Compose

Instalaremos Docker y la herramienta Docker Compose, que es esencial para lanzar toda nuestra infraestructura con un solo comando.

1.  **Instalar Docker:**
    ```bash
    sudo apt install docker.io -y
    ```

2.  **Instalar Docker Compose (v2+):**
    ```bash
    sudo apt install docker-compose -y
    ```

3.  **Añadir tu usuario al grupo `docker`** (para evitar usar `sudo` constantemente):
    ```bash
    sudo usermod -aG docker $USER
    # ¡Importante! Debes cerrar la sesión SSH y volver a conectarte para que este cambio impacte.
    ```

### Paso 3: Configuración del Proyecto y Variables de Entorno

1.  **Clonar el Repositorio:**
    ```bash
    git clone [https://github.com/tu-usuario/proyecto-integrador.git](https://github.com/tu-usuario/proyecto-integrador.git)
    cd proyecto-integrador
    ```

2.  **Configurar Variables de Entorno (`.env`):**
    Debes configurar las credenciales de AWS y los nombres de tus S3 Buckets. Edita el archivo `.env` en la raíz del proyecto.
    
    ```bash
    nano .env
    ```

    Reemplaza los valores con tus credenciales:

    ```bash
    # AWS
    AWS_ACCESS_KEY_ID=TU_ACCESS_KEY
    AWS_SECRET_ACCESS_KEY=TU_SECRET_KEY
    # Nombres de Buckets S3 (ej: infraestructura-datos-raw)
    BUCKET_NAME_RAW=TU_BUCKET_RAW
    BUCKET_NAME_SILVER=TU_BUCKET_SILVER
    BUCKET_NAME_GOLD=TU_BUCKET_GOLD
    S3_REGION=TU_REGION
    ```


### Paso 4: Ejecución de la Arquitectura (Para las diferentes instancias)

Una vez que Docker y las variables de entorno están configuradas

1. Debes pegar la carpeta `spark/` del repositorio en la instancia EC2 que ejecutará el Spark.
   Debes pegar la carpeta `kafka/` del repositorio en la instancia EC2 que ejecutará el Kafka.
   Debes pegar la carpeta `airflow/` del repositorio en la instancia EC2 que ejecutará el Airflow.

1.  **Construir y Levantar los Contenedores:**
    ```bash
    docker-compose up -d --build
    ```
    Esto levantará los servicios de Airflow, Spark, Kafka y la base de datos de Airflow.
    o bien sudo docker-compose up -d --build

2.  **Verificar el Estado:**
    ```bash
    docker-compose ps
    ```
    El contenedor deben mostrar el estado `up`.

### 🌐 Acceso a las Interfaces Web

Una vez levantados los servicios, puedes acceder a las herramientas:

| Servicio | URL | Puerto por Defecto | Notas |
| :--- | :--- | :--- | :--- |
| **Airflow UI** | `http://<IP_PÚBLICA>:8080` | 8080 | Desbloquea el DAG `spark_etl_final` para iniciarlo. |

## 📄 Documentación por Módulo

Para detalles específicos sobre la configuración y scripts de cada componente, consulta los `README.md` individuales:

* **[Airflow Documentation](airflow/readme.md)**
* **[Kafka Documentation](kafka/readme.md)**
* **[Spark Documentation](spark/readme.md)**