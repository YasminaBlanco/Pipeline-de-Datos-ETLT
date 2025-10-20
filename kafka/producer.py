import os, json, time, requests, sys
from datetime import datetime, timezone
from kafka import KafkaProducer

BROKER="172.31.38.86:9092"
TOPIC="openweather_topic"
CITY="Patagonia,ar"

# CORREGIDO: El primer argumento es el nombre de la variable de entorno, el segundo es el valor por defecto.
# Asegúrate de que esta clave sea válida.
API_KEY= "58c3a0e3e3bca9627e5d9f2ed52d341e"

POLL_SECS = int(os.getenv("POLL_SECS", "120"))
MAX_RUNS = int(os.getenv("MAX_RUNS", "0"))
RUN_ONCE = os.getenv("RUN_ONCE", "false").lower() in {"1","true","yes"}

def build_producer():
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all"
    )

def fetch_weather(city: str):

    if not API_KEY or API_KEY in ["CLAVE_DE_EJEMPLO_O_VACÍA"]: 
        print("ERROR: La clave API de OpenWeather (API_KEY) no ha sido reemplazada.", file=sys.stderr)
        sys.exit(1)

    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric&lang=es"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    js = r.json()
    
    # Se añade la corrección para asegurar el formato JSON que espera Spark
    return {
        "source": "openweather",
        "city": js.get("name"),
        "lat": js["coord"]["lat"],
        "lon": js["coord"]["lon"],
        "ts": int(time.time() * 1000), # Usar el timestamp actual en milisegundos para Spark
        "datetime_utc": datetime.fromtimestamp(js.get("dt"), tz=timezone.utc).isoformat().replace('+00:00', 'Z'),
        "temp_c": js["main"]["temp"],
        "humidity": js["main"]["humidity"],
        "pressure": js["main"]["pressure"],
        "wind_speed": js["wind"]["speed"]
    }

def main():
    producer = build_producer()
    print(f"[producer] broker={BROKER} topic={TOPIC} city={CITY}")

    iteration = 0
    while True:
        try:
            data = fetch_weather(CITY)
            producer.send(TOPIC, data).get(timeout=30)
            print(f"[producer] Enviado -> {data['city']}  temp_c={data['temp_c']}  {data['datetime_utc']}")
            iteration += 1
        except Exception as e:
            print(f"[producer] ERROR: {e}", file=sys.stderr)

        if RUN_ONCE or (MAX_RUNS and iteration >= MAX_RUNS):
            break
        time.sleep(POLL_SECS)

    print(f"[producer] Terminado tras {iteration} envíos.")

# CORREGIDO: Doble guion bajo (__name__ == "__main__")
if __name__ == "__main__":
    main()
