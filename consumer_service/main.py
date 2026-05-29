import os
import json
import time
import redis
import requests
from confluent_kafka import Consumer, Producer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
RESPONSE_GENERATOR_URL = os.getenv("RESPONSE_GENERATOR_URL", "http://response_generator:8001/query")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))

TOPIC_MAIN = "consultas_main"
TOPIC_RETRY = "consultas_retry"
TOPIC_DLQ = "consultas_dlq"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
producer = Producer({'bootstrap.servers': KAFKA_BROKER})
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'grupo_procesamiento_1',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([TOPIC_MAIN, TOPIC_RETRY])

def registrar_metrica(evento, latencia):
    archivo_metricas = '/app/metrics/metrics.csv'
    # Escribir cabecera si el archivo es nuevo
    es_nuevo = not os.path.exists(archivo_metricas) or os.path.getsize(archivo_metricas) == 0
    with open(archivo_metricas, 'a') as f:
        if es_nuevo:
            f.write("event_type,latency_ms\n")
        # Convertir a milisegundos
        latencia_ms = round(latencia * 1000, 4)
        f.write(f"{evento},{latencia_ms}\n")

def procesar_consulta(mensaje):
    id_consulta = mensaje["id_consulta"]
    query = mensaje["query"]
    retry_count = mensaje.get("retry_count", 0)
    
    cache_key = json.dumps(query, sort_keys=True)
    inicio_procesamiento = time.time()
    
    cached_response = r.get(cache_key)
    if cached_response:
        print(f"[{id_consulta}] Cache HIT")
        registrar_metrica("hit", time.time() - inicio_procesamiento)
        return True

    print(f"[{id_consulta}] Cache MISS. Consultando generador...")
    try:
        resp = requests.post(RESPONSE_GENERATOR_URL, json=query, timeout=3)
        resp.raise_for_status()
        
        r.setex(cache_key, 300, json.dumps(resp.json()))
        latencia = time.time() - inicio_procesamiento
        
        # Si venía de un reintento y tuvo éxito, es un "recovered". 
        if retry_count > 0:
            registrar_metrica("recovered", latencia)
        else:
            registrar_metrica("miss", latencia)
            
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"[{id_consulta}] Falla temporal en Generador: {e}")
        return False

print("Iniciando Consumidor Kafka...")

while True:
    msg = consumer.poll(1.0)
    if msg is None: continue
    if msg.error():
        print(f"Error de consumidor: {msg.error()}")
        continue

    mensaje = json.loads(msg.value().decode('utf-8'))
    inicio_total = time.time()
    
    exito = procesar_consulta(mensaje)
    
    if not exito:
        mensaje["retry_count"] += 1
        latencia_fallo = time.time() - inicio_total
        
        if mensaje["retry_count"] >= MAX_RETRIES:
            print(f"[{mensaje['id_consulta']}] Max reintentos alcanzado. Enviando a DLQ.")
            registrar_metrica("dlq", latencia_fallo)
            producer.produce(TOPIC_DLQ, value=json.dumps(mensaje).encode('utf-8'))
        else:
            print(f"[{mensaje['id_consulta']}] Reintentando (Intento {mensaje['retry_count']})...")
            registrar_metrica("retry", latencia_fallo)
            time.sleep(2) 
            producer.produce(TOPIC_RETRY, value=json.dumps(mensaje).encode('utf-8'))
            
        producer.flush()