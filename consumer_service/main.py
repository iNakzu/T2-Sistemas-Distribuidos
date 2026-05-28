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

# Conexiones
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
producer = Producer({'bootstrap.servers': KAFKA_BROKER})
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'grupo_procesamiento_1', # Mismo grupo para escalamiento horizontal
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([TOPIC_MAIN, TOPIC_RETRY])

def procesar_consulta(mensaje):
    id_consulta = mensaje["id_consulta"]
    query = mensaje["query"]
    
    # Generar clave de caché basada en los parámetros de la consulta
    cache_key = json.dumps(query, sort_keys=True)
    
    inicio_procesamiento = time.time()
    
    # 1. Revisar Caché
    cached_response = r.get(cache_key)
    if cached_response:
        print(f"[{id_consulta}] Cache HIT")
        registrar_metrica("hit", time.time() - inicio_procesamiento)
        return True # Éxito

    # 2. Cache Miss -> Consultar al Generador de Respuestas
    print(f"[{id_consulta}] Cache MISS. Consultando generador...")
    try:
        # Simulamos un timeout corto para detectar fallas temporales
        resp = requests.post(RESPONSE_GENERATOR_URL, json=query, timeout=3)
        resp.raise_for_status()
        
        # Guardar en caché con TTL de 300s
        r.setex(cache_key, 300, json.dumps(resp.json()))
        registrar_metrica("miss", time.time() - inicio_procesamiento)
        return True # Éxito
        
    except requests.exceptions.RequestException as e:
        print(f"[{id_consulta}] Falla temporal en Generador: {e}")
        return False # Falló

def registrar_metrica(evento, latencia):
    # Aquí puedes escribir al archivo metrics.csv igual que en tu Tarea 1
    # Ejemplo básico:
    with open('/app/metrics/metrics.csv', 'a') as f:
        f.write(f"{evento},{latencia}\n")

print("Iniciando Consumidor Kafka...")

while True:
    msg = consumer.poll(1.0)
    if msg is None: continue
    if msg.error():
        print(f"Error de consumidor: {msg.error()}")
        continue

    mensaje = json.loads(msg.value().decode('utf-8'))
    
    # Intentar procesar
    exito = procesar_consulta(mensaje)
    
    # Lógica de Retry y DLQ
    if not exito:
        mensaje["retry_count"] += 1
        
        if mensaje["retry_count"] >= MAX_RETRIES:
            print(f"[{mensaje['id_consulta']}] Max reintentos alcanzado. Enviando a DLQ.")
            producer.produce(TOPIC_DLQ, value=json.dumps(mensaje).encode('utf-8'))
        else:
            print(f"[{mensaje['id_consulta']}] Reintentando (Intento {mensaje['retry_count']})...")
            # Un pequeño sleep para no saturar inmediatamente el generador caído
            time.sleep(2) 
            producer.produce(TOPIC_RETRY, value=json.dumps(mensaje).encode('utf-8'))
            
        producer.flush()