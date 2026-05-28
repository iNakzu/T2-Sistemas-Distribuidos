from fastapi import FastAPI
from confluent_kafka import Producer
import random
import numpy as np
import os
import time
import json
import uuid

app = FastAPI()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_MAIN = "consultas_main"

# Configuración del Productor
producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

ZONES = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERY_TYPES = ["Q1", "Q2", "Q3", "Q4", "Q5"]

# ... (Mantén aquí tus funciones generate_uniform_query y generate_zipf_query intactas) ...
def generate_uniform_query():
    query_type = random.choice(QUERY_TYPES)
    # ... código original ...
    return {"type": query_type, "params": {"zone_id": random.choice(ZONES), "confidence_min": 0.5}}

def generate_zipf_query():
    # ... código original ...
    return {"type": random.choice(QUERY_TYPES), "params": {"zone_id": "Z1", "confidence_min": 0.5}}

def delivery_report(err, msg):
    if err is not None:
        print(f"Error al enviar mensaje: {err}")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/run")
def run_traffic(payload: dict):
    distribution = payload.get("distribution", "uniform")
    n_requests = int(payload.get("n_requests", 100))

    start = time.perf_counter()

    for _ in range(n_requests):
        if distribution == "zipf":
            query_data = generate_zipf_query()
        else:
            query_data = generate_uniform_query()
        
        # Envoltorio requerido por la Tarea 2
        mensaje = {
            "id_consulta": str(uuid.uuid4()),
            "retry_count": 0,
            "timestamp_creacion": time.time(),
            "query": query_data
        }

        producer.produce(
            topic=TOPIC_MAIN,
            value=json.dumps(mensaje).encode('utf-8'),
            callback=delivery_report
        )
    
    producer.flush() # Espera a que se envíen todos
    total_time = time.perf_counter() - start
    throughput = n_requests / total_time if total_time > 0 else 0

    return {
        "mensaje": f"{n_requests} consultas enviadas a Kafka",
        "throughput_envio": throughput
    }