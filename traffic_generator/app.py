from fastapi import FastAPI
from confluent_kafka import Producer
import random
import numpy as np
import os
import time
import json
import uuid

app = FastAPI()

# Configuración de Kafka (Reemplaza a la antigua CACHE_URL)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_MAIN = "consultas_main"

producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

ZONES = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERY_TYPES = ["Q1", "Q2", "Q3", "Q4", "Q5"]

# --- FUNCIONES MATEMÁTICAS INTACTAS DE TU TAREA 1 ---

def generate_uniform_query():
    query_type = random.choice(QUERY_TYPES)

    if query_type == "Q4":
        zone_a, zone_b = random.sample(ZONES, 2)
        return {
            "type": "Q4",
            "params": {
                "zone_a": zone_a,
                "zone_b": zone_b,
                "confidence_min": random.choice([0.0, 0.5, 0.7])
            }
        }

    if query_type == "Q5":
        return {
            "type": "Q5",
            "params": {
                "zone_id": random.choice(ZONES),
                "bins": random.choice([5, 10])
            }
        }

    return {
        "type": query_type,
        "params": {
            "zone_id": random.choice(ZONES),
            "confidence_min": random.choice([0.0, 0.5, 0.7])
        }
    }

def generate_zipf_zone():
    ranks = np.arange(1, len(ZONES) + 1)
    probs = 1 / ranks
    probs = probs / probs.sum()
    return np.random.choice(ZONES, p=probs)

def generate_zipf_query():
    query_type = random.choice(QUERY_TYPES)
    main_zone = generate_zipf_zone()

    if query_type == "Q4":
        other_zone = random.choice([z for z in ZONES if z != main_zone])
        return {
            "type": "Q4",
            "params": {
                "zone_a": main_zone,
                "zone_b": other_zone,
                "confidence_min": random.choice([0.0, 0.5, 0.7])
            }
        }

    if query_type == "Q5":
        return {
            "type": "Q5",
            "params": {
                "zone_id": main_zone,
                "bins": random.choice([5, 10])
            }
        }

    return {
        "type": query_type,
        "params": {
            "zone_id": main_zone,
            "confidence_min": random.choice([0.0, 0.5, 0.7])
        }
    }

# --- NUEVA LÓGICA DE KAFKA PARA LA TAREA 2 ---

def delivery_report(err, msg):
    if err is not None:
        print(f"Error al enviar mensaje a Kafka: {err}")

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
        
        # Envoltorio con metadata requerida por la Tarea 2
        mensaje = {
            "id_consulta": str(uuid.uuid4()),
            "retry_count": 0,
            "timestamp_creacion": time.time(),
            "query": query_data
        }

        # Enviar a Kafka asíncronamente
        producer.produce(
            topic=TOPIC_MAIN,
            value=json.dumps(mensaje).encode('utf-8'),
            callback=delivery_report
        )
    
    # Asegurar que todos los mensajes salgan antes de responder
    producer.flush() 
    
    total_time = time.perf_counter() - start
    throughput = n_requests / total_time if total_time > 0 else 0

    return {
        "distribution": distribution,
        "n_requests": n_requests,
        "elapsed_seconds": total_time,
        "throughput": throughput,
        "mensaje": "Consultas inyectadas exitosamente a Kafka"
    }