# Tarea 2: Sistemas Distribuidos (Kafka + Redis)

# Hacer entorno virtual para no interferir con otras dependencias
```bash
python3 -m venv venv
source venv/bin/activate
```

# Instalar librerías necesarias para el entorno local (analizador de métricas)
```bash
pip install confluent-kafka pandas
```

# Levantar toda la infraestructura con Docker (Kafka, Redis y Microservicios)
En vez de levantar solo Kafka, esto construye y levanta todos los contenedores al mismo tiempo en segundo plano.
```bash
docker compose up -d --build
```

# Limpiar el historial de métricas antes de hacer una prueba nueva
```bash
> metrics/metrics.csv
```

# Inyectar ráfaga de tráfico para probar el sistema
Esto le pega al Generador de Tráfico para que mande 500 consultas asíncronas a Kafka.
```bash
curl -X POST http://localhost:8002/run \
     -H "Content-Type: application/json" \
     -d '{"distribution": "uniform", "n_requests": 500}'
```

# Analizar las métricas y latencias resultantes
Lee el archivo que generó el consumidor y calcula los porcentajes de Hit, Miss, Reintentos y DLQ.
```bash
python3 analizar_metricas.py metrics/metrics.csv
```

# Escalar el sistema (Levantar múltiples trabajadores)
Crea clones del consumidor de Kafka para que se repartan el trabajo y procesen más rápido.
```bash
docker compose up -d --scale consumer_service=3
```

# Simular una falla temporal para probar la tolerancia a fallos
1. Apagar el motor de respuestas:
```bash
docker stop response_generator
```

2. Inyectar el tráfico con el `curl` de arriba.

3. Esperar unos 10 segundos para ver cómo el consumidor reintenta.

4. Volver a prender el motor para que Kafka rescate los mensajes perdidos:
```bash
docker start response_generator
```