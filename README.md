# Tarea 2: Procesamiento y Fallback con Apache Kafka

Este repositorio contiene la implementación de una arquitectura asíncrona y orientada a eventos. Utiliza **Apache Kafka** como *broker* de mensajería para desacoplar la ingesta de peticiones, **Redis** como sistema de caché (política LRU) y una flota escalable de consumidores para el procesamiento geoespacial implementado en la Tarea 1.

## 1. Estructura y Evolución del Proyecto

Para esta segunda entrega, la arquitectura síncrona original fue modificada para soportar un flujo asíncrono y tolerante a fallos. Los principales cambios en el repositorio son:

* **`consumer_service/` (NUEVO):** Es el núcleo de la Tarea 2. Contiene los Consumidores Kafka que actúan como *workers*. Estos leen los mensajes del tópico principal, verifican las respuestas en el caché y manejan toda la lógica de reintentos, rescate de fallos y derivación a la *Dead Letter Queue* (DLQ).
* **`traffic_generator/` (MODIFICADO):** Su rol cambió por completo. Dejó de hacer peticiones HTTP directas al backend y se transformó en un *Kafka Producer* que inyecta las consultas de forma asíncrona al sistema.
* **`analizar_metricas.py` (NUEVO):** Script diseñado específicamente para leer el archivo `metrics.csv` y calcular las nuevas métricas requeridas: *Retry rate*, *Recovery rate*, *DLQ rate* y el tamaño del *Backlog*.
* **`docker-compose.yml` (MODIFICADO):** Actualizado para orquestar la nueva infraestructura compleja, levantando simultáneamente Zookeeper, Kafka, Redis y todos los microservicios.
* **`response_generator/` y `data/` (REUTILIZADOS):** Mantienen la lógica de cálculo geoespacial de la Tarea 1. Siguen actuando como el motor principal de respuestas, pero ahora son consultados exclusivamente por los consumidores en caso de un *cache miss*.

## 2. Configuración Inicial del Entorno

Para evitar conflictos con otras dependencias del sistema, se recomienda levantar un entorno virtual de Python para el análisis de métricas.

    python3 -m venv venv
    source venv/bin/activate
    pip install confluent-kafka pandas

## 3. Despliegue de la Infraestructura

La arquitectura está dockerizada e incluye Zookeeper, Kafka, Redis, el Generador de Tráfico, el Generador de Respuestas y el Servicio Consumidor.

    docker compose up -d --build

## 4. Protocolo Estricto de Limpieza (Obligatorio)

Para garantizar la validez científica de los experimentos y evitar la contaminación de datos (caché heredado), ejecuta siempre estos comandos antes de iniciar cualquier prueba:

    > metrics/metrics.csv
    docker compose exec redis redis-cli FLUSHALL

## 5. Escenarios de Prueba

A continuación, se detallan los comandos para replicar los cuatro escenarios experimentales definidos en el informe técnico.

### Escenario A: Procesamiento Estándar (1 Consumer)
Asegúrate de tener solo un trabajador activo procesando la carga.

    docker compose up -d --scale consumer_service=1
    curl -X POST http://localhost:8002/run -H "Content-Type: application/json" -d '{"distribution": "uniform", "n_requests": 500}'

### Escenario B: Escalamiento Horizontal (Múltiples Consumers)
Demuestra cómo Kafka balancea las particiones entre varios trabajadores para reducir latencias.

    docker compose up -d --scale consumer_service=3
    curl -X POST http://localhost:8002/run -H "Content-Type: application/json" -d '{"distribution": "uniform", "n_requests": 500}'

### Escenario C: Tolerancia a Fallos (Caída del Backend)
Prueba la resiliencia del sistema, el encolamiento en el tópico de reintentos y la política de la DLQ.

    docker stop response_generator
    curl -X POST http://localhost:8002/run -H "Content-Type: application/json" -d '{"distribution": "uniform", "n_requests": 500}'
    
    # Esperar unos minutos para observar el backlog de reintentos...
    
    docker start response_generator

### Escenario D: Spike de Tráfico masivo (Estrés del Caché)
Somete el sistema a una ráfaga masiva con alta repetición de datos para evaluar la resiliencia bajo sobrecarga.

    curl -X POST http://localhost:8002/run -H "Content-Type: application/json" -d '{"distribution": "zipf", "n_requests": 5000}'

## 6. Análisis de Resultados

Una vez finalizada cualquier prueba, puedes extraer el throughput, percentiles de latencia (p50, p95), tasas de acierto/fallo en caché y efectividad de los reintentos ejecutando el analizador sobre el archivo CSV:

    python3 analizar_metricas.py metrics/metrics.csv