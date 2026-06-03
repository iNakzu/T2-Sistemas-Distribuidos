# Tarea 2 - Sistemas Distribuidos: Procesamiento y Fallback con Apache Kafka

Este repositorio presenta la segunda entrega de la Tarea 2 de Sistemas Distribuidos. El sistema evoluciona desde una solución principalmente síncrona con caché Redis hacia una arquitectura asíncrona basada en Apache Kafka, con consumidores paralelos, reintentos, recuperación ante fallos y una DLQ contemplada en el diseño.

## Integrantes

- Ignacio Antiguay
- Benjamín Guzmán

## Descripción general

La solución organiza el procesamiento de consultas geoespaciales de la siguiente forma:

- `traffic_generator` actúa como productor de consultas y las publica en Kafka.
- Apache Kafka funciona como cola principal de entrada.
- `consumer_service` contiene los consumidores Kafka que procesan las consultas.
- Redis se usa como sistema de caché para responder rápido cuando ya existe una respuesta almacenada.
- `response_generator` resuelve los cache misses generando la respuesta geoespacial.
- `metrics/metrics.csv` registra los eventos y latencias observadas durante cada experimento.
- `analizar_metricas.py` procesa el CSV y resume los resultados de las ejecuciones.

## Flujo del sistema

1. `traffic_generator` crea consultas Q1 a Q5 con distribución uniforme o Zipf.
2. Cada consulta se publica en Kafka en el tópico principal.
3. Los consumidores de `consumer_service` leen los mensajes desde Kafka.
4. Cada consumidor consulta Redis con la clave asociada a la consulta.
5. Si existe un cache hit, la respuesta se entrega directamente desde caché.
6. Si ocurre un cache miss, el consumidor llama a `response_generator` para resolver la consulta.
7. Si `response_generator` falla temporalmente, la consulta se envía a reintento.
8. Si se supera el máximo de reintentos, el diseño contempla el envío a DLQ.
9. Todas las métricas relevantes se registran en formato CSV para su análisis posterior.

## Tecnologías

- Python
- FastAPI
- Apache Kafka
- Redis
- Docker
- Docker Compose
- Pandas
- NumPy
- CSV

## Estructura del proyecto

```text
.
├── consumer_service/
├── traffic_generator/
├── response_generator/
├── metrics/
├── resultados/
├── analizar_metricas.py
├── docker-compose.yml
└── README.md
```

## Requisitos previos

- Docker Desktop instalado y en ejecución.
- Docker Compose disponible.
- Python 3 instalado.
- Git instalado.
- Se recomienda usar VS Code con terminal PowerShell.

## Comandos para levantar el sistema

```powershell
docker compose up -d --scale consumer_service=1
docker ps
```

## Limpieza antes de cada experimento

Antes de ejecutar cada escenario, el repositorio debe quedar limpio para evitar arrastrar métricas o contenido de caché de pruebas anteriores.

```powershell
Clear-Content metrics\metrics.csv
docker compose exec redis redis-cli FLUSHALL
```

## Cómo enviar consultas en PowerShell

En Windows se debe usar `Invoke-RestMethod` y no `curl`, para mantener la compatibilidad con PowerShell.

### Ejemplo uniforme

```powershell
$body = @{
    distribution = "uniform"
    n_requests = 500
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8002/run" -Method POST -ContentType "application/json" -Body $body
```

### Ejemplo Zipf

```powershell
$body = @{
    distribution = "zipf"
    n_requests = 5000
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8002/run" -Method POST -ContentType "application/json" -Body $body
```

## Cómo analizar métricas

```powershell
python analizar_metricas.py metrics\metrics.csv
```

El script entrega los siguientes indicadores:

- Total de eventos
- Consultas completadas
- Hits
- Misses
- Recuperados tras fallo
- Enviados a reintento
- Enviados a DLQ
- Retry rate
- Recovery rate
- DLQ rate
- Latencia promedio
- p50
- p95

## Escenarios experimentales

### a) Kafka con 1 consumer

```powershell
docker compose up -d --scale consumer_service=1
$body = @{
    distribution = "uniform"
    n_requests = 500
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8002/run" -Method POST -ContentType "application/json" -Body $body
```

### b) Kafka con 3 consumers

```powershell
docker compose up -d --scale consumer_service=3
$body = @{
    distribution = "uniform"
    n_requests = 500
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8002/run" -Method POST -ContentType "application/json" -Body $body
```

### c) Falla temporal

```powershell
docker compose stop response_generator
$body = @{
    distribution = "uniform"
    n_requests = 500
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8002/run" -Method POST -ContentType "application/json" -Body $body
docker compose start response_generator
```

### d) Falla prolongada / DLQ no activada experimentalmente

```powershell
docker compose stop response_generator
$body = @{
    distribution = "uniform"
    n_requests = 243
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8002/run" -Method POST -ContentType "application/json" -Body $body
```

En esta prueba la configuración no activó mensajes en DLQ durante la ejecución registrada.

### e) Spike de tráfico con Zipf y 5000 consultas

```powershell
$body = @{
    distribution = "zipf"
    n_requests = 5000
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8002/run" -Method POST -ContentType "application/json" -Body $body
```

## Resultados principales

Los siguientes resultados corresponden a las ejecuciones registradas en el repositorio:

| Escenario | Eventos | Éxito | Hits Caché | Misses | Retry Rate | Recovery Rate | DLQ Rate | Latencia Prom. | p50 | p95 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Kafka + 1 Consumer | 500 | 500 | 394 / 78.8% | 106 / 21.2% | 0.0% | 0% | 0.0% | 2.52 ms | 0.62 ms | 11.06 ms |
| Kafka + 3 Consumers | 500 | 500 | 397 / 79.4% | 103 / 20.6% | 0.0% | 0% | 0.0% | 2.20 ms | 0.61 ms | 10.17 ms |
| Falla temporal | 500 | 490 | 386 / 78.78% | 104 / 21.22% | 2.0% | 0.0% | 0.0% | 2.55 ms | 0.66 ms | 11.62 ms |
| Reintentos + recuperación | 140 | 110 | 54 / 49.09% | 37 / 33.64% | 21.43% | 38.78% | 0.0% | 4.09 ms | 3.30 ms | 14.11 ms |
| Falla prolongada / DLQ no activada | 243 | 0 | 0 / 0% | 0 / 0% | 100.0% | 0.0% | 0.0% | 0 ms | 0 ms | 0 ms |
| Spike tráfico Zipf | 5000 | 5000 | 4885 / 97.7% | 115 / 2.3% | 0.0% | 0% | 0.0% | 0.90 ms | 0.47 ms | 2.30 ms |

## Interpretación general

Los resultados muestran que Kafka permitió desacoplar la generación de consultas del procesamiento. Con 3 consumidores se observa una mejora leve frente a 1 consumidor, sobre todo en latencia promedio y percentiles.

En la falla temporal se registraron reintentos, mientras que en el escenario de recuperación se observaron consultas recuperadas tras fallo. En la falla prolongada se alcanzó un 100% de retry rate, pero no se activó DLQ en las pruebas ejecutadas.

La DLQ se considera implementada y contemplada en el diseño, pero no se registraron mensajes en DLQ bajo esta configuración experimental. En el escenario de spike con distribución Zipf se alcanzó un 97.7% de hits y se completaron 5000 consultas sin pérdida.

## Comandos útiles para video

```powershell
docker ps
docker compose logs consumer_service --tail=50
type resultados\02_kafka_3_consumers.txt
type resultados\03_falla_temporal.txt
type resultados\04_reintentos_sin_dlq.txt
type resultados\05_spike_trafico.txt
```

## Verificación final

Se revisó la documentación final del proyecto y se verificó que los resultados experimentales se encuentren respaldados en la carpeta `resultados/`.

## Video de demostración

Link del video: [Video](https://youtu.be/T9RXCZHUcSs)

