Testeando repositorio

# Hacer entorno virtual para no interferir con otras dependencias

```bash
python3 -m venv venv
source venv/bin/activate
```

# Instalar librería kafka para python

```bash
pip install confluent-kafka
```

# Hacer contenedor con Kafka en Docker

```bash
services:
  kafka:
    image: apache/kafka:latest
    container_name: kafka-server
```