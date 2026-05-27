# Tarea 2 — Procesamiento y Fallback con Apache Kafka

## Requisitos

| Herramienta | Versión mínima | Instalación |
|-------------|---------------|-------------|
| Docker      | 24+           | `sudo apt install docker.io` |
| Docker Compose Plugin | 2.20+ | incluido con Docker Desktop / `sudo apt install docker-compose-plugin` |
| (Opcional) curl + python3 | — | para ver métricas desde terminal |

> **No necesitas instalar Kafka, Python ni Redis localmente.** Todo corre dentro de Docker.

---

## Estructura del proyecto

```
tarea2/
├── docker-compose.yml          ← orquestación de todos los servicios
├── producer/
│   ├── Dockerfile
│   └── producer.py             ← generador de tráfico (Zipf / Uniforme)
├── consumer/
│   ├── Dockerfile
│   └── consumer.py             ← consumidor Kafka + caché + reintentos + DLQ
├── response_generator/
│   ├── Dockerfile
│   └── main.py                 ← API REST que procesa consultas geoespaciales
├── metrics/
│   ├── Dockerfile
│   └── main.py                 ← recopila y expone métricas
└── scripts/
    └── run_scenarios.sh        ← helper para ejecutar cada escenario
```

---

## Instalación rápida

```bash
# 1. Clonar / copiar el proyecto
cd tarea2

# 2. Construir las imágenes (solo la primera vez o tras cambios)
docker compose build

# 3. Verificar que todo está bien
docker compose config
```

---

## Cómo ejecutar

### Levantar todos los servicios

```bash
docker compose up -d kafka redis response_generator metrics
```

### Verificar que Kafka está listo

```bash
# Listar tópicos (debe responder sin error)
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --list
```

### Levantar 1 consumidor

```bash
docker compose up -d --scale consumer=1 consumer
```

### Ejecutar el generador de tráfico (60 segundos, 10 qps, distribución Zipf)

```bash
docker compose run --rm \
  -e QUERIES_PER_SECOND=10 \
  -e DISTRIBUTION=zipf \
  -e DURATION_SECONDS=60 \
  producer
```

### Ver métricas en tiempo real

```bash
# En otra terminal:
watch -n 2 "curl -s http://localhost:8080/metrics | python3 -m json.tool"
```

---

## Escenarios de evaluación

Usa el script helper (requiere bash):

```bash
chmod +x scripts/run_scenarios.sh

# Escenario: Kafka + 1 consumer
./scripts/run_scenarios.sh kafka1

# Escenario: Kafka + 3 consumers
./scripts/run_scenarios.sh kafka_multi 3

# Escenario: Falla temporal del generador de respuestas
./scripts/run_scenarios.sh failure

# Escenario: Spike de tráfico
./scripts/run_scenarios.sh spike

# Ejecutar todos en secuencia
./scripts/run_scenarios.sh all

# Apagar todo
./scripts/run_scenarios.sh down
```

### O manualmente, paso a paso:

#### Escenario: Kafka + múltiples consumers

```bash
# Levantar 3 consumers del mismo grupo (balanceo automático)
docker compose up -d --scale consumer=3 consumer

# Ver que los 3 están corriendo
docker compose ps consumer

# Ver logs de todos en paralelo
docker compose logs -f consumer
```

#### Escenario: Simular falla del generador de respuestas

```bash
# Detener el generador (simula falla)
docker compose stop response_generator

# Los consumers reintentarán hasta MAX_RETRIES veces, luego van a DLQ
# Ver las métricas mientras falla:
curl -s http://localhost:8080/metrics | python3 -m json.tool

# Restaurar
docker compose start response_generator
```

#### Escenario: Inyectar tasa de fallos aleatorios (sin detener el servicio)

```bash
# 30% de fallos aleatorios
curl -X POST "http://localhost:8000/set_failure_rate?rate=0.3"

# Volver a 0%
curl -X POST "http://localhost:8000/set_failure_rate?rate=0.0"
```

---

## Verificar que el sistema funciona correctamente

### 1. Kafka recibe mensajes

```bash
# Ver mensajes en el tópico principal (Ctrl+C para salir)
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic geo-queries \
  --from-beginning \
  --max-messages 5
```

### 2. Tópicos creados automáticamente

```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --list
# Deberías ver: geo-queries, geo-queries-retry, geo-queries-dlq
```

### 3. Consumer lag (backlog)

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group geo-consumers \
  --describe
# LAG = mensajes pendientes sin procesar
```

### 4. Métricas del sistema

```bash
curl -s http://localhost:8080/metrics | python3 -m json.tool
```

Salida esperada:
```json
{
  "throughput_qps": 8.73,
  "total_processed": 524,
  "cache_hits": 311,
  "cache_misses": 213,
  "cache_hit_rate": 0.594,
  "retry_rate": 0.021,
  "dlq_count": 0,
  "dlq_rate": 0.0,
  "latency_p50_ms": 87.4,
  "latency_p95_ms": 195.2
}
```

### 5. Ver logs de cada servicio

```bash
docker compose logs -f consumer           # todos los consumers
docker compose logs -f response_generator
docker compose logs -f producer
```


---

## Configuración de la Caché (Redis)

Para cumplir con la rúbrica de la Tarea 2, el sistema incluye una configuración de caché definida con las siguientes características:

* **Tamaño Máximo:** `128 MB`
* **Política de Remoción (Algoritmo):** `allkeys-lru` (Least Recently Used)
* **TTL (Time to Live):** Definido a nivel de código base dentro de `consumer/consumer.py`.
* **Motor:** Redis 7

*Nota: El límite de memoria y la política están configurados de forma nativa en el `docker-compose.yml` en los parámetros del contenedor `redis`. La caché se llena de manera automática a medida que fluyen las consultas (patrón Cache-Aside).*

---

## Variables de entorno importantes

| Variable | Servicio | Default | Descripción |
|----------|----------|---------|-------------|
| `QUERIES_PER_SECOND` | producer | `10` | Tasa de generación |
| `DISTRIBUTION` | producer | `zipf` | `zipf` o `uniform` |
| `DURATION_SECONDS` | producer | `60` | Duración de la prueba |
| `MAX_RETRIES` | consumer | `3` | Intentos antes de DLQ |
| `FAILURE_RATE` | response_generator | `0.0` | Tasa de fallos simulados (0.0–1.0) |

Puedes cambiarlos en `docker-compose.yml` o pasarlos con `-e` al ejecutar.

---

## Apagar todo

```bash
docker compose down -v   # -v elimina los volúmenes de Kafka
```