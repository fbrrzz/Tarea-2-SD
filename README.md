# Tarea 2 — Procesamiento y Fallback con Apache Kafka

## Requisitos

| Herramienta               | Versión mínima | Instalación                                                            |
| ------------------------- | -------------- | ---------------------------------------------------------------------- |
| Docker                    | 24+            | `sudo apt install docker.io`                                           |
| Docker Compose Plugin     | 2.20+          | incluido con Docker Desktop / `sudo apt install docker-compose-plugin` |
| (Opcional) curl + python3 | —              | para ver métricas desde terminal                                       |

> **No se necesita instalar Kafka, Python ni Redis localmente.** Todo corre dentro de Docker.

---

## Estructura del proyecto

```
tarea2/
├── docker-compose.yml          ← coordinacción de todos los servicios
├── producer/
│   ├── Dockerfile
│   └── producer.py             ← generador de tráfico (Zipf / Uniforme)
├── consumer/
│   ├── Dockerfile
│   └── consumer.py             ← consumidor Kafka + caché + reintentos + DLQ + reporte de backlog
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



---

## Escenarios de evaluación

```bash
chmod +x scripts/run_scenarios.sh
```

| Comando | Descripción | Duración aprox. |
| ------- | ----------- | --------------- |
| `./scripts/run_scenarios.sh kafka1` | Kafka + 1 consumer (60s, 10 qps, Zipf) | ~90s |
| `./scripts/run_scenarios.sh kafka_multi 3` | Kafka + N consumers (default 3) | ~90s |
| `./scripts/run_scenarios.sh failure` | Falla temporal del Generador de Respuestas | ~120s |
| `./scripts/run_scenarios.sh retry` | 30% fallos aleatorios — mide retry/DLQ rate | ~90s |
| `./scripts/run_scenarios.sh spike` | Spike de tráfico (5 → 50 → 5 qps) | ~90s |
| `./scripts/run_scenarios.sh all` | Todos los escenarios en secuencia | ~15 min |
| `./scripts/run_scenarios.sh down` | Detener y limpiar todos los servicios | — |

Cada escenario guarda sus métricas en `results_<nombre>.json` al finalizar.

### Escenario: Kafka + múltiples consumers

```bash
# 1 consumer
./scripts/run_scenarios.sh kafka1

# 3 consumers (default)
./scripts/run_scenarios.sh kafka_multi

# N consumers arbitrario
./scripts/run_scenarios.sh kafka_multi 5
```

### Escenario: Falla temporal

```bash
./scripts/run_scenarios.sh failure
```

Esto levanta 2 consumers → corre producer 90s → detiene `response_generator` 20s → lo restaura → espera recuperación → muestra métricas.


### Escenario: Reintentos con fallos aleatorios

```bash
./scripts/run_scenarios.sh retry
```

Inyecta 30% de fallos aleatorios via API sin detener el servicio, para observar retry rate y DLQ rate con el generador con capacidad reducida.


### Escenario: Spike de tráfico

```bash
./scripts/run_scenarios.sh spike
```

Ejecuta 3 fases consecutivas:
1. Tráfico normal: 5 qps × 20s
2. Spike: 50 qps × 15s (distribución uniforme)
3. Vuelta a normal: 5 qps × 30s

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

El backlog se reporta automáticamente en `/metrics` como `backlog_size` cada 10 segundos.
Para consultarlo directamente en Kafka:

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
  "error_count": 2,
  "recovered_count": 5,
  "recovery_time_s": 12.4,
  "backlog_size": 3,
  "latency_p50_ms": 87.4,
  "latency_p95_ms": 195.2,
  "uptime_s": 61.0
}
```

### 5. Ver logs de cada servicio

```bash
docker compose logs -f consumer           # todos los consumers
docker compose logs -f response_generator
docker compose logs -f producer
```

### Ver métricas en tiempo real

```bash
# En otra terminal:
watch -n 2 "curl -s http://localhost:8080/metrics | python3 -m json.tool"
```
---

## Configuración de la Caché (Redis)

El sistema incluye una configuración de caché definida con las siguientes características:

- **Tamaño Máximo:** `128 MB`
- **Política de Remoción (Algoritmo):** `allkeys-lru` (Least Recently Used)
- **TTL (Time to Live):** Definido a nivel de código base dentro de `consumer/consumer.py`.
- **Motor:** Redis 7

*Nota: El límite de memoria y la política están configurados de forma nativa en el `docker-compose.yml` en los parámetros del contenedor `redis`. La caché se llena de manera automática a medida que fluyen las consultas (patrón Cache-Aside).*

---

## Variables de entorno importantes

| Variable             | Servicio            | Default | Descripción                        |
| -------------------- | ------------------- | ------- | ---------------------------------- |
| `QUERIES_PER_SECOND` | producer            | `10`    | Tasa de generación                 |
| `DISTRIBUTION`       | producer            | `zipf`  | `zipf` o `uniform`                 |
| `DURATION_SECONDS`   | producer            | `60`    | Duración de la prueba              |
| `MAX_RETRIES`        | consumer            | `3`     | Intentos antes de DLQ              |
| `FAILURE_RATE`       | response_generator  | `0.0`   | Tasa de fallos simulados (0.0–1.0) |

Se pueden cambiar en `docker-compose.yml` o pasarlos con `-e` al ejecutar.

---

## Apagar todo

```bash
docker compose down -v
```