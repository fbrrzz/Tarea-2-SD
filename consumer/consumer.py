"""
Consumidor Kafka
----------------
- Consume del tópico principal 'geo-queries'
- Verifica caché Redis (hit → responde inmediato)
- Cache miss → llama al Generador de Respuestas
- Falla temporal → publica en tópico de reintento
- retry_count >= MAX_RETRIES → publica en DLQ
- Reporta todas las métricas al servicio de métricas
"""

import json, os, time, logging, socket
import redis, requests
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] consumer-%(process)d %(message)s"
)
log = logging.getLogger(__name__)

# ── Config via variables de entorno ──────────────────────────────────────────
KAFKA_BOOTSTRAP        = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
REDIS_HOST             = os.getenv("REDIS_HOST", "localhost")
RESPONSE_GENERATOR_URL = os.getenv("RESPONSE_GENERATOR_URL", "http://localhost:8000")
METRICS_URL            = os.getenv("METRICS_URL", "http://localhost:8080")
MAX_RETRIES            = int(os.getenv("MAX_RETRIES", "3"))
CONSUMER_GROUP         = os.getenv("CONSUMER_GROUP", "geo-consumers")

TOPIC_MAIN    = "geo-queries"
TOPIC_RETRY   = "geo-queries-retry"
TOPIC_DLQ     = "geo-queries-dlq"

CACHE_TTL     = 300   # segundos
CACHE_SIZE    = 500   # máximo de keys (gestionado por Redis con allkeys-lru)

# ── Clientes ─────────────────────────────────────────────────────────────────
def make_consumer():
    while True:
        try:
            c = KafkaConsumer(
                TOPIC_MAIN, TOPIC_RETRY,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=CONSUMER_GROUP,
                value_deserializer=lambda b: json.loads(b.decode()),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            log.info("Consumidor conectado a Kafka (%s)", KAFKA_BOOTSTRAP)
            return c
        except Exception as e:
            log.warning("Esperando Kafka: %s", e)
            time.sleep(3)

def make_producer():
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
            )
            return p
        except Exception as e:
            log.warning("Esperando Kafka producer: %s", e)
            time.sleep(3)

def make_redis():
    while True:
        try:
            r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
            r.ping()
            log.info("Redis conectado")
            return r
        except Exception as e:
            log.warning("Esperando Redis: %s", e)
            time.sleep(2)

# ── Métricas (best-effort, no bloquea el flujo) ───────────────────────────────
def report(event: str, latency_ms: float = None, query_id: str = None):
    try:
        requests.post(
            f"{METRICS_URL}/event",
            json={"event": event, "latency_ms": latency_ms, "query_id": query_id},
            timeout=1,
        )
    except Exception:
        pass

# ── Lógica principal ──────────────────────────────────────────────────────────
def process_message(msg, cache: redis.Redis, producer: KafkaProducer):
    query = msg.value
    query_id    = query.get("query_id", "unknown")
    query_type  = query.get("query_type", "Q1")
    zone        = query.get("zone", "santiago")
    retry_count = query.get("retry_count", 0)
    created_at  = query.get("created_at", time.time())

    t_start = time.time()
    cache_key = f"{query_type}:{zone}"

    # ── 1. Verificar caché ────────────────────────────────────────────────────
    cached = cache.get(cache_key)
    if cached:
        latency = (time.time() - t_start) * 1000
        report("cache_hit", latency_ms=latency, query_id=query_id)
        log.info("CACHE HIT  %s | %s | %.1fms", query_id, cache_key, latency)
        return

    report("cache_miss", query_id=query_id)

    # ── 2. Llamar al Generador de Respuestas ──────────────────────────────────
    try:
        resp = requests.post(
            f"{RESPONSE_GENERATOR_URL}/process",
            json={
                "query_id":    query_id,
                "query_type":  query_type,
                "zone":        zone,
                "retry_count": retry_count,
            },
            timeout=5,
        )
        resp.raise_for_status()

        result = resp.json()
        cache.setex(cache_key, CACHE_TTL, json.dumps(result))

        latency = (time.time() - t_start) * 1000
        event   = "recovered" if retry_count > 0 else "processed"
        report(event, latency_ms=latency, query_id=query_id)
        log.info("PROCESSED  %s | intento %d | %.1fms", query_id, retry_count + 1, latency)

    except Exception as e:
        report("error", query_id=query_id)
        log.warning("FALLO  %s | intento %d | %s", query_id, retry_count + 1, e)

        # ── 3. Reintento o DLQ ────────────────────────────────────────────────
        if retry_count + 1 >= MAX_RETRIES:
            producer.send(TOPIC_DLQ, {**query, "retry_count": retry_count + 1, "final_error": str(e)})
            report("dlq", query_id=query_id)
            log.error("DLQ        %s | agotó %d intentos", query_id, MAX_RETRIES)
        else:
            updated = {**query, "retry_count": retry_count + 1}
            producer.send(TOPIC_RETRY, updated)
            report("retry", query_id=query_id)
            log.info("RETRY      %s | intento %d → %d", query_id, retry_count + 1, retry_count + 2)


def main():
    log.info("Iniciando consumidor (grupo=%s, max_retries=%d)", CONSUMER_GROUP, MAX_RETRIES)
    consumer = make_consumer()
    producer = make_producer()
    cache    = make_redis()

    for msg in consumer:
        try:
            process_message(msg, cache, producer)
        except Exception as e:
            log.exception("Error inesperado procesando mensaje: %s", e)

if __name__ == "__main__":
    main()
