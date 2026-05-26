"""
Generador de Tráfico (Kafka Producer)
--------------------------------------
Genera consultas Q1-Q5 con distribución Zipf o Uniforme sobre zonas
de la Región Metropolitana de Santiago y las publica en Kafka.
"""

import json, os, time, uuid, logging
import numpy as np
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] producer %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
QUERIES_PER_SECOND = float(os.getenv("QUERIES_PER_SECOND", "10"))
DISTRIBUTION       = os.getenv("DISTRIBUTION", "zipf")   # zipf | uniform
DURATION_SECONDS   = int(os.getenv("DURATION_SECONDS", "60"))
TOPIC              = "geo-queries"

ZONES = [
    "providencia", "las_condes", "maipu", "santiago",
    "nunoa", "pudahuel", "la_florida", "peñalolen",
]
QUERY_TYPES = ["Q1", "Q2", "Q3", "Q4", "Q5"]

def zipf_distribution(n: int, s: float = 1.2) -> np.ndarray:
    """Distribución Zipf sobre n elementos con parámetro s."""
    ranks = np.arange(1, n + 1, dtype=float)
    weights = 1.0 / np.power(ranks, s)
    return weights / weights.sum()

def make_producer():
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
            )
            log.info("Producer conectado a Kafka (%s)", KAFKA_BOOTSTRAP)
            return p
        except Exception as e:
            log.warning("Esperando Kafka: %s", e)
            time.sleep(3)

def pick_zone(distribution: str) -> str:
    if distribution == "zipf":
        probs = zipf_distribution(len(ZONES))
        return np.random.choice(ZONES, p=probs)
    return np.random.choice(ZONES)   # uniforme

def pick_query_type(distribution: str) -> str:
    if distribution == "zipf":
        probs = zipf_distribution(len(QUERY_TYPES))
        return np.random.choice(QUERY_TYPES, p=probs)
    return np.random.choice(QUERY_TYPES)

def main():
    producer = make_producer()
    interval = 1.0 / QUERIES_PER_SECOND
    end_time = time.time() + DURATION_SECONDS
    sent = 0

    log.info("Iniciando generación: %.1f qps | distribución=%s | duración=%ds",
             QUERIES_PER_SECOND, DISTRIBUTION, DURATION_SECONDS)

    while time.time() < end_time:
        t0 = time.time()

        query = {
            "query_id":    str(uuid.uuid4()),
            "query_type":  pick_query_type(DISTRIBUTION),
            "zone":        pick_zone(DISTRIBUTION),
            "created_at":  t0,
            "retry_count": 0,
            "distribution": DISTRIBUTION,
        }

        try:
            producer.send(TOPIC, query)
            sent += 1
            if sent % 50 == 0:
                log.info("Enviadas %d consultas", sent)
        except KafkaError as e:
            log.error("Error enviando consulta: %s", e)

        elapsed = time.time() - t0
        sleep   = max(0.0, interval - elapsed)
        time.sleep(sleep)

    producer.flush()
    log.info("Generación completada. Total enviadas: %d", sent)

if __name__ == "__main__":
    main()
