"""
Sistema de Métricas
-------------------
Recopila throughput, latencias, retry rate, DLQ rate, backlog y recovery time.
Expone los datos via HTTP para consulta y comparación entre escenarios.
"""

import time, statistics
from collections import deque
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional

app = FastAPI(title="Sistema de Métricas")

# ── Estado global de métricas ─────────────────────────────────────────────────
metrics = {
    "processed":       0,
    "cache_hits":      0,
    "cache_misses":    0,
    "retries":         0,
    "dlq":             0,
    "errors":          0,
    "latencies":       deque(maxlen=10000),
    "start_time":      time.time(),
    "last_failure_at": None,
    "recovered_at":    None,
    "recovered_count": 0,
    "backlog_size":    0,
}

class EventIn(BaseModel):
    event: str          # processed | cache_hit | cache_miss | retry | dlq | error | recovered
    latency_ms: Optional[float] = None
    query_id:   Optional[str]  = None

@app.post("/event")
def record_event(ev: EventIn):
    m = metrics
    if ev.event == "processed":
        m["processed"] += 1
        if ev.latency_ms:
            m["latencies"].append(ev.latency_ms)
    elif ev.event == "cache_hit":
        m["cache_hits"] += 1
        m["processed"]  += 1
        if ev.latency_ms:
            m["latencies"].append(ev.latency_ms)
    elif ev.event == "cache_miss":
        m["cache_misses"] += 1
    elif ev.event == "retry":
        m["retries"] += 1
    elif ev.event == "dlq":
        m["dlq"] += 1
    elif ev.event == "error":
        m["errors"] += 1
        m["last_failure_at"] = time.time()
    elif ev.event == "recovered":
        m["recovered_count"] += 1
        if m["last_failure_at"] and not m["recovered_at"]:
            m["recovered_at"] = time.time()
    elif ev.event == "backlog":
        if ev.latency_ms is not None:   # reutilizamos el campo como valor numérico
            m["backlog_size"] = int(ev.latency_ms)
    return {"ok": True}

@app.get("/metrics")
def get_metrics():
    m    = metrics
    lats = list(m["latencies"])
    elapsed = max(time.time() - m["start_time"], 0.001)

    recovery_time = None
    if m["last_failure_at"] and m["recovered_at"]:
        recovery_time = round(m["recovered_at"] - m["last_failure_at"], 2)

    return {
        "throughput_qps":   round(m["processed"] / elapsed, 2),
        "total_processed":  m["processed"],
        "cache_hits":       m["cache_hits"],
        "cache_misses":     m["cache_misses"],
        "cache_hit_rate":   round(m["cache_hits"] / max(m["processed"], 1), 3),
        "retry_rate":       round(m["retries"] / max(m["processed"] + m["retries"], 1), 3),
        "dlq_count":        m["dlq"],
        "dlq_rate":         round(m["dlq"] / max(m["processed"] + m["dlq"], 1), 3),
        "error_count":      m["errors"],
        "recovered_count":  m["recovered_count"],
        "recovery_time_s":  recovery_time,
        "backlog_size":     m["backlog_size"],
        "latency_p50_ms":   round(statistics.median(lats), 2) if lats else None,
        "latency_p95_ms":   round(sorted(lats)[int(len(lats) * 0.95)], 2) if len(lats) >= 20 else None,
        "uptime_s":         round(elapsed, 1),
    }

@app.post("/reset")
def reset():
    metrics.update({
        "processed": 0, "cache_hits": 0, "cache_misses": 0,
        "retries": 0, "dlq": 0, "errors": 0,
        "latencies": deque(maxlen=10000),
        "start_time": time.time(),
        "last_failure_at": None, "recovered_at": None, "recovered_count": 0,
        "backlog_size": 0,
    })
    return {"ok": True}