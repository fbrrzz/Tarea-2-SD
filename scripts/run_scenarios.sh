#!/usr/bin/env bash
# =============================================================================
# run_scenarios.sh — Ejecuta los escenarios de evaluación de la Tarea 2
# Uso: ./scripts/run_scenarios.sh [escenario]
#   escenarios: base | kafka1 | kafka_multi | failure | spike | all
# =============================================================================

set -euo pipefail
COMPOSE="docker compose"
METRICS_URL="http://localhost:8080"

# ── Helpers ───────────────────────────────────────────────────────────────────
log()  { echo -e "\n\033[1;36m>>> $*\033[0m"; }
ok()   { echo -e "\033[1;32m✔  $*\033[0m"; }
warn() { echo -e "\033[1;33m⚠  $*\033[0m"; }

wait_ready() {
    log "Esperando que los servicios estén listos..."
    sleep 8
}

reset_metrics() {
    curl -s -X POST "$METRICS_URL/reset" > /dev/null && ok "Métricas reseteadas"
}

show_metrics() {
    echo ""
    curl -s "$METRICS_URL/metrics" | python3 -m json.tool
    echo ""
}

# ── Escenario 1: Base (síncrono sin Kafka) ────────────────────────────────────
scenario_base() {
    log "ESCENARIO BASE — Sistema síncrono sin Kafka"
    warn "Este escenario requiere tu implementación de la Tarea 1."
    warn "Documenta las métricas manualmente para comparar luego con Kafka."
}

# ── Escenario 2: Kafka + 1 Consumer ──────────────────────────────────────────
scenario_kafka1() {
    log "ESCENARIO: Kafka + 1 Consumer"
    $COMPOSE up -d kafka redis response_generator metrics
    wait_ready
    $COMPOSE up -d --scale consumer=1 consumer
    sleep 5
    reset_metrics

    log "Iniciando producer (60s, 10 qps, zipf)..."
    $COMPOSE run --rm \
        -e QUERIES_PER_SECOND=10 \
        -e DISTRIBUTION=zipf \
        -e DURATION_SECONDS=60 \
        producer

    log "Métricas finales — Kafka + 1 Consumer:"
    show_metrics
}

# ── Escenario 3: Kafka + Múltiples Consumers ──────────────────────────────────
scenario_kafka_multi() {
    local N=${1:-3}
    log "ESCENARIO: Kafka + $N Consumers"
    $COMPOSE up -d kafka redis response_generator metrics
    wait_ready
    $COMPOSE up -d --scale consumer="$N" consumer
    sleep 5
    reset_metrics

    log "Iniciando producer (60s, 30 qps, zipf)..."
    $COMPOSE run --rm \
        -e QUERIES_PER_SECOND=30 \
        -e DISTRIBUTION=zipf \
        -e DURATION_SECONDS=60 \
        producer

    log "Métricas finales — Kafka + $N Consumers:"
    show_metrics
}

# ── Escenario 4: Falla temporal del Generador de Respuestas ───────────────────
scenario_failure() {
    log "ESCENARIO: Falla Temporal"
    $COMPOSE up -d kafka redis response_generator metrics
    wait_ready
    $COMPOSE up -d --scale consumer=2 consumer
    sleep 5
    reset_metrics

    log "Iniciando producer en background (90s, 10 qps)..."
    $COMPOSE run --rm -d \
        -e QUERIES_PER_SECOND=10 \
        -e DURATION_SECONDS=90 \
        producer || true

    log "Esperando 20s antes de simular fallo..."
    sleep 20
    log "*** DETENIENDO Generador de Respuestas (fallo simulado) ***"
    $COMPOSE stop response_generator

    log "Fallo activo por 20s — los consumers deben reintentar..."
    sleep 20
    log "*** RESTAURANDO Generador de Respuestas ***"
    $COMPOSE start response_generator

    log "Esperando recuperación..."
    sleep 35

    log "Métricas finales — Falla Temporal:"
    show_metrics
}

# ── Escenario 5: Spike de Tráfico ─────────────────────────────────────────────
scenario_spike() {
    log "ESCENARIO: Spike de Tráfico"
    $COMPOSE up -d kafka redis response_generator metrics
    wait_ready
    $COMPOSE up -d --scale consumer=2 consumer
    sleep 5
    reset_metrics

    log "Tráfico normal (20s, 5 qps)..."
    $COMPOSE run --rm \
        -e QUERIES_PER_SECOND=5 \
        -e DURATION_SECONDS=20 \
        producer

    log "*** SPIKE: 100 qps por 10s ***"
    $COMPOSE run --rm \
        -e QUERIES_PER_SECOND=100 \
        -e DURATION_SECONDS=10 \
        producer

    log "Tráfico normal post-spike (30s, 5 qps)..."
    $COMPOSE run --rm \
        -e QUERIES_PER_SECOND=5 \
        -e DURATION_SECONDS=30 \
        producer

    log "Métricas finales — Spike de Tráfico:"
    show_metrics
}

# ── Derribo limpio ────────────────────────────────────────────────────────────
teardown() {
    log "Deteniendo todos los servicios..."
    $COMPOSE down -v
    ok "Limpieza completada"
}

# ── Main ──────────────────────────────────────────────────────────────────────
case "${1:-help}" in
    base)        scenario_base ;;
    kafka1)      scenario_kafka1 ;;
    kafka_multi) scenario_kafka_multi "${2:-3}" ;;
    failure)     scenario_failure ;;
    spike)       scenario_spike ;;
    all)
        scenario_kafka1
        $COMPOSE down -v
        scenario_kafka_multi 3
        $COMPOSE down -v
        scenario_failure
        $COMPOSE down -v
        scenario_spike
        ;;
    down)        teardown ;;
    *)
        echo "Uso: $0 {base|kafka1|kafka_multi [N]|failure|spike|all|down}"
        ;;
esac
