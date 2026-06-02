#!/usr/bin/env bash
# =============================================================================
# run_scenarios.sh — Escenarios de evaluación Tarea 2 (Kafka + Fallback)
# Uso: ./scripts/run_scenarios.sh <escenario> [opciones]
#
#   kafka1              Kafka + 1 consumidor
#   kafka_multi [N]     Kafka + N consumidores (default: 3)
#   failure             Falla temporal del Generador de Respuestas
#   spike               Spike de tráfico
#   all                 Ejecuta todos en secuencia
#   down                Detiene y limpia todos los servicios
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

save_metrics() {
    local label="$1"
    local outfile="results_${label}.json"
    curl -s "$METRICS_URL/metrics" > "$outfile"
    ok "Métricas guardadas en $outfile"
}

teardown_quiet() {
    $COMPOSE down -v --remove-orphans 2>/dev/null || true
}

# ── Infraestructura base ──────────────────────────────────────────────────────
bring_up_base() {
    local consumers="${1:-1}"
    teardown_quiet
    log "Levantando infraestructura base (consumers=$consumers)..."
    $COMPOSE up -d kafka redis response_generator metrics
    wait_ready
    $COMPOSE up -d --scale consumer="$consumers" consumer
    sleep 5
    reset_metrics
}

run_producer() {
    local qps="${1:-10}"
    local duration="${2:-60}"
    local dist="${3:-zipf}"
    log "Producer: ${qps} qps | ${duration}s | distribución=${dist}"
    $COMPOSE run --rm \
        -e QUERIES_PER_SECOND="$qps" \
        -e DURATION_SECONDS="$duration" \
        -e DISTRIBUTION="$dist" \
        producer
}

# ── Escenario 1: Kafka + 1 consumidor ────────────────────────────────────────
scenario_kafka1() {
    log "ESCENARIO: Kafka + 1 Consumidor"
    bring_up_base 1
    run_producer 10 60 zipf
    log "Métricas finales — Kafka + 1 Consumer:"
    show_metrics
    save_metrics "kafka1"
}

# ── Escenario 2: Kafka + múltiples consumidores ───────────────────────────────
scenario_kafka_multi() {
    local n="${1:-3}"
    log "ESCENARIO: Kafka + ${n} Consumidores"
    bring_up_base "$n"
    run_producer 10 60 zipf
    log "Métricas finales — Kafka + ${n} Consumers:"
    show_metrics
    save_metrics "kafka_multi_${n}"
}

# ── Escenario 3: Falla temporal del Generador de Respuestas ──────────────────
scenario_failure() {
    log "ESCENARIO: Falla Temporal"
    bring_up_base 2

    log "Iniciando producer en background (90s, 10 qps)..."
    $COMPOSE run --rm -d \
        -e QUERIES_PER_SECOND=10 \
        -e DURATION_SECONDS=90 \
        -e DISTRIBUTION=zipf \
        producer

    log "Esperando 20s antes de simular fallo..."
    sleep 20
    log "*** DETENIENDO Generador de Respuestas (fallo simulado) ***"
    $COMPOSE stop response_generator

    log "Fallo activo por 20s — los consumers deben reintentar..."
    sleep 20
    log "*** RESTAURANDO Generador de Respuestas ***"
    $COMPOSE start response_generator

    log "Esperando recuperación (35s)..."
    sleep 35

    log "Métricas finales — Falla Temporal:"
    show_metrics
    save_metrics "failure"
}

# ── Escenario 4: Spike de tráfico ─────────────────────────────────────────────
scenario_spike() {
    log "ESCENARIO: Spike de Tráfico"
    bring_up_base 2

    log "Fase 1: tráfico normal (5 qps × 20s)..."
    run_producer 5 20 zipf

    log "Fase 2: SPIKE (50 qps × 15s)..."
    run_producer 50 15 uniform

    log "Fase 3: vuelta a normal (5 qps × 30s)..."
    run_producer 5 30 zipf

    log "Métricas finales — Spike de Tráfico:"
    show_metrics
    save_metrics "spike"
}

# ── Escenario 5: Reintentos con fallos aleatorios ─────────────────────────────
scenario_retry() {
    log "ESCENARIO: Reintentos con fallos aleatorios (30%)"
    bring_up_base 2

    log "Inyectando 30% de fallos aleatorios..."
    curl -s -X POST "http://localhost:8000/set_failure_rate?rate=0.3" > /dev/null
    ok "Failure rate = 30%"

    run_producer 10 60 zipf

    log "Restaurando failure rate a 0%..."
    curl -s -X POST "http://localhost:8000/set_failure_rate?rate=0.0" > /dev/null
    ok "Failure rate = 0%"

    log "Métricas finales — Reintentos:"
    show_metrics
    save_metrics "retry"
}

# ── Escenarios en secuencia ─────────────────────────────────────────
scenario_all() {
    log "EJECUTANDO TODOS LOS ESCENARIOS"

    scenario_kafka1
    teardown_quiet

    scenario_kafka_multi 2
    teardown_quiet

    scenario_kafka_multi 4
    teardown_quiet

    scenario_failure
    teardown_quiet

    scenario_retry
    teardown_quiet

    scenario_spike
    teardown_quiet

    log "Todos los escenarios completados."
    ok "Resultados guardados en results_*.json"
}

teardown() {
    log "Deteniendo todos los servicios..."
    $COMPOSE down -v --remove-orphans
    ok "Limpieza completada"
}

# ── Main ──────────────────────────────────────────────────────────────────────
case "${1:-help}" in
    kafka1)       scenario_kafka1 ;;
    kafka_multi)  scenario_kafka_multi "${2:-3}" ;;
    failure)      scenario_failure ;;
    spike)        scenario_spike ;;
    retry)        scenario_retry ;;
    all)          scenario_all ;;
    down)         teardown ;;
    *)
        echo ""
        echo "Uso: $0 <escenario> [opciones]"
        echo ""
        echo "  kafka1              Kafka + 1 consumidor (60s, 10 qps, Zipf)"
        echo "  kafka_multi [N]     Kafka + N consumidores (default: 3)"
        echo "  failure             Falla temporal del Generador de Respuestas"
        echo "  spike               Spike de tráfico (5→50→5 qps)"
        echo "  retry               Reintentos con 30% fallos aleatorios"
        echo "  all                 Todos los escenarios en secuencia"
        echo "  down                Detener y limpiar todos los servicios"
        echo ""
        ;;
esac
