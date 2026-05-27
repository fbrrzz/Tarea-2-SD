#!/usr/bin/env bash
# =============================================================================
# run_scenarios.sh — Ejecuta el escenario de Falla Temporal de la Tarea 2
# Uso: ./scripts/run_scenarios.sh [opción]
#   opciones: failure | down
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

# ── Escenario: Falla temporal del Generador de Respuestas ───────────────────
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

# ── Derribo limpio ────────────────────────────────────────────────────────────
teardown() {
    log "Deteniendo todos los servicios..."
    $COMPOSE down -v
    ok "Limpieza completada"
}

# ── Main ──────────────────────────────────────────────────────────────────────
case "${1:-help}" in
    failure)     scenario_failure ;;
    down)        teardown ;;
    *)
        echo "Uso: $0 {failure|down}"
        ;;
esac