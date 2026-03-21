#!/usr/bin/env bash
# =============================================================================
# run-experiments.sh — Unified experiment runner for Neural Pub/Sub.
#
# Supports all experiment phases (A through D), local and remote execution,
# auto-sync, and resume.  Replaces the per-phase scripts.
#
# Usage:
#   ./run-experiments.sh [--remote] COMMAND [OPTIONS]
#
# Commands:
#   smoke                Quick smoke test (~2 min)
#   phase-a [--resume]   Phase A: single-site baselines (60 runs)
#   phase-a5             Phase A.5: placement quality micro-benchmark
#   phase-a6 [--resume]  Phase A.6: resource contention (15 runs)
#   phase-b [--resume]   Phase B: slice-aware placement (20 runs)
#   phase-c [--resume]   Phase C: cross-site federation (20 runs, needs HOST_D2)
#   phase-d [--resume]   Phase D: failure resilience (20 runs)
#   single CONFIG RATE STAGES SEED   Single run with explicit parameters
#   stop                 Stop all containers
#   status               Show progress for all phases
#   sync                 Push code + rebuild on remote (no experiments)
#
# Options:
#   --remote             Execute on remote host (configured in .env.local)
#   --resume             Skip runs whose result CSV already exists
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"

# --- Compose file paths (relative to project root) --------------------------
COMPOSE_LOCAL="docker-compose.local.yaml"
COMPOSE_KAFKA="docker-compose.kafka.yaml"
COMPOSE_FLAT="docker-compose.flat.yaml"
COMPOSE_GOVERNANCE="docker-compose.governance.yaml"

# --- Logging helpers ---------------------------------------------------------
info()  { printf "\033[1;34m[run]\033[0m %s\n" "$*"; }
ok()    { printf "\033[1;32m[run]\033[0m %s\n" "$*"; }
warn()  { printf "\033[1;33m[run]\033[0m %s\n" "$*" >&2; }
err()   { printf "\033[1;31m[run]\033[0m %s\n" "$*" >&2; }

# --- Parse global flags ------------------------------------------------------
REMOTE_MODE=""
TARGET_HOST=""
REMOTE_DIR=""
GIT_REMOTE=""

while [[ "${1:-}" == --* ]]; do
    case "$1" in
        --remote)
            REMOTE_MODE=1
            shift
            ;;
        *)
            break
            ;;
    esac
done

ACTION="${1:-help}"
shift || true

# --- Load .env.local for remote configuration --------------------------------
if [[ -n "$REMOTE_MODE" ]]; then
    ENV_FILE="$PROJECT_DIR/.env.local"
    if [[ ! -f "$ENV_FILE" ]]; then
        err "Remote mode requires .env.local. Copy .env.local.example and edit."
        exit 1
    fi
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    TARGET_HOST="${HOST_D1:?HOST_D1 must be set in .env.local}"
    REMOTE_DIR="${HOST_D1_DIR:?HOST_D1_DIR must be set in .env.local}"
    GIT_REMOTE="${HOST_D1_GIT:?HOST_D1_GIT must be set in .env.local}"
    info "Remote mode: host=$TARGET_HOST dir=$REMOTE_DIR git=$GIT_REMOTE"
fi

# --- Remote command helper ---------------------------------------------------
# rcmd "command" — execute on remote host if --remote, else locally.
rcmd() {
    if [[ -n "$TARGET_HOST" ]]; then
        ssh "$TARGET_HOST" "cd $REMOTE_DIR && $*"
    else
        eval "$*"
    fi
}

# rcmd_host HOST DIR "command" — execute on a specific host.
rcmd_host() {
    local host="$1" dir="$2"
    shift 2
    ssh "$host" "cd $dir && $*"
}

# --- Auto-sync check ---------------------------------------------------------
# Ensures the remote repo matches local HEAD.  Pushes and rebuilds if needed.
sync_host() {
    local host="$1" dir="$2" git_remote="$3"
    local local_head remote_head

    # Check for uncommitted local changes
    if [[ -n "$(git -C "$PROJECT_DIR" status --porcelain)" ]]; then
        warn "Local repo has uncommitted changes. Commit or stash first."
        warn "Continuing anyway (remote may be out of date)."
    fi

    local_head="$(git -C "$PROJECT_DIR" rev-parse HEAD)"
    remote_head="$(ssh "$host" "cd $dir && git rev-parse HEAD" 2>/dev/null || echo "unknown")"

    if [[ "$local_head" == "$remote_head" ]]; then
        ok "Remote $host is up to date ($local_head)."
        return 0
    fi

    info "Local HEAD=$local_head, remote HEAD=$remote_head"
    info "Pushing to $git_remote and rebuilding on $host ..."

    git -C "$PROJECT_DIR" push "$git_remote" main 2>&1 | sed 's/^/  [git] /'
    ssh "$host" "cd $dir && git pull --ff-only && docker build -t neural-pubsub:latest ." 2>&1 | tail -5

    ok "Remote $host synced and rebuilt."
}

auto_sync() {
    if [[ -z "$REMOTE_MODE" ]]; then
        return 0
    fi
    sync_host "$TARGET_HOST" "$REMOTE_DIR" "$GIT_REMOTE"
}

# --- Rate label to numeric ---------------------------------------------------
rate_to_numeric() {
    case "$1" in
        low)    echo "2.0" ;;
        medium) echo "5.0" ;;
        high)   echo "10.0" ;;
        *)      echo "$1" ;;
    esac
}

# --- Compose file set for a config -------------------------------------------
# Returns the -f flags needed for a given experiment config.
compose_files_for() {
    local config="$1"
    case "$config" in
        A1)  echo "-f $COMPOSE_LOCAL -f $COMPOSE_KAFKA" ;;
        A2|A3|A4)
             echo "-f $COMPOSE_LOCAL" ;;
        B1)  echo "-f $COMPOSE_LOCAL -f $COMPOSE_FLAT" ;;
        B2)  echo "-f $COMPOSE_LOCAL" ;;
        B3)  echo "-f $COMPOSE_LOCAL -f $COMPOSE_GOVERNANCE" ;;
        B4)  echo "-f $COMPOSE_LOCAL -f $COMPOSE_GOVERNANCE" ;;
        C1|C2|C3|C4)
             echo "-f $COMPOSE_LOCAL" ;;
        D1|D2|D3|D4)
             echo "-f $COMPOSE_LOCAL" ;;
        *)   err "Unknown config: $config"; exit 1 ;;
    esac
}

# --- Broker module for a config -----------------------------------------------
broker_module_for() {
    local config="$1"
    case "$config" in
        A1)  echo "" ;;  # Kafka overlay hardcodes broker command
        A2)  echo "src.broker.static_broker" ;;
        A3)  echo "src.broker.static_broker" ;;  # random mode
        *)   echo "src.broker.neural_broker" ;;
    esac
}

# --- Single experiment run ----------------------------------------------------
# run_single CONFIG RATE STAGES SEED PHASE_DIR [EXTRA_ENV...]
#
# Runs one experiment: brings up compose, waits for workload to finish,
# collects metrics CSV, and tears down.
run_single() {
    local config="$1" rate="$2" stages="$3" seed="$4" phase_dir="$5"
    shift 5
    local extra_env=("$@")

    local arrival_rate
    arrival_rate="$(rate_to_numeric "$rate")"
    local compose_flags
    compose_flags="$(compose_files_for "$config")"
    local broker_module
    broker_module="$(broker_module_for "$config")"

    local outfile="${phase_dir}/${config}_rate-${rate}_stages-${stages}_seed-${seed}.csv"
    local logfile="logs/${config}_${rate}_${stages}_${seed}.log"

    info "Running: ${config} rate=${rate} stages=${stages} seed=${seed} ..."

    # Build env vars for compose
    local env_prefix=""
    env_prefix="ARRIVAL_RATE=$arrival_rate DURATION_S=60 SEED=$seed"
    env_prefix="$env_prefix RESULT_FILE=/app/results/metrics.csv"

    if [[ -n "$broker_module" ]]; then
        env_prefix="$env_prefix BROKER_MODULE=$broker_module"
    fi

    # Add any extra env vars (e.g., WARMUP_S)
    for ev in "${extra_env[@]}"; do
        env_prefix="$env_prefix $ev"
    done

    # Execute
    rcmd "$env_prefix docker compose $compose_flags up --abort-on-container-exit --timeout 120" \
        > "$PROJECT_DIR/$logfile" 2>&1 || true

    # Tear down
    rcmd "docker compose $compose_flags down --remove-orphans" 2>/dev/null || true

    # Collect results
    if rcmd "test -f results/metrics.csv" 2>/dev/null; then
        rcmd "cp results/metrics.csv '$outfile'"
        rcmd "rm -f results/metrics.csv" 2>/dev/null || rcmd "sudo rm -f results/metrics.csv" 2>/dev/null || true
        ok "Completed: ${outfile}"
        return 0
    else
        err "No metrics for ${config}/rate-${rate}/stages-${stages}/seed-${seed}"
        rcmd "touch '${phase_dir}/${config}_rate-${rate}_stages-${stages}_seed-${seed}.FAILED'"
        return 1
    fi
}

# --- Ensure directories exist ------------------------------------------------
ensure_dirs() {
    local phase_dir="$1"
    rcmd "mkdir -p '$phase_dir' logs 2>/dev/null || true"
    # Fix ownership if Docker created dirs as root
    rcmd "if [ -d results ] && [ \"\$(stat -c '%U' results 2>/dev/null)\" = 'root' ]; then sudo chown -R \$(whoami) results logs 2>/dev/null || true; fi" || true
}

# --- Phase runner (generic matrix) -------------------------------------------
# run_phase PHASE_NAME PHASE_DIR CONFIGS RATES STAGES SEEDS [EXTRA_ENV...]
run_phase() {
    local phase_name="$1" phase_dir="$2" configs="$3" rates="$4"
    local stages_list="$5" seeds="$6"
    shift 6
    local extra_env=("$@")

    local resume_flag=""
    # Check for --resume in remaining args (already shifted by caller)
    # Resume is handled by the caller passing it to this function.

    ensure_dirs "$phase_dir"

    # Count total runs
    local n_configs n_rates n_stages n_seeds total
    n_configs=$(echo "$configs" | tr ',' ' ' | wc -w | tr -d ' ')
    n_rates=$(echo "$rates" | tr ',' ' ' | wc -w | tr -d ' ')
    n_stages=$(echo "$stages_list" | tr ',' ' ' | wc -w | tr -d ' ')
    n_seeds=$(echo "$seeds" | tr ',' ' ' | wc -w | tr -d ' ')
    total=$((n_configs * n_rates * n_stages * n_seeds))

    info "=== ${phase_name} ==="
    info "Matrix: configs=${configs} rates=${rates} stages=${stages_list} seeds=${seeds}"
    info "Total runs: ${total}"

    local done_count=0 skip_count=0 fail_count=0

    for config in ${configs//,/ }; do
        for rate in ${rates//,/ }; do
            for stg in ${stages_list//,/ }; do
                for seed in ${seeds//,/ }; do
                    local outfile="${phase_dir}/${config}_rate-${rate}_stages-${stg}_seed-${seed}.csv"

                    # Resume: skip if result already exists
                    if [[ -n "$RESUME" ]] && rcmd "test -f '$outfile'" 2>/dev/null; then
                        info "Skipping ${config}/rate-${rate}/stages-${stg}/seed-${seed} (already done)"
                        skip_count=$((skip_count + 1))
                        continue
                    fi

                    if run_single "$config" "$rate" "$stg" "$seed" "$phase_dir" "${extra_env[@]}"; then
                        done_count=$((done_count + 1))
                    else
                        fail_count=$((fail_count + 1))
                    fi
                done
            done
        done
    done

    ok "=== ${phase_name} complete: ${done_count} succeeded, ${fail_count} failed, ${skip_count} skipped ==="
}

# --- tmux wrapper for long phases --------------------------------------------
# If not already inside tmux and running a phase command, wrap in tmux.
maybe_tmux_wrap() {
    local session_name="$1"
    shift
    local full_cmd="$*"

    if [[ -z "${TMUX:-}" ]]; then
        if [[ -n "$REMOTE_MODE" ]]; then
            # Create tmux session on remote host
            info "Creating tmux session '$session_name' on $TARGET_HOST ..."
            ssh "$TARGET_HOST" "tmux new-session -d -s '$session_name' 'cd $REMOTE_DIR && $full_cmd'" 2>/dev/null || true
            info "Session created. Attach with: ssh $TARGET_HOST -t 'tmux attach -t $session_name'"
            return 0
        else
            # Local tmux
            if tmux has-session -t "$session_name" 2>/dev/null; then
                info "tmux session '$session_name' already exists."
                info "Attach: tmux attach -t $session_name"
                info "Or kill: tmux kill-session -t $session_name"
                exit 1
            fi
            info "Launching inside tmux session '$session_name' ..."
            tmux new-session -d -s "$session_name" "$full_cmd"
            sleep 1
            tmux attach -t "$session_name"
            exit 0
        fi
    fi
    # Already inside tmux — just continue (the phase function will run)
    return 1
}

# --- Parse --resume from remaining args --------------------------------------
RESUME=""
remaining_args=()
for arg in "$@"; do
    case "$arg" in
        --resume) RESUME=1 ;;
        *)        remaining_args+=("$arg") ;;
    esac
done
set -- "${remaining_args[@]+"${remaining_args[@]}"}"


# =============================================================================
# Command dispatch
# =============================================================================

case "$ACTION" in

# -----------------------------------------------------------------------------
# Smoke test
# -----------------------------------------------------------------------------
smoke)
    auto_sync
    ensure_dirs "results/smoke"

    info "=== Smoke Test ==="

    # Build image
    rcmd "docker compose -f $COMPOSE_LOCAL build" 2>/dev/null || true

    # A4 (neural) quick run
    info "Running A4 (neural) smoke: rate=2.0, 30s ..."
    rcmd "ARRIVAL_RATE=2.0 DURATION_S=30 SEED=42 docker compose -f $COMPOSE_LOCAL up --abort-on-container-exit --timeout 60" 2>&1 | tail -20
    rcmd "docker compose -f $COMPOSE_LOCAL down --remove-orphans" 2>/dev/null || true

    if rcmd "test -f results/metrics.csv" 2>/dev/null; then
        ok "A4 smoke passed."
        rcmd "cp results/metrics.csv 'results/smoke/A4_smoke_\$(date +%Y%m%d_%H%M%S).csv'"
        rcmd "rm -f results/metrics.csv" 2>/dev/null || true
    else
        err "A4 smoke failed: no metrics.csv"
        exit 1
    fi

    # A1 (Kafka) quick run
    info "Running A1 (Kafka) smoke: rate=2.0, 30s ..."
    rcmd "ARRIVAL_RATE=2.0 DURATION_S=30 SEED=42 docker compose -f $COMPOSE_LOCAL -f $COMPOSE_KAFKA up --abort-on-container-exit --timeout 60" 2>&1 | tail -20
    rcmd "docker compose -f $COMPOSE_LOCAL -f $COMPOSE_KAFKA down --remove-orphans" 2>/dev/null || true

    if rcmd "test -f results/metrics.csv" 2>/dev/null; then
        ok "A1 (Kafka) smoke passed."
        rcmd "cp results/metrics.csv 'results/smoke/A1_kafka_smoke_\$(date +%Y%m%d_%H%M%S).csv'"
        rcmd "rm -f results/metrics.csv" 2>/dev/null || true
    else
        warn "A1 (Kafka) smoke: no metrics.csv (may need debugging)."
    fi

    # B2 (slice-aware, base) quick run
    info "Running B2 (slice-aware base) smoke: rate=2.0, 30s ..."
    rcmd "ARRIVAL_RATE=2.0 DURATION_S=30 SEED=42 docker compose -f $COMPOSE_LOCAL up --abort-on-container-exit --timeout 60" 2>&1 | tail -20
    rcmd "docker compose -f $COMPOSE_LOCAL down --remove-orphans" 2>/dev/null || true

    if rcmd "test -f results/metrics.csv" 2>/dev/null; then
        ok "B2 smoke passed."
        rcmd "cp results/metrics.csv 'results/smoke/B2_smoke_\$(date +%Y%m%d_%H%M%S).csv'"
        rcmd "rm -f results/metrics.csv" 2>/dev/null || true
    else
        warn "B2 smoke: no metrics.csv."
    fi

    ok "=== Smoke test complete ==="
    ;;

# -----------------------------------------------------------------------------
# Phase A: Single-site baselines
# -----------------------------------------------------------------------------
phase-a)
    auto_sync

    PHASE_SESSION="npubsub-phase-a"
    if maybe_tmux_wrap "$PHASE_SESSION" "$0 ${REMOTE_MODE:+--remote} phase-a ${RESUME:+--resume}"; then
        exit 0
    fi

    # A1=Kafka, A2=static, A3=random, A4=neural
    # 4 configs x 3 rates x 1 complexity x 5 seeds = 60 runs
    run_phase "Phase A: Single-site Baselines" \
        "results/phase_a" \
        "A1,A2,A3,A4" \
        "low,medium,high" \
        "3" \
        "42,123,456,789,0"
    ;;

# -----------------------------------------------------------------------------
# Phase A.5: Placement quality micro-benchmark
# -----------------------------------------------------------------------------
phase-a5)
    auto_sync

    info "=== Phase A.5: Placement Quality ==="
    info "Running placement quality micro-benchmark (pure computation, no Docker) ..."

    rcmd "python3 scripts/run_phase_a5_a6.py --phase a5"

    ok "=== Phase A.5 complete ==="
    ;;

# -----------------------------------------------------------------------------
# Phase A.6: Resource contention
# -----------------------------------------------------------------------------
phase-a6)
    auto_sync

    PHASE_SESSION="npubsub-phase-a6"
    if maybe_tmux_wrap "$PHASE_SESSION" "$0 ${REMOTE_MODE:+--remote} phase-a6 ${RESUME:+--resume}"; then
        exit 0
    fi

    info "=== Phase A.6: Resource Contention ==="
    info "Delegating to scripts/run_phase_a5_a6.py ..."

    local_resume_flag=""
    if [[ -n "$RESUME" ]]; then
        local_resume_flag="--resume"
    fi

    rcmd "python3 scripts/run_phase_a5_a6.py --phase a6 $local_resume_flag"

    ok "=== Phase A.6 complete ==="
    ;;

# -----------------------------------------------------------------------------
# Phase B: Slice-aware placement
# -----------------------------------------------------------------------------
phase-b)
    auto_sync

    PHASE_SESSION="npubsub-phase-b"
    if maybe_tmux_wrap "$PHASE_SESSION" "$0 ${REMOTE_MODE:+--remote} phase-b ${RESUME:+--resume}"; then
        exit 0
    fi

    # B1=flat (no slices), B2=base (slice-aware), B3=governance, B4=governance+failure
    # 4 configs x 1 rate (medium) x 1 complexity x 5 seeds = 20 runs
    # B4 gets failure injection (handled below after the matrix run)
    run_phase "Phase B: Slice-aware Placement" \
        "results/phase_b" \
        "B1,B2,B3,B4" \
        "medium" \
        "3" \
        "42,123,456,789,0" \
        "WARMUP_S=30"
    ;;

# -----------------------------------------------------------------------------
# Phase C: Cross-site federation
# -----------------------------------------------------------------------------
phase-c)
    # Validate HOST_D2 configuration
    if [[ -z "${HOST_D2:-}" ]]; then
        err "Phase C requires HOST_D2 to be set in .env.local"
        err "Configure HOST_D2, HOST_D2_DIR, and HOST_D2_GIT for the second domain."
        exit 1
    fi
    HOST_D2_DIR="${HOST_D2_DIR:?HOST_D2_DIR must be set in .env.local}"
    HOST_D2_GIT="${HOST_D2_GIT:?HOST_D2_GIT must be set in .env.local}"

    auto_sync
    # Also sync D2
    if [[ -n "$REMOTE_MODE" ]]; then
        sync_host "$HOST_D2" "$HOST_D2_DIR" "$HOST_D2_GIT"
    fi

    PHASE_SESSION="npubsub-phase-c"
    if maybe_tmux_wrap "$PHASE_SESSION" "$0 ${REMOTE_MODE:+--remote} phase-c ${RESUME:+--resume}"; then
        exit 0
    fi

    info "=== Phase C: Cross-site Federation ==="
    info "D1=$TARGET_HOST, D2=$HOST_D2"

    PHASE_DIR="results/phase_c"
    ensure_dirs "$PHASE_DIR"

    # C1-C4 configs: cross-site variants
    # C1=basic federation, C2=optimised routing, C3=governance, C4=governance+failure
    CONFIGS="C1,C2,C3,C4"
    RATES="medium"
    SEEDS="42,123,456,789,0"

    for config in ${CONFIGS//,/ }; do
        for seed in ${SEEDS//,/ }; do
            OUTFILE="${PHASE_DIR}/${config}_rate-medium_stages-3_seed-${seed}.csv"

            if [[ -n "$RESUME" ]] && rcmd "test -f '$OUTFILE'" 2>/dev/null; then
                info "Skipping ${config}/seed-${seed} (already done)"
                continue
            fi

            info "Running: ${config} seed=${seed} (cross-site) ..."

            # Start broker+workers on D1
            COMPOSE_D1="$(compose_files_for "$config")"
            rcmd "ARRIVAL_RATE=5.0 DURATION_S=60 SEED=$seed WARMUP_S=30 RESULT_FILE=/app/results/metrics.csv docker compose $COMPOSE_D1 up -d" 2>/dev/null

            # Start broker+workers on D2
            rcmd_host "$HOST_D2" "$HOST_D2_DIR" \
                "ARRIVAL_RATE=5.0 DURATION_S=60 SEED=$seed docker compose -f docker-compose.local.yaml up -d" 2>/dev/null

            # Wait for workload to complete (check periodically)
            info "Waiting for workload to finish ..."
            for i in $(seq 1 60); do
                sleep 10
                if ! rcmd "docker ps --format '{{.Names}}' | grep -q workload" 2>/dev/null; then
                    break
                fi
            done

            # Tear down both domains
            rcmd "docker compose $COMPOSE_D1 down --remove-orphans" 2>/dev/null || true
            rcmd_host "$HOST_D2" "$HOST_D2_DIR" \
                "docker compose -f docker-compose.local.yaml down --remove-orphans" 2>/dev/null || true

            # Collect results from D1
            if rcmd "test -f results/metrics.csv" 2>/dev/null; then
                rcmd "cp results/metrics.csv '$OUTFILE'"
                rcmd "rm -f results/metrics.csv" 2>/dev/null || true
                ok "Completed: $OUTFILE"
            else
                err "No metrics for ${config}/seed-${seed}"
                rcmd "touch '${PHASE_DIR}/${config}_rate-medium_stages-3_seed-${seed}.FAILED'"
            fi
        done
    done

    ok "=== Phase C complete ==="
    ;;

# -----------------------------------------------------------------------------
# Phase D: Failure resilience
# -----------------------------------------------------------------------------
phase-d)
    auto_sync

    PHASE_SESSION="npubsub-phase-d"
    if maybe_tmux_wrap "$PHASE_SESSION" "$0 ${REMOTE_MODE:+--remote} phase-d ${RESUME:+--resume}"; then
        exit 0
    fi

    info "=== Phase D: Failure Resilience ==="

    PHASE_DIR="results/phase_d"
    ensure_dirs "$PHASE_DIR"

    # D1=broker restart, D2=worker failure, D3=network partition, D4=cascading
    CONFIGS="D1,D2,D3,D4"
    RATES="medium"
    SEEDS="42,123,456,789,0"

    for config in ${CONFIGS//,/ }; do
        for seed in ${SEEDS//,/ }; do
            OUTFILE="${PHASE_DIR}/${config}_rate-medium_stages-3_seed-${seed}.csv"

            if [[ -n "$RESUME" ]] && rcmd "test -f '$OUTFILE'" 2>/dev/null; then
                info "Skipping ${config}/seed-${seed} (already done)"
                continue
            fi

            info "Running: ${config} seed=${seed} (failure resilience) ..."

            # Start the system
            COMPOSE_FLAGS="-f $COMPOSE_LOCAL"
            rcmd "ARRIVAL_RATE=5.0 DURATION_S=120 SEED=$seed WARMUP_S=30 RESULT_FILE=/app/results/metrics.csv docker compose $COMPOSE_FLAGS up -d"

            # Wait for warmup (30s)
            sleep 35

            # Inject failure based on config
            case "$config" in
                D1)
                    info "Injecting failure: broker restart ..."
                    rcmd "docker compose $COMPOSE_FLAGS restart broker-d1" || true
                    ;;
                D2)
                    info "Injecting failure: killing worker-d1-urllc-1 ..."
                    rcmd "docker compose $COMPOSE_FLAGS kill worker-d1-urllc-1" || true
                    sleep 30
                    info "Restarting worker-d1-urllc-1 ..."
                    rcmd "docker compose $COMPOSE_FLAGS start worker-d1-urllc-1" || true
                    ;;
                D3)
                    info "Injecting failure: network partition (disconnecting worker) ..."
                    rcmd "docker network disconnect \$(docker network ls --filter name=slice-URLLC -q | head -1) \$(docker ps --filter name=worker-d1-urllc-1 -q | head -1)" || true
                    sleep 30
                    info "Healing partition ..."
                    rcmd "docker network connect \$(docker network ls --filter name=slice-URLLC -q | head -1) \$(docker ps --filter name=worker-d1-urllc-1 -q | head -1)" || true
                    ;;
                D4)
                    info "Injecting failure: cascading (broker restart + worker kill) ..."
                    rcmd "docker compose $COMPOSE_FLAGS restart broker-d1" || true
                    sleep 10
                    rcmd "docker compose $COMPOSE_FLAGS kill worker-d1-urllc-1" || true
                    sleep 30
                    rcmd "docker compose $COMPOSE_FLAGS start worker-d1-urllc-1" || true
                    ;;
            esac

            # Wait for workload to complete
            info "Waiting for workload to finish ..."
            for i in $(seq 1 30); do
                sleep 10
                if ! rcmd "docker ps --format '{{.Names}}' | grep -q workload" 2>/dev/null; then
                    break
                fi
            done

            # Tear down
            rcmd "docker compose $COMPOSE_FLAGS down --remove-orphans" 2>/dev/null || true

            # Collect results
            if rcmd "test -f results/metrics.csv" 2>/dev/null; then
                rcmd "cp results/metrics.csv '$OUTFILE'"
                rcmd "rm -f results/metrics.csv" 2>/dev/null || true
                ok "Completed: $OUTFILE"
            else
                err "No metrics for ${config}/seed-${seed}"
                rcmd "touch '${PHASE_DIR}/${config}_rate-medium_stages-3_seed-${seed}.FAILED'"
            fi
        done
    done

    ok "=== Phase D complete ==="
    ;;

# -----------------------------------------------------------------------------
# Single run
# -----------------------------------------------------------------------------
single)
    CONFIG="${1:?Usage: $0 single CONFIG RATE STAGES SEED}"
    RATE="${2:?}"
    STAGES="${3:?}"
    SEED="${4:?}"

    auto_sync

    # Determine output directory from config prefix
    case "$CONFIG" in
        A*)  PHASE_DIR="results/phase_a" ;;
        B*)  PHASE_DIR="results/phase_b" ;;
        C*)  PHASE_DIR="results/phase_c" ;;
        D*)  PHASE_DIR="results/phase_d" ;;
        *)   PHASE_DIR="results/misc" ;;
    esac

    ensure_dirs "$PHASE_DIR"
    run_single "$CONFIG" "$RATE" "$STAGES" "$SEED" "$PHASE_DIR"
    ;;

# -----------------------------------------------------------------------------
# Stop
# -----------------------------------------------------------------------------
stop)
    info "Stopping all containers ..."
    rcmd "docker compose -f $COMPOSE_LOCAL down --remove-orphans" 2>/dev/null || true
    rcmd "docker compose -f $COMPOSE_LOCAL -f $COMPOSE_KAFKA down --remove-orphans" 2>/dev/null || true
    rcmd "docker compose -f $COMPOSE_LOCAL -f $COMPOSE_FLAT down --remove-orphans" 2>/dev/null || true
    rcmd "docker compose -f $COMPOSE_LOCAL -f $COMPOSE_GOVERNANCE down --remove-orphans" 2>/dev/null || true
    ok "All stopped."
    ;;

# -----------------------------------------------------------------------------
# Status
# -----------------------------------------------------------------------------
status)
    echo "=== Running containers ==="
    rcmd "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'" 2>/dev/null || echo "(none)"
    echo ""

    for phase in phase_a phase_b phase_c phase_d; do
        dir="results/$phase"
        if rcmd "test -d '$dir'" 2>/dev/null; then
            done_count=$(rcmd "ls '$dir'/*.csv 2>/dev/null | grep -cv -E 'partial|smoke'" 2>/dev/null || echo "0")
            failed_count=$(rcmd "ls '$dir'/*.FAILED 2>/dev/null | wc -l" 2>/dev/null || echo "0")
            echo "=== ${phase} ==="
            echo "  Completed: ${done_count}"
            echo "  Failed:    ${failed_count}"
            echo ""
        fi
    done
    ;;

# -----------------------------------------------------------------------------
# Sync (push + rebuild, no experiments)
# -----------------------------------------------------------------------------
sync)
    if [[ -z "$REMOTE_MODE" ]]; then
        err "sync requires --remote flag."
        exit 1
    fi

    sync_host "$TARGET_HOST" "$REMOTE_DIR" "$GIT_REMOTE"

    # Also sync D2 if configured
    if [[ -n "${HOST_D2:-}" ]]; then
        sync_host "$HOST_D2" "${HOST_D2_DIR}" "${HOST_D2_GIT}"
    fi

    ok "All remote hosts synced."
    ;;

# -----------------------------------------------------------------------------
# Help
# -----------------------------------------------------------------------------
*)
    cat <<'HELP'
Usage: ./run-experiments.sh [--remote] COMMAND [OPTIONS]

Commands:
  smoke                Quick smoke test (~2 min)
  phase-a [--resume]   Phase A: single-site baselines (60 runs)
  phase-a5             Phase A.5: placement quality micro-benchmark
  phase-a6 [--resume]  Phase A.6: resource contention (15 runs)
  phase-b [--resume]   Phase B: slice-aware placement (20 runs)
  phase-c [--resume]   Phase C: cross-site federation (needs HOST_D2)
  phase-d [--resume]   Phase D: failure resilience (20 runs)
  single CONFIG RATE STAGES SEED   Single run
  stop                 Stop all containers
  status               Show progress for all phases
  sync                 Push code + rebuild on remote (no experiments)

Options:
  --remote             Execute on remote host (from .env.local)
  --resume             Skip runs whose result CSV already exists

Examples:
  ./run-experiments.sh smoke
  ./run-experiments.sh single A2 medium 3 42
  ./run-experiments.sh --remote phase-b --resume
  ./run-experiments.sh --remote sync
HELP
    ;;

esac
