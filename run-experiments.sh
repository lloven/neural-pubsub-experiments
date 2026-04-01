#!/usr/bin/env bash
# =============================================================================
# run-experiments.sh -- Thin dispatcher for Neural Pub/Sub experiments.
#
# All experiment logic lives in Python (scripts/run_phase_*.py,
# scripts/run_single.py, scripts/monitor.py).  This script handles:
#   1. --remote detection and .env.local loading
#   2. Auto-sync (git push + docker rebuild)
#   3. tmux wrapping (local or remote)
#   4. Dispatching each command to the corresponding Python script
#
# Usage:
#   ./run-experiments.sh [--remote] COMMAND [OPTIONS]
#
# Commands:
#   smoke                   Quick smoke test (~2 min)
#   baseline [--resume]     Single-site baselines (was Phase A)
#   placement               Placement quality micro-benchmark (was Phase A.5)
#   contention [--resume]   Resource contention (15 runs, was Phase A.6)
#   slicing [--resume]      Slice-aware placement (was Phase B)
#   federation [--resume]   Cross-site federation (needs HOST_D2, was Phase C)
#   resilience [--resume]   Failure resilience (was Phase D)
#   stress [--resume]       Combined contention + failure (60 runs, was Phase E)
#   single CONFIG RATE STAGES SEED   Single run with explicit parameters
#   stop                    Stop all containers
#   status                  Show progress for all phases
#   sync                    Push code + rebuild on remote (no experiments)
#
# Options:
#   --remote             Execute on remote host (configured in .env.local)
#   --resume             Skip runs whose result CSV already exists
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"

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
rcmd() {
    if [[ -n "$TARGET_HOST" ]]; then
        ssh "$TARGET_HOST" "cd ~/$REMOTE_DIR && source ~/.venv/bin/activate 2>/dev/null; $*"
    else
        eval "$*"
    fi
}

# --- Auto-sync ---------------------------------------------------------------
sync_host() {
    local host="$1" dir="$2" git_remote="$3"
    local local_head remote_head

    if ! git -C "$PROJECT_DIR" diff --quiet HEAD 2>/dev/null || [[ -n "$(git -C "$PROJECT_DIR" diff --cached --name-only)" ]]; then
        warn "Local repo has uncommitted changes. Commit or stash first."
        warn "Continuing anyway (remote may be out of date)."
    fi

    local_head="$(git -C "$PROJECT_DIR" rev-parse HEAD)"
    remote_head="$(ssh "$host" "cd ~/$dir && git rev-parse HEAD" 2>/dev/null || echo "unknown")"

    if [[ "$local_head" == "$remote_head" ]]; then
        ok "Remote $host is up to date ($local_head)."
        return 0
    fi

    info "Local HEAD=$local_head, remote HEAD=$remote_head"
    info "Pushing to $git_remote and rebuilding on $host ..."

    git -C "$PROJECT_DIR" push "$git_remote" main 2>&1 | sed 's/^/  [git] /'
    ssh "$host" "cd ~/$dir && git pull --ff-only && docker build -t neural-pubsub:latest ." 2>&1 | tail -5

    ok "Remote $host synced and rebuilt."
}

auto_sync() {
    if [[ -z "$REMOTE_MODE" ]]; then
        return 0
    fi
    sync_host "$TARGET_HOST" "$REMOTE_DIR" "$GIT_REMOTE"
}

# --- tmux wrapper for long phases --------------------------------------------
maybe_tmux_wrap() {
    local session_name="$1"
    shift
    local full_cmd="$*"

    # Skip tmux wrapping for dry runs
    if [[ -n "$DRY_RUN" ]]; then
        return 1
    fi

    if [[ -z "${TMUX:-}" ]]; then
        if [[ -n "$REMOTE_MODE" ]]; then
            info "Creating tmux session '$session_name' on $TARGET_HOST ..."
            ssh "$TARGET_HOST" "tmux new-session -d -s '$session_name' 'cd ~/$REMOTE_DIR && source ~/.venv/bin/activate 2>/dev/null; $full_cmd'" 2>/dev/null || true
            info "Session created. Attach with: ssh $TARGET_HOST -t 'tmux attach -t $session_name'"
            return 0
        else
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
    # Already inside tmux -- just continue
    return 1
}

# --- Parse flags from remaining args ------------------------------------------
RESUME=""
DRY_RUN=""
CONFIGS_ARG=""
SEEDS_ARG=""
STRATEGY_ARG=""
WARMUP_ARG=""
MEASUREMENT_ARG=""
FAILURE_DELAY_ARG=""
remaining_args=()
# Parse two-token flags (--configs VALUE, --seeds VALUE, etc.)
while [[ $# -gt 0 ]]; do
    case "$1" in
        --resume)      RESUME=1; shift ;;
        --dry-run)     DRY_RUN=1; shift ;;
        --configs)     CONFIGS_ARG="$2"; shift 2 ;;
        --seeds)       SEEDS_ARG="$2"; shift 2 ;;
        --strategy)    STRATEGY_ARG="$2"; shift 2 ;;
        --warmup)      WARMUP_ARG="$2"; shift 2 ;;
        --measurement)    MEASUREMENT_ARG="$2"; shift 2 ;;
        --failure-delay) FAILURE_DELAY_ARG="$2"; shift 2 ;;
        *)               remaining_args+=("$1"); shift ;;
    esac
done

# Helper: build Python flags
py_resume()      { [[ -n "$RESUME" ]]          && echo "--resume"                  || true; }
py_dry_run()     { [[ -n "$DRY_RUN" ]]         && echo "--dry-run"                 || true; }
py_configs()     { [[ -n "$CONFIGS_ARG" ]]      && echo "--configs $CONFIGS_ARG"    || true; }
py_seeds()       { [[ -n "$SEEDS_ARG" ]]        && echo "--seeds $SEEDS_ARG"        || true; }
py_strategy()    { [[ -n "$STRATEGY_ARG" ]]     && echo "--strategy $STRATEGY_ARG"  || true; }
py_warmup()      { [[ -n "$WARMUP_ARG" ]]       && echo "--warmup $WARMUP_ARG"      || true; }
py_measurement()    { [[ -n "$MEASUREMENT_ARG" ]]   && echo "--measurement $MEASUREMENT_ARG"       || true; }
py_failure_delay()  { [[ -n "$FAILURE_DELAY_ARG" ]] && echo "--failure-delay $FAILURE_DELAY_ARG"   || true; }

# =============================================================================
# Command dispatch -- delegates all experiment logic to Python
# =============================================================================

case "$ACTION" in

# --- Smoke test --------------------------------------------------------------
smoke)
    auto_sync
    # Pass --phases flag if user specified specific phases
    SMOKE_ARGS=""
    for arg in "${remaining_args[@]+"${remaining_args[@]}"}"; do
        SMOKE_ARGS="$SMOKE_ARGS $arg"
    done
    rcmd "python3 -m scripts.run_smoke_test $SMOKE_ARGS"
    ;;

# --- Baseline (was Phase A) --------------------------------------------------
baseline|phase-a)
    auto_sync
    PHASE_SESSION="npubsub-baseline"
    PY_CMD="python3 -m scripts.run_baseline $(py_resume) $(py_dry_run) $(py_configs) $(py_seeds)"
    if maybe_tmux_wrap "$PHASE_SESSION" "$PY_CMD"; then
        exit 0
    fi
    rcmd "$PY_CMD"
    ;;

# --- Placement (was Phase A.5) -----------------------------------------------
placement|phase-a5)
    auto_sync
    rcmd "python3 -m scripts.run_placement $(py_dry_run)"
    ;;

# --- Contention (was Phase A.6) ----------------------------------------------
contention|phase-a6)
    auto_sync
    PHASE_SESSION="npubsub-contention"
    PY_CMD="python3 -m scripts.run_contention $(py_resume) $(py_dry_run) $(py_configs) $(py_seeds)"
    if maybe_tmux_wrap "$PHASE_SESSION" "$PY_CMD"; then
        exit 0
    fi
    rcmd "$PY_CMD"
    ;;

# --- Slicing (was Phase B) ---------------------------------------------------
slicing|phase-b)
    auto_sync
    PHASE_SESSION="npubsub-slicing"
    PY_CMD="python3 -m scripts.run_slicing $(py_resume) $(py_dry_run) $(py_configs) $(py_seeds)"
    if maybe_tmux_wrap "$PHASE_SESSION" "$PY_CMD"; then
        exit 0
    fi
    rcmd "$PY_CMD"
    ;;

# --- Federation (was Phase C) ------------------------------------------------
federation|phase-c)
    if [[ -z "${HOST_D2:-}" ]]; then
        err "Federation requires HOST_D2 to be set in .env.local"
        exit 1
    fi
    auto_sync
    if [[ -n "$REMOTE_MODE" ]]; then
        sync_host "$HOST_D2" "${HOST_D2_DIR:?}" "${HOST_D2_GIT:?}"
    fi
    PHASE_SESSION="npubsub-federation"
    PY_CMD="python3 -m scripts.run_federation $(py_resume) $(py_dry_run) $(py_configs) $(py_seeds)"
    if maybe_tmux_wrap "$PHASE_SESSION" "$PY_CMD"; then
        exit 0
    fi
    rcmd "$PY_CMD"
    ;;

# --- Resilience (was Phase D) ------------------------------------------------
resilience|phase-d)
    auto_sync
    PHASE_SESSION="npubsub-resilience"
    PY_CMD="python3 -m scripts.run_resilience $(py_resume) $(py_dry_run) $(py_configs) $(py_seeds) $(py_strategy) $(py_warmup) $(py_measurement) $(py_failure_delay)"
    if maybe_tmux_wrap "$PHASE_SESSION" "$PY_CMD"; then
        exit 0
    fi
    rcmd "$PY_CMD"
    ;;

# --- Stress (was Phase E) ----------------------------------------------------
stress|phase-e)
    auto_sync
    PHASE_SESSION="npubsub-stress"
    PY_CMD="python3 -m scripts.run_stress $(py_resume) $(py_dry_run) $(py_configs) $(py_seeds) $(py_warmup) $(py_measurement) $(py_failure_delay)"
    if maybe_tmux_wrap "$PHASE_SESSION" "$PY_CMD"; then
        exit 0
    fi
    rcmd "$PY_CMD"
    ;;

# --- Single run --------------------------------------------------------------
single)
    CONFIG="${remaining_args[0]:?Usage: $0 single CONFIG RATE STAGES SEED}"
    RATE="${remaining_args[1]:?}"
    STAGES="${remaining_args[2]:?}"
    SEED="${remaining_args[3]:?}"

    auto_sync
    rcmd "python3 -m scripts.run_single $CONFIG $RATE $STAGES $SEED"
    ;;

# --- Stop all containers -----------------------------------------------------
stop)
    info "Stopping all containers ..."
    # Stop local Docker Compose stacks
    rcmd "docker compose -f docker-compose.local.yaml down --remove-orphans" 2>/dev/null || true
    rcmd "docker compose -f docker-compose.local.yaml -f docker-compose.kafka.yaml down --remove-orphans" 2>/dev/null || true
    rcmd "docker compose -f docker-compose.local.yaml -f docker-compose.flat.yaml down --remove-orphans" 2>/dev/null || true
    rcmd "docker compose -f docker-compose.local.yaml -f docker-compose.governance.yaml down --remove-orphans" 2>/dev/null || true
    # Stop distributed 4-VM cluster
    python3 -m scripts.multi_vm_runner --stop 2>/dev/null || true
    ok "All stopped."
    ;;

# --- Restart (stop + resume) -------------------------------------------------
restart)
    info "Restarting: stopping all containers, then resuming experiments ..."
    # Stop all containers (local + distributed)
    rcmd "docker compose -f docker-compose.local.yaml down --remove-orphans" 2>/dev/null || true
    python3 -m scripts.multi_vm_runner --stop 2>/dev/null || true
    ok "Containers stopped. Re-launching with --resume ..."
    # Re-run all phases with --resume to skip completed runs
    for phase in baseline contention slicing federation resilience stress; do
        info "Resuming $phase ..."
        PY_CMD="python3 -m scripts.run_$phase --topology distributed --resume $(py_warmup) $(py_measurement)"
        rcmd "$PY_CMD" || warn "$phase failed or had errors"
    done
    ok "All phases resumed."
    ;;

# --- Status ------------------------------------------------------------------
status)
    remote_flag=""
    if [[ -n "$REMOTE_MODE" ]]; then
        remote_flag="--remote $TARGET_HOST"
    fi
    python3 -m scripts.monitor --all --distributed --once $remote_flag
    ;;

# --- Sync (push + rebuild, no experiments) -----------------------------------
sync)
    if [[ -z "$REMOTE_MODE" ]]; then
        err "sync requires --remote flag."
        exit 1
    fi
    sync_host "$TARGET_HOST" "$REMOTE_DIR" "$GIT_REMOTE"
    if [[ -n "${HOST_D2:-}" ]]; then
        sync_host "$HOST_D2" "${HOST_D2_DIR}" "${HOST_D2_GIT}"
    fi
    ok "All remote hosts synced."
    ;;

# --- Help --------------------------------------------------------------------
*)
    cat <<'HELP'
Usage: ./run-experiments.sh [--remote] COMMAND [OPTIONS]

Commands:
  smoke                   Quick smoke test (~2 min)
  baseline [--resume]     Single-site baselines (was Phase A)
  placement               Placement quality micro-benchmark (was Phase A.5)
  contention [--resume]   Resource contention (15 runs, was Phase A.6)
  slicing [--resume]      Slice-aware placement (was Phase B)
  federation [--resume]   Cross-site federation (needs HOST_D2, was Phase C)
  resilience [--resume]   Failure resilience (was Phase D)
  stress [--resume]       Combined contention + failure (60 runs, was Phase E)
  single CONFIG RATE STAGES SEED   Single run
  stop                    Stop all containers (local + distributed)
  restart                 Stop + resume all phases with --topology distributed
  status                  Show progress for all phases
  sync                    Push code + rebuild on remote (no experiments)

Legacy aliases: phase-a, phase-a5, phase-a6, phase-b, phase-c, phase-d, phase-e

Options:
  --remote             Execute on remote host (from .env.local)
  --resume             Skip runs whose result CSV already exists

Examples:
  ./run-experiments.sh smoke
  ./run-experiments.sh single rr medium 3 42
  ./run-experiments.sh --remote slicing --resume
  ./run-experiments.sh --remote sync
HELP
    ;;

esac
