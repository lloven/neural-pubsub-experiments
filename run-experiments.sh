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
#   smoke                Quick smoke test (~2 min)
#   phase-a [--resume]   Phase A: single-site baselines (60 runs)
#   phase-a5             Phase A.5: placement quality micro-benchmark
#   phase-a6 [--resume]  Phase A.6: resource contention (15 runs)
#   phase-b [--resume]   Phase B: slice-aware placement (20 runs)
#   phase-c [--resume]   Phase C: cross-site federation (needs HOST_D2)
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
        ssh "$TARGET_HOST" "cd ~/$REMOTE_DIR && $*"
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

    if [[ -z "${TMUX:-}" ]]; then
        if [[ -n "$REMOTE_MODE" ]]; then
            info "Creating tmux session '$session_name' on $TARGET_HOST ..."
            ssh "$TARGET_HOST" "tmux new-session -d -s '$session_name' 'cd ~/$REMOTE_DIR && $full_cmd'" 2>/dev/null || true
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

# Helper: build Python resume flag
py_resume() { [[ -n "$RESUME" ]] && echo "--resume" || true; }

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

# --- Phase A -----------------------------------------------------------------
phase-a)
    auto_sync
    PHASE_SESSION="npubsub-phase-a"
    if maybe_tmux_wrap "$PHASE_SESSION" "$0 ${REMOTE_MODE:+--remote} phase-a ${RESUME:+--resume}"; then
        exit 0
    fi
    rcmd "python3 -m scripts.run_phase_a $(py_resume)"
    ;;

# --- Phase A.5 ---------------------------------------------------------------
phase-a5)
    auto_sync
    rcmd "python3 -m scripts.run_phase_a5_a6 --phase a5"
    ;;

# --- Phase A.6 ---------------------------------------------------------------
phase-a6)
    auto_sync
    PHASE_SESSION="npubsub-phase-a6"
    if maybe_tmux_wrap "$PHASE_SESSION" "$0 ${REMOTE_MODE:+--remote} phase-a6 ${RESUME:+--resume}"; then
        exit 0
    fi
    rcmd "python3 -m scripts.run_phase_a5_a6 --phase a6 $(py_resume)"
    ;;

# --- Phase B -----------------------------------------------------------------
phase-b)
    auto_sync
    PHASE_SESSION="npubsub-phase-b"
    if maybe_tmux_wrap "$PHASE_SESSION" "$0 ${REMOTE_MODE:+--remote} phase-b ${RESUME:+--resume}"; then
        exit 0
    fi
    rcmd "python3 -m scripts.run_phase_b $(py_resume)"
    ;;

# --- Phase C -----------------------------------------------------------------
phase-c)
    if [[ -z "${HOST_D2:-}" ]]; then
        err "Phase C requires HOST_D2 to be set in .env.local"
        exit 1
    fi
    auto_sync
    if [[ -n "$REMOTE_MODE" ]]; then
        sync_host "$HOST_D2" "${HOST_D2_DIR:?}" "${HOST_D2_GIT:?}"
    fi
    PHASE_SESSION="npubsub-phase-c"
    if maybe_tmux_wrap "$PHASE_SESSION" "$0 ${REMOTE_MODE:+--remote} phase-c ${RESUME:+--resume}"; then
        exit 0
    fi
    rcmd "python3 -m scripts.run_phase_c $(py_resume)"
    ;;

# --- Phase D -----------------------------------------------------------------
phase-d)
    auto_sync
    PHASE_SESSION="npubsub-phase-d"
    if maybe_tmux_wrap "$PHASE_SESSION" "$0 ${REMOTE_MODE:+--remote} phase-d ${RESUME:+--resume}"; then
        exit 0
    fi
    rcmd "python3 -m scripts.run_phase_d $(py_resume)"
    ;;

# --- Single run --------------------------------------------------------------
single)
    CONFIG="${1:?Usage: $0 single CONFIG RATE STAGES SEED}"
    RATE="${2:?}"
    STAGES="${3:?}"
    SEED="${4:?}"

    auto_sync
    rcmd "python3 -m scripts.run_single $CONFIG $RATE $STAGES $SEED"
    ;;

# --- Stop all containers -----------------------------------------------------
stop)
    info "Stopping all containers ..."
    rcmd "docker compose -f docker-compose.local.yaml down --remove-orphans" 2>/dev/null || true
    rcmd "docker compose -f docker-compose.local.yaml -f docker-compose.kafka.yaml down --remove-orphans" 2>/dev/null || true
    rcmd "docker compose -f docker-compose.local.yaml -f docker-compose.flat.yaml down --remove-orphans" 2>/dev/null || true
    rcmd "docker compose -f docker-compose.local.yaml -f docker-compose.governance.yaml down --remove-orphans" 2>/dev/null || true
    ok "All stopped."
    ;;

# --- Status ------------------------------------------------------------------
status)
    # Monitor always runs LOCALLY (reads remote data via SSH when --remote)
    remote_flag=""
    if [[ -n "$REMOTE_MODE" ]]; then
        remote_flag="--remote $TARGET_HOST"
    fi
    for phase in phase_a phase_b phase_c phase_d; do
        python3 -m scripts.monitor --once $remote_flag "results/$phase" 2>/dev/null || true
    done
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
