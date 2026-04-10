#!/usr/bin/env bash
# =============================================================================
# run_oracle_then_ablation.sh -- post-main-campaign re-run + ablation orchestrator
#
# Sequence (must be launched FROM VM1, inside a tmux session):
#
#   1. Apply scripts.fix_phantom_done to results/market/.progress.json
#      (re-marks the 26 oracle-global phantom entries as queued, with
#      a .json.bak backup).
#
#   2. Run scripts.run_market --topology distributed --resume so the
#      26 missing oracle runs are re-executed against the latest code
#      (load-aware market mode + DP colocation fix + HTTP pool sizing).
#
#   3. Run scripts.run_ablation --topology distributed --resume so the
#      225 ablation runs execute right after the oracle re-runs, using
#      the same 14-min run length as the main campaign (inherited from
#      the experiment_matrix SSoT).
#
# All output is tee'd to results/oracle_then_ablation.log so the
# operator can detach from tmux and inspect progress later.
#
# Usage (from VM1):
#
#     tmux new-session -d -s campaign \
#         'cd ~/neural-pubsub && bash scripts/run_oracle_then_ablation.sh'
#
#     # Live view:
#     tmux attach -t campaign
#
#     # Graceful stop (sends SIGINT to whichever phase is running):
#     tmux send-keys -t campaign C-c
# =============================================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

LOG_FILE="results/oracle_then_ablation.log"
mkdir -p results

ts() { date '+%Y-%m-%dT%H:%M:%S%z'; }
section() { echo; echo "=== $(ts) :: $* ==="; }

{
    section "STAGE 0: pre-flight checks"
    python3 --version
    git log --oneline -1
    df -h ~/neural-pubsub | tail -1

    section "STAGE 1: phantom-fix on results/market/.progress.json"
    # Dry-run first so the operator can see the affected entries.
    python3 -m scripts.fix_phantom_done \
        results/market/.progress.json \
        --filter oracle-global
    echo
    echo ">>> applying fix..."
    python3 -m scripts.fix_phantom_done \
        results/market/.progress.json \
        --filter oracle-global \
        --apply

    section "STAGE 2: re-run missing oracle-global market runs (--resume)"
    python3 -m scripts.run_market \
        --topology distributed \
        --configs oracle-global \
        --resume
    echo ">>> oracle re-runs complete"

    section "STAGE 3: ablation phase (--resume)"
    python3 -m scripts.run_ablation \
        --topology distributed \
        --resume
    echo ">>> ablation complete"

    section "DONE"
    echo "All stages completed successfully."
} 2>&1 | tee -a "$LOG_FILE"
