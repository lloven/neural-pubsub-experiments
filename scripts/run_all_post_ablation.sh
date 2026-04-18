#!/usr/bin/env bash
# Chain every distributed phase invalidated by the L53 bug (2026-04-18).
#
# Each phase runs via its own run_PHASE.py with --topology distributed
# --resume. Fails fast on any phase so the operator can investigate
# before the next phase starts.
#
# Intended entry point: launched in a tmux session AFTER the ablation
# campaign completes. Does not try to detect ablation completion; caller
# must ensure ablation is done before invoking this.
#
# Expected runtime (14 min/run after the L53 fix):
#   baseline    61 runs  ~14 h
#   slicing     60 runs  ~14 h
#   resilience  90 runs  ~21 h
#   stress      91 runs  ~21 h  (NOTE: stress rates 10/20/50 pps — the
#                                 20 and 50 pps cells sit in the
#                                 collapse regime per the 2026-04-18
#                                 calibration; consider recalibrating
#                                 stress rates before running)
#   market     335 runs  ~78 h
#   TOTAL                ~148 h  = ~6.2 days
#
# Usage (inside tmux on VM1):
#   cd ~/neural-pubsub && bash scripts/run_all_post_ablation.sh

set -e
set -u
set -o pipefail

REPO_DIR="${HOME}/neural-pubsub"
cd "${REPO_DIR}"

LOG_DIR="results/post_ablation_chain"
mkdir -p "${LOG_DIR}"

# Safety check: make sure the L53 fix is deployed (no silent 1.0 pps runs).
python3 -c "
from scripts.multi_vm_runner import _build_workload_cmd
cmd = _build_workload_cmd(
    run_id='selfcheck', results_subdir='selfcheck', seed=0,
    warmup_s=1, measurement_s=1,
    workload_env={'PIPELINE_TYPE': 'cqi_chain', 'ARRIVAL_RATE': '5.0'},
)
assert '--arrival-rate 5.0' in cmd, 'L53 fix missing — refusing to run'
print('L53 fix verified.')
"

echo "==[$(date '+%F %T')]== Starting post-ablation chain..." | tee -a "${LOG_DIR}/chain.log"

PHASES=(baseline slicing resilience stress market)

for phase in "${PHASES[@]}"; do
    echo "==[$(date '+%F %T')]== phase=${phase} starting" | tee -a "${LOG_DIR}/chain.log"
    python3 -m "scripts.run_${phase}" --topology distributed --resume \
        2>&1 | tee "${LOG_DIR}/${phase}.log"
    echo "==[$(date '+%F %T')]== phase=${phase} finished" | tee -a "${LOG_DIR}/chain.log"
done

echo "==[$(date '+%F %T')]== Post-ablation chain complete." | tee -a "${LOG_DIR}/chain.log"
