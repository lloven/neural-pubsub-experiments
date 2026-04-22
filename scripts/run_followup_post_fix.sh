#!/usr/bin/env bash
# Chained follow-up sequence after the current post-fix ablation campaign finishes.
#
# Triggered by: launching this script in a separate tmux session alongside
# the running `campaign` tmux.
#
# Sequence:
#   1. Wait for the currently-running ``scripts.run_ablation`` process to exit.
#   2. Re-launch ``run_ablation --resume`` to pick up the additional sat-10
#      market-quad (15 cells) and failure-* market-quad (90 cells) that were
#      reset to ``queued`` in ``.progress.json`` on 2026-04-22 so the whole
#      saturation sweep + failure factorial is on post-fix market code.
#   3. After the full 450-run ablation is certified post-fix, launch the
#      existing ``run_all_post_ablation.sh`` chain that re-runs baseline,
#      slicing, resilience, stress, and the main market campaign with the
#      fixed code.
#
# Each step tees to its own log file in ``results/ablation/`` for post-hoc
# inspection. ``set -e`` ensures the script halts if any step fails so the
# operator can investigate rather than the chain silently degrading.

set -e
set -u
set -o pipefail

REPO_DIR="${HOME}/neural-pubsub"
cd "${REPO_DIR}"

LOG_PREFIX="results/ablation/followup"

echo "==[$(date '+%F %T')]== waiting for current ablation process..." | tee -a "${LOG_PREFIX}.log"
while pgrep -f "scripts.run_ablation" >/dev/null 2>&1; do
    sleep 30
done
echo "==[$(date '+%F %T')]== current ablation process has exited." | tee -a "${LOG_PREFIX}.log"

# Safety: if the previous run halted on error mid-way, we want to re-run
# whatever is still queued (includes the 105 extras we reset earlier).
echo "==[$(date '+%F %T')]== starting extras run (sat-10 market + failure market)..." \
    | tee -a "${LOG_PREFIX}.log"
python3 -m scripts.run_ablation --topology distributed --resume \
    2>&1 | tee "${LOG_PREFIX}_extras.log"
echo "==[$(date '+%F %T')]== extras run complete." | tee -a "${LOG_PREFIX}.log"

# After the full post-fix ablation is certified, kick off the main-campaign
# chain. It has its own L53-fix self-check at the top, so we know the
# market-quad runs in ``run_market`` will land with the fixed broker code.
echo "==[$(date '+%F %T')]== starting post-ablation chain (baseline/slicing/resilience/stress/market)..." \
    | tee -a "${LOG_PREFIX}.log"
bash scripts/run_all_post_ablation.sh 2>&1 | tee "${LOG_PREFIX}_postablation.log"
echo "==[$(date '+%F %T')]== post-ablation chain complete." | tee -a "${LOG_PREFIX}.log"

echo "==[$(date '+%F %T')]== All followup phases finished. Campaign queue empty." \
    | tee -a "${LOG_PREFIX}.log"
