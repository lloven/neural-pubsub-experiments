#!/usr/bin/env bash
# =============================================================================
# smoke_phase_e.sh -- Phase E integration smoke test
#
# Runs E7 (S1, 20pps, failure) and E8 (S3, 20pps, failure) with short timing
# to validate the experiment pipeline before committing to full 8-hour runs.
#
# Checks:
#   1. Both configs produce CSV output files
#   2. CSVs contain valid data (non-empty, has success column)
#   3. L38: Treatment verification -- failures appear after injection
#   4. H3+H6 key comparison: S1 vs S3 failure counts
#
# Usage:
#   bash scripts/smoke_phase_e.sh             # run from project root
#   bash scripts/smoke_phase_e.sh --dry-run   # dry-run only (no Docker)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$PROJECT_DIR/results/phase_e"

# Smoke timing: short runs (warmup=30s, measurement=120s, failure at 60s)
WARMUP=30
MEASUREMENT=120
FAILURE_DELAY=60
SEED=42

# --- Logging helpers --------------------------------------------------------
info()  { printf "\033[1;34m[smoke-e]\033[0m %s\n" "$*"; }
ok()    { printf "\033[1;32m[smoke-e]\033[0m %s\n" "$*"; }
warn()  { printf "\033[1;33m[smoke-e]\033[0m %s\n" "$*" >&2; }
err()   { printf "\033[1;31m[smoke-e]\033[0m %s\n" "$*" >&2; }

# --- Parse flags ------------------------------------------------------------
DRY_RUN=""
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=1
fi

cd "$PROJECT_DIR"

# ===========================================================================
# Step 1: Run E7 (S1, 20pps, failure) and E8 (S3, 20pps, failure)
# ===========================================================================

info "Phase E smoke test: E7 (S1) + E8 (S3), 20pps, failure at ${FAILURE_DELAY}s"
info "Timing: warmup=${WARMUP}s, measurement=${MEASUREMENT}s, total=$((WARMUP + MEASUREMENT))s"

RUN_ARGS=(
    --configs E7,E8
    --seeds "$SEED"
    --warmup "$WARMUP"
    --measurement "$MEASUREMENT"
    --failure-delay "$FAILURE_DELAY"
)

if [[ -n "$DRY_RUN" ]]; then
    info "[DRY RUN] Verifying config generation only"
    python3 -m scripts.run_phase_e "${RUN_ARGS[@]}" --dry-run
    ok "Dry run passed. Configs are valid."
    exit 0
fi

info "Starting smoke runs (expect ~$((WARMUP + MEASUREMENT + 60))s per run)..."
python3 -m scripts.run_phase_e "${RUN_ARGS[@]}"

# ===========================================================================
# Step 2: Verify CSVs exist
# ===========================================================================

E7_CSV="$RESULTS_DIR/E7_rate-20_S1_fail_seed-${SEED}.csv"
E8_CSV="$RESULTS_DIR/E8_rate-20_S3_fail_seed-${SEED}.csv"

PASS=true

for csv in "$E7_CSV" "$E8_CSV"; do
    label="$(basename "$csv" .csv)"
    if [[ ! -f "$csv" ]]; then
        err "FAIL: Missing CSV: $csv"
        PASS=false
        continue
    fi

    # Check non-empty (more than just header)
    line_count=$(wc -l < "$csv" | tr -d ' ')
    if [[ "$line_count" -le 1 ]]; then
        err "FAIL: $label CSV has only $line_count lines (header only)"
        PASS=false
        continue
    fi
    ok "PASS: $label CSV exists with $line_count lines"
done

if [[ "$PASS" == "false" ]]; then
    err "CSV existence checks failed. Cannot proceed with content validation."
    exit 1
fi

# ===========================================================================
# Step 3: L38 -- Verify treatment effect (failures after injection)
# ===========================================================================

info "L38: Verifying treatment effect (failure injection visible in data)..."

# Check that both CSVs have at least one success=False row (pipeline failures
# caused by the worker kill). A failure-injection experiment with zero failures
# means the treatment was not applied (L38).

for csv in "$E7_CSV" "$E8_CSV"; do
    label="$(basename "$csv" .csv)"

    # Count rows with success=False
    fail_count=$(python3 -c "
import csv, sys
with open('$csv') as f:
    reader = csv.DictReader(f)
    fails = sum(1 for row in reader if row.get('success', '').strip().lower() == 'false')
print(fails)
")

    success_count=$(python3 -c "
import csv, sys
with open('$csv') as f:
    reader = csv.DictReader(f)
    successes = sum(1 for row in reader if row.get('success', '').strip().lower() == 'true')
print(successes)
")

    if [[ "$fail_count" -eq 0 ]]; then
        warn "WARN: $label has 0 failed pipelines. Treatment may not have been applied (L38)."
        warn "  (At 20pps with worker kill, some pipeline failures are expected.)"
    else
        ok "PASS: $label has $fail_count failed + $success_count successful pipelines"
    fi
done

# ===========================================================================
# Step 4: L30 -- Content quality (at least one success=True)
# ===========================================================================

info "L30: Verifying content quality..."

for csv in "$E7_CSV" "$E8_CSV"; do
    label="$(basename "$csv" .csv)"
    success_count=$(python3 -c "
import csv
with open('$csv') as f:
    reader = csv.DictReader(f)
    successes = sum(1 for row in reader if row.get('success', '').strip().lower() == 'true')
print(successes)
")
    if [[ "$success_count" -eq 0 ]]; then
        err "FAIL: $label has 0 successful pipelines (L30: scientifically useless)"
        PASS=false
    else
        ok "PASS: $label has $success_count successful pipelines"
    fi
done

# ===========================================================================
# Step 5: H3+H6 comparison -- S1 vs S3 failure counts
# ===========================================================================

info "H3+H6: Comparing S1 (E7) vs S3 (E8) failure behaviour..."

python3 -c "
import csv, sys

def count_outcomes(path):
    with open(path) as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    successes = sum(1 for r in rows if r.get('success', '').strip().lower() == 'true')
    failures = sum(1 for r in rows if r.get('success', '').strip().lower() == 'false')
    return len(rows), successes, failures

e7_total, e7_ok, e7_fail = count_outcomes('$E7_CSV')
e8_total, e8_ok, e8_fail = count_outcomes('$E8_CSV')

print(f'  E7 (S1 round-robin): {e7_total} total, {e7_ok} success, {e7_fail} failed')
print(f'  E8 (S3 neural):      {e8_total} total, {e8_ok} success, {e8_fail} failed')

if e7_total > 0 and e8_total > 0:
    e7_rate = e7_fail / e7_total * 100
    e8_rate = e8_fail / e8_total * 100
    print(f'  S1 failure rate: {e7_rate:.1f}%')
    print(f'  S3 failure rate: {e8_rate:.1f}%')
    if e8_rate < e7_rate:
        print('  => S3 (neural) has LOWER failure rate than S1 (round-robin) -- supports H3+H6')
    elif e7_rate == e8_rate:
        print('  => Equal failure rates (smoke sample too small, or strategies equivalent at this load)')
    else:
        print('  => S1 has lower failure rate (unexpected; investigate placement logic)')
else:
    print('  WARNING: One or both CSVs have 0 rows. Cannot compare.')
"

# ===========================================================================
# Summary
# ===========================================================================

echo ""
if [[ "$PASS" == "true" ]]; then
    ok "Phase E smoke test PASSED. Ready for full run."
    ok "  Full run: python3 -m scripts.run_phase_e"
    ok "  Or:       ./run-experiments.sh phase-e"
else
    err "Phase E smoke test FAILED. Fix issues before full run."
    exit 1
fi
