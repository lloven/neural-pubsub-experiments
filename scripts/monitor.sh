#!/bin/bash
# Experiment progress monitor
#
# Monitors the progress of parallel experiment runs by reading the
# shared progress file (.progress.json) in the experiment's results directory.
#
# Usage:
#   ./monitor.sh                              # monitor current dir
#   ./monitor.sh /path/to/experiment/results   # monitor specific dir
#   ./monitor.sh --watch                       # auto-refresh every 5s
#   ./monitor.sh --watch 2                     # auto-refresh every 2s

set -e

RESULTS_DIR="${1:-results}"
WATCH_MODE=false
INTERVAL=5

# Parse flags
for arg in "$@"; do
    case $arg in
        --watch)
            WATCH_MODE=true
            ;;
        [0-9]*)
            INTERVAL=$arg
            ;;
    esac
done

PROGRESS_FILE="$RESULTS_DIR/.progress.json"

show_progress() {
    clear 2>/dev/null || true
    echo "═══════════════════════════════════════════════════════════"
    echo "  Experiment Progress Monitor"
    echo "  $(date '+%Y-%m-%d %H:%M:%S')"
    echo "═══════════════════════════════════════════════════════════"

    if [ ! -f "$PROGRESS_FILE" ]; then
        echo ""
        echo "  No progress file found at: $PROGRESS_FILE"
        echo "  Waiting for experiments to start..."
        return
    fi

    # Count statuses
    TOTAL=$(python3 -c "import json; d=json.load(open('$PROGRESS_FILE')); print(len(d))" 2>/dev/null || echo 0)
    DONE=$(python3 -c "import json; d=json.load(open('$PROGRESS_FILE')); print(sum(1 for v in d.values() if v['status']=='done'))" 2>/dev/null || echo 0)
    RUNNING=$(python3 -c "import json; d=json.load(open('$PROGRESS_FILE')); print(sum(1 for v in d.values() if v['status']=='running'))" 2>/dev/null || echo 0)
    FAILED=$(python3 -c "import json; d=json.load(open('$PROGRESS_FILE')); print(sum(1 for v in d.values() if v['status']=='failed'))" 2>/dev/null || echo 0)
    QUEUED=$(python3 -c "import json; d=json.load(open('$PROGRESS_FILE')); print(sum(1 for v in d.values() if v['status']=='queued'))" 2>/dev/null || echo 0)

    # Progress bar
    if [ "$TOTAL" -gt 0 ]; then
        PCT=$((DONE * 100 / TOTAL))
        BAR_LEN=40
        FILLED=$((PCT * BAR_LEN / 100))
        EMPTY=$((BAR_LEN - FILLED))
        BAR=$(printf '█%.0s' $(seq 1 $FILLED 2>/dev/null) 2>/dev/null || true)
        SPACE=$(printf '░%.0s' $(seq 1 $EMPTY 2>/dev/null) 2>/dev/null || true)
        echo ""
        echo "  [$BAR$SPACE] $PCT% ($DONE/$TOTAL)"
    fi

    echo ""
    echo "  ✓ Done:    $DONE"
    echo "  ⚙ Running: $RUNNING"
    echo "  ⏳ Queued:  $QUEUED"
    if [ "$FAILED" -gt 0 ]; then
        echo "  ✗ Failed:  $FAILED"
    fi

    echo ""
    echo "───────────────────────────────────────────────────────────"

    # Show running jobs
    if [ "$RUNNING" -gt 0 ]; then
        echo "  Currently running:"
        python3 -c "
import json
d = json.load(open('$PROGRESS_FILE'))
for k, v in sorted(d.items()):
    if v['status'] == 'running':
        print(f'    ⚙ {k}')
" 2>/dev/null
        echo ""
    fi

    # Show recently completed (last 5)
    if [ "$DONE" -gt 0 ]; then
        echo "  Recently completed:"
        python3 -c "
import json
d = json.load(open('$PROGRESS_FILE'))
done = [(k, v) for k, v in d.items() if v['status'] == 'done']
done.sort(key=lambda x: x[1].get('timestamp', ''), reverse=True)
for k, v in done[:5]:
    detail = v.get('detail', '')
    print(f'    ✓ {k}: {detail}')
" 2>/dev/null
        echo ""
    fi

    # Show failed jobs
    if [ "$FAILED" -gt 0 ]; then
        echo "  Failed:"
        python3 -c "
import json
d = json.load(open('$PROGRESS_FILE'))
for k, v in sorted(d.items()):
    if v['status'] == 'failed':
        detail = v.get('detail', '')[:60]
        print(f'    ✗ {k}: {detail}')
" 2>/dev/null
        echo ""
    fi

    # Show latest result files
    echo "───────────────────────────────────────────────────────────"
    echo "  Result files:"
    ls -t "$RESULTS_DIR"/*.csv 2>/dev/null | head -5 | while read f; do
        SIZE=$(du -h "$f" | cut -f1)
        echo "    $SIZE  $(basename $f)"
    done
    echo ""
}

if [ "$WATCH_MODE" = true ]; then
    while true; do
        show_progress
        echo "  Refreshing every ${INTERVAL}s... (Ctrl+C to stop)"
        sleep "$INTERVAL"
    done
else
    show_progress
fi
