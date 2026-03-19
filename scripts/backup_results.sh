#!/usr/bin/env bash
# Backup experiment results to a remote server.
#
# Creates a timestamped snapshot directory on the remote so that
# successive runs do not overwrite each other.
#
# Usage:
#   bash scripts/backup_results.sh ./results user@backup:/data/neural-pubsub/
#
# Arguments:
#   $1  Local results directory (e.g. ./results)
#   $2  Remote destination   (e.g. user@host:/path)

set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <results-dir> <remote-dest>"
    echo "  Example: $0 ./results user@backup:/data/neural-pubsub/"
    exit 1
fi

RESULTS_DIR="$1"
REMOTE_BASE="$2"

if [ ! -d "$RESULTS_DIR" ]; then
    echo "Error: results directory '$RESULTS_DIR' does not exist."
    exit 1
fi

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
REMOTE_DEST="${REMOTE_BASE%/}/snapshot_${TIMESTAMP}/"

echo "Backing up: $RESULTS_DIR -> $REMOTE_DEST"

rsync -avz --compress --progress "$RESULTS_DIR/" "$REMOTE_DEST"

echo ""
echo "Backup complete: $REMOTE_DEST"
echo "Files transferred: $(find "$RESULTS_DIR" -type f | wc -l | tr -d ' ')"
echo "Total size: $(du -sh "$RESULTS_DIR" | cut -f1)"
