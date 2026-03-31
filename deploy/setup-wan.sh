#!/bin/bash
# Setup WAN emulation between edge (VM2) and cloud (VM3).
# Run on VM2 and VM3 before starting experiments.
#
# Usage: ./setup-wan.sh <remote_ip> [delay_ms] [jitter_ms]
#   e.g.: ./setup-wan.sh 10.0.0.3 50 5

set -euo pipefail

REMOTE_IP="${1:?Usage: setup-wan.sh <remote_ip> [delay_ms] [jitter_ms]}"
DELAY_MS="${2:-50}"
JITTER_MS="${3:-5}"
IFACE="${4:-eth0}"

echo "Setting up WAN emulation: ${DELAY_MS}ms delay, ${JITTER_MS}ms jitter to ${REMOTE_IP} on ${IFACE}"

# Clean existing rules
sudo tc qdisc del dev "$IFACE" root 2>/dev/null || true

# Add prio qdisc with 3 bands
sudo tc qdisc add dev "$IFACE" root handle 1: prio

# Add netem delay to band 3
sudo tc qdisc add dev "$IFACE" parent 1:3 handle 30: netem delay "${DELAY_MS}ms" "${JITTER_MS}ms"

# Route traffic to remote IP through band 3
sudo tc filter add dev "$IFACE" parent 1:0 protocol ip u32 match ip dst "${REMOTE_IP}/32" flowid 1:3

echo "WAN emulation active: ${REMOTE_IP} -> ${DELAY_MS}ms +/- ${JITTER_MS}ms"
