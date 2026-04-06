#!/bin/bash
# One-time setup: allow passwordless tc for the experiment user.
# Run this on all 4 VMs before the Tier 2 campaign.
#
# Usage:
#   ssh vm1 "bash ~/neural-pubsub/deploy/setup-sudo-tc.sh"
#   # repeat for vm2, vm3, vm4
set -euo pipefail

SUDOERS_FILE="/etc/sudoers.d/tc-netem"

echo "Setting up passwordless sudo for /sbin/tc ..."
echo "lloven ALL=(ALL) NOPASSWD: /sbin/tc" | sudo tee "$SUDOERS_FILE"
sudo chmod 440 "$SUDOERS_FILE"

echo "Verifying ..."
if sudo -n tc qdisc show >/dev/null 2>&1; then
    echo "OK: passwordless tc works on $(hostname)."
else
    echo "FAIL: sudo still requires password on $(hostname)."
    exit 1
fi
