#!/bin/bash
# Run the local Neural Pub/Sub emulation environment
set -e
echo "Building Docker image..."
docker compose -f docker-compose.local.yaml build
echo "Starting services..."
docker compose -f docker-compose.local.yaml up --abort-on-container-exit
