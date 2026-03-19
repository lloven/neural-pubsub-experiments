# Neural Pub/Sub

Federated semantic pub/sub broker for distributed AI pipeline orchestration across the 6G computing continuum.

## Overview

Neural Pub/Sub extends the Neural Router (single-broker semantic routing) with broker federation, slice-aware placement, and governance-constrained cross-domain routing. The system distributes AI inference pipelines (represented as service-dependency DAGs) across multiple administrative domains connected by a 6G network, where each domain operates its own Neural Router broker instance.

The architecture implements the distribution model from Section 4 of the paper: brokers exchange compact subscription summaries (centroid embeddings + capacity), enabling cross-domain semantic routing without exposing local topology. A weighted placement algorithm (Eq. 10) assigns pipeline stages to execution units, balancing latency, load, and governance constraints. The system handles three pipeline patterns from 6G RAN use cases: CQI prediction (URLLC), anomaly detection (eMBB), and sensor fusion (multi-slice).

The experiment validates these mechanisms on a real 6G testbed (Nakao Lab Local6G, University of Tokyo) and optionally across a Tokyo-Oulu federation link to 5GTNF. All code is developed and smoke-tested locally via Docker Compose before testbed deployment.

## Architecture

```
                           federation network
                    ┌─────────────────────────────┐
                    │                             │
              ┌─────┴─────┐                 ┌─────┴─────┐
              │ broker-d1 │◄── summaries ──►│ broker-d2 │
              │  (Tokyo)  │   propagation   │  (Oulu)   │
              └─┬───────┬─┘                 └─────┬─────┘
                │       │                         │
        ┌───────┘       └───────┐                 │
        │  URLLC slice          │  eMBB slice     │  eMBB slice
   ┌────┴────┐ ┌────────┐  ┌───┴─────┐     ┌─────┴───┐ ┌─────────┐
   │worker   │ │worker  │  │worker   │     │worker   │ │worker   │
   │d1-urllc │ │d1-urllc│  │d1-embb  │     │d2-embb  │ │d2-embb  │
   │   -1    │ │   -2   │  │   -1    │     │   -1    │ │   -2    │
   └─────────┘ └────────┘  └─────────┘     └─────────┘ └─────────┘

   workload ──► broker-d1 (via federation network)
   generator    publishes pipelines; broker places stages on workers
                or forwards cross-domain to broker-d2
```

## Quick Start

### Prerequisites

- Python 3.11+
- Docker Desktop (for integration and smoke tests)

### Setup

```bash
git clone <repo-url> && cd neural-pubsub

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt   # pytest, dev tools

# Run unit tests (no Docker needed)
pytest tests/ -x -v

# Run full stack locally with Docker Compose
docker compose -f docker-compose.local.yaml up --build

# Run smoke test (shortened durations, validates end-to-end flow)
python scripts/run_smoke_test.py
```

## Project Structure

```
src/
  router_core/              # Ported from neural-router: embedding + LLM matching
    router.py               #   Core semantic router (topic matching, dispatch)
    embeddings.py            #   Sentence-transformer embedding engine
    llm.py                  #   LLM-based match verification
    evaluation.py           #   Routing accuracy evaluation (F1, precision, recall)
  federation/               # Cross-domain federation layer (Section 4.2)
    summary.py              #   SubscriptionSummary dataclass (Eq. 7)
    propagation.py          #   Periodic summary exchange between peers
    routing.py              #   Cross-domain routing protocol (Section 4.2.3)
    integrator.py           #   Composite max-flow capacity (Eq. 8)
  broker/                   # Neural broker = router_core + federation + HTTP API
    neural_broker.py        #   FastAPI broker server with full pipeline lifecycle
    placement.py            #   Slice-aware DP/greedy placement (Eq. 10)
  pipeline/                 # Pipeline representation
    dag.py                  #   Service-dependency DAG (stages, demands, data rates)
    patterns.py             #   Pipeline templates: CQI, anomaly detection, sensor fusion
  worker/                   # Execution units
    worker.py               #   FastAPI worker: receives stages, runs compute, reports
  workload/                 # Experiment driver
    generator.py            #   Poisson-arrival workload generator
  measurement/              # Experiment instrumentation
    harness.py              #   Timestamp injection, metric collection, CSV export
    failure.py              #   Failure injection (kill containers, partition networks)
tests/
  test_dag.py               # DAG construction, topological sort, is_tree
  test_patterns.py          # Pipeline factory functions, structure verification
  test_placement.py         # Placement algorithm, feasibility, constraint checks
  test_federation.py        # Subscription summary, routing, governance filter
  test_measurement.py       # Trace latency, aggregation, federation monitor
  test_failure.py           # Failure injection (mocked Docker client)
  test_integration.py       # Level 2-3: broker + worker integration (requires Docker)
  test_smoke.py             # Level 4: full phase dry runs
scripts/
  run_phase_a.py            # Phase A: single-site baselines
  run_phase_b.py            # Phase B: slice-aware placement
  run_phase_c.py            # Phase C: cross-site federation
  run_phase_d.py            # Phase D: failure and adaptation
  run_smoke_test.py         # Smoke test runner (all phases, shortened durations)
  generate_figures.py       # Generate all paper figures from result CSVs
  measure_latency.py        # Inter-node latency matrix measurement
  deploy.py                 # Container deployment to testbed nodes
configs/
  domain_d1.yaml            # Domain 1 broker config (peers, governance, placement)
  domain_d2.yaml            # Domain 2 broker config
  workload.yaml             # Workload generator config (rate, mix, duration)
  smoke_test.yaml           # Smoke test config (short duration, limited pipelines)
```

## Experiment Phases

| Phase | Purpose                          | Script                      | Key configs                         |
|-------|----------------------------------|-----------------------------|-------------------------------------|
| A     | Single-site baselines            | `scripts/run_phase_a.py`    | A1 Kafka, A2 static, A3 random, A4 neural |
| B     | Slice-aware placement            | `scripts/run_phase_b.py`    | B1 1-slice, B2 3-slice, B3 +governance, B4 +failure |
| C     | Cross-site federation            | `scripts/run_phase_c.py`    | C1 Kafka baseline, C2 federated, C3 +governance, C4 +broker failure |
| D     | Failure and adaptation           | `scripts/run_phase_d.py`    | D1 worker kill, D2 broker kill, D3 partition, D4 funnel failure |
| E     | EISim scaling study (simulation) | `scripts/run_phase_e.py`    | Star/mesh/tree topologies, 10-500 nodes |

Each phase script supports `--dry-run` to validate configuration without executing experiments.

## Configuration

**Domain configs** (`configs/domain_d1.yaml`, `configs/domain_d2.yaml`): broker identity, federation peers, placement weights (alpha/beta/gamma), governance policy. See inline comments in each file.

**Workload config** (`configs/workload.yaml`): Poisson arrival rate, duration, pipeline type mix, random seed.

**Smoke test config** (`configs/smoke_test.yaml`): shortened parameters for fast end-to-end validation. Overrides per phase.

## Running Experiments

```bash
# Unit tests (Level 1, no Docker)
pytest tests/ -x -v

# Docker smoke test (Level 2-3)
docker compose -f docker-compose.local.yaml up --build

# Smoke test with all phases (Level 4)
python scripts/run_smoke_test.py

# Phase scripts (dry run first, then real)
python scripts/run_phase_a.py --dry-run
python scripts/run_phase_a.py

# Generate figures from results
python scripts/generate_figures.py --results-dir results/ --output-dir figs/
```

Environment variables for Docker Compose:

- `ARRIVAL_RATE` (default: 5.0) -- events per second
- `DURATION_S` (default: 60) -- run duration in seconds
- `SEED` (default: 42) -- random seed

## Results

CSV output is written to `results/` with subdirectories per phase (`phase_a/`, `phase_b/`, etc.). The main metrics CSV columns are:

| Column               | Description                                      |
|----------------------|--------------------------------------------------|
| `pipeline_id`        | UUID of the pipeline instance                    |
| `pipeline_type`      | One of: cqi_prediction, anomaly_detection, sensor_fusion |
| `success`            | Whether the pipeline completed                   |
| `error`              | Error message if failed                          |
| `e2e_latency_ms`     | End-to-end pipeline latency in milliseconds      |
| `stage_*_ms`         | Per-stage latency (columns vary by pipeline type)|

Generate publication figures:

```bash
python scripts/generate_figures.py --results-dir results/ --output-dir figs/
```

Figures are output to `figs/` and can be copied to the manuscript directory.

## Related Publications

- L. Loven, "Agentic Edge Intelligence", IEEE/ACM UCC 2025.
- L. Loven et al., AI Service Markets Trilogy (Papers 1-3): economic mechanisms, semantic interconnect, and resource optimisation for distributed AI across the computing continuum.

## License

Apache 2.0 (full LICENSE file to be added).
