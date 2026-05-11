# Neural Pub/Sub

Federated, market-coordinated pub/sub broker for autonomic orchestration of AI inference pipelines across the edge–cloud continuum.

## Overview

Neural Pub/Sub is an autonomic substrate whose self-organising behaviour emerges from market-based price signals rather than centralised control. Each administrative domain runs a Neural broker instance that closes a MAPE-K control loop over per-broker health and load monitoring, marginal-cost clearing-price analysis, placement planning over a polymatroidal feasibility region, federated cross-domain dispatch, and shared peer subscription summaries with bounded-staleness price signals. Under gross-substitutes valuations on tree and series-parallel service-dependency DAGs, decentralised price-based allocation matches the welfare of a centralised oracle (Walrasian convergence; see §3 of the paper).

The architecture implements the distribution model from §4 of the paper: brokers exchange compact subscription summaries (centroid embeddings + capacity), enabling cross-domain semantic routing without exposing local topology. A weighted placement algorithm (Eq. 10) assigns pipeline stages to execution units, balancing latency, load, and tenant- and operator-specified data-sovereignty constraints. Three pipeline patterns from 6G RAN use cases are exercised: CQI prediction (URLLC), anomaly detection (eMBB), and sensor fusion (multi-slice).

The reported campaign runs the substrate on a 4-VM, 4-domain, 48-worker federated edge–cloud testbed (single data centre, 50 ms emulated WAN), with 1005 runs spanning three pipeline structures (tree, series-parallel, entangled DAG) and three arrival rates. All code is developed and smoke-tested locally via Docker Compose before testbed deployment; the two-domain layout in the diagram below is the architectural reference, not the campaign topology.

## Architecture

```
                           federation network
                    ┌─────────────────────────────┐
                    │                             │
              ┌─────┴─────┐                 ┌─────┴─────┐
              │ broker-d1 │◄── summaries ──►│ broker-d2 │
              └─┬───────┬─┘                 └─────┬─────┘
                │       │                         │
        ┌───────┘       └──────┐                  │
        │  URLLC slice         │  eMBB slice      │  eMBB slice
   ┌────┴────┐ ┌────────┐  ┌───┴─────┐      ┌─────┴───┐ ┌─────────┐
   │worker   │ │worker  │  │worker   │      │worker   │ │worker   │
   │d1-urllc │ │d1-urllc│  │d1-embb  │      │d2-embb  │ │d2-embb  │
   │   -1    │ │   -2   │  │   -1    │      │   -1    │ │   -2    │
   └─────────┘ └────────┘  └─────────┘      └─────────┘ └─────────┘

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
    models.py               #   Shared Pydantic models + dataclasses (PublishRequest, WorkerInfo, etc.)
    base.py                 #   BaseBroker ABC: shared endpoints, DAG factory, ready-stage dispatch
    neural_broker.py        #   Full broker: federation, health monitoring, semantic placement
    static_broker.py        #   Baseline: round-robin / random placement (PlacementStrategy enum)
    kafka_broker.py         #   Baseline: Kafka topic-based routing
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
  test_baselines.py          # Kafka + static broker: instantiation, placement, enum
  test_placement_quality.py  # Placement micro-benchmark: 5 scenarios, quality gap, constraints
  test_integration.py       # Level 2-3: broker + worker integration (requires Docker)
  test_smoke.py             # Level 4: full phase dry runs
scripts/
  run_phase_a.py            # Phase A: single-site baselines
  run_phase_b.py            # Phase B: slice-aware placement
  run_phase_c.py            # Phase C: cross-site federation
  run_phase_d.py            # Phase D: failure and adaptation
  run_phase_a5_a6.py        # Phase A.5 placement micro-benchmark + A.6 contention
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

## Experiment Matrix

The full experiment structure (phases, configs, seeds, transports, expected run counts) is defined in `scripts/experiment_matrix.py`. This is the single source of truth for all run count calculations and test assertions.

```bash
# Print the current matrix summary
python -m scripts.experiment_matrix

# Or from Python
from scripts.experiment_matrix import print_summary, expected_run_count
print_summary()
expected_run_count("stress")  # => 60
```

## Experiment Phases

| Phase | Purpose                          | Script                      | Key configs                         |
|-------|----------------------------------|-----------------------------|-------------------------------------|
| A     | Single-site baselines            | `scripts/run_phase_a.py`    | A1 Kafka, A2 static, A3 random, A4 neural |
| A.5   | Placement micro-benchmark        | `scripts/run_phase_a5_a6.py`| 5 topology scenarios, quality gap analysis |
| A.6   | Contention experiment            | `scripts/run_phase_a5_a6.py`| Overload, mixed pipelines, failure injection |
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

## Reproducing the Paper

See [`REPRODUCING.md`](REPRODUCING.md) for the seven-phase campaign protocol, smoke-test instructions, and figure-generation script.

## Citation

If you use this software, please cite the accompanying paper (see [`CITATION.cff`](CITATION.cff)):

> L. Lovén, R. Morabito, A. Kumar, S. Pirttikangas, J. Riekki, S. Tarkoma. *Autonomic Federated-Market Orchestration for the Edge–Cloud Continuum.* ACM Transactions on Autonomous and Adaptive Systems, Special Issue on Autonomic Approaches and Applications for the Edge–HPC/Cloud Computing Continuum (under review), 2026.

## License

[Apache 2.0](LICENSE).
