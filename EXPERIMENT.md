# Experiment Design: Neural Pub/Sub

**Paper:** Neural Pub/Sub: Distributed AI Orchestration across the 6G Computing Continuum
**Target venue:** Elsevier Computer Networks (DCN)
**Manuscript:** `Manuscripts/Neural Pub-Sub (Elsevier DCN)/` (Overleaf-synced)
**Companion:** Neural Router paper (`Manuscripts/Neural Router (Elsevier DCN)/`; experiments in `Experiments/neural-router/`)

---

## 1. Research Questions

The experiment answers three questions about distributing AI inference pipelines across a federated 6G computing continuum:

**RQ1 (Effectiveness).** Does semantic-aware placement outperform static and centralised alternatives in terms of end-to-end latency and throughput?

**RQ2 (Federation overhead).** What is the cost of cross-domain federation (summary propagation, cross-domain routing) relative to single-domain deployment?

**RQ3 (Resilience).** How quickly does the system recover from execution unit failures, broker failures, and network partitions, and what is the impact on pipeline completion rate?

## 2. Hypotheses

| ID | Hypothesis | Phase |
|----|-----------|-------|
| H1 | Under heterogeneous slice constraints, Neural Pub/Sub achieves lower p95 end-to-end latency than static and random placement baselines | B |
| H2 | Neural Pub/Sub maintains throughput within 15% of Kafka while providing semantic routing, governance enforcement, and cross-domain federation | A |
| H3 | Slice-aware placement reduces p95 latency compared to flat (single-slice) deployment | B |
| H4 | Governance constraints (data sovereignty enforcement) introduce measurable but bounded latency overhead | B, C |
| H5 | Under hierarchical federation, summary propagation overhead grows linearly with the number of domains | C, E |
| H6 | The system recovers from single-point failures within two summary propagation intervals, with detection time and re-placement time reported separately | D |

## 3. Experimental Variables

### Independent variables

| Variable | Values | Rationale |
|----------|--------|-----------|
| **Distribution strategy** | S1: Kafka (centralised), S2: static (round-robin), S3: random, S4: Neural Pub/Sub (semantic) | Baseline comparison |
| **Arrival rate** | Low (2/s), medium (5/s), high (10/s) | Stress test placement under load |
| **Pipeline complexity** | 2, 3, 5 stages | Tests DAG placement scaling |
| **Number of slices** | 1, 3 | Tests slice-aware placement value |
| **Governance** | Off, on (data sovereignty: raw data stays in originating domain) | Tests governance overhead |
| **Federation** | Single domain, 2-domain federated | Tests cross-domain routing |
| **Failure type** | None, worker kill, broker kill, network partition, partial sensor loss | Tests resilience mechanisms |

### Dependent variables (metrics)

| Metric | Collection method | Unit |
|--------|------------------|------|
| End-to-end pipeline latency (p50, p95, p99) | Timestamps at pipeline creation and completion | ms |
| Per-stage latency | Timestamps at stage dispatch and result receipt | ms |
| Latency decomposition (routing, transfer, compute) | Inline timestamp injection in broker | ms |
| Throughput | Completed pipelines per second over measurement window | pipelines/s |
| Pipeline completion rate | Completed / submitted | ratio |
| Routing accuracy (F1) | Ground-truth label comparison for pipeline-to-template matching | score |
| Governance violation rate | Count of stages placed in violation of sovereignty policy | count (expected: 0) |
| Federation bandwidth | Bytes exchanged between brokers (summaries + forwarded publications) | KB/s |
| Summary propagation latency | Time from summary generation to receipt at peer | ms |
| Failure detection time | Time from failure injection to broker detecting the failure | ms |
| Recovery time | Time from detection to successful re-placement of affected stages | ms |
| Domain crossings per pipeline | Number of times a pipeline's stages are placed across domains | count |

### Controlled variables

| Variable | Value | Rationale |
|----------|-------|-----------|
| Measurement window | 30 minutes (after 10-minute warm-up) | Steady-state measurement |
| Seeds per configuration | 5 (reduces to 3 if runtime budget exceeded) | Statistical significance |
| Pipeline types | CQI prediction (3-stage map-map-map, URLLC), anomaly detection (3-stage map-map-map, eMBB), sensor fusion (5-stage funnel: 3 inputs + fusion + report, multi-slice) | Representative 6G RAN use cases |
| Embedding model | all-MiniLM-L6-v2 (pre-downloaded, CPU-only, deterministic) | Reproducibility |
| Semantic matching | Simulated (deterministic template matching, not LLM-based) | Isolates distribution architecture; matching quality evaluated in companion Neural Router paper |
| Worker processing time | Configurable per stage type (simulated compute with calibrated delays) | Controlled comparison across strategies |

## 4. Experiment Phases

### Phase A: Single-site baselines

**Purpose:** Establish performance of four distribution strategies on a single domain. This is the core comparison that answers RQ1 and tests H1-H2.

| Config | Strategy | Description |
|--------|----------|-------------|
| A1 | Kafka + static topic routing | Industry-standard centralised pub/sub. Workers consume from topic-named Kafka topics. Static topic-to-worker assignment. |
| A2 | Static placement (round-robin) | HTTP-based dispatch. Workers receive stages in round-robin order. No semantic awareness. |
| A3 | Random placement | HTTP-based dispatch. Workers selected uniformly at random. Lower bound baseline. |
| A4 | Neural Pub/Sub (single broker) | Full semantic routing with embedding-based matching and weighted placement algorithm (Eq. 10). |

**Matrix:** 4 configs x 3 rates x 3 complexities x 5 seeds = 180 runs.
**Per run:** 10-min warm-up + 30-min measurement = 40 min.
**Total runtime:** 120h sequential; ~30h on 4 parallel nodes.
**Fallback:** 4 x 2 rates x 3 complexities x 3 seeds = 72 runs (~48h sequential, ~12h parallel).

**Expected outputs:**
- Latency CDF plots (A1-A4 overlaid, per complexity level)
- Phase A answers RQ1 for flat single-site comparison. The key finding is the latency-throughput tradeoff: Neural Pub/Sub may show higher routing latency but the placement algorithm provides benefits under heterogeneous conditions (tested in Phase B).
- Throughput vs. arrival rate (A1-A4 overlaid)
- Latency decomposition (routing + transfer + compute) stacked bar chart
- Routing accuracy table (A4 vs. companion Neural Router paper results)

### Phase B: Slice-aware placement

**Purpose:** Evaluate the benefit of network-slice-aware stage placement. Tests H3 and H4.

| Config | Slices | Governance | Failure | What it tests |
|--------|--------|------------|---------|---------------|
| B1 | 1 (flat) | Off | None | Baseline: no slice awareness |
| B2 | 3 (URLLC, eMBB, mMTC) | Off | None | Value of slice-aware placement |
| B3 | 3 | On | None | Governance overhead on sliced deployment |
| B4 | 3 | On | Worker kill at t=15min | Resilience under slice constraints |

**Matrix:** 4 configs x 1 rate (medium) x 1 complexity (3-stage) x 5 seeds = 20 runs.
**Per run:** 40 min. **Total:** ~13h.

**Expected outputs:**
- Latency breakdown (per-stage, cross-slice overhead)
- Governance violation count (B3, B4: expected 0)
- Adaptation timeline (B4: detection time, re-placement time, completion rate recovery)

### Phase C: Cross-site federation

**Purpose:** Measure federation overhead and governance enforcement across domains. Tests H4, H5, and partially H6. Answers RQ2.

**Topology:** Two domains (Domain 1: Tokyo/Nakao Lab; Domain 2: Oulu/5GTNF or emulated with calibrated WAN delay).

| Config | Strategy | Federation | Governance | Failure |
|--------|----------|------------|------------|---------|
| C1 | Kafka at each site | Static routing | Off | None |
| C2 | Neural Pub/Sub | 2-broker federated | Off | None |
| C3 | Neural Pub/Sub | 2-broker federated | On (raw radio data stays in originating domain) | None |
| C4 | Neural Pub/Sub | 2-broker federated | On | Broker kill at t=15min |

**Pipeline:** CQI prediction. The `collect` and `preprocess` stages must stay in the originating domain (governance); the `predict` stage can be placed in either domain.

**Matrix:** 4 configs x 1 rate (medium) x 5 seeds = 20 runs.

**Cross-site fallback:** If remote 5GTNF access is unavailable, Phase C runs on the local Docker Compose environment with calibrated WAN latency (measured Tokyo-Oulu RTT injected via `tc qdisc`). This is scientifically valid when clearly described as emulated cross-site with measured latency parameters.

**Expected outputs:**
- Single-site vs. cross-site latency comparison (C1 vs. C2)
- Federation bandwidth breakdown (summary propagation vs. forwarded publications)
- Governance compliance verification log (C3, C4)
- Cross-domain routing decision distribution (local vs. forwarded)

### Phase D: Failure and adaptation

**Purpose:** Systematic failure injection to characterise resilience. Tests H6. Answers RQ3.

| Config | Failure type | Injection time | Expected behaviour |
|--------|-------------|---------------|-------------------|
| D1 | Worker kill (`docker kill`) | t=15min | Broker health check detects failure; affected stages re-placed on surviving workers |
| D2 | Broker kill (one of two federated brokers) | t=15min | Peer broker detects via summary propagation timeout; local-only routing for affected domain |
| D3 | Network partition (`docker network disconnect`) | t=15min | Federation link down; both brokers fall back to local-only routing using cached summaries |
| D4 | Partial sensor loss (scale down sensor workers) | t=15min | Funnel pipeline stages with missing inputs: configurable wait/proceed/abort policy |

**Matrix:** 4 configs x 5 seeds = 20 runs.

**Expected outputs:**
- Recovery timeline plots per failure type (time to detect, time to re-route, pipeline success rate)
- Pipeline completion rate: before, during (30s window around failure), and after recovery
- Comparison of recovery time vs. configuration parameters (health check interval, propagation interval)

### Phase E: Scaling study (simulation)

**Purpose:** Validate that the federation architecture scales beyond the 2-domain testbed. Tests H5. Pure simulation using EISim.

**Configurations:**
- Topologies: star, mesh, tree
- Scale: 10, 50, 100, 500 nodes across 2, 5, 10, 50 domains
- Workload: proportional to node count

**Expected outputs:**
- Federation overhead vs. number of domains (summary traffic, routing latency)
- Throughput scaling: total system throughput vs. number of nodes
- Comparison: flat federation vs. hierarchical federation

**Note:** Phase E is a stretch goal. Paper claims 1-6 (H1-H4, H6) are covered by Phases A-D on the real testbed.

## 5. Baselines

Throughout this document, distribution strategies are labeled S1-S4 when discussed generically. In Phase A, these correspond to configs A1-A4. In other phases, the strategy is fixed (S4 for Phases B-D) and configs vary other parameters.

### S1 (A1): Kafka + static topic routing

An Apache Kafka broker mediates between workload generator and workers. Each pipeline type maps to a Kafka topic. Workers consume from their assigned topic. Placement is static (pre-configured topic-to-worker mapping). This represents the industry-standard centralised pub/sub approach.

**Implementation:** `src/broker/kafka_broker.py` subclasses `BaseBroker`. Adds Kafka producer lifecycle. Placement returns a `"kafka"` sentinel; dispatch sends pipeline stages as Kafka messages. Workers consume via `aiokafka`.

### S2 (A2): Static placement (round-robin)

An HTTP-based broker dispatches stages to workers in round-robin order. No semantic matching, no load awareness. Workers are cycled in registration order.

**Implementation:** `src/broker/static_broker.py` with `PlacementStrategy.ROUND_ROBIN`.

### S3 (A3): Random placement

Same HTTP-based broker, but workers are selected uniformly at random for each stage. Provides a lower bound on any intelligent placement.

**Implementation:** `src/broker/static_broker.py` with `PlacementStrategy.RANDOM`.

### Baseline fairness

All baselines use the same:
- Worker implementation (`src/worker/worker.py`)
- Pipeline DAG representation (`src/pipeline/dag.py`)
- Measurement harness (identical timestamp injection points)
- Workload generator (identical arrival process and pipeline types)
- Docker deployment (same container image, same resource limits)

The only variable is the placement decision and dispatch mechanism.

### Kafka baseline configuration

The Kafka baseline uses:
- Apache Kafka (Confluent image) with a single broker
- Partition count equal to worker count (one partition per worker for fair comparison)
- `acks=1` (leader acknowledgment, not full ISR)
- Default `batch.size` (16384 bytes) and `linger.ms` (0ms)
- JVM heap: 1GB (`KAFKA_HEAP_OPTS=-Xmx1G -Xms1G`)
- One topic per pipeline type (3 topics: cqi_prediction, anomaly_detection, sensor_fusion)

## 6. Testbed Configuration

### Local emulation (development and smoke testing)

Docker Compose with 2 domains, 5 workers (3 in Domain 1: 2 URLLC + 1 eMBB; 2 in Domain 2: 2 eMBB), separated by Docker networks emulating slices and a federation overlay.

**WAN emulation:** `tc qdisc` on the federation network injects configurable latency (default: 150-200ms RTT for Tokyo-Oulu).

**Slice QoS emulation:** URLLC networks: 1ms latency, 1Gbps; eMBB networks: 5ms latency, 100Mbps.

### Real testbed (experiment execution)

- **Domain 1:** Nakao Lab Local6G campus testbed, University of Tokyo
- **Domain 2:** 5GTNF, University of Oulu (or emulated with calibrated WAN delay)
- **Cross-site link:** SINET (Japan) to GEANT/FUNET (Finland) research network path

Testbed-specific parameters (node IPs, slice configs, measured latencies) are stored in `testbed-config.yaml` and applied via `docker-compose.testbed.yaml`.

## 7. Measurement Methodology

### Timestamp injection

Every pipeline carries a `TimestampRecord` (defined in `src/measurement/harness.py`) that records:

1. `pipeline_created` — workload generator submits pipeline to broker
2. `placement_complete` — broker computes placement
3. `stage_dispatched[stage_id]` — broker sends stage to worker
4. `stage_started[stage_id]` — worker begins processing
5. `stage_completed[stage_id]` — worker finishes processing
6. `stage_result_received[stage_id]` — broker receives result
7. `pipeline_completed` — all stages done, final result delivered

This allows decomposition of end-to-end latency into: routing time (1-2), dispatch time (2-3), queue time (3-4), compute time (4-5), and result propagation (5-6).

### Metrics aggregation

The `MetricsCollector` (in `src/measurement/harness.py`) aggregates per-run:
- Latency percentiles (p50, p95, p99) from `TimestampRecord` objects
- Throughput (completed pipelines per second)
- Federation bandwidth (bytes exchanged between brokers)
- Failure/recovery events from the `AdaptationTracker`

Results are exported as CSV via the broker's `/metrics/export` endpoint or by the workload generator at run completion.

### Statistical approach

- Each configuration runs with 5 independent random seeds (workload arrival process and pipeline type selection are seeded)
- Metrics are reported as median with interquartile range across seeds
- Latency CDFs use all pipeline instances from all seeds (thousands of data points per configuration)
- Phase D recovery times are reported per-event (5 events per configuration, one per seed)

### Run ordering

Phase A runs (180 configurations) are executed in randomized order to mitigate ordering effects (thermal throttling, Docker daemon state drift, host system load variation). The randomization seed is fixed for reproducibility. Phases B-D are smaller matrices (20 runs each) and are run sequentially by configuration, with a 60-second cool-down between runs.

### Steady-state validation

The 10-minute warm-up period is validated by monitoring throughput over 30-second sliding windows. Measurement begins only after the throughput coefficient of variation (CV) across consecutive windows drops below 0.1. If steady state is not reached within 10 minutes, the warm-up is extended automatically. A representative warm-up trace is included in supplementary results to demonstrate transient behaviour.

## 8. Semantic Matching Decision

The Neural Router's LLM-based semantic matching is **not** used in this experiment. Instead, pipeline types are matched deterministically (the workload generator specifies the pipeline type explicitly in each publish request).

**Rationale:** This experiment validates the *distribution architecture* (federation, placement, slicing, failure recovery), not the quality of semantic matching. Matching quality is evaluated in the companion Neural Router paper. Deterministic matching isolates the distribution variables and eliminates API latency, cost, and non-determinism as confounds.

The sentence embedding model (all-MiniLM-L6-v2) is still used for federation routing: brokers generate subscription summaries as centroid embeddings of their registered capabilities, and cross-domain routing computes cosine similarity between pipeline embeddings and peer summaries. This embedding-based routing is deterministic and CPU-only.

## 9. Success Criteria

The experiment must produce data sufficient to evaluate all six hypotheses:

| Claim | Data source | Minimum evidence |
|-------|------------|-----------------|
| Intelligent placement outperforms baselines under heterogeneity (H1) | Phase B | B2 (3-slice Neural) p95 latency < A2 (static) p95 latency with p < 0.05 (KS test on per-pipeline latency distributions) |
| Throughput parity with capabilities (H2) | Phase A | A4 throughput >= 0.85 x A1 throughput at all tested rates |
| Slice-aware placement reduces latency (H3) | Phase B | B2 p95 < B1 p95 with p < 0.05 |
| Governance overhead is bounded (H4) | Phase B, C | Report B3/B2 and C3/C2 latency ratios with confidence intervals |
| Federation scales linearly (H5) | Phase C, E | If Phase E completed: summary bandwidth proportional to domain count under hierarchical topology. If not: report 2-domain overhead and state scaling as conjecture |
| Recovery within bounded time (H6) | Phase D | Report detection time and re-placement time separately. Both < 2 x summary_interval_s for D1-D4 |

## 10. Reproducibility

All code, configurations, and analysis scripts are in this repository. To reproduce the experiment on any Docker-capable machine:

```bash
# Install
pip install -r requirements.txt && pip install -r requirements-dev.txt

# Verify correctness
pytest tests/ -x -v

# Run Phase A locally (full matrix, ~30h on 4 cores)
python scripts/run_phase_a.py

# Or run a quick smoke test (~5 min)
python scripts/run_smoke_test.py

# Generate figures
python scripts/generate_figures.py --results-dir results/ --output-dir figs/
```

The local Docker Compose environment (`docker-compose.local.yaml`) emulates the full multi-domain, multi-slice testbed. WAN latency is injected via `tc qdisc` and can be calibrated to match any real network measurement. Results from the local environment are scientifically valid when the emulation parameters are documented.

## 11. Relationship to Paper Sections

| Paper section | Experiment coverage |
|--------------|-------------------|
| Section 4.1 (Pipeline model) | Pipeline DAGs in `src/pipeline/dag.py`; three 6G RAN patterns in `src/pipeline/patterns.py` |
| Section 4.2 (Broker federation) | Phase C; `src/federation/` |
| Section 4.3 (Placement algorithm) | Phase A (comparison), Phase B (slice-awareness); `src/broker/placement.py` |
| Section 4.4 (Failure handling) | Phase D; `src/measurement/failure.py`, health monitoring in `src/broker/neural_broker.py` |
| Section 4.5 (Scaling) | Phase E; EISim federation extension |
| Section 5.1 (Scenario) | 6G RAN use cases: CQI prediction, anomaly detection, sensor fusion |
| Section 5.2 (Testbed) | Section 6 of this document (testbed configuration) |
| Section 5.3 (Baselines) | Section 5 of this document; `src/broker/kafka_broker.py`, `src/broker/static_broker.py` |
| Section 5.4 (Results) | Phases A-D outputs |
| Section 5.5 (Scaling study) | Phase E outputs |
