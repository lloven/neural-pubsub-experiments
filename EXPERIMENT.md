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
| **Failure type** | None, eMBB worker kill, URLLC worker kill, funnel worker kill with wait/proceed/abort modes (Phase D); broker kill, network partition (Phase C) | Tests resilience mechanisms |
| **Funnel mode** | wait, proceed, abort (Phase D only, D3-D5) | Tests funnel pipeline behaviour under partial input loss |

**Transport note:** Transport (HTTP vs Kafka) was validated as orthogonal in Phase B (<0.7% difference). All phases after Phase B use HTTP exclusively. See Section 4b for details.

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
| Seeds per configuration | 5 for all phases (reduces to 3 if runtime budget exceeded) | Statistical significance |
| Pipeline types | CQI prediction (3-stage map-map-map, URLLC), anomaly detection (3-stage map-map-map, eMBB), sensor fusion (5-stage funnel: 3 inputs + fusion + report, multi-slice) | Representative 6G RAN use cases |
| Embedding model | all-MiniLM-L6-v2 (pre-downloaded, CPU-only, deterministic) | Reproducibility |
| Semantic matching | Simulated (deterministic template matching, not LLM-based) | Isolates distribution architecture; matching quality evaluated in companion Neural Router paper |
| Worker processing time | Configurable per stage type (simulated compute with calibrated delays) | Controlled comparison across strategies |

## 4. Experiment Phases

### Phase A: Single-site baselines

**Purpose:** Establish that Neural Pub/Sub introduces no measurable overhead compared to static baselines in the homogeneous (unconstrained) case. This validates H1 only. Phase A does **not** test placement quality under heterogeneity or contention (those are tested in Phases B and E respectively).

| Config | Strategy | Description |
|--------|----------|-------------|
| A1 | Kafka + static topic routing | Industry-standard centralised pub/sub. Workers consume from topic-named Kafka topics. Static topic-to-worker assignment. |
| A2 | Static placement (round-robin) | HTTP-based dispatch. Workers receive stages in round-robin order. No semantic awareness. |
| A3 | Random placement | HTTP-based dispatch. Workers selected uniformly at random. Lower bound baseline. |
| A4 | Neural Pub/Sub (single broker) | Full semantic routing with embedding-based matching and weighted placement algorithm (Eq. 10). |

**Worker pool parity (resolved):** The original topology gave NeuralBroker (S3) access to workers in both domains via federation, while StaticBroker (S1/S2) could only see domain-1 workers (no federation). This resource asymmetry has been resolved: StaticBroker now includes federation support, ensuring all strategies see the same worker pool. See the "Fairness invariant" section below.

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

### Phase A.5: Placement algorithm quality (micro-benchmark)

**Purpose:** Validate that the placement algorithm (Eq. 9) produces near-optimal placements. This isolates placement quality from system-level factors.

**Method:** For small topologies (3-5 workers, 3-5 stages), brute-force all feasible placements and compare algorithm output against the true minimum cost. Reports the optimality gap (algorithm_cost / optimal_cost - 1).

**Topologies tested:** 5 scenarios (homogeneous flat, heterogeneous flat, slice-constrained with 2 slices, cross-domain with governance, and an additional mixed scenario).

**Results:** All 5 scenarios achieved gap_ratio = 0.0 (optimal placement). Results stored in `results/phase_a5_a6/`.

**Scope limitation:** All 5 current scenarios produce tree-structured DAGs (linear chains or funnels), which are handled by the provably optimal DP solver (`_dp_placement`). The greedy heuristic (`_greedy_placement`) for general (non-tree) DAGs is never exercised. The gap_ratio = 0.0 result confirms the DP implementation is correct but does not validate the greedy path. Non-tree scenarios (e.g., diamond DAGs) are being developed to exercise the greedy solver and provide a meaningful optimality gap measurement.

**Runtime:** < 1 minute (pure computation, no Docker).

### Phase A.6: Resource contention

**Purpose:** Validate graceful degradation under overload. Tests whether the system handles arrival rates exceeding aggregate capacity without catastrophic failure (starvation, deadlock, unbounded queue growth).

**Strategy scope:** Currently tests S3 (Neural Pub/Sub) only. Phase A.6 is a stress test for the neural broker, not an H3 comparison across strategies. The proper H3 comparison (S1 vs S3 under contention + failure) is tested in Phase E.

**Matrix:** 3 configs x 5 seeds = 15 runs. Per run: 2-min warmup + 10-min measurement = 12 min (~3 hours total). Supports `--warmup` and `--measurement` overrides for custom timing.

| Config | Arrival rate | Workers | Expected behaviour |
|--------|-------------|---------|-------------------|
| A6.1 | 20/s (2x capacity) | 5 | Queue buildup, graceful degradation |
| A6.2 | 50/s (5x capacity) | 5 | Saturation, measure failure rate |
| A6.3 | 10/s (at capacity) | 5, then kill 2 at t=5min | Dynamic contention from worker loss |

**Expected outputs:**
- Pipeline completion rate vs. arrival rate (at and above capacity)
- Queue depth over time
- Per-pipeline-type fairness under contention
- Latency degradation curve

### Phase B: Slice-aware placement

**Purpose:** Evaluate the benefit of network-slice-aware stage placement. Tests H3 and H4. Transport (HTTP vs Kafka) was validated as orthogonal in Phase B (<0.7% difference). All subsequent phases use HTTP exclusively.

| Config | Slices | Governance | Failure | What it tests |
|--------|--------|------------|---------|---------------|
| B1 | 1 (flat) | Off | None | Baseline: no slice awareness (3 workers, D1 only) |
| B1eq | 1 (flat, equalized) | Off | None | Worker-count control (5 workers, all in D1, single flat network) |
| B2flat | 2 domains, flat placement | Off | None | Infrastructure isolation only, no slice-aware algorithm (being developed) |
| B2 | 3 (URLLC, eMBB, mMTC) | Off | None | Full slice-aware placement (2 brokers, 2 domains, per-slice networks) |
| B3 | 3 | On | None | Governance overhead on sliced deployment |
| B4 | 3 | On | Worker kill at t=15min | Resilience under slice constraints |

**Confound decomposition plan:** The original B1eq vs B2 comparison changes three variables simultaneously: (1) number of brokers/domains, (2) network topology (flat vs per-slice isolation), and (3) the placement algorithm's slice awareness. To decompose the effect:
- **B1eq** (baseline): 5 workers, 1 broker, 1 domain, flat network, no slice awareness.
- **B2flat** (infrastructure only): 5 workers, 2 brokers, 2 domains, per-slice networks, but placement algorithm treats all workers as flat (no slice affinity). Isolates the infrastructure effect.
- **B2** (infrastructure + algorithm): 5 workers, 2 brokers, 2 domains, per-slice networks, full slice-aware placement. The B2flat-to-B2 delta isolates the algorithm's contribution.

**Throughput anomaly (under investigation):** B1/B1eq achieve only 1.0 pps throughput at 5.0 pps arrival rate, while B2 achieves 5.0 pps. The paper reports 100% completion for B1eq, but the throughput gap suggests either (a) the flat configs receive a lower effective arrival rate, or (b) there is pipeline acceptance throttling. This is under investigation before results can be published.

**Clean comparisons:** B2 vs B3 (governance overhead) is a single-variable comparison and produces a clean result (0.0% overhead). HTTP vs Kafka transport orthogonality (<0.7% difference) is also clean.

**Transport:** HTTP only (orthogonality with Kafka proven in Phase B; see note above).

**Matrix:** 6 configs x 1 rate (medium) x 1 complexity (3-stage) x 5 seeds = 30 runs.
**Per run:** 40 min. **Total:** ~20h.

**Expected outputs:**
- Latency breakdown (per-stage, cross-slice overhead)
- Effect decomposition: infrastructure effect (B1eq vs B2flat) and algorithm effect (B2flat vs B2)
- Governance violation count (B3, B4: expected 0)
- Adaptation timeline (B4: detection time, re-placement time, completion rate recovery)

### Phase C: Cross-site federation

**Purpose:** Measure federation overhead and governance enforcement across domains, including federation-level failure resilience. Tests H4, H5, and partially H6. Answers RQ2.

**Topology:** Two domains (Domain 1: Tokyo/Nakao Lab; Domain 2: Oulu/5GTNF or emulated with calibrated WAN delay).

The Neural Pub/Sub is the **broker** (semantic routing and placement), not the transport. Transport (HTTP vs Kafka) was validated as orthogonal in Phase B (<0.7% difference). Phase C uses HTTP exclusively; the independent variables are federation and governance.

| Config | Broker | Transport | Governance | Failure |
|--------|--------|-----------|------------|---------|
| C1 | Static broker (baseline) | HTTP | Off | None |
| C2 | Neural broker (federated) | HTTP | Off | None |
| C3 | Neural broker (federated) | HTTP | On (governance) | None |
| C4 | Neural broker (federated) | HTTP | On | broker-d2 kill at t=15min |
| C5 | Neural broker (federated) | HTTP | On | Federation network partition at t=15min |

**Pipeline:** CQI prediction. The `collect` and `preprocess` stages must stay in the originating domain (governance); the `predict` stage can be placed in either domain.

**Matrix:** 5 configs x 1 rate (medium) x 5 seeds = 25 runs.

**Status:** Pending execution on distributed testbed (Nakao Lab or laptop+5GTN fallback).

**Cross-site fallback:** If remote 5GTNF access is unavailable, Phase C runs on the local Docker Compose environment with calibrated WAN latency (measured Tokyo-Oulu RTT injected via `tc qdisc`). This is scientifically valid when clearly described as emulated cross-site with measured latency parameters.

**Expected outputs:**
- Single-site vs. cross-site latency comparison (C1 vs. C2)
- Federation bandwidth breakdown (summary propagation vs. forwarded publications)
- Governance compliance verification log (C3, C4, C5)
- Cross-domain routing decision distribution (local vs. forwarded)
- Recovery timeline for federation-level failures (C4: broker kill, C5: network partition)

### Phase D: Failure and adaptation

**Purpose:** Systematic worker failure injection to characterise resilience at the execution-unit level. Tests H6. Answers RQ3.

**Transport:** HTTP only (orthogonality proven in Phase B).

**StaticBroker fairness (resolved):** The StaticBroker now includes health checks, dead-worker removal, failed-stage re-placement, and federation (identical to NeuralBroker's infrastructure). This ensures that S1/S2 vs S3 comparisons isolate the placement algorithm, not the monitoring infrastructure. See the "Fairness invariant" section below.

**H6 reframing:** At medium load (5 pps), health checks provide resilience regardless of placement strategy. All strategies (S1, S2, S3) recover from single-worker failure within the health check detection window because surviving workers have sufficient headroom. The differentiating test is Phase E, where high load (20 pps) combined with failure forces the placement algorithm to make non-trivial decisions on scarce surviving capacity.

D1 and D2 target different slice-specific workers to test whether failure impact depends on the worker's role in the pipeline topology. D3-D5 test funnel pipeline behaviour under different `FUNNEL_MODE` settings when a contributing worker is killed.

| Config | Failure type | Injection time | Expected behaviour |
|--------|-------------|---------------|-------------------|
| D1 | eMBB worker kill (`docker kill worker-d1-embb-1`) | t=15min | Tests recovery when an anomaly-detection worker fails. Broker health check detects failure; affected stages re-placed on surviving workers. |
| D2 | URLLC worker kill (`docker kill worker-d1-urllc-1`) | t=15min | Tests recovery when a CQI/sensor worker fails. Broker health check detects failure; affected stages re-placed on surviving URLLC workers. |
| D3 | Funnel wait mode (`docker kill worker-d1-urllc-2`, `FUNNEL_MODE=wait`) | t=15min | Funnel stage waits for all inputs including from recovered worker. Tests recovery latency impact on funnel pipelines. |
| D4 | Funnel proceed mode (`docker kill worker-d1-urllc-2`, `FUNNEL_MODE=proceed`) | t=15min | Funnel stage proceeds with available inputs after timeout. Tests graceful degradation of funnel pipelines. |
| D5 | Funnel abort mode (`docker kill worker-d1-urllc-2`, `FUNNEL_MODE=abort`) | t=15min | Funnel stage aborts pipeline on missing input. Tests fail-fast behaviour of funnel pipelines. |

**Strategy dimension:** `--strategy S1|S2|S3|all`
- Default: S3 only (for D3/D4/D5 funnel tests)
- For H6 comparison: `--strategy all` with D1,D2 runs S1/S2/S3 x 2 configs x 5 seeds = 30 runs
- Funnel tests: 3 configs (D3/D4/D5) x 5 seeds = 15 runs
- **Total: 45 runs** (30 for H6 comparison + 15 for funnel tests)

**S1/S2 comparison runs:** Fair S1/S2 vs S3 comparison is in progress using the updated StaticBroker with health checks and federation. Previous S1/S2 runs (59% failure rate) were confounded by missing health monitoring and are superseded.

**D3-D5 funnel modes:** Deferred pending broker integration of funnel mode dispatch. The architecture defines three modes (wait/proceed/abort) but the broker does not yet propagate the `FUNNEL_MODE` setting to the pipeline execution path.

Federation-level failures (broker kill, network partition) are tested in Phase C configs C4-C5, which provide the cross-domain traffic necessary for meaningful treatment effects.

**Recovery analysis:** Post-hoc recovery time metrics are computed by `scripts/analyze_recovery.py` from Phase D data, reporting: detection_time, recovery_time, degradation_depth, and failed_pipelines.

**Expected outputs:**
- Recovery timeline plots per failure type (time to detect, time to re-route, pipeline success rate)
- Pipeline completion rate: before, during (30s window around failure), and after recovery
- Comparison of recovery time across worker roles (eMBB vs. URLLC)
- Comparison of recovery time vs. configuration parameters (health check interval, propagation interval)
- Funnel mode comparison: completion rate, latency, and data completeness across wait/proceed/abort modes (deferred)
- Strategy comparison (S1/S2/S3) for D1-D2: recovery behaviour under different placement strategies

### Phase E: Combined H3+H6 contention + failure

**Purpose:** Phase D showed that at medium load (5 pps), all placement strategies recover equally from worker failure because the broker's health check handles rerouting. At HIGH load (20 pps, 2x capacity), S3's load-aware re-placement should outperform S1's blind round-robin because surviving workers are near saturation. Phase E combines A.6 contention rates with D failure injection and strategy comparison. Tests H3+H6. Answers RQ1+RQ3.

**Transport:** HTTP only (orthogonality proven in Phase B).

**Rationale:** Medium-load D results motivated this experiment. The key insight is that intelligent placement only matters when resources are scarce: at capacity, any strategy works; at overload + failure, load-aware re-placement should significantly outperform blind round-robin.

**Prerequisite (resolved):** Phase E was blocked by the StaticBroker fairness issue (S1/S2 lacked health checks and federation, making failure comparisons meaningless). The StaticBroker fix is now in place. Phase E is ready for smoke + full run after Phase D S1/S2 fair comparison completes.

**This is the proper H3+H6 test.** Phase D establishes that health checks alone provide recovery at medium load. Phase E tests whether the placement *algorithm* (not just the monitoring infrastructure) provides additional benefit under high load + failure, where surviving workers are near saturation and placement decisions are non-trivial.

| Config | Rate | Strategy | Failure | Tests |
|--------|------|----------|---------|-------|
| E1 | 10 pps | S1 (round-robin) | none | H3 baseline |
| E2 | 10 pps | S3 (neural) | none | H3 baseline |
| E3 | 10 pps | S1 | eMBB worker kill @300s | H6 medium-load |
| E4 | 10 pps | S3 | eMBB worker kill @300s | H6 medium-load |
| E5 | 20 pps | S1 | none | H3 overload |
| E6 | 20 pps | S3 | none | H3 overload |
| E7 | 20 pps | S1 | eMBB worker kill @300s | **H3+H6 key cell** |
| E8 | 20 pps | S3 | eMBB worker kill @300s | **H3+H6 key cell** |

**Matrix:** 8 configs x 5 seeds = 40 runs x 12 min = ~8 hours.

**Failure target:** `worker-d1-embb-1` (on critical path for anomaly-detection pipelines). Uses `docker-compose.failure.yaml` overlay to disable container restart.

**Expected results:**
- E1 ~ E2: at capacity, strategy difference is negligible (confirmed by Phase D)
- E3 ~ E4: at capacity + failure, health check handles rerouting equally (confirmed by D1-S1 smoke)
- E5 vs E6: at overload, S3 should show lower tail latency (H3 prediction)
- **E7 vs E8: at overload + failure, S3 should significantly outperform S1** (H3+H6 combined prediction)

**Expected outputs:**
- Latency comparison (p50, p95, p99) across all 8 configs
- Throughput degradation under overload and failure
- Recovery timeline for E3/E4/E7/E8: detection_time, recovery_time, degradation_depth
- Strategy effect size: S3 advantage over S1 at 10 pps vs 20 pps, with and without failure

### Phase F: Scaling study (simulation)

**Purpose:** Validate that the federation architecture scales beyond the 2-domain testbed. Tests H5. Pure simulation using EISim.

**Configurations:**
- Topologies: star, mesh, tree
- Scale: 10, 50, 100, 500 nodes across 2, 5, 10, 50 domains
- Workload: proportional to node count

**Expected outputs:**
- Federation overhead vs. number of domains (summary traffic, routing latency)
- Throughput scaling: total system throughput vs. number of nodes
- Comparison: flat federation vs. hierarchical federation

**Note:** Phase F is a stretch goal. Paper claims 1-6 (H1-H4, H6) are covered by Phases A-E on the real testbed.

## 4b. Cross-Phase Design Notes

### Transport orthogonality

Transport (HTTP vs Kafka) was validated as orthogonal in Phase B (<0.7% difference via ANOVA). All phases after Phase B use HTTP exclusively. This eliminates transport as a confound in Phases C, D, and E, allowing those phases to isolate their target variables (federation, failure, contention).

### Fairness invariant

**S1, S2, and S3 differ ONLY in the placement algorithm.** All three strategies now share identical infrastructure:

| Capability | S1 (round-robin) | S2 (random) | S3 (neural) |
|------------|-------------------|-------------|-------------|
| Health check loop | Yes (5s interval, 3 failures = dead) | Yes | Yes |
| Dead worker removal | Yes | Yes | Yes |
| Failed stage re-placement | Yes (re-pick via round-robin) | Yes (re-pick via random) | Yes (re-solve placement) |
| Dispatch-time recovery | Yes (retry with next worker) | Yes (retry with random worker) | Yes (evict + re-solve) |
| Federation | Yes (summary propagation, cross-domain forwarding) | Yes | Yes |
| Governance enforcement | Yes (constraint check before placement) | Yes | Yes |
| Worker pool | Same (all registered workers across federated brokers) | Same | Same |

The only variable is the placement decision: S1 cycles through workers in registration order, S2 selects uniformly at random, and S3 solves the cost-optimized placement (Eq. 10) considering latency, load, and governance constraints.

This invariant was established by adding health checks, recovery, and federation to `StaticBroker` (previously these were only in `NeuralBroker`). Earlier results where S1/S2 lacked these capabilities are superseded.

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

- Each configuration runs with 5 independent random seeds
- **Reporting:** Latency reported as median with interquartile range (IQR) across seeds. CDFs constructed from all pipeline instances pooled across seeds (>1000 data points per configuration)
- **Hypothesis tests:** Seed-level aggregate statistics are the unit of analysis (n=5), ensuring independence. Pairwise distribution comparisons use the two-sample Kolmogorov-Smirnov (KS) test with Holm-Bonferroni correction for the three planned contrasts per metric (S4 vs. S1, S4 vs. S2, S4 vs. S3). Exact p-values reported
- **Effect sizes:** Vargha-Delaney A_12 statistic for all pairwise comparisons (A_12 = 0.5 = no effect; A_12 >= 0.71 = large effect). Wasserstein distance (earth-mover distance, in ms) as an interpretable measure of CDF separation
- **Confidence intervals:** Bootstrap 95% CIs (10,000 resamples) for median and p95 latency, computed from pooled pipeline instances
- **Phase D recovery times:** Per-event (one failure per seed). With n=5, use one-sample Wilcoxon signed-rank test against the 2x summary_interval threshold
- **Multiple comparison correction:** Holm-Bonferroni for planned contrasts. When comparing all 4 strategies (6 pairwise tests), apply Holm-Bonferroni across all 6. When comparing S4 against each baseline (3 contrasts), apply across 3 only

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

| Claim | Data source | Minimum evidence | Status |
|-------|------------|-----------------|--------|
| S3 introduces no overhead in the homogeneous case (H1) | Phase A | S3 latency within 2% of S1/S2 at all tested rates | Confirmed |
| S3 placement achieves optimal/near-optimal cost (H2) | Phase A.5 | gap_ratio < 0.05 on representative scenarios including non-tree DAGs | DP path confirmed (gap=0.0); greedy path pending (non-tree scenarios in development) |
| Under contention, S3 maintains lower tail latency (H3) | Phase E | E7 vs E8: S3 p95 < S1 p95 at 20 pps + failure, with p < 0.05 | Pending (blocked on Phase D S1/S2 completion) |
| Slice-aware placement reduces latency (H4) | Phase B | B2flat vs B2: algorithm effect isolated from infrastructure effect. B2 p95 < B2flat p95 with p < 0.05 | Partially confirmed (B1eq vs B2 shows 15% reduction but confounded; B2flat decomposition in development) |
| Federation enables cross-domain pipelines (H5) | Phase C | C2 successfully routes pipelines across domains that C1 (static) cannot serve | Pending (Phase C not yet executed) |
| After failure, S3 recovers faster than S1/S2 (H6) | Phase D, E | At medium load: all strategies recover equally (health checks sufficient). At high load + failure (Phase E): S3 recovers faster due to load-aware re-placement. Report detection_time, recovery_time, degradation_depth | Medium-load parity expected; high-load differentiation pending Phase E |

## 10. Experiment Launch Protocol

Every new or modified experiment configuration MUST pass through this protocol before full runs. No exceptions (L32, L37).

### Step 1: Local tests (RED→GREEN)
```bash
pytest tests/ -x -v --tb=short
```
All tests pass. No new failures.

### Step 2: Local dry-run
```bash
python -m scripts.run_phase_X --configs CONFIG --seeds 99 --dry-run
```
Verify: correct run_id, correct timing, correct failure target, correct strategy.

### Step 3: Local extended smoke (2.5 min)
```bash
python -m scripts.run_phase_X --configs CONFIG --seeds 99 \
    --warmup 30 --measurement 120 --failure-delay 60
```
Verify: CSV produced, treatment effect visible (L38), no silent errors (L39).

### Step 4: Commit + push
```bash
git add ... && git commit && git push origin main && git push 5gtn main
```

### Step 5: Deploy to remote
```bash
ssh 5gtn-npubsub "cd ~/neural-pubsub && git fetch origin && git reset --hard origin/main"
```

### Step 6: Remote dry-run
```bash
ssh 5gtn-npubsub "cd ~/neural-pubsub && python -m scripts.run_phase_X --configs CONFIG --seeds 99 --dry-run"
```

### Step 7: Remote extended smoke (2.5 min)
```bash
ssh 5gtn-npubsub "cd ~/neural-pubsub && python -m scripts.run_phase_X --configs CONFIG --seeds 99 \
    --warmup 30 --measurement 120 --failure-delay 60"
```
L38: Verify treatment effect. L39: Check for silent errors. Compare against local smoke.

### Step 8: Launch full runs
```bash
ssh 5gtn-npubsub "cd ~/neural-pubsub && ./run-experiments.sh phase-X --resume"
```

### Failure injection experiments: additional checks
- L38: Every failure config must show measurable treatment effect (changed metrics post-injection)
- L41: Failure target must be on the critical path
- Each failure TYPE must be independently smoke-tested (worker kill ≠ broker kill ≠ network partition)

### Fairness invariant
When comparing strategies (S1/S2/S3), verify the ONLY difference is the placement algorithm:
- Same health checks, same recovery, same federation, same worker pool
- Same arrival rate, same pipeline mix, same measurement duration
- Document any remaining differences explicitly

## 11. Reproducibility

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
