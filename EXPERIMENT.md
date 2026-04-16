# Experiment Design: Neural Pub/Sub

**Paper:** Neural Pub/Sub: Distributed AI Orchestration across the 6G Computing Continuum
**Target venue:** IEEE Transactions on Network Science and Engineering (TNSE)
**Manuscript:** `Manuscripts/Neural Pub-Sub (IEEE TNSE)/` (Overleaf-synced via Dropbox)
**Companion:** Neural Router paper (`Manuscripts/Neural Router (Elsevier FGCS)/`; experiments in `Experiments/neural-router/`)

---

> **Single source of truth:** The authoritative experiment matrix (phases, configs, seeds, and transports) is defined in `scripts/experiment_matrix.py`. All test assertions and run count calculations derive from this single source. Run `python -m scripts.experiment_matrix` for the current matrix summary.

## 1. Research Questions

The experiment answers three questions about distributing AI inference pipelines across a federated 6G computing continuum:

**RQ1 (Effectiveness).** Does semantic-aware placement outperform static and centralised alternatives in terms of end-to-end latency and throughput?

**RQ2 (Federation overhead).** What is the cost of cross-domain federation (summary propagation, cross-domain routing) relative to single-domain deployment?

**RQ3 (Resilience).** How quickly does the system recover from execution unit failures, broker failures, and network partitions, and what is the impact on pipeline completion rate?

## 2. Hypotheses

| ID | Hypothesis | Phase |
|----|-----------|-------|
| H1 | Under heterogeneous slice constraints, Neural Pub/Sub achieves lower p95 end-to-end latency than static and random placement baselines | Slicing |
| H2 | Neural Pub/Sub maintains throughput within 15% of Kafka while providing semantic routing, governance enforcement, and cross-domain federation | Baseline |
| H3 | Slice-aware placement reduces p95 latency compared to flat (single-slice) deployment | Slicing |
| H4 | Governance constraints (data sovereignty enforcement) introduce measurable but bounded latency overhead | Slicing, Federation |
| H5 | Under hierarchical federation, summary propagation overhead grows linearly with the number of domains | Federation, Stress |
| H6 | The system recovers from single-point failures within two summary propagation intervals, with detection time and re-placement time reported separately | Resilience |

### Manuscript Hypothesis Mapping (Tier 2)

The manuscript (IEEE TNSE) uses a different hypothesis naming scheme. This table maps manuscript hypothesis IDs to experiment phases and configs:

| Manuscript H | Internal H | EXPERIMENT.md Phase | Configs | Runs |
|---|---|---|---|---|
| H-OVERHEAD | H2 | Baseline (A) | S1/S2/S3 | Tier 1 (confirmed) |
| H-TRANSPORT | H2 | Slicing (B) | ANOVA placement x transport | Tier 1 (confirmed) |
| H-SLICE | H1/H3 | Slicing (B) | flat, B2 | Tier 1 (confirmed) |
| H-GOV | H4 | Slicing (B) | B2, B3 | Tier 1 (confirmed) |
| H-NEAR | -- | Market | oracle-global, market-quad, rr-global | 3x3x3x5 = 135 |
| H-EDGE | -- | Market | oracle-global, market-quad (tree/SP only) | subset of above |
| H-ENTANGLE | -- | Market | oracle-global, market-quad (entangled only) | subset of above |
| H-OVERLOAD | -- | Market | oracle-global vs market-quad at 3 loads | subset of above |
| H-COMPOSE | -- | Governance | gov-none, gov-edge-only, gov-cloud-only, gov-both | 4x3x1x5 = 60 |
| H-HEURISTIC | -- | Market | market-quad vs locality/greedy/spillover | 4x3x3x5 = 180 |
| H-ADAPT | -- | Market | market-quad (load step 5->10 pps at t=5min) | separate protocol |
| H-FEDERATION | H5 | Federation (C) | C2, C3, C4 | 3x5 = 15 |
| H-RESILIENCE | H6 | Resilience (D) | D1, D2 x {S1, S3} | 2x2x5 = 20 |
| H-RR-RECOVER | -- | Ablation | failure scenario, oracle/rr/market x 3 pipelines | 3x3x5 = 45 |
| H-RR-SATURATE | -- | Ablation | sat-100/150/200 x oracle/rr/market x 3 pipelines | 3x3x3x5 = 135 |
| H-RR-HETERO | -- | Ablation | heterogeneous scenario, oracle/rr/market x 3 pipelines | 3x3x5 = 45 |

**Oracle-global deployment**: The oracle is a single centralised broker on VM1 with full global visibility and congestion-aware DP placement. All 48 workers (across 4 VMs) register with VM1's broker via `WORKER_BROKER_URL`. VM2-4 run workers only (no broker). The DP solver includes post-placement redistribution to avoid serialising concurrent fan-in stages. This is the theoretical upper bound.

**Conventional centralized (rr-global)**: Same single-broker deployment as the oracle, but with round-robin worker assignment instead of cost-optimised placement. Represents what conventional orchestrators (Kubeflow, Argo) achieve with full visibility but no market or cost-aware placement.

**Overload argument (H-OVERLOAD)**: The theoretical oracle is omniscient with instant computation. The practical oracle is a single broker subject to queuing at high arrival rates. At high load (10 pps), the single-broker bottleneck causes the oracle to degrade while the market distributes placement across 4 brokers. The efficiency gap Delta_eff = 1 - eta_market/eta_oracle is expected to shrink (or invert) with increasing load. The load dimension (2, 5, 10 pps) in the 270-run allocation matrix captures this effect.

**Ablation phase**: Ten stress scenarios test the round-robin baseline's failure modes by isolating distinct Walrasian mechanisms: information completeness (3×2 failure factorial: load × kill ratio), admission control (saturation sweep at 100/150/200 pps spanning the ~94% utilization point), and price discovery (heterogeneous worker speeds). The main allocation experiments use uniform conditions where rr-global is structurally hard to beat; the ablation introduces conditions that expose its limitations. Uses a separate compose file (`docker-compose.vm-ablation.yaml`) and worker module (`src.worker.ablation_worker`, a re-export of the main worker) so the main campaign infrastructure is not modified during ablation runs. The ablation broker runs with `MARKET_LOAD_AWARE=true` and `DYNAMIC_BIDDING=true` env vars (load-aware worker selection + M/M/1 congestion pricing). The main campaign's compose file does NOT set these, preserving reproducibility of already-collected market runs.

**Total Tier 2 runs**: 815 (270 allocation + 60 governance + 15 federation + 20 resilience + 450 ablation) = ~196 hours (market+governance ~80h at 14m/run measured, federation+resilience ~7h at 12m/run, ablation ~109h at 14m/run with the expanded 3x2 failure factorial + 3-rate saturation sweep).

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
| **Failure type** | None, eMBB worker kill, URLLC worker kill, funnel worker kill with wait/proceed/abort modes (Resilience phase); broker kill, network partition (Federation phase) | Tests resilience mechanisms |
| **Funnel mode** | wait, proceed, abort (Resilience phase only, D3-D5) | Tests funnel pipeline behaviour under partial input loss |

**Transport note:** Transport (HTTP vs Kafka) was validated as orthogonal in Slicing phase (<0.7% difference). All phases after Slicing phase use HTTP exclusively. See Section 4b for details.

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

### Baseline: Single-site baselines

**Purpose:** Establish that Neural Pub/Sub introduces no measurable overhead compared to static baselines in the homogeneous (unconstrained) case. This validates H1 only. Baseline phase does **not** test placement quality under heterogeneity or contention (those are tested in Phases B and E respectively).

| Config | Strategy | Description |
|--------|----------|-------------|
| A1 | Kafka + static topic routing | Industry-standard centralised pub/sub. Workers consume from topic-named Kafka topics. Static topic-to-worker assignment. |
| A2 | Static placement (round-robin) | HTTP-based dispatch. Workers receive stages in round-robin order. No semantic awareness. |
| A3 | Random placement | HTTP-based dispatch. Workers selected uniformly at random. Lower bound baseline. |
| A4 | Neural Pub/Sub (single broker) | Full semantic routing with embedding-based matching and weighted placement algorithm (Eq. 10). |

**Worker pool parity (resolved):** The original topology gave NeuralBroker (S3) access to workers in both domains via federation, while StaticBroker (S1/S2) could only see domain-1 workers (no federation). This resource asymmetry has been resolved: StaticBroker now includes federation support, ensuring all strategies see the same worker pool. See the "Fairness invariant" section below.

**Matrix:** See `experiment_matrix.py` for current config and seed counts (baseline phase).
**Per run:** 10-min warm-up + 30-min measurement = 40 min.
**Total runtime:** See `expected_run_count("baseline")` for the authoritative count.

**Expected outputs:**
- Latency CDF plots (A1-A4 overlaid, per complexity level)
- Baseline phase answers RQ1 for flat single-site comparison. The key finding is the latency-throughput tradeoff: Neural Pub/Sub may show higher routing latency but the placement algorithm provides benefits under heterogeneous conditions (tested in Slicing phase).
- Throughput vs. arrival rate (A1-A4 overlaid)
- Latency decomposition (routing + transfer + compute) stacked bar chart
- Routing accuracy table (A4 vs. companion Neural Router paper results)

### Placement phase: Placement algorithm quality (micro-benchmark)

**Purpose:** Validate that the placement algorithm (Eq. 9) produces near-optimal placements. This isolates placement quality from system-level factors.

**Method:** For small topologies (3-5 workers, 3-5 stages), brute-force all feasible placements and compare algorithm output against the true minimum cost. Reports the optimality gap (algorithm_cost / optimal_cost - 1).

**Topologies tested:** 5 scenarios (homogeneous flat, heterogeneous flat, slice-constrained with 2 slices, cross-domain with governance, and an additional mixed scenario).

**Results:** All 5 scenarios achieved gap_ratio = 0.0 (optimal placement). Results stored in `results/contention/`.

**Scope limitation:** All 5 current scenarios produce tree-structured DAGs (linear chains or funnels), which are handled by the provably optimal DP solver (`_dp_placement`). The greedy heuristic (`_greedy_placement`) for general (non-tree) DAGs is never exercised. The gap_ratio = 0.0 result confirms the DP implementation is correct but does not validate the greedy path. Non-tree scenarios (e.g., diamond DAGs) are being developed to exercise the greedy solver and provide a meaningful optimality gap measurement.

**Runtime:** < 1 minute (pure computation, no Docker).

### Contention phase: Resource contention

**Purpose:** Validate graceful degradation under overload. Tests whether the system handles arrival rates exceeding aggregate capacity without catastrophic failure (starvation, deadlock, unbounded queue growth).

**Strategy scope:** Currently tests S3 (Neural Pub/Sub) only. Contention phase is a stress test for the neural broker, not an H3 comparison across strategies. The proper H3 comparison (S1 vs S3 under contention + failure) is tested in Stress phase.

**Matrix:** See `expected_run_count("contention")` in `experiment_matrix.py` for current counts. Per run: 2-min warmup + 10-min measurement = 12 min. Supports `--warmup` and `--measurement` overrides for custom timing.

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

### Slicing: Slice-aware placement

**Purpose:** Evaluate the benefit of network-slice-aware stage placement. Tests H3 and H4. Transport (HTTP vs Kafka) was validated as orthogonal in Slicing phase (<0.7% difference). All subsequent phases use HTTP exclusively.

| Config | Slices | Governance | Failure | What it tests |
|--------|--------|------------|---------|---------------|
| B1 | 1 (flat) | Off | None | Baseline: no slice awareness (3 workers, D1 only) |
| flat | 1 (flat, equalized) | Off | None | Worker-count control (5 workers, all in D1, single flat network) |
| rr | 2 domains, flat placement | Off | None | Infrastructure isolation only, no slice-aware algorithm (being developed) |
| B2 | 3 (URLLC, eMBB, mMTC) | Off | None | Full slice-aware placement (2 brokers, 2 domains, per-slice networks) |
| B3 | 3 | On | None | Governance overhead on sliced deployment |
| B4 | 3 | On | Worker kill at t=15min | Resilience under slice constraints |

**Confound decomposition plan:** The original flat vs B2 comparison changes three variables simultaneously: (1) number of brokers/domains, (2) network topology (flat vs per-slice isolation), and (3) the placement algorithm's slice awareness. To decompose the effect:
- **flat** (baseline): 5 workers, 1 broker, 1 domain, flat network, no slice awareness.
- **rr** (infrastructure only): 5 workers, 2 brokers, 2 domains, per-slice networks, but placement algorithm treats all workers as flat (no slice affinity). Isolates the infrastructure effect.
- **B2** (infrastructure + algorithm): 5 workers, 2 brokers, 2 domains, per-slice networks, full slice-aware placement. The rr-to-B2 delta isolates the algorithm's contribution.

**Throughput anomaly (under investigation):** B1/flat achieve only 1.0 pps throughput at 5.0 pps arrival rate, while B2 achieves 5.0 pps. The paper reports 100% completion for flat, but the throughput gap suggests either (a) the flat configs receive a lower effective arrival rate, or (b) there is pipeline acceptance throttling. This is under investigation before results can be published.

**Clean comparisons:** B2 vs B3 (governance overhead) is a single-variable comparison and produces a clean result (0.0% overhead). HTTP vs Kafka transport orthogonality (<0.7% difference) is also clean.

**Transport:** HTTP only (orthogonality with Kafka proven in Slicing phase; see note above).

**Matrix:** See `expected_run_count("slicing")` in `experiment_matrix.py` for current counts.
**Per run:** 40 min.

**Expected outputs:**
- Latency breakdown (per-stage, cross-slice overhead)
- Effect decomposition: infrastructure effect (flat vs rr) and algorithm effect (rr vs B2)
- Governance violation count (B3, B4: expected 0)
- Adaptation timeline (B4: detection time, re-placement time, completion rate recovery)

### Federation: Cross-site federation

**Purpose:** Measure federation overhead and governance enforcement across domains, including federation-level failure resilience. Tests H4, H5, and partially H6. Answers RQ2.

**Topology:** Two domains (Domain 1: Tokyo/Nakao Lab; Domain 2: Oulu/5GTNF or emulated with calibrated WAN delay).

The Neural Pub/Sub is the **broker** (semantic routing and placement), not the transport. Transport (HTTP vs Kafka) was validated as orthogonal in Slicing phase (<0.7% difference). Federation phase uses HTTP exclusively; the independent variables are federation and governance.

| Config | Broker | Transport | Governance | Failure |
|--------|--------|-----------|------------|---------|
| C1 | Static broker (baseline) | HTTP | Off | None |
| C2 | Neural broker (federated) | HTTP | Off | None |
| C3 | Neural broker (federated) | HTTP | On (governance) | None |
| C4 | Neural broker (federated) | HTTP | On | broker-d2 kill at t=15min |
| C5 | Neural broker (federated) | HTTP | On | Federation network partition at t=15min |

**Pipeline:** CQI prediction. The `collect` and `preprocess` stages must stay in the originating domain (governance); the `predict` stage can be placed in either domain.

**Matrix:** See `expected_run_count("federation")` in `experiment_matrix.py` for current counts.

**Status:** Pending execution on distributed testbed (Nakao Lab or laptop+5GTN fallback).

**Cross-site fallback:** If remote 5GTNF access is unavailable, Federation phase runs on the local Docker Compose environment with calibrated WAN latency (measured Tokyo-Oulu RTT injected via `tc qdisc`). This is scientifically valid when clearly described as emulated cross-site with measured latency parameters.

**Expected outputs:**
- Single-site vs. cross-site latency comparison (C1 vs. C2)
- Federation bandwidth breakdown (summary propagation vs. forwarded publications)
- Governance compliance verification log (C3, C4, C5)
- Cross-domain routing decision distribution (local vs. forwarded)
- Recovery timeline for federation-level failures (C4: broker kill, C5: network partition)

### Resilience: Failure and adaptation

**Purpose:** Systematic worker failure injection to characterise resilience at the execution-unit level. Tests H6. Answers RQ3.

**Transport:** HTTP only (orthogonality proven in Slicing phase).

**StaticBroker fairness (resolved):** The StaticBroker now includes health checks, dead-worker removal, failed-stage re-placement, and federation (identical to NeuralBroker's infrastructure). This ensures that S1/S2 vs S3 comparisons isolate the placement algorithm, not the monitoring infrastructure. See the "Fairness invariant" section below.

**H6 reframing:** At medium load (5 pps), health checks provide resilience regardless of placement strategy. All strategies (S1, S2, S3) recover from single-worker failure within the health check detection window because surviving workers have sufficient headroom. The differentiating test is Stress phase, where high load (20 pps) combined with failure forces the placement algorithm to make non-trivial decisions on scarce surviving capacity.

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
- For H6 comparison: `--strategy all` with D1,D2 runs S1/S2/S3 x 2 configs x seeds
- Funnel tests: D3/D4/D5 configs x seeds
- **Total:** See `expected_run_count("resilience")` in `experiment_matrix.py` for current counts

**S1/S2 comparison runs:** Fair S1/S2 vs S3 comparison is in progress using the updated StaticBroker with health checks and federation. Previous S1/S2 runs (59% failure rate) were confounded by missing health monitoring and are superseded.

**D3-D5 funnel modes:** Deferred pending broker integration of funnel mode dispatch. The architecture defines three modes (wait/proceed/abort) but the broker does not yet propagate the `FUNNEL_MODE` setting to the pipeline execution path.

Federation-level failures (broker kill, network partition) are tested in Federation phase configs C4-C5, which provide the cross-domain traffic necessary for meaningful treatment effects.

**Recovery analysis:** Post-hoc recovery time metrics are computed by `scripts/analyze_recovery.py` from Resilience phase data, reporting: detection_time, recovery_time, degradation_depth, and failed_pipelines.

**Expected outputs:**
- Recovery timeline plots per failure type (time to detect, time to re-route, pipeline success rate)
- Pipeline completion rate: before, during (30s window around failure), and after recovery
- Comparison of recovery time across worker roles (eMBB vs. URLLC)
- Comparison of recovery time vs. configuration parameters (health check interval, propagation interval)
- Funnel mode comparison: completion rate, latency, and data completeness across wait/proceed/abort modes (deferred)
- Strategy comparison (S1/S2/S3) for D1-D2: recovery behaviour under different placement strategies

### Stress: Combined H3+H6 contention + failure

**Purpose:** Resilience phase showed that at medium load (5 pps), all placement strategies recover equally from worker failure because the broker's health check handles rerouting. At HIGH load (20 pps, 2x capacity), S3's load-aware re-placement should outperform S1's blind round-robin because surviving workers are near saturation. Stress phase combines A.6 contention rates with D failure injection and strategy comparison. Tests H3+H6. Answers RQ1+RQ3.

**Transport:** HTTP only (orthogonality proven in Slicing phase).

**Rationale:** Medium-load D results motivated this experiment. The key insight is that intelligent placement only matters when resources are scarce: at capacity, any strategy works; at overload + failure, load-aware re-placement should significantly outperform blind round-robin.

**Prerequisite (resolved):** Stress phase was blocked by the StaticBroker fairness issue (S1/S2 lacked health checks and federation, making failure comparisons meaningless). The StaticBroker fix is now in place. Stress phase is ready for smoke + full run after Resilience phase S1/S2 fair comparison completes.

**This is the proper H3+H6 test.** Resilience phase establishes that health checks alone provide recovery at medium load. Stress phase tests whether the placement *algorithm* (not just the monitoring infrastructure) provides additional benefit under high load + failure, where surviving workers are near saturation and placement decisions are non-trivial.

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

**Matrix:** See `expected_run_count("stress")` in `experiment_matrix.py` for current counts (12 configs x 5 seeds = 60 runs after 50pps addition). ~12 min per run.

**Failure target:** `worker-d1-embb-1` (on critical path for anomaly-detection pipelines). Uses `docker-compose.failure.yaml` overlay to disable container restart.

**Expected results:**
- E1 ~ E2: at capacity, strategy difference is negligible (confirmed by Resilience phase)
- E3 ~ E4: at capacity + failure, health check handles rerouting equally (confirmed by D1-S1 smoke)
- E5 vs E6: at overload, S3 should show lower tail latency (H3 prediction)
- **E7 vs E8: at overload + failure, S3 should significantly outperform S1** (H3+H6 combined prediction)

**Expected outputs:**
- Latency comparison (p50, p95, p99) across all 8 configs
- Throughput degradation under overload and failure
- Recovery timeline for E3/E4/E7/E8: detection_time, recovery_time, degradation_depth
- Strategy effect size: S3 advantage over S1 at 10 pps vs 20 pps, with and without failure

### Market mechanism implementation (ablation)

The ablation tests the Walrasian market mechanism's three properties (information completeness, admission control, price discovery). Each property requires specific implementation components that emerged through iterative debugging on the cluster. The campaign-active configuration combines four feature-flagged components, none of which are active in the main market campaign (preserving its reproducibility):

1. **Worker bid scaling** (`src/worker/worker.py:registration_payload`): each worker advertises `bid_cost_ms = base_cost × processing_speed` at registration. A worker with `processing_speed=2.0` (2× slower) bids 200ms; a `processing_speed=0.67` worker bids 67ms. **Why**: utilization-based pricing alone produces only ~2ms price gaps at low load (5 pps, 48 workers, ρ ≈ 1-3%) — far below the 50ms WAN cost needed for cross-domain trade. Encoding intrinsic worker properties in the bid produces 130ms+ gaps that drive routing decisions.

2. **M/M/1 dynamic congestion pricing** (`_compute_clearing_prices_from`, feature flag `BrokerConfig.dynamic_bidding`, env var `DYNAMIC_BIDDING=true`): clearing price uses `cost = bid / (1 - utilization)`, capped at `util=0.99` to avoid divergence. This is the queueing-theoretic sojourn time under Poisson arrivals. At low utilization, cost ≈ bid; near capacity, cost diverges, providing natural admission control.

3. **Federation price exchange** (`POST /federation/price-signal`, `_peer_prices` cache): each broker pushes its local clearing prices to all peers alongside the existing subscription summary propagation. At placement time, `_dispatch_placement_on` merges local + peer prices before invoking `market_mode_placement`. **Why**: without this, each federated broker only sees its own domain's prices, making `should_trade_cross_domain` blind to remote scarcity.

4. **Oracle-mode market for ablation** (`STRATEGY_CONFIG["market-quad"]["oracle_mode"] = True`): the ablation runs market-quad as a single broker on VM1 with all 48 workers visible, isolating the **pricing mechanism** from **federation forwarding**. The H-RR-* hypotheses test the Walrasian properties (which prices); federation forwarding (which broker handles a request) is tested separately by H-FEDERATION. Combining both in market-quad ablation runs would conflate two independent mechanisms.

See lessons L50 (image rebuild after worker-runtime changes), L51 (run_single failure must propagate), L52 (compose templates must be compatible across modules) in `Tasks/lessons.md`.

### Ablation: Stress scenarios where rr-global breaks down

**Purpose:** Establish that the round-robin baseline's (rr-global) competitive performance in the main allocation experiments is conditional on uniform operating conditions (identical worker capacities, no failures, moderate arrival rates). Ten stress scenarios introduce conditions absent from the main campaign and expose rr-global's failure modes. Each scenario isolates a distinct Walrasian mechanism that round-robin lacks by construction: information completeness (failure factorial), admission control (saturation sweep), price discovery (heterogeneous capacities). Tests H-RR-RECOVER, H-RR-SATURATE, H-RR-HETERO.

**Strategies:** 3 (oracle-global, rr-global, market-quad). The other heuristics (locality-only, latency-greedy, spillover) are excluded — the ablation is a focused comparison between the centralised baselines and the market mechanism.

**Pipelines:** all 3 (cqi-chain, anomaly-sp, ran-entangled).

**Seeds:** 5 (DEFAULT_SEEDS).

| Scenario | Theory | Stress factor | Configuration | Expected effect |
|----------|--------|---------------|---------------|-----------------|
| failure-{50,100,150}-12 | Information completeness | Kill 12 workers (25% capacity) on VM2 at load L | L pps, kill all VM2 workers | 3x2 failure factorial varies load × kill ratio. 12-kill at low load (50) is baseline; at high load (150) tests info-completeness when surviving workers are saturated |
| failure-{50,100,150}-24 | Information completeness | Kill 24 workers (50% capacity) on VM1+VM2 at load L | L pps, kill all edge workers | Severe capacity loss; market should reroute via prices while rr-global keeps dispatching to dead workers |
| sat-100 | Admission control | Near-saturation 100 pps (~47% util) | 100 pps, no failure | Both strategies sustain throughput; baseline for the sweep |
| sat-150 | Admission control | At-saturation 150 pps (~70% util) | 150 pps, no failure | rr-global tail latency begins to diverge as workers queue; market clearing prices start to bind |
| sat-200 | Admission control | Above-saturation 200 pps (~94% util) | 200 pps, no failure | rr-global queues unboundedly; market rejects unaffordable pipelines via congestion pricing |
| heterogeneous | Price discovery | Edge VMs 2x slower, cloud 1.5x faster | `WORKER_PROCESSING_SPEED=2.0` on VM1/VM2; `=0.67` on VM3/VM4 | rr-global splits load equally and bottlenecks on slow workers; market discovers fast workers via bid-scaled prices |

**Run length:** Identical to the main market campaign. `warmup_s` and `measurement_s` are inherited from `EXPERIMENTS["ablation"]` in `scripts/experiment_matrix.py`, which references the same `MAIN_CAMPAIGN_WARMUP_S` / `MAIN_CAMPAIGN_MEASUREMENT_S` constants as `EXPERIMENTS["market"]`. At the current 240 s + 600 s = 14 min/run setting, every ablation scenario uses the same statistical window as the main campaign so that CR / latency / p95 distributions are directly comparable across phases. Failure injection occurs at `measurement_s // 2` (5 min into a 10 min measurement window), leaving a 5 min post-failure observation window. To rescale every market-class phase at once, edit the two constants in `scripts/experiment_matrix.py`.

**Total:** 10 scenarios × 3 strategies × 3 pipelines × 5 seeds = **450 runs (~109 hours at 14 min/run)**.

**Factorial design rationale (H-RR-RECOVER):** the 3×2 design (3 loads × 2 kill ratios) decomposes the failure response into separable effects: load main effect (does the market's price-signal advantage grow with arrival rate?), kill-ratio main effect (does it grow with capacity loss?), and load×kill interaction (do the two stresses compound?). The earlier single-cell failure (5 pps + 1-worker kill) produced a null result because dispatch-time recovery handles trivial failures equally for all strategies — 2% capacity loss with 96% headroom never stresses the routing layer. The factorial isolates the regime where information completeness via prices actually matters: surviving workers near saturation, where every wasted retry cascades into queueing.

**Saturation sweep rationale (H-RR-SATURATE):** initial sweep at 20/25/30 pps produced null results (100% CR, flat latency) because workers use asyncio-concurrent stage execution, giving the 48-worker testbed a real saturation point of ~200 pps (not the 25 pps estimated from sequential-worker assumptions). The rates were rescaled to 100/150/200 pps (~47%/70%/94% utilization) to span the actual inflection point.

**Infrastructure separation:** The ablation uses a distinct compose file (`deploy/docker-compose.vm-ablation.yaml`) and worker module (`src.worker.ablation_worker`, a re-export of `src.worker.worker`) so the main campaign's compose stack and worker code are unchanged. The ablation infrastructure is wired through `multi_vm_runner.start_cluster`'s `compose_file` and `per_vm_env` parameters, which default to existing behaviour for the main campaign.

**Market load-awareness flag:** The ablation broker runs with `--market-load-aware` (`BrokerConfig.market_load_aware=True`), enabling load-aware worker selection in `market_mode_placement` (picks the least-loaded feasible worker rather than the first feasible one). The main campaign's compose file does NOT set this flag, preserving reproducibility of already-collected market runs (which used the legacy first-feasible-worker selection). The fix is feature-flagged so the running campaign is undisturbed; see `EXPERIMENT-PLAN.md` "Resolved" section for the full discovery and reproducibility discussion.

**Phase runner:** `scripts/run_ablation.py`. Standard CLI: `--configs failure-50-12,failure-100-12,failure-150-12,failure-50-24,failure-100-24,failure-150-24,sat-100,sat-150,sat-200,heterogeneous`, `--strategies`, `--pipelines`, `--seeds`, `--resume`. Run via `python -m scripts.run_ablation --topology distributed --resume` after the main campaign completes.

**Expected outputs:**
- Per-scenario CR and latency comparison across the 3 strategies
- Failure scenario: detection_time and recovery_time for each strategy (post-hoc analysis from `analyze_recovery.py`)
- Saturation scenario: tail-latency divergence between rr-global and market
- Heterogeneous scenario: per-VM utilisation showing market preferring fast cloud workers

**Status:** Pending. Will run after the main campaign completes.

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

Transport (HTTP vs Kafka) was validated as orthogonal in Slicing phase (<0.7% difference via ANOVA). All phases after Slicing phase use HTTP exclusively. This eliminates transport as a confound in Phases C, D, and E, allowing those phases to isolate their target variables (federation, failure, contention).

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

Throughout this document, distribution strategies are labeled S1-S4 when discussed generically. In Baseline phase, these correspond to configs A1-A4. In other phases, the strategy is fixed (S4 for Slicing through Resilience) and configs vary other parameters.

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
- **Resilience phase recovery times:** Per-event (one failure per seed). With n=5, use one-sample Wilcoxon signed-rank test against the 2x summary_interval threshold
- **Multiple comparison correction:** Holm-Bonferroni for planned contrasts. When comparing all 4 strategies (6 pairwise tests), apply Holm-Bonferroni across all 6. When comparing S4 against each baseline (3 contrasts), apply across 3 only

### Run ordering

Baseline phase runs are executed in randomized order to mitigate ordering effects (thermal throttling, Docker daemon state drift, host system load variation). The randomization seed is fixed for reproducibility. See `experiment_matrix.py` for per-phase run counts. Smaller phases are run sequentially by configuration, with a 60-second cool-down between runs.

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
| S3 introduces no overhead in the homogeneous case (H1) | Baseline phase | S3 latency within 2% of S1/S2 at all tested rates | Confirmed |
| S3 placement achieves optimal/near-optimal cost (H2) | Placement phase | gap_ratio < 0.05 on representative scenarios including non-tree DAGs | DP path confirmed (gap=0.0); greedy path pending (non-tree scenarios in development) |
| Under contention, S3 maintains lower tail latency (H3) | Stress phase | E7 vs E8: S3 p95 < S1 p95 at 20 pps + failure, with p < 0.05 | Pending (blocked on Resilience phase S1/S2 completion) |
| Slice-aware placement reduces latency (H4) | Slicing phase | rr vs B2: algorithm effect isolated from infrastructure effect. B2 p95 < rr p95 with p < 0.05 | Partially confirmed (flat vs B2 shows 15% reduction but confounded; rr decomposition in development) |
| Federation enables cross-domain pipelines (H5) | Federation phase | C2 successfully routes pipelines across domains that C1 (static) cannot serve | Pending (Federation phase not yet executed) |
| After failure, S3 recovers faster than S1/S2 (H6) | Resilience phase, E | At medium load: all strategies recover equally (health checks sufficient). At high load + failure (Stress phase): S3 recovers faster due to load-aware re-placement. Report detection_time, recovery_time, degradation_depth | Medium-load parity expected; high-load differentiation pending Stress phase |

## 10. Experiment Launch Protocol

Every new or modified experiment configuration MUST pass through this protocol before full runs. No exceptions (L32, L37).

### Step 1: Local tests (RED→GREEN)
```bash
pytest tests/ -x -v --tb=short
```
All tests pass. No new failures.

### Step 2: Local dry-run
```bash
python -m scripts.run_PHASE --configs CONFIG --seeds 99 --dry-run
```
Verify: correct run_id, correct timing, correct failure target, correct strategy. Verify run count matches `expected_run_count(phase)` from `scripts/experiment_matrix.py`.

### Step 3: Local smoke (45s) and extended smoke (90s)

**Quick smoke** (45s): confirms end-to-end pipeline, CSV output, no crashes.
```bash
python -m scripts.run_PHASE --configs CONFIG --seeds 99 --warmup 15 --measurement 30
```

**Extended smoke** (90s): confirms treatment effect (L38), no silent errors (L39), enough pipelines for statistical sanity.
```bash
python -m scripts.run_PHASE --configs CONFIG --seeds 99 --warmup 30 --measurement 60
```

For failure-injection phases (resilience, stress with `-fail`), use ext-smoke — the quick smoke may not leave enough time for failure + recovery.

| Smoke type | Warmup | Measurement | Total | Use when |
|---|---|---|---|---|
| Quick | 15s | 30s | 45s | Verifying pipeline works, CSV schema, no crashes |
| Extended | 30s | 60s | 90s | Verifying treatment effect (L38), failure recovery |

Verify: CSV produced, ≥1 success=True row (L30), treatment effect visible (L38), no silent errors (L39).

### Step 4: Commit + push
```bash
git add ... && git commit && git push origin main && git push 5gtn main
```

### Step 5: Deploy to remote
```bash
# From laptop: rsync code to all VMs (excludes multi_vm_config_local.py)
python -c "from scripts.multi_vm_runner import deploy_code; deploy_code()"
```
Note: `deploy_code()` uses `vm.ssh_host` from the local config. From the laptop, this goes through pomerium. From VM1, it uses direct IPs. The per-VM `multi_vm_config_local.py` is excluded from rsync to prevent overwriting VM1's direct-IP config with the laptop's pomerium-alias config.

### Step 6: Remote dry-run
```bash
ssh 5gtn-npubsub "cd ~/neural-pubsub && python3 -m scripts.run_PHASE --topology distributed --configs CONFIG --seeds 99 --dry-run"
```

### Step 7: Remote smoke (45s) + extended smoke (90s)
```bash
# Quick smoke (45s) — pipeline works on target host
ssh 5gtn-npubsub "cd ~/neural-pubsub && python3 -m scripts.run_PHASE --topology distributed --configs CONFIG --seeds 99 --warmup 15 --measurement 30"

# Extended smoke (90s) — treatment effect + no silent errors
ssh 5gtn-npubsub "cd ~/neural-pubsub && python3 -m scripts.run_PHASE --topology distributed --configs CONFIG --seeds 99 --warmup 30 --measurement 60"
```
L38: Verify treatment effect. L39: Check for silent errors. Compare against local smoke. L32: Smoke must run on the target host through the same code path as the full campaign.

### Step 8: Launch full campaign on VM1 in tmux
The orchestrator MUST run on VM1 in a tmux session, not via laptop SSH. This ensures the campaign survives SSH disconnections and pomerium session expiry.

```bash
# Create tmux session on VM1 (from laptop or VM1 console)
ssh 5gtn-npubsub "tmux new-session -d -s campaign \
  'cd ~/neural-pubsub && python3 -m scripts.run_PHASE --topology distributed --resume 2>&1 | tee results/PHASE/campaign.log'"

# Monitor progress
ssh 5gtn-npubsub "grep -c '=== Completed' ~/neural-pubsub/results/PHASE/campaign.log"

# Live view (interactive)
ssh -t 5gtn-npubsub "tmux attach -t campaign"

# Graceful stop (sends SIGINT, triggers cleanup handler)
ssh 5gtn-npubsub "tmux send-keys -t campaign C-c"
```

NEVER use `nohup` from a pomerium SSH session — the process may survive but tmux provides attach/detach/signal capabilities that `nohup` lacks.

### Pre-flight checklist (MUST execute before every campaign launch)

Run this checklist before Step 8. Every item must pass. If any fails, STOP and fix before launching. References: L23 (multi-level smoke), L30 (validate content), L31 (TDD), L32 (smoke on target), L38 (verify treatment), L39 (no silent errors), L47 (follow protocol), L50 (rebuild images after deploy), L51 (propagate run_single failures).

- [ ] **L31 — Tests pass locally**: `pytest tests/ -x --ignore=tests/test_system.py` — 0 failures. New tests written for any code changes.
- [ ] **Code deployed**: `deploy_code()` completed for all VMs, no errors
- [ ] **L50 — Docker images rebuilt**: If code changes affect modules running inside containers (broker, worker, workload), rebuild on all VMs: `docker build -t neural-pubsub:latest .`. Verify with a feature check: `docker run --rm --entrypoint python neural-pubsub:latest -c "from src.broker.neural_broker import BrokerConfig; print(BrokerConfig.__dataclass_fields__.keys())"`. deploy_code() rsyncs source but does NOT rebuild images.
- [ ] **VM1 config intact**: `ssh 5gtn-npubsub "cat ~/neural-pubsub/scripts/multi_vm_config_local.py"` shows `lloven@193.166.32.x` for VM2-4 (NOT pomerium aliases)
- [ ] **VM1→VM2 SSH works**: `ssh 5gtn-npubsub "ssh -o ConnectTimeout=5 lloven@193.166.32.50 hostname"` returns `5gtn50`
- [ ] **Dry-run correct**: `--dry-run` on VM1 shows correct compose commands, oracle mode for oracle/rr-global
- [ ] **L23/L32 — Quick smoke from VM1** (not laptop): 1 run with `--warmup 15 --measurement 30` executed FROM VM1 via SSH. Verify: CSV has ≥1 success=True row, non-zero latency (L30)
- [ ] **L23 — Extended smoke from VM1**: 1 run with `--warmup 30 --measurement 60`. Verify treatment visible (L38), no silent errors in logs (L39)
- [ ] **L38 — Treatment verified**: different strategies produce different placements or metrics (not all identical)
- [ ] **Progress file clean**: no stale `running` entries; invalidated runs marked `queued`; no old CSVs that will be rediscovered by `--resume`
- [ ] **tmux session**: campaign launched in tmux on VM1, NOT via nohup/laptop SSH

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

# Run Baseline phase locally (full matrix, ~30h on 4 cores)
python scripts/run_baseline.py

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
| Section 4.2 (Broker federation) | Federation phase; `src/federation/` |
| Section 4.3 (Placement algorithm) | Baseline phase (comparison), Slicing phase (slice-awareness); `src/broker/placement.py` |
| Section 4.4 (Failure handling) | Resilience phase; `src/measurement/failure.py`, health monitoring in `src/broker/neural_broker.py` |
| Section 4.5 (Scaling) | Stress phase; EISim federation extension |
| Section 5.1 (Scenario) | 6G RAN use cases: CQI prediction, anomaly detection, sensor fusion |
| Section 5.2 (Testbed) | Section 6 of this document (testbed configuration) |
| Section 5.3 (Baselines) | Section 5 of this document; `src/broker/kafka_broker.py`, `src/broker/static_broker.py` |
| Section 5.4 (Results) | Phases A-D outputs |
| Section 5.5 (Scaling study) | Stress phase outputs |
