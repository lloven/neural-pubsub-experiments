# Test Coverage Analysis

Generated: 2026-03-27

---

## 1. Unit Test Coverage by Source Module

### 1.1 `src/pipeline/dag.py` -- PipelineDAG, Stage, Edge

**Test file:** `test_dag.py` (10 tests)

| Function / Class | Tested? | Notes |
|---|---|---|
| `Stage` dataclass | Yes | Indirectly via all placement/DAG tests |
| `Edge` dataclass | Yes | Indirectly via edge-adding tests |
| `PipelineDAG.add_stage` | Yes | Includes duplicate-detection test |
| `PipelineDAG.add_edge` | Yes | Includes cycle-detection test |
| `PipelineDAG.topological_sort` | Yes | Linear chain ordering verified |
| `PipelineDAG.is_tree` | Yes | Linear, funnel, fan-out, diamond cases |
| `PipelineDAG.sources` / `sinks` | Yes | Funnel DAG verified |
| `PipelineDAG.predecessors` / `successors` | Yes | Via add_edge test |
| `PipelineDAG.get_stage` | Yes | Indirectly |
| `PipelineDAG.get_edge` | No | Never directly tested |
| `PipelineDAG.__len__` / `__contains__` | Yes | Via add_stage test |
| `PipelineDAG._can_reach` | Yes | Indirectly via cycle detection |

**Gaps:** `get_edge` return value (including None case) is never directly asserted.

---

### 1.2 `src/pipeline/patterns.py` -- Pipeline factories

**Test file:** `test_patterns.py`

| Function | Tested? | Notes |
|---|---|---|
| `map_pipeline` | Yes | Structure and parameter tests |
| `funnel_pipeline` | Yes | Structure tests |
| `cqi_prediction_pipeline` | Yes | Structure, sovereignty, slice tests |
| `anomaly_detection_pipeline` | Yes | Structure tests |
| `sensor_fusion_pipeline` | Yes | Structure, scaling tests |

**Gaps:** None significant. Factories are well covered.

---

### 1.3 `src/broker/placement.py` -- Placement algorithm

**Test files:** `test_placement.py` (14 tests), `test_placement_compute_cost.py`, `test_placement_quality.py`

| Function / Class | Tested? | Notes |
|---|---|---|
| `ExecutionUnit` | Yes | Via placement tests |
| `NetworkTopology` | Yes | Via placement tests |
| `GovernancePolicy` | Yes | Trust levels, local stage types |
| `slice_matches` | Yes | Flat wildcard + strict matching (7 tests in TestFlatSliceWildcard) |
| `check_feasibility` | Yes | Pass/fail cases, all 4 constraint classes |
| `find_placement` (public API) | Yes | Single stage, capacity, slice, sovereignty |
| `_greedy_placement` | Yes | Indirectly via find_placement on non-tree DAGs |
| `_dp_placement` | Yes | CQI pipeline (tree DAG) with sovereignty |
| `_placement_cost` (legacy) | Partial | Indirectly via greedy selection; no direct assertions on cost values |
| `compute_placement_cost` (extended) | Yes | `test_placement_compute_cost.py` tests compute-time inclusion |
| `_is_node_feasible` | Yes | Indirectly via greedy placement |
| `_is_node_feasible_simple` | Yes | Indirectly via DP placement |

**Gaps:**
- `_placement_cost` and `compute_placement_cost` are never tested with explicit cost assertions for the gamma (domain-crossing) term in isolation.
- No test verifies DP vs. greedy strategy selection (i.e., that `find_placement` actually dispatches to `_dp_placement` for tree DAGs and `_greedy_placement` otherwise). The CQI test uses a tree DAG but does not assert that DP was used.
- `test_placement_quality.py` benchmarks optimality gaps, which is good, but is more of a quality test than a coverage test.

---

### 1.4 `src/broker/base.py` -- BaseBroker

**Test files:** `test_handle_result.py`, `test_baselines.py`, `test_refactor.py`

| Function / Class | Tested? | Notes |
|---|---|---|
| `BaseBroker.__init__` | Yes | Indirectly via StaticBroker instantiation |
| `BaseBroker._dispatch_stage` (HTTP) | Partial | `test_handle_result.py` mocks dispatch; never tests real HTTP dispatch |
| `BaseBroker._dispatch_stage` (Kafka) | Partial | `test_kafka_transport.py` tests Kafka dispatch path |
| `BaseBroker._find_ready_stages` | Yes | `test_handle_result.py` tests multi-stage cascading and funnel logic |
| `BaseBroker._dispatch_ready_stages` | Yes | Indirectly via handle_result tests |
| `BaseBroker._handle_result` | Yes | 8+ tests covering completion, cascading, funnel, duplicates, unknown pipeline, metrics |
| `BaseBroker.build_app` (endpoints) | Partial | `/publish`, `/result` tested; `/register`, `/health`, `/metrics/export` tested indirectly |
| `BaseBroker._periodic_snapshot` | No | Never tested |
| `BaseBroker._on_worker_change` | Yes | Via StaticBroker tests |
| `_build_dag` (pipeline factory) | Yes | Via publish endpoint tests |

**Gaps:**
- `_periodic_snapshot` crash-resilience CSV export is completely untested.
- `/workers` endpoint (GET) is never tested.
- `/register` DELETE (deregister) endpoint tested only via worker shutdown tests.
- Kafka producer startup/shutdown lifecycle in `build_app` is not tested.

---

### 1.5 `src/broker/static_broker.py` -- StaticBroker

**Test files:** `test_baselines.py`, `test_static_broker_fairness.py`, `test_static_broker_slice_aware.py`

| Function / Class | Tested? | Notes |
|---|---|---|
| `StaticBroker.__init__` | Yes | Both placement strategies |
| `StaticBroker._rebuild_cycle` | Yes | Indirectly via worker registration |
| `StaticBroker._pick_worker` (round_robin) | Yes | Per-slice cycle tested |
| `StaticBroker._pick_worker` (random) | Yes | Tested |
| `StaticBroker._compute_placement` | Yes | Topological order, slice-aware |
| `StaticBroker._eligible_workers` | Yes | Indirectly |
| `StaticBroker._health_check_loop` | Yes | `test_static_broker_fairness.py` verifies presence |
| `StaticBroker._remove_dead_worker` | Partial | Logic verified but not full async flow |
| `StaticBroker._replace_failed_stages` | No | Never directly tested (funnel bypass logic untested) |
| `StaticBroker._dispatch_stage` (recovery) | No | Dispatch-time recovery with re-placement never tested end-to-end |
| `StaticBroker._build_capacity_summary` | No | Federation capacity summary never tested |
| `StaticBroker._try_federation_forward` | No | Federation forwarding never tested |
| `StaticBroker.build_app` (federation endpoints) | Partial | Fairness tests check structure, not HTTP behavior |

**Gaps:**
- Dispatch-time recovery (the retry-on-failure path in `_dispatch_stage`) is completely untested.
- Federation forwarding (`_try_federation_forward`) is untested.
- `_replace_failed_stages` with `FUNNEL_BYPASS_REPLACE` is untested.

---

### 1.6 `src/broker/neural_broker.py` -- NeuralBroker

**Test files:** No dedicated test file. Tested only via integration/system tests.

**Gaps:** This is a significant gap. The NeuralBroker (the primary experimental broker, S3) has no unit tests at all. All testing relies on Docker-based system tests that skip in CI.

---

### 1.7 `src/broker/funnel_resilience.py` -- Funnel policy

**Test file:** `test_funnel_resilience.py`

| Function | Tested? | Notes |
|---|---|---|
| `FunnelMode` enum | Yes | |
| `apply_funnel_policy` | Yes | All three modes (wait, proceed, abort), timeout, all-received |
| `get_funnel_mode` | Yes | Env var reading |
| `get_funnel_timeout` | Yes | Env var reading |
| `get_funnel_bypass_replace` | Yes | Env var reading |
| `find_funnel_predecessor_stages` | Partial | Tested via integration but not isolated |

**Gaps:** `find_funnel_predecessor_stages` with various DAG topologies is not comprehensively tested.

---

### 1.8 `src/worker/worker.py` -- Worker

**Test file:** `test_worker_unit.py` (35+ tests), `test_worker_capabilities.py`

| Function / Class | Tested? | Notes |
|---|---|---|
| `Worker.__init__` | Yes | |
| `Worker.register` | Yes | Metadata, URL, error cases |
| `Worker.execute_stage` | Yes | Timing, load tracking, concurrency |
| `Worker._report_result` | Yes | Retry logic, error handling, logging |
| `Worker.shutdown` | Yes | Deregistration, cleanup, error tolerance |
| `Worker._build_app` (endpoints) | Yes | `/execute`, `/health`, `/shutdown` |
| `Worker.run` | No | Server lifecycle never tested (requires uvicorn) |
| `Tier`, `Capability` | Yes | `test_worker_capabilities.py` |
| `parse_capabilities` | Yes | `test_worker_capabilities.py` |

**Gaps:** `Worker.run()` (the full server lifecycle with uvicorn) is untested.

---

### 1.9 `src/federation/propagation.py` -- SummaryPropagator

**Test file:** `test_propagation.py`

| Function / Class | Tested? | Notes |
|---|---|---|
| `SummaryPropagator.start` / `stop` | Yes | Lifecycle tests |
| `SummaryPropagator.push_summary` | Yes | HTTP posting verified |
| `SummaryPropagator.receive_summary` | Yes | Freshness check, acceptance |
| `SummaryPropagator.is_peer_healthy` | Yes | Failure tracking |
| `SummaryPropagator._select_eligible_peers` | Yes | Recovery probe interval |
| `SummaryPropagator._send_to_peer` | Yes | Error handling |
| `SummaryPropagator.propagation_latencies_ms` | Partial | Recorded but not asserted |

**Gaps:** `propagation_latencies_ms` return values are never asserted in tests.

---

### 1.10 `src/federation/summary.py` -- SubscriptionSummary

**Test file:** `test_federation.py`

| Function | Tested? | Notes |
|---|---|---|
| `serialize` / `deserialize` | Yes | Round-trip test |
| `create_summary` | No | Never tested |
| `compress_summary` | No | Second-level compression never tested |

**Gaps:** `create_summary` (builds summaries from Neural Router cluster state) and `compress_summary` (k-means re-clustering) are completely untested. The compression function is a critical federation bandwidth optimization.

---

### 1.11 `src/federation/routing.py` -- Federated routing

**Test file:** `test_federation.py`

| Function | Tested? | Notes |
|---|---|---|
| `route_locally` | Yes | Match and no-match cases |
| `select_federation_candidates` | No | Never directly tested |
| `apply_governance_filter` | Yes | Block and pass cases |
| `federated_route` (full 5-step) | No | Never tested as a complete pipeline |

**Gaps:** The full 5-step `federated_route` function is never tested end-to-end. `select_federation_candidates` (step 2) is never tested in isolation.

---

### 1.12 `src/federation/integrator.py` -- Composite capacity

**Test file:** `test_federation.py`

| Function / Class | Tested? | Notes |
|---|---|---|
| `compute_composite_capacity` | Yes | Basic homogeneous case |
| `Integrator` class | No | Never tested (wrapper around module functions) |
| `register_pipeline_type` | Yes | Used in test setup |

**Gaps:** The `Integrator` class methods (`add_node`, `remove_node`, `composite_capacity`) are never tested directly.

---

### 1.13 `src/measurement/harness.py` -- Metrics

**Test files:** `test_measurement.py`, `test_csv_integration.py`, `test_refactor.py`

| Function / Class | Tested? | Notes |
|---|---|---|
| `TimestampRecord` | Yes | Validation, creation |
| `PipelineTrace.end_to_end_latency_ms` | Yes | |
| `PipelineTrace.stage_latencies_ms` | Yes | |
| `PipelineTrace.network_latencies_ms` | No | Never tested |
| `PipelineTrace.domain_crossings` | No | Never tested |
| `MetricsCollector.record` | Yes | |
| `MetricsCollector.complete_pipeline` | Yes | |
| `MetricsCollector.compute_aggregate` | Yes | |
| `MetricsCollector.export_csv` | Yes | `test_csv_integration.py` |
| `MetricsCollector.export_json` | No | Never tested |
| `MetricsCollector.reset` | No | Never tested |
| `FederationMonitor` | Yes | Bytes tracking |
| `AdaptationTracker` | Yes | Failure/recovery pairing |
| `AdaptationTracker.detection_times_ms` | No | Never tested |
| `AdaptationTracker.replacement_times_ms` | No | Never tested |

**Gaps:**
- `PipelineTrace.network_latencies_ms` -- used in analysis scripts but never unit-tested. A bug here silently corrupts latency decomposition in paper results.
- `PipelineTrace.domain_crossings` -- never tested; governance penalty accounting depends on this.
- `AdaptationTracker.detection_times_ms` and `replacement_times_ms` -- the three-phase decomposition (detection/replacement/total) is only tested for total adaptation time, not for the two sub-phases.
- `MetricsCollector.export_json` -- never tested.

---

### 1.14 `src/measurement/failure.py` -- FailureInjector

**Test file:** `test_failure.py`

| Function / Class | Tested? | Notes |
|---|---|---|
| `FailureInjector.kill_worker` | Yes | Mock Docker client |
| `FailureInjector.restart_worker` | Yes | |
| `FailureInjector.kill_broker` | Yes | |
| `FailureInjector.partition_network` | Yes | |
| `FailureInjector.heal_partition` | Yes | |
| `FailureInjector.kill_partial_inputs` | Yes | |
| `FailureInjector.run_scenario` | Yes | |
| `FailureInjector.cleanup` | Yes | |

**Gaps:** Good coverage with mocks. Real Docker integration is only tested via system tests.

---

### 1.15 `src/measurement/warmup.py` -- WarmupCVDetector

**Test file:** `test_warmup.py`

| Function | Tested? | Notes |
|---|---|---|
| `WarmupCVDetector.record` | Yes | |
| `WarmupCVDetector.current_cv` | Yes | Known values, edge cases |
| `WarmupCVDetector.is_steady_state` | Yes | |
| `WarmupCVDetector.total_recorded` | Yes | |

**Gaps:** None. Well covered.

---

### 1.16 `src/workload/generator.py` -- WorkloadGenerator

**Test file:** `test_workload_rejected.py`

| Function / Class | Tested? | Notes |
|---|---|---|
| `WorkloadConfig` validation | Yes | |
| `WorkloadGenerator.generate_request` | Partial | Tested via run loop, not isolated |
| `WorkloadGenerator.run` | Partial | Rejection tracking tested |
| `WorkloadGenerator._publish` | Partial | Error handling tested |
| `WorkloadGenerator.get_stats` | Yes | |
| `build_pipeline_mix_from_env` | Yes | Env var wiring |
| `load_config` | No | YAML loading never tested |
| `WorkloadGenerator._tag_warmup_in_csv` | No | Warmup CSV tagging never tested |

**Gaps:**
- `load_config` (YAML parsing) is untested.
- `_tag_warmup_in_csv` is untested.
- Poisson inter-arrival timing accuracy is untested (only rate verified via stats).

---

### 1.17 `src/broker/models.py` -- Pydantic models

**Test file:** No dedicated tests. Covered indirectly by broker/worker tests.

**Gaps:** `PipelineState.__post_init__` (all_stages computation) is never directly tested.

---

### 1.18 `src/broker/kafka_consumer.py` -- Kafka consumer

**Test file:** `test_kafka_transport.py`

**Gaps:** Concurrent consumer logic tested with mocks.

---

### 1.19 `embeddings.py` -- (listed in task, does not exist yet)

This module does not exist in the codebase. No tests needed until it is created.

---

## 2. Integration Test Gaps

### What exists:
- `test_handle_result.py` -- Tests broker result handling with mocked dispatch (broker + pipeline DAG walking). This is the closest to integration-level for the core dispatch loop.
- `test_csv_integration.py` -- MetricsCollector to CSV round-trip (measurement + file I/O).
- `test_static_broker_fairness.py` -- Tests that StaticBroker has the same infrastructure as NeuralBroker (broker + federation + health checks), but uses mocks.

### What is missing:
1. **Broker + Worker end-to-end (in-process):** No test creates a real StaticBroker and real Worker in the same process, submits a pipeline via `/publish`, and verifies that stages execute and results flow back. This would catch dispatch payload mismatches, URL construction bugs, and result-handling edge cases without Docker.
2. **Placement + Dispatch integration:** No test verifies that `find_placement` output feeds correctly into `_dispatch_stage` (e.g., that the placement dict keys match the DAG stage IDs the dispatch loop expects).
3. **Federation round-trip (in-process):** No test creates two SummaryPropagators, has them exchange summaries, and verifies that `federated_route` uses the received summaries for routing. The propagator, summary, and routing modules are tested in isolation but never wired together.
4. **Workload + Broker (in-process):** The WorkloadGenerator is never tested against a real broker (even an in-process one). All broker interactions use mocks.

---

## 3. System Test Gaps

### What exists:
- `test_system.py` -- 7 Docker-based system tests (baseline CSV, env propagation, failure injection, cleanup, seed determinism, schema consistency). All marked `@pytest.mark.integration` and require Docker.
- `test_integration.py` -- 3 skeleton tests for worker failure, broker failure, and network partition. All skip with `pytest.skip()` (TODO).

### What is missing:
1. **Multi-domain federation end-to-end:** No system test verifies that two brokers in different domains exchange summaries and route pipelines across domains. `test_integration.py::test_broker_failure_proxy_recovery` is a skeleton.
2. **Kafka transport system test:** No test spins up Docker with Kafka and verifies the Kafka transport path end-to-end.
3. **NeuralBroker system test:** No system test specifically exercises the neural placement engine (S3) as opposed to the static broker (S1/S2). `test_system.py::TestCSVSchemaConsistencyAcrossPhases` mentions "neural" config but the assertion is only about CSV schema, not placement quality.
4. **Funnel resilience system test:** No Docker test exercises the wait/proceed/abort modes under actual container failures. `test_funnel_resilience.py` is unit-level only.

---

## 4. Tests Needed for New Modules

### 4.1 `src/broker/market.py` -- Market clearing engine

**Unit tests needed:**

1. **Bid/ask data structures:** Verify bid creation with price, quantity, agent ID, constraints.
2. **Clearing mechanism:** Given a set of bids and asks, verify that the market clears at the correct equilibrium price and allocates resources correctly.
3. **Individual rationality:** No agent pays more than their bid or receives less than their ask.
4. **Budget balance:** Total payments equal or exceed total receipts (weak budget balance).
5. **Allocation feasibility:** Cleared allocations do not exceed available capacity.
6. **Empty market:** Zero bids or zero asks produces no allocation.
7. **Single bid/ask:** Degenerate case with one participant per side.
8. **Tie-breaking:** When multiple bids have the same price, verify deterministic ordering.
9. **Integration with placement:** Verify that market output (resource allocations) maps correctly to `ExecutionUnit` assignments that the placement solver can use.
10. **Incentive compatibility:** Verify that truthful bidding is a dominant strategy (if using VCG or similar mechanism), or document the mechanism's properties.

### 4.2 `src/broker/governance.py` -- Two-level governance

**Unit tests needed:**

1. **Local governance rules:** Verify that stage-level constraints (data sovereignty, slice requirements) are enforced.
2. **Domain-level governance:** Verify that cross-domain data flow rules are enforced (trust levels, data type restrictions).
3. **Two-level composition:** Verify that local rules compose correctly with domain rules (i.e., a stage that passes local governance but fails domain governance is correctly blocked).
4. **Governance policy loading:** Verify that governance rules can be configured via policy objects or config files.
5. **Governance violation recording:** Verify that violations are recorded in MetricsCollector for post-hoc analysis.
6. **Governance + market interaction:** Verify that market allocations that violate governance constraints are rejected before dispatch.
7. **Backward compatibility:** Verify that the existing `GovernancePolicy` from `placement.py` and `GovernanceConstraints` from `routing.py` integrate cleanly (or are superseded).

### 4.3 `src/measurement/gamma.py` -- Non-modularity gap measurement

**Unit tests needed:**

1. **Gamma computation:** Given a known DAG, placement, and topology, verify that the non-modularity gap (gamma) is computed correctly against a hand-calculated reference value.
2. **Zero gap:** A fully modular placement (all stages on one node, or all in same domain) should have gamma = 0.
3. **Maximum gap:** A pathologically fragmented placement should produce a predictable gamma value.
4. **Monotonicity:** Adding a domain crossing to a placement should not decrease gamma.
5. **Scaling:** Gamma computation should handle DAGs of varying sizes (1 stage, 100 stages).
6. **Integration with MetricsCollector:** Verify that gamma values are correctly recorded in the CSV export and aggregate metrics.
7. **Relationship to D_cross:** Verify the relationship between gamma (non-modularity gap) and D_cross (domain crossings) from the placement cost function. They should be consistent or the distinction should be documented.

---

## 5. Critical Gaps -- Where Bugs Could Invalidate Experiment Results

### CRITICAL-1: `PipelineTrace.network_latencies_ms` is untested

**Risk:** HIGH. This function computes per-edge network latency from timestamp records. It is used by analysis scripts to decompose end-to-end latency into network vs. compute components. A bug here (e.g., wrong stage ordering, missing edge, timestamp mismatch) would silently produce incorrect latency decomposition figures in the paper's results section. The function uses a heuristic (sort by first stage_start, then measure consecutive gaps) that may not correctly handle fan-in/fan-out DAG topologies.

**Recommended test:** Construct a PipelineTrace with known timestamps for a funnel DAG (3 sensors -> fuse -> decide) and verify that network latencies match expected inter-stage gaps.

### CRITICAL-2: NeuralBroker has zero unit tests

**Risk:** HIGH. The NeuralBroker is the primary experimental subject (S3). Its placement algorithm, health-check loop, federation forwarding, and dispatch-time recovery are tested only via Docker system tests that skip in CI. Any regression in the neural placement engine will go undetected until a full Docker run. The comparison between S1/S2 (StaticBroker) and S3 (NeuralBroker) is the paper's central hypothesis (H6), and S3 is the only broker without unit-level coverage.

**Recommended action:** Write unit tests for NeuralBroker's `_compute_placement`, focusing on the neural embedding + placement solver integration.

### CRITICAL-3: `federated_route` (full 5-step protocol) is untested

**Risk:** MEDIUM-HIGH. The complete federated routing protocol is never tested end-to-end. Individual steps (local routing, governance filter) are tested, but the composition (especially the interaction between local match, federation candidate selection, and governance filtering) is not. A bug in the step sequencing (e.g., governance filter applied before candidate selection, or local match threshold inconsistent with federation threshold) would cause incorrect routing decisions in multi-domain experiments.

### CRITICAL-4: `compress_summary` (second-level compression) is untested

**Risk:** MEDIUM. This function uses k-means to compress subscription summaries for bandwidth-efficient federation. If the super-cluster radius calculation is wrong, remote clusters could be missed (false negatives) or spuriously matched (false positives), corrupting routing accuracy (F1 score) measurements.

### CRITICAL-5: `AdaptationTracker` sub-phase decomposition is untested

**Risk:** MEDIUM. `detection_times_ms` and `replacement_times_ms` decompose adaptation time into detection and re-placement phases. These are reported separately in the paper (Section 5, Table 3). If the event matching logic (peek vs. pop, detection_complete marker) has a bug, the decomposition will be incorrect while total adaptation time remains correct, producing misleading results about where time is spent during recovery.

### CRITICAL-6: Dispatch-time recovery path is untested

**Risk:** MEDIUM. Both StaticBroker and NeuralBroker have dispatch-time recovery (if dispatch fails, evict worker, re-place on survivor, re-dispatch). This path is never tested. A bug here means that even though health-check recovery works correctly, dispatch failures during high-load periods could cascade into pipeline failures that the experiment attributes to the placement algorithm rather than to a recovery bug.

### CRITICAL-7: No in-process broker+worker integration test

**Risk:** MEDIUM. The HTTP payload format between broker dispatch and worker `/execute` endpoint is verified only through separate unit tests on each side. If the broker sends a field name that the worker's Pydantic model does not recognize (or vice versa), the error would only surface in Docker runs. An in-process integration test (ASGI transport, no network) would catch such mismatches cheaply.

---

## Summary Table

| Module | Unit Coverage | Critical Gaps |
|---|---|---|
| `pipeline/dag.py` | Good | `get_edge` untested |
| `pipeline/patterns.py` | Good | None |
| `broker/placement.py` | Good | Strategy dispatch, gamma term isolation |
| `broker/base.py` | Moderate | `_periodic_snapshot`, `/workers` endpoint |
| `broker/static_broker.py` | Moderate | Dispatch recovery, federation forward, funnel bypass |
| `broker/neural_broker.py` | **None** | **Entire module untested (CRITICAL-2)** |
| `broker/funnel_resilience.py` | Good | `find_funnel_predecessor_stages` isolation |
| `broker/models.py` | Indirect only | `PipelineState.__post_init__` |
| `worker/worker.py` | Excellent | `run()` lifecycle |
| `federation/propagation.py` | Good | Latency tracking assertions |
| `federation/summary.py` | Partial | **`create_summary`, `compress_summary` untested (CRITICAL-4)** |
| `federation/routing.py` | Partial | **`federated_route` untested (CRITICAL-3)**, `select_federation_candidates` untested |
| `federation/integrator.py` | Partial | `Integrator` class untested |
| `measurement/harness.py` | Moderate | **`network_latencies_ms` untested (CRITICAL-1)**, `domain_crossings`, sub-phase decomposition |
| `measurement/failure.py` | Good (mocked) | None |
| `measurement/warmup.py` | Excellent | None |
| `workload/generator.py` | Moderate | `load_config`, warmup tagging |
