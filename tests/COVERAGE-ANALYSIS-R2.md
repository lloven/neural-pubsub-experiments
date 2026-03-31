# Test Coverage Analysis -- Round 2

Generated: 2026-03-27

Review of: Block 1-5 test additions against COVERAGE-ANALYSIS.md gaps.

Total tests collected: 828

---

## 1. Unit Test Coverage per Source Module

| Source module | Test file(s) | Coverage level | Notes |
|---|---|---|---|
| `src/pipeline/dag.py` | `test_dag.py`, `test_complex_dag.py`, `test_pipeline_values.py` | **Good** | `get_edge` still untested. `value_budget` and `accepts_cost` newly covered by `test_pipeline_values.py`. Complex DAG structure (8-stage, 10-edge, non-tree) newly covered. |
| `src/pipeline/patterns.py` | `test_patterns.py` | **Good** | No change. |
| `src/broker/placement.py` | `test_placement.py`, `test_placement_compute_cost.py`, `test_placement_quality.py`, `test_market_placement.py` | **Good** | `compute_placement_cost` now tested with compute-time terms and gamma penalty in isolation (`test_placement_compute_cost.py`). `market_mode_placement` newly tested. Strategy dispatch (DP vs greedy) still untested. |
| `src/broker/base.py` | `test_handle_result.py`, `test_baselines.py`, `test_refactor.py` | **Moderate** | No change from R1. `_periodic_snapshot`, `/workers` endpoint still untested. |
| `src/broker/static_broker.py` | `test_baselines.py`, `test_static_broker_fairness.py`, `test_static_broker_slice_aware.py` | **Moderate** | No change. Dispatch recovery, federation forward, funnel bypass still untested. |
| `src/broker/neural_broker.py` | None | **None** | Still zero unit tests. CRITICAL-2 unresolved. |
| `src/broker/market.py` | `test_market_clearing.py`, `test_market_placement.py` | **Good** | NEW module, NEW tests. `WorkerBid`, `compute_clearing_prices`, `PriceSignal`, `should_trade_cross_domain`, `market_mode_placement` all covered. |
| `src/broker/models.py` | Indirect only | **Minimal** | No change. |
| `src/broker/funnel_resilience.py` | `test_funnel_resilience.py` | **Good** | No change. |
| `src/broker/kafka_consumer.py` | `test_kafka_transport.py` | **Moderate** | No change. |
| `src/worker/worker.py` | `test_worker_unit.py`, `test_worker_capabilities.py` | **Excellent** | `Tier`, `Capability`, `parse_capabilities`, `can_execute`, `resolve_compute_ms` newly covered with 16 tests. Backward compatibility tested. |
| `src/federation/propagation.py` | `test_propagation.py` | **Good** | No change. Latency tracking assertions still missing. |
| `src/federation/summary.py` | `test_federation.py` | **Partial** | No change. `create_summary`, `compress_summary` still untested. CRITICAL-4 unresolved. |
| `src/federation/routing.py` | `test_federation.py` | **Partial** | No change. `federated_route`, `select_federation_candidates` still untested. CRITICAL-3 unresolved. |
| `src/federation/integrator.py` | `test_federation.py` | **Partial** | No change. |
| `src/measurement/harness.py` | `test_measurement.py`, `test_csv_integration.py` | **Moderate** | No change. `network_latencies_ms`, `domain_crossings` still untested. CRITICAL-1 unresolved. |
| `src/measurement/failure.py` | `test_failure.py` | **Good** | No change. |
| `src/measurement/warmup.py` | `test_warmup.py` | **Excellent** | No change. |
| `src/workload/generator.py` | `test_workload_rejected.py` | **Moderate** | No change. |

### Modules with zero dedicated tests

| Module | Risk |
|---|---|
| `src/broker/neural_broker.py` | **HIGH** -- Central experimental subject (S3), paper's main hypothesis |
| `src/broker/models.py` | LOW -- Pydantic models, indirectly tested |
| `src/broker/__main__.py` | LOW -- Entry point |
| `src/worker/__main__.py` | LOW -- Entry point |
| `src/workload/__main__.py` | LOW -- Entry point |

### New modules that do NOT yet exist in `src/`

| Module | Status |
|---|---|
| `src/broker/governance.py` | **Does not exist.** Governance is currently embedded in `placement.py` (`GovernancePolicy`). `test_governance_composition.py` tests the existing `GovernancePolicy` class, not a standalone governance module. |
| `src/measurement/gamma.py` | **Does not exist.** No gamma measurement module. The gamma term is tested only within `compute_placement_cost` (as the `gamma` weight on `D_cross`). No standalone non-modularity gap computation. |

---

## 2. Integration Tests

### What exists

| Test file | Scope | Real I/O? |
|---|---|---|
| `test_handle_result.py` | Broker result handling + DAG walking | Mocked dispatch |
| `test_csv_integration.py` | MetricsCollector to CSV round-trip | File I/O |
| `test_static_broker_fairness.py` | StaticBroker infrastructure parity | Mocked |
| `test_market_placement.py` (NEW) | Market clearing + placement + budget rejection | In-memory, no HTTP |
| `test_governance_composition.py` (NEW) | GovernancePolicy + placement constraints across 4 scenarios | In-memory, no HTTP |

### What is still missing

1. **Broker + Worker end-to-end (in-process):** No test creates a StaticBroker and Worker in the same process via ASGI transport. CRITICAL-7 unresolved.
2. **Placement + Dispatch integration:** No test verifies that `find_placement` output keys match `_dispatch_stage` expectations.
3. **Federation round-trip (in-process):** Two SummaryPropagators exchanging summaries and feeding into `federated_route` is never tested.
4. **Workload + Broker (in-process):** WorkloadGenerator never tested against a real broker.
5. **Market + Broker integration:** `market_mode_placement` is tested in isolation, but no test verifies that a broker actually invokes it in market mode and dispatches accordingly.
6. **Governance + Market interaction:** No test verifies that market allocations violating governance are rejected before dispatch.

---

## 3. System Tests

### What exists

- `test_system.py`: 7 Docker-based tests (baseline CSV, env propagation, failure injection, cleanup, seed determinism, schema consistency).
- `test_integration.py`: 3 skeleton tests (all `pytest.skip`).

### What is still missing

1. Multi-domain federation end-to-end (Docker).
2. Kafka transport system test.
3. NeuralBroker-specific system test (S3 placement quality, not just CSV schema).
4. Funnel resilience system test under container failures.
5. Market-mode system test (price-based allocation in Docker).
6. Governance composition system test (4-scenario welfare comparison in Docker).

---

## 4. Assessment of the 7 Critical Gaps

### CRITICAL-1: `PipelineTrace.network_latencies_ms` untested

**Status: UNRESOLVED.**

None of the Block 1-5 tests address this. The function is still untested. A bug here silently corrupts latency decomposition figures in the paper.

### CRITICAL-2: NeuralBroker has zero unit tests

**Status: UNRESOLVED.**

No new tests target `NeuralBroker`. It remains the only broker without unit-level coverage. The comparison S1/S2 vs S3 is the paper's central hypothesis (H6).

### CRITICAL-3: `federated_route` (full 5-step) untested

**Status: UNRESOLVED.**

No new test addresses the complete federated routing protocol. Individual steps remain tested in isolation but never composed.

### CRITICAL-4: `compress_summary` untested

**Status: UNRESOLVED.**

No new test addresses second-level summary compression. Federation bandwidth optimization remains unverified.

### CRITICAL-5: `AdaptationTracker` sub-phase decomposition untested

**Status: UNRESOLVED.**

`detection_times_ms` and `replacement_times_ms` remain untested. Total adaptation time is tested; the two sub-phases are not.

### CRITICAL-6: Dispatch-time recovery path untested

**Status: UNRESOLVED.**

Both StaticBroker and NeuralBroker dispatch-time recovery (evict + re-place + re-dispatch on failure) remains untested.

### CRITICAL-7: No in-process broker+worker integration test

**Status: UNRESOLVED.**

No ASGI-transport integration test exists. HTTP payload format between broker and worker is verified only through separate unit tests.

### Summary of Critical Gap Resolution

| Gap | Addressed by Block 1-5? | Status |
|---|---|---|
| CRITICAL-1: `network_latencies_ms` | No | Open |
| CRITICAL-2: NeuralBroker unit tests | No | Open |
| CRITICAL-3: `federated_route` end-to-end | No | Open |
| CRITICAL-4: `compress_summary` | No | Open |
| CRITICAL-5: AdaptationTracker sub-phases | No | Open |
| CRITICAL-6: Dispatch-time recovery | No | Open |
| CRITICAL-7: Broker+Worker integration | No | Open |

**Observation:** The Block 1-5 tests are focused on the new market/governance/capability layer (which they cover well). They do not address any of the 7 pre-existing critical gaps from R1.

---

## 5. New Module Test Assessment

### 5.1 `src/broker/market.py` (exists, 145 LOC)

**Test coverage: Good.**

| Function / Class | Test file | Tested? |
|---|---|---|
| `WorkerBid` dataclass | `test_market_clearing.py` | Yes |
| `PriceSignal` | `test_market_clearing.py` | Yes |
| `PriceSignal.from_clearing_prices` | `test_market_clearing.py` | Yes |
| `compute_clearing_prices` | `test_market_clearing.py` | Yes (7 scenarios: single domain, excess supply, excess demand, two domains, multiple stage types, empty bids, no demand) |
| `should_trade_cross_domain` | `test_market_clearing.py` | Yes (4 scenarios: remote cheaper, local cheaper, tie, high WAN) |
| `market_mode_placement` | `test_market_placement.py` | Yes (4 scenarios: cheapest domain, cross-domain when price gap > WAN, budget rejection, no-budget legacy) |

**Remaining gaps in market.py testing:**

- No test for **individual rationality**: i.e., verifying that no worker is paid less than its bid.
- No test for **budget balance**: total payments vs total receipts.
- No test for tie-breaking determinism when bids have identical costs.
- No test for `compute_clearing_prices` with a single bid and demand > 1 (the current code sets price to the one bid; should test that supply-constrained behavior is correct).
- No test for `market_mode_placement` with governance constraints (governance + market interaction).

### 5.2 `src/broker/governance.py` (DOES NOT EXIST)

Governance is implemented within `src/broker/placement.py` as `GovernancePolicy`. The `test_governance_composition.py` file tests `GovernancePolicy` across 4 scenarios (neither/d1_only/d2_only/both), including trust levels and supermodularity metric structure.

**Coverage of governance-related functionality:**

| Aspect | Tested? | Notes |
|---|---|---|
| `GovernancePolicy.local_stage_types` | Yes | `test_governance_composition.py` |
| `GovernancePolicy.get_trust` (symmetric) | Yes | `test_governance_composition.py` |
| 4 governance composition scenarios | Yes | Structural tests |
| Supermodularity metric computation | Yes | With mock welfare values |
| Governance violation recording in MetricsCollector | No | Not tested |
| Two-level governance (local + domain rules) | No | Only single-level tested |
| Governance + market rejection | No | Not tested |

### 5.3 `src/measurement/gamma.py` (DOES NOT EXIST)

The gamma term is tested only as a cost weight in `compute_placement_cost` (via `test_placement_compute_cost.py::test_domain_crossing_penalty_independent`). There is no standalone gamma measurement module and no test for:

- Non-modularity gap computation from a known DAG + placement + topology.
- Zero gamma for fully modular placements.
- Monotonicity (adding a domain crossing does not decrease gamma).
- Scaling behavior.
- Integration with MetricsCollector CSV export.

---

## 6. Edge Case Coverage

| Edge case | Tested? | Where |
|---|---|---|
| Empty worker pool | Yes | `test_baselines.py::test_static_broker_no_workers` (raises `RuntimeError`) |
| All workers busy / at capacity | No | No test submits work when all workers are at `current_load == capacity` |
| Network timeouts (propagation) | Yes | `test_propagation.py::test_network_timeout_records_failure` |
| Network timeouts (dispatch) | No | Dispatch-time HTTP timeout handling untested |
| Malformed requests | Partial | `test_propagation.py` tests malformed responses; no test for malformed `/publish` or `/execute` payloads |
| Concurrent pipeline submissions | No | No test submits multiple pipelines simultaneously and verifies correct interleaving |
| Pipeline with single stage | Yes | `test_placement.py` tests single-stage placement |
| Pipeline with zero stages | No | Not tested |
| Worker deregistration during active pipeline | No | Not tested |
| Market clearing with all identical bids | No | Tie-breaking determinism untested |
| `value_budget = 0` (accept nothing) | Yes | `test_pipeline_values.py::test_value_budget_zero_is_valid` |
| Extremely large DAG (100+ stages) | No | Not tested |

---

## 7. What Block 1-5 Tests Added (Summary)

| Test file | Tests | What it covers |
|---|---|---|
| `test_worker_capabilities.py` | 16 | Tier/Capability parsing, compute-time resolution, stage rejection, backward compatibility |
| `test_placement_compute_cost.py` | 4 | `compute_placement_cost` with compute-time terms, remote-primary vs local-secondary tradeoff, gamma independence, backward compatibility |
| `test_market_clearing.py` | 14 | `WorkerBid`, `compute_clearing_prices` (7 scenarios), `PriceSignal`, `should_trade_cross_domain` (4 scenarios) |
| `test_pipeline_values.py` | 7 | `PipelineDAG.value_budget`, `accepts_cost` |
| `test_market_placement.py` | 4 | `market_mode_placement` with price-based allocation, cross-domain trade, budget rejection |
| `test_complex_dag.py` | 10 | 8-stage entangled DAG structure, non-tree detection, fan-out, diamond, cross-tree fan-in, topological sort |
| `test_governance_composition.py` | 12 | 4 governance scenarios, policy enforcement, trust symmetry, supermodularity metric, experiment matrix integration |

**Total new tests: ~67**

These tests solidly cover the new market-governance-capability layer. They do not address the pre-existing critical gaps (CRITICAL-1 through CRITICAL-7).

---

## 8. Prioritised Missing Tests

### Priority 1 -- Blocks paper results if wrong

| # | Test | Addresses | Effort |
|---|---|---|---|
| 1 | `PipelineTrace.network_latencies_ms` with funnel DAG timestamps | CRITICAL-1 | Small |
| 2 | `AdaptationTracker.detection_times_ms` and `replacement_times_ms` with known event sequences | CRITICAL-5 | Small |
| 3 | `PipelineTrace.domain_crossings` with multi-domain placement | CRITICAL-1 (related) | Small |
| 4 | `MetricsCollector.export_json` round-trip | R1 gap | Small |

### Priority 2 -- Risks silent regression in core experiment logic

| # | Test | Addresses | Effort |
|---|---|---|---|
| 5 | NeuralBroker `_compute_placement` unit test (mock embeddings, verify placement output) | CRITICAL-2 | Medium |
| 6 | `federated_route` full 5-step protocol with mock summaries | CRITICAL-3 | Medium |
| 7 | `select_federation_candidates` in isolation | CRITICAL-3 (related) | Small |
| 8 | `compress_summary` with known clusters, verify super-cluster radius | CRITICAL-4 | Medium |
| 9 | `create_summary` from mock cluster state | CRITICAL-4 (related) | Small |
| 10 | Dispatch-time recovery: mock dispatch failure, verify evict + re-place + re-dispatch | CRITICAL-6 | Medium |

### Priority 3 -- Integration gaps that catch payload mismatches

| # | Test | Addresses | Effort |
|---|---|---|---|
| 11 | In-process broker + worker via ASGI transport (submit pipeline, verify completion) | CRITICAL-7 | Medium |
| 12 | Market-mode broker integration (broker invokes `market_mode_placement`, dispatches, collects results) | New gap | Medium |
| 13 | Governance + market: market allocation violating governance is rejected | New gap | Small |
| 14 | Federation round-trip: two propagators exchange summaries, `federated_route` uses them | R1 gap | Large |

### Priority 4 -- Edge cases and robustness

| # | Test | Addresses | Effort |
|---|---|---|---|
| 15 | All workers at capacity (submit pipeline, verify graceful failure/queueing) | Edge case | Small |
| 16 | Concurrent pipeline submissions (2+ pipelines, verify no state corruption) | Edge case | Medium |
| 17 | Malformed `/publish` payload (missing fields, wrong types) | Edge case | Small |
| 18 | Worker deregistration during active pipeline execution | Edge case | Medium |
| 19 | Market clearing with identical bids (tie-breaking determinism) | Market gap | Small |
| 20 | `_periodic_snapshot` CSV export (verify file created, correct schema) | R1 gap | Small |

### Priority 5 -- System tests (require Docker)

| # | Test | Addresses | Effort |
|---|---|---|---|
| 21 | NeuralBroker system test (S3 placement quality, not just CSV schema) | CRITICAL-2 (system) | Large |
| 22 | Multi-domain federation end-to-end | R1 gap | Large |
| 23 | Market-mode system test | New gap | Large |
| 24 | Governance composition 4-scenario welfare comparison | New gap | Large |
| 25 | Kafka transport end-to-end | R1 gap | Large |

---

## 9. Overall Assessment

**Strengths:**
- The new Block 1-5 tests add 67 tests covering the market clearing engine, worker capabilities, pipeline value budgets, governance composition, and complex DAG structures. This is solid coverage for the new functionality.
- The market clearing tests include good boundary cases (empty, excess supply, excess demand, multi-domain, no demand).
- The governance composition tests structurally verify all 4 TEAC scenarios and include the supermodularity metric computation.
- The complex DAG test verifies structural properties (non-tree, fan-out, diamonds, cross-tree fan-in) that are prerequisites for the non-modularity gap argument.

**Weaknesses:**
- All 7 critical gaps from R1 remain open. The Block 1-5 tests are orthogonal to these gaps.
- The two planned new modules (`governance.py`, `gamma.py`) do not exist as standalone source files. The tests reference existing functionality in `placement.py`.
- No integration test exercises the new market/governance layer end-to-end through the broker dispatch path.
- Edge case coverage is thin: concurrent submissions, capacity exhaustion, and dispatch-timeout handling are untested.

**Recommendation:** Address Priority 1 items first (4 small tests, ~2 hours). These directly protect paper results. Then address Priority 2 items (6 tests, medium effort) to cover the core experiment logic. Priority 3-5 can be tackled incrementally.
