# Dual-Transport Factorial Experiment — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development for all implementation. Use superpowers:requesting-code-review after each task.

**Goal:** Run all placement baselines (S2, S3, S4) under both transport modes (direct HTTP, Kafka) to create a two-factor experiment that proves transport orthogonality by construction and isolates placement intelligence as the sole determinant of performance.

**Architecture:** Add a configurable transport layer (`TRANSPORT=http|kafka`) to BaseBroker. In HTTP mode, the broker POSTs directly to the placed worker (existing behaviour). In Kafka mode, the broker publishes to a Kafka topic with the target worker embedded; a concurrent sidecar consumer reads and dispatches. Placement logic is unchanged in both modes.

**Experimental design:** 3 (placement) × 2 (transport) factorial. Two-factor ANOVA tests whether transport interacts with placement. Expected: no significant interaction, confirming transport orthogonality.

**Tech Stack:** Python 3.12, aiokafka, FastAPI/uvicorn, Docker Compose, pytest

**Skills:** TDD (strict RED-GREEN-REFACTOR), code-review after each task.

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/broker/base.py` | Modify | Add `transport` param; `_dispatch_stage()` branches HTTP vs Kafka |
| `src/broker/kafka_consumer.py` | Rewrite | Concurrent consumer, reads `target_url` from message |
| `src/broker/kafka_broker.py` | Delete | Replaced by transport flag in BaseBroker |
| `src/broker/static_broker.py` | Minor | Remove `_dispatch_stage()` override (inherits dual-transport from base) |
| `src/broker/neural_broker.py` | Modify | Remove `_dispatch_stage()` override; keep placement, federation, governance, health monitoring |
| `docker-compose.local.yaml` | Keep | HTTP-only base (no Kafka services) |
| `docker-compose.kafka.yaml` | Rewrite | Overlay that adds Kafka + consumer + `TRANSPORT=kafka` env |
| `docker-compose.flat.yaml` | Review | Add consumer to flat network when Kafka overlay active |
| `scripts/_common.py` | Modify | Drop A1/C1; add transport dimension to config resolution |
| `scripts/run_phase_a.py` | Modify | 3 configs × 2 transports × 3 rates × 5 seeds = 90 runs |
| `scripts/run_phase_b.py` | Modify | 4 configs × 2 transports × 5 seeds = 40 runs |
| `tests/test_kafka_transport.py` | Create | Concurrent consumer, placement-from-message, transport switching |
| `tests/test_orchestration.py` | Modify | Update config resolution, add transport dimension tests |
| `txt/Evaluation.tex` | Modify | Drop S1, add transport factor, add ANOVA paragraph |

---

## Task 1: Concurrent Kafka Consumer with Placement-from-Message

**Files:**
- Create: `tests/test_kafka_transport.py`
- Rewrite: `src/broker/kafka_consumer.py`

- [ ] **Step 1.1: Write failing test — consumer dispatches to target worker from message**

```python
class TestKafkaConsumerPlacement:
    def test_dispatches_to_target_url_from_message(self):
        """Consumer reads target_url from message and dispatches there."""
```

- [ ] **Step 1.2: Run test — verify RED**
- [ ] **Step 1.3: Write failing test — concurrent dispatch**

```python
    def test_dispatches_concurrently_not_sequentially(self):
        """10 messages × 1s each should complete in ~1s, not ~10s."""
```

- [ ] **Step 1.4: Run test — verify RED**
- [ ] **Step 1.5: Write failing test — bounded concurrency**

```python
    def test_bounds_concurrent_dispatches_with_semaphore(self):
        """Max concurrent dispatches capped by semaphore."""
```

- [ ] **Step 1.6: Run tests — verify RED**
- [ ] **Step 1.7: Implement concurrent consumer**

Rewrite `kafka_consumer.py`:
- Read `target_url` from Kafka message payload
- `asyncio.create_task()` per message with `asyncio.Semaphore(max_concurrent=20)`
- Fall back to round-robin if `target_url` absent (transition safety)

- [ ] **Step 1.8: Run tests — verify GREEN**
- [ ] **Step 1.9: Commit**

---

## Task 2: Dual-Transport Dispatch in BaseBroker

**Files:**
- Modify: `src/broker/base.py`
- Modify: `src/broker/static_broker.py`
- Append: `tests/test_kafka_transport.py`

- [ ] **Step 2.1: Write failing test — BaseBroker accepts transport param**

```python
class TestDualTransport:
    def test_base_broker_accepts_transport_http(self):
        """BaseBroker(transport='http') dispatches via direct HTTP POST."""

    def test_base_broker_accepts_transport_kafka(self):
        """BaseBroker(transport='kafka') publishes to Kafka with target_url."""
```

- [ ] **Step 2.2: Run tests — verify RED**
- [ ] **Step 2.3: Write failing test — Kafka message contains target_url**

```python
    def test_kafka_message_embeds_placement(self):
        """Kafka message must contain target_worker and target_url from placement."""
```

- [ ] **Step 2.4: Run test — verify RED**
- [ ] **Step 2.5: Implement dual-transport in BaseBroker**

Add to `BaseBroker.__init__()`:
- `self.transport: str` — `"http"` (default) or `"kafka"`
- `self.kafka_bootstrap: str | None`

`_dispatch_stage()` implementation:
```python
if self.transport == "kafka":
    # Publish to Kafka topic with target_url in payload
    await self._producer.send_and_wait(topic, value={...target_url...})
else:
    # Direct HTTP POST to worker (existing path)
    await client.post(f"{worker.url}/execute", json=payload)
```

`build_app()` startup:
- If `transport == "kafka"`: create AIOKafkaProducer
- If `transport == "http"`: create httpx.AsyncClient (existing)

Remove `_dispatch_stage()` from StaticBroker (inherits base).

- [ ] **Step 2.6: Run all tests — verify GREEN**
- [ ] **Step 2.7: Commit**

---

## Task 3: Adapt NeuralBroker to Dual Transport

**Files:**
- Modify: `src/broker/neural_broker.py`
- Append: `tests/test_kafka_transport.py`

- [ ] **Step 3.1: Write failing test — NeuralBroker uses transport param**

```python
class TestNeuralBrokerTransport:
    def test_neural_broker_http_dispatch(self):
        """NeuralBroker(transport='http') dispatches via HTTP."""

    def test_neural_broker_kafka_dispatch(self):
        """NeuralBroker(transport='kafka') dispatches via Kafka."""
```

- [ ] **Step 3.2: Run tests — verify RED**
- [ ] **Step 3.3: Remove NeuralBroker._dispatch_stage() override**

Keep: `_compute_placement()`, federation, governance, health monitoring, retry logic.
Lose: direct HTTP dispatch (now handled by base class transport switch).
Keep `self._http_client` for federation peer forwarding and health probes only.

- [ ] **Step 3.4: Run all tests — verify GREEN**
- [ ] **Step 3.5: Commit**

---

## Task 4: Rewrite Kafka Compose Overlay + Delete KafkaBroker

**Files:**
- Delete: `src/broker/kafka_broker.py`
- Rewrite: `docker-compose.kafka.yaml` — now adds Kafka services + sets `TRANSPORT=kafka`
- Keep: `docker-compose.local.yaml` — HTTP-only base (no change)

- [ ] **Step 4.1: Write failing test — compose overlay structure**

```python
class TestComposeOverlays:
    def test_local_yaml_has_no_kafka(self):
        """Base compose is HTTP-only; no kafka service."""

    def test_kafka_yaml_adds_kafka_and_sets_transport(self):
        """Kafka overlay adds kafka + kafka-consumer services and TRANSPORT=kafka."""
```

- [ ] **Step 4.2: Run tests — verify RED**
- [ ] **Step 4.3: Rewrite docker-compose.kafka.yaml**

New overlay:
- `kafka` service (apache/kafka:3.7.0, KRaft, healthcheck)
- `kafka-consumer` service (concurrent consumer, all slice networks)
- Override `broker-d1` and `broker-d2`: add `TRANSPORT: kafka`, `KAFKA_BOOTSTRAP: kafka:9092`, `depends_on: kafka`

Delete `src/broker/kafka_broker.py`.

- [ ] **Step 4.4: Run all tests — verify GREEN**
- [ ] **Step 4.5: Commit**

---

## Task 5: Transport Dimension in Orchestration Scripts

**Files:**
- Modify: `scripts/_common.py`
- Modify: `scripts/run_phase_a.py`
- Modify: `scripts/run_phase_b.py`
- Modify: `scripts/run_phase_c.py`
- Modify: `tests/test_orchestration.py`

- [ ] **Step 5.1: Write failing tests — config table with transport dimension**

```python
class TestTransportDimension:
    def test_phase_a_generates_http_and_kafka_variants(self):
        """Phase A: 3 placements × 2 transports = 6 configs per rate."""

    def test_resolve_config_kafka_adds_overlay(self):
        """resolve_config('A2', transport='kafka') includes kafka overlay."""

    def test_resolve_config_http_uses_base_only(self):
        """resolve_config('A2', transport='http') uses base compose only."""

    def test_config_table_has_no_A1_or_C1(self):
        """Old Kafka-only baselines removed."""
```

- [ ] **Step 5.2: Run tests — verify RED**
- [ ] **Step 5.3: Implement transport dimension**

Update `_CONFIG_TABLE`:
```python
# Drop A1. Configs A2, A3, A4 remain. Transport is a separate dimension.
_CONFIG_TABLE = {
    "A2": {"env": {"BROKER_MODULE": "...", "PLACEMENT": "round_robin"}},
    "A3": {"env": {"BROKER_MODULE": "...", "PLACEMENT": "random"}},
    "A4": {"env": {}},  # NeuralBroker default
}
TRANSPORTS = ["http", "kafka"]
```

`resolve_config(config, transport="http")`:
- If `transport == "kafka"`: add `docker-compose.kafka.yaml` to compose_files
- If `transport == "http"`: base compose only

Phase A run matrix: `for config in [A2,A3,A4]: for transport in [http,kafka]: for rate in [...]: for seed in [...]:`

Phase B: same transport loop around B1-B4.

- [ ] **Step 5.4: Run all tests — verify GREEN**
- [ ] **Step 5.5: Commit**

---

## Task 6: Update Manuscript

**Files:**
- Modify: `txt/Evaluation.tex`

Changes:
- Drop S1 from baselines list; renumber or keep S2/S3/S4
- Add transport factor:

> **Transport factor.** Each placement baseline is run under two transport modes: direct HTTP dispatch (broker posts stage assignments directly to workers) and Apache Kafka transport (broker publishes to Kafka topics; a concurrent consumer sidecar reads and dispatches to the placed worker). This 3×2 factorial design allows a two-factor ANOVA to test whether transport mode interacts with placement strategy.

- Add to results:

> **Transport orthogonality.** Two-factor ANOVA (placement × transport) shows no significant interaction ($F = ..., p > 0.05$). Kafka transport adds a constant overhead of $X \pm Y$\,ms per stage across all placement strategies. All subsequent analyses pool HTTP and Kafka runs unless otherwise noted.

- Update "Baseline fairness" paragraph to claim both transport and placement are controlled
- Update experiment run counts (Phase A: 90, Phase B: 40)

- [ ] **Step 6.1: Edit Evaluation.tex**
- [ ] **Step 6.2: Verify Overleaf compiles**
- [ ] **Step 6.3: Commit**

---

## Task 7: Smoke Tests (Local + Remote, Both Tiers)

Per L24 (multi-level smoke testing) and L39 (smoke on target host before full runs):

- [ ] **Step 7.1: Local unit smoke** (HTTP transport)

`./run-experiments.sh smoke --transport http`
~1 min. Catches imports, config resolution, basic pipeline completion.

- [ ] **Step 7.2: Local unit smoke** (Kafka transport)

`./run-experiments.sh smoke --transport kafka`
~1 min. Catches Kafka producer/consumer startup, message routing.

- [ ] **Step 7.3: Local extended smoke** (HTTP transport)

`./run-experiments.sh smoke --extended --transport http`
~30 min. All placement configs (A2/A3/A4), representative rates, 100 events.

- [ ] **Step 7.4: Local extended smoke** (Kafka transport)

`./run-experiments.sh smoke --extended --transport kafka`
~30 min. Same as above but through Kafka.

- [ ] **Step 7.5: Push to 5GTN remote + build Docker image**

- [ ] **Step 7.6: Remote unit smoke** (both transports)

`./run-experiments.sh --remote smoke --transport http`
`./run-experiments.sh --remote smoke --transport kafka`

- [ ] **Step 7.7: Remote extended smoke** (both transports)

`./run-experiments.sh --remote smoke --extended --transport http`
`./run-experiments.sh --remote smoke --extended --transport kafka`

---

## Task 8: Full Experiment Runs on VM

Only after ALL smoke tests pass (L24, L39).

- [ ] **Step 8.1: Run Phase A** (90 runs: 3 configs × 2 transports × 3 rates × 5 seeds, ~16h)
- [ ] **Step 8.2: Run Phase B** (40 runs: 4 configs × 2 transports × 5 seeds, ~26h)

---

## Verification

1. `pytest tests/ -v` — all tests pass
2. Smoke test on VM: both transport modes work, all placement configs complete
3. Phase A: all 90 runs complete with ~100% pipeline completion
4. Phase B: all 40 runs complete
5. Two-factor ANOVA: no significant interaction between transport and placement
6. Overleaf: manuscript compiles, factorial design reads cleanly
7. Code review (superpowers:requesting-code-review) after each task

---

## Design Alignment Analysis

### Two-Factor Experimental Design

| | S2 (round-robin) | S3 (random) | S4 (neural) |
|---|---|---|---|
| **Direct HTTP** | S2/HTTP | S3/HTTP | S4/HTTP |
| **Kafka** | S2/Kafka | S3/Kafka | S4/Kafka |

**Independent variables:**
1. Placement strategy (S2, S3, S4) — the paper's contribution
2. Transport mode (HTTP, Kafka) — controlled factor

**Dependent variables:** e2e latency, completion rate, throughput, routing accuracy

**Statistical test:** Two-factor ANOVA with interaction term
- Main effect (placement): expected significant — S4 outperforms S2/S3 in Phase B
- Main effect (transport): expected significant — Kafka adds ~5–10ms constant
- Interaction (placement × transport): expected NOT significant — proves orthogonality

### What This Proves That Single-Transport Cannot

1. **Transport orthogonality by construction:** If S4/HTTP − S2/HTTP ≈ S4/Kafka − S2/Kafka, placement quality is transport-independent. Mathematical proof, not assertion.
2. **Quantifies Kafka overhead exactly:** S2/Kafka − S2/HTTP across seeds gives precise transport cost.
3. **Eliminates ALL reviewer transport objections:** Both modes tested. No confound.
4. **Clean ANOVA table in the paper:** A single table proves the point concisely.

### Theory-to-Architecture Mapping

| Theory (§3–4) | Architecture | Experiment role |
|---|---|---|
| Placement $\pi: V \to \mathcal{N}$ | `_compute_placement()` | **Factor 1** (S2/S3/S4) |
| Stage dispatch | `_dispatch_stage()` | **Factor 2** (HTTP/Kafka) |
| Pipeline DAG | `PipelineDAG` | Constant across all cells |
| Feasibility constraints | Capacity/latency checks | Constant |
| Federation | `SummaryPropagator` | Only S4 (Phase C) |
| Governance | `GovernancePolicy` | Only B3/B4 |

### Manuscript Impact

The "Baseline fairness" paragraph becomes:

> "All baselines share the same worker implementation, pipeline DAG representation, measurement harness, and workload generator. The experiment varies two factors independently: (1) placement algorithm (S2, S3, S4) and (2) transport mode (direct HTTP, Apache Kafka). A two-factor ANOVA confirms that transport does not interact with placement ($p > 0.05$), validating the architectural separation between the routing/placement layer (this paper's contribution) and the underlying message transport."

This is the strongest possible experimental fairness claim.
