# Neural Pub/Sub Experiment Plan

**Paper:** Neural Pub/Sub: A Semantic Interconnect for Agentic AI in the Computing Continuum (target: IEEE JSAC or Elsevier DCN)
**Location:** Nakao Lab, University of Tokyo (4-week visit starting March 2026)
**Testbed:** Tiered deployment (see below)

---

## Overview

The experiment validates the distribution architecture (paper Section 4) by deploying federated Neural Pub/Sub broker instances and comparing against Kafka and static-placement baselines.

**Development principle: local-first.** All software is developed, containerised, and smoke-tested on a local Docker Compose environment that emulates the multi-domain, multi-slice testbed. The experiment is designed to produce scientifically valid results at every deployment tier, with higher tiers adding credibility rather than changing the scientific conclusions.

### Deployment tiers

The experiment runs on a tiered deployment model. Each tier is self-sufficient for producing publishable results. Higher tiers add realism and reviewer credibility but do not change the algorithmic evaluation.

| Tier | Platform | What it validates | Status |
|------|----------|-------------------|--------|
| **T1: Local Docker Compose** | Laptop (Docker Desktop) | Placement algorithm, baselines, slice emulation (`tc qdisc`), failure injection, federation (emulated WAN). All phases (A-D) with full statistical methodology. | **Ready now** |
| **T2: Multi-machine (same site)** | Nakao Lab compute nodes OR any UOulu server | Real network hops, CPU isolation, realistic TCP overhead. Same phases as T1 but on separate hardware. | Pending testbed access |
| **T3: Cross-site federation** | Laptop (Tokyo) + 5GTN VM (Oulu) | Real WAN latency (Oulu-Tokyo via GEANT/NORDUnet/SINET), real internet path variability. Phase C federation and Phase D partition recovery. | **Access confirmed (2026-03-20).** Username `lloven`, Pomerium SSH. Credentials set up, deployment scripts ready. Hostname confirmation and mTLS CA plan pending. |
| **T4: 5G/6G testbed** | Nakao Lab Local6G + 5GTN | Real 5G infrastructure context. No S-NSSAI slicing available; best-effort LAN. Adds deployment credibility for paper. GEANT L2VPN comparison possible (Aleksi offered). | Opportunistic; 5GTN confirmed no RIC, no slicing |

**Key insight (2026-03-19):** Since we have no UEs and do not exercise the air interface, real 5G/6G slices provide no QoS differentiation to our containerised workloads. Slice QoS is emulated via `tc qdisc` at all tiers. The testbeds' value is as compute platforms with physical network separation, not as 5G infrastructure. Any multi-machine setup (including a plain UOulu Linux server) provides the same scientific value for Phases A-D.

**Cross-site fallback chain:**
1. **Best:** Laptop (Tokyo) + 5GTN VM (Oulu) via SINET/GEANT/NORDUnet/FUNET — real cross-site with academic backbone
2. **Good:** Laptop (Tokyo) to any UOulu server via internet
3. **Acceptable:** Local Docker Compose with calibrated WAN delay (`tc netem`, RTT measured from Tokyo to Oulu via `ping`)

All three produce valid Phase C results. The difference is reviewer perception, not scientific validity.

**Bonus (2026-03-20):** Phase A (single-site baselines) can also run on the 5GTN VM for a "real infrastructure" comparison against the local Docker Desktop results. The VM has dedicated CPU (no desktop app contention), giving cleaner measurements. Run Phase A on both platforms and compare; if results are consistent, the Docker Desktop methodology is validated.

The Neural Router single-broker code exists in `Experiments/neural-router/src/` (router.py, embeddings.py, llm.py, llm_async.py, evaluation.py). This experiment extends it with federation, placement, and deployment infrastructure.

---

## Phase 0: Local Development and Smoke Testing (Week 1-2)

Everything in this phase runs on the laptop (or any machine with Docker). No testbed access required.

### 0.1 Local emulation environment (days 1-2)

Create a Docker Compose setup that emulates the full multi-domain, multi-slice testbed:

```yaml
# docker-compose.local.yaml
services:
  # Domain 1 (emulating Tokyo)
  broker-d1:
    build: .
    command: python -m src.broker.neural_broker --domain d1 --config /config/domain_d1.yaml
    networks: [slice-nearrt-d1, slice-edge-d1, federation]

  worker-d1-nearrt-1:
    build: .
    command: python -m src.worker --node-id d1-nearrt-1 --slice nearrt
    networks: [slice-nearrt-d1]

  worker-d1-nearrt-2:
    build: .
    command: python -m src.worker --node-id d1-nearrt-2 --slice nearrt
    networks: [slice-nearrt-d1]

  worker-d1-edge-1:
    build: .
    command: python -m src.worker --node-id d1-edge-1 --slice edge
    networks: [slice-edge-d1]

  # Domain 2 (emulating Oulu)
  broker-d2:
    build: .
    command: python -m src.broker.neural_broker --domain d2 --config /config/domain_d2.yaml
    networks: [slice-nearrt-d2, slice-edge-d2, federation]

  worker-d2-nearrt-1:
    build: .
    command: python -m src.worker --node-id d2-nearrt-1 --slice nearrt
    networks: [slice-nearrt-d2]

  worker-d2-edge-1:
    build: .
    command: python -m src.worker --node-id d2-edge-1 --slice edge
    networks: [slice-edge-d2]

  # Kafka baseline
  kafka:
    image: apache/kafka:latest
    networks: [slice-nearrt-d1, slice-edge-d1, federation]

  # Workload generator
  workload:
    build: .
    command: python -m src.pipeline.workload --config /config/workload.yaml
    networks: [federation]

networks:
  slice-nearrt-d1:    # Near-RT RIC slice, domain 1
  slice-edge-d1:      # Edge slice, domain 1
  slice-nearrt-d2:    # Near-RT RIC slice, domain 2
  slice-edge-d2:      # Edge slice, domain 2
  federation:         # Cross-domain overlay (emulates WAN)
```

**WAN emulation:** Use `tc qdisc` (via Docker network options or a sidecar container) to inject realistic latency on the `federation` network:
- Intra-domain: 1-5ms (local network)
- Cross-domain: 150-200ms RTT (emulating Tokyo-Oulu, to be calibrated from actual measurement)

**Slice QoS emulation:** Use `tc` on slice networks to enforce:
- `slice-nearrt-*`: 1ms latency, 1Gbps bandwidth (near-RT RIC)
- `slice-edge-*`: 5ms latency, 100Mbps bandwidth (edge)

### 0.2 Containerisation (days 1-2)

Single Dockerfile for all components (broker, worker, workload generator):

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ src/
COPY configs/ configs/
# Embedding model downloaded at build time (avoids runtime download)
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"
ENTRYPOINT ["python"]
```

- [x] Dockerfile created (Python 3.11-slim, pre-downloads embedding model)
- [x] docker-compose.local.yaml created (2 domains, 5 networks, 8 services)
- [x] Domain configs created (`configs/domain_d1.yaml`, `configs/domain_d2.yaml`, `configs/workload.yaml`)
- [x] `__main__.py` entry points for `src.worker` and `src.workload` packages (verified with `--help`)
- [x] Fixed compose workload command (`--rate` → `--arrival-rate`)
- [x] `requirements-dev.txt` split from `requirements.txt` (docker/pytest excluded from image)
- [x] Build and verify: `docker build -t npubsub .` (image builds, pre-downloads embedding model)
- [x] `src/broker/__main__.py` created (enables `python -m src.broker`)
- [x] Test single-container broker: health, register, publish, deregister endpoints all working
- [x] Test compose-up: full 2-domain stack (2 brokers, 5 workers, workload generator) passes Level 2
- [x] Fix: `b""` in `_dispatch_stage` payload → `""` (bytes not JSON-serialisable)
- [x] Final compose run: 21/22 pipelines completed, mean latency 1515ms, per-stage breakdown recorded

### 0.3 Software development (days 1-7)

#### 0.3.1 Core: Federation layer

Implements Section 4.2 (Broker Federation):

- [x] **Pipeline DAG representation** (`src/pipeline/dag.py`): Service-dependency DAG with stages, demands (`rho_v`), data rates (`omega_v`). Map and funnel patterns. Fixed `is_tree()` to correctly identify fan-in (funnel) DAGs as DP-eligible.
- [x] **Subscription summary** (`src/federation/summary.py`, Section 4.2.2, Eq. 7): Each broker exports `{centroid_embedding, radius, available_capacity}` per cluster. Serialisable via msgpack.
- [x] **Summary propagation** (`src/federation/propagation.py`): Periodic push to federation peers over HTTP. Configurable interval `delta_prop`.
- [x] **Cross-domain routing protocol** (`src/federation/routing.py`, Section 4.2.3): 5-step protocol: embed, candidate selection, governance filter, forward, response aggregation.
- [x] **Integrator encapsulation** (`src/federation/integrator.py`, Section 4.2.4, Eq. 8): Compute composite max-flow capacity per domain.
- [x] **Neural broker** (`src/broker/neural_broker.py`): Wraps Neural Router core + federation layer. HTTP API for: publish, subscribe, get_summary, receive_forwarded. Full pipeline lifecycle management.
- [x] **Placement algorithm** (`src/broker/placement.py`, Section 4.3, Eq. 10): Weighted cost (latency + utilisation + domain crossings). DP for tree/funnel pipelines, greedy for general DAGs. Constraint checking: capacity, latency, governance, slice.

#### 0.3.2 Core: Worker and workload

- [x] **Worker process** (`src/worker/worker.py`): Receives stage assignments from broker, executes simulated compute (configurable processing time), reports results back. Registers with broker on startup. FastAPI server.
- [x] **Workload generator** (`src/workload/generator.py`): Poisson arrivals, 3 pipeline templates (CQI prediction, anomaly detection, sensor fusion). Configurable rate, complexity, YAML config.
- [x] **Measurement harness** (`src/measurement/harness.py`): Timestamp injection at each stage. Latency decomposition (end-to-end, per-stage, network). Throughput counter. Federation bandwidth monitor. Adaptation tracker.
- [x] **Failure injection** (`src/measurement/failure.py`): Kill worker containers, kill broker containers, partition networks (via `docker network disconnect`). Scenario runner with configurable delay/duration and auto-recovery. 12 unit tests (mocked Docker client).

### 0.4 Local smoke tests (days 5-8)

**Multi-level smoke testing** (per L24 in lessons.md):

#### Level 1: Unit tests (no Docker, no network) — ✅ COMPLETE (54/54 passing)
- [x] `test_federation.py`: SubscriptionSummary serialisation roundtrip, routing, governance filter, composite capacity
- [x] `test_dag.py`: DAG construction, map/funnel patterns, topological sort, is_tree (fan-in vs fan-out)
- [x] `test_placement.py`: Placement algorithm on toy graphs, feasibility checks, sovereignty, slice, trust constraints
- [x] `test_patterns.py`: All 5 pipeline factories, structure verification, DP-eligibility
- [x] `test_measurement.py`: Trace latency, stage latencies, aggregate metrics, federation monitor, adaptation tracker

#### Level 2: Single-broker integration (Docker, 1 domain) — ✅ COMPLETE
- [x] Start `broker-d1` standalone: health endpoint returns `{"status": "ok", "workers": 0}`
- [x] Register worker: `POST /register` returns `{"status": "registered"}`
- [x] Publish pipeline: validates pipeline type, attempts DP placement
- [x] Deregister worker: `DELETE /register/{node_id}` returns `{"status": "deregistered"}`
- [x] Metrics endpoint: returns zeroed metrics when no pipelines completed
- [x] Verify: placement respects slice constraints (CQI→URLLC, anomaly→eMBB, sensor_fusion→any)
- [x] Inject worker failure, verify re-placement (health check loop + _replace_failed_stages implemented; integration test skeleton in test_integration.py)

#### Level 3: Federation integration (Docker Compose, 2 domains) — ✅ COMPLETE
- [x] Start full `docker-compose.local.yaml`: 7 containers (2 brokers, 5 workers), both brokers healthy
- [x] Workers auto-register (3 in d1: 2 URLLC + 1 eMBB, 2 in d2: 2 eMBB)
- [x] Pipelines complete end-to-end: 40/40 on D1 (0 failed), mean latency 1357ms; 2/2 on D2
- [x] Per-stage latency breakdown: collect ~103ms, feature_extract ~404ms, predict ~703ms, sensor ~245-332ms, fuse ~855ms, decide ~604ms
- [x] Subscription summaries propagate between brokers (federation bandwidth tracked: 25-50KB)
- [x] Cross-domain routing works: CQI published to D2 (no URLLC) → forwarded to D1 → placed on URLLC worker, status="forwarded"
- [x] Governance: `__local__` sovereignty domain resolved to broker's domain_id; CQI collect stage stays local
- **Fixes applied:** (1) capacity-based summary generation + propagator integration, (2) federation-aware publish with loop prevention, (3) `__local__` sovereignty domain resolution, (4) compose workers aligned to pipeline slice requirements (URLLC/eMBB)
- [x] Inject broker failure in d2, verify proxy recovery in d1 (peer health tracking in SummaryPropagator; integration test skeleton)
- [x] Inject network partition, verify graceful degradation (integration test skeleton; graceful local-only fallback via cached summaries)

#### Level 4: Full experiment dry run (Docker Compose, all phases) — ✅ SMOKE PASSED
- [x] Stack smoke: 3 workers registered, 26/27 pipelines completed, mean latency 1388ms
- [x] Phase A dry run: run_phase_a.py executes with --dry-run (exit 0)
- [x] Figure generation: generate_figures.py runs without error (exit 0)
- [x] Smoke test runner created: `scripts/run_smoke_test.py`
- [x] Smoke test config created: `configs/smoke_test.yaml`
- [ ] Run Phase A configs (A1-A4) with full Docker Compose (non-dry-run, shortened duration)
- [ ] Run Phase B configs (B1-B4) with `--max-pipelines 50`
- [ ] Run Phase C configs (C1-C4) with `--max-pipelines 50`
- [ ] Run Phase D failure tests with failure injection
- [ ] Verify all CSV outputs are generated and metrics look reasonable
- [ ] Verify figure generation script produces all expected plots from CSV data

**Gate: Do not proceed to testbed deployment until all Level 4 smoke tests pass.**

---

## Pre-Departure Checklist

Items that must be resolved before travelling. Send Nakao Lab and 5GTNF emails as early as possible.

### Software readiness

- [x] **Kafka baseline (2026-03-19):** KafkaBroker implemented (`src/broker/kafka_broker.py`), subclasses BaseBroker, 13 tests passing.
- [x] **Static/random baseline (2026-03-19):** StaticBroker with PlacementStrategy enum (ROUND_ROBIN, RANDOM).
- [x] **Testbed deployment script (2026-03-19):** `scripts/deploy.py` — push images, start/stop services, parallel execution.
- [x] **Testbed compose template (2026-03-19):** `docker-compose.testbed.yaml` parameterised by `testbed-config.yaml`.
- [x] **Placement micro-benchmark (2026-03-19):** Phase A.5 (5 topology scenarios) + A.6 (contention). `scripts/run_phase_a5_a6.py`.
- [x] **Result backup (2026-03-19):** `scripts/backup_results.sh` — rsync wrapper.
- [x] **Traffic shaping (2026-03-19):** `iproute2` installed in Docker image, `cap_add: [NET_ADMIN]` in testbed compose.
- [ ] **Phase E (EISim):** Pre-build EISim federation extension, OR explicitly mark Phase E as stretch goal / future work
- [ ] **Runtime estimate:** Run one full Phase A config locally to measure actual wall time. Extrapolate.
- [ ] **T1 local full run:** Execute Phases A-D on local Docker Compose with full statistical methodology (5 seeds, 30-min windows). This is the minimum publishable dataset.

### Connectivity

- [x] **UOulu server:** 5GTN VM access confirmed (2026-03-20). Username `lloven`. SSH via Pomerium (`pomerium-cli`). Docker host networking. Internet-routable IPv4. Deployment scripts in `scripts/5gtn/`. Pending: hostname confirmation, mTLS CA plan for firewall opening.
- [ ] **5GTN VM setup:** Once provisioned: install Docker, pull neural-pubsub image, run smoke test, measure RTT to Nakao Lab. Also run Phase A baselines on VM for "real infrastructure" comparison.

### Resolved

- [x] **Semantic matcher (2026-03-19):** Simulated matching sufficient. Distribution architecture is the focus; matching quality evaluated in companion Neural Router paper. No LLM/API dependency.
- [x] **Slice emulation (2026-03-19):** No UEs in experiment; slices emulated via `tc qdisc` at all tiers. Real 5G slicing adds no value for containerised workloads without air interface. Documented in Threats to Validity.

---

## Testbed Deployment (Week 2-3)

### 1.0 Pre-visit questions for Nakao Lab (send by email before arrival)

These questions should be sent to the Nakao Lab contact person before the visit so they can prepare access, allocations, and documentation. Items marked (on-site) can only be resolved in person.

#### Compute and deployment
- [ ] **Compute nodes:** How many nodes are available for guest experiments? What specs (CPU cores, RAM, storage, GPU if any)?
- [ ] **Container runtime:** Is Docker, Kubernetes, or Podman available? Can we deploy arbitrary containers? Any security restrictions (e.g., rootless only, no host networking)?
- [ ] **Container registry:** Is there a local registry we can push to, or should we `docker save`/`docker load` via SCP?
- [ ] **OS and architecture:** What OS do the nodes run (Ubuntu, CentOS, etc.)? x86_64 or ARM?
- [ ] **Resource quotas:** Is there a booking system? Can we reserve nodes for 2-3 weeks of dedicated use?
- [ ] **Internet access from nodes:** Can containers on testbed nodes reach the public internet (needed for OpenAI API calls)? If not, is there a proxy?

#### Network and slicing
- [ ] **Network slicing:** Does Local6G support configurable network slices? What slice types are pre-configured (eMBB, URLLC, mMTC)? Can we create custom slices with specific QoS profiles?
- [ ] **O-RAN stack:** Is a near-RT RIC or non-RT RIC deployed? What xApps are available? Can we deploy custom xApps?
- [ ] **Network topology:** How are the nodes connected? Is there a switch topology diagram? Are there multiple network segments we can use as slice proxies?
- [ ] **Traffic shaping:** If native slicing is not available, can we use `tc qdisc` on nodes to emulate slice QoS?

#### Access and logistics
- [ ] **Access model:** SSH keys, VPN, or web console? Can we get credentials before arrival to test connectivity?
- [ ] **Remote access after visit:** Can we SSH into the testbed from outside Japan after the visit (for follow-up experiments or debugging)?
- [ ] **Technical contact:** Name and email of the day-to-day testbed manager we should coordinate with
- [ ] **Lab hours and access:** Any restrictions on physical lab access (badge, hours, weekends)?
- [ ] **WiFi/network for laptop:** Is there guest WiFi or should we connect via ethernet?

#### Data and institutional
- [ ] **Data export:** Can experiment logs and CSV results leave the lab freely? Any restrictions on data that transits the testbed?
- [ ] **Acknowledgments:** How should the Local6G testbed be acknowledged in the paper?
- [ ] **Institutional framing:** Which project umbrella for this work? Options: 6GBridge Local6G (Business Finland), MIRAI-HARMONY (NICT), Nakao's UOulu guest professorship

### 1.1 Nakao Lab testbed inventory (day 1, on-site)

Verify and extend the pre-visit information. Measure what can only be measured on-site:

- [ ] **Inter-node latency matrix:** Measure RTT between all available nodes (use `scripts/measure_latency.py`)
- [ ] **Actual container performance:** Deploy one broker + one worker, measure stage processing time on real hardware
- [ ] **Network bandwidth:** Measure sustained throughput between nodes (`iperf3`)
- [ ] **Slice QoS verification:** If slices are available, measure actual latency/bandwidth per slice
- [ ] **Confirm deployment path:** Successfully deploy and run the Docker smoke test on testbed nodes
- [ ] **Calibrate params:** Update `configs/` with actual measured values (latencies, capacities)

### 1.2 Cross-site link assessment (day 2)

- [ ] **Tokyo-Oulu RTT:** Measure via SINET/GEANT/FUNET path
- [ ] **Bandwidth:** Sustained throughput on research network
- [ ] **FABRIC option:** Alternative inter-site transport
- [ ] **Institutional framing:** 6GBridge? MIRAI-HARMONY? Nakao's guest professorship?

### 1.3 6GTN/5GTNF coordination (see dedicated section below)

### 1.4 Testbed config file

Write `testbed-config.yaml` with actual node IDs, slice configs, measured latencies. This file parameterises the same Docker Compose structure used locally, but with real endpoints.

### 1.5 Testbed deployment and validation (days 8-10)

- [ ] Push Docker images to testbed (local registry or `docker save`/`docker load`)
- [ ] Deploy broker + workers on Nakao Lab nodes
- [ ] Run Level 2 and Level 3 smoke tests on real hardware
- [ ] Calibrate processing times from actual workloads (update `params.yaml`)
- [ ] Measure real inter-node latency matrix, update testbed config

---

## Experiment Execution (Week 3)

### Phase A: Single-site baseline equivalence (days 10-11)

All on T1 (local Docker) or T2 (Nakao Lab), single domain, no federation. **Purpose: demonstrate S4 introduces no overhead in the homogeneous case.**

The theory predicts no differentiation in a flat, homogeneous deployment (all workers same slice, no governance, uniform network). The placement cost function's four terms (latency, load, slice, governance) are all equal, so S4 degenerates to S2. The timing test (2026-03-19) confirmed this: p50 differs by <2ms across S2/S3/S4 at medium rate.

| Config | Description | What it tests |
|--------|-------------|---------------|
| S1 | Kafka + static topic routing | Industry-standard baseline |
| S2 | Static placement (round-robin) | No dynamic routing baseline |
| S3 | Random placement | Lower bound reference |
| S4 | Neural Pub/Sub (single broker) | Full system, dynamic placement |

**Per config:** 5 seeds x 1 rate (medium) x 1 complexity (3-stage). Each run: 10 min warm-up, 30 min measurement.

**Runtime estimate:** 4 configs x 5 seeds x 40 min = ~13 hours.

**Expected result:** S4 matches S2/S3 within statistical noise. This is the correct result; it confirms the placement algorithm adds negligible overhead when there is nothing to optimize.

### Phase A.5: Placement micro-benchmark (day 11)

Isolate placement decision cost. **Purpose: quantify the computational overhead of the cost-function-based placement algorithm.**

- 1000 placement decisions x 3 DAG sizes (3, 5, 10 stages) x 3 worker counts (2, 5, 10)
- Measure: placement decision time (ms), memory allocation
- No Docker needed; runs as a unit test

**Runtime estimate:** <1 hour.

**Expected result:** Placement decision time is <5ms for 10-stage DAG with 10 workers. This is <0.5% of the median e2e latency (~1300ms), confirming the algorithm is not a bottleneck.

### Phase A.6: Contention experiment (days 11-12)

Increase arrival rate until workers saturate. **Purpose: show S4's load-aware placement maintains lower tail latency under contention.**

At high load, workers saturate asymmetrically (some get more expensive pipelines). S4's load term in the cost function steers new placements to less loaded workers. S2 cycles blindly. S3 creates random hot spots.

| Config | Rates (req/s) | What it tests |
|--------|--------------|---------------|
| S2 vs S4 | 2, 5, 8, 10, 15 | Load-aware placement advantage |
| S3 vs S4 | 2, 5, 8, 10, 15 | Random hot-spot avoidance |

**Per config:** 3 seeds per rate. Each run: 10 min warm-up, 30 min measurement.

**Runtime estimate:** 2 strategies x 5 rates x 3 seeds x 40 min = ~20 hours. (S1 Kafka excluded; it tests a different dispatch mechanism, not placement.)

**Expected result:** At rates >=8 req/s, S4 shows lower p95/p99 divergence than S2/S3. The gap widens with rate, demonstrating load-aware placement value.

Expected outputs (Phases A + A.5 + A.6):
- [ ] `results/phase_a/` with CSV per run
- [ ] Baseline equivalence table (p50/p95 with CIs, KS p-values showing no significant difference)
- [ ] Placement micro-benchmark table (decision time vs DAG size)
- [ ] Contention plot: p95 latency vs arrival rate for S2/S3/S4 (the key Phase A figure)
- [ ] Throughput vs arrival rate (saturation point identification)

### Story for reviewers

Each experiment phase adds one sentence to the contribution:

1. **Phase A (equivalence):** "In the homogeneous case, S4 matches S2/S3 performance, introducing no measurable overhead."
2. **Phase A.5 (micro-benchmark):** "The placement decision completes in <5ms for 10-stage DAGs, representing <0.5% of end-to-end latency."
3. **Phase A.6 (contention):** "Under high load, S4's load-aware placement maintains X% lower p95 latency than static placement."
4. **Phase B (slice-awareness):** "With heterogeneous slices, S4 reduces CQI prediction latency by Y% by placing latency-sensitive stages on the URLLC slice."
5. **Phase C (federation):** "Cross-domain routing enables pipelines that S1 (Kafka) cannot serve at all, with Z ms federation overhead."
6. **Phase D (failure recovery):** "After worker failure, S4 recovers in W seconds while S2/S3 lose V% of in-flight pipelines."

The full factorial Phase A (120h) was replaced with focused experiments because the theory predicts no differentiation in the flat case, and the timing test confirmed this empirically. Compute budget is reallocated to Phase A.6 (contention) and Phase B (slice-awareness), where the placement algorithm's value is theoretically predicted and experimentally testable.

### Phase B: Slice-aware placement (days 12-13)

Single site, multiple network slices.

| Config | Description | What it tests |
|--------|-------------|---------------|
| B1 | Neural Pub/Sub, 1 slice | Baseline (no slice awareness) |
| B2 | Neural Pub/Sub, 3 slices, no governance | Placement with slice constraints |
| B3 | Neural Pub/Sub, 3 slices + governance | Placement with sovereignty constraints |
| B4 | Neural Pub/Sub, 3 slices + governance + failure injection | Resilience (Section 4.4) |

**Per config:**
- 5 runs x medium workload x 3-stage pipeline
- B4: inject node failure at t=15min, measure adaptation time
- Record: latency breakdown per stage, domain crossings, governance violations (should be 0)

Expected outputs:
- [ ] `results/phase_b/` with CSV per run
- [ ] Latency breakdown stacked bar chart (routing + transfer + compute)
- [ ] Adaptation time histogram (B4)

### Phase C: Cross-site federation (days 13-17)

Two domains: laptop (Tokyo) + remote server (Oulu).

**Deployment options (in preference order):**
1. Laptop (Tokyo) + 5GTNF edge node (Oulu) — T4, best reviewer optics
2. Laptop (Tokyo) + any UOulu server with Docker (Oulu) — T3, same scientific validity
3. Local Docker Compose with calibrated WAN delay — T1 fallback, still valid

**Prerequisites (for T3/T4):**
- [ ] SSH access to remote server confirmed and tested
- [ ] Neural broker container deployed at remote site
- [ ] WAN latency and bandwidth measured (`ping`, `iperf3`) and documented

| Config | Description | What it tests |
|--------|-------------|---------------|
| C1 | Kafka at each site, static routing | Cross-site baseline |
| C2 | Neural Pub/Sub, federated (2 brokers) | Federation protocol |
| C3 | C2 + governance (raw radio data stays in JP) | Governance-constrained federation |
| C4 | C3 + broker failure at one site | Cross-site resilience |

**Per config:**
- 5 runs x medium workload
- Pipeline: CQI prediction where collect+preprocess must stay in Tokyo (governance), predict can be in either domain
- Record: all Phase A metrics + inter-broker bandwidth + summary propagation latency

**Fallback chain:** T4 (5GTNF) > T3 (any UOulu server) > T1 (local Docker with `tc netem` WAN emulation, RTT calibrated from real measurement). All produce valid results; document which tier was used.

Expected outputs:
- [ ] `results/phase_c/` with CSV per run
- [ ] Cross-site latency vs single-site comparison
- [ ] Bandwidth overhead breakdown (summary vs forwarded publications)
- [ ] Governance compliance verification log

### Phase D: Failure and adaptation (days 15-17)

Overlaps with Phase C. Systematic failure injection.

| Test | Failure type | Expected behaviour |
|------|-------------|-------------------|
| D1 | eMBB worker kill (worker-d1-embb-1) | Tests recovery when an anomaly-detection worker fails. Stage re-placement on surviving workers (Section 4.4.2). |
| D2 | URLLC worker kill (worker-d1-urllc-1) | Tests recovery when a CQI/sensor worker fails. Stage re-placement on surviving URLLC workers (Section 4.4.2). |

D1 and D2 target different slice-specific workers to test whether failure impact depends on the worker's role. Federation-level failures (broker kill, network partition) are tested in Phase C configs C4-C5, which provide the cross-domain traffic necessary for meaningful treatment effects.

**Per test:**
- 10 runs (2 configs x 10 seeds = 20 runs), inject failure at t=15min, measure recovery time and pipeline completion rate
- Record: time to detect, time to re-route, pipeline success rate during/after failure

Expected outputs:
- [ ] `results/phase_d/` with CSV per run
- [ ] Recovery timeline plots (per failure type)
- [ ] Pipeline success rate before/during/after failure

---

## Analysis and Writing (Week 4)

### Phase E: EISim scaling study (days 17-19)

Pure simulation, runs on any machine.

- [ ] Extend EISim with Neural Pub/Sub broker federation model
- [ ] Topologies: star, mesh, tree; 10/50/100/500 nodes
- [ ] Measure: federation overhead, routing latency vs domains, throughput scaling
- [ ] Compare: flat vs hierarchical federation (Section 4.5.1)

Expected outputs:
- [ ] `results/phase_e/` with CSV per topology x scale
- [ ] Scaling plots: latency and throughput vs number of nodes/domains
- [ ] Federation overhead vs number of domains

### Paper drafting (days 17-20)

- [ ] Generate all figures (copy to `Manuscripts/Neural Pub-Sub (Elsevier DCN)/fig/`)
- [ ] Draft Section 5.2 (Testbed and Configuration) with actual testbed details
- [ ] Draft Section 5.4 (Results) with data from Phases A-D
- [ ] Draft Section 5.5 (Scaling Study) with Phase E data
- [ ] Update abstract with summary metrics
- [ ] Update Section 5.1 (Scenario) with O-RAN details observed at the lab

---

## 6GTN/5GTNF Coordination

To enable Phase C (cross-site federation), the following must be arranged with the 6GTN team at the University of Oulu. **Contact before end of Week 1.**

### Questions for 6GTN team

**Findings from 5GTN handbook and 6GTNF website (2026-03-19):**

#### Access and logistics
- [x] **Who is the contact person:** **Olli Liinamaa** (6GTNF ecosystem), olli.liinamaa@oulu.fi, +358 40 5461418. Technical support: **5gtn-admin@oulu.fi**. (Sources: 6gtnf.fi, 5GTN handbook)
- [x] **SSH/remote access:** SSH via **Pomerium proxy** (5gtnp.oulu.fi). No VPN needed. Install `pomerium-cli`, configure SSH ProxyCommand, authenticate via browser. Supports tunneled SSH, RDP, database connections. (Source: 5GTN handbook)
- [x] **User accounts:** Create at 5gtn-identity.oulu.fi. Min 14-char password, TOTP 2FA mandatory, passkeys supported. (Source: 5GTN handbook)
- [x] **Container deployment:** VMs provided as IaaS. User responsible for OS, security, Docker installation. Request requires: project name, vCPU/RAM/storage specs, OS preference, SSH public key, timeline. No managed Kubernetes; deploy Docker directly on provisioned VM. (Source: 5GTN handbook)
- [x] **Resource allocation:** 1 VM, access confirmed (2026-03-20). Internet-routable IPv4 + IPv6 GUA. In "services" block. Full container freedom (Docker host networking recommended). No packet restrictions. Username `lloven`, Pomerium SSH. Deployment scripts ready (`scripts/5gtn/`). Pending: hostname confirmation, mTLS CA plan for gRPC firewall opening, Nakao Lab endpoint IP.
- [x] **Booking procedure:** Email 5gtn-admin@oulu.fi with project name, resource specs, SSH key, and timeline. (Source: 5GTN handbook)

#### Technical details (partially answered)
- [x] **Edge compute:** DELL R730 server stack near radio access points, 100TB storage, GPU processing available. (Source: 6gtnf.fi/oulu-university/)
- [x] **5G deployment:** Standalone 5G on n78 band (50/60 MHz), 8 cells at Linnanmaa campus (gNodeB 1340), indoor small cells at Tellus/Agora/Fablab/OAMK Robotics, mmWave in Tietotalo. (Source: 5GTN handbook ch02-01-sa)
- [x] **Project-specific isolated networks:** Two standalone options: USRP SDR with OAIBOX (supports SA and O-RAN), or Open5GS with USRP/commercial radios. (Source: 6gtnf.fi/oulu-university/)
- [x] **Network slicing:** **No S-NSSAI slicing available.** Best-effort LAN only. DSCP/DiffServ not restricted but not enforced. No TSN (802.1Qb{r,u,v}). Confirms tc qdisc emulation approach. (Source: Aleksi Pirttimaa response 2026-03-20)
- [x] **Available slices:** None. Single service class "best-effort". (Source: Aleksi 2026-03-20)
- [x] **O-RAN components:** **Not available.** Some users had FlexRIC but not integrated into testbed. No xApp deployment possible. (Source: Aleksi 2026-03-20)
- [ ] **Inter-node latency:** Must measure ourselves. Aleksi confirmed no latency matrix available.
- [x] **Monitoring tools:** Keysight NEMO for radio, Kaitotek Qosium for QoE measurement. (Source: 6gtnf.fi/oulu-university/)

#### Companion compute (non-5GTN)
- [x] **Lehmus HPC:** Available at lehmus.oulu.fi (SSH: lehmus-login1.oulu.fi). Linux-based, SLURM scheduler, NVIDIA GPUs, Lustre storage. Access for all UOulu staff/students. *(Source: Lehmus documentation.)*
- [x] **CSC cloud:** Available via docs.csc.fi/cloud/ for additional compute if needed.
- **Note:** For T3 (cross-site), the best option is a **5GTN VM** on the DELL R730 stack (request via 5gtn-admin@oulu.fi). This gives us SSH via Pomerium from Tokyo, Docker on our own VM, and proximity to the 5G infrastructure for T4 optics. Lehmus (SLURM HPC) and CSE department servers are fallbacks but lack the 5GTN association.

#### Cross-site connectivity
- [x] **European federation:** 5GTN participates in SLICES-SC, linking into European testbed cluster with federation portal for remote use. (Source: 6gtnf.fi/oulu-university/)
- [x] **FUNET/NORDUnet path:** Confirmed: 5GTN → Funet (via UOulu transit, redundant CE + MC-LAG L2) → NORDUnet → GEANT → SINET → UTokyo. 5GTN addresses: 2001:708:521::/48, 193.166.30.0/23, 193.166.32.0/24. Test: `ping 5gtn-web.oulu.fi`. Aleksi noted GEANT L2VPN connection is planned (slow progress) and could provide a comparison vs public internet. (Source: Aleksi 2026-03-20)
- [x] **Firewall/NAT:** Connection-tracking firewall with segmented zones. "Services" zone (where our VMs would be) has **filtered** outbound to Internet and **filtered** inbound. Firewall openings require approved authentication (mTLS, OIDC, Kerberos, or OAuth2 with rotated tokens). For our gRPC broker-to-broker link, we need to request a firewall opening for the specific TCP port with mTLS or OAuth2 auth. (Source: 5GTN handbook firewall chapter)
- [x] **Bandwidth measurement:** Use `iperf3 -c iperf.funet.fi` (FUNET iPerf server) from the 5GTN VM. Also speedtest.oulu.fi (HTTP) available. (Source: 5GTN handbook, speed test chapter)
- [x] **VPN tunnel:** Not needed. VM gets internet-routable IPv4 directly. Firewall opening for TCP 50051 (gRPC) with mTLS approved. No NAT. (Source: Aleksi 2026-03-20)

#### Institutional framing
- [ ] **Which project covers this?** Options: 6G Flagship SRA3 (ending Jun 2026), HPRNET (starting Jun 2026), 6GBridge Local6G (Business Finland), MIRAI-HARMONY (NICT). Nakao's guest professorship at UOulu provides the collaboration basis.
- [ ] **Ethics/data handling:** Any constraints on what data can transit the cross-site link? Do we need a data processing agreement? *(Likely no real data sovereignty issues since we generate synthetic workloads.)*
- [ ] **Acknowledgments:** How should 5GTNF be acknowledged in the paper?

### Timeline for 6GTN coordination

| When | Action |
|------|--------|
| Day 1-2 | Send email to 6GTN contact (cc: Lovén's UOulu email) with experiment summary and access request |
| Day 3-5 | Technical call to discuss connectivity and slice configuration |
| Day 7-8 | Receive VPN/SSH credentials, test basic connectivity (ping, iperf) |
| Day 9-10 | Deploy broker container at 5GTNF, run Level 2 smoke test remotely |
| Day 13+ | Begin Phase C cross-site experiments |

---

## Repo Structure

```
Experiments/neural-pubsub/
  EXPERIMENT-PLAN.md          # This file
  README.md                   # Setup instructions
  Dockerfile                  # Single image for broker, worker, workload
  docker-compose.local.yaml   # Local emulation (2 domains, 4 slices, WAN delay)
  docker-compose.testbed.yaml # Real testbed deployment (parameterised)
  testbed-config.yaml         # Testbed mapping (filled in after inventory)
  requirements.txt
  params.yaml                 # Experiment parameters
  configs/
    domain_d1.yaml            # Domain 1 config (subscriptions, governance, slices)
    domain_d2.yaml            # Domain 2 config
    workload.yaml             # Workload generator config
    phase_a.yaml              # Phase-specific overrides
    phase_b.yaml
    phase_c.yaml
    phase_d.yaml
    phase_e.yaml
  src/
    __init__.py
    router_core/              # Ported from neural-router/src/
      __init__.py
      router.py
      embeddings.py
      llm.py
      evaluation.py
    federation/
      __init__.py
      summary.py              # SubscriptionSummary dataclass (Eq. 7)
      propagation.py          # Periodic summary exchange
      routing.py              # Cross-domain routing protocol (Section 4.2.3)
      integrator.py           # Composite max-flow capacity (Eq. 8)
    broker/
      __init__.py
      neural_broker.py        # Full broker = router_core + federation + HTTP API
      placement.py            # Slice-aware placement (Eq. 10)
    pipeline/
      __init__.py
      dag.py                  # Service-dependency DAG representation
      patterns.py             # Map and funnel pattern implementations
      workload.py             # Pipeline template generator + Poisson arrivals
    worker/
      __init__.py
      __main__.py             # Entry point for `python -m src.worker`
      worker.py               # Execution unit: receives stages, runs compute, reports
    workload/
      __init__.py
      __main__.py             # Entry point for `python -m src.workload`
      generator.py            # Poisson-arrival workload generator
    measurement/
      __init__.py
      harness.py              # Timestamp injection, metric collection, CSV export
      failure.py              # Failure injection (kill containers, partition networks)
  tests/
    test_dag.py               # DAG construction, topo sort, is_tree
    test_patterns.py          # Pipeline factory functions
    test_placement.py         # Placement algorithm + constraint checks
    test_federation.py        # Subscription summary, routing, governance
    test_measurement.py       # Trace, aggregate, federation monitor, adaptation
    test_failure.py           # Failure injection (mocked Docker)
    test_integration.py       # Level 2-3: integration tests (requires Docker)
    test_smoke.py             # Level 4: full phase dry runs
  scripts/
    run_phase_a.py
    run_phase_b.py
    run_phase_c.py
    run_phase_d.py
    run_phase_e.py
    deploy.py                 # Container deployment to testbed nodes
    measure_latency.py        # Inter-node latency matrix measurement
    generate_figures.py       # All paper figures from results CSVs
  results/
    local/                    # Local smoke test results
    phase_a/
    phase_b/
    phase_c/
    phase_d/
    phase_e/
  figs/                       # Generated figures (copied to manuscript)
  logs/
```

---

## Dependencies on Other Work

| Dependency | Status | Impact |
|------------|--------|--------|
| Neural Router single-broker code (`Experiments/neural-router/src/`) | Exists, D1+D2 ablation complete, D3 running | Companion paper; not ported into pubsub (simulated matching used instead) |
| ~~OpenAI API access~~ | ~~Not needed~~ | ~~Simulated matching; matching quality evaluated in Neural Router paper~~ |
| 5GTNF remote access (Oulu) | Emails sent (2026-03-19) | T4 (opportunistic); T3 fallback: any UOulu server; T1 fallback: local emulation |
| EISim codebase | Exists separately | Needed for Phase E only |
| Docker on testbed | Unknown | Required for deployment; fallback: direct Python if no Docker |
| Worker health monitoring | **Implemented** (2026-03-18) | Health check loop, re-placement, dispatch retry |
| Peer broker health | **Implemented** (2026-03-18) | Failure counting, cached summary fallback |
| Metrics CSV export | **Implemented** (2026-03-18) | Broker /metrics/export endpoint, workload auto-export |
| Phase B/C/D run scripts | **Created** (2026-03-18) | All 4 phase scripts dry-run validated |
| Kafka baseline broker | **Complete** (2026-03-19) | `src/broker/kafka_broker.py`, subclasses BaseBroker, 13 tests |
| Static/random baseline | **Complete** (2026-03-19) | `src/broker/static_broker.py`, PlacementStrategy enum |
| BaseBroker extraction | **Complete** (2026-03-19) | `src/broker/base.py` + `src/broker/models.py`, eliminated 850 lines duplication |
| Testbed deployment script | **Complete** (2026-03-19) | `scripts/deploy.py`, parallel push via ThreadPoolExecutor |
| docker-compose.testbed.yaml | **Complete** (2026-03-19) | Parameterised, `cap_add: [NET_ADMIN]` for `tc qdisc` |
| Placement micro-benchmark | **Complete** (2026-03-19) | Phase A.5 (5 scenarios) + A.6 (contention), `test_placement_quality.py` |
| Traffic shaping tools | **Complete** (2026-03-19) | `iproute2` in Docker image, NET_ADMIN capability |
| Code documentation | **Complete** (2026-03-19) | README, inline docs, config comments, test docstrings |
| Code simplification | **Complete** (2026-03-19) | /simplify pass: O(1) lookups, shared phase scripts, HTTP client reuse |
| Docker smoke test | **Passed** (2026-03-18) | 63 pipelines, 2 domains, 5 workers, clean CSV output |

---

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| No GPU nodes at Nakao Lab | Not needed; experiment uses simulated matching + CPU-only MiniLM embeddings |
| ~~OpenAI API unreliable from Japan~~ | ~~Resolved: no API needed; simulated matching with known pipeline types~~ |
| 5GTNF access not arranged in time | T3: use any UOulu server with Docker. T1 fallback: local Docker with calibrated WAN delay |
| O-RAN stack not available | Use NWDAF as logical framing; pipeline stages are generic containers |
| Network slicing not configurable | Emulate via `tc qdisc` (required at all tiers; no UEs means no real slice QoS) |
| No testbed access at all | T1 (local Docker) produces full publishable dataset; testbeds add credibility only |
| Insufficient testbed nodes | Minimum viable: 4 nodes (2 per domain). EISim compensates (Phase E) |
| No Docker on testbed | Run processes directly via SSH + virtualenv; Docker Compose for local only |
| Container registry not available | `docker save` / `docker load` via SCP |
| Data loss (node crash, Docker prune) | Automated SCP of results/ to backup host after each run; git push results to private repo |
| Silent worker hang during long runs | Workload generator logs pipeline completion count every 60s; health check detects unresponsive workers |
| Phase A runtime exceeds allocation | Reduce to 3 seeds x 2 rates = 24 configs per phase (12h parallel on 4 nodes) |
| Phase E (EISim) not ready in time | Mark as future work; core paper claims (1-6) are covered by Phases A-D |

---

## Success Criteria

The experiment must produce data for these paper claims:

1. **Routing accuracy is preserved under federation** (Phase A: A4 F1 comparable to Neural Router single-node results)
2. **Semantic routing outperforms static/random placement** (Phase A: A4 latency < A2 < A3; A4 throughput > A2 > A3)
3. **Slice-aware placement reduces latency** (Phase B: B2 < B1 in cross-slice scenarios)
4. **Governance constraints are enforceable without performance collapse** (Phase B: B3 latency overhead < 20% vs B2)
5. **Federation adds bounded overhead** (Phase C: C2 latency = C1 + O(summary_propagation))
6. **System recovers from failures within bounded time** (Phase D: recovery < 2x propagation interval)
7. **Architecture scales to 100+ nodes** (Phase E: sub-linear overhead growth with number of domains)

---

## What Can Be Built Right Now

Everything in Phase 0 is testbed-independent. Starting immediately:

| Component | Depends on | Can start now? |
|-----------|-----------|---------------|
| Pipeline DAG (`dag.py`, `patterns.py`) | Nothing | **Yes** |
| Subscription summary (`summary.py`) | Neural Router embeddings | **Yes** (port embeddings first) |
| Placement algorithm (`placement.py`) | DAG + summary | **Yes** |
| Cross-domain routing (`routing.py`) | Summary + governance model | **Yes** |
| Integrator encapsulation (`integrator.py`) | DAG + capacity model | **Yes** |
| Neural broker (`neural_broker.py`) | All above + ported router core | **Yes** (after porting) |
| Worker process (`worker.py`) | Nothing | **Yes** |
| Workload generator (`workload.py`) | DAG + patterns | **Yes** |
| Measurement harness (`harness.py`) | Nothing | **Yes** |
| Failure injection (`failure.py`) | Docker API | **Yes** |
| Docker Compose local env | All above | **Yes** |
| Unit tests (Level 1) | Components above | **Yes** |
| Integration tests (Level 2-4) | Docker Compose | **Yes** |
| Testbed deployment scripts | Testbed inventory | **No** (need node IDs, IPs) |
| Phase A-D run scripts | Smoke tests passing | **Yes** (parameterised) |
| Phase E (EISim) | EISim codebase | Partially (federation model yes, EISim integration later) |
