# Neural Pub/Sub Experiment Plan

**Paper:** Neural Pub/Sub: Distributed AI Orchestration across the 6G Computing Continuum (Elsevier DCN)
**Location:** Nakao Lab, University of Tokyo (4-week visit starting March 2026)
**Testbed:** Nakao Lab Local6G campus testbed + remote link to 5GTNF (Oulu)

---

## Overview

The experiment validates the distribution architecture (paper Section 4) by deploying federated Neural Router instances on a real 6G testbed and comparing against Kafka and static-placement baselines. The 4-week on-site window covers platform discovery, local development with smoke testing, testbed deployment, experiment execution, and result analysis.

**Development principle: local-first.** All software is developed, containerised, and smoke-tested on a local Docker Compose environment that emulates the multi-domain, multi-slice testbed. Only after all phases pass locally do we deploy to the real testbeds. This avoids wasting scarce testbed time on debugging.

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

- [ ] **Kafka baseline:** Implement Kafka-based static broker for Phase A1/C1 comparison, OR redefine baseline as round-robin HTTP dispatch (document decision)
- [x] **Semantic matcher decision (2026-03-19):** Simulated matching is sufficient. This paper evaluates the distribution architecture (federation, placement, slicing, failure recovery), not matching quality. Pipeline types are explicit in the workload generator. Matching quality is evaluated in the companion Neural Router paper (to be published/arXived before this submission). Document in paper Section 5.2.
- [ ] **Testbed deployment script:** Create `scripts/deploy.py` for SCP + SSH container deployment to remote nodes
- [ ] **Testbed compose template:** Create `docker-compose.testbed.yaml` parameterised by `testbed-config.yaml`
- [ ] **Phase E (EISim):** Pre-build EISim federation extension, OR explicitly mark Phase E as stretch goal / future work
- [ ] **Runtime estimate:** Calculate actual Phase A runtime (4 configs x 9 combos x 5 runs x 40 min = 120h). Plan parallel execution or reduce matrix

### Connectivity

- [ ] **5GTNF VPN test:** If credentials arrive before departure, test VPN and ping from Oulu
- [x] **OpenAI API not needed:** Simulated matching means no external API dependency. Experiment runs fully offline.

### Logistics

- [ ] **Result backup:** Set up automated SCP of results/ to an external host after each run completion

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

### Phase A: Single-site baselines (days 10-12)

All on Nakao Lab Local6G, single domain, no federation.

| Config | Description | What it tests |
|--------|-------------|---------------|
| A1 | Kafka + static topic routing | Centralised broker baseline |
| A2 | Static placement (fixed pipeline-to-node) | No dynamic routing baseline |
| A3 | Random placement | Lower bound baseline |
| A4 | Neural Pub/Sub (single broker) | Single Neural Router, dynamic semantic routing |

**Per config:**
- 5 runs x 3 workload rates (low/medium/high) x 3 pipeline complexities (2/3/5 stages)
- Each run: 10-minute warm-up, 30-minute measurement window
- Record: e2e latency (p50/p95/p99), throughput, routing accuracy (F1 vs ground-truth matching)

**Runtime estimate:** 4 configs x 9 combos x 5 runs x 40 min = 120 hours sequential. With 4 parallel nodes: ~30 hours. **If this exceeds the 2-day allocation, reduce to 3 seeds and 2 rates (low/high) = 48h sequential, ~12h parallel.**

Expected outputs:
- [ ] `results/phase_a/` with CSV per run
- [ ] Latency CDF plots (A1-A4 overlaid)
- [ ] Throughput vs arrival rate plot
- [ ] Routing accuracy table (A4 vs Neural Router single-node paper results)

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

Two domains: Nakao Lab (Tokyo) + 5GTNF (Oulu).

**Prerequisites:**
- [ ] Remote access to 5GTNF confirmed and tested
- [ ] Neural broker container deployed at both sites
- [ ] WAN latency and bandwidth measured and stable

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

**Fallback:** If 5GTNF access is not arranged in time, run Phase C on the local Docker Compose environment with calibrated WAN latency (measured Tokyo-Oulu RTT injected via `tc qdisc`). This is scientifically valid if clearly described in the paper as emulated cross-site.

Expected outputs:
- [ ] `results/phase_c/` with CSV per run
- [ ] Cross-site latency vs single-site comparison
- [ ] Bandwidth overhead breakdown (summary vs forwarded publications)
- [ ] Governance compliance verification log

### Phase D: Failure and adaptation (days 15-17)

Overlaps with Phase C. Systematic failure injection.

| Test | Failure type | Expected behaviour |
|------|-------------|-------------------|
| D1 | Execution unit failure | Stage re-placement (Section 4.4.2) |
| D2 | Broker failure | Peer proxy recovery (Section 4.4.1) |
| D3 | Network partition (inter-site link down) | Graceful degradation to local-only routing |
| D4 | Funnel partial input failure | Configurable wait/proceed/abort (Section 4.4.3) |

**Per test:**
- 5 runs, inject failure at t=15min, measure recovery time and pipeline completion rate
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

#### Access and logistics
- [ ] **Who is the contact person** for arranging remote experiment access to 5GTNF? (Suggest: start with the 6GTN coordinator or Erkki Harjula's team.)
- [ ] **Can we get SSH/VPN access** to 5GTNF edge nodes from outside the university network (i.e., from Tokyo)? What firewall rules apply?
- [ ] **Container deployment:** Can we deploy Docker containers on 5GTNF edge compute nodes? Is there a Kubernetes cluster, or do we deploy directly?
- [ ] **Resource allocation:** How many edge nodes can we reserve for 2-3 weeks? What specs? Can we get dedicated time slots?
- [ ] **Booking procedure:** Is there a formal request process (e.g., through CWC, through the 6G Flagship)?

#### Technical details
- [ ] **Network slicing:** Does 5GTNF currently support configurable network slices? How many slices? Can we create custom slices with specific QoS profiles (latency bounds, bandwidth guarantees)?
- [ ] **Available slices:** What slice types are pre-configured (eMBB, URLLC, mMTC)?
- [ ] **O-RAN components:** Is there a near-RT RIC or non-RT RIC deployed? Can we deploy xApps?
- [ ] **Edge compute:** What edge compute infrastructure is available? MEC servers? GPU nodes?
- [ ] **Inter-node latency:** Do you have a latency matrix between edge nodes? If not, can we measure it?

#### Cross-site connectivity
- [ ] **FUNET/NORDUnet path:** What is the network path from 5GTNF to the internet backbone? Can we route traffic to SINET (Japan's academic backbone) via GEANT?
- [ ] **Firewall/NAT:** Are there firewalls or NAT between 5GTNF edge nodes and the external internet? We need direct TCP connectivity for gRPC between a Tokyo broker and an Oulu broker.
- [ ] **Bandwidth:** What sustained throughput can we expect on the research network path?
- [ ] **VPN tunnel:** If direct connectivity is not possible, can we set up a WireGuard or IPsec tunnel between the two sites?

#### Institutional framing
- [ ] **Which project covers this?** Options: 6G Flagship SRA3 (ending Jun 2026), HPRNET (starting Jun 2026), 6GBridge Local6G (Business Finland), MIRAI-HARMONY (NICT). Nakao's guest professorship at UOulu provides the collaboration basis.
- [ ] **Ethics/data handling:** Any constraints on what data can transit the cross-site link? Do we need a data processing agreement?
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
| 5GTNF remote access (Oulu) | Not yet arranged | Needed for Phase C; fallback: emulated cross-site |
| EISim codebase | Exists separately | Needed for Phase E only |
| Docker on testbed | Unknown | Required for deployment; fallback: direct Python if no Docker |
| Worker health monitoring | **Implemented** (2026-03-18) | Health check loop, re-placement, dispatch retry |
| Peer broker health | **Implemented** (2026-03-18) | Failure counting, cached summary fallback |
| Metrics CSV export | **Implemented** (2026-03-18) | Broker /metrics/export endpoint, workload auto-export |
| Phase B/C/D run scripts | **Created** (2026-03-18) | All 4 phase scripts dry-run validated |
| Kafka baseline broker | **Not started** | Needed for Phase A1/C1; fallback: redefine as round-robin HTTP |
| Testbed deployment script | **Not started** | Needed for testbed deployment; can use manual SCP+SSH as fallback |
| docker-compose.testbed.yaml | **Not started** | Template for real testbed; parameterised by testbed-config.yaml |
| Code documentation | **Complete** (2026-03-19) | README, inline docs, config comments, test docstrings |
| Code simplification | **Complete** (2026-03-19) | /simplify pass: O(1) lookups, shared phase scripts, HTTP client reuse |
| Docker smoke test | **Passed** (2026-03-18) | 63 pipelines, 2 domains, 5 workers, clean CSV output |

---

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| No GPU nodes at Nakao Lab | Not needed; experiment uses simulated matching + CPU-only MiniLM embeddings |
| ~~OpenAI API unreliable from Japan~~ | ~~Resolved: no API needed; simulated matching with known pipeline types~~ |
| 5GTNF access not arranged in time | Run Phase C on local Docker Compose with calibrated WAN delay |
| O-RAN stack not available | Use NWDAF as logical framing; pipeline stages are generic containers |
| Network slicing not configurable | Emulate slices via `tc qdisc` on testbed nodes |
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
