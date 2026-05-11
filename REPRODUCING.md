# Reproducing the Experimental Results

This document describes how to reproduce the seven-phase experimental campaign reported in:

> L. Lovén, R. Morabito, A. Kumar, S. Pirttikangas, J. Riekki, S. Tarkoma. *Autonomic Federated-Market Orchestration for the Edge–Cloud Continuum.* ACM Transactions on Autonomous and Adaptive Systems, Special Issue on Autonomic Approaches and Applications for the Edge–HPC/Cloud Computing Continuum (under review), 2026.

The campaign comprises 1005 runs across seven phases on a 4-VM, 4-domain, 48-worker federated edge–cloud testbed (5GTNF, single data centre, 50 ms emulated WAN). Each phase produces a CSV result set under `results/<phase>/` and a `.progress.json` checkpoint.

## Phases

| Phase | Cells | What it tests |
|---|---|---|
| baseline | 40 | Single-domain throughput baseline (oracle + market + heuristics + round-robin) |
| ablation | 450 | Three-property structural decomposition under five stressors |
| slicing | 50 | Slice-aware vs.\ slice-blind placement |
| resilience | 50 | Worker-failure recovery across edge sites |
| stress | 60 | Saturation collapse curves |
| market | 330 | Main allocation campaign (3 pipelines × 5 strategies × 3 loads × 3 governance grids) |
| federation | 25 | Broker death + WAN partition |

## Local smoke test (laptop)

The single-VM Docker Compose stack reproduces the broker pipeline on a laptop. Smoke testing the full topology end-to-end takes ~3 minutes.

```bash
# 1. Bring up the local 4-domain stack (3 brokers + workers + workload generator)
docker compose -f docker-compose.market.yaml up -d

# 2. Run a single-pipeline smoke
python -m scripts.run_smoke_test --pipeline cqi-chain --duration 60

# 3. Inspect results
cat results/smoke/metrics.csv
```

The smoke test exercises subscription matching, market clearing, federation routing, and governance enforcement. Pass criterion: every cell yields ≥98 % completion rate at 5 pps.

## Multi-VM testbed deployment

The full 1005-run campaign requires four host VMs (two edge + two cloud) connected by an emulated WAN link. The deployment templates live under `deploy/vm{1,2,3,4}-*.env.example`. Copy each to `.env` (gitignored) and fill in your own host addresses, SSH aliases, and identity files. The orchestrator (`scripts/multi_vm_runner.py`) reads these files and dispatches phases over SSH.

Each phase is launched with:

```bash
python -m scripts.run_<phase> --topology distributed --configs <config-set> --seeds 0 1 2
```

`scripts/post_ablation_chain.sh` runs phases sequentially with progress checkpoints in `results/<phase>/.progress.json`, allowing resumption after interruption.

## Reproducibility envelope

- **Pinned dependencies:** `requirements.txt` (Python 3.11+) and the `Dockerfile` produce a reproducible runtime.
- **Config hashes:** every cell logs its config hash to the result CSV; re-running a cell with the same config + seed reproduces the same output bit-for-bit (modulo network jitter).
- **Wall-clock cost:** the full 1005-run campaign takes ~129 hours on the testbed described in the paper.
- **Seeds:** 5 independent seeds per cell (paper-configured; can be increased via `--seeds`).

## Generating manuscript figures

Once a phase has completed, the §5 figures of the manuscript are produced by:

```bash
python scripts/generate_manuscript_figures.py
```

The script reads CSVs from `results/<phase>/` and writes PDFs into the configured output directory. CSV-to-figure mapping is documented inside the script.

## Hardware envelope (paper testbed)

The paper's 5G Test Network Finland (5GTNF) deployment uses four physical VMs (12 vCPU, 32 GB RAM each) connected by an emulated WAN link (`tc qdisc netem`, 50 ms delay). Smaller deployments (single VM, 4 brokers co-located) reproduce the qualitative findings but with proportionally smaller absolute latencies.

## Contact

For questions about reproducing the campaign, please open an issue on the repository or contact the corresponding author.
