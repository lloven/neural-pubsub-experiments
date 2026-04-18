# Operations log

Reverse-chronological record of cluster operations: campaign launches,
kills, deploys, image rebuilds, file migrations, progress resets, smoke
results. Complements:

- `Tasks/lessons.md` (general patterns, prevention rules)
- `EXPERIMENT-PLAN.md` "Resolved" section (code-level bug fixes with trigger/diagnosis/fix/lesson)
- `results/ablation/campaign_notes.log` on VM1 (scientific observations during runs)

Audience: operator reproducing or tracing the campaign timeline.
Format: one line per significant action, terse, no nested bullets.
New entries at top of the appropriate date section. Timestamps in EEST (cluster timezone).

---

## 2026-04-18

- **09:44** Relaunched ablation campaign in tmux `campaign` (commit bb59752). First run `failure-5-12_oracle-global_cqi-chain_seed-42` at `--arrival-rate 5.0` → confirmed new rates honored. ETA 450 runs × 14 min ≈ 105h.
- **~09:43** Archived stale results on VM1 to `results/_archive_1pps_bug_20260418/` (ablation/baseline/market/resilience/stress/slicing ≈ 380 MB of pre-fix data; all invalid due to L53 bug).
- **09:40** Smoke PASS: sat-10 cell (10 pps × 60s) → 835 pipelines, 100% CR, matches 2026-04-18 calibration.
- **~09:30** Redesigned ablation scenarios around calibrated knee (commit bb59752). Replaces failure-{50,100,150}-{12,24} with failure-{5,8,10}-{12,24} and sat-{100,150,200} with sat-{5,10,15}. Updates tests + EXPERIMENT.md + manuscript Evaluation.tex.
- **09:21–09:32** Saturation calibration sweep on VM1. Result: knee at 10–15 pps (CR 100%→80.7%, p99 1992→14750 ms), effective saturation ~13.8 pps. Pre-fix theoretical estimate of ~200 pps was off by a factor of 14. Results saved to `results/calibration/SWEEP-RESULTS.md`.
- **~09:15** `deploy_code()` + `docker build` on VM2–VM4 (commit a7b88f2). Verified `--arrival-rate` in generator CLI help across all 4 images.
- **~09:00** `./run-experiments.sh --remote sync` → `git reset --hard origin/main` + `docker build` on VM1 (commit a7b88f2).
- **~08:30** Committed L53 fix + calibration script (commits a7b88f2, f5ce4ff). 1142 tests pass. Added TDD tests in `tests/test_arrival_rate_plumbing.py`.
- **08:05** `multi_vm_runner --stop` cleaned VM1–VM4 compose stacks.
- **08:00** Stopped ongoing ablation (244/450 completed under bug) via SIGINT to tmux `campaign`.
- **~07:40** **Discovery — L53 bug**: `multi_vm_runner.run_single` set `-e ARRIVAL_RATE=<rate>` but `src/workload/generator.py` only reads `--arrival-rate` CLI (default 1.0). Every distributed run across every phase ran at actual 1.0 pps regardless of configured load. Detected via identical cell pipeline counts (~821/seed/600s = 1.37 pps) across failure-50-12, failure-100-12, failure-150-12, failure-50-24 — load doubling had no effect because load wasn't actually scaling.
- **~07:30** Comparison of failure-150-12 against failure-50-12 showed identical distributions, prompting deeper investigation that uncovered L53.
- **Fix** (same day): extracted `_build_workload_cmd()` helper. ARRIVAL_RATE now passed as `--arrival-rate <rate>` CLI arg. Also fixed `run_slicing.py` missing ARRIVAL_RATE in distributed workload_env. Lesson L53 added.

## 2026-04-16

- **13:52** Launched 450-run ablation campaign on VM1 tmux `campaign`. `--resume` picks up 71 existing (26 failure-50-12 + 45 heterogeneous). ETA ~91h.
- **13:50** Cluster smoke `failure-150-24` PASS (89 ok, 0 failed, median 1825ms). Multi-VM kill verified.
- **13:45** `deploy_code()` + `docker build` x4 VMs (commit 16a2139).
- **13:30** Killed previous campaign (was running obsolete failure scenario at 50 pps + 1-VM kill, no signal).
- **13:25** Migrated 26 `failure_*.csv` → `failure-50-12_*.csv` on VM1 (parameters identical, name only). Updated `.progress.json` keys. Saved ~6h.
- **09:15** Decision — redesign ablation: 3×2 failure factorial + saturation sweep at 100/150/200 pps. Total 450 runs.

## 2026-04-15

- **21:45** Launched heterogeneous market-quad block (15 runs) with oracle_mode + dynamic bidding + bid scaling. Smoke showed 1257ms median (vs oracle 2159ms, static-bid 3497ms). Market mechanism confirmed under heterogeneous conditions.
- **~21:30** Killed ongoing market-quad re-run (was using old code). `deploy_code()` + `docker build` x4 VMs (commit 6196ab4).
- **~20:00** Reverted federation forwarding fix (commit 97d97be) — broke result collection (0 pipelines completed). Switched market-quad to oracle_mode for ablation: isolates pricing mechanism (Walrasian) from federation forwarding (separate test, H-FEDERATION).

## 2026-04-14

- **~22:00** Discovery — market mechanism needed 4-layer fix: (1) bid scaling, (2) M/M/1 congestion, (3) federation price exchange, (4) topology workaround. Federation forwarding chain broken on cluster.
- **~10:00** Federation price exchange added (commit 21d8b3a): `POST /federation/price-signal` endpoint, `_peer_prices` cache, merge in `_dispatch_placement_on`.
- **~09:30** Worker bid scaling by `processing_speed` (commit d219b5d): `bid_cost_ms = base × processing_speed` at registration.
- **~08:30** M/M/1 dynamic congestion pricing (commit 2fb1fe2): `cost = bid / (1 - utilization)`, capped at 0.99.
- **~05:30** VM2 came back online; relaunched stalled rr-global block.

## 2026-04-13

- **~08:00** VM2 went offline mid-campaign ("No route to host" during failure injection). Campaign tmux died with it.

## 2026-04-12

- **~17:00** Launched 75-run market-quad re-run after rr-global block completed.

## 2026-04-11

- **13:48** Launched first ablation campaign (225 runs) after fixing market-load-aware CLI flag → env var (Bug A: rr-global StaticBroker rejected `--market-load-aware`) and CSV writer crash (Bug B: 'error' key in result dict). Both bugs documented in lessons L51, L52.
- **earlier** Initial deploy + image rebuild on all 4 VMs after L50 lesson captured the "deploy_code() does not rebuild Docker images" rule.

## 2026-04-10

- **02:16** Main market campaign completed: 330/330 successful, 0 failed (~80h wall clock from launch on Apr 6).
- **earlier** Discovery — 26 oracle-global runs phantom-marked done; `scripts/fix_phantom_done.py` utility added; `.csv.old` residue archived.

## 2026-04-06

- **19:08** Launched main market campaign on VM1 (270 allocation + 60 governance = 330 runs).
