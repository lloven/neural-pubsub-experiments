"""Funnel resilience mode logic for Neural Pub/Sub.

Implements the three funnel resilience modes from Section 4.4.3:
  - wait:    Buffer and wait for all inputs (bounded by timeout). Pipeline stalls.
  - proceed: Execute with partial inputs (graceful degradation).
  - abort:   Signal failure immediately when an input is missing.

STUB IMPLEMENTATION: This is a simplified policy engine. The full integration
with the broker's stage-dispatch loop (checking predecessor completion in
_dispatch_ready_stages) is future work. This module provides:
  1. The FunnelMode enum and env-var reader.
  2. The apply_funnel_policy() function that encodes the decision logic.
  3. The FunnelPolicyResult dataclass that carries the decision.

The broker calls apply_funnel_policy() when a funnel stage's predecessor
set is incomplete (some inputs missing) to decide whether to wait, proceed
with partial data, or abort the pipeline.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum


class FunnelMode(Enum):
    """Funnel resilience mode (Section 4.4.3).

    Controls how a funnel stage handles missing inputs from failed upstream
    workers.
    """

    WAIT = "wait"
    PROCEED = "proceed"
    ABORT = "abort"


@dataclass
class FunnelPolicyResult:
    """Result of applying the funnel resilience policy.

    Attributes:
        action: One of "wait", "proceed", "abort", "fail".
            - "wait": keep waiting for missing inputs.
            - "proceed": advance with partial inputs.
            - "abort": abort the pipeline immediately.
            - "fail": the pipeline has timed out after waiting.
        partial: True if proceeding with incomplete inputs.
        pipeline_complete: True if the funnel decision completes the pipeline.
        pipeline_failed: True if the funnel decision fails the pipeline.
    """

    action: str
    partial: bool
    pipeline_complete: bool
    pipeline_failed: bool


def get_funnel_timeout() -> float:
    """Read the funnel wait timeout in seconds from FUNNEL_TIMEOUT env var.

    Returns 30.0 if not set (a conservative default that gives workers
    time to recover before the pipeline is declared failed).
    """
    return float(os.environ.get("FUNNEL_TIMEOUT", "30.0"))


def get_funnel_grace() -> float:
    """Read the funnel grace period in seconds from FUNNEL_GRACE env var.

    The grace period is the minimum time to wait before checking whether
    a predecessor is truly dead vs. merely slow. Returns 5.0 if not set.
    """
    return float(os.environ.get("FUNNEL_GRACE", "5.0"))


def get_funnel_bypass_replace() -> bool:
    """Read the FUNNEL_BYPASS_REPLACE flag from the environment.

    When True, _replace_failed_stages() skips re-placement for stages that
    are predecessors of fan-in (funnel) stages. This lets the dead worker
    remain in the placement map so that _find_ready_stages can detect it
    and invoke apply_funnel_policy with the actual funnel mode.

    Without this flag, the health-check recovery re-places dead workers
    before the funnel policy is consulted, making all three funnel modes
    produce identical results.

    Returns False if the env var is not set (backward compatible).
    """
    raw = os.environ.get("FUNNEL_BYPASS_REPLACE", "false")
    return raw.lower() in ("true", "1", "yes")


def get_funnel_mode() -> FunnelMode:
    """Read the funnel resilience mode from the FUNNEL_MODE env var.

    Returns FunnelMode.WAIT if the env var is not set (backward compatible
    with the default behavior where the broker waits for all inputs).
    """
    raw = os.environ.get("FUNNEL_MODE", "wait")
    return FunnelMode(raw.lower())


def find_funnel_predecessor_stages(dag) -> set[str]:
    """Return stage IDs that are predecessors of fan-in (funnel) stages.

    A fan-in stage has more than one predecessor. This function returns
    all stages that feed into any fan-in stage. These are the stages
    whose re-placement should be skipped when FUNNEL_BYPASS_REPLACE is
    active, so that the funnel policy can detect dead predecessors.

    Args:
        dag: A PipelineDAG instance with .stages and .predecessors().

    Returns:
        Set of stage IDs that are predecessors of at least one fan-in stage.
    """
    funnel_preds: set[str] = set()
    for stage_id in dag.stages:
        preds = dag.predecessors(stage_id)
        if len(preds) > 1:
            # This is a fan-in stage; all its predecessors are funnel predecessors
            funnel_preds.update(preds)
    return funnel_preds


def apply_funnel_policy(
    mode: FunnelMode,
    expected_inputs: set[str],
    received_inputs: set[str],
    timeout_reached: bool,
) -> FunnelPolicyResult:
    """Decide how to handle a funnel stage with potentially missing inputs.

    Args:
        mode: The configured funnel resilience mode.
        expected_inputs: Set of stage IDs that should feed into the funnel.
        received_inputs: Set of stage IDs that have actually completed.
        timeout_reached: True if the wait timeout has been exceeded.

    Returns:
        FunnelPolicyResult encoding the decision.
    """
    # All inputs received: always proceed normally regardless of mode
    if received_inputs >= expected_inputs:
        return FunnelPolicyResult(
            action="proceed",
            partial=False,
            pipeline_complete=False,
            pipeline_failed=False,
        )

    # Missing inputs: apply mode-specific policy
    if mode == FunnelMode.WAIT:
        if timeout_reached:
            return FunnelPolicyResult(
                action="fail",
                partial=False,
                pipeline_complete=False,
                pipeline_failed=True,
            )
        return FunnelPolicyResult(
            action="wait",
            partial=False,
            pipeline_complete=False,
            pipeline_failed=False,
        )

    elif mode == FunnelMode.PROCEED:
        return FunnelPolicyResult(
            action="proceed",
            partial=True,
            pipeline_complete=False,
            pipeline_failed=False,
        )

    elif mode == FunnelMode.ABORT:
        return FunnelPolicyResult(
            action="abort",
            partial=False,
            pipeline_complete=False,
            pipeline_failed=True,
        )

    # Should not reach here
    raise ValueError(f"Unknown funnel mode: {mode}")
