"""Tests for workload generator rejected-submission tracking and env var wiring.

These tests verify fixes for the Phase B bug where:
1. The workload generator silently swallowed HTTP 503 errors (L39).
2. PIPELINE_MIX_* env vars were dead code, never consumed by the workload container.
"""

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.workload.generator import WorkloadConfig, WorkloadGenerator


# ---------------------------------------------------------------------------
# Fix 3: Track rejected submissions (503 errors must not be silent)
# ---------------------------------------------------------------------------


class TestRejectedSubmissionTracking:
    """The workload generator must count and expose failed publishes, not silently discard them."""

    def _make_config(self, **overrides) -> WorkloadConfig:
        defaults = dict(
            arrival_rate=10.0,
            duration_s=1.0,
            pipeline_mix={"sensor_fusion": 1.0},
            broker_url="http://fake:8080",
            seed=42,
        )
        defaults.update(overrides)
        return WorkloadConfig(**defaults)

    def test_stats_include_rejected_count(self):
        """get_stats() must include a 'rejected_submissions' key."""
        gen = WorkloadGenerator(self._make_config())
        stats = gen.get_stats()
        assert "rejected_submissions" in stats, (
            "get_stats() must report rejected_submissions count (L39: no silent errors)"
        )

    def test_rejected_counter_starts_at_zero(self):
        """Before any run, rejected_submissions must be 0."""
        gen = WorkloadGenerator(self._make_config())
        assert gen.get_stats()["rejected_submissions"] == 0

    @pytest.mark.asyncio
    async def test_503_increments_rejected_counter(self):
        """An HTTP 503 response from the broker must increment the rejected counter."""
        config = self._make_config(
            arrival_rate=100.0,
            duration_s=0.1,
            pipeline_mix={"sensor_fusion": 1.0},
        )
        gen = WorkloadGenerator(config)

        # Mock httpx to return 503 for all publish calls
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError(
                "503 Service Unavailable",
                request=MagicMock(),
                response=mock_response,
            )
        )

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_client):
            await gen.run()

        stats = gen.get_stats()
        assert stats["rejected_submissions"] > 0, (
            "503 responses must increment rejected_submissions counter"
        )

    @pytest.mark.asyncio
    async def test_successful_publish_does_not_increment_rejected(self):
        """Successful publishes (HTTP 200) must NOT increment the rejected counter."""
        config = self._make_config(
            arrival_rate=100.0,
            duration_s=0.1,
            pipeline_mix={"sensor_fusion": 1.0},
        )
        gen = WorkloadGenerator(config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_client):
            await gen.run()

        stats = gen.get_stats()
        assert stats["rejected_submissions"] == 0


# ---------------------------------------------------------------------------
# Fix 2: Wire PIPELINE_MIX env vars
# ---------------------------------------------------------------------------


class TestPipelineMixEnvVars:
    """PIPELINE_MIX_CQI/ANOMALY/FUSION env vars must be consumed by the workload CLI."""

    def test_env_vars_override_default_mix(self):
        """When PIPELINE_MIX_* env vars are set, the CLI must use them instead of defaults."""
        from src.workload.generator import _parse_args

        env_patch = {
            "PIPELINE_MIX_CQI": "0.5",
            "PIPELINE_MIX_ANOMALY": "0.5",
            "PIPELINE_MIX_FUSION": "0.0",
        }
        with patch.dict(os.environ, env_patch):
            with patch("sys.argv", ["workload"]):
                args = _parse_args()

        # The parsed args or the resulting config must reflect the env vars.
        # We test the full config construction path.
        from src.workload.generator import WorkloadConfig

        # Build config the same way main() does when no --config is given
        with patch.dict(os.environ, env_patch):
            mix = _build_mix_from_env_or_default()
            assert abs(mix["cqi_prediction"] - 0.5) < 1e-6
            assert abs(mix["anomaly_detection"] - 0.5) < 1e-6
            assert abs(mix["sensor_fusion"] - 0.0) < 1e-6

    def test_env_vars_absent_uses_default_mix(self):
        """Without PIPELINE_MIX_* env vars, the default 40/40/20 mix is used."""
        env_clean = {
            k: v for k, v in os.environ.items()
            if not k.startswith("PIPELINE_MIX_")
        }
        with patch.dict(os.environ, env_clean, clear=True):
            mix = _build_mix_from_env_or_default()
            assert abs(mix["cqi_prediction"] - 0.4) < 1e-6
            assert abs(mix["anomaly_detection"] - 0.4) < 1e-6
            assert abs(mix["sensor_fusion"] - 0.2) < 1e-6


def _build_mix_from_env_or_default() -> dict[str, float]:
    """Helper that mimics how main() should build the pipeline mix.

    This function exists to test the env-var wiring. After the fix,
    the workload generator's main() should call something equivalent.
    """
    from src.workload.generator import build_pipeline_mix_from_env
    return build_pipeline_mix_from_env()
