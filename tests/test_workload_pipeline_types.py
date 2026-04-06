"""Tests for 8-stage O-RAN pipeline registration and selection.

Verifies that:
- 8-stage pipeline factories are registered in WorkloadGenerator._TEMPLATES
- 8-stage pipeline factories are registered in broker _PIPELINE_FACTORIES
- Templates and factories are synchronized (same keys)
- PIPELINE_TYPE env var selects a single pipeline type (100%)
- 8-stage pipelines publish successfully through the NeuralBroker
"""

import os

import pytest

from src.workload.generator import WorkloadGenerator, build_pipeline_mix_from_env


# ---------------------------------------------------------------------------
# Task 2.1: 8-stage pipelines registered in workload generator
# ---------------------------------------------------------------------------


class TestEightStageInWorkloadTemplates:
    """8-stage O-RAN pipeline factories must be in _TEMPLATES."""

    def test_cqi_chain_in_templates(self):
        assert "cqi_chain" in WorkloadGenerator._TEMPLATES

    def test_anomaly_sp_in_templates(self):
        assert "anomaly_sp" in WorkloadGenerator._TEMPLATES

    def test_ran_entangled_in_templates(self):
        assert "ran_entangled" in WorkloadGenerator._TEMPLATES

    def test_cqi_chain_returns_8_stages(self):
        dag = WorkloadGenerator._TEMPLATES["cqi_chain"]()
        assert len(dag.stages) == 8

    def test_anomaly_sp_returns_8_stages(self):
        dag = WorkloadGenerator._TEMPLATES["anomaly_sp"]()
        assert len(dag.stages) == 8

    def test_ran_entangled_returns_8_stages(self):
        dag = WorkloadGenerator._TEMPLATES["ran_entangled"]()
        assert len(dag.stages) == 8


# ---------------------------------------------------------------------------
# Task 2.2: 8-stage pipelines registered in broker factories
# ---------------------------------------------------------------------------


class TestEightStageInBrokerFactories:
    """8-stage pipelines must also be in the broker's _PIPELINE_FACTORIES."""

    def test_cqi_chain_in_broker_factories(self):
        from src.broker.base import _PIPELINE_FACTORIES
        assert "cqi_chain" in _PIPELINE_FACTORIES

    def test_anomaly_sp_in_broker_factories(self):
        from src.broker.base import _PIPELINE_FACTORIES
        assert "anomaly_sp" in _PIPELINE_FACTORIES

    def test_ran_entangled_in_broker_factories(self):
        from src.broker.base import _PIPELINE_FACTORIES
        assert "ran_entangled" in _PIPELINE_FACTORIES

    def test_broker_factory_cqi_chain_returns_8_stages(self):
        from src.broker.base import _PIPELINE_FACTORIES
        dag = _PIPELINE_FACTORIES["cqi_chain"]({})
        assert len(dag.stages) == 8

    def test_broker_factory_anomaly_sp_returns_8_stages(self):
        from src.broker.base import _PIPELINE_FACTORIES
        dag = _PIPELINE_FACTORIES["anomaly_sp"]({})
        assert len(dag.stages) == 8

    def test_broker_factory_ran_entangled_returns_8_stages(self):
        from src.broker.base import _PIPELINE_FACTORIES
        dag = _PIPELINE_FACTORIES["ran_entangled"]({})
        assert len(dag.stages) == 8

    def test_templates_and_factories_synchronized(self):
        from src.broker.base import _PIPELINE_FACTORIES
        for key in ["cqi_chain", "anomaly_sp", "ran_entangled"]:
            assert key in WorkloadGenerator._TEMPLATES, (
                f"'{key}' in _PIPELINE_FACTORIES but missing from _TEMPLATES"
            )
            assert key in _PIPELINE_FACTORIES, (
                f"'{key}' in _TEMPLATES but missing from _PIPELINE_FACTORIES"
            )


# ---------------------------------------------------------------------------
# Task 2.3: PIPELINE_TYPE env var override
# ---------------------------------------------------------------------------


class TestPipelineTypeEnvOverride:
    """PIPELINE_TYPE env var must select a single pipeline type at 100%."""

    def test_pipeline_type_env_selects_single_type(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_TYPE", "cqi_chain")
        mix = build_pipeline_mix_from_env()
        assert mix == {"cqi_chain": 1.0}

    def test_pipeline_type_env_overrides_pipeline_mix(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_TYPE", "anomaly_sp")
        monkeypatch.setenv("PIPELINE_MIX_CQI", "0.5")
        monkeypatch.setenv("PIPELINE_MIX_ANOMALY", "0.5")
        monkeypatch.setenv("PIPELINE_MIX_FUSION", "0.0")
        mix = build_pipeline_mix_from_env()
        assert mix == {"anomaly_sp": 1.0}

    def test_invalid_pipeline_type_raises(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_TYPE", "nonexistent")
        with pytest.raises(ValueError, match="Unknown PIPELINE_TYPE"):
            build_pipeline_mix_from_env()

    def test_original_3stage_types_still_work(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_TYPE", "cqi_prediction")
        mix = build_pipeline_mix_from_env()
        assert mix == {"cqi_prediction": 1.0}


# ---------------------------------------------------------------------------
# Task 2.6: Integration — publish 8-stage pipeline through broker
# ---------------------------------------------------------------------------


class TestPublish8StageThroughBroker:
    """8-stage pipelines must publish successfully through NeuralBroker."""

    @pytest.mark.asyncio
    async def test_publish_cqi_chain_8stage(self):
        from src.broker.models import WorkerInfo
        from src.broker.neural_broker import BrokerConfig, NeuralBroker
        from httpx import ASGITransport, AsyncClient

        broker = NeuralBroker(BrokerConfig(domain_id="d1", broker_id="b1"))
        for i in range(4):
            broker._workers[f"w{i}"] = WorkerInfo(
                node_id=f"w{i}",
                domain_id=f"d{i % 2 + 1}",
                slice_id="flat",
                capacity=5.0,
                url=f"http://w{i}:8081",
                bid_cost_ms=10.0,
            )
        broker._rebuild_topology()
        app = broker.build_app()

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            resp = await c.post(
                "/publish",
                json={"pipeline_type": "cqi_chain", "config": {}},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert len(data["placement"]) == 8, (
            f"Expected 8 stages placed, got {len(data['placement'])}"
        )
