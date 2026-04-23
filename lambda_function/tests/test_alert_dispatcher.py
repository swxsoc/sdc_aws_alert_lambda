import json
import os
import sys
import types
from pathlib import Path

import pandas as pd
import pytest


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from alert_dispatcher import AlertDispatcher, handle_event  # noqa: E402


class FakeSecretsClient:
    def get_secret_value(self, SecretId):
        secrets = {
            "arn:client-id": {"gcn_client_id": "client-id-value"},
            "arn:client-secret": {"gcn_client_secret": "client-secret-value"},
        }
        return {"SecretString": json.dumps(secrets[SecretId])}


class FakeSession:
    def client(self, service_name):
        assert service_name == "secretsmanager"
        return FakeSecretsClient()


class FakeProducer:
    instances = []

    def __init__(self, client_id, client_secret, domain):
        self.client_id = client_id
        self.client_secret = client_secret
        self.domain = domain
        self.messages = []
        FakeProducer.instances.append(self)

    def produce(self, topic, data):
        self.messages.append((topic, json.loads(data.decode())))

    def flush(self):
        return None


@pytest.fixture(autouse=True)
def clear_producer_instances():
    FakeProducer.instances.clear()
    yield
    FakeProducer.instances.clear()


@pytest.fixture
def alert_dispatcher_module(monkeypatch):
    import alert_dispatcher as alert_dispatcher_module

    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.session = types.SimpleNamespace(Session=lambda: FakeSession())
    fake_gcn_kafka = types.ModuleType("gcn_kafka")
    fake_gcn_kafka.Producer = FakeProducer

    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    monkeypatch.setitem(sys.modules, "gcn_kafka", fake_gcn_kafka)
    monkeypatch.setenv("GCN_CLIENT_ID_SECRET_ARN", "arn:client-id")
    monkeypatch.setenv("GCN_CLIENT_SECRET_SECRET_ARN", "arn:client-secret")
    monkeypatch.delenv("GCN_CLIENT_ID", raising=False)
    monkeypatch.delenv("GCN_CLIENT_SECRET", raising=False)
    return alert_dispatcher_module


def test_handle_event_rejects_missing_resources():
    response = handle_event({}, {})
    assert response["statusCode"] == 500
    assert "resources" in json.loads(response["body"])["error"]


def test_alert_dispatcher_loads_secrets(monkeypatch, alert_dispatcher_module):
    AlertDispatcher("get_GOESXRS_alert_stream")
    assert os.environ["GCN_CLIENT_ID"] == "client-id-value"
    assert os.environ["GCN_CLIENT_SECRET"] == "client-secret-value"


def test_goes_alert_stream_publishes_flux_and_threshold_alert(
    monkeypatch, alert_dispatcher_module
):
    now = pd.Timestamp("2026-03-25T12:00:00Z")
    frame = pd.DataFrame(
        [
            {"time_tag": "2026-03-25T11:50:00Z", "energy": "0.1-0.8nm", "flux": 4e-6},
            {"time_tag": "2026-03-25T11:56:00Z", "energy": "0.1-0.8nm", "flux": 6e-6},
            {"time_tag": "2026-03-25T11:58:00Z", "energy": "0.1-0.8nm", "flux": 7e-6},
            {"time_tag": "2026-03-25T11:59:00Z", "energy": "0.05-0.4nm", "flux": 1e-7},
        ]
    )

    class FakeDateTime:
        @staticmethod
        def now(tz=None):
            return now.to_pydatetime()

    monkeypatch.setattr(pd, "read_json", lambda url: frame.copy())
    monkeypatch.setattr(alert_dispatcher_module, "datetime", FakeDateTime)

    AlertDispatcher("get_GOESXRS_alert_stream").goes_xrs_alert_stream()

    producer = FakeProducer.instances[-1]
    assert producer.client_id == "client-id-value"
    assert producer.client_secret == "client-secret-value"
    assert producer.domain == "test.gcn.nasa.gov"

    topics = [topic for topic, _ in producer.messages]
    assert "gcn.notices.swxsoc.goes_xrs_flux" in topics
    assert "gcn.notices.swxsoc.goes_xrs_c5flare_alert" in topics

    alert_payload = next(
        payload
        for topic, payload in producer.messages
        if topic == "gcn.notices.swxsoc.goes_xrs_c5flare_alert"
    )
    assert alert_payload["alert_tense"] == "current"
    assert alert_payload["alert_type"] == "C5 Flare Alert"


def test_goes_alert_stream_publishes_threshold_end_alert(
    monkeypatch, alert_dispatcher_module
):
    now = pd.Timestamp("2026-03-25T12:00:00Z")
    frame = pd.DataFrame(
        [
            {"time_tag": "2026-03-25T11:54:00Z", "energy": "0.1-0.8nm", "flux": 6e-6},
            {"time_tag": "2026-03-25T11:56:00Z", "energy": "0.1-0.8nm", "flux": 4e-6},
            {"time_tag": "2026-03-25T11:58:00Z", "energy": "0.1-0.8nm", "flux": 4e-6},
        ]
    )

    class FakeDateTime:
        @staticmethod
        def now(tz=None):
            return now.to_pydatetime()

    monkeypatch.setattr(pd, "read_json", lambda url: frame.copy())
    monkeypatch.setattr(alert_dispatcher_module, "datetime", FakeDateTime)

    AlertDispatcher("get_GOESXRS_alert_stream").goes_xrs_alert_stream()

    producer = FakeProducer.instances[-1]
    end_payload = next(
        payload
        for topic, payload in producer.messages
        if topic == "gcn.notices.swxsoc.goes_xrs_c5flare_alert"
        and payload["alert_type"] == "C5 Flare Alert End"
    )
    assert end_payload["description"] == "GOES XRS flux has decreased below C5 threshold"


def test_handle_event_dispatches_matching_rule(monkeypatch, alert_dispatcher_module):
    called = {"executed": False}

    def fake_execute(self):
        called["executed"] = True

    monkeypatch.setattr(AlertDispatcher, "execute", fake_execute)
    event = {"resources": ["arn:aws:events:us-east-1:123456789012:rule/get_GOESXRS_alert_stream"]}

    response = handle_event(event, {})

    assert response["statusCode"] == 200
    assert called["executed"] is True
