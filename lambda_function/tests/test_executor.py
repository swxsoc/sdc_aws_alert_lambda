import importlib
import os
import sys
from unittest.mock import MagicMock, patch


def _load_lambda_module():
    tests_dir = os.path.dirname(__file__)
    src_dir = os.path.abspath(os.path.join(tests_dir, "..", "src"))
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    return importlib.import_module("lambda")


def test_handler_success():
    lambda_mod = _load_lambda_module()
    handler = lambda_mod.handler
    event = {
        "resources": [
            "arn:aws:events:us-east-1:123456789012:rule/get_GOESXRS_alert_stream"
        ]
    }
    context = MagicMock()

    with patch.object(lambda_mod.executor.Executor, "__init__", return_value=None), patch.object(
        lambda_mod.executor.Executor, "execute", return_value=None
    ):
        response = handler(event, context)

    assert response["statusCode"] == 200
