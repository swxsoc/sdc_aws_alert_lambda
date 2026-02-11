import pytest_lazyfixture
from unittest.mock import patch, MagicMock
from lambda_function.src.lambda import handler

def test_handler_success():
    event = {"key": "value"}
    context = MagicMock()
    
    response = handler(event, context)
    assert response["statusCode"] == 200