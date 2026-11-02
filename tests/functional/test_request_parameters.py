import pytest
from tests.support.requests import request_parameters


def test_default_page_limit():
    assert request_parameters({}) == {"$limit": "1000"}


def test_api_can_rename_limit_and_set_page_size():
    config = {
        "pagination": {"limit_parameter": "page_size", "page_size": 50},
        "query_parameters": {"category": "taxi"},
    }
    assert request_parameters(config) == {"page_size": "50", "category": "taxi"}


def test_api_can_disable_pagination():
    assert request_parameters({"pagination": None}) == {}


def test_explicit_query_value_overrides_default():
    assert request_parameters({"query_parameters": {"$limit": "25"}}) == {"$limit": "25"}


@pytest.mark.parametrize("size", [0, -1, True, "ten"])
def test_invalid_page_size_rejected(size):
    with pytest.raises(ValueError, match="page_size"):
        request_parameters({"pagination": {"page_size": size}})


def test_empty_limit_parameter_rejected():
    with pytest.raises(ValueError, match="limit_parameter"):
        request_parameters({"pagination": {"limit_parameter": ""}})
