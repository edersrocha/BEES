import pytest
from unittest.mock import patch, MagicMock
from src.api_client.openbrewerydb import breweries
from src.config import API_OPEN_BREWERY_DB_BASE_URL, API_OPEN_BREWERY_DB_TIMEOUT


@patch("src.api_client.openbrewerydb.requests.get")
def test_breweries_returns_all_pages(mock_get):
    response_page_1 = MagicMock()
    response_page_1.json.return_value = [{"id": 1, "name": "Brew 1"}]
    response_page_1.status_code = 200
    response_page_1.raise_for_status.return_value = None

    response_page_2 = MagicMock()
    response_page_2.json.return_value = [{"id": 2, "name": "Brew 2"}]
    response_page_2.status_code = 200
    response_page_2.raise_for_status.return_value = None

    response_page_3 = MagicMock()
    response_page_3.json.return_value = []
    response_page_3.status_code = 200
    response_page_3.raise_for_status.return_value = None

    mock_get.side_effect = [response_page_1, response_page_2, response_page_3]

    result = breweries(per_page=1)

    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2

    expected_url = f"{API_OPEN_BREWERY_DB_BASE_URL}/breweries"
    mock_get.assert_any_call(expected_url, params={"page": 1, "per_page": 1}, timeout=API_OPEN_BREWERY_DB_TIMEOUT)
    mock_get.assert_any_call(expected_url, params={"page": 2, "per_page": 1}, timeout=API_OPEN_BREWERY_DB_TIMEOUT)
    mock_get.assert_any_call(expected_url, params={"page": 3, "per_page": 1}, timeout=API_OPEN_BREWERY_DB_TIMEOUT)
    assert mock_get.call_count == 3
