import pytest
from unittest.mock import patch, MagicMock
from airflow.exceptions import AirflowException
from requests.models import Response
from requests_oauthlib import OAuth1
import requests
import json
from apache_airflow_provider_magento.hooks.magento import MagentoHook  # Replace with the actual import path

@pytest.fixture
def mock_connection():
    """Fixture to mock the Airflow connection."""
    conn = MagicMock()
    conn.host = "magento.local"
    conn.extra_dejson = {
        "consumer_key": "key",
        "consumer_secret": "secret",
        "access_token": "token",
        "access_token_secret": "token_secret"
    }
    return conn

@pytest.fixture
def magento_hook(mock_connection):
    """Fixture to create an instance of MagentoHook with the mocked connection."""
    with patch("apache_airflow_provider_magento.hooks.magento.MagentoHook.get_connection", return_value=mock_connection):  # Adjust import as needed
        return MagentoHook(magento_conn_id="test_conn_id")

def test_validate_connection_success(magento_hook):
    """Test successful connection validation."""
    magento_hook._validate_connection()

def test_validate_connection_missing_host(magento_hook, mock_connection):
    """Test connection validation with missing host."""
    mock_connection.host = None
    with pytest.raises(AirflowException, match="Magento connection host is not set properly"):
        magento_hook._validate_connection()

def test_validate_connection_missing_credentials(magento_hook, mock_connection):
    """Test connection validation with missing OAuth credentials."""
    mock_connection.extra_dejson = {}
    with pytest.raises(AirflowException, match="Magento OAuth credentials are not set properly"):
        magento_hook._validate_connection()

def test_configure_oauth(magento_hook):
    """Test OAuth configuration."""
    magento_hook._configure_oauth()
    assert isinstance(magento_hook.oauth, OAuth1)
    assert magento_hook.oauth.client.client_key == "key"

def test_build_url(magento_hook):
    """Test URL construction."""
    url = magento_hook._build_url("/rest/V1/orders")
    assert url == "https://magento.local/rest/V1/orders"

def test_handle_response_success(magento_hook):
    """Test handling a successful HTTP response."""
    response = MagicMock(spec=Response)
    response.raise_for_status = MagicMock()
    response.json.return_value = {"success": True}
    result = magento_hook._handle_response(response)
    assert result == {"success": True}

def test_handle_response_http_error(magento_hook):
    """Test handling an HTTP error."""
    response = MagicMock(spec=Response)
    response.raise_for_status.side_effect = requests.exceptions.HTTPError("HTTP Error")
    response.status_code = 500
    response.headers = {"Content-Type": "application/json"}
    response.text = '{"error": "test"}'
    response.json.return_value = {"error": "test"}
    with pytest.raises(AirflowException, match="HTTP error occurred: HTTP Error"):
        magento_hook._handle_response(response)

def test_handle_response_timeout(magento_hook):
    """Test handling a timeout error."""
    response = MagicMock(spec=Response)
    response.raise_for_status.side_effect = requests.exceptions.Timeout("Timeout Error")
    with pytest.raises(AirflowException, match="Request timed out: Timeout Error"):
        magento_hook._handle_response(response)

def test_handle_response_json_error(magento_hook):
    """Test handling a JSON decode error."""
    response = MagicMock(spec=Response)
    response.raise_for_status = MagicMock()
    response.json.side_effect = json.JSONDecodeError("Expecting value", "", 0)
    with pytest.raises(AirflowException, match="Failed to decode JSON response"):
        magento_hook._handle_response(response)

def test_send_request_get(magento_hook):
    """Test sending a GET request."""
    with patch("requests.request") as mock_request:
        mock_response = MagicMock(spec=Response)
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"data": "test"}
        mock_request.return_value = mock_response
        result = magento_hook.send_request("/rest/V1/orders", method="GET")
        mock_request.assert_called_once_with("GET", "https://magento.local/rest/V1/orders", json=None, auth=magento_hook.oauth, headers=None, verify=False)
        assert result == {"data": "test"}

def test_send_request_post(magento_hook):
    """Test sending a POST request."""
    with patch("requests.request") as mock_request:
        mock_response = MagicMock(spec=Response)
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"data": "test"}
        mock_request.return_value = mock_response
        data = {"order": "test"}
        result = magento_hook.send_request("/rest/V1/orders", method="POST", data=data)
        mock_request.assert_called_once_with("POST", "https://magento.local/rest/V1/orders", json=data, auth=magento_hook.oauth, headers=None, verify=False)
        assert result == {"data": "test"}

def test_send_request_with_authorization_header(magento_hook):
    """Test sending a request with an Authorization header."""
    with patch("requests.request") as mock_request:
        mock_response = MagicMock(spec=Response)
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"data": "test"}
        mock_request.return_value = mock_response
        headers = {"Authorization": "Bearer test_token"}
        result = magento_hook.send_request("/rest/V1/orders", method="GET", headers=headers)
        mock_request.assert_called_once_with("GET", "https://magento.local/rest/V1/orders", json=None, headers=headers, verify=False)
        assert result == {"data": "test"}

