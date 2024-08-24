import pytest
import requests
from unittest.mock import patch, MagicMock
from requests_oauthlib import OAuth1
from airflow.exceptions import AirflowException
from apache_airflow_provider_magento.hooks.magento import MagentoHook

@pytest.fixture
def mock_airflow_connection():
    """Fixture to mock Airflow connection."""
    mock_connection = MagicMock()
    mock_connection.host = 'magento.test'
    mock_connection.extra_dejson = {
        'consumer_key': 'key',
        'consumer_secret': 'secret',
        'access_token': 'token',
        'access_token_secret': 'token_secret'
    }
    return mock_connection

@pytest.fixture
def magento_hook(mock_airflow_connection):
    """Fixture to initialize MagentoHook with mocked connection."""
    with patch.object(MagentoHook, 'get_connection', return_value=mock_airflow_connection):
        hook = MagentoHook()
        hook._configure_oauth()  # Manually configure OAuth to ensure it's set up
        return hook

def test_validate_connection_success(magento_hook):
    """Test successful connection validation."""
    try:
        magento_hook._validate_connection()
    except AirflowException:
        pytest.fail("Unexpected AirflowException raised during connection validation.")

def test_validate_connection_failure(magento_hook):
    """Test connection validation failure due to missing credentials."""
    magento_hook.connection.extra_dejson = {
        'consumer_key': 'key',
        'consumer_secret': 'secret',
        'access_token': 'token'
        # Missing 'access_token_secret'
    }
    with pytest.raises(AirflowException, match="Magento OAuth credentials are not set properly"):
        magento_hook._validate_connection()

@patch('requests.request')
def test_send_request_success(mock_request, magento_hook):
    """Test successful API request."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()
    mock_response.json = MagicMock(return_value={'result': 'success'})
    mock_request.return_value = mock_response

    result = magento_hook._send_request('endpoint', method='GET')
    assert result == {'result': 'success'}
    mock_request.assert_called_once_with(
        'GET',
        'https://magento.test/rest/default/V1/endpoint',
        auth=magento_hook.oauth,
        json=None,
        verify=False
    )

@patch('requests.request')
def test_handle_response_error(mock_request, magento_hook):
    """Test handling an error response."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock(side_effect=requests.exceptions.HTTPError('Error'))
    mock_response.json = MagicMock(return_value={'error': 'details'})
    mock_request.return_value = mock_response

    with pytest.raises(AirflowException, match="Request failed: Error. Error details: {'error': 'details'}"):
        magento_hook._handle_response(mock_response)

def test_get_full_url(magento_hook):
    """Test URL construction."""
    url = magento_hook._get_full_url('endpoint')
    assert url == 'https://magento.test/rest/default/V1/endpoint'

@patch('requests.request')
def test_get_request(mock_request, magento_hook):
    """Test GET request method."""
    mock_response = MagicMock()
    mock_response.json = MagicMock(return_value={'result': 'success'})
    mock_request.return_value = mock_response

    result = magento_hook.get_request('endpoint')
    assert result == {'result': 'success'}

# Add similar tests for post_request, put_request, and delete_request if needed.



