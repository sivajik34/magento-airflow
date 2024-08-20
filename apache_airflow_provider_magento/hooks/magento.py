from requests_oauthlib import OAuth1
import requests
from urllib.parse import urlencode
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

class MagentoHook(BaseHook):
    """Creates a connection to Magento and allows data interactions via Magento's REST API."""

    conn_name_attr = "magento_conn_id"
    default_conn_name = "magento_default"
    conn_type = "magento"
    hook_name = "Magento"
    BASE_URL = "/rest/default/V1"  # Common base URL for Magento API

    def __init__(self, magento_conn_id=default_conn_name):
        super().__init__()
        self.magento_conn_id = magento_conn_id
        self.connection = self.get_connection(self.magento_conn_id)
        self._validate_connection()
        self._configure_oauth()

    def _validate_connection(self):
        """Validate Magento connection configuration."""
        if not self.connection.host:
            raise AirflowException("Magento connection host is not set properly in Airflow connection")
        if not all([self.connection.extra_dejson.get(field) for field in ["consumer_key", "consumer_secret", "access_token", "access_token_secret"]]):
            raise AirflowException("Magento OAuth credentials are not set properly in Airflow connection")

    def _configure_oauth(self):
        """Configure OAuth authentication for Magento API."""
        self.consumer_key = self.connection.extra_dejson.get("consumer_key")
        self.consumer_secret = self.connection.extra_dejson.get("consumer_secret")
        self.access_token = self.connection.extra_dejson.get("access_token")
        self.access_token_secret = self.connection.extra_dejson.get("access_token_secret")

        self.oauth = OAuth1(
            self.consumer_key,
            self.consumer_secret,
            self.access_token,
            self.access_token_secret,
            signature_method='HMAC-SHA256'
        )

    def _get_full_url(self, endpoint):
        """Construct the full URL for Magento API."""        
        base_url = self.connection.host
        base_url = base_url if base_url.startswith('http') else f"https://{base_url}"
        base_url = base_url.rstrip('/')  # Ensure no trailing slash
        endpoint_url = endpoint.lstrip('/')  # Ensure no leading slash
        return f"{base_url}{self.BASE_URL}/{endpoint_url}"        

    def _handle_response(self, response):
        """Handle HTTP response, logging errors and raising exceptions if needed."""
        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            error_details = response.json() if response.content else {}
            self.log.error("Error details: %s", error_details)
            raise AirflowException(f"Request failed: {http_err}. Error details: {error_details}")
        except requests.exceptions.RequestException as e:
            raise AirflowException(f"Request failed: {str(e)}")

    def _send_request(self, endpoint, method="GET", data=None, search_criteria=None):
        """Perform an API request to Magento."""
        url = self._get_full_url(endpoint)
        if search_criteria:
            query_string = urlencode(search_criteria, doseq=True)
            url = f"{url}?{query_string}"

        try:
            response = requests.request(method, url, auth=self.oauth, json=data, verify=False)
            return self._handle_response(response)
        except requests.exceptions.RequestException as e:
            raise AirflowException(f"Request failed: {str(e)}")

    def get_request(self, endpoint, search_criteria=None):
        """Perform a GET API request to Magento."""
        return self._send_request(endpoint, method="GET", search_criteria=search_criteria)

    def post_request(self, endpoint, data=None):
        """Perform a POST API request to Magento."""
        return self._send_request(endpoint, method="POST", data=data)

    def put_request(self, endpoint, data=None):
        """Perform a PUT API request to Magento."""
        return self._send_request(endpoint, method="PUT", data=data)

    def delete_request(self, endpoint, data=None):
        """Perform a DELETE API request to Magento."""
        return self._send_request(endpoint, method="DELETE", data=data)

