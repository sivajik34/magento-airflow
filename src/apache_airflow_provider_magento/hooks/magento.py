from __future__ import annotations
from requests_oauthlib import OAuth1
import requests

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
import json

class MagentoHook(BaseHook):
    """Interacts with Magento via REST API and GraphQL API, supporting synchronous and asynchronous operations."""

    conn_name_attr = "magento_conn_id"
    default_conn_name = "magento_default"
    conn_type = "magento"
    hook_name = "Magento" 
 
    def __init__(self, magento_conn_id=default_conn_name):
        super().__init__()
        self.magento_conn_id = magento_conn_id        
        self.connection = self.get_connection(self.magento_conn_id)
        
        self._validate_connection()
        self._configure_oauth()

    def _validate_connection(self):
        """Validate that all necessary Magento connection details are provided."""
        if not self.connection.host:
            raise AirflowException("Magento connection host is not set properly in Airflow connection.")
        
        required_fields = ["consumer_key", "consumer_secret", "access_token", "access_token_secret"]
        if not all(self.connection.extra_dejson.get(field) for field in required_fields):
            raise AirflowException("Magento OAuth credentials are not set properly in Airflow connection.")

    def _configure_oauth(self):
        """Configure OAuth authentication for Magento API requests."""
        self.oauth = OAuth1(
            self.connection.extra_dejson["consumer_key"],
            self.connection.extra_dejson["consumer_secret"],
            self.connection.extra_dejson["access_token"],
            self.connection.extra_dejson["access_token_secret"],
            signature_method='HMAC-SHA256'
        )

    def _build_url(self, endpoint):
        """Construct the full URL for Magento API endpoints."""
        base_url = self.connection.host if self.connection.host.startswith('http') else f"https://{self.connection.host}"
        base_url = base_url.rstrip('/')
        return f"{base_url}/{endpoint.lstrip('/')}"

    def _handle_response(self, response):
        """Handle and log HTTP responses from the Magento API."""
        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            self._log_error_response(response)
            raise AirflowException(f"HTTP error occurred: {http_err}")
        except requests.exceptions.Timeout as timeout_err:
            self.log.error("Request timed out: %s", timeout_err)
            raise AirflowException(f"Request timed out: {timeout_err}")
        except requests.exceptions.RequestException as req_err:
            self._log_error_response(response)
            raise AirflowException(f"Request failed: {req_err}")
        except json.JSONDecodeError as json_err:
            self.log.error("Failed to decode JSON response: %s", json_err)
            raise AirflowException(f"Failed to decode JSON response: {json_err}")
        except Exception as e:
            self.log.error("An unexpected error occurred: %s", e, exc_info=True)
            raise AirflowException(f"An unexpected error occurred: {e}")

    def _log_error_response(self, response):
        """Log error details from the HTTP response."""
        try:
            error_details = response.json()
        except json.JSONDecodeError:
            error_details = {"error": "Failed to decode error details"}
        self.log.error("Response status code: %d", response.status_code)
        self.log.error("Response headers: %s", response.headers)
        self.log.error("Response body: %s", response.text)
        self.log.error("Error details: %s", error_details)

    def send_request(self, endpoint, method="GET", data=None, headers=None):
        """Send an HTTP request to the Magento API."""       
        url = self._build_url(endpoint)
        
        if headers and 'Authorization' in headers:
           response = requests.request(method, url, json=data, headers=headers, verify=False)
        else:
           response = requests.request(method, url, json=data, auth=self.oauth, headers=headers, verify=False)
        return self._handle_response(response)       
    
