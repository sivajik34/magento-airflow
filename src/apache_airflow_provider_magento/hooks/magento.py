from __future__ import annotations
from requests_oauthlib import OAuth1
import requests
from urllib.parse import urlencode
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
import json
import time

class MagentoHook(BaseHook):
    """Interacts with Magento via REST API and GraphQL API, supporting synchronous and asynchronous operations."""

    conn_name_attr = "magento_conn_id"
    default_conn_name = "magento_default"
    conn_type = "magento"
    hook_name = "Magento"

    REST_ENDPOINT_TEMPLATE = "/rest/{store_view_code}/{api_version}"
    GRAPHQL_ENDPOINT = "/graphql"
    ASYNC_ENDPOINT_TEMPLATE = "/rest/{store_view_code}/async/{api_version}"
    BULK_ENDPOINT_TEMPLATE = "/rest/{store_view_code}/async/bulk/{api_version}"

    SUPPORTED_ASYNC_METHODS = {'POST', 'PUT', 'DELETE', 'PATCH'}

    def __init__(self, magento_conn_id=default_conn_name, store_view_code='default', api_version='V1'):
        super().__init__()
        self.magento_conn_id = magento_conn_id
        self.store_view_code = store_view_code
        self.api_version = api_version.upper()
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

    def _build_url(self, endpoint, template):
        """Construct the full URL for Magento API endpoints."""
        base_url = self.connection.host if self.connection.host.startswith('http') else f"https://{self.connection.host}"
        base_url = base_url.rstrip('/')
        return f"{base_url}{template.format(store_view_code=self.store_view_code, api_version=self.api_version)}/{endpoint.lstrip('/')}"

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

    def _send_request(self, endpoint, method="GET", data=None, search_criteria=None, headers=None, async_mode=False, bulk=False):
        """Send an HTTP request to the Magento API."""
        url_template = self.BULK_ENDPOINT_TEMPLATE if bulk else self.ASYNC_ENDPOINT_TEMPLATE if async_mode else self.REST_ENDPOINT_TEMPLATE
        url = self._build_url(endpoint, url_template)

        if search_criteria:
            query_string = urlencode(search_criteria, doseq=True)
            url = f"{url}?{query_string}"
        if headers and 'Authorization' in headers:
           response = requests.request(method, url, json=data, headers=headers, verify=False)
        else:
           response = requests.request(method, url, json=data, auth=self.oauth, headers=headers, verify=False)
        return self._handle_response(response)

    def get_request(self, endpoint, search_criteria=None, headers=None):
        """Perform a GET request to Magento."""
        return self._send_request(endpoint, method="GET", search_criteria=search_criteria, headers=headers)

    def post_request(self, endpoint, data=None, headers=None):
        """Perform a POST request to Magento."""
        return self._send_request(endpoint, method="POST", data=data, headers=headers)

    def put_request(self, endpoint, data=None, headers=None):
        """Perform a PUT request to Magento."""
        return self._send_request(endpoint, method="PUT", data=data, headers=headers)

    def delete_request(self, endpoint, data=None, headers=None):
        """Perform a DELETE request to Magento."""
        return self._send_request(endpoint, method="DELETE", data=data, headers=headers)

    def graphql_request(self, query, variables=None, headers=None):
        """Perform a GraphQL request to Magento."""
        url = self._build_url("", self.GRAPHQL_ENDPOINT)
        payload = {'query': query, 'variables': variables or {}}
        if headers and 'Authorization' in headers:
           response = requests.post(url, json=payload, headers=headers, verify=False)
        else:
           response = requests.post(url, json=payload, auth=self.oauth, headers=headers, verify=False)
        return self._handle_response(response)

    def async_post_request(self, endpoint, method, data=None, headers=None, bulk=False):
        """Perform an asynchronous API request to Magento."""
        if bulk and method == 'GET':
            raise AirflowException("Bulk GET requests are not supported.")
        if method not in self.SUPPORTED_ASYNC_METHODS:
            raise AirflowException(f"Unsupported HTTP method '{method}' for asynchronous requests. Supported methods are: {', '.join(self.SUPPORTED_ASYNC_METHODS)}.")
        
        return self._send_request(endpoint, method=method, data=data, headers=headers, async_mode=True, bulk=bulk)
    
