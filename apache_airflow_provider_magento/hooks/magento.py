from __future__ import annotations

from requests_oauthlib import OAuth1
import requests
from urllib.parse import urlencode
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
import json

class MagentoHook(BaseHook):
    """Creates a connection to Magento and allows data interactions via Magento's REST API and GraphQL API."""

    conn_name_attr = "magento_conn_id"
    default_conn_name = "magento_default"
    conn_type = "magento"
    hook_name = "Magento"

    BASE_URL = "/rest/default/V1"  # REST API base URL
    GRAPHQL_ENDPOINT = "/graphql"  # GraphQL API endpoint

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

    def _get_graphql_url(self):
        """Construct the full URL for Magento GraphQL API."""
        base_url = self.connection.host
        base_url = base_url if base_url.startswith('http') else f"https://{base_url}"
        return f"{base_url}{self.GRAPHQL_ENDPOINT}"

    def _handle_response(self, response):
        """Handle HTTP response, logging errors and raising exceptions if needed."""
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
        """Log detailed error information from the response."""
        try:
            error_details = response.json()
        except json.JSONDecodeError:
            error_details = {"error": "Failed to decode error details"}
        self.log.error("Response status code: %d", response.status_code)
        self.log.error("Response headers: %s", response.headers)
        self.log.error("Response body: %s", response.text)
        self.log.error("Error details: %s", error_details)

    def _send_request(self, endpoint, method="GET", data=None, search_criteria=None, headers=None):
        """Perform an API request to Magento."""
        url = self._get_full_url(endpoint)

        if search_criteria:
            query_string = urlencode(search_criteria, doseq=True)
            url = f"{url}?{query_string}"

        # Determine if headers contain an authorization token
        if headers and 'Authorization' in headers:
            response = requests.request(method, url, json=data, headers=headers, verify=False)
        else:
            response = requests.request(method, url, auth=self.oauth, json=data, headers=headers, verify=False)

        return self._handle_response(response)

    def get_request(self, endpoint, search_criteria=None, headers=None):
        """Perform a GET API request to Magento."""
        return self._send_request(endpoint, method="GET", search_criteria=search_criteria, headers=headers)

    def post_request(self, endpoint, data=None, headers=None):
        """Perform a POST API request to Magento."""
        return self._send_request(endpoint, method="POST", data=data, headers=headers)

    def put_request(self, endpoint, data=None, headers=None):
        """Perform a PUT API request to Magento."""
        return self._send_request(endpoint, method="PUT", data=data, headers=headers)

    def delete_request(self, endpoint, data=None, headers=None):
        """Perform a DELETE API request to Magento."""
        return self._send_request(endpoint, method="DELETE", data=data, headers=headers)

    def graphql_request(self, query, variables=None, headers=None):
        """Perform a GraphQL API request to Magento."""
        url = self._get_graphql_url()
        payload = {
            'query': query,
            'variables': variables or {}
        }

        # Determine if headers contain an authorization token
        if headers and 'Authorization' in headers:
            response = requests.post(url, json=payload, headers=headers, verify=False)
        else:
            response = requests.post(url, json=payload, auth=self.oauth, headers=headers, verify=False)

        return self._handle_response(response)

