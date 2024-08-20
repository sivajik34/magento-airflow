from requests_oauthlib import OAuth1
import requests
from urllib.parse import urlencode
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

class MagentoHook(BaseHook):
    """Creates new connection to Magento and allows you to pull data from Magento and push data to Magento."""

    conn_name_attr = "magento_conn_id"
    default_conn_name = "magento_default"
    conn_type = "magento"
    hook_name = "Magento"

    BASE_URL = "/rest/default/V1"  # Common base URL for Magento API

    def __init__(self, magento_conn_id=default_conn_name):
        super().__init__()
        self.magento_conn_id = magento_conn_id
        self.connection = self.get_connection(self.magento_conn_id)
        self._configure_oauth()
        if not self.connection.host:
            raise AirflowException("Magento connection host is not set properly in Airflow connection")

    def _configure_oauth(self):
        """Configure OAuth authentication for Magento API."""
        self.consumer_key = self.connection.extra_dejson.get("consumer_key")
        self.consumer_secret = self.connection.extra_dejson.get("consumer_secret")
        self.access_token = self.connection.extra_dejson.get("access_token")
        self.access_token_secret = self.connection.extra_dejson.get("access_token_secret")

        if not all([self.consumer_key, self.consumer_secret, self.access_token, self.access_token_secret]):
            raise AirflowException("Magento OAuth credentials are not set properly in Airflow connection")

        # Create an OAuth1 object with the credentials, specifying HMAC-SHA256 as the signature method
        self.oauth = OAuth1(
            self.consumer_key,
            self.consumer_secret,
            self.access_token,
            self.access_token_secret,
            signature_method='HMAC-SHA256'
        )

    def _get_full_url(self, endpoint):
        """Construct the full URL for Magento API."""
        return f"https://{self.connection.host}{self.BASE_URL}/{endpoint}"

    def get_request(self, endpoint, search_criteria=None, method="GET", data=None):
        """Perform an API request to Magento."""
        url = self._get_full_url(endpoint)
        if search_criteria:
            query_string = urlencode(search_criteria, doseq=True)
            url = f"{url}?{query_string}"

        try:
            if method.upper() == "GET":
                response = requests.get(url, auth=self.oauth, verify=False)
            else:
                response = requests.request(method, url, auth=self.oauth, json=data, verify=False)
            response.raise_for_status()  # Will raise an error for bad responses
        except requests.exceptions.HTTPError as http_err:
            error_details = response.json() if response.content else {}
            self.log.error("Error details: %s", error_details)
            raise AirflowException(f"Request failed: {http_err}. Error details: {error_details}")
        except requests.exceptions.RequestException as e:
            raise AirflowException(f"Request failed: {str(e)}")

        return response.json()

    def post_request(self, endpoint, data=None):
        """Perform a POST API request to Magento."""
        url = self._get_full_url(endpoint)
        try:
            response = requests.post(url, auth=self.oauth, json=data, verify=False)
            response.raise_for_status()  # Will raise an error for bad responses
        except requests.exceptions.HTTPError as http_err:
            error_details = response.json() if response.content else {}
            self.log.error("Error details: %s", error_details)
            raise AirflowException(f"Request failed: {http_err}. Error details: {error_details}")
        except requests.exceptions.RequestException as e:
            raise AirflowException(f"Request failed: {str(e)}")
        self.log.info(response.json)
        return response.json()

