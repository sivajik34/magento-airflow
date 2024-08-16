import urllib.parse
import hmac
import hashlib
import base64
import time
import uuid
from urllib.parse import urlencode
import requests
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

class MagentoHook(BaseHook):

    def __init__(self, magento_conn_id='magento', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.magento_conn_id = magento_conn_id
        self.connection = self.get_connection(self.magento_conn_id)
        self._configure_oauth()

    def _configure_oauth(self):
        """Configure OAuth authentication for Magento API"""
        conn = self.get_connection(self.magento_conn_id)
        self.consumer_key = conn.extra_dejson.get('consumer_key')
        self.consumer_secret = conn.extra_dejson.get('consumer_secret')
        self.access_token = conn.extra_dejson.get('access_token')
        self.access_token_secret = conn.extra_dejson.get('access_token_secret')

        if not all([self.consumer_key, self.consumer_secret, self.access_token, self.access_token_secret]):
            raise AirflowException("Magento OAuth credentials are not set properly in Airflow connection")

    def _generate_oauth_parameters(self, url, method, data=None):
        """Generate OAuth parameters for the request"""
        oauth_nonce = str(uuid.uuid4())
        oauth_timestamp = str(int(time.time()))
        oauth_signature_method = 'HMAC-SHA256'
        oauth_version = '1.0'

        # Parse the URL to separate the query parameters
        parsed_url = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qsl(parsed_url.query)

        # Prepare OAuth parameters
        oauth_params = {
            'oauth_consumer_key': self.consumer_key,
            'oauth_nonce': oauth_nonce,
            'oauth_signature_method': oauth_signature_method,
            'oauth_timestamp': oauth_timestamp,
            'oauth_token': self.access_token,
            'oauth_version': oauth_version
        }

        # Include data parameters if present
        if data:
            query_params.extend(data.items())

        # Combine OAuth and query/data parameters, and sort them
        all_params = oauth_params.copy()
        all_params.update(query_params)
        sorted_params = sorted(all_params.items(), key=lambda x: x[0])

        # Encode and create the parameter string
        param_str = urllib.parse.urlencode(sorted_params, safe='')

        # Construct the base string
        base_string = f"{method.upper()}&{urllib.parse.quote(parsed_url.scheme + '://' + parsed_url.netloc + parsed_url.path, safe='')}&{urllib.parse.quote(param_str, safe='')}"

        # Create the signing key
        signing_key = f"{urllib.parse.quote(self.consumer_secret, safe='')}&{urllib.parse.quote(self.access_token_secret, safe='')}"

        # Generate the OAuth signature
        oauth_signature = base64.b64encode(hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha256).digest()).decode()

        # Add the signature to OAuth parameters
        oauth_params['oauth_signature'] = oauth_signature

        # Construct the Authorization header
        auth_header = 'OAuth ' + ', '.join([f'{urllib.parse.quote(k)}="{urllib.parse.quote(v)}"' for k, v in oauth_params.items()])        
        return auth_header

    def get_request(self, endpoint, method='GET', data=None):
        """Perform an API request to Magento"""
        url = f"https://{self.connection.host}/rest/default/V1/{endpoint}"
        headers = {
            'Content-Type': 'application/json',
            'Authorization': self._generate_oauth_parameters(url, method, data)
        }

        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, verify=False)  # Use GET method
            else:
                response = requests.request(method, url, headers=headers, json=data, verify=False)  # Use method provided

            response.raise_for_status()  # Will raise an error for bad responses

        except requests.exceptions.HTTPError as http_err:
            error_details = None
            try:
                # Attempt to parse and log the error response
                error_details = response.json() if response.content else {}
                self.log.error(f"Error details: {error_details}")
            except ValueError:
                # Failed to parse JSON response
                pass

            raise AirflowException(
                f"Request failed: {http_err}. Error details: {error_details}"
            )

        except requests.exceptions.RequestException as e:
            raise AirflowException(f"Request failed: {str(e)}")

        return response.json()

    def get_orders(self, search_criteria=None):
        """
        Fetch orders from Magento.

        :param search_criteria: Dictionary containing search criteria to filter orders
        :return: List of orders
        """
        endpoint = "orders"

        if search_criteria:
            # Convert search criteria dictionary to URL parameters
            query_string = urlencode(search_criteria, doseq=True)
            endpoint = f"{endpoint}?{query_string}"

        self.log.info(f"Requesting orders from Magento: {endpoint}")
        return self.get_request(endpoint)

