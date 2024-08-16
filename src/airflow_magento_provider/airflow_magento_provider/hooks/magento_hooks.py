import time
import requests
import uuid
import hashlib
import hmac
import base64
from requests_oauthlib import OAuth1Session
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

        # Generate the signature
        base_string = f"{method.upper()}&{requests.utils.quote(url, safe='')}"

        parameters = {
            'oauth_consumer_key': self.consumer_key,
            'oauth_nonce': oauth_nonce,
            'oauth_signature_method': oauth_signature_method,
            'oauth_timestamp': oauth_timestamp,
            'oauth_token': self.access_token,
            'oauth_version': oauth_version
        }

        parameter_string = '&'.join([f"{requests.utils.quote(k, safe='')}={requests.utils.quote(v, safe='')}" 
                                     for k, v in sorted(parameters.items())])
        base_string += f"&{requests.utils.quote(parameter_string, safe='')}"

        signing_key = f"{requests.utils.quote(self.consumer_secret, safe='')}&{requests.utils.quote(self.access_token_secret, safe='')}"
        oauth_signature = base64.b64encode(hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha256).digest()).decode()


        parameters['oauth_signature'] = oauth_signature
        auth_header = 'OAuth ' + ', '.join([f'{k}="{v}"' for k, v in parameters.items()])

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
        except requests.exceptions.RequestException as e:
            raise AirflowException(f"Request failed: {str(e)}")

        return response.json()

