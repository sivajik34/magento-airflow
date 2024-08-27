from airflow.providers.http.hooks.http import HttpHook
from requests_oauthlib import OAuth1
from airflow.exceptions import AirflowException
import json

class MagentoHook(HttpHook):
    """Interacts with Magento via REST API and GraphQL API, supporting synchronous and asynchronous operations."""

    conn_name_attr = "magento_conn_id"
    default_conn_name = "magento_default"
    conn_type = "magento"
    hook_name = "Magento"

    def __init__(self, magento_conn_id=default_conn_name, method="GET"):
        super().__init__(http_conn_id=magento_conn_id, method=method)
        self.magento_conn_id = magento_conn_id        
        self.connection = self.get_connection(self.magento_conn_id)
        self.method = method
        self.base_url = self.connection.host        
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
    
    def get_conn(self, headers=None):
        if headers and 'Authorization' in headers:
            return super().get_conn(headers)
        else:    
            session = super().get_conn(headers)        
            session.auth = self.oauth
            return session    
   
    def send_request(self, endpoint, data=None, headers=None, extra_options=None):
        """Send an HTTP request to the Magento API."""
        
        # Ensure extra_options is initialized properly
        if extra_options is None:
            extra_options = {}
        
        if headers is None:
            headers = {}
            
        # Set the Content-Type to application/json if not already set
        headers.setdefault('Content-Type', 'application/json')

        # Convert data to JSON if data is provided and method is POST/PUT
        if data and self.method in ['POST', 'PUT','DELETE']:
            data = json.dumps(data)
        return self.run(endpoint, data=data, headers=headers, extra_options={"verify": False})       

