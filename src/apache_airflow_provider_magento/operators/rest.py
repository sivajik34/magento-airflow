from __future__ import annotations

from airflow.models.baseoperator import BaseOperator
from apache_airflow_provider_magento.hooks.magento import MagentoHook
from airflow.exceptions import AirflowException
import logging
from urllib.parse import urlencode

class MagentoRestOperator(BaseOperator):
    REST_ENDPOINT_TEMPLATE = "/rest/{store_view_code}/{api_version}"
    def __init__(self, 
                 endpoint: str, 
                 method: str = 'GET', 
                 data: dict = None, 
                 search_criteria: dict = None, 
                 headers: dict = None,
                 magento_conn_id: str = "magento_default",
                 store_view_code: str = "default",
                 api_version='V1', 
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.method = method.upper()
        self.data = data or {}
        self.search_criteria = search_criteria or {}
        self.headers = headers or {}  # Initialize headers
        self.magento_conn_id = magento_conn_id
        self.store_view_code = store_view_code
        self.api_version = api_version.upper()

        # Validate HTTP method
        if self.method not in ['GET', 'POST', 'PUT', 'DELETE']:
            raise ValueError(f"Unsupported HTTP method: {self.method}")

    def execute(self, context):
        magento_hook = MagentoHook(self.magento_conn_id)
        endpoint=f"{self.REST_ENDPOINT_TEMPLATE.format(store_view_code=self.store_view_code, api_version=self.api_version)}/{self.endpoint.lstrip('/')}"        
        if self.search_criteria:
            query_string = urlencode(self.search_criteria, doseq=True)                       
            endpoint = f"{endpoint}?{query_string}"  
        result = None

        try:
            
            result = magento_hook.send_request(endpoint, self.method, data=self.data,  headers=self.headers)            

            self.log.info("Response received from Magento API: %s", result)
            return result

        except AirflowException as ae:
            self.log.error("AirflowException occurred: %s", ae)
            raise

        except ValueError as ve:
            self.log.error("ValueError occurred: %s", ve)
            raise

        except Exception as e:
            self.log.error("An unexpected error occurred: %s", e, exc_info=True)
            raise

