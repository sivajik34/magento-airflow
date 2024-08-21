from __future__ import annotations
from airflow.models.baseoperator import BaseOperator
from apache_airflow_provider_magento.hooks.magento import MagentoHook
import logging
from airflow.exceptions import AirflowException

class MagentoApiOperator(BaseOperator):
    def __init__(self, endpoint: str, method: str = 'GET', data: dict = None, search_criteria: dict = None, magento_conn_id: str = "magento_default", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.method = method.upper()
        self.data = data or {}
        self.search_criteria = search_criteria or {}
        self.magento_conn_id = magento_conn_id

        # Validate HTTP method
        if self.method not in ['GET', 'POST', 'PUT', 'DELETE']:
            raise ValueError(f"Unsupported HTTP method: {self.method}")

    def execute(self, context):
        magento_hook = MagentoHook(self.magento_conn_id)  # Instantiate the MagentoHook
        result = None

        try:
            if self.method == 'GET':
                result = magento_hook.get_request(self.endpoint, search_criteria=self.search_criteria)
            elif self.method == 'POST':
                result = magento_hook.post_request(self.endpoint, data=self.data)
            elif self.method == 'PUT':
                result = magento_hook.put_request(self.endpoint, data=self.data)
            elif self.method == 'DELETE':
                result = magento_hook.delete_request(self.endpoint, data=self.data)

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

