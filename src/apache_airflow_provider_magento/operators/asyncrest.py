from __future__ import annotations

from airflow.models.baseoperator import BaseOperator
from apache_airflow_provider_magento.hooks.magento import MagentoHook
from airflow.exceptions import AirflowException
import logging

class MagentoRestAsyncOperator(BaseOperator):

    SUPPORTED_ASYNC_METHODS = {'POST', 'PUT', 'DELETE', 'PATCH'}

    def __init__(self,
                 endpoint: str,
                 method: str = 'POST',
                 data: dict = None,
                 headers: dict = None,
                 bulk: bool = False,  
                 timeout: int = 300,
                 interval: int = 10,
                 store_view_code: str = "default",
                 magento_conn_id: str = "magento_default",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.method = method.upper()  
        self.data = data or {}
        self.headers = headers or {}
        self.bulk = bulk  
        self.magento_conn_id = magento_conn_id
        self.timeout = timeout
        self.interval = interval
        self.store_view_code = store_view_code

        self._validate_parameters()

    def _validate_parameters(self):
        # Validate that the method is supported for asynchronous operations
        if self.method not in self.SUPPORTED_ASYNC_METHODS:
            raise AirflowException(f"Unsupported HTTP method '{self.method}' for asynchronous requests. "
                                   f"Supported methods are: {', '.join(self.SUPPORTED_ASYNC_METHODS)}.")

        # Bulk requests should not use the GET method
        if self.bulk and self.method == 'GET':
            raise AirflowException("Bulk GET requests are not supported.")

    def execute(self, context):
        magento_hook = MagentoHook(self.magento_conn_id, store_view_code=self.store_view_code)

        # Perform the asynchronous request
        try:
            response = magento_hook.async_post_request(self.endpoint, self.method, data=self.data, headers=self.headers, bulk=self.bulk)
            bulk_uuid = response.get("bulk_uuid")

            if not bulk_uuid:
                raise AirflowException("No bulk_uuid found in the response.")

            self.log.info(f"Bulk UUID: {bulk_uuid}")

            # Wait for the asynchronous request to complete
            result = magento_hook.wait_for_bulk_completion(bulk_uuid, timeout=self.timeout, interval=self.interval)
            self.log.info(f"Bulk operation result: {result}")

            return result
        except Exception as e:
            self.log.error(f"Failed to perform asynchronous operation: {e}")
            raise

