from __future__ import annotations

from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
import logging

class MagentoRestAsyncOperator(BaseOperator):

    def __init__(self,
                 endpoint: str,
                 data: dict = None,
                 headers: dict = None,
                 magento_conn_id: str = "magento_default",
                 timeout: int = 300,
                 interval: int = 10,
                 store_view_code: str = "default"
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.data = data or {}
        self.headers = headers or {}
        self.magento_conn_id = magento_conn_id
        self.timeout = timeout
        self.interval = interval
        self.store_view_code = store_view_code

    def execute(self, context):
        magento_hook = MagentoHook(self.magento_conn_id, store_view_code=self.store_view_code)

        # Perform the asynchronous request
        try:
            response = magento_hook.async_post_request(self.endpoint, data=self.data, headers=self.headers)
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

