from __future__ import annotations

from airflow.models.baseoperator import BaseOperator
from apache_airflow_provider_magento.hooks.magento import MagentoHook
from airflow.exceptions import AirflowException
import time

class MagentoRestAsyncOperator(BaseOperator):

    ASYNC_ENDPOINT_TEMPLATE = "/rest/{store_view_code}/async/{api_version}"
    BULK_ENDPOINT_TEMPLATE = "/rest/{store_view_code}/async/bulk/{api_version}"
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
                 api_version='V1',
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
        self.api_version = api_version.upper()

        self._validate_parameters()

    def _validate_parameters(self):
        # Validate that the method is supported for asynchronous operations
        if self.method not in self.SUPPORTED_ASYNC_METHODS:
            raise AirflowException(f"Unsupported HTTP method '{self.method}' for asynchronous requests. "
                                   f"Supported methods are: {', '.join(self.SUPPORTED_ASYNC_METHODS)}.")

    def execute(self, context):
        magento_hook = MagentoHook(self.magento_conn_id, self.method)

        # Perform the asynchronous request
        try:
            url_template = self.BULK_ENDPOINT_TEMPLATE if self.bulk else self.ASYNC_ENDPOINT_TEMPLATE
            endpoint=f"{url_template.format(store_view_code=self.store_view_code, api_version=self.api_version)}/{self.endpoint.lstrip('/')}"
            response = magento_hook.send_request(endpoint, data=self.data, headers=self.headers)
            bulk_uuid = response.get("bulk_uuid")

            if not bulk_uuid:
                raise AirflowException("No bulk_uuid found in the response.")

            self.log.info(f"Bulk UUID: {bulk_uuid}")

            # Wait for the asynchronous request to complete
            result = self.wait_for_bulk_completion(bulk_uuid, timeout=self.timeout, interval=self.interval)
            self.log.info(f"Bulk operation result: {result}")

            return result

        except Exception as e:
            self.log.error(f"Failed to perform asynchronous operation: {e}")
            raise          

    def get_bulk_status(self, bulk_uuid):
        """Retrieve the status of an asynchronous request using the bulk UUID."""
        magento_hook = MagentoHook(self.magento_conn_id,"GET")
        endpoint = f"/rest/V1/bulk/{bulk_uuid}/detailed-status" #TODO temp fix
        response = magento_hook.send_request(endpoint)
        #self.log.info(response)
        return {
            "bulk_id": response.get("bulk_id"),
            "user_type": response.get("user_type"),
            "description": response.get("description"),
            "start_time": response.get("start_time"),
            "user_id": response.get("user_id"),
            "operation_count": response.get("operation_count"),
            "operations": [
                {
                    "id": operation.get("id"),
                    "status": operation.get("status"),
                    "result_message": operation.get("result_message"),
                    "serialized_data": operation.get("serialized_data"),
                    "error_code": operation.get("error_code")
                }
                for operation in response.get("operations_list", [])
            ]
        }

    def wait_for_bulk_completion(self, bulk_uuid, timeout=300, interval=10):
        """Wait for the asynchronous bulk operation to complete."""
        start_time = time.time()

        while True:
            status_response = self.get_bulk_status(bulk_uuid)
            operations = status_response["operations"]

            # Check if there are any operations with status 4 (open)
            open_operations = any(op["status"] == 4 for op in operations)

            if not open_operations:
                # If no operations are open, return the response
                return status_response

            if time.time() - start_time > timeout:
                raise AirflowException(f"Bulk operation with UUID {bulk_uuid} timed out.")

            time.sleep(interval)

