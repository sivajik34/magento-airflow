from airflow.providers.http.operators.http import HttpOperator
from apache_airflow_provider_magento.hooks.magento import MagentoHook
from typing import Any, Dict, Optional, Callable
from urllib.parse import urlencode

class MagentoRestOperator(HttpOperator):
    """
    Operator to interact with Magento's REST API using a custom MagentoHook for OAuth1 authentication.
    Supports pagination if a pagination function is provided.
    """

    template_fields = ('endpoint', 'data', 'headers', 'search_criteria')
    ui_color = '#f4a261'
    REST_ENDPOINT_TEMPLATE = "/rest/{store_view_code}/{api_version}"

    def __init__(
        self,
        endpoint: str,
        method: str = 'GET',
        data: Optional[Dict[str, Any]] = None,
        search_criteria: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        magento_conn_id: str = "magento_default",
        store_view_code: str = "default",
        api_version: str = 'V1',
        response_check: Optional[Any] = None,
        extra_options: Optional[Dict[str, Any]] = None,
        log_response: bool = False,
        pagination_function: Optional[Callable] = None,  # Function to handle pagination
        **kwargs,
    ):
        """
        Initializes the MagentoRestOperator.
        """
        self.magento_conn_id = magento_conn_id
        self.store_view_code = store_view_code
        self.api_version = api_version.upper()
        self.data = data
        self.search_criteria = search_criteria or {}
        self.log_response = log_response        
        self.method=method.upper()

        formatted_endpoint = self._build_endpoint(endpoint,pagination_function)

        if headers is None:
            headers = {}
        headers.setdefault('Content-Type', 'application/json')

        super().__init__(
            http_conn_id=magento_conn_id,
            endpoint=formatted_endpoint,
            method=method.upper(),
            data=data,  # Data is used for POST, PUT, DELETE requests
            headers=headers,
            response_check=response_check,
            extra_options=extra_options or {"verify": False},            
            **kwargs,
        )
        self.pagination_function = pagination_function

    def _build_endpoint(self, endpoint: str, pagination_function=None) -> str:
        """
        Constructs the full endpoint URL with store view, API version, and search criteria.
        """
        base_endpoint = f"{self.REST_ENDPOINT_TEMPLATE.format(store_view_code=self.store_view_code, api_version=self.api_version)}/{endpoint.lstrip('/')}"

        if self.search_criteria and self.method == 'GET' and not pagination_function:
            query_string = urlencode(self.search_criteria, doseq=True)
            return f"{base_endpoint}?{query_string}"

        return base_endpoint

    def get_hook(self) -> MagentoHook:
        """
        Returns an instance of MagentoHook configured with OAuth1 authentication.
        """
        return MagentoHook(
            magento_conn_id=self.magento_conn_id,
            method=self.method,
        )

    def execute(self, context):
        """
        Executes the HTTP request using MagentoHook and processes the response.
        """
        try:
            hook = self.get_hook()
            # Debug log to check the values
            self.log.info("Method: %s", self.method)
            self.log.info("Pagination Function: %s", self.pagination_function)
            if self.pagination_function and self.method == 'GET':
                self.log.info("calling pagination")
                all_results = self.pagination_function(
                    hook=hook,
                    endpoint=self.endpoint,                    
                    headers=self.headers,
                    extra_options=self.extra_options,
                    search_criteria=self.search_criteria
                )
                if self.log_response:
                    self.log.info("Aggregated Response Body: %s", json.dumps(all_results, indent=2))
                return all_results
            self.log.info("not calling pagination")
            response = hook.send_request(
                endpoint=self.endpoint,
                data=self.data,
                headers=self.headers,
                extra_options=self.extra_options
            )

            if self.log_response:
                self.log.info("Response Body: %s", response)

            return response

        except AirflowException as ae:
            self.log.error("AirflowException occurred: %s", ae)
            raise
        except Exception as e:
            self.log.error("An unexpected error occurred: %s", e, exc_info=True)
            raise

