from airflow.providers.http.operators.http import HttpOperator
from apache_airflow_provider_magento.hooks.magento import MagentoHook
from urllib.parse import urlencode
from airflow.exceptions import AirflowException
from typing import Any, Dict, Optional
import json


class MagentoRestOperator(HttpOperator):
    """
    Operator to interact with Magento's REST API using a custom MagentoHook for OAuth1 authentication.
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
        **kwargs,
    ):
        """
        Initializes the MagentoRestOperator.

        :param endpoint: The API endpoint to hit.
        :param method: HTTP method to use (GET, POST, PUT, DELETE).
        :param data: Data to send in the request body.
        :param search_criteria: Query parameters for GET requests.
        :param headers: HTTP headers to include in the request.
        :param magento_conn_id: Airflow connection ID for Magento.
        :param store_view_code: Magento store view code.
        :param api_version: Magento API version.
        :param response_check: A callable to check the response.
        :param extra_options: Additional options for the request.
        :param log_response: Whether to log the response content.
        :param kwargs: Additional keyword arguments.
        """
        self.magento_conn_id = magento_conn_id
        self.store_view_code = store_view_code
        self.api_version = api_version.upper()
        self.search_criteria = search_criteria or {}
        self.log_response = log_response

        # Build the full endpoint URL
        formatted_endpoint = self._build_endpoint(endpoint)

        # Set default headers if not provided
        if headers is None:
            headers = {}
        headers.setdefault('Content-Type', 'application/json')

        super().__init__(
            http_conn_id=magento_conn_id,
            endpoint=formatted_endpoint,
            method=method.upper(),
            data=data,  # No need to serialize, the hook will handle it
            headers=headers,
            response_check=response_check,
            extra_options=extra_options or {"verify": False},
            **kwargs,
        )

    def _build_endpoint(self, endpoint: str) -> str:
        """
        Constructs the full endpoint URL with store view, API version, and search criteria.

        :param endpoint: The API endpoint to hit.
        :return: Formatted endpoint URL.
        """
        base_endpoint = f"{self.REST_ENDPOINT_TEMPLATE.format(store_view_code=self.store_view_code, api_version=self.api_version)}/{endpoint.lstrip('/')}"
        if self.search_criteria:
            query_string = urlencode(self.search_criteria, doseq=True)
            return f"{base_endpoint}?{query_string}"
        return base_endpoint

    def get_hook(self) -> MagentoHook:
        """
        Returns an instance of MagentoHook configured with OAuth1 authentication.

        :return: MagentoHook instance.
        """
        return MagentoHook(
            magento_conn_id=self.magento_conn_id,
            method=self.method,
        )

    def execute(self, context):
        """
        Executes the HTTP request using MagentoHook and processes the response.

        :param context: Airflow context.
        :return: Parsed JSON response.
        """
        try:
            hook = self.get_hook()
            response = hook.send_request(endpoint=self.endpoint,data=self.data,headers=self.headers,        extra_options=self.extra_options
    )

            if self.log_response:
                self.log.info("Response Body: %s", response.text) #TODO need to check         

        except AirflowException as ae:
            self.log.error("AirflowException occurred: %s", ae)
            raise
        except Exception as e:
            self.log.error("An unexpected error occurred: %s", e, exc_info=True)
            raise

