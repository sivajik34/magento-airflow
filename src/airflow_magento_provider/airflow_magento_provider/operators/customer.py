from __future__ import annotations
from airflow.models import BaseOperator
from airflow_magento_provider.hooks.magento import MagentoHook

class CreateCustomerOperator(BaseOperator):
    """Create a customer in Magento."""

    def __init__(
        self,
        magento_conn_id="magento_default",
        customer_data=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.magento_conn_id = magento_conn_id
        self.customer_data = customer_data or {}

    def execute(self, context):
        magento_hook = MagentoHook(magento_conn_id=self.magento_conn_id)
        endpoint = "customers"

        if not self.customer_data:
            raise ValueError("No customer data provided")

        # Create customer
        response = magento_hook.post_request(endpoint, data=self.customer_data)

        # Log the response for debugging
        self.log.info(f"Response from Magento: {response}")

        # Check if the response contains the 'id' key
        if "id" in response:
            customer_id = response.get("id")
            self.log.info(f"Customer created successfully with ID: {customer_id}")
            context["ti"].xcom_push(key="magento_customer_id", value=customer_id)
        else:
            error_message = response.get('message', 'Unknown error')
            self.log.error(f"Failed to create customer: {error_message}")
            raise Exception(f"Failed to create customer: {error_message}")

