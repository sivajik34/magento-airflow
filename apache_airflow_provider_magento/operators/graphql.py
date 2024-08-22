from __future__ import annotations
from airflow.models.baseoperator import BaseOperator
from apache_airflow_provider_magento.hooks.magento import MagentoHook
from airflow.exceptions import AirflowException

class MagentoGraphQLOperator(BaseOperator):
    """Operator for executing GraphQL API requests to Magento."""

    def __init__(self, query: str, variables: dict = None, magento_conn_id: str = "magento_default", headers: dict = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
        self.variables = variables or {}
        self.magento_conn_id = magento_conn_id
        self.headers = headers or {}

    def execute(self, context):
        magento_hook = MagentoHook(self.magento_conn_id)

        try:
            result = magento_hook.graphql_request(query=self.query, variables=self.variables, headers=self.headers)
            if 'errors' in result:
                self.log.error("GraphQL errors received: %s", result['errors'])
                raise AirflowException(f"GraphQL errors: {result['errors']}")
            self.log.info("Response received from Magento GraphQL API: %s", result)
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

