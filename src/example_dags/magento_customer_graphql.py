from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from apache_airflow_provider_magento.operators.graphql import MagentoGraphQLOperator
from airflow.exceptions import AirflowException
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'magento_graphql_customer',
    default_args=default_args,
    description='A DAG to handle Magento GraphQL operations using TaskFlow API',
    schedule_interval=None,
    start_date=datetime(2024, 8, 22),
    catchup=False,
) as dag:

    @task
    def create_customer() -> dict:
        try:
            operator = MagentoGraphQLOperator(
                task_id='create_customer',
                query="""
                mutation {
                  createCustomer(
                    input: {
                      firstname: "John"
                      lastname: "Doe"
                      email: "john.doe@example.com"
                      password: "Airflow@123"
                    }
                  ) {
                    customer {
                      id
                      firstname
                      lastname
                      email
                    }
                  }
                }
                """,
                magento_conn_id='magento_default',
            )
            result = operator.execute(context={})
            customer_email = result['data']['createCustomer']['customer']['email']
            logging.info(f"Customer email created: {customer_email}")
            return {'customer_email': customer_email}  # Return as a dictionary
        except AirflowException as e:
            logging.error(f"Error in create_customer task: {e}")
            raise

    @task
    def generate_customer_token(customer_email: str) -> str:
        logging.info(f"Customer email received: {customer_email}")
        try:
            operator = MagentoGraphQLOperator(
                task_id='generate_customer_token',
                query=f"""
                mutation {{
                  generateCustomerToken(
                    email: "{customer_email}"
                    password: "Airflow@123"
                  ) {{
                    token
                  }}
                }}
                """,
                magento_conn_id='magento_default',
            )
            result = operator.execute(context={})
            if 'errors' in result:
                raise AirflowException(f"GraphQL errors: {result['errors']}")
            customer_token = result['data']['generateCustomerToken']['token']
            logging.info(f"Customer token generated: {customer_token}")
            return customer_token
        except AirflowException as e:
            logging.error(f"Error in generate_customer_token task: {e}")
            raise

    @task
    def get_customer_info(customer_token: str) -> dict:
        logging.info(f"Customer token received: {customer_token}")
        try:
            operator = MagentoGraphQLOperator(
                task_id='get_customer_info',
                query="""
                query {
                  customer {
                    firstname
                    lastname
                    suffix
                    email
                    addresses {
                      firstname
                      lastname
                      street
                      city
                      region {
                        region_code
                        region
                      }
                      postcode
                      country_code
                      telephone
                    }
                  }
                }
                """,
                magento_conn_id='magento_default',
                headers={
                    'Authorization': f'Bearer {customer_token}'
                }
            )
            result = operator.execute(context={})
            if 'errors' in result:
                raise AirflowException(f"GraphQL errors: {result['errors']}")
            return result
        except AirflowException as e:
            logging.error(f"Error in get_customer_info task: {e}")
            raise

    # Define task dependencies
    customer_creation_result = create_customer()
    customer_email = customer_creation_result['customer_email']  # Extract email from the dictionary
    customer_token = generate_customer_token(customer_email)
    get_customer_info(customer_token)

