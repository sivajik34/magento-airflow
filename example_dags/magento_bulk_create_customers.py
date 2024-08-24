from __future__ import annotations
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from apache_airflow_provider_magento.operators.asyncrest import MagentoRestAsyncOperator

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 21),
    'retries': 1,
}

with DAG(
    dag_id='magento_bulk_create_customers',
    default_args=default_args,
    description='DAG to create multiple customers in Magento using bulk operation',
    schedule_interval=None,
    catchup=False,
) as dag:

    start = DummyOperator(task_id='start')

    bulk_create_customers = MagentoRestAsyncOperator(
        task_id='bulk_create_customers',
        endpoint='customers',  # Endpoint for creating customers in bulk
        method='POST',
        data=[
            {
                "customer": {
                    "email": "mshaw@example.com",
                    "firstname": "Melanie",
                    "lastname": "Shaw"
                },
                "password": "Strong-Password"
            },
            {
                "customer": {
                    "email": "bmartin@example.com",
                    "firstname": "Bryce",
                    "lastname": "Martin"
                },
                "password": "Strong-Password"
            },
            {
                "customer": {
                    "email": "tgomez@example.com",
                    "firstname": "Teresa",
                    "lastname": "Gomez"
                },
                "password": "Strong-Password"
            }
        ],
        bulk=True,  # Enable bulk operation
        store_view_code="default",  # Store view code
        magento_conn_id="magento_default",  # Magento connection ID in Airflow
    )

    end = DummyOperator(task_id='end')

    start >> bulk_create_customers >> end

