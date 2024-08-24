from __future__ import annotations

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from apache_airflow_provider_magento.operators.asyncrest import MagentoRestAsyncOperator  # Update with your module name

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'magento_async_operations',
    default_args=default_args,
    description='A DAG to perform asynchronous operations with Magento',
    schedule_interval=None,  # Set to None or a specific schedule if needed
    start_date=datetime(2024, 8, 21),  # Update to your desired start date
    catchup=False,
) as dag:

    # Define the start task
    start = DummyOperator(
        task_id='start'
    )

    # Define the asynchronous operation task
    async_operation = MagentoRestAsyncOperator(
        task_id='perform_async_operation',
        endpoint='/products/product1',
        method="PUT",  
        data={
            "product": {
                "price": 170
            }
        },        
        magento_conn_id='magento_default',
        timeout=300,
        interval=10,
        store_view_code='default'
    )

    # Define the end task
    end = DummyOperator(
        task_id='end'
    )

    # Set task dependencies
    start >> async_operation >> end

