from __future__ import annotations

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from apache_airflow_provider_magento.operators.asyncrest import MagentoRestAsyncOperator

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 25),
    'retries': 1,
}

with DAG(
    dag_id='magento_bulk_delete_products',
    default_args=default_args,
    description='DAG to delete multiple products in Magento using bulk operation',
    schedule_interval=None,
    catchup=False,
) as dag:

    start = DummyOperator(task_id='start')

    # Bulk delete products
    bulk_delete_products = MagentoRestAsyncOperator(
        task_id='bulk_delete_products',
        endpoint='products/bySku',  # Endpoint for deleting products in bulk
        method='DELETE',
        data=[
            {
                "sku": "product_sku_1"
            },
            {
                "sku": "SKUT001000"
            },
            {
                "sku": "product_sku_3"
            }
            # Add more SKUs as needed
        ],
        bulk=True,  # Enable bulk operation
        store_view_code="default",  # Store view code
        magento_conn_id="magento_default",  # Magento connection ID in Airflow
    )

    end = DummyOperator(task_id='end')

    start >> bulk_delete_products >> end

