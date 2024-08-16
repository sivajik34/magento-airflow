from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_magento_provider.operators.product_sync_operator import ProductSyncOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'magento_product_sync',
    default_args=default_args,
    description='A simple DAG to sync products from Magento',
    schedule_interval='@daily',
)

sync_products = ProductSyncOperator(
    task_id='sync_products',
    dag=dag,
)

