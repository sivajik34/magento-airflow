from airflow import DAG
from apache_airflow_provider_magento.operators.rest import MagentoRestOperator
from apache_airflow_provider_magento.utils.paginate import paginate

from datetime import datetime, timedelta

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='magento_products_dag',
    default_args=default_args,
    description='A DAG to fetch paginated products from Magento',
    schedule_interval='@daily',  # Adjust the schedule as needed
    start_date=datetime(2024, 8, 23),
    catchup=False,
) as dag:

    def paginate_products(hook, endpoint, headers=None, extra_options=None, search_criteria=None):
        """
        Custom pagination function for fetching products from Magento.
        """
        return paginate(
            hook=hook,
            endpoint=endpoint,
            headers=headers,
            extra_options=extra_options,
            page_size=100,  # Adjust page size as needed
            search_criteria=search_criteria,
            search_criteria_key='searchCriteria'
        )

    fetch_products = MagentoRestOperator(
        task_id='fetch_paginated_products',
        endpoint='products',  # API endpoint for products
        method='GET',
        search_criteria= {
    "searchCriteria[filterGroups][0][filters][0][field]": "status",
    "searchCriteria[filterGroups][0][filters][0][value]": "1",  # '1' typically represents enabled products
    "searchCriteria[filterGroups][0][filters][0][conditionType]": "eq"  # Equality condition
}, # Initial search criteria and it is optional
        magento_conn_id='magento_default', #optional
        store_view_code='default', #optional
        api_version='V1', #optional
        response_check=lambda response: 'items' in response,  # Example response check and it is optional
        pagination_function=paginate_products,  # Pass the custom pagination function
        log_response=True, #optional and default value is False
    )

    fetch_products

