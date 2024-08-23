from __future__ import annotations

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from apache_airflow_provider_magento.operators.dataimport import MagentoImportOperator
from airflow.exceptions import AirflowException

@dag(
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 8, 23),  # Use a fixed start date
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple DAG to perform Magento import operations',
    schedule_interval=None,  # Set to a cron expression or timedelta for scheduling
    catchup=False,
    tags=['magento'],
)
def magento_import_dag():

    @task
    def start_task():
        print("Starting the Magento import DAG")

    @task
    def import_csv():
        # Define the import data (example CSV data as a string with necessary fields)
        csv_data = "sku,name,price,product_type,attribute_set_code\nproduct3,Product 3,10.00,simple,Default\nproduct4,Product 4,20.00,simple,Default"

        # Create a MagentoImportOperator instance with required parameters
        import_csv_operator = MagentoImportOperator(
            task_id='import_csv_task',
            endpoint='import/csv',  # Specify the endpoint
            store_view_code='default',  # Store view code
            data=csv_data,
            data_format='csv',
            entity='catalog_product',
            behavior='append',
            validation_strategy='validation-stop-on-errors',
            allowed_error_count='10',
            import_field_separator=',',
            import_multiple_value_separator=',',
            import_empty_attribute_value_constant='',
            import_images_file_dir='',
            magento_conn_id='magento_default'
        )

        response_data = import_csv_operator.execute(context={})            

    @task
    def import_json():
        # Define the import data (example JSON data with necessary fields)
        json_data = [
            {"sku": "product5", "name": "Product 5", "price": 30.00, "product_type": "simple","attribute_set_code": "Default"},
            {"sku": "product6", "name": "Product 6", "price": 40.00, "product_type": "simple","attribute_set_code": "Default"}
        ]

        # Create a MagentoImportOperator instance with required parameters
        import_json_operator = MagentoImportOperator(
            task_id='import_json_task',
            endpoint='import/json',  # Specify the endpoint
            store_view_code='default',  # Store view code
            data=json_data,
            data_format='json',
            entity='catalog_product',
            behavior='append',
            validation_strategy='validation-stop-on-errors',
            allowed_error_count='10',
            magento_conn_id='magento_default'
        )        
        response_data = import_json_operator.execute(context={})            
        

    @task
    def end_task():
        print("Ending the Magento import DAG")

    # Set task dependencies
    start = start_task()
    csv_import = import_csv()
    json_import = import_json()
    end = end_task()

    start >> [csv_import, json_import] >> end

# Instantiate the DAG
dag_instance = magento_import_dag()

