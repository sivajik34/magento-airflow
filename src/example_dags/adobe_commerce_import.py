from __future__ import annotations

import os
import csv
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from apache_airflow_provider_magento.operators.dataimport import MagentoImportOperator

# CSV file path
CSV_FILE_PATH = "/home/sivakumar/magento-airflow/src/example_dags/data/sample_skus.csv"

# Define the generate_sample_csv function
def generate_sample_csv(file_path: str, num_skus: int = 100000):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, 'w', newline='') as csvfile:
        fieldnames = ['sku', 'name', 'price', 'qty', 'product_type', 'attribute_set_code']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for i in range(1, num_skus + 1):
            writer.writerow({
                'sku': f'SKU{i:06d}',
                'name': f'Product {i:06d}',
                'price': round(10 + i * 0.01, 2),
                'qty': i % 100 + 1,
                'product_type': 'simple',  # Assuming 'simple' as a default product type
                'attribute_set_code': 'Default'  # Assuming 'default' as the attribute set code
            })
    
    print(f"CSV file generated with {num_skus} SKUs at {file_path}")

@dag(
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 8, 23),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple DAG to perform Magento import operations',
    schedule_interval=None,
    catchup=False,
    tags=['magento'],
)
def magento_import_dag():

    @task
    def start_task():
        print("Starting the Magento import DAG")

    @task
    def generate_csv():
        # Generate the CSV file with 100,000 SKUs
        generate_sample_csv(CSV_FILE_PATH)
        print(f"Generated CSV file at {CSV_FILE_PATH}")

    @task
    def import_csv():
        # Create a MagentoImportOperator instance with the CSV file path
        import_csv_operator = MagentoImportOperator(
            task_id='import_csv_task',
            endpoint='import/csv',
            store_view_code='default',
            csv_file_path=CSV_FILE_PATH,
            chunk_size=10000,  # Define chunk size for better handling
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
        import_csv_operator.execute(context={})
        print("CSV import completed.")

    @task
    def end_task():
        print("Ending the Magento import DAG")

    start = start_task()
    generate_csv_task = generate_csv()
    csv_import = import_csv()
    end = end_task()

    start >> generate_csv_task >> csv_import >> end

# Instantiate the DAG
dag_instance = magento_import_dag()

