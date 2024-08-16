from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow_magento_provider.sensors.magento_order_sensor import MagentoOrderSensor
import csv

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'magento_order_monitoring_and_export',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
)

check_for_new_orders = MagentoOrderSensor(
    task_id='check_for_new_orders',
    magento_conn_id='magento',
    dag=dag,
    poke_interval=900  # Run every 15 minutes
)

def export_orders_to_csv(**context):
    orders = context['ti'].xcom_pull(key='new_orders', task_ids='check_for_new_orders')
    orders = orders.get('items', [])
    print(f"Orders received: {orders}")
    # Example path; you might want to make this configurable
    output_path = '/tmp/new_orders.csv'
    
    if orders:
        keys = orders[0].keys()  # Assuming all orders have the same keys
        with open(output_path, 'w', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(orders)
        print(f"Orders exported to {output_path}")
    else:
        print("No orders to export.")

export_orders = PythonOperator(
    task_id='export_orders_to_csv',
    python_callable=export_orders_to_csv,
    provide_context=True,
    dag=dag,
)

check_for_new_orders >> export_orders


