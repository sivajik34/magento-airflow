from __future__ import annotations

from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from apache_airflow_provider_magento.operators.sales import GetOrdersOperator


def process_orders(**kwargs):
    ti = kwargs["ti"]
    orders = ti.xcom_pull(task_ids="get_orders_task", key="magento_orders")
    if orders:
        # Process orders
        for order in orders:
            print(order)
    else:
        print("No orders to process")


dag = DAG(
    dag_id="magento_get_orders",
    start_date=datetime(2024, 8, 18),
    schedule_interval="@daily",
)

get_orders_task = GetOrdersOperator(task_id="get_orders_task", dag=dag, status="pending")

process_orders_task = PythonOperator(
    task_id="process_orders_task",
    python_callable=process_orders,
    provide_context=True,
    dag=dag,
)

get_orders_task >> process_orders_task
