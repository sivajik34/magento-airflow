from __future__ import annotations

from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from apache_airflow_provider_magento.operators.customer import CreateCustomerOperator

def process_customer_creation(**kwargs):
    ti = kwargs["ti"]
    customer_response = ti.xcom_pull(task_ids="create_customer_task", key="magento_customer")

    if customer_response:
        # Process the response
        print("Customer created successfully:", customer_response)
    else:
        print("Customer creation failed or no response received.")

dag = DAG(
    dag_id="magento_create_customer_dag",
    start_date=datetime(2024, 8, 19),
    schedule_interval="@daily",
)

customer_data = {
  "customer": {
    "email": "jdoe34@example.com",
    "firstname": "Jane",
    "lastname": "Doe",
    "addresses": [
      {
        "defaultShipping": True,
        "defaultBilling": True,
        "firstname": "Jane",
        "lastname": "Doe",
        "region": {
          "regionCode": "NY",
          "region": "New York",
          "regionId": 43
        },
        "postcode": "10755",
        "street": [
          "123 Oak Ave"
        ],
        "city": "Purchase",
        "telephone": "512-555-1111",
        "countryId": "US"
      }
    ]
  },
  "password": "Password1"
}


create_customer_task = CreateCustomerOperator(
    task_id="create_customer_task",
    magento_conn_id="magento_default",
    customer_data=customer_data,
    dag=dag,
)

process_customer_creation_task = PythonOperator(
    task_id="process_customer_creation_task",
    python_callable=process_customer_creation,
    provide_context=True,
    dag=dag,
)

create_customer_task >> process_customer_creation_task

