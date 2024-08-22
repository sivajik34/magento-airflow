from airflow import DAG
from airflow.decorators import task
from datetime import datetime

from apache_airflow_provider_magento.operators.rest import MagentoRestOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

dag = DAG(
    'magento_customer_order_rest_demo',
    default_args=default_args,
    description='Magento Customer Order Creation DAG',
    schedule_interval=None,
    start_date=datetime(2024, 8, 22),
    catchup=False,
)

customer_email = 'customer@example.com'
customer_password = 'Airflow@123'

@task
def generate_customer_token(email: str, password: str, **kwargs) -> str:
    op = MagentoRestOperator(
        task_id='generate_customer_token_op',
        endpoint='integration/customer/token',
        method='POST',
        data={
            'username': email,
            'password': password
        }
    )
    response = op.execute(context=kwargs)
    return response  # Customer OAuth token

@task
def check_customer_exists(email: str, **kwargs) -> bool:
    op = MagentoRestOperator(
        task_id='check_customer_exists_op',
        endpoint=f'customers/search?searchCriteria[filterGroups][0][filters][0][field]=email&searchCriteria[filterGroups][0][filters][0][value]={email}',
        method='GET'
    )
    response = op.execute(context=kwargs)
    return len(response.get('items', [])) > 0

@task
def create_customer(email: str, password: str, **kwargs) -> str:
    op = MagentoRestOperator(
        task_id='create_customer_op',
        endpoint='customers',
        method='POST',
        data={
            "customer": {
                "email": email,
                "firstname": "John",
                "lastname": "Doe",
                "website_id": 1,
                "store_id": 1,
                "group_id": 1
            },
            "password": password
        }
    )
    response = op.execute(context=kwargs)
    return response.get('id')

@task
def create_cart(customer_token: str, **kwargs) -> str:
    op = MagentoRestOperator(
        task_id='create_cart_op',
        endpoint='carts/mine',
        method='POST',
        headers={'Authorization': f'Bearer {customer_token}'}
    )
    response = op.execute(context=kwargs)
    return response  # Returns quoteId

@task
def add_product_to_cart(customer_token: str, quote_id: str, sku: str, qty: int, **kwargs):
    op = MagentoRestOperator(
        task_id=f'add_{sku}_to_cart_op',
        endpoint=f'carts/mine/items',
        method='POST',
        headers={'Authorization': f'Bearer {customer_token}'},
        data={
            "cartItem": {
                "quote_id": quote_id,
                "sku": sku,
                "qty": qty
            }
        }
    )
    response = op.execute(context=kwargs)
    return response

@task
def set_billing_shipping_address(customer_token: str, quote_id: str, **kwargs):
    op = MagentoRestOperator(
        task_id='set_billing_shipping_address_op',
        endpoint=f'carts/mine/shipping-information',
        method='POST',
        headers={'Authorization': f'Bearer {customer_token}'},
        data={
            "addressInformation": {
                "shipping_address": {
                    "region": "NY",
                    "region_id": 43,
                    "country_id": "US",
                    "street": ["123 Main St"],
                    "telephone": "1234567890",
                    "postcode": "12345",
                    "city": "New York",
                    "firstname": "John",
                    "lastname": "Doe",
                    "email": customer_email
                },
                "billing_address": {
                    "region": "NY",
                    "region_id": 43,
                    "country_id": "US",
                    "street": ["123 Main St"],
                    "telephone": "1234567890",
                    "postcode": "12345",
                    "city": "New York",
                    "firstname": "John",
                    "lastname": "Doe",
                    "email": customer_email
                },
                "shippingMethodCode": "flatrate",
                "shippingCarrierCode": "flatrate"
            }
        }
    )
    op.execute(context=kwargs)

@task
def set_payment_method(customer_token: str, quote_id: str, **kwargs):
    op = MagentoRestOperator(
        task_id='set_payment_method_op',
        endpoint=f'carts/mine/payment-information',
        method='POST',
        headers={'Authorization': f'Bearer {customer_token}'},
        data={
            "paymentMethod": {
                "method": "checkmo"
            }
        }
    )
    order_id =op.execute(context=kwargs)
    return order_id

@task
def create_invoice(order_id: str, **kwargs):
    op = MagentoRestOperator(
        task_id='create_invoice_op',
        endpoint=f'order/{order_id}/invoice',
        method='POST',
        data={
            "capture": True,
            "notify": True,
            "appendComment": True,
            "comment": {
                "comment": "Invoice created",
                "is_visible_on_front": 0
            }
        }
    )
    op.execute(context=kwargs)

@task
def create_shipment(order_id: str, **kwargs):
    op = MagentoRestOperator(
        task_id='create_shipment_op',
        endpoint=f'order/{order_id}/ship',
        method='POST',
        data={
            "notify": True,
            "appendComment": True,
            "comment": {
                "comment": "Shipment created",
                "is_visible_on_front": 0
            },
            "tracks": [
                {
                    "track_number": "123456",
                    "title": "Carrier",
                    "carrier_code": "custom"
                }
            ]
        }
    )
    op.execute(context=kwargs)

with dag:
    # Define the product SKUs and prices
    sku_1 = 'product_sku_1'
    sku_2 = 'product_sku_2'
    price_1 = 100.00
    price_2 = 150.00

    # Task 1: Check if customer exists
    customer_exists = check_customer_exists(customer_email)

    # Task 2: Create customer if not exists
    create_customer_task = create_customer(email=customer_email, password=customer_password)

    # Task 3: Generate customer token
    generate_customer_token_task = generate_customer_token(email=customer_email, password=customer_password)

    # Task 4: Create cart
    create_cart_task = create_cart(customer_token=generate_customer_token_task)

    # Task 5: Add products to cart
    add_product_1_to_cart = add_product_to_cart(customer_token=generate_customer_token_task, quote_id=create_cart_task, sku=sku_1, qty=1)
    add_product_2_to_cart = add_product_to_cart(customer_token=generate_customer_token_task, quote_id=create_cart_task, sku=sku_2, qty=1)

    # Task 6: Set billing and shipping address
    set_billing_shipping_address_task = set_billing_shipping_address(customer_token=generate_customer_token_task, quote_id=create_cart_task)

    # Task 7: Set payment method and place order
    set_payment_method_task = set_payment_method(customer_token=generate_customer_token_task, quote_id=create_cart_task)   

    # Task 8: Create invoice
    create_invoice_task = create_invoice(order_id=set_payment_method_task)

    # Task 9: Create shipment
    create_shipment_task = create_shipment(order_id=set_payment_method_task)

    # Define task dependencies
    customer_exists >> create_customer_task >> generate_customer_token_task
    generate_customer_token_task >> create_cart_task >> [add_product_1_to_cart, add_product_2_to_cart]
    [add_product_1_to_cart, add_product_2_to_cart] >> set_billing_shipping_address_task >> set_payment_method_task 
    set_payment_method_task >> [create_invoice_task, create_shipment_task]

