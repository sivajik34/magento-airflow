from __future__ import annotations
from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from apache_airflow_provider_magento.operators.rest import MagentoRestOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def generate_product_name(sku: str) -> str:
    return f"Product Name - {sku}"

@task
def create_product(sku: str, price: float, **kwargs) -> str:
    product_name = generate_product_name(sku)
    product_data = {
        "product": {
            "sku": sku,
            "name": product_name,
            "price": price,
            "status": 1,
            "type_id": "simple",
            "attribute_set_id": 4,
            "weight": 1,
            "extension_attributes": {
                "stock_item": {
                    "qty": 450,
                    "is_in_stock": True
                }
            }
        }
    }
    op = MagentoRestOperator(
        task_id=f'create_product_{sku}_op',
        endpoint='products',
        method='POST',
        data=product_data,
    )
    response = op.execute(context=kwargs)
    product_id = response['sku']
    return product_id

@task
def check_product_exists(sku: str, **kwargs) -> bool:
    op = MagentoRestOperator(
        task_id=f'check_product_exists_{sku}_op',
        endpoint=f'products/{sku}',
        method='GET',
        data={},
    )
    try:
        response = op.execute(context=kwargs)
        return True  # Product exists
    except Exception as e:
        if '404' in str(e):
            return False
        else:
            raise

@task
def create_guest_cart(**kwargs) -> str:
    op = MagentoRestOperator(
        task_id='create_guest_cart_op',
        endpoint='guest-carts',
        method='POST',
        data={},
    )
    response = op.execute(context=kwargs)
    quote_id = response
    return quote_id

@task
def add_item_to_cart(product_id: str, quote_id: str, **kwargs) -> dict:
    cart_item = {
        "cartItem": {
            "sku": product_id,
            "qty": 1,
            "name": generate_product_name(product_id),
            "price": 100.00,
            "product_type": "simple",
            "quote_id": quote_id
        }
    }
    op = MagentoRestOperator(
        task_id=f'add_item_{product_id}_to_cart_op',
        endpoint=f'guest-carts/{quote_id}/items',
        method='POST',
        data=cart_item,
    )
    response = op.execute(context=kwargs)
    return {
        "quote_id": quote_id,
        "item_id": response.get('item_id')
    }

@task
def set_billing_address(quote_id: str, **kwargs) -> str:
    op = MagentoRestOperator(
        task_id='set_billing_address_op',
        endpoint=f'guest-carts/{quote_id}/billing-address',
        method='POST',
        data={
            "address": {
                "region_code": "CA",
                "region": "California",
                "region_id": 12,
                "country_id": "US",
                "street": ["123 Main Street"],
                "postcode": "90210",
                "city": "Beverly Hills",
                "telephone": "1234567890",
                "firstname": "John",
                "lastname": "Doe",
                "middlename": "",
                "prefix": "",
                "suffix": "",
                "vat_id": "",
                "company": "",
                "fax": ""
            }
        },
    )
    response = op.execute(context=kwargs)
    return quote_id

@task
def set_shipping_address(quote_id: str, **kwargs) -> str:
    op = MagentoRestOperator(
        task_id='set_shipping_address_op',
        endpoint=f'guest-carts/{quote_id}/shipping-information',
        method='POST',
        data={
            "addressInformation": {
                "shipping_address": {
                    "region_code": "CA",
                    "region": "California",
                    "region_id": 12,
                    "country_id": "US",
                    "street": ["123 Main Street"],
                    "postcode": "90210",
                    "city": "Beverly Hills",
                    "telephone": "1234567890",
                    "firstname": "John",
                    "lastname": "Doe",
                    "middlename": "",
                    "prefix": "",
                    "suffix": "",
                    "vat_id": "",
                    "company": "",
                    "fax": ""
                },
                "shippingCarrierCode": "flatrate",
                "shippingMethodCode": "flatrate"
            }
        },
    )
    response = op.execute(context=kwargs)
    return quote_id

@task
def set_payment_method_and_place_order(quote_id: str, **kwargs) -> str:
    op = MagentoRestOperator(
        task_id='place_order_op',
        endpoint=f'guest-carts/{quote_id}/payment-information',
        method='POST',
        data={
            "email": "guest@example.com",
            "paymentMethod": {"method": "checkmo"}
        },
    )
    order_id = op.execute(context=kwargs)
    return order_id

@task
def get_order_info(order_id: str, **kwargs) -> dict:
    op = MagentoRestOperator(
        task_id='get_order_info_op',
        endpoint=f'orders/{order_id}',
        method='GET',
        data={},
    )
    response = op.execute(context=kwargs)
    return response

@task
def create_invoice(order_id: str, order_items: list, **kwargs) -> str:
    items = [{"order_item_id": item["item_id"], "qty": item["qty_ordered"]} for item in order_items]
    op = MagentoRestOperator(
        task_id='create_invoice_op',
        endpoint=f'order/{order_id}/invoice',
        method='POST',
        data={
            "capture": True,
            "items": items,
            "notify": True,
            "appendComment": True,
            "comment": {
                "comment": "Invoice created",
                "is_visible_on_front": 0
            },
            "arguments": {}
        },
    )
    response = op.execute(context=kwargs)
    invoice_id = response
    return invoice_id

@task
def create_shipment(order_id: str, order_items: list, **kwargs) -> str:
    items = [{"order_item_id": item["item_id"], "qty": item["qty_ordered"]} for item in order_items]
    op = MagentoRestOperator(
        task_id='create_shipment_op',
        endpoint=f'order/{order_id}/ship',
        method='POST',
        data={
            "items": items,
            "notify": True,
            "appendComment": True,
            "comment": {
                "comment": "Shipment created",
                "is_visible_on_front": 0
            },
            "tracks": [
                {
                    "track_number": "12345",
                    "title": "Tracking",
                    "carrier_code": "carrier"
                }
            ],
            "packages": [{}]
            
        },
    )
    response = op.execute(context=kwargs)
    shipment_id = response
    return shipment_id

with DAG(
    'magento_guest_order_rest_demo',
    default_args=default_args,
    description='A DAG to perform various operations on Magento',
    schedule_interval=None,  # Run only once manually
    start_date=datetime(2024, 8, 19),
    catchup=False,
) as dag:

    # Define product SKUs and prices
    sku_1 = 'product_sku_1'
    sku_2 = 'product_sku_2'
    price_1 = 100.00
    price_2 = 150.00

    # Define tasks
    product_exists_1 = check_product_exists(sku_1)
    product_exists_2 = check_product_exists(sku_2)
    create_product_1 = create_product(sku_1, price_1)
    create_product_2 = create_product(sku_2, price_2)
    quote_id = create_guest_cart()
    item_1 = add_item_to_cart(product_id=sku_1, quote_id=quote_id)
    item_2 = add_item_to_cart(product_id=sku_2, quote_id=quote_id)
    quote_with_billing = set_billing_address(quote_id)
    quote_with_shipping = set_shipping_address(quote_with_billing)
    order_id = set_payment_method_and_place_order(quote_with_shipping)
    order_info = get_order_info(order_id)
    invoice_id = create_invoice(order_id, order_info['items'])
    shipment_id = create_shipment(order_id, order_info['items'])

    # Set task dependencies
    product_exists_1 >> create_product_1
    product_exists_2 >> create_product_2
    create_product_1 >> quote_id
    create_product_2 >> quote_id
    quote_id >> item_1 >> item_2 >> quote_with_billing >> quote_with_shipping >> order_id >> order_info >> invoice_id >> shipment_id

