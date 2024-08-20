from __future__ import annotations

from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from apache_airflow_provider_magento.operators.magento import MagentoApiOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

@task
def create_product(**kwargs) -> str:
    op = MagentoApiOperator(
        task_id='create_product_op',
        endpoint='products',
        method='POST',
        data={
            "product": {
                "sku": "product_sku",
                "name": "Product Name",
                "price": 100.00,
                "status": 1,
                "type_id": "simple",
                "attribute_set_id": 4,
                "weight": 1,
                "extension_attributes": {
                    "stock_item": {
                        "qty": 100,
                        "is_in_stock": True
                    }
                }
            }
        },
        dag=kwargs['dag'],
    )
    response = op.execute(context=kwargs)
    product_id = response['sku']
    return product_id

@task
def create_guest_cart(**kwargs) -> str:
    op = MagentoApiOperator(
        task_id='create_guest_cart_op',
        endpoint='guest-carts',
        method='POST',
        data={},
        dag=kwargs['dag'],
    )
    response = op.execute(context=kwargs)
    quote_id = response
    return quote_id

@task
def add_items_to_cart(product_id: str, quote_id: str, **kwargs) -> str:
    op = MagentoApiOperator(
        task_id='add_items_to_cart_op',
        endpoint=f'guest-carts/{quote_id}/items',
        method='POST',
        data={
            "cartItem": {
                "quote_id": quote_id,
                "sku": product_id,
                "qty": 1
            }
        },
        dag=kwargs['dag'],
    )
    response = op.execute(context=kwargs)
    return quote_id

@task
def set_billing_address(quote_id: str, **kwargs) -> str:
    op = MagentoApiOperator(
        task_id='set_billing_address_op',
        endpoint=f'guest-carts/{quote_id}/billing-address',
        method='POST',
        data={
            "address": {               
                "region_code": "CA",
                "region": "California",
                "region_id": 12                ,
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
        dag=kwargs['dag'],
    )
    response = op.execute(context=kwargs)
    return quote_id

@task
def set_shipping_address(quote_id: str, **kwargs) -> str:
    op = MagentoApiOperator(
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
            },"shippingCarrierCode": "flatrate", "shippingMethodCode": "flatrate" }
        },
        dag=kwargs['dag'],
    )
    response = op.execute(context=kwargs)
    return quote_id

@task
def set_payment_method(quote_id: str, **kwargs) -> str:
    op = MagentoApiOperator(
        task_id='set_payment_method_op',
        endpoint=f'guest-carts/{quote_id}/payment-information',
        method='POST',
        data={"email": "guest@example.com","paymentMethod": {"method": "checkmo"}},
        dag=kwargs['dag'],
    )
    response = op.execute(context=kwargs)
    return quote_id

@task
def place_order(quote_id: str, **kwargs) -> str:
    op = MagentoApiOperator(
        task_id='place_order_op',
        endpoint=f'guest-carts/{quote_id}/order',
        method='PUT',
        data={},
        dag=kwargs['dag'],
    )
    response = op.execute(context=kwargs)
    order_id = response['order_id']
    return order_id

@task
def create_invoice(order_id: str, **kwargs) -> str:
    op = MagentoApiOperator(
        task_id='create_invoice_op',
        endpoint='invoices',
        method='POST',
        data={
            "entity": {
                "order_id": order_id,
                "items": [
                    {
                        "order_item_id": "order-item-id-placeholder",  # Replace with actual order item ID
                        "qty": 1
                    }
                ],
                "invoice_status": "pending"
            }
        },
        dag=kwargs['dag'],
    )
    response = op.execute(context=kwargs)
    invoice_id = response['invoice_id']
    return invoice_id

@task
def create_shipment(order_id: str, **kwargs) -> str:
    op = MagentoApiOperator(
        task_id='create_shipment_op',
        endpoint='shipments',
        method='POST',
        data={
            "entity": {
                "order_id": order_id,
                "items": [
                    {
                        "order_item_id": "order-item-id-placeholder",  # Replace with actual order item ID
                        "qty": 1
                    }
                ],
                "shipment_status": "pending"
            }
        },
        dag=kwargs['dag'],
    )
    response = op.execute(context=kwargs)
    shipment_id = response['shipment_id']
    return shipment_id

with DAG(
    'magento_order',
    default_args=default_args,
    description='A DAG to perform various operations on Magento',
    schedule_interval=None,  # Run only once manually
    start_date=datetime(2024, 8, 19),
    catchup=False,
) as dag:

    # Define tasks
    product_id = create_product()
    quote_id = create_guest_cart()
    updated_quote_with_items = add_items_to_cart(product_id, quote_id)
    updated_quote_with_billing = set_billing_address(updated_quote_with_items)
    updated_quote_with_shipping = set_shipping_address(updated_quote_with_billing)
    updated_quote_with_payment = set_payment_method(updated_quote_with_shipping)
    order_id = place_order(updated_quote_with_payment)
    invoice_id = create_invoice(order_id)
    shipment_id = create_shipment(order_id)

    # Set task dependencies
    product_id >> quote_id >> updated_quote_with_items >> updated_quote_with_billing >> updated_quote_with_shipping >> updated_quote_with_payment >> order_id >> invoice_id >> shipment_id

