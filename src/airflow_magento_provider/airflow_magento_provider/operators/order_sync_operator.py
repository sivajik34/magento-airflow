from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow_magento_provider.hooks.magento_hooks import MagentoHook

class OrderSyncOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(OrderSyncOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        hook = MagentoHook()
        # Get the orders from Magento
        orders = hook.get_request('orders')
        # Implement the logic to sync orders
        self.log.info(f"Fetched {len(orders)} orders from Magento.")

