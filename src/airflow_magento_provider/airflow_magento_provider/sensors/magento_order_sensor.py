from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow_magento_provider.hooks.magento_hooks import MagentoHook
class MagentoOrderSensor(BaseSensorOperator):
    
    @apply_defaults
    def __init__(self, magento_conn_id='magento', poke_interval=900, **kwargs):
        super(MagentoOrderSensor, self).__init__(**kwargs)
        self.magento_conn_id = magento_conn_id
        self.poke_interval = poke_interval

    def poke(self, context):
        magento_hook = MagentoHook()  # Ensure the hook is properly configured
        search_criteria = {
            'searchCriteria[filterGroups][0][filters][0][field]': 'status',
            'searchCriteria[filterGroups][0][filters][0][value]': 'pending',
            'searchCriteria[filterGroups][0][filters][0][conditionType]': 'eq'
        }
        orders = magento_hook.get_orders(search_criteria=search_criteria)
        if orders and orders.get('total_count', 0) > 0:
            context['ti'].xcom_push(key='new_orders', value=orders)
            return True
        return False

