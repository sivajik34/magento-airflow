from __future__ import annotations

from airflow.models import BaseOperator
from apache_airflow_provider_magento.hooks.magento import MagentoHook


class GetOrdersOperator(BaseOperator):
    """Fetch orders based on order status from Magento."""

    def __init__(self, magento_conn_id="magento_default", status="pending", page_size=100, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.magento_conn_id = magento_conn_id
        self.status = status
        self.page_size = page_size

    def execute(self, context):
        magento_hook = MagentoHook(magento_conn_id=self.magento_conn_id)
        endpoint = "orders"
        orders = []
        current_page = 1
        while True:
            search_criteria = {
                "searchCriteria[pageSize]": self.page_size,
                "searchCriteria[currentPage]": current_page,
            }

            if self.status:
                search_criteria.update(
                    {
                        "searchCriteria[filterGroups][0][filters][0][field]": "status",
                        "searchCriteria[filterGroups][0][filters][0][value]": self.status,
                        "searchCriteria[filterGroups][0][filters][0][conditionType]": "eq",
                    }
                )
            data = magento_hook.get_request(endpoint, search_criteria=search_criteria)
            if not data["items"]:
                break

            orders.extend(data["items"])
            current_page += 1

        if orders:
            context["ti"].xcom_push(key="magento_orders", value=orders)
        else:
            self.log.info("No new orders found.")
