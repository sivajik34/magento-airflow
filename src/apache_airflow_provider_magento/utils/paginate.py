from urllib.parse import urlencode
from typing import Callable, Dict, Any,Optional
from apache_airflow_provider_magento.hooks.magento import MagentoHook
import logging
def paginate(
    hook: MagentoHook,
    endpoint: str,
    headers: Optional[Dict[str, str]] = None,
    extra_options: Optional[Dict[str, Any]] = None,
    page_size: int = 100,
    search_criteria: Optional[Dict[str, Any]] = None,
    search_criteria_key: str = 'searchCriteria'
) -> Dict[str, Any]:
    """
    Handles pagination for fetching results from Magento API.

    :param hook: MagentoHook instance.
    :param endpoint: API endpoint to hit.
    :param headers: HTTP headers to include in the request.
    :param extra_options: Additional options for the request.
    :param page_size: Number of items per page.
    :param search_criteria: Additional search criteria to apply.
    :param search_criteria_key: Key for search criteria in the request.
    :return: Aggregated data from all pages.
    """
    all_results = []
    page = 1

    while True:
        # Prepare pagination parameters
        paginated_criteria = {
            f"{search_criteria_key}[pageSize]": page_size,
            f"{search_criteria_key}[currentPage]": page
        }
        
        if search_criteria:
            paginated_criteria.update(search_criteria)
     
        # Append pagination parameters to the endpoint URL
        query_string = urlencode(paginated_criteria, doseq=True)
        paginated_endpoint = f"{endpoint}?{query_string}"
        logging.info(f"paginated_endpoint:{paginated_endpoint}")
        response = hook.send_request(
            endpoint=paginated_endpoint,
            headers=headers,
            extra_options=extra_options
        )

        items = response.get('items', [])
        if not items:
            break
        
        all_results.extend(items)
        
        # Check if there is another page
        if len(items) < page_size:
            break
        
        page += 1
    
    return all_results

