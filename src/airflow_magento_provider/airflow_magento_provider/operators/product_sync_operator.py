import csv
import os
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow_magento_provider.hooks.magento_hooks import MagentoHook


class ProductSyncOperator(BaseOperator):

    @apply_defaults
    def __init__(self, output_file='/tmp/magento_products.csv', *args, **kwargs):
        super(ProductSyncOperator, self).__init__(*args, **kwargs)
        self.output_file = output_file

    def execute(self, context):
        hook = MagentoHook()

        # Get the product data from Magento
        product_data = hook.get_request('products/24-WB02')

        # Ensure the product data is in the format expected (usually a list of dicts)
        if isinstance(product_data, dict):
            product_data = [product_data]

        # Write the product data to a CSV file
        self._write_to_csv(product_data)

        self.log.info(f"Exported product data to {self.output_file}")

    def _write_to_csv(self, product_data):
        # Ensure the directory for the output file exists
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

        # Open the CSV file for writing
        with open(self.output_file, mode='w', newline='') as file:
            if product_data:
                # Extract the headers from the first product's keys
                fieldnames = product_data[0].keys()

                # Create a CSV DictWriter
                writer = csv.DictWriter(file, fieldnames=fieldnames)

                # Write the header
                writer.writeheader()

                # Write the product data
                for product in product_data:
                    writer.writerow(product)
            else:
                self.log.warning("No product data found to write to CSV.")


