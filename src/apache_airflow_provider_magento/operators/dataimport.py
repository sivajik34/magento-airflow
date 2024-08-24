import os
import base64
import gzip
from airflow.models import BaseOperator
from apache_airflow_provider_magento.hooks.magento import MagentoHook
from typing import List, Union, Dict


class MagentoImportOperator(BaseOperator):

    def __init__(self,
                 endpoint: str,
                 store_view_code: str,
                 csv_file_path: str = None,
                 data: Union[List[Dict], None] = None,  # For JSON data
                 data_format: str = 'csv',  # 'csv' or 'json'
                 entity: str = None,
                 behavior: str = None,
                 validation_strategy: str = None,
                 allowed_error_count: str = None,
                 import_field_separator: str = ',',
                 import_multiple_value_separator: str = ',',
                 import_empty_attribute_value_constant: str = '',
                 import_images_file_dir: str = '',
                 chunk_size: int = 10000,
                 magento_conn_id: str = "magento_default",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.store_view_code = store_view_code
        self.csv_file_path = csv_file_path
        self.data = data
        self.data_format = data_format
        self.entity = entity
        self.behavior = behavior
        self.validation_strategy = validation_strategy
        self.allowed_error_count = allowed_error_count
        self.import_field_separator = import_field_separator
        self.import_multiple_value_separator = import_multiple_value_separator
        self.import_empty_attribute_value_constant = import_empty_attribute_value_constant
        self.import_images_file_dir = import_images_file_dir
        self.chunk_size = chunk_size
        self.magento_conn_id = magento_conn_id

    def base64_encode(self, data: str) -> str:
        """Base64 encode and optionally gzip compress the data."""
        if isinstance(data, str):
            data = data.encode('utf-8')
        compressed_data = gzip.compress(data)
        return base64.b64encode(compressed_data).decode('utf-8')

    def read_csv_in_chunks(self, file_path: str, chunk_size: int) -> List[List[str]]:
        """Read the CSV file in chunks."""
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Read the header row
            chunk = []
            for row in reader:
                chunk.append(row)
                if len(chunk) >= chunk_size:
                    yield [headers] + chunk
                    chunk = []
            if chunk:
                yield [headers] + chunk

    def execute(self, context):
        hook = MagentoHook(magento_conn_id=self.magento_conn_id, store_view_code=self.store_view_code)

        if self.data_format == 'csv':
            # Read the CSV file in chunks
            chunks = self.read_csv_in_chunks(self.csv_file_path, self.chunk_size)
            for chunk in chunks:
                csv_data = '\n'.join([','.join(row) for row in chunk])
                encoded_data = self.base64_encode(csv_data)
                payload = {
                    "source": {
                        "locale": "en_EN",  # Assuming locale is required
                        "entity": self.entity,
                        "behavior": self.behavior,
                        "validationStrategy": self.validation_strategy,
                        "allowedErrorCount": self.allowed_error_count,
                        "csvData": encoded_data,
                        "importFieldSeparator": self.import_field_separator,
                        "importMultipleValueSeparator": self.import_multiple_value_separator,
                        "importEmptyAttributeValueConstant": self.import_empty_attribute_value_constant,
                        "importImagesFileDir": self.import_images_file_dir
                    }
                }

        elif self.data_format == 'json':
            # Ensure that data is a list of entities for JSON format
            if not isinstance(self.data, list):
                raise ValueError("Data must be a list of entities for JSON format.")
            payload = {
                "source": {
                    "locale": "en_EN",  # Assuming locale is required
                    "entity": self.entity,
                    "behavior": self.behavior,
                    "validationStrategy": self.validation_strategy,
                    "allowedErrorCount": self.allowed_error_count,
                    "items": self.data
                }
            }

        else:
            raise ValueError("Invalid data format. Must be 'csv' or 'json'.")

        headers = {'Content-Type': 'application/json'}
        response = hook.post_request(endpoint=self.endpoint, data=payload, headers=headers)
        self.log.info("Import result: %s", response)

