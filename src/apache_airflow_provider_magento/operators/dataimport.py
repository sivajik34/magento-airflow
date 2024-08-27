from __future__ import annotations
import os
import csv
import json
import base64
import gzip
from typing import List, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from apache_airflow_provider_magento.hooks.magento import MagentoHook

class MagentoImportOperator(BaseOperator):
    def __init__(
        self,        
        endpoint: str,        
        store_view_code: str,    
        entity: str,
        behavior: str,
        validation_strategy: str,
        allowed_error_count: str,
        import_field_separator: str,
        import_multiple_value_separator: str,
        import_empty_attribute_value_constant: str,
        import_images_file_dir: str,
        method: str = "POST",
        csv_file_path: Optional[str] = None,
        chunk_size: int = 10000,
        data_format: str = 'csv',
        magento_conn_id: str = 'magento_default',
        data: Optional[List[dict]] = None,*args,
        **kwargs
    ) -> None:
        super().__init__(*args,**kwargs)
        self.endpoint = f"/rest/default/V1/{endpoint}"
        self.method = method
        self.store_view_code = store_view_code
        self.csv_file_path = csv_file_path
        self.chunk_size = chunk_size
        self.data_format = data_format
        self.entity = entity
        self.behavior = behavior
        self.validation_strategy = validation_strategy
        self.allowed_error_count = allowed_error_count
        self.import_field_separator = import_field_separator
        self.import_multiple_value_separator = import_multiple_value_separator
        self.import_empty_attribute_value_constant = import_empty_attribute_value_constant
        self.import_images_file_dir = import_images_file_dir
        self.magento_conn_id = magento_conn_id
        self.data = data

    def read_csv_in_chunks(self, file_path: str, chunk_size: int) -> List[List[str]]:
        chunks = []
        with open(file_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader)  # Skip header
            chunk = [header]
            for row in reader:
                chunk.append(row)
                if len(chunk) == chunk_size:
                    chunks.append(chunk)
                    chunk = [header]  # Start new chunk with header
            if chunk:
                chunks.append(chunk)  # Add last chunk if it has remaining rows
        return chunks

    def base64_encode(self, data: str) -> str:
        if isinstance(data, str):
            data = data.encode('utf-8')
        compressed_data = gzip.compress(data)
        return base64.b64encode(compressed_data).decode('utf-8')

    def execute(self, context):
        hook = MagentoHook(magento_conn_id=self.magento_conn_id, method=self.method)

        if self.data_format == 'csv':
            if not self.csv_file_path:
                raise AirflowException("CSV file path must be provided for CSV format.")

            chunks = self.read_csv_in_chunks(self.csv_file_path, self.chunk_size)

            for chunk in chunks:
                csv_data = '\n'.join([','.join(row) for row in chunk])
                encoded_data = self.base64_encode(csv_data)

                payload = {
                    "source": {
                        "locale": "en_EN",
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

                headers = {'Content-Type': 'application/json'}
                response = hook.send_request(endpoint=self.endpoint, data=payload, headers=headers)
                self.log.info("Import result for chunk: %s", response)

        elif self.data_format == 'json':
            if not isinstance(self.data, list):
                raise ValueError("Data must be a list of entities for JSON format.")

            payload = {
                "source": {
                    "locale": "en_EN",
                    "entity": self.entity,
                    "behavior": self.behavior,
                    "validationStrategy": self.validation_strategy,
                    "allowedErrorCount": self.allowed_error_count,
                    "items": self.data
                }
            }

            headers = {'Content-Type': 'application/json'}
            response = hook.send_request(endpoint=self.endpoint, data=payload, headers=headers)
            self.log.info("Import result: %s", response)

        else:
            raise ValueError("Invalid data format. Must be 'csv' or 'json'.")

        self.log.info("All data has been processed and imported.")

