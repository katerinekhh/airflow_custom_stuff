import json
import requests

from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator


class LoadDataToAirtableOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            airtable_api_key: str,
            table_key: str,
            table_name: str,
            http_conn_id: str,
            json_file_path: str,
            *args, **kwargs,
    ):
        super(LoadDataToAirtableOperator, self).__init__(*args, **kwargs)
        self.airtable_api_key = airtable_api_key
        self.table_key = table_key
        self.table_name = table_name
        self.http_conn_id = http_conn_id
        self.json_file_path = json_file_path

    def execute(self, context):
        headers = {
            'Authorization': f'Bearer {self.airtable_api_key}',
            'Content-Type': 'application/json',
        }
        with open(self.json_file_path, 'r') as json_file:
            body = json.load(json_file)
        airtable_url = f'https://api.airtable.com/v0/{self.table_key}/{self.table_name}'
        requests.post(airtable_url, headers=headers, json=body)
