from datetime import datetime
import json
import csv

from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.http_hook import HttpHook


class CreateCsvForAirtableOperator(BaseOperator):
    @apply_defaults
    def __init__(self,  # noqa: CFQ002
                 endpoint: str,
                 http_conn_id: str,
                 message_id_filepath: str,
                 update_csv_filepath: str,
                 method='GET',
                 request_params=None,
                 *args, **kwargs):
        super(CreateCsvForAirtableOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.request_params = request_params or {}
        self.endpoint = endpoint
        self.message_id_filepath = message_id_filepath
        self.update_csv_filepath = update_csv_filepath
        self.hook = HttpHook(
            method=method,
            http_conn_id=http_conn_id)

    def poke(self, context):
        response = self.hook.run(self.endpoint, data=self.request_params)

        with open(self.message_id_filepath, 'r') as id_file:
            message_id = id_file.read()
        json_response = json.loads(response.text)
        all_updates_data = []
        for update in json_response['result']:
            update_data = {}
            if update['callback_query']['message']['message_id'] == message_id:
                chat_id = update['callback_query']['message']['chat']['id']
                username = update['callback_query']['from']['username']
                triggered_at = datetime.utcnow().isoformat()[:-3] + "Z"

                update_data['chat_id'] = chat_id
                update_data['username'] = username
                update_data['triggered_at'] = triggered_at
                update_data['event_type'] = 'push_go_button'
                update_data['reporter_name'] = 'khkaterina'
                all_updates_data.append(update_data)

        headers = all_updates_data[0].keys()
        with open(self.update_csv_filepath, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, headers)
            writer.writeheader()
            writer.writerows(all_updates_data)