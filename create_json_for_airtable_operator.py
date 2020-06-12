from datetime import datetime
import json

from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.http_hook import HttpHook


class CreateJsonForAirtableOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            endpoint: str,
            http_conn_id: str,
            message_id_filepath: str,
            update_filepath: str,
            method='GET',
            request_params=None,
            *args, **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.request_params = request_params or {}
        self.endpoint = endpoint
        self.message_id_filepath = message_id_filepath
        self.update_filepath = update_filepath
        self.hook = HttpHook(
            method=method,
            http_conn_id=http_conn_id)


    def execute(self, context):
        response = self.hook.run(self.endpoint, data=self.request_params)

        with open(self.message_id_filepath, 'r') as id_file:
            message_id = id_file.read()
        json_response = json.loads(response.text)
        airtable_updates_data = {}
        airtable_updates_data['records'] = []
        for update in json_response['result']:
            update_data_fields = {}
            update_data = {}
            if update['callback_query']['message']['message_id'] == int(message_id):
                chat_id = update['callback_query']['message']['chat']['id']
                username = update['callback_query']['from']['username']
                triggered_at = datetime.fromtimestamp(
                    update['callback_query']['message']['date']).isoformat()[:-3] + "Z"

                update_data['chat_id'] = chat_id
                update_data['username'] = username
                update_data['triggered_at'] = triggered_at
                update_data['event_type'] = 'push_go_button'
                update_data['reporter_name'] = 'khkaterina'
                update_data_fields['fields'] = update_data

                airtable_updates_data['records'].append(update_data_fields)

        with open(self.update_filepath, 'w') as file:
            json.dump(airtable_updates_data, file)

