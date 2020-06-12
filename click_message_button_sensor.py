from airflow.utils.decorators import apply_defaults
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.hooks.http_hook import HttpHook


class ClickMessageButtonSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self,
                 endpoint,
                 http_conn_id,
                 message_id_filepath: str,
                 method='GET',
                 request_params=None,
                 *args, **kwargs,
    ):
        super(ClickMessageButtonSensor, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.request_params = request_params or {}
        self.endpoint = endpoint
        self.message_id_filepath = message_id_filepath
        self.hook = HttpHook(
            method=method,
            http_conn_id=http_conn_id)

    def poke(self, context):
        self.log.info('Poking: %s', self.endpoint)
        try:
            response = self.hook.run(self.endpoint, data=self.request_params)
        except AirflowException as ae:
            if str(ae).startswith(('404', '500')):
                return False
            raise ae

        with open(self.message_id_filepath, 'r') as id_file:
            message_id = id_file.read()

        json_response = json.loads(response.text)
        is_clicked = False
        for update in json_response['result']:
            if update['callback_query']['message']['message_id'] == int(message_id):
                is_clicked = True

        return is_clicked
