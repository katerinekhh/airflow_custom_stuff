import telebot

from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator


class SendTelegramButtonMessageOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            tg_token: str,
            chat_id: str,
            message_text: str,
            message_id_filepath: str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.tg_token = tg_token
        self.chat_id = chat_id
        self.message_text = message_text
        self.message_id_filepath = message_id_filepath

    def execute(self, context):
        bot = telebot.TeleBot(self.tg_token)
        markup = telebot.types.InlineKeyboardMarkup()
        go_button = telebot.types.InlineKeyboardButton(text=self.message_text, callback_data='hi')
        markup.add(go_button)
        message = bot.send_message(self.chat_id, self.message_text, reply_markup=markup)
        with open(self.message_id_filepath, 'w') as id_file:
            id_file.write(str(message.message_id))
