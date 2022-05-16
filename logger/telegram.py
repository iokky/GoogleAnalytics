import os
import telebot

from dotenv import load_dotenv

load_dotenv()

bot = telebot.TeleBot(os.getenv('TOKEN'))


def send_message(data):
    consumer = os.getenv('USER')
    bot.send_message(consumer, data)
