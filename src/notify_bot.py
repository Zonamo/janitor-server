import time, os
from telegram import Bot

NOTIFY_TOKEN = os.getenv('NOTIFY_TOKEN')
ID = os.getenv('ID')
ID_REPORT = os.getenv('ID_REPORT')
bot = Bot(token=NOTIFY_TOKEN)

def send_notification(message):
    bot.sendMessage(chat_id=ID, text=message)

def send_messages(messages):
    send_notification("+"*22 + "\n" + f"{len(messages)} NEW OPTIONS")
    for message in messages:
        send_notification(message)
        time.sleep(3.1)  
    send_notification("+"*22 + "\n")


def send_file(file_path):
    with open(file_path, 'rb') as file:
        bot.sendDocument(chat_id=ID_REPORT, document=file)

def send_report(message):
    bot.sendMessage(chat_id=ID_REPORT, text=message)


if __name__ == "__main__":
    send_report('it is known')