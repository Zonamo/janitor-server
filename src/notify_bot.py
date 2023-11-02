import time, os
from telegram import Bot

NOTIFY_TOKEN = os.getenv('NOTIFY_TOKEN')
ID_ZOMBIE = os.getenv('ID_ZOMBIE')
ID_NEW = os.getenv('ID_NEW')
ID_REPORT = os.getenv('ID_REPORT')
bot = Bot(token=NOTIFY_TOKEN)

def send_notification(message, id):
    bot.sendMessage(chat_id=id, text=message)

def send_messages(messages):
    send_notification("+"*22 + "\n" + f"{len(messages)} NEW OPTIONS")
    for message in messages:
        if message[1] == 0:
            id = ID_ZOMBIE
        if message[1] == 1:
            id = ID_NEW
        send_notification(message[0], id)
        time.sleep(3.1)  
    send_notification("+"*22 + "\n")


def send_file(file_path):
    with open(file_path, 'rb') as file:
        bot.sendDocument(chat_id=ID_REPORT, document=file)

def send_report(message):
    bot.sendMessage(chat_id=ID_REPORT, text=message)


if __name__ == "__main__":
    send_report('it is known')