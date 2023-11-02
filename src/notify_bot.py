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
    new, zombie = [], []
    for message in messages:
        if message[1] == 0:
            zombie.append(message[0])
        if message[1] == 1:
            new.append(message[0])
    if new:
        send_notification("+"*22 + "\n" + f"{len(new)} NEW OPTIONS", ID_NEW)
        for message in new:
            send_notification(message, ID_NEW)
            time.sleep(3.1)  
        send_notification("+"*22 + "\n", ID_NEW)

    if zombie:
        send_notification("+"*22 + "\n" + f"{len(zombie)} OPTIONS", ID_ZOMBIE)
        for message in zombie:
            send_notification(message, ID_ZOMBIE)
            time.sleep(3.1)  
        send_notification("+"*22 + "\n", ID_ZOMBIE)


def send_file(file_path):
    with open(file_path, 'rb') as file:
        bot.sendDocument(chat_id=ID_REPORT, document=file)

def send_report(message):
    bot.sendMessage(chat_id=ID_REPORT, text=message)


if __name__ == "__main__":
    send_messages([('peter mogelijk jood', 0), ('peter extreem jood', 1)])