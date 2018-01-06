import logging
import redis
import threading
import io
import telegram
from telegram.ext import Updater
from telegram.ext import CommandHandler

token = io.open("./.bot_token", mode="r", encoding="utf-8").read().replace("\r", "").replace("\n", "")
updater = Updater(token=token)
dispatcher = updater.dispatcher

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

listeners = []

def start(bot, update):
    print update.message
    print update.message.chat_id
    print "New Listener"
    listeners.append(update.message.chat_id)
    bot.send_message(chat_id=update.message.chat_id, text="You're now listening.")

start_handler = CommandHandler('21blocks_subscribe', start)
dispatcher.add_handler(start_handler)
updater.start_polling()

# updater.bot.send_message(chat_id="-1001312685209", text="OK")

class Listener(threading.Thread):
    def __init__(self, r, channels):
        threading.Thread.__init__(self)
        self.redis = r
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe(channels)
    
    def work(self, item):
        for listener in listeners:
            updater.bot.send_message(chat_id=listener, text=item["data"], parse_mode=telegram.ParseMode.MARKDOWN)
    
    def run(self):
        for item in self.pubsub.listen():
            if item['data'] == "KILL":
                self.pubsub.unsubscribe()
                print self, "unsubscribed and finished"
                break
            else:
                self.work(item)

r = redis.Redis()
client = Listener(r, ['bot_messages'])
client.start()
