import logging
import redis
import threading
import io
import telegram
import pg
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

def latest_masternode(bot, update):
    cur = pg.cursor()
    cur.execute("SELECT * FROM topic WHERE name ILIKE '%masternode%' LIMIT 10")

    def format(row):
        return """{0}
https://bitcointalk.org/index.php?topic={1}""".format(row[1], row[0])

    messages = map(lambda x: format(x), cur.fetchall())
    message = "\r\n".join(messages)

    updater.bot.send_message(chat_id=update.message.chat_id, text=message, parse_mode=telegram.ParseMode.MARKDOWN)

def latest(bot, update):
    cur = pg.cursor()
    cur.execute("SELECT sid, name FROM topic ORDER BY sid DESC LIMIT 10")

    def format(row):
        return """{0}
https://bitcointalk.org/index.php?topic={1}""".format(row[1], row[0])

    messages = map(lambda x: format(x), cur.fetchall())
    message = "\r\n".join(messages)

    updater.bot.send_message(chat_id=update.message.chat_id, text=message, parse_mode=telegram.ParseMode.MARKDOWN)

start_handler = CommandHandler('21blocks_subscribe', start)
latest_handler = CommandHandler('latest', latest)
latest_masternodes_handler = CommandHandler('latest', latest_masternode)
dispatcher.add_handler(start_handler)
dispatcher.add_handler(latest_handler)
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
