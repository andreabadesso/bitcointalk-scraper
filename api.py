#encoding: utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import pg
import json
from flask import Flask
app = Flask(__name__)

def row_to_topic(row):
    dict = {
            "topic_id": row[0],
            "name": row[1],
            "last_updated": "{}".format(row[2]),
            "num_pages": row[3],
            "coun_read": row[4]
            }

    return dict

@app.route("/")
def all_topics():
    cur = pg.cursor()
    cur.execute("SELECT sid, name, db_update_time, num_pages, count_read FROM topic")
    rows = cur.fetchall()
    data = map(lambda x: row_to_topic(x), rows)
    print json.dumps(data)

    return json.dumps(data)

