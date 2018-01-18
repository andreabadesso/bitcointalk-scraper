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
            "read_increase": row[2],
            "page_increase": row[3],
            "num_pages": row[4],
            "count_read": row[5]
            }

    return dict

@app.route("/")
def all_topics():
    cur = pg.cursor()
    cur.execute("SELECT sid, name, read_increase, page_increase, num_pages, count_read FROM alerts")
    rows = cur.fetchall()
    data = map(lambda x: row_to_topic(x), rows)
    print json.dumps(data)

    return json.dumps(data)

