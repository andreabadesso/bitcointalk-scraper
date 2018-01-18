#encoding: utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import pg
import redis

query = """ 
    WITH
        latest AS
        (
            SELECT
                DISTINCT ON (topic_id) topic_id,
                id,
                num_pages,
                count_read
            FROM topic_daily
            ORDER BY topic_id, id DESC
        ),
        last_day AS (
            SELECT DISTINCT ON
                (topic_id) topic_id,
                id,
                num_pages,
                count_read
            FROM 
                topic_daily
            WHERE date < (now() - INTERVAL '1 day') ORDER BY topic_id, id DESC
        )

    SELECT *,
        (latest.num_pages * 100) / (
            SELECT 
                num_pages
            FROM last_day
            WHERE topic_id = latest.topic_id
        )::float - 100 AS pages_percent_increase,
        (latest.count_read * 100) / (
            SELECT 
                count_read
            FROM last_day
            WHERE topic_id = latest.topic_id
        )::float - 100 AS count_read_percent_increase,
        (
            SELECT num_pages
            FROM last_day
            WHERE topic_id = latest.topic_id
        ) AS last_day_num_pages,
        (
            SELECT count_read
            FROM last_day
            WHERE topic_id = latest.topic_id
        ) AS last_day_count_read,
        (
        	SELECT latest.num_pages::float / 5000
        ) AS weight

    FROM latest
    WHERE
        (((latest.num_pages * 100) / (
            SELECT 
                num_pages
            FROM last_day
            WHERE topic_id = latest.topic_id
        )::float - 100) * (latest.num_pages::float / 5000)) > 0.1
"""

def row_to_topic(row):
    dict = {
            "topic_id": row[0],
            "name": row[1],
            "last_updated": row[2]
            }

    return dict

def get_topics(cur):
    cur.execute("SELECT sid, name, db_update_time FROM topic")
    rows = cur.fetchall()
    return map(lambda x: row_to_topic(x), rows)

def create_dict(row, topics):
    dict = {}
    for topic in topics:
        if topic["topic_id"] == row[0]:
            dict["name"] = topic["name"]
    dict["topic_id"] = row[0]
    dict["num_pages"] = row[2]
    dict["count_read"] = row[3]
    dict["pages_increase"] = row[4]
    dict["read_increase"] = row[5]
    dict["last_day_pages"] = row[6]
    dict["last_day_reads"] = row[7]

    return dict

def get_alarms(query, cur, topics):
    cur.execute(query)
    rows = cur.fetchall()
    print "{0} alarms found.".format(len(rows))
    return map(lambda x: create_dict(x, topics), rows)

cur = pg.cursor()
topics = get_topics(cur)
alarms = get_alarms(query, cur, topics)

def chunkify(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i + n]

if len(alarms) > 0:
    counter = 0
    messages = [u"🔔🔔 Announcements com aumento de relevância: 🔔🔔", "\r\n"]
    for alarm in alarms:
        messages.append(u"""**{0}**
Aumento no número de visualizações do tópico: `+{1}%` ({4})
Aumento no número de paginas do tópico: `+{2}%` ({5})
URL: https://bitcointalk.org/index.php?topic={3}
""".format(alarm["name"],
            round(alarm["read_increase"], 2),
            round(alarm["pages_increase"], 2),
            alarm["topic_id"],
            "{0} -> {1}".format(alarm["last_day_reads"], alarm["count_read"]),
            "{0} -> {1}".format(alarm["last_day_pages"], alarm["num_pages"])
            ))
        counter = counter + 1

    r = redis.Redis()
    chunks = chunkify(messages, 10)
    for chunk in chunks:
        message = "\r\n".join(chunk)
        r.publish("bot_messages", message)

try: 
    cur.execute("BEGIN TRANSACTION;")
    cur.execute("TRUNCATE TABLE alerts")
    cur.execute("""
        INSERT INTO alerts
        (sid, name, board, num_pages, count_read, read_increase, pages_increase) VALUES
        ({0}, {1}, {2}, {3}, {4}, {5}, {6})
    """.format(alarm["topic_id"], alarm["name"], 1, alarm["num_pages"], alarm["count_read"], alarm["read_increase"], alarm["pages_increase"]))
except Exception as e:
    print("EXCEPTION")
    cur.execute("ROLLBACK;")
else:
    cur.execute("COMMIT;")

print len(alarms)
