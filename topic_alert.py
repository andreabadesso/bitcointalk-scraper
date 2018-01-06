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
        ) AS last_day_count_read

    FROM latest
    WHERE
        ((latest.num_pages * 100) / (
            SELECT 
                num_pages
            FROM last_day
            WHERE topic_id = latest.topic_id
        )::float - 100) > 10 OR
        ((latest.count_read * 100) / (
            SELECT 
                count_read
            FROM last_day
            WHERE topic_id = latest.topic_id
        )::float - 100) > 10
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
if len(alarms) > 0:
    counter = 0
    messages = ["Announcements com aumento de relevancia:", "\r\n"]
    for alarm in alarms:
        messages.append("""**{0}**
Aumento no numero de visualizacoes do topico: `+{1}%` ({4})
Aumento no numero de paginas do topico: `+{2}%` ({5})
URL: https://bitcointalk.org/index.php?topic={3}
""".format(alarm["name"],
            alarm["read_increase"],
            alarm["pages_increase"],
            alarm["topic_id"],
            alarm["num_pages"],
            alarm["count_read"]
            ))
        counter = counter + 1
    message = "\r\n".join(messages)
    r = redis.Redis()
    r.publish("bot_messages", message)

print len(alarms)
# print alarms
