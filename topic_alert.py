import pg

query_check = """ 
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

def create_dict(row):
    dict = {}
    dict["topic_id"] = row[0]
    dict["num_pages"] = row[2]
    dict["count_read"] = row[3]
    dict["pages_increase"] = row[4]
    dict["read_increase"] = row[5]
    dict["last_day_pages"] = row[6]
    dict["last_day_reads"] = row[7]

cur = pg.cursor()
cur.execute(query)


rows = cur.fetchall()
topics = map(lambda x: create_dict(x), rows)

print topics

"""
try:
    for row in rows:
        topic_id = row[0]
        date = "now()"
        num_pages = row[3]
        count_read = row[4]

        cur.execute("""
        INSERT INTO topic_daily
        (topic_id, date, num_pages, count_read) VALUES
        ({0}, {1}, {2}, {3})
        """.format(topic_id, date, num_pages, count_read))
except Exception as e:
    print "Erroed"
    print e
    cur.execute("ROLLBACK")
else:
    cur.execute("COMMIT")
"""

