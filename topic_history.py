import pg

cur = pg.cursor()
cur.execute("SELECT * FROM topic")

rows = cur.fetchall()

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
