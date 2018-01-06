CREATE TABLE topic_daily (
    id SERIAL,
    topic_id INTEGER NOT NULL,
    date TIMESTAMP NOT NULL,
    num_pages INTEGER NOT NULL,
    count_read INTEGER NOT NULL
);
