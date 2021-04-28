#!/usr/bin/python3

"""
Main program to receive monitoring results from Kafka
and to store the received data in a PostgreSQL database
"""

import argparse
import logging
import json

import kafka
import psycopg2

from config import MonitorConfig,MonitorConfigError

class KafkaConsumer:
    """
    Class for the Kafka consumer
    """

    def __init__(self, config):
        """ Constructor """
        # producer config - copy the original
        pconfig = dict(config.publish)
        pconfig.update({
            "value_deserializer": json.loads,
            "group_id": "monitor-data-group"
        })

        logging.debug(pconfig)
        self.consumer = kafka.KafkaConsumer("monitor-data", **pconfig)

    def receive(self):
        """ Receives all new data from Kafka and yields them """
        raw_msgs = self.consumer.poll(timeout_ms=10000)
        for _, msgs in raw_msgs.items():
            yield from msgs

    def commit(self):
        """ Notifies Kafka that the data has been processed """
        self.consumer.commit()

class PGDb:
    """
    Class to store monitoring results in a PostgreSQl database
    The database consists of two tables:
    1. url: a mapping of monitored URLs to an url_id
    2. data: the collected monitoring data

    As in a monitoring application like this there is usually a
    fixed set of URL that rarely change the technical decision I made here
    is that the application will cache all these URLs and their IDs,
    so when inserting a new monitor record into the DB the URL ID is already
    known, so no need for a nested SELECT, leading to some performance increase.
    """

    def __init__(self, config):
        """
        Constructs the object:
        - creates the DB connection
        - creates nonexistant DB tables
        - loads existing URLs into memory for faster operation
        """
        self.conn = psycopg2.connect(config.db["uri"])

        self.cur = self.conn.cursor()
        self.urls = {}
        self.init_tables()
        self.load_urls()

    def execute(self, *args, **kwargs):
        """ Executes an SQL statement """
        logging.debug(f"PG execute: {args} {kwargs}")
        self.cur.execute(*args, **kwargs)

    def fetchall(self):
        """ Fetches the results of the previous DB operation """
        return self.cur.fetchall()

    def commit(self):
        """ Commits the last DB transaction """
        self.cur.commit()

    def load_urls(self):
        """ Loads all the urls in the DB into the local cache """

        self.execute("SELECT * FROM url;")
        for url_id, url in self.fetchall():
            logging.debug(f"Caching url_id={url_id} url={url}")
            self.urls[url] = url_id

    def url_id(self, url):
        """
        Checks if the ID of the url is already known (if it exists in the DB)
        If not, it creates it and stores locally
        """

        if url in self.urls:
            return self.urls[url]

        self.execute("INSERT INTO url (url) VALUES (%s) RETURNING url_id;", (url,))
        (url_id,) = self.fetchall()
        self.urls[url] = url_id
        logging.debug(f"Created url_id={url_id} url={url}")

        self.execute("COMMIT")
        return url_id

    def add_data(self, data):
        """ Adds a monitoring data record to the DB """
        url_id = self.url_id(data["url"])
        self.execute("""
           INSERT INTO data VALUES (%s, to_timestamp(%s), %s, %s, %s);
           """, (url_id, data["time"], data["status"], data["found"], data["duration"]))

    def init_tables(self):
        """ Creates the database table if they don't exist """
        self.execute("""
            CREATE TABLE IF NOT EXISTS url (
                url_id SERIAL PRIMARY KEY,
                url VARCHAR(1024) UNIQUE NOT NULL
            );
            """)
        self.execute("""
            CREATE TABLE IF NOT EXISTS data (
                url_id integer REFERENCES url (url_id),
                time TIMESTAMP NOT NULL,
                status CHAR(3) NOT NULL,
                found BOOLEAN,
                duration NUMERIC(8) NOT NULL
            );
            """)

        self.execute("COMMIT")

    def drop(self):
        """ Drops the database tables """
        self.execute("DROP TABLE IF EXISTS data;")
        self.execute("DROP TABLE IF EXISTS url;")

    def list(self):
        """ Lists a few lines from the DB """
        self.execute("""
            SELECT * FROM data LIMIT 100;
            """)

        for row in self.fetchall():
            print(row)

def get_args():
    """ Processes command line args """
    ap = argparse.ArgumentParser()
    ap.add_argument('--debug', action="store_true", help="Enable debug logging")
    ap.add_argument('--config', help="Name of config file")
    ap.add_argument('--drop', action="store_true",
                    help="Drop database tables (easy way to lose your data)")
    ap.add_argument('--list', action="store_true", help="List data")

    return ap.parse_args()

def main():
    """ Main program """

    # Processing command line args
    args = get_args()

    # Setting up the logger
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level)
    logging.getLogger("chardet").setLevel(logging.CRITICAL)
    logging.getLogger("kafka").setLevel(logging.CRITICAL)

    logging.debug(f"args: {args}")

    # Loading config
    try:
        config = MonitorConfig(args.config)
    except MonitorConfigError as exc:
        raise SystemExit(f"Failed loading config: {exc}")

    # Creating the DB object
    pg_db = PGDb(config)

    # Helper to drop the DB tables
    if args.drop:
        pg_db.drop()
        raise SystemExit("Dropped database tables")

    # Helper to list a few rows from the DB
    if args.list:
        pg_db.list()
        raise SystemExit()

    # Creating Kafka Consumer
    consumer = KafkaConsumer(config)

    # Main loop
    while True:
        # We infinitely consume data received from Kafka
        # and store them in the DB.
        for msg in consumer.receive():
            logging.info("Received: {}".format(msg.value))
            pg_db.add_data(msg.value)
        consumer.commit()

if __name__ == '__main__':
    main()
