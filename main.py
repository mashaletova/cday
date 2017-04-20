from src import database
from src import httpclient
import argparse
import sqlite3
import time

DB_PATH = 'iot.db'
DDL = """
CREATE TABLE "datastreams" (
	`ds_id`	INTEGER NOT NULL UNIQUE,
	`ds_deployment`	TEXT NOT NULL DEFAULT 'indoor',
	`ds_location`	TEXT DEFAULT -1,
	`ds_kind`	TEXT NOT NULL,
	`feed_id`	INTEGER NOT NULL DEFAULT 4,
	PRIMARY KEY(`ds_id`)
);
CREATE TABLE "datapoints" (
	`ch_id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	`ch_timestamp`	INTEGER NOT NULL,
	`ch_value`	REAL NOT NULL,
	`ds_id`	INTEGER NOT NULL,
	FOREIGN KEY(`ds_id`) REFERENCES datastreams(ds_id)
);
"""


parser = argparse.ArgumentParser()
parser.add_argument('--reset', action='store_true',
                    default=False,
                    dest='reset_db',
                    help='Should we reset the database?')
parser.add_argument('--period', action='store',
                    default=30,
                    dest='period',
                    help='Period between database updates')

args = parser.parse_args()

# reset database


# instantiate classes
api = httpclient.CDayAPI('http://37.139.11.127:3000', '4')
db = database.DBConnection(DB_PATH, reset_db=args.reset_db)
# create tables
try: db.c.executescript(DDL)
except sqlite3.OperationalError: print('Tables exist, skipping creation...')
# get data from API
while True:
    datastreams = api.get_datastreams()
    datapoints = []
    for datastream in datastreams:
        datapoints.append(api.get_datapoints(str(datastream['id'])))

    # insert data into the database
    datastreams_cooked = [(x['id'], x['deployment'], x['location'], x['kind'], x['feed_id']) for x in datastreams]
    db.c.executemany('INSERT OR IGNORE INTO datastreams(ds_id, ds_deployment, ds_location, ds_kind, feed_id)'
                     'VALUES (?,?,?,?,?)', datastreams_cooked)

    datapoints_cooked = []
    for x in datapoints:
        for data in x['chunk']:
            datapoints_cooked.append((x['meta']['id'], data['timestamp'], data['value']))
    db.c.executemany('INSERT OR IGNORE INTO datapoints(ds_id, ch_timestamp, ch_value)'
                     'VALUES (?,?,?)', datapoints_cooked)
    db.c.commit()
    time.sleep(args.period)
