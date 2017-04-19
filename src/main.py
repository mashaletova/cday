import requests
import sqlite3
import os
# import click


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


class HTTPClient:
    def __init__(self):
        self._s = requests.Session()
        self._s.headers.update({'Accept': 'application/json'})

    def get(self, url, json=False):
        return self._s.get(url, json=json)


class CDayAPI(HTTPClient):
    def __init__(self, url, team_id):
        super().__init__()
        self.url = url
        self.team_id = team_id

    def get_datastreams(self):
        return self.get('{}/feeds/{}/datastreams'.format(self.url, self.team_id)).json()

    def get_datapoints(self, datastream_id):
        return self.get('{}/feeds/{}/datastreams/{}/datapoints'.format(self.url, self.team_id, datastream_id)).json()


class DBConnection:
    def __init__(self, db_path):
        try: os.unlink(db_path)
        except FileNotFoundError: pass
        self.c = sqlite3.connect(db_path)


if __name__ == '__main__':
    # instantiate classes
    api = CDayAPI('http://37.139.11.127:3000', '4')
    db = DBConnection('../iot.db')
    # get data from API
    datastreams = api.get_datastreams()
    datapoints = []
    for datastream in datastreams:
        datapoints.append(api.get_datapoints(str(datastream['id'])))
    # create tables
    db.c.executescript(DDL)
    # insert data into the database
    datastreams_cooked = [(x['id'], x['deployment'], x['location'], x['kind'], x['feed_id']) for x in datastreams]
    db.c.executemany('INSERT INTO datastreams(ds_id, ds_deployment, ds_location, ds_kind, feed_id)'
                     'VALUES (?,?,?,?,?)', datastreams_cooked)

    datapoints_cooked = []
    for x in datapoints:
        for data in x['chunk']:
            datapoints_cooked.append((x['meta']['id'], data['timestamp'], data['value']))
    db.c.executemany('INSERT INTO datapoints(ds_id, ch_timestamp, ch_value)'
                     'VALUES (?,?,?)', datapoints_cooked)

    db.c.commit()
    pass