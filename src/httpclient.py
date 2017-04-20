import requests

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
