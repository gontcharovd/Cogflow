import requests

from datetime import datetime
from airflow.hooks.base_hook import BaseHook


class TimeSeriesHook(BaseHook):
    VERSION = 'v1'
    PROJECT = 'publicdata'
    RESOURCE = 'timeseries'

    def __init__(self, conn_id):
        super().__init__(source=None)
        self._conn_id = conn_id
        self._session = None
        self._base_url = None

    def get_conn(self):
        if self._session is None:
            config = self.get_connection(self._conn_id)
            self._session = requests.Session()
            if config.password:
                self._session.headers['api-key'] = config.password
            self._base_url = (
                f'{config.host}/{self.VERSION}/projects/'
                f'{self.PROJECT}/{self.RESOURCE}'
            )
        return self._session, self._base_url

    def _create_body(
        self,
        ids,
        start_date,
        end_date,
        aggregates=['average'],
        granularity='5m',
        limit=5000,
        **kwargs
    ):
        """ Create the request body sent to the Cognite REST API. """
        body = {
            'items': [],
            'start': start_date,
            'end': end_date,
            'limit': limit,
            'aggregates': aggregates,
            'granularity': granularity,
            'includeOutsidePoints': False,
            'ignoreUnknownIds': False
        }

        for id in ids:
            body['items'].append({'id': str(id)})

        return body

    def _convert_to_ms(self, timestamp):
        """ Convert YYYY-MM-DD HH:MM:SS to ms since unix epoch. """
        seconds = datetime.fromisoformat(timestamp).timestamp()
        milliseconds = seconds * 1000
        return int(milliseconds)

    def get_time_series(self, ids, start_date, end_date, **kwargs):
        """ Fetch the data of given time series between the given dates.

        Args:
            ids (list): Cognite sensor asset ids as integers.
            start_date (str): Start date to start fetching data from.
                Expected format is YYYY-MM-DD HH:MM:SS
            end_date (str): End date to start fetching data from.
                Expected format is YYYY-MM-DD HH:MM:SS.
            kwargs: Optional arguments passed to the Cognite REST API.
                Refer to: https://docs.cognite.com/api/v1/#operation/getMultiTimeSeriesDatapoints # noqa
        """
        session, base_url = self.get_conn()

        body = self._create_body(
            ids=ids,
            start_date=self._convert_to_ms(start_date),
            end_date=self._convert_to_ms(end_date),
            **kwargs
        )

        response = session.post(
            url=f'{base_url}/data/list',
            json=body
        )
        response.raise_for_status()

        return response.content
