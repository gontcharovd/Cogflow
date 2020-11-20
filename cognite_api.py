import os
import requests
import pandas as pd

from dotenv import load_dotenv

VERSION = 'v1'
PROJECT = 'publicdata'
RESOURCE = 'timeseries'


def _get_session():
    """Builds a requests Session for the Movielens API."""
    session = requests.Session()
    load_dotenv()
    api_key = os.environ.get('API_KEY')
    session.headers['api-key'] = api_key
    base_url = (
        'https://api.cognitedata.com/api/'
        f'{VERSION}/projects/{PROJECT}/{RESOURCE}'
    )
    return session, base_url


def _get_data(session, url, body):
    response = session.post(url, json=body)
    response.raise_for_status()
    return response.json()


def _create_body(
        ids,
        start_date,
        end_date,
        aggregates=['average', 'min'],
        granularity='5m',
        limit=10,
        **kwargs
):
    body = {
        'items': [],
        'start': start_date,
        'end': end_date,
        'limit': int(limit),
        'aggregates': aggregates,
        'granularity': granularity,
        'includeOutsidePoints': False,
        'ignoreUnknownIds': False
    }
    for id in ids:
        body['items'].append({'id': str(id)})
    return body


def fetch_data(ids, start_date, end_date, **kwargs):
    session, base_url = _get_session()
    body = _create_body(ids, start_date, end_date, **kwargs)
    response = _get_data(
        session=session,
        url=base_url + '/data/list',
        body=body
    )
    all_df = pd.DataFrame()
    for data in response.get('items'):
        df = pd.DataFrame.from_dict(data.get('datapoints'))
        df.insert(1, 'id', data.get('id'))
        all_df = pd.concat([all_df, df], axis=0)
    return all_df


if __name__ == '__main__':
    data = fetch_data(
        ids=[622801209626283, 3262727548303974],
        start_date='2w-ago',
        end_date='now',
        granularity='5m',
        limit=5000,
        aggregates=['average']
    )
    print(data)
