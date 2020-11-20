import os
import requests

from dotenv import load_dotenv

VERSION = 'v1'
PROJECT = 'publicdata'
RESOURCE = 'timeseries'


def get_session():
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


def get_request():
    session, base_url = get_session()
    url = f'{base_url}'
    response = session.get(url)
    response.raise_for_status()
    return response.json()


def post_request(body):
    session, base_url = get_session()
    url = f'{base_url}/byids'
    # data = {'id': "2251496734604682"}
    response = session.post(url, json=body)
    response.raise_for_status()
    return response.json()


def create_body(ts_ids):
    body = {
        "items": [],
        "ignoreUnknownIds": False
    }
    for id in ts_ids:
        body['items'].append({'id': str(id)})
    return body


if __name__ == '__main__':
    body = create_body([622801209626283, 2251496734604682, 3262727548303974])
    request = post_request(body)
    print(request)
