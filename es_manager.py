import requests
import json

es_url = 'http://localhost:9200'


def es_post(url, **kwargs):
    r = es_request('post', url, **kwargs)
    return r


def es_get(url, **kwargs):
    r = es_request('get', url, **kwargs)
    return r


def es_request(m, url, **kwargs):
    headers = {'Content-Type': 'application/json', 'Accept': 'appliction/json'}
    method = {'get': requests.get, 'post': requests.post}
    r = method[m](f'{es_url}/{url}', headers=headers, **kwargs)
    if r.status_code not in (200, 201, 202, 203, 204, 205):
        raise RuntimeError(f'Unable to get feed results: {r.status_code} {r.text}')
    return r


def send_doc(index, doc):
    r = es_post(f'{index}/_doc/', data=doc)
    return r


def get_feeds(category=None):
    params = {"query": {"bool": {"must": []}}}
    if category:
        params = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "categories.keywords": category
                            }
                        }
                    ]
                }
            }
        }

    r = es_get('feed/_search', data=json.dumps(params))
    results = r.json().get('hits').get('hits')
    return [feed.get('_source') for feed in results]


def get_all_articles(feed=None):
    params = {"query": {"bool": {"must": []}}}
    if feed:
        params = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "feed_name.keywords": feed
                            }
                        }
                    ]
                }
            }
        }

    r = es_get('articles/_search', data=json.dumps(params))
    return r.json().get('hits').get('hits')


