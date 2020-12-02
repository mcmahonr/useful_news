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
        params = {"query": {"bool": {"must": [{"match": {"categories.keyword": category}}],
                                     "must_not": [],
                                     "should": []}},
                  "from": 0,
                  "size": 250,
                  "sort": [],
                  "aggs": {}}

    r = es_get('feed/_search', data=json.dumps(params))
    results = r.json().get('hits').get('hits')
    return [feed.get('_source') for feed in results]


def get_all_articles(feed=None):
    params = {"query": {"bool": {"must": []}},
              "from": 0,
              "size": 100,
              "sort": [{'tweet_count': {'order': 'desc'}},
                       {'pub_date': {'order': 'desc'}}],
              "aggs": {}}

    if feed:
        params = {"query": {"bool": {"must": [{"match": {"feed_name": "SANS Newsbites"}}]}},
                  "from": 0,
                  "size": 100,
                  "sort": [{'tweet_count': {'order': 'desc'}},
                           {'pub_date': {'order': 'desc'}}],
                  "aggs": {}}
    result_count = 1
    processed_results = 0
    results = []

    while processed_results < result_count:
        r = es_get('articles/_search', data=json.dumps(params))
        hits = r.json().get('hits')
        articles = hits.get('hits')
        result_count = hits.get('total').get('value')
        if not result_count:
            break
        for article in articles:
            results.append(article)
        result_length = len(articles)
        params['from'] += result_length
        processed_results += result_length
    return results


def delete_index_docs(index):
    params = {'query': {'match_all': {}}}
    return es_post(f'{index}/_delete_by_query', data=json.dumps(params))
