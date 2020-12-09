import os
import time
import requests


def strip_protocol(link_url):
    if link_url.startswith('http'):
        if link_url.startswith('https'):
            return link_url[12:]
        else:
            return link_url[11:]


def twitter_request(url, method, api_ver, **kwargs):
    base_url = 'https://api.twitter.com'
    t_url = f'{base_url}/{url}'
    bearer = f'Bearer {os.environ.get("twitter_bearer")}'
    oauth = os.environ.get('twitter_oauth')
    if api_ver >= 2:
        auth = bearer
    else:
        auth = oauth
    headers = {'Authorization': auth}
    methods = {'get': requests.get, 'post': requests.post, 'put': requests.put}
    req_func = methods[method]
    return req_func(t_url, headers=headers, **kwargs)


def tweet_search_request(params):
    search_url = '2/tweets/search/recent'
    r = twitter_request(search_url, 'get', params=params)
    if r.status_code == 429:
        try:
            resets_time = int(r.headers.get('x-rate-limit-reset'))
        except ValueError:
            resets_time = 30
        print(f'Rate Limit Hit. Resets in {resets_time - time.time()}')
        time.sleep(resets_time - time.time())
        return tweet_search_request(params)
    if r.status_code == 400 and 'is too long' in r.text:
        params['query'] = params['query'].split('/')[-1]
        return tweet_search_request(params)
    return r


def tweet_search(q, next_token=None):
    params = {'query': q, 'max_results': 100}
    if next_token:
        params['next_token'] = next_token
    r = tweet_search_request(params)
    if r.status_code != 200:
        print(f'Unable to get search results, returning none: {r.status_code} {r.text}')
        return 0, None
    results = r.json().get('meta')
    return results.get('result_count'), results.get('next_token')


def get_tweet_count(link_url):
    link_url = strip_protocol(link_url)
    count = 0
    next_token = None
    more_results = True
    while more_results:
        search_count, next_token = tweet_search(link_url, next_token=next_token)
        count += search_count
        if not next_token:
            more_results = False
    return count


def get_lists(user_id=None):
    params = {'user_id': user_id}
    url = '1.1/lists/list.json'
    r = twitter_request(url, 'get', 1.1, params=params)
    return r.json()


def get_list_statuses(list_id, since_id=None, get_rts=True, get_entities=True):
    params = {'list_id': list_id, 'since_id': since_id, 'include_entities': get_entities, 'include_rts': get_rts}
    url = '1.1/lists/statuses.json'
    return twitter_request(url, 'get', 1.1, params=params)
