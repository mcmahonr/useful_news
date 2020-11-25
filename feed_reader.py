import requests
import untangle
import json
import os

from es_manager import send_doc, get_all_articles, es_post


def strip_protocol(link_url):
    if link_url.startswith('http'):
        if link_url.startswith('https'):
            return link_url[12:]
        else:
            return link_url[11:]


def tweet_search(q, next_token=None):
    twitter_bearer = f'Bearer {os.environ.get("twitter_bearer")}'
    search_url = 'https://api.twitter.com/2/tweets/search/recent'
    headers = {'Authorization': twitter_bearer}
    params = {'query': q, 'max_results': 100}
    if next_token:
        params['next_token'] = next_token

    r = requests.get(search_url, headers=headers, params=params)
    if r.status_code != 200:
        if r.status_code == 429:
            print(f'Rate Limit Hit. Resets in {r.headers}')
        raise RuntimeError(f'Unable to get search results: {r.status_code} {r.text}')

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


def get_xml(feed_url):
    r = requests.get(feed_url)
    if r.status_code not in (200, 201, 202, 203, 204):
        raise RuntimeError(f'Unable to connect to RSS feed: {r.status_code} {r.text}')

    return r.content.decode('utf-8')


def get_author(article):
    try:
        return article.author.cdata
    except AttributeError:
        try:
            return article.dc_creator.cdata
        except AttributeError:
            return None


def get_media(article):
    try:
        return article.media_content._attributes.get('url')
    except AttributeError:
        try:
            return article.enclosure._attributes.get('url')
        except AttributeError:
            return None


def get_description(article):
    try:
        return article.description.cdata
    except AttributeError:
        try:
            return article.summary.cdata
        except AttributeError:
            return None


def get_pub_date(article):
    try:
        return article.pubDate.cdata
    except AttributeError:
        return None


def parse_article(rss_item, feed_name):
    return {'title': rss_item.title.cdata,
            'description': get_description(rss_item),
            'link': rss_item.link.cdata,
            'media_url': get_media(rss_item),
            'author': get_author(rss_item),
            'pub_date': get_pub_date(rss_item),
            'feed_name': feed_name,
            'tweet_count': 0}


def get_articles(parsed_feed, feed_name):
    try:
        return [parse_article(article, feed_name) for article in parsed_feed.item]
    except AttributeError:
        return [parse_article(article, feed_name) for article in parsed_feed.entry]


def untangle_feed(feed_url):
    xml_content = get_xml(feed_url)
    return untangle.parse(xml_content)


def get_feed_info(feed_url):
    rss_feed = untangle_feed(feed_url)

    return {'title': rss_feed.channel.title.cdata,
            'description': get_description(rss_feed.channel),
            'link': rss_feed.link.cdata}


def get_feed(feed_url, feed_name):
    rss_feed = untangle_feed(feed_url)
    parsed_rss = rss_feed.rss.channel
    return get_articles(parsed_rss, feed_name)


def create_articles(feed_url, feed_name):
    articles = get_feed(feed_url, feed_name)
    for article in articles:
        send_doc('articles', json.dumps(article))


def update_tweet_count():
    articles = get_all_articles()
    for article in articles:
        link = article.get('_source').get('link')
        doc_id = article.get('_id')
        tweet_count = get_tweet_count(link)
        params = {'script': {"source": "ctx._source.tweet_count += params.tweet_count",
                             "lang": "painless",
                             "params": {"tweet_count": tweet_count}}}

        es_post(f'articles/_update/{doc_id}', data=json.dumps(params))

