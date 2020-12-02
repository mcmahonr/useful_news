import requests
import untangle
import json
import dateparser

import es_manager
from twitter_manager import get_tweet_count


def get_xml(feed_url):
    r = requests.get(feed_url)
    if r.status_code not in (200, 201, 202, 203, 204):
        raise RuntimeError(f'Unable to connect to RSS feed: {feed_url}\n{r.status_code}\n{r.text}')
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
        dt_str = article.pubDate.cdata
        pub_date = dateparser.parse(dt_str)
        return pub_date
    except AttributeError:
        return None


def parse_article(rss_item, feed_name):
    pub_date = get_pub_date(rss_item)
    if pub_date:
        pub_date = pub_date.strftime('%Y-%m-%dT%H:%M:%S%z')
    return {'title': rss_item.title.cdata,
            'description': get_description(rss_item),
            'link': rss_item.link.cdata,
            'media_url': get_media(rss_item),
            'author': get_author(rss_item),
            'pub_date': pub_date,
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
        es_manager.send_doc('articles', json.dumps(article))


def update_tweet_count():
    articles = es_manager.get_all_articles()
    for article in articles:
        link = article.get('_source').get('link')
        doc_id = article.get('_id')
        tweet_count = get_tweet_count(link)
        print(f'{article.get("_source").get("feed_name")}\n{link}\n{tweet_count}\n{"*" * len(link)}')
        params = {'script': {"source": "ctx._source.tweet_count += params.tweet_count",
                             "lang": "painless",
                             "params": {"tweet_count": tweet_count}}}

        es_manager.es_post(f'articles/_update/{doc_id}', data=json.dumps(params))


def sync_feeds(category=None):
    feeds = es_manager.get_feeds(category=category)
    for feed in feeds:
        create_articles(feed.get('url'), feed.get('name'))


if __name__ == '__main__':
    sync_feeds()
