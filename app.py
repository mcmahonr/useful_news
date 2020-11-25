import json
import datetime
import requests
import os
from flask import Flask, render_template, request, flash
from feed_reader import get_feed
from es_manager import send_doc

app = Flask(__name__)
app.secret_key = os.environ.get('app_secret')


def build_feed_json(feed_data):
    is_frontpage = False
    if feed_data.get('in_frontpage'):
        is_frontpage = True
    feed = {'name': feed_data.get('feed_name'),
            'url': feed_data.get('feed_url'),
            'categories': feed_data.getlist('category'),
            'is_frontpage': is_frontpage,
            'added_on': datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}

    return json.dumps(feed)


@app.route('/')
def front_page():
    return render_template('index.html', title='Front Page', articles=articles)


@app.route('/<fltr>')
def news_filter(fltr):
    return render_template('index.html', title=fltr)


@app.route('/feeds', methods=['GET', 'POST'])
def feeds():
    if request.method == 'POST':
        feed_json = build_feed_json(request.form)
        success, status_code = send_doc('feed', feed_json)
        if not success:
            flash(f'Unable to add feed. ES status code: {status_code}')
        else:
            flash('Succesfully added new feed')

    return render_template('feeds.html')


@app.route('/cors/<path:url>')
def cors_proxy(url):
    r = requests.get(url)
    return r.content


if __name__ == '__main__':
    app.run()
