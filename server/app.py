import sys
import signal
import threading

from .database import db

from server import config
from server import data_stream

from flask import Flask, jsonify, request

from server.algos import algos
from server.data_filter import operations_callback
from server.accounts import refresh_valid_accounts


app = Flask(__name__)


# All threads:
stream_stop_event = threading.Event()

account_thread = threading.Thread(
    target=refresh_valid_accounts, args=(stream_stop_event,)
)
account_thread.start()

stream_thread = threading.Thread(
    target=data_stream.run, args=(config.SERVICE_DID, operations_callback, stream_stop_event,)
)
stream_thread.start()


def sigint_handler(*_):
    print('Stopping data stream...')
    stream_stop_event.set()
    sys.exit(0)


signal.signal(signal.SIGINT, sigint_handler)


# This hook ensures that a connection is opened to handle any queries
# generated by the request.
@app.before_request
def _db_connect():
    if db.is_closed():
        db.connect()

# This hook ensures that the connection is closed when we've finished
# processing the request.
# Todo split app into multiple components so that this can be done ok
# @app.teardown_request
# def _db_close(exc):
#     if not db.is_closed():
#         db.close()


@app.route('/')
def index():
    return 'Homepage of astronomy feeds on Bluesky. To be able to post in a feed, visit https://signup.astronomy.blue'


@app.route('/.well-known/did.json', methods=['GET'])
def did_json():
    if not config.SERVICE_DID.endswith(config.HOSTNAME):
        return '', 404

    return jsonify({
        '@context': ['https://www.w3.org/ns/did/v1'],
        'id': config.SERVICE_DID,
        'service': [
            {
                'id': '#bsky_fg',
                'type': 'BskyFeedGenerator',
                'serviceEndpoint': f'https://{config.HOSTNAME}'
            }
        ]
    })


@app.route('/xrpc/app.bsky.feed.describeFeedGenerator', methods=['GET'])
def describe_feed_generator():
    feeds = [{'uri': uri} for uri in algos.keys()]
    response = {
        'encoding': 'application/json',
        'body': {
            'did': config.SERVICE_DID,
            'feeds': feeds
        }
    }
    return jsonify(response)


@app.route('/xrpc/app.bsky.feed.getFeedSkeleton', methods=['GET'])
def get_feed_skeleton():
    feed = request.args.get('feed', default=None, type=str)
    algo = algos.get(feed)
    if not algo:
        return 'Unsupported algorithm', 400

    try:
        cursor = request.args.get('cursor', default=None, type=str)
        limit = request.args.get('limit', default=20, type=int)
        body = algo(cursor, limit)
    except ValueError:
        return 'Malformed cursor', 400

    return jsonify(body)
