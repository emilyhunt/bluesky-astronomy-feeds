import multiprocessing

from atproto import CAR, AtUri
from atproto.firehose import FirehoseSubscribeReposClient, parse_subscribe_repos_message
from atproto.firehose.models import MessageFrame
from atproto.xrpc_client import models
from atproto.xrpc_client.models import get_or_create, ids, is_record_type
from atproto.exceptions import FirehoseError
from atproto.xrpc_client.models.common import XrpcError

from server.data_filter import operations_callback
from server import config

import logging
import traceback


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> dict:  # noqa: C901
    operation_by_type = {
        'posts': {'created': [], 'deleted': []},
        'reposts': {'created': [], 'deleted': []},
        'likes': {'created': [], 'deleted': []},
        'follows': {'created': [], 'deleted': []},
    }

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')

        if op.action == 'update':
            # not supported yet
            continue

        if op.action == 'create':
            if not op.cid:
                continue

            create_info = {'uri': str(uri), 'cid': str(op.cid), 'author': commit.repo}

            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue

            record = get_or_create(record_raw_data, strict=False)
            if uri.collection == ids.AppBskyFeedPost and is_record_type(record, ids.AppBskyFeedPost):
                operation_by_type['posts']['created'].append({'record': record, **create_info})

            # The following types of event don't need to be tracked by the feed right now, and are removed.
            # elif uri.collection == ids.AppBskyFeedLike and is_record_type(record, ids.AppBskyFeedLike):
            #     operation_by_type['likes']['created'].append({'record': record, **create_info})
            # elif uri.collection == ids.AppBskyFeedRepost and is_record_type(record, ids.AppBskyFeedRepost):
            #     operation_by_type['reposts']['created'].append({'record': record, **create_info})
            # elif uri.collection == ids.AppBskyGraphFollow and is_record_type(record, ids.AppBskyGraphFollow):
            #     operation_by_type['follows']['created'].append({'record': record, **create_info})

        if op.action == 'delete':
            if uri.collection == ids.AppBskyFeedPost:
                operation_by_type['posts']['deleted'].append({'uri': str(uri)})

            # The following types of event don't need to be tracked by the feed right now.
            # elif uri.collection == ids.AppBskyFeedLike:
            #     operation_by_type['likes']['deleted'].append({'uri': str(uri)})
            # elif uri.collection == ids.AppBskyFeedRepost:
            #     operation_by_type['reposts']['deleted'].append({'uri': str(uri)})
            # elif uri.collection == ids.AppBskyGraphFollow:
            #     operation_by_type['follows']['deleted'].append({'uri': str(uri)})

    return operation_by_type


def worker_main(receiver) -> None:
    print("- worker process started")
    while True:
        # Wait for the multiprocessing.connection.Connection to contain something. This is blocking, btw!
        message = receiver.recv()

        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            continue

        operations_callback(_get_ops_by_type(commit))

        # ops = _get_ops_by_type(commit)
        # for post in ops['posts']['created']:
        #     post_msg = post['record'].text
        #     post_langs = post['record'].langs
        #     print(f'New post in the network! Langs: {post_langs}. Text: {post_msg}')


def run(stream_stop_event=None):
    """Continually the firehose and processes posts from on the network."""
    while True:
        # Run the firehose! (_run is the main function here)
        try:
            _run(stream_stop_event=stream_stop_event)

        # Try to handle ConsumerTooSlow exceptions. These can happen if the network is very busy or if there's a bad
        # internet connection. In this case, we'll try to just restart the firehose.
        except FirehoseError as e:
            if e.args:
                xrpc_error = e.args[0]
                if isinstance(xrpc_error, XrpcError) and xrpc_error.error == 'ConsumerTooSlow':
                    logger.warn('Reconnecting to Firehose due to ConsumerTooSlow...')
                    continue

            raise e


def _run(stream_stop_event=None):
    
    name = config.SERVICE_DID
    print(f"Running firehose for {name}")

    # This is the client used to subscribe to the firehose from the atproto lib.
    client = FirehoseSubscribeReposClient(None)

    # Setup workers to analyse and process posts (i.e. this is done as separately as possible to atproto post ingestion)
    # TODO: multi-workers are currently NOT supported! Only 1 worker is allowed at this time.
    #       There are too many things that need to be thread-safed for it to get implemented right now...
    #       Also: AWS doesn't support Queue and Pool objects, so it takes a lot more manual coding (sad.)
    receiver, pipe = multiprocessing.Pipe(duplex=False)
    worker = multiprocessing.Process(target=worker_main, args=(receiver,))

    # The handler below tells the client what to do when a new commit is encountered
    def on_message_handler(message: MessageFrame) -> None:
        pipe.send(message)

    worker.start()
    client.start(on_message_handler)
