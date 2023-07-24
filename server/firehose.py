import typing as t

from atproto import models
from atproto.firehose import FirehoseSubscribeReposClient, parse_subscribe_repos_message

from server.data_filter import ParallelDataFilter
from server import config

from multiformats import CID as _CID

import logging
import traceback
import pickle

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

if t.TYPE_CHECKING:
    from atproto.firehose import MessageFrame


def run():
    name = config.SERVICE_DID
    print(f"Running firehose from HOSTNAME {name}")

    # state = SubscriptionState.select(SubscriptionState.service == name).first()
    # params = None
    # if state:
    #     params = models.ComAtprotoSyncSubscribeRepos.Params(cursor=state.cursor)

    client = FirehoseSubscribeReposClient(None)
    data_filter = ParallelDataFilter()
    data_filter.start()

    # if not state:
    #     SubscriptionState.create(service=name, cursor=0)    

    def on_message_handler(message: 'MessageFrame') -> None:
        data_filter.add_message(pickle.dumps(message))

    def on_error_callback(e):
        logger.info(f"Exception encountered in on_message_handler! {e}")
        traceback.print_exc()

        client.stop()
        data_filter.stop()

    client.start(on_message_handler, on_error_callback)
