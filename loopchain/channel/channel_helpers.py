from loopchain import utils
from loopchain.utils.message_queue import StubCollection


def shutdown_peer(**kwargs):
    utils.logger.debug(f"channel_service:shutdown_peer")
    StubCollection().peer_stub.sync_task().stop(message=kwargs['message'])

