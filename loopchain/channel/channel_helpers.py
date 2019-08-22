from loopchain import configure as conf
from loopchain import utils
from loopchain.channel.channel_property import ChannelProperty

from loopchain.utils.message_queue import StubCollection


def shutdown_peer(**kwargs) -> None:
    utils.logger.debug(f"channel_service:shutdown_peer")
    StubCollection().peer_stub.sync_task().stop(message=kwargs['message'])


def get_channel_option() -> dict:
    return conf.CHANNEL_OPTION[ChannelProperty().name]
