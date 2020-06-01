from typing import Optional, Sequence

from loopchain import utils
from loopchain.blockchain.blocks import BlockHeader
from loopchain.blockchain.types import Hash32, ExternalAddress
from loopchain.channel.channel_property import ChannelProperty


class RepGetter:
    @staticmethod
    def get_next_rep_in_reps(rep, reps: Sequence[ExternalAddress]):
        try:
            return reps[reps.index(rep) + 1]
        except IndexError:
            return reps[0]
        except ValueError:
            utils.logger.debug(f"rep({rep}) not in reps({[str(rep) for rep in reps]})")
            return None

    @staticmethod
    def get_next_rep_string_in_reps(rep, reps: Sequence[ExternalAddress]) -> Optional[str]:
        try:
            return reps[reps.index(rep) + 1].hex_hx()
        except IndexError:
            return reps[0].hex_hx()
        except ValueError:
            utils.logger.debug(f"rep({rep}) not in reps({[str(rep) for rep in reps]})")
            return None

    @staticmethod
    def get_reps_hash_by_header(header: BlockHeader) -> Hash32:
        try:
            roothash = header.reps_hash
            if not roothash:
                raise AttributeError
        except AttributeError:
            roothash = ChannelProperty().crep_root_hash
        return roothash
