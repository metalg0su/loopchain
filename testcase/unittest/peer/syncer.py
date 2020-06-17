from loopchain.peer.syncer import Syncer


class TestSyncer:
    def test_sync(self):
        syncer = Syncer()
        syncer.sync()
