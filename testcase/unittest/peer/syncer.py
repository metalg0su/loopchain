from loopchain.peer import Syncer


class TestSyncer:
    def test_sync(self):
        syncer = Syncer()
        syncer.sync()
