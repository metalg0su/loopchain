import multiprocessing as mp
from typing import List

import pytest

from loopchain import configure_default as conf
from loopchain.baseservice import ObjectManager, BroadcastCommand, TimerService, Timer
from loopchain.baseservice.broadcast_scheduler import BroadcastScheduler, BroadcastSchedulerFactory
from loopchain.baseservice.broadcast_scheduler import _Broadcaster, _BroadcastSchedulerMp, _BroadcastSchedulerThread
from loopchain.baseservice.tx_item_helper import TxItem
from loopchain.blockchain.transactions import TransactionVersioner
from loopchain.protos import loopchain_pb2

CHANNEL_NAME = "icon_dex"
PEER_TARGET = "peer_target"


@pytest.fixture
def bc() -> _Broadcaster:
    """Init and start _Broadcaster"""
    channel = "chann"
    self_target = "peer_target"

    broadcaster = _Broadcaster(channel, self_target)
    broadcaster.start()

    yield broadcaster

    broadcaster.stop()


@pytest.fixture
def bc_scheduler() -> BroadcastScheduler:
    """Init and start BroadcastScheduler."""

    bc_scheduler = BroadcastSchedulerFactory.new(
        channel=CHANNEL_NAME, self_target=PEER_TARGET, is_multiprocessing=True
    )

    # Make broadcast_queue be public of BroadcastScheduler, so that dealing with it in tests easily.
    queue_attr_name = f"{bc_scheduler.__class__.__name__}__broadcast_queue"
    broadcast_queue = getattr(bc_scheduler, queue_attr_name)
    bc_scheduler.broadcast_queue = broadcast_queue
    assert bc_scheduler.broadcast_queue.empty()

    bc_scheduler.start()

    yield bc_scheduler

    bc_scheduler.stop()
    bc_scheduler.wait()


class TestBroadcaster:
    @pytest.fixture
    def mocking_(self, mocker):
        """Mocking handlers.

        Do before init Broadcaster or BroadcastSchedulers!
        """
        handler_attrs = [handler for handler in dir(_Broadcaster) if "__handler_" in handler]
        for handler_attr in handler_attrs:
            mock_handler = mocker.MagicMock()
            mocker.patch.object(_Broadcaster, handler_attr, mock_handler)

    def test_call_handler_broadcast(self, bc, mocker):
        method_name = "method_name"
        method_param = "method_param"
        method_kwparam = {"retry_times": 10, "timeout": 10}
        broadcast_param = (method_name, method_param, method_kwparam)

        bc._Broadcaster__broadcast_run = mocker.MagicMock()
        bc._Broadcaster__handler_broadcast(broadcast_param)

        bc._Broadcaster__broadcast_run.assert_called_with(method_name, method_param, **method_kwparam)

    def test_call_handler_send_to_single_target(self, bc, mocker):
        method_name = "method_name"
        method_param = "method_param"
        target = "target"
        param = (method_name, method_param, target)

        bc._Broadcaster__call_async_to_target = mocker.MagicMock()
        bc._Broadcaster__handler_send_to_single_target(param)

        bc._Broadcaster__call_async_to_target.assert_called_with(target, method_name, method_param, True, 0, conf.GRPC_TIMEOUT_BROADCAST_RETRY)

    @pytest.mark.parametrize("command, params", [
        (BroadcastCommand.CREATE_TX, ("tx", "tx_versioner")),
        (BroadcastCommand.UPDATE_AUDIENCE, ["p2pEndpoint1:port", "p2pEndpoint2:port"]),
        (BroadcastCommand.BROADCAST, ("method", "method_param", "kwargs")),
        (BroadcastCommand.SEND_TO_SINGLE_TARGET, ("method", "method_param", "kwargs")),
    ])
    def test_handle_command_passes_param_to_deserving_handler(self, mocking_, bc, command: str, params):
        bc.handle_command(command, params)

        target_handler_attr = f"{bc.__class__.__name__}__handler_{command.lower()}"
        target_handler = getattr(bc, target_handler_attr)

        target_handler.assert_called_with(params)

    def test_handler_update_audience(self, bc, mocker):
        # Mock stubmanager to avoid actual grpc stub initialized.
        mocker.patch("loopchain.baseservice.StubManager")

        audience_targets = [f"endpoint:{i}" for i in range(5)]

        assert not bc._Broadcaster__audience

        bc._Broadcaster__handler_update_audience(audience_targets)
        assert bc._Broadcaster__audience
        assert len(bc._Broadcaster__audience) == len(audience_targets)

    def test_handler_update_audience_twice_and_updated_with_new_audiences(self, bc, mocker):
        # Mock stubmanager to avoid actual grpc stub initialized.
        mocker.patch("loopchain.baseservice.StubManager")

        expected_audience_count = 5
        new_audience_start_at = 2
        new_audience_end_at = new_audience_start_at + expected_audience_count

        orig_audience_targets = [f"endpoint:{i}" for i in range(expected_audience_count)]
        new_audience_targets = [f"endpoint:{i}" for i in range(new_audience_start_at, new_audience_end_at)]
        assert len(orig_audience_targets) == len(new_audience_targets)  # expected 5 audiences

        audience_list: dict = bc._Broadcaster__audience
        assert not audience_list

        # Add first time
        bc._Broadcaster__handler_update_audience(orig_audience_targets)
        assert audience_list
        assert len(audience_list) == len(orig_audience_targets)

        for audience in orig_audience_targets:
            assert audience in audience_list.keys()
        for common_audience in new_audience_targets[:new_audience_start_at]:
            assert common_audience in audience_list.keys()
        for not_yet_updated_audience in new_audience_targets[new_audience_start_at+1:]:
            assert not_yet_updated_audience not in audience_list.keys()

        # Add second time
        bc._Broadcaster__handler_update_audience(new_audience_targets)
        assert len(audience_list) == len(new_audience_targets)

        for removed_audience in orig_audience_targets[:new_audience_start_at]:
            assert removed_audience not in audience_list.keys()
        for updated_audience in new_audience_targets:
            assert updated_audience in audience_list.keys()

    def test_send_tx_in_timer_starts_add_tx_timer(self, bc, tx):
        channel_name = "icon_dex"
        tx_versioner = TransactionVersioner()

        tx_param = tx, tx_versioner
        tx_item = TxItem.create_tx_item(tx_param, channel_name)

        # Raises Exception `Empty` if nothing exists in queue.Queue
        with pytest.raises(Exception):
            bc.stored_tx.get(block=False)

        bc._Broadcaster__send_tx_in_timer(tx_item=tx_item)
        timer_serivce: TimerService = bc._Broadcaster__timer_service
        timer: Timer = timer_serivce.get_timer(TimerService.TIMER_KEY_ADD_TX)
        assert bc.stored_tx.get(block=False) == tx_item
        assert timer.duration == conf.SEND_TX_LIST_DURATION

    def test_make_tx_list_message_if_no_tx_item(self, bc, tx):
        assert bc.stored_tx.empty()
        remains, message = bc._Broadcaster__make_tx_list_message()

        assert message.channel == bc._Broadcaster__channel
        assert not message.tx_list

    def test_make_tx_list_message(self, bc, tx, mocker):
        channel_name = "icon_dex"
        tx_versioner = TransactionVersioner()

        tx_param = tx, tx_versioner
        tx_item = TxItem.create_tx_item(tx_param, channel_name)

        bc.stored_tx.put(tx_item)
        assert not tx_item >= conf.MAX_TX_SIZE_IN_BLOCK

        mocker.patch.object(loopchain_pb2, "TxSendList")
        bc._Broadcaster__make_tx_list_message()


class TestBroadcastScheduler:
    @pytest.mark.parametrize("is_multiprocessing", [True, False])
    def test_factory_returns_correct_scheduler(self, is_multiprocessing):
        bc_scheduler: BroadcastScheduler = BroadcastSchedulerFactory.new(
            channel="chann", is_multiprocessing=is_multiprocessing
        )

        if is_multiprocessing:
            assert isinstance(bc_scheduler, _BroadcastSchedulerMp)
        else:
            assert isinstance(bc_scheduler, _BroadcastSchedulerThread)

    @pytest.mark.parametrize("command, params", [
        (BroadcastCommand.CREATE_TX, ("tx", "tx_versioner")),
        (BroadcastCommand.UPDATE_AUDIENCE, ["p2pEndpoint1:port", "p2pEndpoint2:port"]),
        (BroadcastCommand.BROADCAST, ("method", "method_param", "kwargs")),
        (BroadcastCommand.SEND_TO_SINGLE_TARGET, ("method", "method_param", "kwargs")),
    ])
    def test_schedule_job(self, bc_scheduler, command, params):
        broadcast_queue: mp.Queue = bc_scheduler.broadcast_queue

        bc_scheduler.schedule_job(command, params)
        job = broadcast_queue.get()

        assert job == (command, params)

    @pytest.mark.xfail(reason="commands condition in `add_schedule_lister never reaches. It always True or raises TypeError if `commands` param not exists.")
    def test_add_schedule_listner_without_command(self, bc_scheduler, mocker):
        listeners = bc_scheduler._BroadcastScheduler__schedule_listeners
        assert not listeners

        mock_callback = mocker.MagicMock()

        with pytest.raises(ValueError, match="commands parameter is required"):
            bc_scheduler.add_schedule_listener(mock_callback, commands=(None, ))

    @pytest.mark.parametrize("command, params", [
        (BroadcastCommand.CREATE_TX, ("tx", "tx_versioner")),
        (BroadcastCommand.UPDATE_AUDIENCE, ["p2pEndpoint1:port", "p2pEndpoint2:port"]),
        (BroadcastCommand.BROADCAST, ("method", "method_param", "kwargs")),
        (BroadcastCommand.SEND_TO_SINGLE_TARGET, ("method", "method_param", "kwargs")),
    ])
    def test_listener_called_when_job_scheduled(self, bc_scheduler, command, params, mocker):
        listeners = bc_scheduler._BroadcastScheduler__schedule_listeners
        assert not listeners

        mock_callback = mocker.MagicMock()
        bc_scheduler.add_schedule_listener(mock_callback, commands=(command, ))

        assert len(listeners) == 1
        assert not mock_callback.called

        bc_scheduler.schedule_job(command, params)
        mock_callback.assert_called_with(command, params)

    def test_listener_not_called_when_another_job_scheduled(self, bc_scheduler, mocker):
        listeners = bc_scheduler._BroadcastScheduler__schedule_listeners
        assert not listeners

        mock_callback = mocker.MagicMock()
        bc_scheduler.add_schedule_listener(mock_callback, commands=(BroadcastCommand.UPDATE_AUDIENCE,
                      BroadcastCommand.BROADCAST,
                      BroadcastCommand.SEND_TO_SINGLE_TARGET))

        assert len(listeners) == 3
        assert not mock_callback.called

        command = BroadcastCommand.CREATE_TX
        param = ("tx", "tx_versioner")

        bc_scheduler.schedule_job(command, param)
        assert not mock_callback.called

    @pytest.fixture
    def mocking_(self, mocker):
        # HARD JOURNEY TO MOCK, REFACTORING NEEDED!!!!!!
        from loopchain.blockchain.blockchain import BlockChain
        from loopchain.peer.block_manager import BlockManager
        from loopchain.channel.channel_service import ChannelService

        update_reps = [
            {"p2pEndpoint": "endpoint:0"},
            {"p2pEndpoint": "endpoint:1"},
            {"p2pEndpoint": "endpoint:2"},
        ]

        mock_channel_service = mocker.MagicMock(spec=ChannelService)
        mock_blockmanager = mocker.MagicMock(spec=BlockManager)
        mock_blockchain = mocker.MagicMock(spec=BlockChain)
        mock_blockchain.find_preps_by_roothash.return_value = update_reps

        mock_blockmanager.blockchain = mock_blockchain
        mock_channel_service.block_manager = mock_blockmanager

        ObjectManager().channel_service = mock_channel_service

    def test_update_audience_with_valid_reps_hash(self, mocking_, bc_scheduler: BroadcastScheduler):
        fake_update_reps: List[dict] = ObjectManager().channel_service.block_manager.blockchain.find_preps_targets_by_roothash("")
        expected_reps = [rep["p2pEndpoint"] for rep in fake_update_reps]

        # Mocking end. Actual Test
        expected_audience_reps_hash = "reps_hash"
        bc_scheduler._update_audience(reps_hash=expected_audience_reps_hash)

        assert bc_scheduler._BroadcastScheduler__audience_reps_hash == expected_audience_reps_hash
        assert bc_scheduler.broadcast_queue.get() == (BroadcastCommand.UPDATE_AUDIENCE, expected_reps)

    def test_schedule_broadcast(self, mocking_, bc_scheduler: BroadcastScheduler, mocker):
        fake_reps_hash = "reps_hash"
        method_name = "AnnounceUnconfirmedBlock"
        method_param = "loopchain_pb2.BlockSend(some_params)"

        bc_scheduler._BroadcastScheduler__audience_reps_hash = fake_reps_hash
        bc_scheduler.schedule_broadcast(method_name=method_name, method_param=method_param, reps_hash=fake_reps_hash)

        assert bc_scheduler.broadcast_queue.get() == (BroadcastCommand.BROADCAST, (method_name, method_param, mocker.ANY))

    def test_schedule_broadcast_and_update_audience_if_differs_reps_hash(self, mocking_, bc_scheduler: BroadcastScheduler, mocker):
        fake_reps_hash = "aaa"
        method_name = "AnnounceUnconfirmedBlock"
        method_param = "loopchain_pb2.BlockSend(some_params)"

        bc_scheduler._BroadcastScheduler__audience_reps_hash = None
        bc_scheduler._put_command = mocker.MagicMock()
        bc_scheduler.schedule_broadcast(method_name=method_name, method_param=method_param, reps_hash=fake_reps_hash)

        assert bc_scheduler._BroadcastScheduler__audience_reps_hash == fake_reps_hash
        bc_scheduler._put_command.assert_called_with(
            BroadcastCommand.BROADCAST,
            (method_name, method_param, mocker.ANY),
            block=mocker.ANY,
            block_timeout=mocker.ANY
        )
