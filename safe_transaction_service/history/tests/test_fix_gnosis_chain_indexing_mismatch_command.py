# SPDX-License-Identifier: FSL-1.1-MIT
from collections import OrderedDict
from datetime import timedelta
from io import StringIO
from unittest import mock
from unittest.mock import MagicMock

from django.core.management import CommandError, call_command
from django.test import TestCase, override_settings
from django.utils import timezone

import factory
from eth_account import Account
from eth_utils import event_abi_to_log_topic
from hexbytes import HexBytes
from redis.exceptions import LockError
from safe_eth.eth.contracts import get_safe_V1_3_0_contract
from safe_eth.eth.ethereum_client import EthereumClient, TracingManager
from web3 import Web3

from safe_transaction_service.utils.redis import get_redis
from safe_transaction_service.utils.tasks import LOCK_TIMEOUT, get_task_lock_name

from ..indexers import InternalTxIndexer
from ..management.commands.fix_gnosis_chain_indexing_mismatch import (
    DEFAULT_FROM_BLOCK,
    DEFAULT_TO_BLOCK,
    SAFE_LOCK_TIMEOUT,
    Command,
    SafeState,
)
from ..models import (
    EthereumTxCallType,
    InternalTx,
    InternalTxDecoded,
    InternalTxType,
    MultisigConfirmation,
    MultisigTransaction,
    SafeLastStatus,
    SafeStatus,
)
from ..services import IndexServiceProvider
from ..tasks import process_decoded_internal_txs_for_safe_task
from .factories import (
    EthereumBlockFactory,
    EthereumTxFactory,
    InternalTxDecodedFactory,
    InternalTxFactory,
    ModuleTransactionFactory,
    MultisigConfirmationFactory,
    MultisigTransactionFactory,
    SafeContractFactory,
    SafeLastStatusFactory,
    SafeMasterCopyFactory,
    SafeStatusFactory,
)

COMMAND = "fix_gnosis_chain_indexing_mismatch"
MODULE = f"safe_transaction_service.history.management.commands.{COMMAND}"
TRACING_NODE_URL = "http://localhost:8545"
_SAFE_EVENTS = get_safe_V1_3_0_contract(Web3()).events
EXECUTION_SUCCESS_TOPIC = HexBytes(
    event_abi_to_log_topic(_SAFE_EVENTS.ExecutionSuccess().abi)
)
MODULE_FAILURE_TOPIC = HexBytes(
    event_abi_to_log_topic(_SAFE_EVENTS.ExecutionFromModuleFailure().abi)
)
EXEC_TRANSACTION_DATA = b"\x6a\x76\x12\x02"

_BASE_TIME = timezone.now() - timedelta(days=30)


class OrderedEthereumBlockFactory(EthereumBlockFactory):
    """
    Block whose timestamp grows with its number, as the command filters ranges by
    block timestamp. A block with the same number is reused
    """

    class Meta:
        django_get_or_create = ("number",)

    timestamp = factory.LazyAttribute(
        lambda block: _BASE_TIME + timedelta(seconds=5 * block.number)
    )


class EtherRefundInternalTxFactory(InternalTxFactory):
    """
    Ether refund a Safe pays to the executor at the end of ``execTransaction``
    """

    data = None
    value = 1000
    gas = 2300
    gas_used = 0


class ExecTransactionInternalTxFactory(InternalTxFactory):
    """
    Delegate call of a Safe to its singleton for ``execTransaction``
    """

    trace_address = "0"
    call_type = EthereumTxCallType.DELEGATE_CALL.value
    tx_type = InternalTxType.CALL.value
    value = 0
    gas = 100_000
    gas_used = 50_000
    data = HexBytes(EXEC_TRANSACTION_DATA)


def _trace(
    trace_address: list[int],
    _from: str,
    to: str,
    call_type: str = "call",
    value: int = 0,
    gas: int = 2300,
    gas_used: int = 0,
    data: bytes = b"",
) -> dict:
    return {
        "action": {
            "from": _from,
            "to": to,
            "callType": call_type,
            "gas": gas,
            "value": value,
            "input": HexBytes(data),
        },
        "result": {"gasUsed": gas_used, "output": HexBytes("0x")},
        "traceAddress": trace_address,
        "subtraces": 0,
        "type": "call",
        "transactionHash": HexBytes(b"\x01" * 32),
        "blockHash": HexBytes(b"\x02" * 32),
        "blockNumber": 100,
        "transactionPosition": 0,
    }


def _exec_transaction_trace(safe: str, singleton: str, trace_address=(0,)) -> dict:
    return _trace(
        list(trace_address),
        safe,
        singleton,
        call_type="delegatecall",
        gas=100_000,
        gas_used=50_000,
        data=EXEC_TRANSACTION_DATA,
    )


def _execution_success_log(safe: str, safe_tx_hash: str, payment: int = 0) -> dict:
    """
    :return: v1.3.0 ``ExecutionSuccess`` log as stored in ``EthereumTx.logs``
    """
    return {
        "address": safe,
        "topics": [EXECUTION_SUCCESS_TOPIC.to_0x_hex()],
        "data": HexBytes(
            HexBytes(safe_tx_hash) + payment.to_bytes(32, "big")
        ).to_0x_hex(),
    }


def _raw_execution_log(safe: str, tx_hash: HexBytes | None = None) -> dict:
    """
    :return: ``ExecutionSuccess`` log as returned by the node
    """
    log = {
        "address": safe,
        "topics": [EXECUTION_SUCCESS_TOPIC],
        "data": HexBytes(b"\x01" * 64),
    }
    if tx_hash is not None:
        log["transactionHash"] = tx_hash
    return log


def _safe_lock_name(address: str) -> str:
    return get_task_lock_name(
        process_decoded_internal_txs_for_safe_task.name, lock_name_suffix=address
    )


def _chain_nonces_mock(nonces_by_block: dict[int, int]):
    """
    :return: ``Command._chain_nonces`` replacement returning the nonce per block
    """

    def chain_nonces(_, safes_and_blocks):
        return [nonces_by_block.get(block) for _, block in safes_and_blocks]

    return chain_nonces


@override_settings(ETHEREUM_TRACING_NODE_URL=TRACING_NODE_URL)
class TestFixGnosisChainIndexingMismatchCommand(TestCase):
    @staticmethod
    def _command(**options) -> tuple[Command, StringIO]:
        """
        :param options: ``Command.setup`` options. The range is blocks 100-200
        :return: Command ready to run its steps and its output
        """
        output = StringIO()
        command = Command(stdout=output)
        command.setup(**{"from_block": 100, "to_block": 200, **options})
        return command, output

    @staticmethod
    def _safe_with_last_status(nonce: int, block_number: int) -> str:
        """
        :return: Address of a Safe whose last status has ``nonce`` at ``block_number``
        """
        safe_address = Account.create().address
        SafeLastStatusFactory(
            address=safe_address,
            nonce=nonce,
            internal_tx=InternalTxFactory(
                _from=safe_address,
                ethereum_tx__block=OrderedEthereumBlockFactory(number=block_number),
            ),
        )
        return safe_address

    @staticmethod
    def _executed_multisig_tx(
        safe_address: str,
        nonce: int,
        logged: bool = True,
        block_number: int = 1_000,
        **kwargs,
    ) -> MultisigTransaction:
        """
        :param logged: Store the execution log of the multisig tx in its tx
        :return: Executed multisig tx created by the indexer
        """
        ethereum_tx = EthereumTxFactory(
            block=OrderedEthereumBlockFactory(number=block_number)
        )
        multisig_tx = MultisigTransactionFactory(
            **{
                "safe": safe_address,
                "nonce": nonce,
                "ethereum_tx": ethereum_tx,
                "created": ethereum_tx.block.timestamp,
                "origin": {},
                "proposer": None,
                **kwargs,
            }
        )
        execution_hash = (
            multisig_tx.safe_tx_hash if logged else HexBytes(b"\xee" * 32).to_0x_hex()
        )
        ethereum_tx.logs = [_execution_success_log(safe_address, execution_hash)]
        ethereum_tx.save(update_fields=["logs"])
        return multisig_tx

    def test_arguments(self):
        with self.assertRaisesMessage(CommandError, "lower or equal"):
            call_command(
                COMMAND, "--from-block=200", "--to-block=100", stdout=StringIO()
            )
        with self.assertRaisesMessage(CommandError, "Invalid Ethereum address"):
            call_command(COMMAND, "--addresses", "0x1234", stdout=StringIO())

        output = StringIO()
        call_command(COMMAND, "--only-fix-safes", "--dry-run", stdout=output)
        self.assertIn(
            f"Range: blocks {DEFAULT_FROM_BLOCK}-{DEFAULT_TO_BLOCK}", output.getvalue()
        )
        self.assertIn("Would fix 0 Safes", output.getvalue())

    # ------------------------------------------------------------------
    # Empty logs
    # ------------------------------------------------------------------
    @mock.patch.object(EthereumClient, "get_transaction_receipts", autospec=True)
    def test_fix_empty_logs(self, get_transaction_receipts_mock: MagicMock):
        block_in_range = EthereumBlockFactory(number=150)
        block_out_of_range = EthereumBlockFactory(number=500)
        empty_tx = EthereumTxFactory(block=block_in_range, logs=[], status=1)
        legit_empty_tx = EthereumTxFactory(block=block_in_range, logs=[], status=1)
        missing_receipt_tx = EthereumTxFactory(block=block_in_range, logs=[])
        out_of_range_tx = EthereumTxFactory(block=block_out_of_range, logs=[])
        with_logs_tx = EthereumTxFactory(
            block=block_in_range,
            logs=[{"address": Account.create().address, "data": "0x", "topics": []}],
        )
        safe_address = Account.create().address
        module_address = Account.create().address
        # Processed from the receipt without logs
        multisig_tx = MultisigTransactionFactory(
            safe=safe_address, ethereum_tx=empty_tx, failed=None, payment=None
        )
        module_tx = ModuleTransactionFactory(
            safe=safe_address,
            module=module_address,
            internal_tx__ethereum_tx=empty_tx,
            failed=False,
        )
        # Processed with a wrong nonce: its hash is not in the logs
        safe_wrong_hash = Account.create().address
        MultisigTransactionFactory(safe=safe_wrong_hash, ethereum_tx=empty_tx)
        receipts_by_hash = {
            HexBytes(empty_tx.tx_hash): {
                "status": 1,
                "logs": [
                    {
                        "address": safe_address,
                        "data": HexBytes(multisig_tx.safe_tx_hash)
                        + (7).to_bytes(32, "big"),
                        "topics": [EXECUTION_SUCCESS_TOPIC],
                    },
                    {
                        "address": safe_address,
                        "data": HexBytes(b""),
                        "topics": [
                            MODULE_FAILURE_TOPIC,
                            HexBytes(module_address).rjust(32, b"\x00"),
                        ],
                    },
                    _raw_execution_log(safe_wrong_hash),
                ],
            },
            HexBytes(legit_empty_tx.tx_hash): {"status": 1, "logs": []},
            HexBytes(missing_receipt_tx.tx_hash): None,
        }
        get_transaction_receipts_mock.side_effect = lambda _, tx_hashes: [
            receipts_by_hash[HexBytes(tx_hash)] for tx_hash in tx_hashes
        ]

        command, output = self._command(dry_run=True)
        command.fix_empty_logs()
        self.assertIn("Found 3 txs without logs", output.getvalue())
        self.assertIn(
            "Would update logs of 1/3 txs. still_empty=1, missing_receipt=1, "
            "multisig_txs_changed=1, module_txs_changed=1",
            output.getvalue(),
        )
        # Logs are only used for `failed` and `payment`, the Safe is not fixed
        self.assertNotIn(f"Safe to fix: {safe_address}", output.getvalue())
        self.assertIn(f"Safe to fix: {safe_wrong_hash}", output.getvalue())
        empty_tx.refresh_from_db()
        self.assertEqual(empty_tx.logs, [])

        command, output = self._command()
        command.fix_empty_logs()
        self.assertIn("Updated logs of 1/3 txs", output.getvalue())
        empty_tx.refresh_from_db()
        self.assertEqual(len(empty_tx.logs), 3)
        self.assertEqual(empty_tx.logs[0]["address"], safe_address)
        multisig_tx.refresh_from_db()
        self.assertFalse(multisig_tx.failed)
        self.assertEqual(multisig_tx.payment, 7)
        module_tx.refresh_from_db()
        self.assertTrue(module_tx.failed)
        # Not in range or already with logs, never fetched
        fetched = {
            HexBytes(tx_hash)
            for call in get_transaction_receipts_mock.call_args_list
            for tx_hash in call.args[1]
        }
        self.assertFalse(
            fetched
            & {HexBytes(out_of_range_tx.tx_hash), HexBytes(with_logs_tx.tx_hash)}
        )

    # ------------------------------------------------------------------
    # Traces of the range
    # ------------------------------------------------------------------
    @mock.patch.object(InternalTxIndexer, "process_elements", autospec=True)
    @mock.patch.object(TracingManager, "trace_transactions", autospec=True)
    def test_sync_range_traces(
        self, trace_transactions_mock: MagicMock, process_elements_mock: MagicMock
    ):
        master_copy = SafeMasterCopyFactory().address
        block = OrderedEthereumBlockFactory(number=150)
        safe_address = Account.create().address
        receiver = Account.create().address
        safe_tx = EthereumTxFactory(block=block)
        refund = {"ethereum_tx": safe_tx, "_from": safe_address, "to": receiver}
        execution = ExecTransactionInternalTxFactory(
            ethereum_tx=safe_tx, _from=safe_address, to=master_copy
        )
        # Same refund transfer stored twice. On chain `0,1` is the refund and
        # `0,0` is a different call, so the row at `0,0` is wrong
        wrong_trace = EtherRefundInternalTxFactory(trace_address="0,0", **refund)
        InternalTxDecodedFactory(
            internal_tx=wrong_trace, function_name="transfer", processed=True
        )
        right_trace = EtherRefundInternalTxFactory(trace_address="0,1", **refund)
        # Gas is not compared: nodes of different clients can report another gas
        other_gas_trace = EtherRefundInternalTxFactory(
            trace_address="0,2", gas=5000, **refund
        )
        # Repeated call that exists twice on chain too
        legit_1 = EtherRefundInternalTxFactory(trace_address="0,3", value=7, **refund)
        legit_2 = EtherRefundInternalTxFactory(trace_address="0,4", value=7, **refund)
        # Stored for an ERC20 transfer, it does not call a Safe singleton
        erc20_tx = EthereumTxFactory(block=block)
        # Out of the range
        out_of_range_tx = EthereumTxFactory(
            block=OrderedEthereumBlockFactory(number=500)
        )
        out_of_range = EtherRefundInternalTxFactory(
            trace_address="0,0", **{**refund, "ethereum_tx": out_of_range_tx}
        )
        safe_traces = [
            _trace([], Account.create().address, safe_address, gas=100_000),
            _exec_transaction_trace(safe_address, master_copy),
            _trace([0, 0], safe_address, receiver, gas=9000, data=b"\xab\xcd"),
            _trace([0, 1], safe_address, receiver, value=1000),
            _trace([0, 2], safe_address, receiver, value=1000, gas=6000),
            _trace([0, 3], safe_address, receiver, value=7),
            _trace([0, 4], safe_address, receiver, value=7),
        ]
        traces_by_tx = {
            HexBytes(safe_tx.tx_hash): safe_traces,
            HexBytes(erc20_tx.tx_hash): [
                _trace([], Account.create().address, receiver, value=5)
            ],
        }
        trace_transactions_mock.side_effect = lambda _, tx_hashes: [
            traces_by_tx.get(HexBytes(tx_hash), []) for tx_hash in tx_hashes
        ]

        command, output = self._command(dry_run=True)
        command.sync_range_traces()
        self.assertIn(
            "Would change the traces of 1/2 txs. traces_deleted=1, unverifiable=0",
            output.getvalue(),
        )
        # The deleted trace was decoded, its Safe must be fixed
        self.assertIn(f"Safe to fix: {safe_address}", output.getvalue())
        process_elements_mock.assert_not_called()
        self.assertTrue(InternalTx.objects.filter(pk=wrong_trace.pk).exists())

        command, output = self._command()
        command.sync_range_traces()
        self.assertIn("Changed the traces of 1/2 txs", output.getvalue())
        self.assertIn("Traces synced up to block 200", output.getvalue())
        # The traces on chain are indexed in case one was skipped, the ERC20 tx is
        # not indexed
        self.assertEqual(
            process_elements_mock.call_args.args[1],
            OrderedDict([(HexBytes(safe_tx.tx_hash), safe_traces)]),
        )
        self.assertFalse(InternalTx.objects.filter(pk=wrong_trace.pk).exists())
        self.assertFalse(
            InternalTxDecoded.objects.filter(internal_tx_id=wrong_trace.pk).exists()
        )
        for internal_tx in (
            execution,
            right_trace,
            other_gas_trace,
            legit_1,
            legit_2,
            out_of_range,
        ):
            self.assertTrue(InternalTx.objects.filter(pk=internal_tx.pk).exists())

    @mock.patch.object(InternalTxIndexer, "process_elements", autospec=True)
    @mock.patch.object(TracingManager, "trace_transactions", autospec=True)
    def test_sync_range_traces_missing_trace(
        self, trace_transactions_mock: MagicMock, process_elements_mock: MagicMock
    ):
        master_copy = SafeMasterCopyFactory().address
        safe_address = Account.create().address
        safe_tx = EthereumTxFactory(block=OrderedEthereumBlockFactory(number=150))
        ExecTransactionInternalTxFactory(
            ethereum_tx=safe_tx, _from=safe_address, to=master_copy
        )
        # The refund was skipped by the node
        traces = [
            _exec_transaction_trace(safe_address, master_copy),
            _trace([0, 0], safe_address, Account.create().address, value=1000),
        ]
        trace_transactions_mock.return_value = [traces]

        command, output = self._command()
        command.sync_range_traces()
        self.assertIn(
            "Changed the traces of 1/1 txs. traces_deleted=0", output.getvalue()
        )
        self.assertEqual(
            process_elements_mock.call_args.args[1],
            OrderedDict([(HexBytes(safe_tx.tx_hash), traces)]),
        )

        # Nothing missing: the tx is not changed
        process_elements_mock.reset_mock()
        trace_transactions_mock.return_value = [traces[:1]]
        command, output = self._command()
        command.sync_range_traces()
        self.assertIn("Changed the traces of 0/1 txs", output.getvalue())
        process_elements_mock.assert_not_called()

    @mock.patch.object(InternalTxIndexer, "process_elements", autospec=True)
    @mock.patch.object(TracingManager, "trace_transactions", autospec=True)
    def test_sync_range_traces_node_fails(
        self, trace_transactions_mock: MagicMock, process_elements_mock: MagicMock
    ):
        safe_tx = EthereumTxFactory(block=OrderedEthereumBlockFactory(number=150))
        rows = [
            EtherRefundInternalTxFactory(ethereum_tx=safe_tx, trace_address=address)
            for address in ("0,0", "0,1")
        ]
        # Node returns nothing: nothing is deleted
        trace_transactions_mock.return_value = [[]]
        command, output = self._command()
        command.sync_range_traces()
        self.assertIn("unverifiable=1", output.getvalue())

        # Node fails: nothing is deleted
        trace_transactions_mock.side_effect = OSError("timeout")
        command, output = self._command()
        command.sync_range_traces()
        self.assertIn("Cannot get traces for 1 txs", output.getvalue())
        self.assertIn("unverifiable=1", output.getvalue())
        self.assertEqual(
            InternalTx.objects.filter(pk__in=[row.pk for row in rows]).count(), 2
        )
        process_elements_mock.assert_not_called()

        # A stopped run continues from a block
        trace_transactions_mock.reset_mock()
        command, output = self._command(traces_from_block=151)
        command.sync_range_traces()
        trace_transactions_mock.assert_not_called()
        self.assertIn("Changed the traces of 0/0 txs", output.getvalue())

    # ------------------------------------------------------------------
    # Missing executions
    # ------------------------------------------------------------------
    @mock.patch.object(InternalTxIndexer, "process_elements", autospec=True)
    @mock.patch.object(InternalTxIndexer, "trace_transactions", autospec=True)
    @mock.patch(f"{MODULE}.get_auto_ethereum_client")
    def test_index_missing_executions(
        self,
        get_auto_ethereum_client_mock: MagicMock,
        trace_transactions_mock: MagicMock,
        process_elements_mock: MagicMock,
    ):
        get_logs_mock = get_auto_ethereum_client_mock.return_value.slow_w3.eth.get_logs
        master_copy = SafeMasterCopyFactory().address
        safe_missing = Account.create().address
        safe_indexed = Account.create().address
        safe_not_monitored = Account.create().address
        safe_unverifiable = Account.create().address
        tx_missing = HexBytes(b"\x0a" * 32)
        tx_indexed = HexBytes(b"\x0b" * 32)
        tx_not_monitored = HexBytes(b"\x0c" * 32)
        tx_unverifiable = HexBytes(b"\x0d" * 32)

        # `tx_indexed` already has its execution decoded
        InternalTxDecodedFactory(
            function_name="execTransaction",
            internal_tx=InternalTxFactory(
                ethereum_tx=EthereumTxFactory(tx_hash=tx_indexed.to_0x_hex()),
                _from=safe_indexed,
            ),
        )
        get_logs_mock.return_value = [
            _raw_execution_log(safe_missing, tx_missing),
            _raw_execution_log(safe_indexed, tx_indexed),
            _raw_execution_log(safe_not_monitored, tx_not_monitored),
            _raw_execution_log(safe_unverifiable, tx_unverifiable),
        ]
        traces = {
            tx_missing: [_exec_transaction_trace(safe_missing, master_copy)],
            tx_not_monitored: [
                _exec_transaction_trace(safe_not_monitored, Account.create().address)
            ],
            tx_unverifiable: [],
        }
        trace_transactions_mock.side_effect = lambda _, tx_hashes, batch_size: [
            traces[HexBytes(tx_hash)] for tx_hash in tx_hashes
        ]

        command, output = self._command(dry_run=True)
        command.index_missing_executions()
        self.assertIn(
            "4 executions on chain, 3 txs with missing executions", output.getvalue()
        )
        self.assertIn(
            f"Missing execution in tx {tx_missing.to_0x_hex()}", output.getvalue()
        )
        self.assertIn("singleton that is not monitored", output.getvalue())
        self.assertIn(
            f"Node returned no traces for tx {tx_unverifiable.to_0x_hex()}",
            output.getvalue(),
        )
        self.assertIn("Would index 1 txs", output.getvalue())
        self.assertIn("unverifiable=1", output.getvalue())
        process_elements_mock.assert_not_called()

        command, output = self._command()
        command.index_missing_executions()
        self.assertIn("Indexed 1 txs with missing executions", output.getvalue())
        self.assertIn("missing_executions=1", output.getvalue())
        self.assertIn(f"Safe to fix: {safe_missing}", output.getvalue())
        # The tx is not stored, its traces are indexed
        self.assertEqual(
            process_elements_mock.call_args.args[1],
            OrderedDict([(tx_missing, traces[tx_missing])]),
        )
        self.assertEqual(get_logs_mock.call_count, 2)
        self.assertEqual(get_logs_mock.call_args.args[0]["fromBlock"], 100)
        self.assertEqual(get_logs_mock.call_args.args[0]["toBlock"], 200)

    @mock.patch(f"{MODULE}.get_auto_ethereum_client")
    def test_index_missing_executions_splits_range_on_error(
        self, get_auto_ethereum_client_mock: MagicMock
    ):
        get_logs_mock = get_auto_ethereum_client_mock.return_value.slow_w3.eth.get_logs
        # Node rejects the full range, accepts the halves
        get_logs_mock.side_effect = [ValueError("too many results"), [], []]
        command, output = self._command()
        command.index_missing_executions()
        self.assertIn("splitting the range", output.getvalue())
        self.assertIn("failed_blocks=0", output.getvalue())
        ranges = [
            (call.args[0]["fromBlock"], call.args[0]["toBlock"])
            for call in get_logs_mock.call_args_list
        ]
        self.assertEqual(ranges, [(100, 200), (100, 150), (151, 200)])

        # Node always fails: single blocks are skipped and reported, the run goes on
        get_logs_mock.reset_mock()
        get_logs_mock.side_effect = ValueError("bad block")
        command, output = self._command(from_block=100, to_block=101)
        command.index_missing_executions()
        self.assertIn("Cannot get logs for block 100, skipping it", output.getvalue())
        self.assertIn("Cannot get logs for block 101, skipping it", output.getvalue())
        self.assertIn("failed_blocks=2", output.getvalue())
        self.assertEqual(get_logs_mock.call_count, 3)

    @mock.patch.object(InternalTxIndexer, "process_elements", autospec=True)
    @mock.patch.object(InternalTxIndexer, "trace_transactions", autospec=True)
    @mock.patch(f"{MODULE}.get_auto_ethereum_client")
    def test_index_missing_executions_in_indexed_tx(
        self,
        get_auto_ethereum_client_mock: MagicMock,
        trace_transactions_mock: MagicMock,
        process_elements_mock: MagicMock,
    ):
        get_logs_mock = get_auto_ethereum_client_mock.return_value.slow_w3.eth.get_logs
        master_copy = SafeMasterCopyFactory().address
        safe_address = Account.create().address
        executor = Account.create().address
        ethereum_tx = EthereumTxFactory(block=OrderedEthereumBlockFactory(number=150))
        tx_hash = HexBytes(ethereum_tx.tx_hash)
        # The tx executes 2 txs of the Safe, only the first one was indexed
        indexed_execution = ExecTransactionInternalTxFactory(
            ethereum_tx=ethereum_tx, _from=safe_address, to=master_copy
        )
        InternalTxDecodedFactory(
            internal_tx=indexed_execution, function_name="execTransaction"
        )
        # The refund is stored at the trace address of the second execution
        wrong_trace = EtherRefundInternalTxFactory(
            ethereum_tx=ethereum_tx, _from=safe_address, to=executor, trace_address="1"
        )
        execution_log = _raw_execution_log(safe_address, tx_hash)
        get_logs_mock.return_value = [execution_log, execution_log]
        traces = [
            _exec_transaction_trace(safe_address, master_copy),
            _exec_transaction_trace(safe_address, master_copy, trace_address=(1,)),
            _trace([1, 0], safe_address, executor, value=1000),
        ]
        trace_transactions_mock.return_value = [traces]

        command, output = self._command()
        command.index_missing_executions()
        self.assertIn(
            "2 executions on chain, 1 txs with missing executions", output.getvalue()
        )
        self.assertIn(f"Safe to fix: {safe_address}", output.getvalue())
        self.assertIn("wrong_traces_deleted=1", output.getvalue())
        self.assertFalse(InternalTx.objects.filter(pk=wrong_trace.pk).exists())
        self.assertTrue(InternalTx.objects.filter(pk=indexed_execution.pk).exists())
        self.assertEqual(
            process_elements_mock.call_args.args[1], OrderedDict([(tx_hash, traces)])
        )

        # Node fails: the chunk is reported and the run goes on
        trace_transactions_mock.side_effect = OSError("timeout")
        command, output = self._command()
        command.index_missing_executions()
        self.assertIn("Cannot get traces for 1 txs", output.getvalue())
        self.assertIn("unverifiable=1", output.getvalue())

    # ------------------------------------------------------------------
    # Safes
    # ------------------------------------------------------------------
    @mock.patch.object(Command, "_chain_nonces", autospec=True)
    def test_fix_safes_clean(self, chain_nonces_mock: MagicMock):
        safe_address = self._safe_with_last_status(nonce=1, block_number=150)
        chain_nonces_mock.side_effect = _chain_nonces_mock({150: 1})
        right = self._executed_multisig_tx(safe_address, nonce=0)
        # Hash not executed in its tx and created by the indexer
        wrong = self._executed_multisig_tx(safe_address, nonce=0, logged=False)
        MultisigConfirmationFactory(multisig_transaction=wrong)
        # Hash not executed in its tx and proposed by a user
        proposed = self._executed_multisig_tx(
            safe_address, nonce=0, logged=False, origin={"url": "https://app"}
        )
        decoded = InternalTxDecodedFactory(
            internal_tx=InternalTxFactory(
                _from=safe_address,
                ethereum_tx__block=OrderedEthereumBlockFactory(number=150),
            ),
            processed=True,
        )

        command, output = self._command(dry_run=True)
        command.fix_safes([safe_address])
        self.assertIn(
            f"[{safe_address}] action=clean to_delete=1 to_unlink=1 db_nonce=1 "
            f"chain_nonce=1 correct_nonces=1",
            output.getvalue(),
        )
        self.assertTrue(MultisigTransaction.objects.filter(pk=wrong.pk).exists())

        command, output = self._command()
        command.fix_safes([safe_address])
        self.assertIn(
            "Fixed 1 Safes. clean=1, multisig_txs_deleted=1, "
            "multisig_txs_unlinked=1, failed=0",
            output.getvalue(),
        )
        self.assertTrue(MultisigTransaction.objects.filter(pk=right.pk).exists())
        self.assertFalse(MultisigTransaction.objects.filter(pk=wrong.pk).exists())
        self.assertFalse(
            MultisigConfirmation.objects.filter(
                multisig_transaction_hash=wrong.safe_tx_hash
            ).exists()
        )
        proposed.refresh_from_db()
        self.assertIsNone(proposed.ethereum_tx)
        # The state is right, the Safe is not processed again
        self.assertTrue(SafeLastStatus.objects.filter(address=safe_address).exists())
        decoded.refresh_from_db()
        self.assertTrue(decoded.processed)
        # The lock of the Safe is released
        self.assertFalse(get_redis().lock(_safe_lock_name(safe_address)).locked())

        # Nothing left to fix
        command, output = self._command()
        command.fix_safes([safe_address])
        self.assertIn(f"[{safe_address}] action=ok", output.getvalue())

    @mock.patch.object(Command, "_chain_nonces", autospec=True)
    def test_fix_safes_reset(self, chain_nonces_mock: MagicMock):
        # Range start is block 150
        OrderedEthereumBlockFactory(number=150)
        chain_nonces_mock.side_effect = _chain_nonces_mock({50: 1, 160: 3, 170: 4})

        def internal_tx(address: str, block_number: int) -> InternalTx:
            return InternalTxFactory(
                _from=address,
                ethereum_tx__block=OrderedEthereumBlockFactory(number=block_number),
            )

        # Last status before the range is right: reset from the range start
        from_range = self._safe_with_last_status(nonce=9, block_number=160)
        before_range = SafeStatusFactory(
            address=from_range, nonce=1, internal_tx=internal_tx(from_range, 50)
        )
        in_range = SafeStatusFactory(
            address=from_range, nonce=9, internal_tx=internal_tx(from_range, 160)
        )
        decoded_before = InternalTxDecodedFactory(
            internal_tx=before_range.internal_tx, function_name="setup", processed=True
        )
        decoded_in_range = InternalTxDecodedFactory(
            internal_tx=in_range.internal_tx,
            function_name="execTransaction",
            processed=True,
        )

        # No status before the range: reset from the setup
        from_setup = self._safe_with_last_status(nonce=9, block_number=170)
        setup = InternalTxDecodedFactory(
            internal_tx=internal_tx(from_setup, 170),
            function_name="setup",
            processed=True,
        )
        SafeStatusFactory(address=from_setup, internal_tx=setup.internal_tx)

        # Being processed by the workers without wrong multisig txs: skipped
        processing = self._safe_with_last_status(nonce=9, block_number=170)
        InternalTxDecodedFactory(
            internal_tx=internal_tx(processing, 170), processed=False
        )

        # Banned: skipped
        banned = self._safe_with_last_status(nonce=9, block_number=170)
        SafeContractFactory(address=banned, banned=True)

        # Being processed with wrong multisig txs: only cleaned, the workers rebuild
        # its state
        processing_wrong = self._safe_with_last_status(nonce=9, block_number=170)
        InternalTxDecodedFactory(
            internal_tx=internal_tx(processing_wrong, 170), processed=False
        )
        wrong = self._executed_multisig_tx(processing_wrong, nonce=0, logged=False)

        # Its status was deleted with a wrong trace: reset from the setup
        lost_status = Account.create().address
        lost_setup = InternalTxDecodedFactory(
            internal_tx=internal_tx(lost_status, 170),
            function_name="setup",
            processed=True,
        )

        addresses = [
            from_range,
            from_setup,
            processing,
            banned,
            processing_wrong,
            lost_status,
        ]
        command, output = self._command(dry_run=True)
        command.fix_safes(addresses)
        self.assertIn(f"[{from_range}] action=reset-from-range", output.getvalue())
        self.assertIn(f"[{from_setup}] action=reset-from-setup", output.getvalue())
        self.assertIn(f"[{processing}] action=skip-processing", output.getvalue())
        self.assertIn(f"[{banned}] action=skip-banned", output.getvalue())
        self.assertIn(
            f"[{processing_wrong}] action=clean to_delete=1", output.getvalue()
        )
        self.assertIn(f"[{lost_status}] action=reset-from-setup", output.getvalue())

        # Waves: only one Safe is changed per run
        command, output = self._command(max_safes=1)
        command.fix_safes(addresses)
        self.assertIn("Reached --max-safes=1", output.getvalue())
        self.assertIn("Fixed 1 Safes", output.getvalue())
        self.assertTrue(SafeLastStatus.objects.filter(address=from_setup).exists())

        # The fixed Safe is being processed now, the next run continues
        command, output = self._command()
        command.fix_safes(addresses)
        self.assertIn(f"[{from_range}] action=skip-processing", output.getvalue())
        self.assertIn("Fixed 3 Safes", output.getvalue())
        self.assertFalse(MultisigTransaction.objects.filter(pk=wrong.pk).exists())
        self.assertTrue(
            SafeLastStatus.objects.filter(address=processing_wrong).exists()
        )
        lost_setup.refresh_from_db()
        self.assertFalse(lost_setup.processed)

        # Statuses from the range start are deleted and processed again
        self.assertFalse(SafeLastStatus.objects.filter(address=from_range).exists())
        self.assertTrue(SafeStatus.objects.filter(pk=before_range.pk).exists())
        self.assertFalse(SafeStatus.objects.filter(pk=in_range.pk).exists())
        decoded_before.refresh_from_db()
        self.assertTrue(decoded_before.processed)
        decoded_in_range.refresh_from_db()
        self.assertFalse(decoded_in_range.processed)

        # Every status is deleted and every decoded tx is processed again
        self.assertFalse(SafeLastStatus.objects.filter(address=from_setup).exists())
        self.assertFalse(SafeStatus.objects.filter(address=from_setup).exists())
        setup.refresh_from_db()
        self.assertFalse(setup.processed)

    @mock.patch(f"{MODULE}.get_redis")
    @mock.patch.object(Command, "_chain_nonces", autospec=True)
    def test_fix_safes_lock(
        self, chain_nonces_mock: MagicMock, get_redis_mock: MagicMock
    ):
        chain_nonces_mock.side_effect = _chain_nonces_mock({150: 1})
        locked = self._safe_with_last_status(nonce=1, block_number=150)
        self._executed_multisig_tx(locked, nonce=0)
        locked_wrong = self._executed_multisig_tx(locked, nonce=0, logged=False)
        free = self._safe_with_last_status(nonce=1, block_number=150)
        self._executed_multisig_tx(free, nonce=0)
        free_wrong = self._executed_multisig_tx(free, nonce=0, logged=False)
        lock_mock = get_redis_mock.return_value.lock
        released: list[bool] = []

        def acquire(*args):
            if lock_mock.call_args.args[0] == _safe_lock_name(locked):
                raise LockError("Locked by a worker")
            return lock_mock.return_value

        def release(*args) -> bool:
            # The Safe is changed before releasing its lock
            released.append(
                MultisigTransaction.objects.filter(pk=free_wrong.pk).exists()
            )
            return False

        lock_mock.return_value.__enter__.side_effect = acquire
        lock_mock.return_value.__exit__.side_effect = release

        command, output = self._command()
        command.fix_safes([locked, free])
        self.assertIn(
            f"[{locked}] Cannot hold its lock: Locked by a worker", output.getvalue()
        )
        self.assertIn(f"Safe not fixed, run again: {locked}", output.getvalue())
        self.assertIn("failed=1", output.getvalue())
        self.assertTrue(MultisigTransaction.objects.filter(pk=locked_wrong.pk).exists())
        self.assertFalse(MultisigTransaction.objects.filter(pk=free_wrong.pk).exists())
        self.assertEqual(released, [False])
        lock_mock.assert_called_with(
            _safe_lock_name(free),
            timeout=SAFE_LOCK_TIMEOUT,
            blocking_timeout=LOCK_TIMEOUT,
        )

    @mock.patch.object(Command, "_chain_nonces", autospec=True)
    def test_fix_safes_default_safes(self, chain_nonces_mock: MagicMock):
        chain_nonces_mock.side_effect = _chain_nonces_mock({150: 1})
        repeated = self._safe_with_last_status(nonce=1, block_number=150)
        self._executed_multisig_tx(repeated, nonce=0)
        self._executed_multisig_tx(repeated, nonce=0, logged=False)
        fine = self._safe_with_last_status(nonce=1, block_number=150)
        self._executed_multisig_tx(fine, nonce=0)
        # Same nonce but not executed, it is a proposal
        MultisigTransactionFactory(safe=fine, nonce=0, ethereum_tx=None)
        # Wrong hash in the range with every count right: checked against the logs
        wrong_in_range = self._safe_with_last_status(nonce=1, block_number=150)
        self._executed_multisig_tx(
            wrong_in_range, nonce=0, logged=False, block_number=160
        )

        # Executions in the range: the quick check decides if the logs are read
        shifted = self._safe_with_last_status(nonce=3, block_number=150)
        fine_in_range = self._safe_with_last_status(nonce=1, block_number=150)
        self._executed_multisig_tx(fine_in_range, nonce=0)
        for address in (shifted, fine_in_range):
            InternalTxDecodedFactory(
                internal_tx=InternalTxFactory(
                    _from=address,
                    ethereum_tx__block=OrderedEthereumBlockFactory(number=150),
                ),
                function_name="execTransaction",
                processed=True,
            )

        command, output = self._command(dry_run=True)
        command.fix_safes(None)
        self.assertIn("Checking 4 Safes", output.getvalue())
        self.assertIn(f"[{repeated}] action=clean to_delete=1", output.getvalue())
        # The only executed tx is wrong, so no nonce has a right multisig tx
        self.assertIn(
            f"[{wrong_in_range}] action=skip-no-setup to_delete=1", output.getvalue()
        )
        # No status before the range and no `setup` to process it again from
        self.assertIn(f"[{shifted}] action=skip-no-setup", output.getvalue())
        self.assertIn("ok=1", output.getvalue())
        self.assertNotIn(fine, output.getvalue())
        self.assertNotIn(fine_in_range, output.getvalue())

    @mock.patch.object(EthereumClient, "raw_batch_request", autospec=True)
    def test_chain_nonces_with_failing_call(self, raw_batch_request_mock: MagicMock):
        safe_address = Account.create().address

        def raw_batch_request(_, payload):
            # A batch fails if one of its calls fails
            if len(payload) > 1 or payload[0]["params"][1] == hex(20):
                raise ValueError("Batch request error")
            return [hex(int(payload[0]["params"][1], 16) // 10)]

        raw_batch_request_mock.side_effect = raw_batch_request
        command, output = self._command()
        self.assertEqual(
            command._chain_nonces(
                [(safe_address, 10), (safe_address, 20), (safe_address, 30)]
            ),
            [1, None, 3],
        )
        self.assertIn(
            f"Cannot get the nonce of {safe_address} at block 20", output.getvalue()
        )

    # ------------------------------------------------------------------
    # End to end
    # ------------------------------------------------------------------
    @mock.patch.object(Command, "_chain_nonces", autospec=True)
    def test_fix_safe_end_to_end(self, chain_nonces_mock: MagicMock):
        """
        A Safe processed with a wrong nonce is fixed and processed again by the
        regular processing
        """
        index_service = IndexServiceProvider()
        safe_address = Account.create().address
        master_copy = SafeMasterCopyFactory(version="1.3.0").address
        safe_call = {
            "internal_tx___from": safe_address,
            "internal_tx__to": master_copy,
            "internal_tx__value": 0,
        }
        InternalTxDecodedFactory(
            function_name="setup",
            internal_tx__ethereum_tx__block=OrderedEthereumBlockFactory(number=50),
            **safe_call,
        )
        # 2 executions of the same tx data, so only the nonce changes the hash
        executions = [
            InternalTxDecodedFactory(
                function_name="execTransaction",
                internal_tx__ethereum_tx__block=OrderedEthereumBlockFactory(
                    number=block_number
                ),
                **safe_call,
            )
            for block_number in (150, 160)
        ]
        index_service.process_decoded_txs_for_safe(safe_address)
        first, second = MultisigTransaction.objects.filter(safe=safe_address).order_by(
            "nonce"
        )
        for multisig_tx in (first, second):
            multisig_tx.ethereum_tx.logs = [
                _execution_success_log(safe_address, multisig_tx.safe_tx_hash)
            ]
            multisig_tx.ethereum_tx.save(update_fields=["logs"])
        second_tx = second.ethereum_tx
        second_safe_tx_hash = second.safe_tx_hash

        # The second execution was processed with a wrong nonce
        second.delete()
        wrong = MultisigTransactionFactory(
            safe=safe_address,
            nonce=5,
            ethereum_tx=second_tx,
            created=second_tx.block.timestamp,
            origin={},
            proposer=None,
        )
        proposed = MultisigTransactionFactory(
            safe=safe_address,
            nonce=7,
            ethereum_tx=first.ethereum_tx,
            origin={"url": "https://app"},
        )
        SafeLastStatus.objects.filter(address=safe_address).update(nonce=6)
        chain_nonces_mock.side_effect = _chain_nonces_mock({50: 0, 160: 2})

        command, output = self._command()
        command.fix_safes([safe_address])
        self.assertIn(
            f"[{safe_address}] action=reset-from-range to_delete=1 to_unlink=1 "
            f"db_nonce=6 chain_nonce=2 correct_nonces=1",
            output.getvalue(),
        )
        for execution in executions:
            execution.refresh_from_db()
            self.assertFalse(execution.processed)

        # Regular processing from the last status before the range
        index_service.process_decoded_txs_for_safe(safe_address)
        self.assertEqual(SafeLastStatus.objects.get(address=safe_address).nonce, 2)
        executed = MultisigTransaction.objects.executed().filter(safe=safe_address)
        self.assertEqual(
            {(tx.nonce, tx.ethereum_tx_id) for tx in executed},
            {(0, first.ethereum_tx_id), (1, second_tx.tx_hash)},
        )
        self.assertIn(first, executed)
        self.assertEqual(executed.get(nonce=1).safe_tx_hash, second_safe_tx_hash)
        self.assertFalse(MultisigTransaction.objects.filter(pk=wrong.pk).exists())
        proposed.refresh_from_db()
        self.assertIsNone(proposed.ethereum_tx)

        # The Safe is right now
        command, output = self._command()
        self.assertEqual(
            command._safe_summaries([safe_address])[safe_address].state, SafeState.OK
        )
        command.fix_safes([safe_address])
        self.assertIn(f"[{safe_address}] action=ok", output.getvalue())
