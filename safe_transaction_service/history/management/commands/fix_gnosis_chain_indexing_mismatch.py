# SPDX-License-Identifier: FSL-1.1-MIT
"""
Repair data indexed from a tracing node that returned incomplete data for a
block range: receipts without logs, traces missing and traces stored under a
wrong ``traceAddress``. As the Safe nonce of a traced execution is counted from
the processed executions, those errors left Safes with wrong nonces,
``MultisigTransaction`` rows with wrong ``safe_tx_hash`` and correct hashes linked
to the wrong tx.

Steps, every one only touches rows that are still wrong, so the command can be run
again:

1. Fetch the receipt again for ``EthereumTx`` rows with no logs, store the logs and
   recalculate ``failed``/``payment`` of the multisig txs and ``failed`` of the
   module txs of those txs, the only fields built from logs.
2. Compare the ``ExecutionSuccess``/``ExecutionFailure`` logs on chain with the
   decoded ``execTransaction`` calls and sync the traces of the txs with missing
   executions, also when the tx is not stored.
3. Sync the traces of every stored tx of the range that calls a Safe singleton with
   ``trace_transaction``. Txs stored only for token transfers are skipped.
4. Fix the affected Safes: delete the executed ``MultisigTransaction`` rows whose
   hash is not in the execution logs of their tx (or unlink them if they were
   proposed by a user), then mark the Safe to be processed again by the workers
   from the start of the range, or from its ``setup`` if its state before the
   range is also wrong.

Syncing the traces of a tx deletes the stored traces that are not in
``trace_transaction`` and indexes the relevant traces that are missing, so a trace
stored under a wrong ``traceAddress`` cannot take the place of the real one.
"""

import argparse
import dataclasses
from collections import Counter, OrderedDict, defaultdict
from collections.abc import Iterable, Iterator, Sequence
from datetime import datetime
from enum import StrEnum
from functools import cached_property
from typing import Any, cast

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import connection, transaction
from django.db.models import Count, F, Func, IntegerField, Max, Min, QuerySet

from eth_typing import ChecksumAddress, HexStr
from hexbytes import HexBytes
from redis.exceptions import LockError
from redis.lock import Lock
from safe_eth.eth import get_auto_ethereum_client
from safe_eth.eth.utils import fast_to_checksum_address
from safe_eth.util.util import to_0x_hex_str
from web3.exceptions import Web3RPCError
from web3.types import FilterTrace, LogReceipt

from safe_transaction_service.utils.redis import get_redis
from safe_transaction_service.utils.tasks import LOCK_TIMEOUT, get_task_lock_name
from safe_transaction_service.utils.utils import chunks

from ...cache import CacheSafeTxsView, remove_cache_view_for_addresses
from ...indexers import InternalTxIndexer, InternalTxIndexerProvider
from ...indexers.tx_processor import SafeTxProcessor, SafeTxProcessorProvider
from ...models import (
    EthereumBlock,
    EthereumTx,
    InternalTx,
    InternalTxDecoded,
    ModuleTransaction,
    MultisigConfirmation,
    MultisigTransaction,
    SafeContract,
    SafeLastStatus,
    SafeStatus,
)
from ...tasks import process_decoded_internal_txs_for_safe_task
from ...utils import clean_receipt_log


class SafeAction(StrEnum):
    """
    What is done with a Safe
    """

    OK = "ok"  # Nothing to fix
    CLEAN = "clean"  # Only wrong multisig txs, the Safe state is right
    RESET_FROM_RANGE = "reset-from-range"  # Process again from the range start
    RESET_FROM_SETUP = "reset-from-setup"  # Process again from the `setup`
    SKIP_PROCESSING = "skip-processing"  # Workers are processing it
    SKIP_BANNED = "skip-banned"
    SKIP_NO_SETUP = "skip-no-setup"
    SKIP_NODE_ERROR = "skip-node-error"


RESET_ACTIONS = (SafeAction.RESET_FROM_RANGE, SafeAction.RESET_FROM_SETUP)
CHANGE_ACTIONS = (SafeAction.CLEAN, *RESET_ACTIONS)


class SafeState(StrEnum):
    """
    State of a Safe found by the quick check, without reading the logs
    """

    OK = "ok"
    PROCESSING = "processing"
    NO_STATUS = "no_status"
    NODE_ERROR = "node_error"
    WRONG_NONCE = "wrong_nonce"
    REPEATED_NONCES = "repeated_nonces"
    MISSING_MULTISIG_TXS = "missing_multisig_txs"


@dataclasses.dataclass
class SafeSummary:
    """
    Data of a Safe used by the quick check
    """

    pending: bool = False
    db_nonce: int | None = None
    status_block: int | None = None
    chain_nonce: int | None = None
    executed_rows: int = 0
    executed_nonces: int = 0
    max_executed_nonce: int | None = None

    @property
    def state(self) -> SafeState:
        """
        :return: ``OK`` when the nonce of the last status is the nonce on chain at
            the block of that status and every nonce below it has exactly one
            executed multisig tx
        """
        if self.pending:
            return SafeState.PROCESSING
        if self.db_nonce is None:
            return SafeState.NO_STATUS
        if self.chain_nonce is None:
            return SafeState.NODE_ERROR
        if self.chain_nonce != self.db_nonce:
            return SafeState.WRONG_NONCE
        if self.executed_rows != self.executed_nonces:
            return SafeState.REPEATED_NONCES
        if self.executed_nonces != self.chain_nonce or (
            self.chain_nonce and self.max_executed_nonce != self.chain_nonce - 1
        ):
            return SafeState.MISSING_MULTISIG_TXS
        return SafeState.OK


@dataclasses.dataclass
class SafeRepair:
    """
    Result of checking a Safe against the logs
    """

    address: ChecksumAddress
    action: SafeAction
    to_delete: list[bytes] = dataclasses.field(default_factory=list)
    to_unlink: list[HexStr] = dataclasses.field(default_factory=list)
    db_nonce: int | None = None
    chain_nonce: int | None = None
    correct_nonces: int = 0


# Fields that identify a trace. The gas is left out, as nodes of different clients
# can report a different gas for the same trace
TRACE_KEY_FIELDS = (
    "trace_address",
    "_from",
    "to",
    "data",
    "value",
    "call_type",
    "tx_type",
)

# Gnosis Chain blocks indexed from the bad node. Receipts without logs go from
# block 41,454,189 (2025-08-05 16:14 UTC) to block 42,531,904 (2025-10-09 07:27 UTC).
# The defaults add about one day of margin on each side
DEFAULT_FROM_BLOCK = 41_437_000
DEFAULT_TO_BLOCK = 42_549_000

NONCE_SELECTOR = "0xaffed0e0"  # `nonce()` of the Safe contract
# An empty `logs` array takes a few bytes. Filtering by size first avoids reading
# the logs of every tx of the range from TOAST
EMPTY_LOGS_MAX_SIZE = 64
RPC_BATCH_SIZE = 100  # Elements per RPC batch request
LOGS_BLOCK_CHUNK = 1_000  # Blocks per `eth_getLogs` request
TRACES_BLOCK_WINDOW = 1_000  # Blocks whose stored txs are synced at a time
PROGRESS_EVERY = 100
# Rows per statement when cleaning a Safe. Big Safes have millions of rows, so they
# are changed in short statements that do not block the workers for long
DB_BATCH_SIZE = 5_000
# Expiration of the per Safe processing lock while a Safe is cleaned. It is renewed
# after every batch, the expiration only frees the Safe if the command is killed
SAFE_LOCK_TIMEOUT = 30 * 60


def _checksum_address(value: str) -> ChecksumAddress:
    """
    ``argparse`` type for Ethereum addresses

    :param value:
    :return: Checksummed address
    :raises argparse.ArgumentTypeError: If ``value`` is not an address
    """
    try:
        return fast_to_checksum_address(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid Ethereum address: {value}") from None


class Command(BaseCommand):
    help = (
        "Repair txs, traces and Safe multisig txs indexed from a node that returned "
        "incomplete data in a block range"
    )

    def add_arguments(self, parser: CommandParser):
        parser.add_argument(
            "--from-block",
            type=int,
            default=DEFAULT_FROM_BLOCK,
            help=f"First block of the range (default: {DEFAULT_FROM_BLOCK})",
        )
        parser.add_argument(
            "--to-block",
            type=int,
            default=DEFAULT_TO_BLOCK,
            help=f"Last block of the range (default: {DEFAULT_TO_BLOCK})",
        )
        parser.add_argument(
            "--traces-from-block",
            type=int,
            default=None,
            help="Continue syncing the traces of the range from this block, printed "
            "by a previous run. Default: --from-block",
        )
        parser.add_argument(
            "--only-fix-safes",
            action="store_true",
            help="Only fix the Safes, to fix them in waves after the data of the "
            "range was repaired",
        )
        parser.add_argument(
            "--addresses",
            nargs="+",
            type=_checksum_address,
            help="Safes to fix. If not provided, the Safes with wrong multisig txs "
            "and the Safes with executions in the range that fail a quick check",
        )
        parser.add_argument(
            "--max-safes",
            type=int,
            default=None,
            help="Max number of Safes changed in this run, to fix them in waves. Safes "
            "being processed by the workers are not processed again, so the next run "
            "continues with the rest",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Only report what would be changed",
        )

    def handle(self, *args, **options):
        self.setup(**options)
        if not options["only_fix_safes"]:
            self.fix_empty_logs()
            self.index_missing_executions()
            self.sync_range_traces()
        self.fix_safes(options["addresses"])

    def setup(
        self,
        from_block: int = DEFAULT_FROM_BLOCK,
        to_block: int = DEFAULT_TO_BLOCK,
        traces_from_block: int | None = None,
        max_safes: int | None = None,
        dry_run: bool = False,
        **kwargs,
    ) -> None:
        """
        Prepare the command to run its steps

        :raises CommandError: If the block range is not valid
        """
        if from_block > to_block:
            raise CommandError("--from-block must be lower or equal than --to-block")
        self.from_block = from_block
        self.to_block = to_block
        self.traces_from_block = (
            from_block if traces_from_block is None else traces_from_block
        )
        self.max_safes = max_safes
        self.dry_run = dry_run

        # The service timeouts protect the API from slow queries and lock queues.
        # This command scans millions of rows and waits on rows the workers lock,
        # so the timeouts are disabled for its own database session
        with connection.cursor() as cursor:
            cursor.execute("SET statement_timeout = 0")
            cursor.execute("SET lock_timeout = 0")

        self.ethereum_client = get_auto_ethereum_client()
        # Safes whose processed data changed in the repair steps, always checked
        # against the logs when fixing the Safes
        self.safes_to_fix: set[ChecksumAddress] = set()

        if self.dry_run:
            self.stdout.write(self.style.WARNING("Dry run, nothing will be changed"))
        self.stdout.write(f"Range: blocks {self.from_block}-{self.to_block}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @cached_property
    def tracing_indexer(self) -> InternalTxIndexer:
        """
        :return: A new indexer instance, so its already-processed cache does not
            interfere with the one of the running indexer. Its client is the
            tracing node, which also serves historical state
        """
        if not settings.ETHEREUM_TRACING_NODE_URL:
            raise CommandError("ETHEREUM_TRACING_NODE_URL is required")
        return InternalTxIndexerProvider.get_new_instance()

    @cached_property
    def master_copies(self) -> set[ChecksumAddress]:
        """
        :return: Monitored Safe singletons
        """
        return self.tracing_indexer._get_master_copy_addresses()

    @cached_property
    def tx_processor(self) -> SafeTxProcessor:
        return cast(SafeTxProcessor, SafeTxProcessorProvider())

    @cached_property
    def execution_topics(self) -> list[HexStr]:
        """
        :return: ``topic0`` of every ``ExecutionSuccess``/``ExecutionFailure`` version
        """
        return [
            to_0x_hex_str(topic)
            for topic in self.tx_processor.safe_tx_execution_events_topics
        ]

    @cached_property
    def safes_with_repeated_executed_nonces(self) -> set[ChecksumAddress]:
        """
        :return: Safes with more than one executed ``MultisigTransaction`` for the same
            nonce. A nonce is executed once on chain, so these Safes were processed
            with a wrong nonce
        """
        self.stdout.write("Looking for Safes with repeated executed nonces")
        return {
            row["safe"]
            for row in MultisigTransaction.objects.executed()
            .values("safe", "nonce")
            .annotate(repeated=Count("safe_tx_hash"))
            .filter(repeated__gt=1)
            .order_by()
        }

    @cached_property
    def safes_with_wrong_executions_in_range(self) -> set[ChecksumAddress]:
        """
        :return: Safes with an executed ``MultisigTransaction`` in the range whose hash
            is not in the execution logs of its tx. Safes with repeated executed
            nonces are already known and not checked again
        """
        self.stdout.write("Looking for Safes with wrong executions in the range")
        safes: set[ChecksumAddress] = set()
        # Inside a transaction the cursor is not materialized in the database, the
        # rows are streamed with the logs of their tx
        with transaction.atomic():
            for safe, safe_tx_hash, logs in (
                MultisigTransaction.objects.executed()
                .filter(
                    ethereum_tx__block_id__gte=self.from_block,
                    ethereum_tx__block_id__lte=self.to_block,
                )
                .exclude(safe__in=self.safes_with_repeated_executed_nonces)
                .values_list("safe", "safe_tx_hash", "ethereum_tx__logs")
                .iterator(chunk_size=2_000)
            ):
                if safe in safes:
                    continue
                executed_hashes = self._executed_safe_tx_hashes(logs or [], safe)
                if executed_hashes and HexBytes(safe_tx_hash) not in executed_hashes:
                    safes.add(safe)
        return safes

    @cached_property
    def safes_with_executions_in_range(self) -> set[ChecksumAddress]:
        """
        :return: Safes with a decoded ``execTransaction`` in the range. They can have
            a shifted nonce without any wrong multisig tx found yet
        """
        self.stdout.write("Looking for Safes with executions in the range")
        return set(
            self._internal_txs_in_range()
            .filter(decoded_tx__function_name="execTransaction")
            .values_list("_from", flat=True)
            .distinct()
        )

    @cached_property
    def range_start_timestamp(self) -> datetime | None:
        """
        :return: Timestamp of the first block of the range in the database
        """
        return (
            EthereumBlock.objects.since_block(self.from_block)
            .order_by("number")
            .values_list("timestamp", flat=True)
            .first()
        )

    def _internal_txs_in_range(self) -> QuerySet[InternalTx]:
        """
        :return: ``InternalTx`` queryset for the block range. ``InternalTx`` has no
            index on ``block_number`` but it has one on ``timestamp``, so the range is
            also filtered by the block timestamps when the blocks are in the database
        """
        queryset = InternalTx.objects.filter(
            block_number__gte=self.from_block, block_number__lte=self.to_block
        )
        bounds = (
            EthereumBlock.objects.since_block(self.from_block)
            .until_block(self.to_block)
            .aggregate(min_timestamp=Min("timestamp"), max_timestamp=Max("timestamp"))
        )
        if bounds["min_timestamp"] is not None:
            queryset = queryset.filter(
                timestamp__gte=bounds["min_timestamp"],
                timestamp__lte=bounds["max_timestamp"],
            )
        return queryset

    def _add_safes_to_fix(self, safes: Iterable[ChecksumAddress]) -> None:
        """
        Mark the Safes to be fixed. Every new Safe is printed when it is found, so the
        list is not lost if the command fails before fixing the Safes

        :param safes:
        """
        for safe in sorted(set(safes) - self.safes_to_fix):
            self.stdout.write(f"Safe to fix: {safe}")
            self.safes_to_fix.add(safe)

    def _add_safes_of_decoded_to_fix(self, decoded_queryset: QuerySet) -> None:
        """
        Mark the Safes of the given ``InternalTxDecoded`` rows to be fixed

        :param decoded_queryset:
        """
        self._add_safes_to_fix(
            decoded_queryset.values_list("safe_address", flat=True).distinct()
        )

    def _progress(self, current: int, total: int, label: str) -> None:
        """
        Print progress every ``PROGRESS_EVERY`` elements

        :param current: Elements processed
        :param total: Elements to process
        :param label: Text printed before the counter
        """
        if current % PROGRESS_EVERY == 0:
            self.stdout.write(f"{label} {current}/{total}")

    def _executed_safe_tx_hashes(
        self, logs: Iterable[dict[str, Any] | None], safe_address: ChecksumAddress
    ) -> set[bytes]:
        """
        :param logs: Logs of a tx, as stored in ``EthereumTx.logs``
        :param safe_address:
        :return: ``safe_tx_hash`` of every ``ExecutionSuccess``/``ExecutionFailure`` of
            the Safe in the tx. The hash is ``topics[1]`` from v1.4.1 and the first 32
            bytes of ``data`` before
        """
        execution_topics = self.tx_processor.safe_tx_execution_events_topics
        hashes: set[bytes] = set()
        for log in logs:
            if not log:
                continue
            topics = log.get("topics") or []
            if not topics or HexBytes(topics[0]) not in execution_topics:
                continue
            if "address" in log and log["address"].lower() != safe_address.lower():
                continue
            if len(topics) == 2:
                hashes.add(bytes(HexBytes(topics[1])))
            elif log.get("data"):
                hashes.add(bytes(HexBytes(log["data"])[:32]))
        return hashes

    def _chain_nonces(
        self, safes_and_blocks: list[tuple[ChecksumAddress, int]]
    ) -> list[int | None]:
        """
        :param safes_and_blocks: Safe and the block to read its nonce at
        :return: Safe nonce on chain at the end of each block, ``None`` if the call
            fails
        """
        payload = [
            {
                "jsonrpc": "2.0",
                "id": i,
                "method": "eth_call",
                "params": [{"to": safe, "data": NONCE_SELECTOR}, hex(block)],
            }
            for i, (safe, block) in enumerate(safes_and_blocks)
        ]
        nonces: list[int | None] = []
        client = self.tracing_indexer.ethereum_client
        for payload_chunk in chunks(payload, RPC_BATCH_SIZE):
            try:
                results = list(client.raw_batch_request(payload_chunk))
            except (OSError, ValueError):
                # A batch fails if one of its calls fails, so the calls are sent
                # one by one to only lose the failing ones
                results = []
                for request in payload_chunk:
                    try:
                        results.extend(client.raw_batch_request([request]))
                    except (OSError, ValueError) as exc:
                        self.stdout.write(
                            self.style.WARNING(
                                f"Cannot get the nonce of {request['params'][0]['to']} "
                                f"at block {int(request['params'][1], 16)}: {exc}"
                            )
                        )
                        results.append(None)
            nonces.extend(
                int(cast(str, result), 16) if result and result != "0x" else None
                for result in results
            )
        return nonces

    def _safe_summaries(
        self, safes: list[ChecksumAddress]
    ) -> dict[ChecksumAddress, SafeSummary]:
        """
        :param safes:
        :return: Data for the quick check of every Safe, reading the nonces on chain
            in one batch
        """
        summaries = {address: SafeSummary() for address in safes}
        for address in InternalTxDecoded.objects.safes_pending_to_be_processed().filter(
            safe_address__in=safes
        ):
            summaries[address].pending = True
        for last_status in SafeLastStatus.objects.filter(
            address__in=safes
        ).select_related("internal_tx"):
            summary = summaries[last_status.address]
            summary.db_nonce = last_status.nonce
            summary.status_block = last_status.internal_tx.block_number
        for row in (
            MultisigTransaction.objects.executed()
            .filter(safe__in=safes)
            .values("safe")
            .annotate(
                rows=Count("safe_tx_hash"),
                nonces=Count("nonce", distinct=True),
                max_nonce=Max("nonce"),
            )
            .order_by()
        ):
            summary = summaries[row["safe"]]
            summary.executed_rows = row["rows"]
            summary.executed_nonces = row["nonces"]
            summary.max_executed_nonce = row["max_nonce"]

        with_status = [
            (address, summary.status_block)
            for address, summary in summaries.items()
            if summary.status_block is not None
        ]
        for (address, _), chain_nonce in zip(
            with_status, self._chain_nonces(with_status), strict=True
        ):
            summaries[address].chain_nonce = chain_nonce
        return summaries

    # ------------------------------------------------------------------
    # Step 1: empty logs
    # ------------------------------------------------------------------
    def _update_execution_results(
        self, ethereum_txs: dict[HexStr, EthereumTx]
    ) -> tuple[int, int]:
        """
        Recalculate the fields built from the logs: ``failed`` and ``payment`` of the
        multisig txs and ``failed`` of the module txs of the provided txs

        :param ethereum_txs: Txs with the new logs, by tx hash
        :return: Number of multisig txs and module txs changed
        """
        multisig_txs = []
        wrong_safes: set[ChecksumAddress] = set()
        for multisig_tx in MultisigTransaction.objects.filter(
            ethereum_tx__in=list(ethereum_txs)
        ).only("safe_tx_hash", "safe", "ethereum_tx", "failed", "payment"):
            ethereum_tx = ethereum_txs[multisig_tx.ethereum_tx_id]
            executed_hashes = self._executed_safe_tx_hashes(
                ethereum_tx.logs, multisig_tx.safe
            )
            if executed_hashes and HexBytes(multisig_tx.safe_tx_hash) not in (
                executed_hashes
            ):
                # Processed with a wrong nonce, the Safe must be fixed
                wrong_safes.add(multisig_tx.safe)
            result = self.tx_processor.get_execution_result(
                ethereum_tx, multisig_tx.safe_tx_hash
            )
            if result != (multisig_tx.failed, multisig_tx.payment):
                multisig_tx.failed, multisig_tx.payment = result
                multisig_txs.append(multisig_tx)

        module_txs = []
        for (
            internal_tx_id,
            tx_hash,
            module,
            safe,
            failed,
        ) in ModuleTransaction.objects.filter(
            internal_tx__ethereum_tx__in=list(ethereum_txs)
        ).values_list(
            "internal_tx_id", "internal_tx__ethereum_tx", "module", "safe", "failed"
        ):
            is_failed = self.tx_processor.is_module_failed(
                ethereum_txs[tx_hash], module, safe
            )
            if is_failed != failed:
                module_txs.append(
                    ModuleTransaction(internal_tx_id=internal_tx_id, failed=is_failed)
                )

        self._add_safes_to_fix(wrong_safes)
        # `bulk_update` does not change `modified` or send signals, so no events are
        # sent for these old txs
        if not self.dry_run:
            MultisigTransaction.objects.bulk_update(multisig_txs, ["failed", "payment"])
            ModuleTransaction.objects.bulk_update(module_txs, ["failed"])
        return len(multisig_txs), len(module_txs)

    def fix_empty_logs(self) -> None:
        """
        The bad node returned receipts without logs, so the receipt of every tx
        without logs is fetched again. A tx can also have no logs on chain (e.g.
        old Safe versions emit no events), those are left as they are.

        Logs are only used for ``failed`` and ``payment``, so those fields are
        recalculated and the Safes do not need to be processed again.
        """
        self.stdout.write(self.style.MIGRATE_HEADING("Fixing txs without logs"))
        tx_hashes = list(
            EthereumTx.objects.filter(
                block_id__gte=self.from_block, block_id__lte=self.to_block
            )
            .alias(
                logs_size=Func(
                    F("logs"), function="pg_column_size", output_field=IntegerField()
                )
            )
            .filter(logs_size__lt=EMPTY_LOGS_MAX_SIZE, logs__len=0)
            .order_by("block_id")
            .values_list("tx_hash", flat=True)
        )
        total = len(tx_hashes)
        self.stdout.write(f"Found {total} txs without logs")
        counters = Counter[str]()
        checked = 0
        for tx_hashes_chunk in chunks(tx_hashes, RPC_BATCH_SIZE):
            receipts = self.ethereum_client.get_transaction_receipts(tx_hashes_chunk)
            updated: dict[HexStr, EthereumTx] = {}
            for tx_hash, receipt in zip(tx_hashes_chunk, receipts, strict=True):
                checked += 1
                self._progress(checked, total, "Checked receipts")
                if not receipt:
                    counters["missing_receipt"] += 1
                elif logs := [
                    clean_receipt_log(log) for log in receipt.get("logs", [])
                ]:
                    updated[tx_hash] = EthereumTx(tx_hash=tx_hash, logs=logs)
                else:
                    counters["still_empty"] += 1
            counters["updated"] += len(updated)

            multisig_changed, module_changed = self._update_execution_results(updated)
            counters["multisig_txs_changed"] += multisig_changed
            counters["module_txs_changed"] += module_changed
            if not self.dry_run:
                EthereumTx.objects.bulk_update(updated.values(), ["logs"])

        action = "Would update" if self.dry_run else "Updated"
        self.stdout.write(
            self.style.SUCCESS(
                f"{action} logs of {counters['updated']}/{total} txs. "
                f"still_empty={counters['still_empty']}, "
                f"missing_receipt={counters['missing_receipt']}, "
                f"multisig_txs_changed={counters['multisig_txs_changed']}, "
                f"module_txs_changed={counters['module_txs_changed']}"
            )
        )

    # ------------------------------------------------------------------
    # Traces sync, used by steps 2 and 3
    # ------------------------------------------------------------------
    @staticmethod
    def _trace_key(internal_tx: InternalTx) -> tuple:
        """
        :param internal_tx: Stored or built from a trace
        :return: Hashable key with the ``TRACE_KEY_FIELDS`` values
        """
        return tuple(
            bytes(value)
            if field == "data" and (value := getattr(internal_tx, field)) is not None
            else getattr(internal_tx, field)
            for field in TRACE_KEY_FIELDS
        )

    def _get_traces(
        self, tx_hashes: Sequence[bytes | str]
    ) -> list[list[FilterTrace]] | None:
        """
        :param tx_hashes:
        :return: Traces of every tx from ``trace_transaction``, ``None`` if the node
            fails
        """
        try:
            return self.tracing_indexer.trace_transactions(
                [to_0x_hex_str(HexBytes(tx_hash)) for tx_hash in tx_hashes],
                batch_size=RPC_BATCH_SIZE,
            )
        except (OSError, ValueError, Web3RPCError) as exc:
            self.stdout.write(
                self.style.ERROR(
                    f"Cannot get traces for {len(tx_hashes)} txs, run the command "
                    f"again for them: {exc}"
                )
            )
            return None

    def _sync_traces(
        self, traces_per_tx: dict[bytes, list[FilterTrace]]
    ) -> tuple[int, int]:
        """
        Make the stored traces of the txs match ``trace_transaction``: delete the
        stored traces that are not on chain and index the relevant traces that are
        missing. A trace stored under a wrong ``trace_address`` takes the place of
        the real trace, which would be ignored when the tx is indexed again. It runs
        in one transaction, so a tx is never left with its traces deleted and the
        real ones not indexed.

        :param traces_per_tx: Traces on chain per tx hash. Txs not stored are indexed
        :return: Number of stored traces deleted and number of txs indexed
        """
        tracing = self.tracing_indexer.ethereum_client.tracing
        tx_hashes = [to_0x_hex_str(tx_hash) for tx_hash in traces_per_tx]
        rows_per_tx: dict[str, list[InternalTx]] = defaultdict(list)
        for row in InternalTx.objects.filter(ethereum_tx__in=tx_hashes).only(
            "ethereum_tx", *TRACE_KEY_FIELDS
        ):
            rows_per_tx[row.ethereum_tx_id].append(row)
        ethereum_txs = {
            ethereum_tx.tx_hash: ethereum_tx
            for ethereum_tx in EthereumTx.objects.filter(tx_hash__in=tx_hashes)
            .select_related("block")
            .only("tx_hash", "block_id", "block__timestamp")
        }

        wrong_ids: list[int] = []
        to_index: OrderedDict[bytes, list[FilterTrace]] = OrderedDict()
        for tx_hash, traces in traces_per_tx.items():
            # The traces indexer only indexes txs that call a Safe singleton. Other
            # txs are stored for other reasons (e.g. ERC20 transfers) and their
            # traces must not be indexed
            if not self.tracing_indexer._extract_safe_proxies(
                traces, self.master_copies
            ):
                continue
            ethereum_tx = ethereum_txs.get(to_0x_hex_str(tx_hash))
            if not ethereum_tx:
                to_index[tx_hash] = traces
                continue
            # Errored traces are never stored
            keys_on_chain = {
                self._trace_key(
                    InternalTx.objects.build_from_trace(trace, ethereum_tx)
                ): trace
                for trace in cast(
                    list[FilterTrace], tracing.filter_out_errored_traces(traces)
                )
            }
            stored = rows_per_tx[ethereum_tx.tx_hash]
            stored_keys = {self._trace_key(row) for row in stored}
            tx_wrong_ids = [
                row.pk for row in stored if self._trace_key(row) not in keys_on_chain
            ]
            is_missing_traces = any(
                key not in stored_keys and InternalTxIndexer.is_relevant_trace(trace)
                for key, trace in keys_on_chain.items()
            )
            if tx_wrong_ids or is_missing_traces:
                self.stdout.write(
                    f"Tx {ethereum_tx.tx_hash}: deleting {len(tx_wrong_ids)} traces "
                    f"not on chain, missing traces={is_missing_traces}"
                )
                wrong_ids.extend(tx_wrong_ids)
                to_index[tx_hash] = traces

        # Deleting a decoded trace deletes the status built from it
        self._add_safes_of_decoded_to_fix(
            InternalTxDecoded.objects.filter(internal_tx_id__in=wrong_ids)
        )
        if to_index and not self.dry_run:
            with transaction.atomic():
                InternalTx.objects.filter(pk__in=wrong_ids).delete()
                # `process_elements` receives the list of traces of every tx, as
                # returned by `trace_transactions`
                self.tracing_indexer.process_elements(to_index)  # type: ignore[arg-type]
            # Decoded txs indexed now are processed by the workers out of order,
            # the Safes must be checked
            self._add_safes_of_decoded_to_fix(
                InternalTxDecoded.objects.filter(
                    internal_tx__ethereum_tx__in=[
                        to_0x_hex_str(tx_hash) for tx_hash in to_index
                    ],
                    processed=False,
                )
            )
        return len(wrong_ids), len(to_index)

    # ------------------------------------------------------------------
    # Step 2: missing executions
    # ------------------------------------------------------------------
    def _get_execution_logs(
        self, from_block: int, to_block: int, failed_blocks: list[int]
    ) -> list[LogReceipt]:
        """
        ``eth_getLogs`` for every ``ExecutionSuccess``/``ExecutionFailure`` in the range,
        splitting the range in two when the node rejects it. A single block that keeps
        failing is skipped and added to ``failed_blocks``, so the rest of the range is
        still checked.

        :param from_block:
        :param to_block:
        :param failed_blocks: Blocks that could not be read
        :return: Execution logs in the range
        """
        try:
            return self.ethereum_client.slow_w3.eth.get_logs(
                {
                    "fromBlock": from_block,
                    "toBlock": to_block,
                    "topics": [self.execution_topics],
                }
            )
        except (OSError, ValueError, Web3RPCError) as exc:
            if from_block == to_block:
                self.stdout.write(
                    self.style.ERROR(
                        f"Cannot get logs for block {from_block}, skipping it: {exc}"
                    )
                )
                failed_blocks.append(from_block)
                return []
            middle = (from_block + to_block) // 2
            self.stdout.write(
                self.style.WARNING(
                    f"Cannot get logs for blocks {from_block}-{to_block}, "
                    f"splitting the range: {exc}"
                )
            )
            return self._get_execution_logs(
                from_block, middle, failed_blocks
            ) + self._get_execution_logs(middle + 1, to_block, failed_blocks)

    def _find_missing_executions(
        self, logs: Iterable[LogReceipt]
    ) -> dict[HexBytes, set[ChecksumAddress]]:
        """
        :param logs: Execution logs on chain
        :return: Safes per tx hash with more executions on chain than decoded
            ``execTransaction`` in the database. Executions are counted, as a tx
            can execute several txs of the same Safe
        """
        on_chain: Counter[tuple[HexBytes, ChecksumAddress]] = Counter(
            (
                HexBytes(log["transactionHash"]),
                fast_to_checksum_address(log["address"]),
            )
            for log in logs
        )
        tx_hashes = list({tx_hash for tx_hash, _ in on_chain})

        in_db: Counter[tuple[HexBytes, ChecksumAddress]] = Counter()
        for tx_hashes_chunk in chunks(tx_hashes, 1_000):
            in_db.update(
                (HexBytes(tx_hash), safe_address)
                for tx_hash, safe_address in InternalTxDecoded.objects.filter(
                    function_name="execTransaction",
                    internal_tx__ethereum_tx__in=tx_hashes_chunk,
                ).values_list("internal_tx__ethereum_tx", "safe_address")
            )

        missing: dict[HexBytes, set[ChecksumAddress]] = defaultdict(set)
        for (tx_hash, safe), executions in on_chain.items():
            if executions > in_db[(tx_hash, safe)]:
                missing[tx_hash].add(safe)
        return dict(missing)

    def index_missing_executions(self) -> None:
        """
        ``trace_filter`` of a bad node can skip txs. Execution events are the source
        of truth for executions, so the traces of every tx with an execution on chain
        and no decoded ``execTransaction`` are synced with ``trace_transaction``,
        also when the tx is not stored.
        """
        self.stdout.write(self.style.MIGRATE_HEADING("Indexing missing executions"))
        counters = Counter[str]()
        failed_blocks: list[int] = []
        not_monitored: set[ChecksumAddress] = set()

        for chunk_from in range(self.from_block, self.to_block + 1, LOGS_BLOCK_CHUNK):
            chunk_to = min(chunk_from + LOGS_BLOCK_CHUNK - 1, self.to_block)
            logs = self._get_execution_logs(chunk_from, chunk_to, failed_blocks)
            counters["executions_on_chain"] += len(logs)
            missing = self._find_missing_executions(logs)
            self.stdout.write(
                f"Blocks {chunk_from}-{chunk_to}: {len(logs)} executions on chain, "
                f"{len(missing)} txs with missing executions"
            )
            if not missing:
                continue

            traces_per_tx = self._get_traces(list(missing))
            if traces_per_tx is None:
                counters["unverifiable"] += sum(
                    len(safes) for safes in missing.values()
                )
                continue
            to_sync: OrderedDict[bytes, list[FilterTrace]] = OrderedDict()
            for (tx_hash, missing_safes), traces in zip(
                missing.items(), traces_per_tx, strict=True
            ):
                if not traces:
                    counters["unverifiable"] += len(missing_safes)
                    self.stdout.write(
                        self.style.WARNING(
                            f"Node returned no traces for tx {to_0x_hex_str(tx_hash)} "
                            f"with executions of {sorted(missing_safes)}, skipping"
                        )
                    )
                    continue
                safe_proxies = self.tracing_indexer._extract_safe_proxies(
                    traces, self.master_copies
                )
                monitored_safes = missing_safes & safe_proxies
                not_monitored.update(missing_safes - safe_proxies)
                if not monitored_safes:
                    continue
                counters["missing"] += len(monitored_safes)
                self._add_safes_to_fix(monitored_safes)
                self.stdout.write(
                    f"Missing execution in tx {to_0x_hex_str(tx_hash)} for Safes "
                    f"{sorted(monitored_safes)}"
                )
                to_sync[tx_hash] = traces

            if to_sync:
                deleted, indexed = self._sync_traces(to_sync)
                counters["wrong_traces"] += deleted
                counters["indexed"] += indexed

        if not_monitored:
            self.stdout.write(
                self.style.WARNING(
                    f"{len(not_monitored)} Safes with executions on chain use a singleton "
                    f"that is not monitored, ignored: {sorted(not_monitored)[:20]}"
                )
            )
        if failed_blocks:
            self.stdout.write(
                self.style.ERROR(
                    f"{len(failed_blocks)} blocks could not be checked, run the command "
                    f"again for them: {failed_blocks[:20]}"
                )
            )
        action = "Would index" if self.dry_run else "Indexed"
        self.stdout.write(
            self.style.SUCCESS(
                f"{action} {counters['indexed']} txs with missing executions. "
                f"executions_on_chain={counters['executions_on_chain']}, "
                f"missing_executions={counters['missing']}, "
                f"wrong_traces_deleted={counters['wrong_traces']}, "
                f"unverifiable={counters['unverifiable']}, "
                f"failed_blocks={len(failed_blocks)}, "
                f"safes_to_fix={len(self.safes_to_fix)}"
            )
        )

    # ------------------------------------------------------------------
    # Step 3: traces of every tx of the range
    # ------------------------------------------------------------------
    def sync_range_traces(self) -> None:
        """
        Sync the traces of every stored tx of the range with ``trace_transaction``,
        a window of blocks at a time. The last synced block is printed after every
        window, so a stopped run can continue with ``--traces-from-block``.
        """
        self.stdout.write(self.style.MIGRATE_HEADING("Syncing the traces of the range"))
        counters = Counter[str]()
        for window_from in range(
            self.traces_from_block, self.to_block + 1, TRACES_BLOCK_WINDOW
        ):
            window_to = min(window_from + TRACES_BLOCK_WINDOW - 1, self.to_block)
            tx_hashes = list(
                EthereumTx.objects.filter(
                    block_id__gte=window_from, block_id__lte=window_to
                )
                .order_by("block_id")
                .values_list("tx_hash", flat=True)
            )
            for tx_hashes_chunk in chunks(tx_hashes, RPC_BATCH_SIZE * 10):
                traces_per_tx = self._get_traces(tx_hashes_chunk)
                if traces_per_tx is None:
                    counters["unverifiable"] += len(tx_hashes_chunk)
                    continue
                to_sync: OrderedDict[bytes, list[FilterTrace]] = OrderedDict()
                for tx_hash, traces in zip(tx_hashes_chunk, traces_per_tx, strict=True):
                    # Every stored tx has traces, the node failed to return them
                    if traces:
                        to_sync[HexBytes(tx_hash)] = traces
                    else:
                        counters["unverifiable"] += 1
                deleted, changed = self._sync_traces(to_sync)
                counters["deleted"] += deleted
                counters["changed_txs"] += changed
            counters["txs"] += len(tx_hashes)
            self.stdout.write(
                f"Traces synced up to block {window_to}: txs={counters['txs']}, "
                f"changed_txs={counters['changed_txs']}, deleted={counters['deleted']}, "
                f"unverifiable={counters['unverifiable']}"
            )

        action = "Would change" if self.dry_run else "Changed"
        self.stdout.write(
            self.style.SUCCESS(
                f"{action} the traces of {counters['changed_txs']}/{counters['txs']} "
                f"txs. traces_deleted={counters['deleted']}, "
                f"unverifiable={counters['unverifiable']}, "
                f"safes_to_fix={len(self.safes_to_fix)}"
            )
        )

    # ------------------------------------------------------------------
    # Step 4: fix the Safes
    # ------------------------------------------------------------------
    def check_safe(self, address: ChecksumAddress, summary: SafeSummary) -> SafeRepair:
        """
        Find the wrong multisig txs of the Safe and how to fix its state.

        An executed multisig tx is wrong if its tx has execution events of the Safe
        and none of them has its ``safe_tx_hash``. A tx without execution events of
        the Safe cannot be checked, so its multisig tx is kept.

        The Safe state is right if the nonce of its last status is the nonce on
        chain at the block of that status, and every nonce has a right multisig tx.
        If not, it is processed again from the range start when its last status
        before the range is right, or from its ``setup`` otherwise.

        :param address:
        :param summary: Result of the quick check of the Safe
        :return: Action and rows to change
        """
        if SafeContract.objects.banned([address]).exists():
            return SafeRepair(address, SafeAction.SKIP_BANNED)
        if summary.state == SafeState.NODE_ERROR:
            return SafeRepair(address, SafeAction.SKIP_NODE_ERROR)

        repair = SafeRepair(
            address,
            SafeAction.OK,
            db_nonce=summary.db_nonce,
            chain_nonce=summary.chain_nonce,
        )
        correct_nonces: set[int] = set()
        # Inside a transaction the cursor is not materialized in the database, the
        # rows are streamed with the logs of their tx
        with transaction.atomic():
            for (
                safe_tx_hash,
                nonce,
                created,
                origin,
                proposer,
                logs,
                block_timestamp,
            ) in (
                MultisigTransaction.objects.executed()
                .filter(safe=address)
                .values_list(
                    "safe_tx_hash",
                    "nonce",
                    "created",
                    "origin",
                    "proposer",
                    "ethereum_tx__logs",
                    "ethereum_tx__block__timestamp",
                )
                .iterator(chunk_size=2_000)
            ):
                executed_hashes = self._executed_safe_tx_hashes(logs or [], address)
                if not executed_hashes or HexBytes(safe_tx_hash) in executed_hashes:
                    correct_nonces.add(nonce)
                elif created == block_timestamp and origin == {} and proposer is None:
                    # Created by the indexer when processing the tx
                    repair.to_delete.append(bytes(HexBytes(safe_tx_hash)))
                else:
                    # Proposed by a user, its data cannot be rebuilt
                    repair.to_unlink.append(safe_tx_hash)
        repair.correct_nonces = len(correct_nonces)

        has_wrong_rows = bool(repair.to_delete or repair.to_unlink)
        if summary.state == SafeState.PROCESSING:
            # The workers rebuild the state, only the wrong multisig txs are deleted
            repair.action = (
                SafeAction.CLEAN if has_wrong_rows else SafeAction.SKIP_PROCESSING
            )
        elif (
            summary.state != SafeState.NO_STATUS
            and summary.db_nonce == summary.chain_nonce
            and repair.correct_nonces >= cast(int, summary.chain_nonce)
        ):
            if has_wrong_rows:
                repair.action = SafeAction.CLEAN
        elif self._status_before_range_is_right(address):
            repair.action = SafeAction.RESET_FROM_RANGE
        else:
            repair.action = SafeAction.RESET_FROM_SETUP
        if (
            repair.action == SafeAction.RESET_FROM_SETUP
            and not InternalTxDecoded.objects.filter(
                safe_address=address, function_name="setup"
            ).exists()
        ):
            repair.action = SafeAction.SKIP_NO_SETUP
        return repair

    def _status_before_range_is_right(self, address: ChecksumAddress) -> bool:
        """
        :param address:
        :return: ``True`` if the last ``SafeStatus`` before the range has the nonce on
            chain at its block. Searched through the ``InternalTx`` ``_from`` index
        """
        if not self.range_start_timestamp:
            return False
        internal_tx = (
            InternalTx.objects.filter(
                _from=address,
                timestamp__lt=self.range_start_timestamp,
                safe_status__isnull=False,
            )
            .select_related("safe_status")
            .order_by("-timestamp", "-id")
            .first()
        )
        if not internal_tx:
            return False
        return (
            self._chain_nonces([(address, internal_tx.block_number)])[0]
            == internal_tx.safe_status.nonce
        )

    @staticmethod
    def _in_batches(queryset: QuerySet, lock: Lock) -> Iterator[list[Any]]:
        """
        :param queryset: Rows to change, read once with one index scan
        :param lock: Safe processing lock, renewed after every batch
        :return: Primary keys of the rows in batches of ``DB_BATCH_SIZE``
        """
        for pks in chunks(list(queryset.values_list("pk", flat=True)), DB_BATCH_SIZE):
            yield pks
            lock.reacquire()

    @staticmethod
    def _delete_multisig_txs(safe_tx_hashes: list[bytes], lock: Lock) -> None:
        """
        Delete the ``MultisigTransaction`` rows and their confirmations in batches.

        Raw SQL is used because the ORM loads every row to send its ``post_delete``
        signal, which only publishes events for txs modified in the last minutes and
        the deleted txs are old.

        :param safe_tx_hashes:
        :param lock: Safe processing lock, renewed after every batch
        """
        confirmation_table = MultisigConfirmation._meta.db_table
        multisig_tx_table = MultisigTransaction._meta.db_table
        for hashes in chunks(safe_tx_hashes, DB_BATCH_SIZE):
            with transaction.atomic(), connection.cursor() as cursor:
                # Confirmations cascade from Django, not from the database
                cursor.execute(
                    f"DELETE FROM {confirmation_table} "
                    "WHERE multisig_transaction_id = ANY(%s)",
                    [hashes],
                )
                cursor.execute(
                    f"DELETE FROM {multisig_tx_table} WHERE safe_tx_hash = ANY(%s)",
                    [hashes],
                )
            lock.reacquire()

    def _apply(self, repair: SafeRepair, lock: Lock) -> None:
        """
        Delete and unlink the wrong multisig txs and, for a reset, mark the decoded
        txs of the Safe as not processed and delete its statuses from the reset
        point. The workers then process the Safe again from the last status before
        that point.

        :param repair:
        :param lock: Safe processing lock, renewed after every batch
        """
        address = repair.address
        self._delete_multisig_txs(repair.to_delete, lock)
        for safe_tx_hashes in chunks(repair.to_unlink, DB_BATCH_SIZE):
            MultisigTransaction.objects.filter(safe_tx_hash__in=safe_tx_hashes).update(
                ethereum_tx=None, failed=None, payment=None
            )
        if repair.action in RESET_ACTIONS:
            statuses = SafeStatus.objects.filter(address=address)
            # Filtering by `_from` uses its `InternalTx` index, `safe_address` only
            # has partial indexes that do not cover processed rows
            decoded = InternalTxDecoded.objects.filter(
                internal_tx___from=address, safe_address=address, processed=True
            )
            if repair.action == SafeAction.RESET_FROM_RANGE:
                statuses = statuses.filter(
                    internal_tx__timestamp__gte=self.range_start_timestamp
                )
                decoded = decoded.filter(
                    internal_tx__timestamp__gte=self.range_start_timestamp
                )
            # Decoded txs are marked first: if the command stops before deleting the
            # statuses, the workers find txs out of order and reset the Safe
            for pks in self._in_batches(decoded, lock):
                InternalTxDecoded.objects.filter(pk__in=pks).update(processed=False)
            for pks in self._in_batches(statuses, lock):
                SafeStatus.objects.filter(pk__in=pks).delete()
            SafeLastStatus.objects.filter(address=address).delete()

        # The deletes and updates above send no signals, so the cached views of the
        # Safe are removed here
        for cache_tag in (
            CacheSafeTxsView.LIST_MULTISIGTRANSACTIONS_VIEW_CACHE_KEY,
            CacheSafeTxsView.LIST_MODULETRANSACTIONS_VIEW_CACHE_KEY,
        ):
            remove_cache_view_for_addresses(cache_tag, [address])

    def fix_safe(self, repair: SafeRepair) -> None:
        """
        Apply the repair holding the lock of the task that processes the Safe, so the
        workers do not process it while it is changed. The lock waits for a running
        task of the Safe to end.

        :param repair:
        :raises LockError: If the lock cannot be taken or is lost
        """
        lock_name = get_task_lock_name(
            process_decoded_internal_txs_for_safe_task.name,
            lock_name_suffix=repair.address,
        )
        with get_redis().lock(
            lock_name, timeout=SAFE_LOCK_TIMEOUT, blocking_timeout=LOCK_TIMEOUT
        ) as lock:
            self._apply(repair, lock)

    def fix_safes(self, addresses: list[ChecksumAddress] | None) -> None:
        """
        Check the affected Safes and fix the ones with wrong multisig txs or state.

        The provided Safes, the Safes with wrong multisig txs and the Safes changed
        by the repair steps are always checked against the logs. The other Safes
        with executions in the range are only checked against the logs when the
        quick check finds a problem.

        :param addresses: Safes to fix. If not provided, the Safes found as described
        """
        self.stdout.write(self.style.MIGRATE_HEADING("Fixing the Safes"))
        suspicious = set(addresses or ()) or (
            self.safes_with_repeated_executed_nonces
            | self.safes_with_wrong_executions_in_range
            | self.safes_to_fix
        )
        safes = addresses or sorted(suspicious | self.safes_with_executions_in_range)
        self.stdout.write(f"Checking {len(safes)} Safes")
        actions = Counter[SafeAction]()
        totals = Counter[str]()
        failed: list[ChecksumAddress] = []
        changed = checked = 0
        for safes_chunk in chunks(safes, RPC_BATCH_SIZE):
            if self.max_safes is not None and changed >= self.max_safes:
                break
            summaries = self._safe_summaries(safes_chunk)
            for address in safes_chunk:
                if self.max_safes is not None and changed >= self.max_safes:
                    break
                checked += 1
                summary = summaries[address]
                if address not in suspicious and summary.state == SafeState.OK:
                    actions[SafeAction.OK] += 1
                    continue
                repair = self.check_safe(address, summary)
                actions[repair.action] += 1
                self.stdout.write(
                    f"[{address}] action={repair.action} "
                    f"to_delete={len(repair.to_delete)} "
                    f"to_unlink={len(repair.to_unlink)} db_nonce={repair.db_nonce} "
                    f"chain_nonce={repair.chain_nonce} "
                    f"correct_nonces={repair.correct_nonces}"
                )
                if repair.action not in CHANGE_ACTIONS:
                    continue
                changed += 1
                totals["deleted"] += len(repair.to_delete)
                totals["unlinked"] += len(repair.to_unlink)
                if not self.dry_run:
                    try:
                        self.fix_safe(repair)
                    except LockError as exc:
                        failed.append(address)
                        self.stdout.write(
                            self.style.ERROR(f"[{address}] Cannot hold its lock: {exc}")
                        )
            self._progress(checked, len(safes), "Safes checked")

        if checked < len(safes):
            self.stdout.write(
                self.style.WARNING(
                    f"Reached --max-safes={self.max_safes}, run again with "
                    f"--only-fix-safes for the remaining {len(safes) - checked} Safes"
                )
            )
        for address in failed:
            self.stdout.write(self.style.ERROR(f"Safe not fixed, run again: {address}"))
        action = "Would fix" if self.dry_run else "Fixed"
        actions_text = ", ".join(
            f"{name}={count}" for name, count in sorted(actions.items())
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"{action} {changed - len(failed)} Safes. {actions_text}, "
                f"multisig_txs_deleted={totals['deleted']}, "
                f"multisig_txs_unlinked={totals['unlinked']}, failed={len(failed)}"
            )
        )
