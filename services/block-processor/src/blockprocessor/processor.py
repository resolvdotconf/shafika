import os
import time
from datetime import datetime
from typing import cast

from common.db import SessionLocal
from common.failedjob import FailedJobManager
from common.queue import RedisQueueManager
from common.token import TokenMetadata
from dotenv import load_dotenv
from requests.exceptions import HTTPError
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from web3 import Web3
from web3.types import TxData

from db.models.models import (
    AddressStats,
    Block,
    Contract,
    JobType,
    Transaction,
    WorkerStatus,
)


class BlockProcessor:
    web3: Web3
    redis_client: RedisQueueManager
    queue_name: str
    failed_job: FailedJobManager

    def __init__(self, queue_name: str = "blocks"):
        load_dotenv()
        http_url = os.getenv("ETH_HTTP_URL")
        self.web3 = Web3(Web3.HTTPProvider(http_url))
        self.redis_client = RedisQueueManager()
        self.queue_name = queue_name
        self.failed_job = FailedJobManager(queue_name, JobType.BLOCK)
        self.token = TokenMetadata(self.web3)

    def run(self):
        print(f"Worker listening on queue '{self.queue_name}'...")
        while True:
            job_id, job = self.redis_client.bl_pop_block(self.queue_name)

            if not job:
                if job_id:
                    print(f"Job {job_id} data missing or expired")
                continue

            if not isinstance(job_id, str):
                continue

            block_number = job.get("block_number")
            block_hash = job.get("block_hash")
            block_status = job.get("status")
            is_retry = block_status == "retrying"

            if is_retry:
                print(f"Processing Block {block_number} (RETRY)")
            else:
                print(f"Processing Block {block_number}")

            try:
                self.process_block(block_number, block_hash, block_status)
                self.redis_client.delete_job(job_id)

                # If this was a retry, remove from failed_jobs table
                if is_retry:
                    if self.failed_job.remove_failed_job(job_id):
                        print(f"Removed {job_id} from failed_jobs table")
                    else:
                        print(f"Could not remove {job_id} from failed_jobs table")
            except Exception as e:
                print(f"Error processing block {block_number}: {e}")
                if self.failed_job.record(job_id, job, str(e)):
                    self.redis_client.delete_job(job_id)
                else:
                    print(
                        f"CRITICAL: Could not record failure for {job_id} - left in Redis"
                    )

    def _fetch_block_with_retry(self, block_number: int, max_retries: int = 5):
        """Fetch block from Web3 with exponential backoff for rate limiting."""
        for attempt in range(max_retries):
            try:
                return self.web3.eth.get_block(block_number, full_transactions=True)
            except HTTPError as e:
                if e.response.status_code == 429:
                    wait_time = (2**attempt) + (attempt * 0.5)  # Exponential backoff
                    print(
                        f"Rate limited (429) on block {block_number}, retrying in {wait_time:.1f}s (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    if attempt == max_retries - 1:
                        raise
                else:
                    raise
            except Exception:
                # For non-rate-limit errors, fail immediately
                raise

    def is_canonical(self, block_number: int, block_hash: str) -> bool:
        """
        Check if a given block hash is the canonical one at the given height.
        """
        try:
            # Fetch the current block at this height
            canonical_block = self.web3.eth.get_block(block_number)

            # Compare the hashes
            current_canonical_hash = canonical_block["hash"].hex()

            return current_canonical_hash == block_hash
        except Exception as e:
            print(f"Error checking canonical status: {e}")
            return False

    def process_block(self, block_number: int, block_hash: str, block_status: str):
        """Fetch block, parse txs, write to DB."""
        session = SessionLocal()
        block_record = None

        try:
            if block_status == "new":
                block_record = Block(
                    block_number=block_number,
                    block_hash=block_hash,
                    worker_status=WorkerStatus.PROCESSING,
                )
                session.add(block_record)

            else:
                block_record = (
                    session.query(Block)
                    .filter(Block.block_number == block_number)
                    .first()
                )

            block = self._fetch_block_with_retry(block_number)

            # Verify if the hash from the queue matches the actual canonical hash
            actual_hash = block["hash"].hex()
            is_canonical = actual_hash == block_hash

            if not is_canonical:
                print(
                    f"Warning: Block {block_number} reorg detected. Queue hash: {block_hash}, Canonical hash: {actual_hash}"
                )
                # Update block_hash to use the canonical one
                block_hash = actual_hash
                if block_record:
                    block_record.block_hash = actual_hash
                    block_record.canonical = True  # Since we fetched it from get_block(number), it IS canonical

            # If it matches, we can also explicitly set canonical=True
            if block_record:
                block_record.canonical = True

            # EIP-1559 Fields
            base_fee = block.get("baseFeePerGas")
            gas_used = block.get("gasUsed", 0)
            burned_eth = None

            if base_fee is not None:
                burned_eth = base_fee * gas_used
                if block_record:
                    block_record.base_fee_per_gas = base_fee
                    block_record.burned_eth = burned_eth

            block_ts = datetime.fromtimestamp(block["timestamp"])
            tx_count = len(block["transactions"])
            print(f"Processing {tx_count} txs from block {block_number}")

            address_updates = {}  # Collect stats to avoid deadlocks
            transactions_to_insert = []
            contracts_to_insert = []
            new_contract_addresses = set()

            for tx in block["transactions"]:
                tx_data = cast(TxData, tx)
                try:
                    # Check for contract creation
                    new_contract = self._check_contract_creation(
                        tx_data, block_number, block_ts, address_updates
                    )
                    if new_contract:
                        contracts_to_insert.append(new_contract)
                        new_contract_addresses.add(new_contract.contract_address)

                    tx_model = self._parse_transaction(
                        tx_data, block_number, block_hash, block_ts, base_fee
                    )
                    transactions_to_insert.append(tx_model)

                except Exception as e:
                    print(f"Error parsing tx {tx_data.get('hash', 'unknown')}: {e}")

            unique_to_addresses = {
                tx.to_address for tx in transactions_to_insert if tx.to_address
            }

            existing_contracts = set()
            if unique_to_addresses:
                results = (
                    session.query(Contract.contract_address)
                    .filter(Contract.contract_address.in_(unique_to_addresses))
                    .all()
                )
                existing_contracts = {row[0] for row in results}

            for tx_model in transactions_to_insert:
                if tx_model.from_address:
                    self._collect_address_stats(
                        address_updates,
                        tx_model.from_address,
                        block_number,
                        eth_sent=tx_model.value,
                    )

                if tx_model.to_address:
                    # Check if receiver is a contract (newly created OR existing in DB)
                    is_contract = (
                        tx_model.to_address in new_contract_addresses
                        or tx_model.to_address in existing_contracts
                    )

                    self._collect_address_stats(
                        address_updates,
                        tx_model.to_address,
                        block_number,
                        eth_received=tx_model.value,
                        is_contract=is_contract,
                    )

            # Flush everything in one go
            if contracts_to_insert:
                session.bulk_save_objects(contracts_to_insert)

            if transactions_to_insert:
                session.bulk_save_objects(transactions_to_insert)

            self._flush_address_stats(session, address_updates)

            if block_record:
                block_record.worker_status = WorkerStatus.DONE

            session.commit()
            print(f"Block {block_number} completed ({tx_count} txs)")

        except Exception as e:
            session.rollback()
            self._mark_error(session, block_record, block_number, e)
            raise
        finally:
            session.close()

    def _parse_transaction(
        self, tx: TxData, block_number, block_hash, block_ts, base_fee_per_gas=None
    ):
        """Parse a single transaction."""

        eth_price = self.token.get_eth_price(self.redis_client.client)

        tx_value = int(tx.get("value", 0))
        wei = self.web3.from_wei(tx_value, "ether")

        value_usd = float(wei) * eth_price if eth_price else None

        txn_type = int(tx.get("type", 0)) if tx.get("type") is not None else 0
        max_fee = int(tx.get("maxFeePerGas", 0)) if tx.get("maxFeePerGas") else None
        max_priority = (
            int(tx.get("maxPriorityFeePerGas", 0))
            if tx.get("maxPriorityFeePerGas")
            else None
        )
        gas_price = int(tx.get("gasPrice", 0))

        effective_gas_price = gas_price
        if txn_type == 2 and base_fee_per_gas is not None and max_fee is not None:
            priority_fee = max_priority if max_priority is not None else 0
            effective_gas_price = min(max_fee, base_fee_per_gas + priority_fee)

        return Transaction(
            tx_hash=tx["hash"].hex(),
            block_number=block_number,
            block_hash=block_hash,
            block_timestamp=block_ts,
            from_address=tx.get("from"),
            to_address=tx.get("to"),
            value=tx_value,
            value_usd=value_usd,
            gas_used=tx.get("gas"),
            gas_price=gas_price,
            effective_gas_price=effective_gas_price,
            max_fee_per_gas=max_fee,
            max_priority_fee_per_gas=max_priority,
            txn_type=txn_type,
            input=tx.get("input"),
            status=1,
        )

    def _check_contract_creation(
        self,
        tx: TxData,
        block_number,
        block_ts,
        address_updates: dict,
    ):
        """Check if transaction is a contract creation and return the model."""
        # Contract creation: transaction with no 'to' address
        if tx.get("to") is None:
            tx_hash = tx["hash"]

            try:
                receipt = self.web3.eth.get_transaction_receipt(tx_hash)
                contract_address = receipt.get("contractAddress")

                if contract_address:
                    bytecode = self.web3.eth.get_code(contract_address)
                    bytecode_hash = (
                        self.web3.keccak(bytecode).hex() if bytecode else None
                    )

                    contract = Contract(
                        contract_address=contract_address,
                        deployer_address=tx.get("from"),
                        deployment_tx_hash=tx_hash.hex(),
                        deployment_block_number=block_number,
                        deployment_timestamp=block_ts,
                        bytecode_hash=bytecode_hash,
                    )
                    print(f"  Contract deployed: {contract_address}")

                    # Update deployer's contract deployment count
                    deployer = tx.get("from")
                    if deployer:
                        self._collect_address_stats(
                            address_updates,
                            deployer,
                            block_number,
                            contract_deployment=True,
                        )
                    return contract
            except Exception as e:
                print(f"Error processing contract creation {tx_hash}: {e}")
        return None

    def _collect_address_stats(
        self,
        updates: dict,
        address: str,
        block_number: int,
        eth_received: int = 0,
        eth_sent: int = 0,
        is_contract: bool = False,
        contract_deployment: bool = False,
    ):
        """Collect stats in memory instead of writing to DB directly"""
        address_lower = address.lower()

        if address_lower not in updates:
            updates[address_lower] = {
                "first_seen_block": block_number,
                "last_seen_block": block_number,
                "tx_count": 0,
                "eth_received": 0,
                "eth_sent": 0,
                "contract_deployments": 0,
                "is_contract": is_contract,
            }

        stats = updates[address_lower]
        stats["last_seen_block"] = max(stats["last_seen_block"], block_number)
        stats["first_seen_block"] = min(stats["first_seen_block"], block_number)
        stats["tx_count"] += 1
        stats["eth_received"] += eth_received
        stats["eth_sent"] += eth_sent
        if contract_deployment:
            stats["contract_deployments"] += 1
        if is_contract:
            stats["is_contract"] = True

    def _flush_address_stats(self, session: Session, updates: dict):
        """Write collected stats to DB in a SINGLE Bulk Upsert to prevent deadlocks"""
        if not updates:
            return

        # Prepare list of dicts for bulk insert
        # Sort for deterministic locking
        values_list = []
        for address in sorted(updates.keys()):
            stats = updates[address]
            values_list.append(
                {
                    "address": address,
                    "first_seen_block": stats["first_seen_block"],
                    "last_seen_block": stats["last_seen_block"],
                    "tx_count": stats["tx_count"],
                    "eth_received": stats["eth_received"],
                    "eth_sent": stats["eth_sent"],
                    "contract_deployments": stats["contract_deployments"],
                    "token_transfers_sent": 0,
                    "token_transfers_received": 0,
                    "is_contract": stats["is_contract"],
                }
            )

        stmt = insert(AddressStats).values(values_list)

        stmt = stmt.on_conflict_do_update(
            index_elements=["address"],
            set_={
                "last_seen_block": func.greatest(
                    AddressStats.last_seen_block, stmt.excluded.last_seen_block
                ),
                "tx_count": AddressStats.tx_count + stmt.excluded.tx_count,
                "eth_received": AddressStats.eth_received + stmt.excluded.eth_received,
                "eth_sent": AddressStats.eth_sent + stmt.excluded.eth_sent,
                "contract_deployments": AddressStats.contract_deployments
                + stmt.excluded.contract_deployments,
                "is_contract": AddressStats.is_contract | stmt.excluded.is_contract,
                "updated_at": func.now(),
            },
        )

        session.execute(stmt)

    def _mark_error(self, session: Session, block_record, block_number, error):
        """Mark block as error in DB."""
        try:
            if block_record:
                block_record.worker_status = WorkerStatus.ERROR
                session.commit()
        except Exception:
            pass
        print(f"Error processing block {block_number}: {error}")
