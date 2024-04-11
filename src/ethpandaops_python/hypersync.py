from dataclasses import dataclass, field
import asyncio
import hypersync
import polars as pl
from typing import List, Optional
from hypersync import BlockField, TransactionField, HypersyncClient


@dataclass
class Hypersync:
    client: HypersyncClient = field(
        default_factory=HypersyncClient)
    transactions: List[hypersync.TransactionField] = field(
        default_factory=list)
    blocks: List[hypersync.BlockField] = field(default_factory=list)

    async def fetch_data(self, address: str, period: int) -> None:
        height = await self.client.get_height()

        query = hypersync.Query(
            from_block=height - (period * 7200),
            transactions=[
                hypersync.TransactionSelection(
                    from_=["0x5050f69a9786f081509234f1a7f4684b5e5b76c9"]
                )
            ],
            field_selection=hypersync.FieldSelection(
                transaction=[el.value for el in TransactionField],
                block=[el.value for el in BlockField],
            ),
        )

        while True:
            res = await self.client.send_req(query)

            self.transactions += res.data.transactions
            self.blocks += res.data.blocks

            if res.archive_height < res.next_block:
                # Quit if reached the tip (consider adding a delay with asyncio.sleep if you wish to poll)
                break

            query.from_block = res.next_block
            print("Scanned up to block " + str(query.from_block))

    def convert_hex_to_float(self, hex: str) -> float:
        """
        Converts hexadecimal values in a transaction dictionary to integers, skipping specific keys.

        Args:
        transaction (dict): A dictionary containing transaction data, where some values are hexadecimals.

        Returns:
        dict: A new dictionary with hexadecimals converted to integers, excluding specified keys.
        """
        # Only convert hex strings; leave other values as is
        if isinstance(hex, str) and hex.startswith("0x"):
            # Convert hex string to float
            return float(int(hex, 16))

    def query_txs(self, address: str, period: int) -> pl.DataFrame:
        asyncio.run(self.fetch_data(address=address, period=period))

        data = []
        for tx in self.transactions:
            tx_data = {
                "block_number": tx.block_number,
                "hash": tx.hash,
                "from_": tx.from_,
                "to": tx.to,
                "gas": self.convert_hex_to_float(tx.gas),
                "transaction_index": tx.transaction_index,
                "gas_price": self.convert_hex_to_float(tx.gas_price),
                "effective_gas_price": self.convert_hex_to_float(tx.effective_gas_price),
                "gas_used": self.convert_hex_to_float(tx.gas_used),
                "cumulative_gas_used": self.convert_hex_to_float(tx.cumulative_gas_used),
                "max_fee_per_gas": self.convert_hex_to_float(tx.max_fee_per_gas),
                "max_priority_fee_per_gas": self.convert_hex_to_float(tx.max_priority_fee_per_gas),
            }

            data.append(tx_data)

        return pl.DataFrame(data)
