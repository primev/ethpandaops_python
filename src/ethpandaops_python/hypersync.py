from dataclasses import dataclass, field
import asyncio
import hypersync
import polars as pl
from typing import List, Union, Dict
from hypersync import BlockField, TransactionField, HypersyncClient


@dataclass
class Hypersync:
    client: HypersyncClient = field(
        default_factory=HypersyncClient)
    transactions: List[hypersync.TransactionField] = field(
        default_factory=list)
    blocks: List[hypersync.BlockField] = field(default_factory=list)

    async def fetch_data(self, address: Union[str, Dict[str, str]], period: int) -> None:
        """
        Asynchronously fetches blockchain data for a specified address over a given period.

        This method queries transactions and blocks from the blockchain starting from the
        current block height and going backwards in time, for the duration of the specified period.

        Parameters:
        - address (str): The blockchain address to fetch transactions for.
        - period (int): The period over which to fetch data, measured in blocks. The method
                        calculates the starting block as the current height minus the product
                        of period and an estimated number of blocks per hour (assuming 7200).

        Note: This function updates the `self.transactions` and `self.blocks` lists with the fetched data.
        """

        # Get the current block height from the blockchain.
        height = await self.client.get_height()

        # Define the query to fetch transactions and blocks. The starting block is calculated
        # based on the given period and an assumption of 7200 blocks per hour.
        query = hypersync.Query(
            from_block=height - (period * 7200),  # Calculate starting block.
            transactions=[
                hypersync.TransactionSelection(
                    # Specify the address to fetch transactions from.
                    from_=address
                )
            ],
            field_selection=hypersync.FieldSelection(
                # Select transaction fields to fetch.
                transaction=[el.value for el in TransactionField],
                # Select block fields to fetch.
                block=[el.value for el in BlockField],
            ),
        )

        # Continuously fetch data until the end of the specified period is reached.
        while True:
            # Send the query to the blockchain client.
            res = await self.client.send_req(query)

            # Append the fetched transactions and blocks to their respective lists.
            self.transactions += res.data.transactions
            self.blocks += res.data.blocks

            # Check if the fetched data has reached the current archive height or next block.
            if res.archive_height < res.next_block:
                # Exit the loop if the end of the period (or the blockchain's current height) is reached.
                break

            # Update the query to fetch the next set of data starting from the next block.
            query.from_block = res.next_block
            print(f"Scanned up to block {query.from_block}")  # Log progress.

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

    def query_txs(self, address: Union[str, Dict[list, list]], period: int) -> pl.DataFrame:
        """ Query transactions for a given address and period.

         Parameters:
         - address (str): The blockchain address to query transactions for.
         - period (int): The time period over which transactions should be queried.

         Returns:
         - A DataFrame containing transaction details for the specified address and period.
        """
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
