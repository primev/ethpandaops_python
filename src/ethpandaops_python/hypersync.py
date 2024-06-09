from dataclasses import dataclass, field
import asyncio
import hypersync
import polars as pl
from typing import List, Union, Dict
from hypersync import BlockField, TransactionField, HypersyncClient, ColumnMapping, DataType


@dataclass
class Hypersync:
    client: HypersyncClient = field(
        default_factory=lambda: HypersyncClient(
            hypersync.ClientConfig(
                url='http://1.backup.hypersync.xyz'
            )
        )
    )
    transactions: List[hypersync.TransactionField] = field(
        default_factory=list)
    blocks: List[hypersync.BlockField] = field(default_factory=list)

    async def fetch_data(self, address: Union[str, Dict[str, str]], period: int) -> None:
        """
        Asynchronously fetches blockchain data for a specified "from" address over a given block range. .

        Saves query results as parquet files in a data folder.
        """

        # Get the current block height from the blockchain.
        height = await self.client.get_height()

        # The starting block is calculated based on the given period and an assumption of 7200 blocks per day.
        query = hypersync.Query(
            from_block=height - (period * 7200),  # Calculate starting block.
            transactions=[
                hypersync.TransactionSelection(
                    # Specify the address to fetch transactions from.
                    from_=address
                )
            ],
            to_block=height,
            field_selection=hypersync.FieldSelection(
                # Select transaction fields to fetch.
                transaction=[el.value for el in TransactionField],
                # Select block fields to fetch.
                block=[el.value for el in BlockField],
            ),
        )

        # Setting this number lower reduces client sync console error messages.
        query.max_num_transactions = 1_000  # for troubleshooting

        # configuration settings to predetermine type output here
        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            column_mapping=ColumnMapping(
                transaction={
                    TransactionField.GAS_USED: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
                    TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.GAS_PRICE: DataType.FLOAT64,
                    TransactionField.CUMULATIVE_GAS_USED: DataType.FLOAT64,
                    TransactionField.EFFECTIVE_GAS_PRICE: DataType.FLOAT64,
                    TransactionField.NONCE: DataType.INT64,
                    TransactionField.GAS: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
                    TransactionField.VALUE: DataType.FLOAT64,
                },
                block={
                    BlockField.GAS_LIMIT: DataType.FLOAT64,
                    BlockField.GAS_USED: DataType.FLOAT64,
                    BlockField.SIZE: DataType.FLOAT64,
                    BlockField.BLOB_GAS_USED: DataType.FLOAT64,
                    BlockField.EXCESS_BLOB_GAS: DataType.FLOAT64,
                    BlockField.BASE_FEE_PER_GAS: DataType.FLOAT64,
                    BlockField.TIMESTAMP: DataType.INT64,
                }
            )
        )

        return await self.client.collect_parquet('data', query, config)

        # # Continuously fetch data until the end of the specified period is reached.
        # while True:
        #     # Send the query to the blockchain client.
        #     res = await self.client.send_req(query)

        #     # Append the fetched transactions and blocks to their respective lists.
        #     self.transactions += res.data.transactions
        #     self.blocks += res.data.blocks

        #     # Check if the fetched data has reached the current archive height or next block.
        #     if res.archive_height < res.next_block:
        #         # Exit the loop if the end of the period (or the blockchain's current height) is reached.
        #         break

        #     # Update the query to fetch the next set of data starting from the next block.
        #     query.from_block = res.next_block

    # def convert_hex_to_float(self, hex: str) -> float:
    #     """
    #     Converts hexadecimal values in a transaction dictionary to integers, skipping specific keys.

    #     Args:
    #     transaction (dict): A dictionary containing transaction data, where some values are hexadecimals.

    #     Returns:
    #     dict: A new dictionary with hexadecimals converted to integers, excluding specified keys.
    #     """
    #     # Only convert hex strings; leave other values as is
    #     if isinstance(hex, str) and hex.startswith("0x"):
    #         # Convert hex string to float
    #         return float(int(hex, 16))

    def query_txs(self, address: Union[str, Dict[list, list]], period: int) -> pl.DataFrame:
        """ Query transactions for a given address and period.

         Parameters:
         - address (str): The blockchain address to query transactions for.
         - period (int): The time period over which transactions should be queried.

         Returns:
         - A DataFrame containing transaction details for the specified address and period.
        """
        asyncio.run(self.fetch_data(address=address, period=period))

        # Merge separate datasets into a single dataset
        txs_df = pl.scan_parquet('data/transactions.parquet')
        blocks_df = pl.scan_parquet(
            'data/blocks.parquet').rename({'number': 'block_number'})

        txs_blocks_joined = txs_df.join(
            blocks_df,
            on='block_number',
            how='left',
            coalesce=True,
            suffix='_block'
        ).unique()

        final_df = txs_blocks_joined.select(
            'block_number',
            'extra_data',
            'base_fee_per_gas',
            'timestamp',
            'hash',
            'from',
            'to',
            'gas',
            'transaction_index',
            'gas_price',
            'effective_gas_price',
            'gas_used',
            'cumulative_gas_used',
            'max_fee_per_gas',
            'max_priority_fee_per_gas',
        ).collect()

        return pl.DataFrame(final_df)
