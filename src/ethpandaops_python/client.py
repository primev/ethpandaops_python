import clickhouse_connect
import pandas as pd
import os

from clickhouse_connect.driver.client import Client
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import Union, Dict

# Set read formats to customize data output from Clickhouse
# https://clickhouse.com/docs/en/integrations/python#read-format-options-python-types
from clickhouse_connect.datatypes.format import set_read_format

# Return both IPv6 and IPv4 values as strings
set_read_format("IPv*", "string")

# Return binary as string
set_read_format("FixedString", "string")

# sets large ints to floats so that there are no large int overflow errors when converting to polars dataframe
set_read_format("Int*", "float")


@dataclass
class Queries:
    """
    `Queries` contains multiple query functions for different tables. The primary
    purpose of this class is to categorize different queries by table and make it easier to automate broad filters such as
    network and number of days to query from.
    """
    load_dotenv()
    # Create ClickHouse client
    client: Client = clickhouse_connect.get_client(
        host=os.environ.get("HOST"),
        username=os.environ.get("USERNAME"),
        password=os.environ.get("PASSWORD"),
        secure=True,
    )

    def slot_inclusion_query(
        self,
        blob_producer: Union[str, Dict[list[str], list[str]]],
        n_days: int,
        network: str,
    ) -> dict[str, pd.DataFrame]:
        """
        slot_inclusion_query() makes queries to the Ethpandaops Clickhouse instance to get mempool and canonical beacon block sidecar data for a specific rollup.

        Returns a dictionary formatted as:

        {'mempool_df': mempool_df,
        'canonical_beacon_blob_sidecar_df': canonical_beacon_blob_sidecar_df,
        }
        """

        # Build query conditions based on blob_producer type
        if isinstance(blob_producer, str):
            blob_producer_condition = f"from = '{blob_producer}'"
        elif isinstance(blob_producer, dict):
            # Convert the dictionary values to a properly formatted SQL string
            blob_producer_list = ", ".join(
                f"'{addr}'" for addr in blob_producer['sequencer_addresses'])

            blob_producer_condition = f"from IN ({blob_producer_list})"

        # Mempool query
        mempool_query = f"""
        SELECT
            event_date_time,
            type,
            blob_sidecars_size,
            blob_sidecars_empty_size,
            hash,
            to,
            from,
            blob_hashes,
            nonce,
            meta_network_name,
            length(blob_hashes) as blob_hashes_length,
            ROUND(100 - (blob_sidecars_empty_size / blob_sidecars_size) * 100, 2) AS fill_percentage,
            blob_gas,
            blob_gas_fee_cap,
            gas_price,
            gas_tip_cap,
            gas_fee_cap
        FROM mempool_transaction
        WHERE event_date_time > NOW() - INTERVAL '{n_days} DAYS'
        AND type = 3
        AND meta_network_name = '{network}'
        AND {blob_producer_condition}
        """

        # Canonical beacon block sidecar query
        canonical_beacon_blob_sidecar_query = f"""
        SELECT
            slot,
            slot_start_date_time,
            block_root,
            kzg_commitment,
            meta_network_name,
            blob_index,
            versioned_hash,
            blob_size,
            blob_empty_size
        FROM canonical_beacon_blob_sidecar
        WHERE event_date_time > NOW() - INTERVAL '{n_days} DAYS'
        """

        # Query dataframes
        mempool_df: pd.DataFrame = self.client.query_df(mempool_query)
        canonical_beacon_blob_sidecar_df: pd.DataFrame = self.client.query_df(
            canonical_beacon_blob_sidecar_query
        )

        return {
            "mempool_df": mempool_df,
            "canonical_beacon_blob_sidecar_df": canonical_beacon_blob_sidecar_df,
        }

    def canonical_beacon_block_execution_transaction(self, all_cols: str = 'blobs', time: int = 7, network: str = 'mainnet', type: int = 3) -> pd.DataFrame:
        """
        Queries that utilize data captured by Xatu Cannon, which collect execution layer transaction data from the beacon chain.
        - https://dbt.platform.ethpandaops.io/#!/source/source.xatu.clickhouse.canonical_beacon_block_execution_transaction
        """
        match all_cols:
            case 'blobs':
                query = f"""
                SELECT
                    slot_start_date_time,
                    event_date_time as block_process_time,
                    slot,
                    epoch,
                    hash,
                    blob_hashes,
                    length(blob_hashes) as blob_hashes_length,
                    type,
                    gas_price,
                    gas,
                    size,
                    call_data_size,
                    gas_tip_cap as priority_fee,
                    gas_fee_cap as max_fee_per_gas,
                    blob_gas,
                    blob_gas_fee_cap,
                    meta_network_name
                FROM canonical_beacon_block_execution_transaction
                WHERE event_date_time > NOW() - INTERVAL '{time} DAYS'
                AND type = {type} AND meta_network_name = '{network}'
                ORDER BY slot DESC
                """
                return self.client.query_df(query)
            case 'all':
                query = f"""
                    SELECT * FROM canonical_beacon_block_execution_transaction
                    WHERE event_date_time > NOW() - INTERVAL '{time} DAYS'
                    AND meta_network_name = '{network}'
                    """
                return self.client.query_df(query)
            case 'sample':
                query = f"""
                    SELECT * FROM canonical_beacon_block_execution_transaction
                    WHERE event_date_time > NOW() - INTERVAL '{time} DAYS'
                    AND meta_network_name = '{network}'
                    LIMIT 1000
                    """
                return self.client.query_df(query)

    def mempool_transaction(self,
                            all_cols: str = 'blobs',
                            time: int = 7,
                            network: str = 'mainnet',
                            type: int = 3
                            ) -> pd.DataFrame:
        """
        Queries that utilize mempool_data table - https://dbt.platform.ethpandaops.io/#!/source/source.xatu.clickhouse.mempool_transaction
        Xatu Mimicry collectes this data from the execution layer P2P network.

        all_cols:
         * 'blobs' returns blob mempool data.
         * 'all' returns all mempool data.
         * 'sample' returns all mempool data with LIMIT 1000
        """
        match all_cols:
            case 'blobs':
                query = f"""
                SELECT
                    MIN(event_date_time) AS earliest_event_date_time,
                    type,
                    blob_sidecars_size,
                    blob_sidecars_empty_size,
                    hash,
                    blob_hashes,
                    nonce,
                    meta_network_name,
                    length(blob_hashes) as blob_hashes_length,
                    ROUND(100 - (blob_sidecars_empty_size / blob_sidecars_size) * 100, 2) AS fill_percentage,
                    from,
                    to
                FROM mempool_transaction
                WHERE event_date_time > NOW() - INTERVAL '{time} DAYS'
                AND type = {type} AND meta_network_name = '{network}'
                GROUP BY hash, type, blob_sidecars_size, blob_sidecars_empty_size, blob_hashes, nonce, meta_network_name, blob_hashes_length, fill_percentage, to, from
                """
                return self.client.query_df(query)
            case 'all':
                query = f"""
                    SELECT * FROM mempool_transaction
                    WHERE event_date_time > NOW() - INTERVAL '{time} DAYS'
                    """
                return self.client.query_df(query).drop('meta_labels', axis=1)
            case 'sample':
                query = f"""
                    SELECT * FROM mempool_transaction
                    WHERE event_date_time > NOW() - INTERVAL '{time} DAYS'
                    LIMIT 1000
                    """
                return self.client.query_df(query).drop('meta_labels', axis=1)

    def canonical_beacon_chain(self,
                               all_cols: str = 'block_blobs',
                               time: int = 7,
                               network: str = 'mainnet',
                               ) -> pd.DataFrame:
        """
        Queries that utilize data captured by Xatu Cannon, which collect canonical consensus client data from the beacon chain.
        - https://dbt.platform.ethpandaops.io/#!/source/source.xatu.clickhouse.canonical_beacon_blob_sidecar
        - https://dbt.platform.ethpandaops.io/#!/source/source.xatu.clickhouse.canonical_beacon_block


        all_cols:
         * 'block_blobs' returns blob mempool data.
        """
        match all_cols:
            case 'block_blobs':
                query = f"""
                SELECT
                    a.slot,
                    a.slot_start_date_time,
                    a.epoch,
                    a.epoch_start_date_time,
                    a.block_total_bytes,
                    a.block_total_bytes_compressed,
                    a.eth1_data_block_hash,
                    a.execution_payload_block_number,
                    a.execution_payload_transactions_count,
                    a.execution_payload_transactions_total_bytes,
                    a.execution_payload_transactions_total_bytes_compressed,
                    b.blob_index,
                    b.blob_size,
                    a.meta_network_name AS meta_network_name_block,
                    b.meta_network_name AS meta_network_name_blob
                FROM
                    (SELECT
                        slot,
                        slot_start_date_time,
                        epoch,
                        epoch_start_date_time,
                        block_total_bytes,
                        block_total_bytes_compressed,
                        eth1_data_block_hash,
                        execution_payload_block_number,
                        execution_payload_transactions_count,
                        execution_payload_transactions_total_bytes,
                        execution_payload_transactions_total_bytes_compressed,
                        meta_network_name
                    FROM canonical_beacon_block
                    WHERE slot_start_date_time > NOW() - INTERVAL '{time} DAYS'
                    AND meta_network_name = '{network}'
                    ) AS a
                LEFT JOIN
                    (SELECT
                        slot,
                        slot_start_date_time,
                        epoch,
                        block_root,
                        versioned_hash,
                        kzg_commitment,
                        kzg_proof,
                        blob_index,
                        blob_size,
                        blob_empty_size,
                        meta_network_name
                    FROM canonical_beacon_blob_sidecar
                    WHERE slot_start_date_time > NOW() - INTERVAL '{time} DAYS'
                    AND meta_network_name = '{network}'
                    ) AS b
                ON a.slot = b.slot
                """
                return self.client.query_df(query)

    def blob_propagation(self,
                         all_cols: str = 'blob_propagation',
                         time: int = 7,
                         network: str = 'mainnet',
                         ) -> pd.DataFrame:
        """
        Queries that utilize data captured by Xatu Sentry, which collect consensus client event data from the beacon chain.
        - https://dbt.platform.ethpandaops.io/#!/source/source.xatu.clickhouse.beacon_api_eth_v1_events_blob_sidecar

        all_cols:
         * 'blob_propagation' returns blob sidecar events.
        """
        match all_cols:
            case 'blob_propagation':
                query = f"""
                SELECT
                    event_date_time,
                    slot_start_date_time,
                    propagation_slot_start_diff,
                    slot,
                    epoch,
                    meta_network_name,
                    meta_client_name
                FROM beacon_api_eth_v1_events_blob_sidecar
                WHERE slot_start_date_time > NOW() - INTERVAL '{time} DAYS'
                AND meta_network_name = '{network}'
                """
                return self.client.query_df(query)
