import polars as pl
from dataclasses import dataclass, field
from ethpandaops_python.client import Queries
from ethpandaops_python.hypersync import Hypersync
from typing import Union, Dict


@dataclass
class Preprocessor:
    """
    `Preprocessor` queries data and caches query results in memory in a dict[str] of dataframes. 
    """
    # blob_producer can be a string or a dictionary of addresses with keys indicating their use
    blob_producer: Union[str, Dict[list[str], list[str]]] = field(default_factory=lambda: {
        "sequencer_addresses": [
            "0xC1b634853Cb333D3aD8663715b08f41A3Aec47cc",
            "0x5050F69a9786F081509234F1a7F4684b5E5b76C9",
            "0x6887246668a3b87F54DeB3b94Ba47a6f63F32985",
            "0x000000633b68f5d8d3a86593ebb815b4663bcbe0",
            "0x415c8893d514f9bc5211d36eeda4183226b84aa7",
            "0xa9268341831eFa4937537bc3e9EB36DbecE83C7e",
            "0xcF2898225ED05Be911D3709d9417e86E0b4Cfc8f",
            "0x0D3250c3D5FAcb74Ac15834096397a3Ef790ec99",
            "0x2c169dfe5fbba12957bdd0ba47d9cedbfe260ca7",
        ],
        "sequencer_names": [
            "arbitrum",
            "base",
            "optimism",
            "taiko",
            "blast",
            "linea",
            "scroll",
            "zksync",
            "starknet",
        ],
    })

    # default time period, in days
    period: int = 1
    clickhouse_client: Queries = field(default_factory=Queries)
    hypersync_client: Hypersync = field(default_factory=Hypersync)
    network: str = "mainnet"

    cached_data: dict[str, pl.DataFrame] = field(default_factory=dict)

    def __post_init__(self):
        # query clickhouse data
        data: dict[str] = self.clickhouse_client.slot_inclusion_query(
            blob_producer=self.blob_producer, n_days=self.period, network=self.network)

        self.cached_data['mempool_df'] = pl.from_pandas(data['mempool_df'])
        self.cached_data['canonical_beacon_blob_sidecar_df'] = pl.from_pandas(
            data['canonical_beacon_blob_sidecar_df'])

        # query hypersync data
        self.cached_data['txs'] = self.hypersync_client.query_txs(
            address=self.blob_producer['sequencer_addresses'], period=self.period)
