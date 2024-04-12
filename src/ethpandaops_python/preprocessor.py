import polars as pl
from dataclasses import dataclass, field
from ethpandaops_python.client import Queries
from ethpandaops_python.hypersync import Hypersync


@dataclass
class Preprocessor:
    """
    `Preprocessor` applies transformations and calculations on the data before it is used for analysis.
    Data is automatically fetched at instantiation and stored in memory in a dict[str] of dataframes. This makes the same data reusable throughout the class for multiple
    transformations.
    """
    # default address is Base. Replace this address to filter for a specific address
    blob_producer: str = '0x5050F69a9786F081509234F1a7F4684b5E5b76C9'
    # default time period, in days
    period: int = 1
    clickhouse_client: Queries = field(default_factory=Queries)
    hypersync_client: Hypersync = field(default_factory=Hypersync)
    network: str = "mainnet"

    cached_data: dict[str] = field(default_factory=dict)

    def __post_init__(self):
        # query clickhouse data
        data: dict[str] = self.clickhouse_client.slot_inclusion_query(
            blob_producer=self.blob_producer, n_days=self.period, network=self.network)

        self.cached_data['mempool_df'] = pl.from_pandas(data['mempool_df'])
        self.cached_data['canonical_beacon_blob_sidecar_df'] = pl.from_pandas(
            data['canonical_beacon_blob_sidecar_df'])

        # query hypersync data
        self.cached_data['txs'] = self.hypersync_client.query_txs(
            address=self.blob_producer, period=self.period)

    def create_slot_inclusion_df(self) -> pl.DataFrame:
        """
        `slot_inclusion` returns the slot, slot inclusion time, and slot start time for the last `time` days.

        Returns a pl.DataFrame
        """

        blob_mempool_table: pl.DataFrame = (
            self.cached_data['mempool_df']
            .rename({"blob_hashes": "versioned_hash"})
            .sort(by="event_date_time")
            .group_by(
                (
                    pl.col("versioned_hash").cast(pl.List(pl.Categorical)),
                    "nonce",
                )
            )
            .agg(
                [
                    # min/max datetime
                    pl.col("event_date_time").min().alias(
                        "event_date_time_min"),
                    pl.col("event_date_time").max().alias(
                        "event_date_time_max"),
                    # blob sidecar data
                    pl.col("blob_hashes_length").mean().alias(
                        "blob_hashes_length"),
                    pl.col("blob_sidecars_size").mean().alias(
                        "blob_sidecars_size"),
                    # blob utilization data
                    pl.col("fill_percentage").mean().alias("fill_percentage"),
                    pl.col("blob_gas").mean(),
                    pl.col("blob_gas_fee_cap").mean(),
                    pl.col("gas_price").mean(),
                    pl.col("gas_tip_cap").mean(),
                    pl.col("gas_fee_cap").mean(),
                    # tx info
                    pl.col('hash').last(),
                    pl.col('from').last(),
                    pl.col('to').last(),
                ]
            )
            .with_columns(
                # count number of times a versioned hash gets resubmitted under a new transaction hash
                pl.len().over("versioned_hash").alias("submission_count")
            )
            .sort(by="submission_count")
        )

        canonical_sidecar_df: pl.DataFrame = self.cached_data['canonical_beacon_blob_sidecar_df'].drop(
            "blob_index")

        return (
            (
                # .explode() separates all blob versioned hashes from a list[str] to single str rows
                blob_mempool_table.explode("versioned_hash")
                .with_columns(pl.col("versioned_hash").cast(pl.String))
            )
            .join(canonical_sidecar_df, on="versioned_hash", how="left")
            .unique()
            .with_columns(
                # divide by 1000 to convert from ms to s
                ((pl.col("slot_start_date_time") - pl.col("event_date_time_min")) / 1000)
                .alias("beacon_inclusion_time")
                .cast(pl.Float64),
            )
            .with_columns(
                # divide by 12 to get beacon slot number
                (pl.col("beacon_inclusion_time") / 12)
                .abs()
                .ceil()
                .alias("num_slot_inclusion")
            )
            .sort(by="slot_start_date_time")
            .with_columns(
                # calculate rolling average
                pl.col("num_slot_inclusion")
                .rolling_mean(50)
                .alias("rolling_num_slot_inclusion_50"),
                # add base inclusion target
                pl.lit(2).alias("base_line_2_slots"),
            )

            # rename columns for niceness
            .rename(
                {
                    "slot_start_date_time": "slot _time",
                    "num_slot_inclusion": "slot_inclusion _rate",
                    "rolling_num_slot_inclusion_50": "slot_inclusion_rate_50_blob_avg",
                    "base_line_2_slots": "2_slot_target_inclusion_rate",
                }
            )
            .drop_nulls()
        )

    def create_slot_count_breakdown_df(self) -> pl.DataFrame:
        """
        breakdown slot inclusion rate into three groups:
        1 slot, 2 slot, 3+ slots
        """
        slot_inclusion_df = self.create_slot_inclusion_df()

        return (
            slot_inclusion_df
            .select("hash", "slot inclusion rate")
            .unique()
            .with_columns(
                pl.when(pl.col("slot inclusion rate") == 1)
                .then(True)
                .otherwise(False)
                .alias("1 slot"),
                pl.when(pl.col("slot inclusion rate") == 2)
                .then(True)
                .otherwise(False)
                .alias("2 slots"),
                pl.when(pl.col("slot inclusion rate") >= 3)
                .then(True)
                .otherwise(False)
                .alias("3+ slots"),
            )
            .with_columns(
                pl.col("1 slot").sum(),
                pl.col("2 slots").sum(),
                pl.col("3+ slots").sum(),
            )
            .select("1_slot", "2_slots", "3_plus_slots")[0]
        )

    def create_slot_gas_bidding_df(self) -> pl.DataFrame:
        """
        join slot inclusion transformation with tx data to get gas bidding info
        """
        slot_inclusion_df = self.create_slot_inclusion_df()

        joined_df = (
            slot_inclusion_df
            .join(
                self.cached_data['txs'], on="hash", how="left"
            ).with_columns(
                (pl.col("effective_gas_price") / 10**9)
                .round(3)
                .alias(
                    "effective_gas_price_gwei"
                ),  # gas price in gwei that was paid, including priority fee
                (pl.col("max_fee_per_gas") / 10**9)
                .round(3)
                .alias(
                    "max_fee_per_gas_gwei"
                ),  # max gas price in gwei that rollup is willing to pay
                (pl.col("max_priority_fee_per_gas") / 10**9).round(3)
                # priority gas fee in gwei,
                .alias("max_priority_fee_per_gas_gwei"),
            ).with_columns(
                (
                    (pl.col("max_priority_fee_per_gas_gwei") /
                     pl.col("effective_gas_price_gwei"))
                    * 100
                )
                .round(3)
                .alias("priority_fee_bid_percent_premium")
            )
            .select(
                "block_number",
                "max_priority_fee_per_gas_gwei",
                "effective_gas_price_gwei",
                "priority_fee_bid_percent_premium",
                "slot inclusion rate",
                "submission_count",
            )
            .unique()
            .sort(by="block_number")
            .with_columns(
                (
                    # estimate min block gas by taking the gwei paid minus the priority fee
                    pl.col("effective_gas_price_gwei")
                    - pl.col("max_priority_fee_per_gas_gwei")
                ).alias("min_block_gas_gwei")
            )
            .with_columns(
                # calculate per tx gas fluctuations
                pl.col("min_block_gas_gwei")
                .diff()
                .abs()
                .alias("gas_fluctuation_gwei")
            )
            .with_columns(
                (pl.col("gas_fluctuation_gwei") / pl.col("min_block_gas_gwei") * 100).alias(
                    "gas_fluctuation_percent"
                )
            )
        )

        return joined_df
