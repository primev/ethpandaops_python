import pandas as pd
import polars as pl
from dataclasses import dataclass, field
from ethpandaops_python.client import Queries


@dataclass
class Preprocessor:
    """
    `Preprocessor` applies transformations and calculations on the data before it is used for analysis.
    Data will be fetched from client.py and then preprocessed using this class.
    """
    # default address is Base. Replace this address to filter for a specific address
    blob_producer: str = '0x5050F69a9786F081509234F1a7F4684b5E5b76C9'
    # default time period, in days
    period: int = 1
    query_engine: Queries = field(default_factory=Queries)
    network: str = "mainnet"

    def slot_inclusion(self) -> pl.DataFrame:
        """
        `slot_inclusion` returns the slot, slot inclusion time, and slot start time for the last `time` days.

        Returns a pl.DataFrame
        """
        # query data
        data: dict[str] = self.query_engine.slot_inclusion_query(
            blob_producer=self.blob_producer, n_days=self.period, network=self.network)

        # convert pandas to polars df
        mempool_df: pl.DataFrame = pl.from_pandas(data["mempool_df"])
        canonical_beacon_blob_sidecar_df: pl.DataFrame = pl.from_pandas(
            data["canonical_beacon_blob_sidecar_df"])

        # preprocessing
        blob_mempool_table = (
            mempool_df
            .rename({"blob_hashes": "versioned_hash"})
            .sort(by="event_date_time")
            .group_by(
                (
                    pl.col("versioned_hash").cast(pl.List(pl.Categorical)),
                    "hash",
                    "from",
                    "to",
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
                ]
            )
            .with_columns(
                # count number of times a versioned hash gets resubmitted under a new transaction hash
                pl.len().over("versioned_hash").alias("submission_count")
            )
            .sort(by="submission_count")
        )

        canonical_sidecar_df = canonical_beacon_blob_sidecar_df.drop(
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
                    "slot_start_date_time": "slot time",
                    "num_slot_inclusion": "slot inclusion rate",
                    "rolling_num_slot_inclusion_50": "slot inclusion rate (50 blob average)",
                    "base_line_2_slots": "slot target inclusion rate (2 slots)",
                }
            )
        )
