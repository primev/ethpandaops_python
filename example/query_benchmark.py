from ethpandaops_python.preprocessor import Preprocessor
import polars as pl

# instantiate a Preprocessor for Base blob data fora 1 day period
preprocessor: Preprocessor = Preprocessor(
    period=30,     # 1 day
    network='mainnet'   # mainnet
)

# 4/24/24 Hypersync query is timing out, would need to update to parquet query config. This is a blocker for historical data access.
preprocessor.cached_data['mempool_df'].write_parquet(
    'mempool_df.parquet')
preprocessor.cached_data['canonical_beacon_blob_sidecar_df'].write_parquet(
    'canonical_beacon_blob_sidecar_df.parquet')
preprocessor.cached_data['txs'].write_parquet('txs.parquet')
