from ethpandaops_python.preprocessor import Preprocessor

# instantiate a Preprocessor for Base blob data fora 1 day period
preprocessor: Preprocessor = Preprocessor(
    period=1,     # 1 day
    network='mainnet'   # mainnet
)

# write dataframes to parquet files
preprocessor.cached_data['mempool_df'].write_parquet(
    'mempool_df.parquet')
preprocessor.cached_data['canonical_beacon_blob_sidecar_df'].write_parquet(
    'canonical_beacon_blob_sidecar_df.parquet')
preprocessor.cached_data['txs'].write_parquet('txs.parquet')
