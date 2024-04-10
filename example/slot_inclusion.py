from ethpandaops_python.preprocessor import Preprocessor
import polars as pl

# instantiate a Preprocessor for Base blob data fora 1 day period
preprocessor: Preprocessor = Preprocessor(
    blob_producer='0x5050F69a9786F081509234F1a7F4684b5E5b76C9',  # base
    period=1,     # 1 day
    network='mainnet'   # mainnet
)


# get preprocessed data
slot_inclusion_df: pl.DataFrame = preprocessor.slot_inclusion()


print(slot_inclusion_df.shape)
print(slot_inclusion_df.head(5))
print('done')
