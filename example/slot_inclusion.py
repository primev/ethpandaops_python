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


# number of rows
print(slot_inclusion_df.shape)
# columns and datatypes
print(slot_inclusion_df.dtypes)

# df output sample of all data
print(slot_inclusion_df.head(10))
# ! - note that the head of data has some nulls because the mempool data and beacon chain data are not 100% synced up.
# df output with selected columns
print(slot_inclusion_df.drop_nulls().select('slot time', 'slot inclusion rate',
      'slot inclusion rate (50 blob average)', 'slot target inclusion rate (2 slots)').head(10))

# json output
print(slot_inclusion_df.drop_nulls().select('slot time', 'slot inclusion rate',
      'slot inclusion rate (50 blob average)', 'slot target inclusion rate (2 slots)').head(10).to_dicts())
