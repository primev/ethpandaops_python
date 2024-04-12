from ethpandaops_python.preprocessor import Preprocessor
import polars as pl

# instantiate a Preprocessor for Base blob data fora 1 day period
preprocessor: Preprocessor = Preprocessor(
    blob_producer='0x5050F69a9786F081509234F1a7F4684b5E5b76C9',  # base
    period=1,     # 1 day
    network='mainnet'   # mainnet
)


# get preprocessed data
slot_inclusion_df: pl.DataFrame = preprocessor.create_slot_inclusion_df()

# json output for time series
print(slot_inclusion_df.select('slot time', 'slot inclusion rate',
      'slot inclusion rate (50 blob average)', 'slot target inclusion rate (2 slots)').head(10).to_dicts())

slot_count_breakdown_df: pl.DataFrame = preprocessor.create_slot_count_breakdown_df()

# json output for slot count breakdown barchart/pie chart
print(slot_count_breakdown_df.to_dict())
