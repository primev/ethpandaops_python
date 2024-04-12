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
print('\nslot inclusion time:')
print(slot_inclusion_df.select('slot_time', 'slot_inclusion_rate',
      'slot_inclusion_rate_50_blob_avg', '2_slot_target_inclusion_rate').to_dicts()[:5])

slot_count_breakdown_df: pl.DataFrame = preprocessor.create_slot_count_breakdown_df()

# json output for slot count breakdown barchart/pie chart
print('\nslot count breakdown:')
print(slot_count_breakdown_df.to_dicts())

# priority gas bidding
print('\npriority gas bidding:')
slot_gas_bidding_df: pl.DataFrame = preprocessor.create_slot_gas_bidding_df()
print(slot_gas_bidding_df.to_dicts()[:5])


print('done')
