from ethpandaops_python.client import Queries
import pandas as pd

# instantiate Queries class, which contains all the query logic.
query = Queries()

# returns pandas dataframe
query_df: pd.DataFrame = query.canonical_beacon_chain(
    all_cols='block_blobs', time=1, network='mainnet')

# convert dataframe to dictionary
query_json: dict[str] = query_df.to_dict(orient='records')

# see results
print('pandas dataframe:')
print(query_df.head(5))

print('json row 1:')
print(query_json[0])

print('json row 2:')
print(query_json[1])
