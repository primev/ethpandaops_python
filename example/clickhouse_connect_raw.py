import clickhouse_connect
import pandas as pd
import os

# Set read formats to customize data output from Clickhouse
# https://clickhouse.com/docs/en/integrations/python#read-format-options-python-types
from clickhouse_connect.datatypes.format import set_read_format

# Return both IPv6 and IPv4 values as strings
set_read_format("IPv*", "string")

# Return binary as string
set_read_format("FixedString", "string")

# sets large ints to floats so that there are no large int overflow errors when converting to polars dataframe
set_read_format("Int*", "float")


# Create ClickHouse client
client = clickhouse_connect.get_client(
    host=os.environ.get("HOST"),
    username=os.environ.get("USERNAME"),
    password=os.environ.get("PASSWORD"),
    secure=True,
)

# Execute the query and return as a pandas dataframe
query: str = """
SELECT 
    slot,
    slot_start_date_time,
    epoch,
    epoch_start_date_time,
    meta_client_name,
    meta_network_name,
    meta_consensus_implementation,
    meta_consensus_version,
FROM beacon_api_slot 
WHERE slot_start_date_time > NOW() - INTERVAL '1 HOUR'
"""
results: pd.DataFrame = client.query_df(query)

print(results.shape)
print(results.head(5))
