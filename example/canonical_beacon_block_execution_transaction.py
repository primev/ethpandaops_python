from ethpandaops_python.client import Queries


query = Queries()

print(
    query.canonical_beacon_block_execution_transaction().shape
)

print('done')
