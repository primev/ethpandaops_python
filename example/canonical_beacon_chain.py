from ethpandaops_python.client import Queries


query = Queries()

print(
    query.canonical_beacon_chain().shape
)

print('done')
