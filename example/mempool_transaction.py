from ethpandaops_python.client import Queries


query = Queries()

# Test the query_mempool method
print(
    query.mempool_transaction(all_cols='blobs', time=1).head(5)
)

print(
    query.mempool_transaction(all_cols='sample', time=1).head(5)
)
print('done')
