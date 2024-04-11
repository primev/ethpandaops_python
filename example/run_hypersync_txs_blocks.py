from ethpandaops_python.hypersync import Hypersync

client = Hypersync()

df = client.query_txs("0x5050f69a9786f081509234f1a7f4684b5e5b76c9", 1)


print(df.shape)
print(df.head(5))
