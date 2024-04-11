from ethpandaops_python.hypersync import Hypersync
import asyncio
import polars as pl

client = Hypersync()
asyncio.run(client.fetch_data())

data = []
for tx in client.transactions:
    tx_data = {
        "block_number": tx.block_number,
        "hash": tx.hash,
        "from_": tx.from_,
        "to": tx.to,
        "gas": client.convert_hex_to_float(tx.gas),
        "transaction_index": tx.transaction_index,
        "gas_price": client.convert_hex_to_float(tx.gas_price),
        "effective_gas_price": client.convert_hex_to_float(tx.effective_gas_price),
        "gas_used": client.convert_hex_to_float(tx.gas_used),
        "cumulative_gas_used": client.convert_hex_to_float(tx.cumulative_gas_used),
        "max_fee_per_gas": client.convert_hex_to_float(tx.max_fee_per_gas),
        "max_priority_fee_per_gas": client.convert_hex_to_float(tx.max_priority_fee_per_gas),
    }

    data.append(tx_data)

df = pl.DataFrame(data)

print(df.shape)
print(df.head(5))
