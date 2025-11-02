import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

import pyarrow as pa
schema = pa.schema([('timestamp', pa.timestamp('us')), ('category', pa.string()), ('amount', pa.float64())])

np.random.seed(42)
n_transactions = 10000
start_date = datetime(2024, 1, 1)
dates = [start_date + timedelta(days=random.randint(0, 365)) for _ in range(n_transactions)]
categories = np.random.choice(['Food', 'Transport', 'Entertainment', 'Bills', 'Shopping'], n_transactions)
amounts = np.abs(np.random.normal(50, 30, n_transactions))
df = pd.DataFrame({'timestamp': dates, 'category': categories, 'amount': amounts})
df_dupe = df.sample(frac=0.05)
df.loc[df_dupe.index, 'category'] = np.random.choice(['Food', 'Transport'], len(df_dupe))
df = pd.concat([df, df_dupe])
df['timestamp'] = df['timestamp'].astype('datetime64[us]')
df.to_parquet('raw_transactions.parquet', index=False, coerce_timestamps="ms")
print(f"Generated {len(df)} noisy transactions.")
print("Schema looks like:")
print(pa.parquet.read_schema('raw_transactions.parquet'))

