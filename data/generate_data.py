import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

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
df.to_parquet('raw_transactions.parquet', index=False)
print(f"Generated {len(df)} noisy transactions.")

