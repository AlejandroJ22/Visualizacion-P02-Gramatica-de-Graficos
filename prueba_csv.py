import pandas as pd

df = pd.read_csv("data/rentamedia-sc-3.csv")

print(df.columns.tolist())
print(df.head(3))
