import pandas as pd
import os

# Path to processed CSV
processed_file = os.path.join("data", "processed", "Superstore_Processed.csv")

# Load the processed CSV
df = pd.read_csv(processed_file)

# 1. Columns and data types
print("Columns and data types:")
print(df.dtypes)
print("\n")

# 2. First 5 rows
print("Sample rows:")
print(df.head())

# 3. Missing values
print("\nMissing values per column:")
print(df.isna().sum())

# 4. Basic statistics
print("\nBasic stats for numeric columns:")
print(df.describe())
