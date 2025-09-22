import pandas as pd
import os
import chardet

# Paths
RAW_DATA = os.path.join("data", "Sample - Superstore.csv")
PROCESSED_DIR = os.path.join("data", "processed")
os.makedirs(PROCESSED_DIR, exist_ok=True)

# 1. Extract
print("🔹 Detecting file encoding...")
with open(RAW_DATA, 'rb') as f:
    raw_bytes = f.read()
    result = chardet.detect(raw_bytes)
encoding = result['encoding']
print(f"Detected encoding: {encoding}")

print("🔹 Reading raw dataset...")
df = pd.read_csv(RAW_DATA, encoding=encoding)
print(f"Dataset loaded: {df.shape[0]} rows, {df.shape[1]} columns")

# 2. Basic Transformations
print("🔹 Cleaning data...")

# Convert dates safely
for col in ['Order Date', 'Ship Date']:
    df[col] = pd.to_datetime(df[col], errors='coerce')

# Create Year/Month/Quarter columns
df['Year'] = df['Order Date'].dt.year
df['Month'] = df['Order Date'].dt.month
df['Quarter'] = df['Order Date'].dt.to_period("Q")

# Derive Profit Margin safely (avoid division by zero)
df['Profit Margin'] = df.apply(
    lambda x: round(x['Profit'] / x['Sales'], 2) if x['Sales'] != 0 else 0,
    axis=1
)

# Drop duplicates
df = df.drop_duplicates()

# Optional: drop rows with invalid dates
df = df.dropna(subset=['Order Date', 'Ship Date'])

# 3. Save processed dataset
output_file = os.path.join(PROCESSED_DIR, "Superstore_Processed.csv")
df.to_csv(output_file, index=False)

print(f"✅ ETL complete! Processed file saved to {output_file}")
