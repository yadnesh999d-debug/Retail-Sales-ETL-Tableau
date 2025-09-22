import pandas as pd
import os
import chardet
import pyodbc

# ------------------------
# 1. Extract
# ------------------------
def extract_data():
    RAW_DATA = os.path.join("data", "Sample - Superstore.csv")
    PROCESSED_DIR = os.path.join("data", "processed")
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # Detect encoding
    with open(RAW_DATA, 'rb') as f:
        raw_bytes = f.read()
        result = chardet.detect(raw_bytes)
    encoding = result['encoding']

    print(f"✅ Extracted raw file with encoding {encoding}")
    return RAW_DATA, PROCESSED_DIR, encoding


# ------------------------
# 2. Transform
# ------------------------
def transform_data():
    RAW_DATA, PROCESSED_DIR, encoding = extract_data()

    df = pd.read_csv(RAW_DATA, encoding=encoding)

    # Convert dates safely
    for col in ['Order Date', 'Ship Date']:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # Create Year/Month/Quarter columns
    df['Year'] = df['Order Date'].dt.year
    df['Month'] = df['Order Date'].dt.month
    df['Quarter'] = df['Order Date'].dt.to_period("Q")

    # Profit Margin
    df['Profit Margin'] = df.apply(
        lambda x: round(x['Profit'] / x['Sales'], 2) if x['Sales'] != 0 else 0,
        axis=1
    )

    # Drop bad rows
    df = df.drop_duplicates()
    df = df.dropna(subset=['Order Date', 'Ship Date'])

    # Save processed
    output_file = os.path.join(PROCESSED_DIR, "Superstore_Processed.csv")
    df.to_csv(output_file, index=False)

    print(f"✅ Transformed & saved to {output_file}")
    return output_file


# ------------------------
# 3. Load
# ------------------------
def load_data():
    CSV_FILE = transform_data()   # use processed file

    # SQL Server config
    SERVER = "YASH\\YASH"
    DATABASE = "RetailSalesDW"
    DRIVER = "ODBC Driver 17 for SQL Server"

    conn_str = f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    df = pd.read_csv(CSV_FILE)
    df['Order Date'] = pd.to_datetime(df['Order Date'])
    df['Ship Date'] = pd.to_datetime(df['Ship Date'])

    # ---- Load DimDate ----
    def load_dim_date(dates):
        unique_dates = pd.DataFrame({'DateValue': pd.to_datetime(dates).drop_duplicates()})
        unique_dates['DateID'] = unique_dates['DateValue'].dt.strftime('%Y%m%d').astype(int)
        unique_dates['Day'] = unique_dates['DateValue'].dt.day
        unique_dates['Month'] = unique_dates['DateValue'].dt.month
        unique_dates['Quarter'] = unique_dates['DateValue'].dt.to_period('Q').astype(str)
        unique_dates['Year'] = unique_dates['DateValue'].dt.year

        for _, row in unique_dates.iterrows():
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM dw.DimDate WHERE DateID = ?)
                INSERT INTO dw.DimDate (DateID, DateValue, Day, Month, Quarter, Year)
                VALUES (?, ?, ?, ?, ?, ?)
            """, row['DateID'], row['DateID'], row['DateValue'], row['Day'], row['Month'], row['Quarter'], row['Year'])
        conn.commit()
        return unique_dates[['DateValue', 'DateID']]

    load_dim_date(df['Order Date'])
    load_dim_date(df['Ship Date'])

    # ---- Load DimProduct ----
    dim_product = df[['Product ID', 'Product Name', 'Category', 'Sub-Category']].drop_duplicates()
    for _, row in dim_product.iterrows():
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dw.DimProduct WHERE ProductID = ?)
            INSERT INTO dw.DimProduct (ProductID, ProductName, Category, SubCategory)
            VALUES (?, ?, ?, ?)
        """, row['Product ID'], row['Product ID'], row['Product Name'], row['Category'], row['Sub-Category'])
    conn.commit()

    product_map = pd.read_sql("SELECT ProductKey, ProductID FROM dw.DimProduct", conn)
    product_dict = dict(zip(product_map['ProductID'], product_map['ProductKey']))

    # ---- Load DimCustomer ----
    dim_customer = df[['Customer ID', 'Customer Name', 'Segment', 'City', 'State', 'Postal Code', 'Region', 'Country']].drop_duplicates()
    for _, row in dim_customer.iterrows():
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dw.DimCustomer WHERE CustomerID = ?)
            INSERT INTO dw.DimCustomer (CustomerID, CustomerName, Segment, City, State, PostalCode, Region, Country)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, row['Customer ID'], row['Customer ID'], row['Customer Name'], row['Segment'], row['City'], row['State'], row['Postal Code'], row['Region'], row['Country'])
    conn.commit()

    customer_map = pd.read_sql("SELECT CustomerKey, CustomerID FROM dw.DimCustomer", conn)
    customer_dict = dict(zip(customer_map['CustomerID'], customer_map['CustomerKey']))

    # ---- Load FactSales ----
    for _, row in df.iterrows():
        order_date_id = int(row['Order Date'].strftime('%Y%m%d'))
        ship_date_id = int(row['Ship Date'].strftime('%Y%m%d'))
        product_key = product_dict[row['Product ID']]
        customer_key = customer_dict[row['Customer ID']]
        cursor.execute("""
            INSERT INTO dw.FactSales
            (OrderID, CustomerKey, ProductKey, OrderDateID, ShipDateID, ShipMode, Sales, Quantity, Discount, Profit, ProfitMargin)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row['Order ID'], customer_key, product_key, order_date_id, ship_date_id,
           row['Ship Mode'], row['Sales'], row['Quantity'], row['Discount'], row['Profit'], row['Profit Margin'])
    conn.commit()

    cursor.close()
    conn.close()
    print("✅ Loaded all data into SQL Server.")
