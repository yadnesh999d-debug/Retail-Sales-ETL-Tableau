import pandas as pd
import pyodbc

# -----------------------------
# Configuration
# -----------------------------
CSV_FILE = r"data\processed\Superstore_Processed.csv"

# SQL Server connection parameters
SERVER = "YASH\YASH"  # or your server name
DATABASE = "RetailSalesDW"
USERNAME = "sa"         # leave empty if using Windows Auth
PASSWORD = "yadnesh99d"         # leave empty if using Windows Auth
DRIVER = "ODBC Driver 17 for SQL Server"  # make sure you have this driver

# Connection string (Windows Authentication if username/password empty)
conn_str = f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"

# -----------------------------
# Load CSV
# -----------------------------
df = pd.read_csv(CSV_FILE)

# Convert dates to datetime
df['Order Date'] = pd.to_datetime(df['Order Date'])
df['Ship Date'] = pd.to_datetime(df['Ship Date'])

# -----------------------------
# Connect to SQL Server
# -----------------------------
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# -----------------------------
# 1. Load DimDate
# -----------------------------
def load_dim_date(dates):
    unique_dates = pd.DataFrame({'DateValue': pd.to_datetime(dates).drop_duplicates()})
    unique_dates['DateID'] = unique_dates['DateValue'].dt.strftime('%Y%m%d').astype(int)
    unique_dates['Day'] = unique_dates['DateValue'].dt.day
    unique_dates['Month'] = unique_dates['DateValue'].dt.month
    unique_dates['Quarter'] = unique_dates['DateValue'].dt.to_period('Q').astype(str)
    unique_dates['Year'] = unique_dates['DateValue'].dt.year

    # Insert into DimDate
    for _, row in unique_dates.iterrows():
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM dw.DimDate WHERE DateID = ?)
            INSERT INTO dw.DimDate (DateID, DateValue, Day, Month, Quarter, Year)
            VALUES (?, ?, ?, ?, ?, ?)
        """, row['DateID'], row['DateID'], row['DateValue'], row['Day'], row['Month'], row['Quarter'], row['Year'])
    conn.commit()
    return unique_dates[['DateValue', 'DateID']]

# Load order and ship dates
order_dates = load_dim_date(df['Order Date'])
ship_dates = load_dim_date(df['Ship Date'])

# -----------------------------
# 2. Load DimProduct
# -----------------------------
dim_product = df[['Product ID', 'Product Name', 'Category', 'Sub-Category']].drop_duplicates()
for _, row in dim_product.iterrows():
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM dw.DimProduct WHERE ProductID = ?)
        INSERT INTO dw.DimProduct (ProductID, ProductName, Category, SubCategory)
        VALUES (?, ?, ?, ?)
    """, row['Product ID'], row['Product ID'], row['Product Name'], row['Category'], row['Sub-Category'])
conn.commit()

# Fetch ProductKey mapping
product_map = pd.read_sql("SELECT ProductKey, ProductID FROM dw.DimProduct", conn)
product_dict = dict(zip(product_map['ProductID'], product_map['ProductKey']))

# -----------------------------
# 3. Load DimCustomer
# -----------------------------
dim_customer = df[['Customer ID', 'Customer Name', 'Segment', 'City', 'State', 'Postal Code', 'Region', 'Country']].drop_duplicates()
for _, row in dim_customer.iterrows():
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM dw.DimCustomer WHERE CustomerID = ?)
        INSERT INTO dw.DimCustomer (CustomerID, CustomerName, Segment, City, State, PostalCode, Region, Country)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, row['Customer ID'], row['Customer ID'], row['Customer Name'], row['Segment'], row['City'], row['State'], row['Postal Code'], row['Region'], row['Country'])
conn.commit()

# Fetch CustomerKey mapping
customer_map = pd.read_sql("SELECT CustomerKey, CustomerID FROM dw.DimCustomer", conn)
customer_dict = dict(zip(customer_map['CustomerID'], customer_map['CustomerKey']))

# -----------------------------
# 4. Load FactSales
# -----------------------------
for _, row in df.iterrows():
    order_date_id = int(row['Order Date'].strftime('%Y%m%d'))
    ship_date_id = int(row['Ship Date'].strftime('%Y%m%d'))
    product_key = product_dict[row['Product ID']]
    customer_key = customer_dict[row['Customer ID']]
    cursor.execute("""
        INSERT INTO dw.FactSales
        (OrderID, CustomerKey, ProductKey, OrderDateID, ShipDateID, ShipMode, Sales, Quantity, Discount, Profit, ProfitMargin)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, row['Order ID'], customer_key, product_key, order_date_id, ship_date_id, row['Ship Mode'], row['Sales'], row['Quantity'], row['Discount'], row['Profit'], row['Profit Margin'])
conn.commit()

# -----------------------------
# Done
# -----------------------------
cursor.close()
conn.close()
print("✅ ETL Complete! All tables loaded into SQL Server.")
