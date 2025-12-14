import os
import yaml
import pandas as pd
import csv
import mysql.connector
from datetime import date
from mysql.connector import Error
from datetime import datetime

# === PATHS ===
BASE_DIR = r"C:\Users\Manish Computers\Downloads"
SECTOR_FILE = "C:\\Users\\Manish Computers\\Downloads\\Sector_data - Sheet1.csv"
OUTPUT_DIR = r"C:\Users\Manish Computers\Downloads\output_csv"
MASTER_CSV = r"C:\Users\Manish Computers\Downloads\all_tickers_combined.csv"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================================================
# STEP 1 → LOAD SYMBOLS FROM SECTOR FILE (OPTION C)
# =========================================================

def extract_ticker(value):
    """Extract ticker from 'COMPANY: TICKER' format"""
    if ":" in value:
        return value.split(":")[1].strip().upper()
    return value.strip().upper()

df_symbols = pd.read_csv(SECTOR_FILE)

# Extract ticker from Symbol column
df_symbols["TickerClean"] = df_symbols["Symbol"].astype(str).apply(extract_ticker)
df_symbols["TickerClean"] = df_symbols["TickerClean"].str.upper()

# =========================================================
# STEP 2 → APPLY SYMBOL CORRECTIONS
# =========================================================
symbol_map = {
    "AIRTEL": "BHARTIARTL",
    "TATACONSUMER": "TATACONSUM",
    "ADANIGREEN": "ADANIENT"
}

df_symbols["TickerClean"] = df_symbols["TickerClean"].replace(symbol_map)

# Remove IOC completely
df_symbols = df_symbols[df_symbols["TickerClean"] != "IOC"]

# Final symbol list
symbol_list = df_symbols["TickerClean"].tolist()

print("\nFinal Symbol List Used:")
print(symbol_list)

# Create storage for collected rows
collected = {sym: [] for sym in symbol_list}

# =========================================================
# STEP 3 → READ ALL YAML FILES AND COLLECT DATA
# =========================================================
for root, dirs, files in os.walk(BASE_DIR):
    for file in files:
        if not file.endswith(".yaml"):
            continue

        yaml_path = os.path.join(root, file)

        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if not isinstance(data, list):
            continue

        for record in data:

            ticker = str(record.get("Ticker", "")).upper()

            if ticker in collected:
                # Attach company & sector info
                row = record.copy()
                row["Company"] = df_symbols.loc[df_symbols["TickerClean"] == ticker, "COMPANY"].values[0]
                row["Sector"] = df_symbols.loc[df_symbols["TickerClean"] == ticker, "sector"].values[0]

                collected[ticker].append(row)

# =========================================================
# STEP 4 → WRITE INDIVIDUAL CSV FILES
# =========================================================
print("\nGenerating per-ticker CSV files...")

all_rows = []

for symbol in symbol_list:
    rows = collected[symbol]

    if not rows:
        print(f"No data for {symbol}")
        continue

    df = pd.DataFrame(rows)

    # Clean the dataframe
    df = df.drop_duplicates()
    df = df.sort_values(by="date")
    df = df.reset_index(drop=True)

    # Save individual CSV
    out_path = os.path.join(OUTPUT_DIR, f"{symbol}.csv")
    df.to_csv(out_path, index=False)
    print(f"Saved {out_path}")

    # Add to master list
    all_rows.append(df)

# =========================================================
# STEP 5 → COMBINE ALL TICKERS INTO SINGLE CLEAN CSV
# =========================================================
print("\nCreating master combined CSV...")

if all_rows:
    master_df = pd.concat(all_rows, ignore_index=True)

    # Final cleaning
    master_df = master_df.drop_duplicates()
    master_df = master_df.sort_values(["Ticker", "date"])
    master_df.to_csv(MASTER_CSV, index=False)

    print(f"✔ Master CSV saved at: {MASTER_CSV}")
else:
    print("⚠ No data found to combine!")

print("\n✔ DONE — All CSVs generated successfully!")

# === Load CSV ===
csv_path = r"C:\Users\Manish Computers\Downloads\all_tickers_combined.csv"
df = pd.read_csv(csv_path)

# Correct column ordering and names
df = df.rename(columns={
    "Ticker": "ticker",
    "Company": "company",
    "Sector": "sector",
    "date": "trade_date",
    "month": "month_tag",
    "open": "open_price",
    "high": "high_price",
    "low": "low_price",
    "close": "close_price"
})

required_cols = [
    "ticker", "company", "sector", "trade_date", "month_tag",
    "open_price", "high_price", "low_price", "close_price", "volume"
]

df = df[required_cols]


# ============================================================
# 1. MYSQL CONNECTION 
# ============================================================
try:
    conn = mysql.connector.connect(
         host="localhost",
        user="bankONE",
        password="0701",
        database="stock")

    if conn.is_connected():
        print("✔ Connected to MySQL database!")
        cursor = conn.cursor()

except Error as e:
    print("❌ MySQL Connection Failed:", e)
    exit()


# ============================================================
# 2. INSERT QUERY (MATCHES YOUR TABLE)
# ============================================================
insert_query = """
INSERT INTO stockprices (
    Ticker, Company, Sector, trade_date, month_tag,
    open_price, high_price, low_price, close_price, volume
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


# ============================================================
# 3. INSERT ROWS SAFELY
# ============================================================
rows_inserted = 0

for idx, row in df.iterrows():

    values = (
        row["ticker"],
        row["company"],
        row["sector"],
        row['trade_date'],
        row["month_tag"],
        float(row["open_price"]),
        float(row["high_price"]),
        float(row["low_price"]),
        float(row["close_price"]),
        int(row["volume"])
    )

    cursor.execute(insert_query, values)
    rows_inserted += 1


conn.commit()
cursor.close()
conn.close()

print(f"✔ Successfully inserted {rows_inserted} rows into stockprices!")