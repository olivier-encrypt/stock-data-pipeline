# ============================================
# fetch_metadata.py
# Purpose: Fetch company data from Alpha
# Vantage and insert DIRECTLY into Azure SQL
# stock_metadata table
#
# This is a fully automated ETL script:
# E → Extract  (call the API)
# T → Transform (parse + clean the JSON)
# L → Load      (insert into Azure SQL)
# ============================================

import requests
import pyodbc
from dotenv import load_dotenv
import os
import time
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.db_connection import get_connection

load_dotenv()
api_key = os.getenv("ALPHA_VANTAGE_API_KEY")


# Our 5 tickers — our scope decision
TICKERS = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"]

# Fields we want from the API
FIELD_MAPPING = {
    "Symbol"  : "ticker",
    "Name"    : "company_name",
    "Exchange": "exchange",
    "Currency": "currency",
    "Country" : "country",
    "Sector"  : "sector",
    "Industry": "industry"
}


# ============================================
# STEP 1: EXTRACT
# Pull raw data from the API
# ============================================

def extract_company_data(ticker):
    """
    Calls Alpha Vantage OVERVIEW endpoint
    Returns raw company data as a dictionary
    """
    print(f"  [EXTRACT] Calling API for {ticker}...")

    url    = "https://www.alphavantage.co/query"
    params = {
        "function": "OVERVIEW",
        "symbol"  : ticker,
        "apikey"  : api_key
    }

    response = requests.get(url, params=params)
    data     = response.json()

    # Check for API errors
    if "Symbol" not in data:
        print(f"  API error for {ticker}: {data}")
        return None

    print(f" Data received for {ticker}")
    return data


# ============================================
# STEP 2: TRANSFORM
# Clean and shape the data for our table
# ============================================

def transform_company_data(raw_data):
    """
    Takes raw API response and extracts only
    the fields we need in the format we need

    This is the TRANSFORM step of ETL:
    - Select only fields we want
    - Clean up any messy values
    - Ensure correct data types
    """
    print(f"  [TRANSFORM] Cleaning data for {raw_data['Symbol']}...")

    # Extract only fields we want
  
    transformed={}
    for api_field, our_column in FIELD_MAPPING.items():
        transformed[our_column] = raw_data.get(api_field, None)

    # Data cleaning example:
    # Sometimes API returns "None" as a string
    # We replace those with actual None (NULL in SQL)
    for key, value in transformed.items():
        if value in ["None", "N/A", "-", ""]:
            transformed[key] = None

    print(f" Transform complete for {transformed['ticker']}")
    return transformed


# ============================================
# STEP 3: LOAD
# Insert the cleaned data into Azure SQL
# ============================================

def load_company_data(cursor, company):
    """
    Inserts one company record into stock_metadata

    We use IF NOT EXISTS to handle duplicates —
    if the ticker already exists we skip it
    This makes the pipeline safe to run multiple times
    
    """
    print(f"  [LOAD] Inserting {company['ticker']} into stock_metadata...")

    # This SQL checks if ticker exists first
    # If it does, skip (don't duplicate)
    # If it doesn't, insert
   
    sql = """
        IF NOT EXISTS (
            SELECT 1 FROM stock_metadata 
            WHERE ticker = ?
        )
        INSERT INTO stock_metadata 
            (ticker, company_name, exchange, currency, country, sector, industry)
        VALUES 
            (?, ?, ?, ?, ?, ?, ?)
    """

    # The ? marks are placeholders — called parameterized query
    # We NEVER put values directly in SQL strings like:
    # f"INSERT INTO ... VALUES ('{ticker}')"
    # That's dangerous — it opens you up to SQL injection attacks
    # Using ? placeholders lets pyodbc handle it safely
    cursor.execute(sql, (    
        company["ticker"],       # for the where clause in IF NOT EXISTS        
        company["ticker"],       # for the INSERT
        company["company_name"],
        company["exchange"],
        company["currency"],
        company["country"],
        company["sector"],
        company["industry"]
    ))

    print(f"  {company['ticker']} loaded successfully")


# ============================================
# MAIN PIPELINE — ties E, T, L together
# ============================================

def run_metadata_pipeline():
    """
    Runs the full ETL pipeline for stock_metadata:
    Extract from API → Transform → Load to Azure SQL
    """

    print("=" * 55)
    print("STOCK METADATA PIPELINE STARTING")
    print("=" * 55)

    # --- Connect to database ONCE ---
    # We open one connection and reuse it for all inserts
    # Opening a new connection per insert is wasteful
    print("\nConnecting to Azure SQL...")
    conn   = get_connection()
    cursor = conn.cursor()
    print(" Connected!\n")

    success_count = 0
    failed_count  = 0

    for i, ticker in enumerate(TICKERS):
        print(f"\n[{i+1}/{len(TICKERS)}] Processing {ticker}")
        print("-" * 40)

        # EXTRACT
        raw_data = extract_company_data(ticker)
        if raw_data is None:
            failed_count += 1
            continue  # skip to next ticker if API failed

        # TRANSFORM
        clean_data = transform_company_data(raw_data)

        # LOAD
        load_company_data(cursor, clean_data)
        success_count += 1

        # Rate limiting between API calls
        if ticker != TICKERS[-1]:
            print(f"\n⏳ Waiting 15 seconds (rate limiting)...")
            time.sleep(15)

    # --- Commit all inserts at once ---
    # In databases, a "commit" makes your changes permanent
    # Until you commit, changes exist only in memory
    # If something crashes before commit, nothing is saved
    # This protects data integrity
    conn.commit()
    print("\n All changes committed to database")

    # --- Close the connection ---
    # Always close connections when done
    # Leaving connections open wastes database resources
    cursor.close()
    conn.close()
    print(" Connection closed")

    # --- Summary ---
  
    print(f"   Successfully loaded : {success_count} tickers")
    print(f"   Failed              : {failed_count} tickers")
    print(f"   Total processed     : {len(TICKERS)} tickers")


# ============================================
# RUN THE PIPELINE
# ============================================
# This pattern — if __name__ == "__main__" —
# means "only run this if we execute this file
# directly, not if it's imported by another file"
# It's standard Python best practice

if __name__ == "__main__":
    run_metadata_pipeline()