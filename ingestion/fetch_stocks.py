# fetch_stock.py
# fetches daily OHLCV price data(Open, High, Low,
# Close, Volume) for each ticker and loads it into 
# azure sql raw_stocks table

import requests
import pyodbc
import json
from dotenv import load_dotenv
import os
import sys
import time
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.db_connection import get_connection


load_dotenv()
api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

TICKERS = ["AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"]

def extract_stock_data(ticker):

    url= "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "apikey": api_key
    }

    print(f"Extracting stock data for {ticker}...")

    response =requests.get(url,params=params)
    data = response.json()

    if "Time Series (Daily)" not in data:
        print(f"API error for {ticker}: {data}")
        return None
    print(f" Data received for {ticker}")
    return data

def transform_stock_data(raw_data, ticker):
    time_series = raw_data["Time Series (Daily)"]

    records = []
    for date, values in time_series.items():
        record= {
            "ticker": ticker,
            "trade_date": date,
            "open_price": float(values["1. open"]),
            "high_price": float(values["2. high"]),
            "low_price": float(values["3. low"]),
            "close_price": float(values["4. close"]),
            "volume": int(values["5. volume"])
        }
        records.append(record)
    print(f"  Transformed {len(records)} records for {ticker}")
    return records
   
def load_stock_data(cursor, records):
    print(f"loading {records[0]['ticker']} stock data into Azure SQL...")

    load_count=0
    for record in records:
        sql="""
            if not exists(
            select 1 from raw_stocks
            where ticker = ? and trade_date = ?
            )
            INSERT INTO raw_stocks
            (ticker, trade_date, open_price, high_price, low_price, close_price, volume)
            VALUES
                (?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(sql,
                    record["ticker"],
                    record["trade_date"],
                    record["ticker"],
                    record["trade_date"],
                    record["open_price"],
                    record["high_price"],
                    record["low_price"],
                    record["close_price"],
                    record["volume"]

        )
        load_count += 1

    print(f"  Loaded data for {record['ticker']} on {record['trade_date']} succesfully.")

def run_stock_pipeline():

    print("=" * 55)
    print("RAW_STOCK DATA PIPELINE STARTING")
    print("=" * 55)

    conn = get_connection()
    cursor = conn.cursor()
    print(" Connected!\n")

    success_count = 0
    failed_count  = 0

    for i, ticker in enumerate(TICKERS):
        print(f"\n[{i+1}/{len(TICKERS)}] Processing {ticker}")
        print("-" * 40)

        raw_data = extract_stock_data(ticker)
        if raw_data is None:
            failed_count += 1
            continue

        records = transform_stock_data(raw_data, ticker)
        load_stock_data(cursor, records)

        success_count += 1

        if ticker != TICKERS[-1]:
            print(f"\n⏳ Waiting 15 seconds (rate limiting)...")
            time.sleep(15)
    conn.commit()
    print("\n All changes committed to database")

    cursor.close()
    conn.close()
    print(" Connection closed")

     # --- Summary ---
  
    print(f"   Successfully loaded : {success_count} tickers")
    print(f"   Failed              : {failed_count} tickers")
    print(f"   Total processed     : {len(TICKERS)} tickers")

if __name__ == "__main__":
    run_stock_pipeline()
