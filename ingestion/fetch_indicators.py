# fetch_indicators.py
#The putpose is to fetch daily technical indicators(SMA, RSI, EMA) for each ticker
# and load it into the technical_idicators table in azure sql

import requests
import pyodbc
from dotenv import load_dotenv
import os
import json
import time
import sys

from sqlalchemy import values
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.db_connection import get_connection

load_dotenv()
api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

TICKERS = ["AAPL", "MSFT", "GOOGL", "TSLA","AMZN"]

SMA_PERIOD = 20
EMA_PERIOD = 20
RSI_PERIOD = 14

def extract_indicator(ticker, function, period):
    url = "https://www.alphavantage.co/query"
    params = {
        "function": function,
        "symbol": ticker,
        "interval": "daily",
        "series_type": "close",
        "time_period": period,
        "apikey": api_key
    }
    print(f"Extracting {function} data for {ticker}...")
    response = requests.get(url, params=params)
    data = response.json()
    
    if f"Technical Analysis: {function}" not in data:
        print(f"API error for {ticker} {function}: {data}")
        return None
    print(f" Data received for {ticker} {function}")
    return data

def transform_indicators(ticker, sma_data, ema_data, rsi_data):
    print(f"  [TRANSFORM] Merging indicators for {ticker}...")
    sma_series = sma_data["Technical Analysis: SMA"]
    ema_series = ema_data["Technical Analysis: EMA"]
    rsi_series = rsi_data["Technical Analysis: RSI"]

# we create a set of dates for each indicator, then find the intersection of those sets to get the dates that have all 3 indicators available. 
# This way we ensure our final dataset only includes rows where we have complete data for all indicators.
    common_dates = set(sma_series.keys()) & set(ema_series.keys()) & set(rsi_series.keys())

    print(f" Found {len(common_dates)} common dates across all indicators")

    records = []
    for date in sorted(common_dates, reverse=True):
        record = {
            "ticker"    : ticker,
            "trade_date": date,
            "sma_20"    : float(sma_series[date]["SMA"]),
            "ema_20"    : float(ema_series[date]["EMA"]),
            "rsi_14"    : float(rsi_series[date]["RSI"])
        }
        records.append(record)
    print(f" Transformed {len(records)} records for {ticker}")
    return records

def load_indicators(cursor,records):
    print(f" INserting {records[0]['ticker']} indicators into Azure SQL...")

    sql = """
        IF NOT EXISTS (
            SELECT 1 FROM technical_indicators
            WHERE ticker = ? AND trade_date = ?
        )
        INSERT INTO technical_indicators
        (ticker, trade_date, sma_20, ema_20, rsi_14)
        VALUES (?, ?, ?, ?, ?)
    """
    #list comprehension to create a list of tuples for executemany. 
    # Each tuple corresponds to one record and contains the values in the order expected by the SQL query's placeholders.
    values= [(
        record["ticker"],       # WHERE ticker = ?
        record["trade_date"],   # WHERE trade_date = ?
        record["ticker"],
        record["trade_date"],
        record["sma_20"],
        record["ema_20"],
        record["rsi_14"]
    ) for record in records]     
    cursor.executemany(sql, values)
    
    print(f" Loaded {len(records)} records for {records[0]['ticker']}")

def run_indicators_pipeline():

    print("=" * 55)
    print("TECHNICAL INDICATORS PIPELINE STARTING")
    print("=" * 55)

    success_count = 0
    failed_count  = 0

    for i, ticker in enumerate(TICKERS):
        print(f"\n[{i+1}/{len(TICKERS)}] Processing {ticker}")
        print("-" * 40)

        # Fresh connection per ticker
        # Prevents timeout from killing the whole pipeline
        # If one ticker's connection dies, others are unaffected
        try:
            conn   = get_connection()
            cursor = conn.cursor()
            print("   Connected!")
        except Exception as e:
            print(f"   Could not connect: {e}")
            failed_count += 1
            continue

        try:
            sma_data = extract_indicator(ticker, "SMA", SMA_PERIOD)
            time.sleep(15)
            ema_data = extract_indicator(ticker, "EMA", EMA_PERIOD)
            time.sleep(15)
            rsi_data = extract_indicator(ticker, "RSI", RSI_PERIOD)

            if sma_data and ema_data and rsi_data:
                records = transform_indicators(ticker, sma_data, ema_data, rsi_data)
                load_indicators(cursor, records)

                # Fix 2 — Commit immediately after each ticker
                # Data is saved right away — if next ticker fails
                # this ticker's data is already safe in the database
                conn.commit()
                print(f"   {ticker} committed successfully")
                success_count += 1
            else:
                print(f"   Skipping {ticker} — missing indicator data")
                failed_count += 1

        except Exception as e:
            print(f"   Error processing {ticker}: {e}")
            failed_count += 1

        finally:
            # "finally" always runs — success OR failure
            # Guarantees connection is always closed cleanly
            try:
                cursor.close()
                conn.close()
                print(f"   Connection closed for {ticker}")
            except:
                pass

        # Rate limit between tickers
        if ticker != TICKERS[-1]:
            print(f"\n Waiting 45 seconds before next ticker...")
            time.sleep(45)

    # Summary
    # print("\n" + "=" * 55)
    # print("PIPELINE COMPLETE — SUMMARY")
    # print("=" * 55)
    print(f"   Successfully loaded : {success_count} tickers")
    print(f"   Failed              : {failed_count} tickers")
    print(f"   Total processed     : {len(TICKERS)} tickers")


  
if __name__ == "__main__":
    run_indicators_pipeline()