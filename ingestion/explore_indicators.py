# ============================================
# explore_indicators.py
# Purpose: Explore technical indicator data
# FIXED: Added rate limiting + error handling
# ============================================

import requests
from dotenv import load_dotenv
import os
import time  # ← NEW: lets us pause between API calls

load_dotenv()
api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
ticker  = "AAPL"

def fetch_indicator(function, extra_params, label):
    """
    Fetches a technical indicator from Alpha Vantage
    
    Why time.sleep(15)?
    Alpha Vantage free tier = 5 calls per minute
    60 seconds / 5 calls = 12 seconds minimum between calls
    We use 15 to be safe
    """
    
    print("=" * 55)
    print(f"INDICATOR: {label}")
    print("=" * 55)
    
    params = {
        "function"   : function,
        "symbol"     : ticker,
        "interval"   : "daily",
        "series_type": "close",
        "apikey"     : api_key
    }
    params.update(extra_params)
    
    response = requests.get(
        "https://www.alphavantage.co/query",
        params=params
    )
    data = response.json()
    
    # ← NEW: CHECK FOR API ERROR BEFORE DOING ANYTHING
    # If Alpha Vantage returns an error, it won't have
    # a "Meta Data" key — it'll have "Information" or
    # "Note" instead. We check for this first.
    if "Meta Data" not in data:
        # Print whatever the API actually returned
        print("⚠️  API did not return data. Response received:")
        for key, value in data.items():
            print(f"  → {key}: {value}")
        print("Skipping this indicator...\n")
        return None, None  # return nothing and move on
    
    # If we get here, data is good — proceed normally
    print(f"Top level keys:")
    for key in data.keys():
        print(f"  → {key}")
    
    data_key    = [k for k in data.keys() if k != "Meta Data"][0]
    time_series = data[data_key]
    latest_date = list(time_series.keys())[0]
    latest_data = time_series[latest_date]
    
    print(f"\nData key: '{data_key}'")
    print(f"\nMeta Data:")
    for k, v in data["Meta Data"].items():
        print(f"  {k}: {v}")
    
    print(f"\nMost recent date: {latest_date}")
    print(f"Fields returned:")
    for key, value in latest_data.items():
        clean_key = key.split(". ")[-1].lower().replace(" ", "_")
        print(f"  API field: '{key:<15}' → value: {value:<15} → SQL column: '{clean_key}'")
    
    # ← NEW: WAIT 15 SECONDS BEFORE NEXT API CALL
    # This is called "rate limiting" — respecting the
    # API's rules about how many calls you can make
    print(f"\n⏳ Waiting 15 seconds before next API call (rate limiting)...\n")
    time.sleep(15)
    
    return data_key, time_series


# ============================================
# EXPLORE EACH INDICATOR
# ============================================
# Note: This will take about 75 seconds total
# (5 indicators × 15 second pause each)
# That's normal — we're respecting the API limits

print("Note: Script will pause 15 seconds between calls")
print("This is rate limiting — totally normal!\n")

sma_key, sma_data = fetch_indicator(
    function     = "SMA",
    extra_params = {"time_period": "20"},
    label        = "SMA - Simple Moving Average (20 day)"
)

ema_key, ema_data = fetch_indicator(
    function     = "EMA",
    extra_params = {"time_period": "20"},
    label        = "EMA - Exponential Moving Average (20 day)"
)

rsi_key, rsi_data = fetch_indicator(
    function     = "RSI",
    extra_params = {"time_period": "14"},
    label        = "RSI - Relative Strength Index (14 day)"
)

macd_key, macd_data = fetch_indicator(
    function     = "MACD",
    extra_params = {
        "fastperiod"  : "12",
        "slowperiod"  : "26",
        "signalperiod": "9"
    },
    label = "MACD - Moving Average Convergence Divergence"
)

bb_key, bb_data = fetch_indicator(
    function     = "BBANDS",
    extra_params = {"time_period": "20"},
    label        = "BBANDS - Bollinger Bands (20 day)"
)

# ============================================
# SCHEMA RECOMMENDATION
# ============================================
print("=" * 55)
print("PROPOSED TABLE: technical_indicators")
print("=" * 55)
print("""
Based on the API responses, here are our columns:

  id              → auto increment primary key
  ticker          → stock symbol (FK to stock_metadata)
  trade_date      → date of the indicator value
  sma_20          → Simple Moving Average (20 day)
  ema_20          → Exponential Moving Average (20 day)
  rsi_14          → Relative Strength Index (14 day)
  macd            → MACD line value
  macd_signal     → MACD signal line
  macd_hist       → MACD histogram
  bb_upper        → Bollinger Band upper
  bb_middle       → Bollinger Band middle
  bb_lower        → Bollinger Band lower
  ingested_at     → our timestamp

CONSTRAINT: UNIQUE (ticker, trade_date)
""")