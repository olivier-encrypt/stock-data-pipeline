# ============================================
# explore_api.py
# Purpose: Call the Alpha Vantage API, inspect
# the response, and figure out our schema
# This is called "data profiling" in real life
# ============================================

#--- import ---
import requests    #let's python make API calls for us
import json        #let's python read the JSON responses we get back from the API
import os
from dotenv import load_dotenv # the tool that reads our .env file

#--load API KEY from .env file--
load_dotenv()
api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

# ============================================
#  CALL THE API
# ============================================
ticker ="AAPL" #the stock ticker we want to get data for

URL = (f"https://www.alphavantage.co/query"
       f"?function=TIME_SERIES_DAILY"
       f"&symbol={ticker}"
       f"&apikey={api_key}"
       )

print("=" * 50)
print("CALLING THE API")
print("=" * 50)
print(f"requesting data for: {ticker}")

response=requests.get(URL) #make the API call and store the response in a variable
print(f"API call status code: {response.status_code}\n") #print the status code of the response to see if it was successful (200 means success)


# ============================================
#  INSPECT THE RAW JSON
# ============================================

print("=" * 50)
print(" RAW JSON RESPONSE (first 500 characters)")
print("=" * 50)

# .text gives us the raw response as a string
raw_text = response.text
print(raw_text[:500]) #print the first 500 characters of the raw response to see what it looks like
print("...\n")

# ============================================
#  CONVERT JSON TO PYTHON DICTIONARY
# ============================================

print("=" * 50)
print(" CONVERTED TO PYTHON DICTIONARY")
print("=" * 50)


# .json() converts the raw JSON string into a Python dictionary that we can work with more easily
data_dict = response.json()
for key in data_dict.keys():
    print(f" -> {key}")
print("\n")

print("=" * 50)
print(" ACCESSING DICTIONARY KEYS")
print("=" * 50)

# The API response is a nested dictionary, so we need to access the keys step by step to get to the data we want
# The top level keys are "Meta Data" and "Time Series (Daily)"

Meta = data_dict["Meta Data"]

for key, value in Meta.items():
    print(f"{key}: {value}")
print("\n")
time_series = data_dict["Time Series (Daily)"]

# Get just the most recent date's data
most_recent_date = list(time_series.keys())[0]  # first key = most recent date
most_recent_data  = time_series[most_recent_date]

print(f"Most recent trading date: {most_recent_date}")
for key, value in most_recent_data.items():
    print(f"{key}: {value}")
print("\n")


print("=" * 50)
print(" SCHEMA DISCOVERY - COLUMNS WE NEED")
print("=" * 50)

# Based on the data we see in the API response, we can determine what columns we need in our database table

print(f"  ticker        → we pass this in ourselves         → example: '{ticker}'")
print(f"  trade_date    → the date key in time_series       → example: '{most_recent_date}'")

for key, value in most_recent_data.items():
    # Clean up the key name (remove "1. ", "2. " prefixes)
    clean_key = key.split(". ")[1].replace(" ", "_")
    print(f"  {clean_key:<14}→ from API field '{key}'  → example: {value}")

print(f"\n  id            → we generate this ourselves (auto increment)")
print(f"  ingested_at   → we generate this ourselves (current timestamp)")




