import yfinance as yf
import requests
import pandas as pd
import time
from datetime import datetime, timedelta

# Base API URL for fetching stock symbols
BASE_URL = "https://www.tsx.com/json/company-directory/search/tsx/"

# Letters to scrape (A-Z & 0-9)
letters = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["0-9"]

# List to store scraped data
symbols = []

# Step 1: Scrape stock symbols from TSX
for letter in letters:
    print(f"Fetching data for: {letter}")
    url = f"{BASE_URL}{letter}"

    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()  # Check for HTTP errors

        data = response.json()  # Convert response to JSON
        if "results" in data:
            for entry in data["results"]:
                symbol = entry.get("symbol", "").strip()
                company_name = entry.get("name", "").strip()

                # Append TSX suffix ('.TO') for Yahoo Finance compatibility
                yf_symbol = symbol + ".TO"

                symbols.append((symbol, company_name, yf_symbol))

            print(f"Found {len(data['results'])} symbols for {letter}")
        else:
            print(f"No results for {letter}")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching {letter}: {e}")

# Convert to DataFrame
df = pd.DataFrame(symbols, columns=["Symbol", "Company Name", "Yahoo Symbol"])
df.to_csv("tmx_symbols.csv", index=False)

print(f"\n✅ Scraped {len(symbols)} symbols and saved to tmx_symbols.csv")

# Step 2: Fetch stock market data (Open, High, Low, Close) for the last 3 months
stock_data = []
end_date = datetime.today().strftime('%Y-%m-%d')
start_date = (datetime.today() - timedelta(days=90)).strftime('%Y-%m-%d')

for yf_symbol in df["Yahoo Symbol"]:
    try:
        print(f"Fetching stock data for: {yf_symbol} (Last 3 Months)")
        stock = yf.Ticker(yf_symbol)
        hist = stock.history(start=start_date, end=end_date)

        if not hist.empty:
            for index, row in hist.iterrows():
                stock_data.append((yf_symbol, index, row["Open"], row["High"], row["Low"], row["Close"]))
            print(f"✅ {yf_symbol}: Data fetched for last 3 months")

        time.sleep(1)  # To avoid getting blocked by Yahoo

    except Exception as e:
        print(f"❌ Error fetching data for {yf_symbol}: {e}")

# Convert to DataFrame and save stock prices
df_prices = pd.DataFrame(stock_data, columns=["Yahoo Symbol", "Date", "Open", "High", "Low", "Close"])
df_prices.to_csv("stock_prices.csv", index=False)

print(f"\n✅ Fetched stock prices for last 3 months and saved to stock_prices.csv")
