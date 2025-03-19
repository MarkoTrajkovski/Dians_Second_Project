import yfinance as yf
import requests
import pandas as pd
import time
from datetime import datetime, timedelta


BASE_URL = "https://www.tsx.com/json/company-directory/search/tsx/"
letters = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["0-9"]


def scrape_symbols():
    symbols = []

    for letter in letters:
        print(f"Fetching data for: {letter}")
        url = f"{BASE_URL}{letter}"

        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
            data = response.json()

            if "results" in data:
                for entry in data["results"]:
                    symbol = entry.get("symbol", "").strip()
                    company_name = entry.get("name", "").strip()
                    yf_symbol = symbol + ".TO"

                    symbols.append((symbol, company_name, yf_symbol))

                print(f"Found {len(data['results'])} symbols for {letter}")
            else:
                print(f"No results for {letter}")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching {letter}: {e}")

    return pd.DataFrame(symbols, columns=["Symbol", "Company Name", "Yahoo Symbol"])


def fetch_stock_data(symbols_df):
    stock_data = []
    end_date = datetime.today().strftime('%Y-%m-%d')
    start_date = (datetime.today() - timedelta(days=90)).strftime('%Y-%m-%d')

    for yf_symbol in symbols_df["Yahoo Symbol"]:
        try:
            print(f"Fetching stock data for: {yf_symbol} (Last 3 Months)")
            stock = yf.Ticker(yf_symbol)
            hist = stock.history(start=start_date, end=end_date)


            last_price = stock.fast_info.get('lastPrice', None) or stock.fast_info.get('regularMarketPrice', None)

            if not hist.empty:
                for index, row in hist.iterrows():
                    date_str = index.strftime('%Y-%m-%d')
                    stock_data.append((yf_symbol, date_str, row["Open"], row["High"], row["Low"], row["Close"], last_price))
                print(f"{yf_symbol}: Data fetched for last 3 months")

            time.sleep(1)

        except Exception as e:
            print(f"Error fetching data for {yf_symbol}: {e}")

    return pd.DataFrame(stock_data, columns=["Yahoo Symbol", "Date", "Open", "High", "Low", "Close", "Last Price"])


def run_scraper():
    while True:
        print("\nRunning Stock Scraper...")


        symbols_df = scrape_symbols()
        symbols_df.to_csv("tmx_symbols.csv", index=False)
        print(f"Scraped {len(symbols_df)} symbols and saved to tmx_symbols.csv")


        stock_prices_df = fetch_stock_data(symbols_df)
        stock_prices_df.to_csv("stock_prices.csv", index=False)
        print(f"Fetched stock prices and saved to stock_prices.csv")

        print("Waiting 5 minutes for next update...\n")
        time.sleep(300)


run_scraper()
