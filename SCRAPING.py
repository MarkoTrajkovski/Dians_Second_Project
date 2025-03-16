import yfinance as yf
import requests
import pandas as pd
import time
import random
from datetime import datetime, timedelta



def scrape_symbols():
    BASE_URL = "https://www.tsx.com/json/company-directory/search/tsx/"
    letters = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["0-9"]
    symbols = []

    for letter in letters:
        print(f"Fetching symbols for: {letter}")
        url = f"{BASE_URL}{letter}"
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
            data = response.json()
            if "results" in data:
                for entry in data["results"]:
                    symbol = entry.get("symbol", "").strip()
                    yf_symbol = symbol + ".TO"
                    symbols.append(yf_symbol)

            time.sleep(random.uniform(1, 3))

        except requests.exceptions.RequestException as e:
            print(f"Error fetching {letter}: {e}")

    return symbols



def scrape_and_save_stock_data():
    symbols = scrape_symbols()
    stock_data = []
    end_date = datetime.today().strftime('%Y-%m-%d')
    start_date = (datetime.today() - timedelta(days=90)).strftime('%Y-%m-%d')

    for yf_symbol in symbols:
        retries = 3
        while retries > 0:
            try:
                print(f"Fetching stock data for: {yf_symbol}")
                stock = yf.Ticker(yf_symbol)
                hist = stock.history(start=start_date, end=end_date)

                if not hist.empty:
                    for index, row in hist.iterrows():
                        stock_data.append((yf_symbol, index.strftime('%Y-%m-%d'),
                                           row["Open"], row["High"], row["Low"], row["Close"]))
                    print(f"Data fetched for {yf_symbol}")

                time.sleep(random.uniform(3, 7))
                break

            except Exception as e:
                print(f"Error fetching data for {yf_symbol}: {e}")
                if "Too Many Requests" in str(e):
                    retries -= 1
                    wait_time = random.uniform(10, 30)
                    print(f"Too many requests. Waiting {wait_time:.2f} sec before retrying...")
                    time.sleep(wait_time)
                else:
                    break


    df_prices = pd.DataFrame(stock_data, columns=["Yahoo Symbol", "Date", "Open", "High", "Low", "Close"])


    df_prices.to_csv("stock_prices.csv", index=False)
    print("\nUpdated stock data saved to stock_prices.csv")



while True:
    print("\nRunning stock data scraper...")
    scrape_and_save_stock_data()
    time.sleep(300)
