import yfinance as yf
import requests
import pandas as pd
import time
import random
from datetime import datetime, timedelta
import logging
import os
import sys

# Set up logging with proper encoding
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stock_scraper.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()


# Remove emojis from log messages to avoid encoding issues on Windows
def log_info(message):
    # Replace common emojis with text equivalents
    clean_message = (message
                     .replace("🔍", "[SEARCH]")
                     .replace("🔄", "[REFRESH]")
                     .replace("📊", "[STATS]")
                     .replace("🚀", "[BATCH]")
                     .replace("✅", "[OK]")
                     .replace("⚠️", "[WARN]")
                     .replace("❌", "[ERROR]")
                     .replace("⏰", "[TIME]")
                     )
    logger.info(clean_message)


def log_warning(message):
    clean_message = (message
                     .replace("🔍", "[SEARCH]")
                     .replace("🔄", "[REFRESH]")
                     .replace("📊", "[STATS]")
                     .replace("🚀", "[BATCH]")
                     .replace("✅", "[OK]")
                     .replace("⚠️", "[WARN]")
                     .replace("❌", "[ERROR]")
                     .replace("⏰", "[TIME]")
                     )
    logger.warning(clean_message)


def log_error(message):
    clean_message = (message
                     .replace("🔍", "[SEARCH]")
                     .replace("🔄", "[REFRESH]")
                     .replace("📊", "[STATS]")
                     .replace("🚀", "[BATCH]")
                     .replace("✅", "[OK]")
                     .replace("⚠️", "[WARN]")
                     .replace("❌", "[ERROR]")
                     .replace("⏰", "[TIME]")
                     )
    logger.error(clean_message)


# Batch settings
BATCH_SIZE = 50  # Fetch stocks in batches
BATCH_DELAY = 10  # Reduce delay to 10 seconds between batches


# Function to scrape stock symbols from TSX
def scrape_symbols():
    BASE_URL = "https://www.tsx.com/json/company-directory/search/tsx/"
    letters = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ["0-9"]
    symbols = []

    for letter in letters:
        log_info(f"[SEARCH] Fetching symbols for: {letter}")
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

            time.sleep(random.uniform(1, 3))  # Prevents API blocking

        except requests.exceptions.RequestException as e:
            log_error(f"[ERROR] Error fetching {letter}: {e}")

    return symbols


# Load known problematic symbols from file or create if not exists
def load_problematic_symbols():
    try:
        with open("problematic_symbols.txt", "r") as f:
            return set(line.strip() for line in f.readlines())
    except FileNotFoundError:
        return set()


# Save problematic symbols to file
def save_problematic_symbol(symbol):
    with open("problematic_symbols.txt", "a") as f:
        f.write(f"{symbol}\n")


# Function to check if a stock is valid
def is_valid_stock(ticker):
    try:
        # Quietly check if the ticker exists
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1d")
        return not hist.empty
    except Exception:
        return False


# Function to fetch live stock prices (Last Price)
def fetch_last_prices(symbols):
    last_prices = {}
    problematic_symbols = load_problematic_symbols()

    for symbol in symbols:
        # Skip already known problematic symbols
        if symbol in problematic_symbols:
            continue

        try:
            # First check if the symbol is valid
            if not is_valid_stock(symbol):
                log_warning(f"[WARN] Skipping invalid symbol: {symbol}")
                save_problematic_symbol(symbol)
                continue

            # Use ticker.fast_info instead of ticker.info to avoid 404 errors
            stock = yf.Ticker(symbol)
            try:
                last_price = stock.fast_info.get('lastPrice')
                if last_price is None or pd.isna(last_price):
                    last_price = stock.fast_info.get('regularMarketPrice')
            except Exception:
                # Fall back to info if fast_info fails
                try:
                    info = stock.info
                    last_price = info.get("regularMarketPrice") if info and isinstance(info, dict) else None
                except Exception:
                    last_price = None

            last_prices[symbol] = last_price
            time.sleep(random.uniform(0.5, 1.5))  # Reduced delay to speed up process

        except Exception as e:
            log_warning(f"[WARN] Skipping {symbol}: Could not fetch last price")
            save_problematic_symbol(symbol)  # Add to problematic symbols
            continue  # Skip to the next symbol

    return last_prices


# Validate symbols to filter out invalid ones
def validate_symbols(symbols):
    valid_symbols = []
    problematic_symbols = load_problematic_symbols()

    for symbol in symbols:
        # Skip already known problematic symbols
        if symbol in problematic_symbols:
            continue

        # Skip symbols with complex structures that often cause errors
        if symbol.count('.') > 2 or '.DB.' in symbol or '.RT.' in symbol or '.PR.' in symbol:
            log_warning(f"[WARN] Skipping potentially problematic symbol: {symbol}")
            save_problematic_symbol(symbol)
            continue

        valid_symbols.append(symbol)
    return valid_symbols


# Function to scrape stock data in batches
def scrape_and_save_stock_data():
    try:
        all_symbols = scrape_symbols()
        # Filter out problematic symbols
        symbols = validate_symbols(all_symbols)

        log_info(f"[STATS] Processing {len(symbols)} valid symbols out of {len(all_symbols)} total")

        stock_data = []
        end_date = datetime.today().strftime('%Y-%m-%d')
        start_date = (datetime.today() - timedelta(days=90)).strftime('%Y-%m-%d')

        # Fetch Last Prices separately
        last_prices = fetch_last_prices(symbols)

        # Update symbols list after filtering during last_prices fetch
        symbols = [s for s in symbols if s in last_prices]
        log_info(f"[REFRESH] Proceeding with {len(symbols)} symbols after price validation")

        # Process in batches
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]  # Get a batch of stocks
            log_info(f"[BATCH] Fetching batch {i // BATCH_SIZE + 1}: {len(batch)} symbols")

            try:
                # Fetch historical data for the batch with error output suppressed
                with open(os.devnull, 'w') as devnull:
                    original_stderr = sys.stderr
                    sys.stderr = devnull
                    hist = yf.download(batch, start=start_date, end=end_date, group_by="ticker", progress=False)
                    sys.stderr = original_stderr

                # Handle different data formats
                if len(batch) > 1:  # If multiple stocks
                    for yf_symbol in batch:
                        try:
                            if yf_symbol in hist:
                                symbol_data = hist[yf_symbol]
                                if isinstance(symbol_data, pd.DataFrame) and not symbol_data.empty:
                                    stock_hist = symbol_data.dropna(subset=["Open", "Close"])
                                    if not stock_hist.empty:
                                        last_price = last_prices.get(yf_symbol)
                                        for index, row in stock_hist.iterrows():
                                            stock_data.append((yf_symbol, index.strftime('%Y-%m-%d'),
                                                               row.get("Open"), row.get("High"),
                                                               row.get("Low"), row.get("Close"), last_price))
                                        log_info(f"[OK] Data fetched for {yf_symbol}")
                                    else:
                                        log_warning(f"[WARN] No valid data for {yf_symbol}")
                                        save_problematic_symbol(yf_symbol)
                                else:
                                    log_warning(f"[WARN] Empty data for {yf_symbol}")
                                    save_problematic_symbol(yf_symbol)
                        except Exception as e:
                            log_warning(f"[WARN] Error processing {yf_symbol}")
                            save_problematic_symbol(yf_symbol)
                            continue

                else:  # If only one stock is returned
                    try:
                        if not hist.empty:
                            stock_hist = hist.dropna(subset=["Open", "Close"])
                            if not stock_hist.empty:
                                last_price = last_prices.get(batch[0])
                                for index, row in stock_hist.iterrows():
                                    stock_data.append((batch[0], index.strftime('%Y-%m-%d'),
                                                       row.get("Open"), row.get("High"),
                                                       row.get("Low"), row.get("Close"), last_price))
                                log_info(f"[OK] Data fetched for {batch[0]}")
                            else:
                                log_warning(f"[WARN] No valid data for {batch[0]}")
                                save_problematic_symbol(batch[0])
                        else:
                            log_warning(f"[WARN] Empty data for {batch[0]}")
                            save_problematic_symbol(batch[0])
                    except Exception as e:
                        log_warning(f"[WARN] Error processing {batch[0]}")
                        save_problematic_symbol(batch[0])

                time.sleep(BATCH_DELAY)  # Prevents hitting API limit

            except Exception as e:
                log_error(f"[ERROR] Error fetching batch {i // BATCH_SIZE + 1}: {e}")
                time.sleep(BATCH_DELAY)  # Still wait before trying next batch

        # Convert to DataFrame
        if stock_data:
            df_prices = pd.DataFrame(stock_data,
                                     columns=["Yahoo Symbol", "Date", "Open", "High", "Low", "Close", "Last Price"])

            # Deduplicate data
            df_prices = df_prices.drop_duplicates()

            # Save to CSV
            df_prices.to_csv("stock_prices.csv", index=False)
            log_info(
                f"[OK] Updated stock data saved to stock_prices.csv with {len(df_prices)} rows for {len(df_prices['Yahoo Symbol'].unique())} unique symbols")
        else:
            log_error("[ERROR] No stock data was collected")

    except Exception as e:
        log_error(f"[ERROR] Major error in scraping process: {e}")


# Run scraper every 5 minutes
while True:
    try:
        log_info("[REFRESH] Running stock data scraper...")
        scrape_and_save_stock_data()
    except Exception as e:
        log_error(f"[ERROR] Main scraper error: {e}")

    log_info(f"[TIME] Waiting for 5 minutes before next run...")
    time.sleep(300)  # Wait 5 minutes