import yfinance as yf
import pandas as pd
import datetime as dt
from datetime import datetime, time as dt_time
import pytz
import random
from sqlalchemy import create_engine, text
import schedule
import time
import sys



# ✅ PostgreSQL connection
DB_URL = "postgresql://postgres:HappyFriday%4021@postgres:5432/stock_data"
engine = create_engine(DB_URL)

def get_market_hours():
    eastern = pytz.timezone('US/Eastern')
    now = dt.datetime.now(eastern)
    today = now.strftime('%Y-%m-%d')

    market_open = eastern.localize(dt.datetime.strptime(f"{today} 09:30:00", '%Y-%m-%d %H:%M:%S'))
    market_close = eastern.localize(dt.datetime.strptime(f"{today} 16:00:00", '%Y-%m-%d %H:%M:%S'))

    return market_open, market_close, now

def is_market_open():
    market_open, market_close, now = get_market_hours()
    return now.weekday() < 5 and market_open <= now <= market_close

def fetch_symbols():
    try:
        query = "SELECT DISTINCT yahoo_symbol FROM stock_prices ORDER BY yahoo_symbol;"
        df = pd.read_sql(query, engine)
        return df['yahoo_symbol'].tolist()
    except Exception as e:
        print(f"⚠️ Error fetching symbols from DB: {e}")
        return ['AAPL.TO', 'MSFT.TO', 'SHOP.TO', 'TD.TO', 'RY.TO']

def store_live_data_batch(data_map):
    if not data_map:
        return

    market_open, market_close, _ = get_market_hours()
    all_records = []

    for symbol, df in data_map.items():
        if df is None or df.empty:
            continue

        df = df.reset_index()
        df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_convert('US/Eastern')
        df = df[(df['Datetime'] >= market_open) & (df['Datetime'] <= market_close)]
        df['time'] = df['Datetime'].dt.strftime('%H:%M:%S')

        for _, row in df.iterrows():
            all_records.append({
                "yahoo_symbol": symbol,
                "timestamp": row['Datetime'].strftime('%Y-%m-%d %H:%M:%S'),
                "time": row['time'],
                "open_price": float(row['Open']),
                "high_price": float(row['High']),
                "low_price": float(row['Low']),
                "close_price": float(row['Close']),
                "volume": float(row['Volume']) if 'Volume' in row else 0
            })

    if not all_records:
        return

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS intraday_prices (
                    id SERIAL PRIMARY KEY,
                    yahoo_symbol VARCHAR(20) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    time VARCHAR(10) NOT NULL,
                    open_price FLOAT NOT NULL,
                    high_price FLOAT NOT NULL,
                    low_price FLOAT NOT NULL,
                    close_price FLOAT NOT NULL,
                    volume FLOAT,
                    UNIQUE(yahoo_symbol, timestamp)
                );
            """))

            upsert_query = text("""
                INSERT INTO intraday_prices 
                (yahoo_symbol, timestamp, time, open_price, high_price, low_price, close_price, volume)
                VALUES 
                (:yahoo_symbol, :timestamp, :time, :open_price, :high_price, :low_price, :close_price, :volume)
                ON CONFLICT (yahoo_symbol, timestamp) 
                DO UPDATE SET 
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume;
            """)
            conn.execute(upsert_query, all_records)

        print(f"✅ Stored {len(all_records)} intraday records for {len(data_map)} symbols")

    except Exception as e:
        print(f"❌ Error writing batch to DB: {e}")

def fetch_and_store_batch(batch_symbols):
    try:
        ticker_str = " ".join(batch_symbols)
        tickers = yf.Tickers(ticker_str)

        data_map = {}
        failed = []

        for symbol in batch_symbols:
            try:
                df = tickers.tickers[symbol].history(period="1d", interval="1m")
                data_map[symbol] = df
            except Exception as e:
                print(f"⚠️ Error for {symbol}: {e}")
                failed.append(symbol)

        store_live_data_batch(data_map)

        if failed:
            print(f"🔁 Retrying {len(failed)} failed symbols in smaller bursts...")
            for symbol in failed:
                try:
                    time.sleep(random.uniform(0.5, 1.5))
                    df = yf.Ticker(symbol).history(period="1d", interval="1m")
                    if not df.empty:
                        store_live_data_batch({symbol: df})
                except Exception as e:
                    print(f"❌ Final fail for {symbol}: {e}")

    except Exception as e:
        print(f"❌ Batch fetch failed: {e}")

def update_all_symbols():
    if not is_market_open():
        print(f"⏳ Market closed. Skipping fetch at {datetime.datetime.now()}")
        return

    symbols = fetch_symbols()
    batch_size = 50
    total = len(symbols)
    print(f"🟢 Total symbols: {total}, batching {batch_size} per request...")

    for i in range(0, total, batch_size):
        batch = symbols[i:i + batch_size]
        print(f"📦 Fetching batch {i // batch_size + 1}: {batch[:3]}...{batch[-1]} ({len(batch)} symbols)")
        fetch_and_store_batch(batch)
        time.sleep(random.uniform(4, 6))  # Short sleep to avoid hammering

    print(f"✅ Completed update for {total} symbols at {dt.datetime.now()}")

def run_scheduler():
    print("🔄 Real-time tracker with scheduling (9:30 AM to 4:00 PM EST)")

    schedule.every(1).minutes.do(update_all_symbols)

    try:
        while True:
            tz = pytz.timezone("US/Eastern")
            now = datetime.now(tz).time()

            start_time = dt_time(9, 30)
            end_time = dt_time(16, 0)

            if start_time <= now <= end_time:
                schedule.run_pending()
            else:
                print("⏳ Outside allowed time window. Skipping updates...")

            time.sleep(10)

    except KeyboardInterrupt:
        print("🛑 Scheduler stopped.")

if __name__ == "__main__":
    print("📈 Real-Time Stock Tracker (Batch x200)")
    print("---------------------------------------")
    market_open, market_close, now = get_market_hours()
    print(f"Market Hours: {market_open.strftime('%H:%M:%S')} - {market_close.strftime('%H:%M:%S')} EST")
    print(f"Current Time: {now.strftime('%Y-%m-%d %H:%M:%S')} EST")
    print(f"Market Status: {'OPEN' if is_market_open() else 'CLOSED'}")

    run_scheduler()
