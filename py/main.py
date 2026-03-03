import time
import threading
import requests
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()

# Allow frontend to access this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global cache to serve the frontend instantly
DASHBOARD_CACHE = {"status": "initializing"}
STABLECOINS = ['USDT', 'USDC', 'FDUSD', 'TUSD', 'BUSD', 'DAI']

def fetch_market_data():
    """Background task to fetch and calculate market breadth."""
    global DASHBOARD_CACHE
    
    while True:
        try:
            print("Fetching new market cycle data...")
            
            # 1. Fetch Fear & Greed
            fg_res = requests.get('https://api.alternative.me/fng/?limit=30&format=json').json()
            fg_history = [{"time": pd.to_datetime(d['timestamp'], unit='s').strftime('%Y-%m-%d'), "value": int(d['value'])} for d in fg_res['data']]
            fg_history.reverse()

            # 2. Fetch Binance Tickers (Daily & Weekly)
            # Binance allows rolling windows, we use 1d and 7d
            daily_tickers = requests.get('https://api.binance.com/api/v3/ticker?windowSize=1d').json()
            weekly_tickers = requests.get('https://api.binance.com/api/v3/ticker?windowSize=7d').json()
            
            # Filter for pure USDT Altcoins
            valid_symbols = []
            for t in daily_tickers:
                sym = t['symbol']
                if sym.endswith('USDT') and not any(sym.startswith(s) for s in STABLECOINS) and not sym.endswith('UPUSDT') and not sym.endswith('DOWNUSDT'):
                    valid_symbols.append(sym)

            # Process Gainers & Volume (Daily)
            daily_valid = [t for t in daily_tickers if t['symbol'] in valid_symbols]
            daily_gainers = sorted(daily_valid, key=lambda x: float(x['priceChangePercent']), reverse=True)[:10]
            daily_volume = sorted(daily_valid, key=lambda x: float(x['quoteVolume']), reverse=True)[:10]

            # Process Gainers & Volume (Weekly)
            weekly_valid = [t for t in weekly_tickers if t['symbol'] in valid_symbols]
            weekly_gainers = sorted(weekly_valid, key=lambda x: float(x['priceChangePercent']), reverse=True)[:10]
            weekly_volume = sorted(weekly_valid, key=lambda x: float(x['quoteVolume']), reverse=True)[:10]

            # 3. Calculate SMA Breadth & 52-Week Extremes
            breadth_history = {}
            highs_52w = 0
            lows_52w = 0
            
            # Limit to top 200 by volume to keep engine fast (adjustable)
            top_200_symbols = [t['symbol'] for t in daily_volume[:200]]

            for sym in top_200_symbols:
                # Fetch 365 days of klines
                klines = requests.get(f'https://api.binance.com/api/v3/klines?symbol={sym}&interval=1d&limit=365').json()
                if len(klines) < 200: continue # Skip coins without enough history
                
                df = pd.DataFrame(klines, columns=['time', 'open', 'high', 'low', 'close', 'vol', 'ct', 'qav', 'not', 'tbv', 'tbqav', 'ign'])
                df['time'] = pd.to_datetime(df['time'], unit='ms').dt.strftime('%Y-%m-%d')
                df['close'] = df['close'].astype(float)
                df['high'] = df['high'].astype(float)
                df['low'] = df['low'].astype(float)
                
                # Calculate SMAs
                df['sma10'] = df['close'].rolling(10).mean()
                df['sma20'] = df['close'].rolling(20).mean()
                df['sma50'] = df['close'].rolling(50).mean()
                df['sma200'] = df['close'].rolling(200).mean()

                # 52-Week Extremes (Current close vs 365-day min/max)
                current_close = df['close'].iloc[-1]
                max_365 = df['high'].max()
                min_365 = df['low'].min()
                if current_close >= (max_365 * 0.98): highs_52w += 1
                if current_close <= (min_365 * 1.02): lows_52w += 1

                # Aggregate Breadth for the last 30 days
                last_30 = df.tail(30)
                for _, row in last_30.iterrows():
                    date = row['time']
                    if date not in breadth_history:
                        breadth_history[date] = {'total': 0, 'a10': 0, 'a20': 0, 'a50': 0, 'a200': 0}
                    
                    breadth_history[date]['total'] += 1
                    if pd.notna(row['sma10']) and row['close'] > row['sma10']: breadth_history[date]['a10'] += 1
                    if pd.notna(row['sma20']) and row['close'] > row['sma20']: breadth_history[date]['a20'] += 1
                    if pd.notna(row['sma50']) and row['close'] > row['sma50']: breadth_history[date]['a50'] += 1
                    if pd.notna(row['sma200']) and row['close'] > row['sma200']: breadth_history[date]['a200'] += 1

                time.sleep(0.05) # Respect Binance rate limits

            # Format Breadth for Frontend
            formatted_breadth = {
                'b10': [], 'b20': [], 'b50': [], 'b200': []
            }
            for date in sorted(breadth_history.keys()):
                d = breadth_history[date]
                if d['total'] > 0:
                    formatted_breadth['b10'].append({'time': date, 'value': (d['a10'] / d['total']) * 100})
                    formatted_breadth['b20'].append({'time': date, 'value': (d['a20'] / d['total']) * 100})
                    formatted_breadth['b50'].append({'time': date, 'value': (d['a50'] / d['total']) * 100})
                    formatted_breadth['b200'].append({'time': date, 'value': (d['a200'] / d['total']) * 100})

            # Update Global Cache
            DASHBOARD_CACHE = {
                "status": "active",
                "fear_greed": fg_history,
                "breadth": formatted_breadth,
                "extremes": {"highs": highs_52w, "lows": lows_52w},
                "lists": {
                    "daily_gainers": [{"symbol": t['symbol'], "val": float(t['priceChangePercent'])} for t in daily_gainers],
                    "weekly_gainers": [{"symbol": t['symbol'], "val": float(t['priceChangePercent'])} for t in weekly_gainers],
                    "daily_volume": [{"symbol": t['symbol'], "val": float(t['quoteVolume'])} for t in daily_volume],
                    "weekly_volume": [{"symbol": t['symbol'], "val": float(t['quoteVolume'])} for t in weekly_volume]
                }
            }
            print("Market data synced successfully.")
            
            # Rest for 1 hour before updating historicals again
            time.sleep(3600) 

        except Exception as e:
            print(f"Engine Error: {e}")
            time.sleep(60)

@app.on_event("startup")
def startup_event():
    # Start the data engine in the background
    thread = threading.Thread(target=fetch_market_data, daemon=True)
    thread.start()

@app.get("/api/dashboard")
def get_dashboard_data():
    return DASHBOARD_CACHE

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
