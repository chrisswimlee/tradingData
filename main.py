import asyncio
import json
import websockets
import aiohttp  # Use the async HTTP client
from config import POLYGON_WS_URL, POLYGON_API_KEY
from datetime import date, timedelta

# --- Configuration ---
API_KEY = POLYGON_API_KEY
WS_URL = POLYGON_WS_URL

# --- Global State ---
yesterday_close = {}
running_volume = {}
gainers = {}


def get_last_trading_day():
    """Calculates the most recent trading day (handles weekends)."""
    today = date.today()
    # If today is Sunday (weekday is 6), subtract 2 days to get Friday.
    if today.weekday() == 6:
        offset = timedelta(days=2)
    # If today is Monday (weekday is 0), subtract 3 days to get Friday.
    elif today.weekday() == 0:
        offset = timedelta(days=3)
    # Otherwise, just subtract 1 day.
    else:
        offset = timedelta(days=1)
    last_day = today - offset
    return last_day.strftime('%Y-%m-%d')  # Format as YYYY-MM-DD

async def fetch_yesterday_closes():
    """Asynchronously fetches the previous day's closing prices."""
    print("Fetching yesterday's closing prices...")
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{get_last_trading_day()}?apiKey={API_KEY}"
    # Use aiohttp for non-blocking network requests
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"Error fetching data: {response.status}")
                return
            data = await response.json()
            for stock in data.get("results", []):
                yesterday_close[stock["T"]] = stock["c"]
    print(f"Successfully fetched {len(yesterday_close)} closing prices.")

async def stream():
    """Connects to the WebSocket and processes real-time data."""
    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps({"action": "auth", "params": API_KEY}))
        await ws.send(json.dumps({"action": "subscribe", "params": "AM.*"}))
        print("Connected and Waiting for data...")

        try:
            # Use `async for` for a robust loop that handles closure
            async for msg in ws:
                data = json.loads(msg)
                for event in data:
                    if event.get("ev") == "AM":
                        ticker = event["T"]
                        close_price = event["c"]
                        volume = event["v"]

                        if ticker in yesterday_close:
                            prev_close = yesterday_close[ticker]
                            pct_change = ((close_price - prev_close) / prev_close) * 100
                            running_volume[ticker] = running_volume.get(ticker, 0) + volume
                            if (pct_change >= 5 and
                                    running_volume[ticker] >= 2_000_000 and
                                    close_price >= 3):
                                gainers[ticker] = {
                                    "price": close_price,
                                    "pct_change": pct_change,
                                    "volume": running_volume[ticker]
                                }
                                print(f"🚀 ALERT: {ticker} is up {pct_change:.2f}% | "
                                      f"Price: ${close_price} | Volume: {running_volume[ticker]:,}")

        except websockets.exceptions.ConnectionClosedError as e:
            print(f"Connection lost unexpectedly: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


async def main():
    """Main function to orchestrate the startup sequence."""
    await fetch_yesterday_closes()

    if yesterday_close:
        await stream()
    else:
        print("Could not fetch initial data. Shutting down.")


if __name__ == "__main__":
    # Run the main async function just once
    asyncio.run(main())