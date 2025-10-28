import json
import pandas as pd
from datetime import datetime, timedelta, time
from collections import defaultdict
from SharekhanApi.sharekhanWebsocket import SharekhanWebSocket
from SharekhanApi.sharekhanConnect import SharekhanConnect


# ------------------------------
# Client for Live Ticks
# ------------------------------
class SharekhanClient:
    def __init__(self, token_file='access.txt'):
        with open(token_file, 'r') as f:
            self.access_token = f.read().strip()
        self.sws = SharekhanWebSocket(self.access_token)
        self.data_processor = None
        self.sws.on_open = self.on_open
        self.sws.on_data = self.on_data
        self.sws.on_error = self.on_error

    def on_open(self, wsapp):
        print("✅ Connected to WebSocket")
        self.sws.subscribe({
            "action": "feed", "key": ["ltp"], "value": ["MX458697"]
        })

    def on_data(self, wsapp, message):
        if message == "heartbeat":
            return
        try:
            data = json.loads(message)
            if self.data_processor:
                self.data_processor.add_tick(data)
        except Exception as e:
            print("Parse error:", e, message)

    def on_error(self, wsapp, error):
        print("WebSocket error:", error)

    def set_processor(self, processor):
        self.data_processor = processor

    def connect(self):
        try:
            self.sws.connect()
        except KeyboardInterrupt:
            self.sws.close_connection()


# ------------------------------
# Historical Data Fetcher
# ------------------------------
class HistoricalDataFetcher:
    def __init__(self, api_key, access_token):
        self.sharekhan = SharekhanConnect(api_key, access_token=access_token)

    def fetch_candles(self, days_back=5):
        try:
            order = self.sharekhan.master("MX")
            df = pd.DataFrame(order.get('data', []))
            if df.empty:
                raise ValueError("Master API returned no data")

            gold_fut = df[
                df['tradingSymbol'].str.contains("GOLDM", na=False) &
                (df['optionType'] == "FUT")
            ].copy()

            if gold_fut.empty:
                raise ValueError("No GOLDM FUT in master data")

            gold_fut['expiry'] = pd.to_datetime(gold_fut['expiry'], errors='coerce', dayfirst=True)
            nearest = gold_fut[gold_fut['expiry'] >= pd.Timestamp.today()].sort_values('expiry').iloc[0]

            scripcode = str(nearest['scripCode'])
            print(f"📌 Using {nearest['tradingSymbol']} (scripCode {scripcode})")

            history = self.sharekhan.historicaldata("MX", scripcode, "15minute")
            if not history or 'data' not in history:
                raise ValueError("No historical data in response")

            hist_df = pd.DataFrame(history['data'])
            hist_df['tradeDate'] = pd.to_datetime(hist_df['tradeDate'], format="%d/%m/%Y", errors="coerce")
            hist_df = hist_df.dropna(subset=['tradeDate'])
            cutoff = datetime.now() - timedelta(days=days_back)
            hist_df = hist_df[hist_df['tradeDate'] >= cutoff]

            candles = []
            for _, row in hist_df.iterrows():
                try:
                    candles.append({
                        'open': float(row['open']),
                        'high': float(row['high']),
                        'low': float(row['low']),
                        'close': float(row['close']),
                        'volume': int(row.get('volume', 1) or 1),
                        'timestamp': pd.to_datetime(row['tradeDate'])
                    })
                except Exception:
                    continue

            print(f"📈 Fetched {len(candles)} historical candles")
            return sorted(candles, key=lambda x: x['timestamp'])

        except Exception as e:
            print("Historical fetch failed:", e)
            return []


# ------------------------------
# 15-Minute Candle Builder
# ------------------------------
class FifteenMinOHLCProcessor:
    def __init__(self):
        self.data = defaultdict(dict)
        self.last_slot = {}
        self.on_candle_complete = None
        self.on_live_tick = None

    def preload_historical(self, candles):
        for c in candles:
            ts = c['timestamp']
            date = ts.strftime('%Y-%m-%d')
            slot = (ts.hour * 60 + ts.minute) // 15
            self.data[date][slot] = {**c, 'slot': slot}

    def add_tick(self, tick):
        if not isinstance(tick, dict):
            return
        if 'data' in tick and isinstance(tick['data'], list) and tick['data']:
            d = tick['data'][0]
            ltp = float(d.get('ltp', 0))
            vol = int(d.get('ltq', 1) or 1)
        else:
            ltp = float(tick.get('ltp', tick.get('price', 0)) or 0)
            vol = int(tick.get('volume', 1) or 1)

        if ltp <= 0:
            return

        ts = datetime.now()
        date = ts.strftime('%Y-%m-%d')
        slot = (ts.hour * 60 + ts.minute) // 15

        if ts.hour < 9 or ts.hour > 23:
            return

        # Update current candle
        if slot not in self.data[date]:
            self.data[date][slot] = {
                'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp,
                'volume': vol, 'timestamp': ts, 'slot': slot
            }
        else:
            c = self.data[date][slot]
            c['high'] = max(c['high'], ltp)
            c['low'] = min(c['low'], ltp)
            c['close'] = ltp
            c['volume'] += vol
            c['timestamp'] = ts

        # Finalize previous candle when slot changes
        if date in self.last_slot and slot != self.last_slot[date]:
            self._finalize(date, self.last_slot[date])
        
        if self.on_live_tick:
            self.on_live_tick(ltp, ts)
        
        self.last_slot[date] = slot

    def _finalize(self, date, slot):
        if slot in self.data[date] and self.on_candle_complete:
            self.on_candle_complete(date, slot, self.data[date][slot])


# ------------------------------
# Simple MACD Strategy
# ------------------------------
class SimpleMACDStrategy:
    def __init__(self, api_key, access_token, quantity=100, customer_id="4036182", scripcode="458697"):
        self.api_key = api_key
        self.sharekhan = SharekhanConnect(api_key, access_token=access_token)
        self.quantity = quantity
        self.customer_id = customer_id
        self.scripcode = scripcode
        
        # MACD parameters
        self.fast = 12
        self.slow = 26
        self.signal = 9
        
        # Data storage
        self.candles = []
        self.position = None  # 'BUY', 'SELL', or None
        self.trades = []
        self.ready = False
        self.last_signal_time = None
        
        # Current MACD values
        self.macd = 0
        self.macd_signal = 0
        self.prev_macd = 0
        self.prev_signal = 0

    def _ema(self, values, period):
        if len(values) < period:
            return sum(values) / len(values)
        mult = 2.0 / (period + 1)
        ema = sum(values[:period]) / period
        for v in values[period:]:
            ema = (v * mult) + (ema * (1 - mult))
        return ema

    def _calculate_macd(self):
        if len(self.candles) < max(self.fast, self.slow, self.signal) + 1:
            return
            
        closes = [c['close'] for c in self.candles]
        
        # Calculate MACD
        ema_fast = self._ema(closes, self.fast)
        ema_slow = self._ema(closes, self.slow)
        self.macd = ema_fast - ema_slow
        
        # Calculate MACD signal line
        if len(self.candles) >= max(self.fast, self.slow, self.signal):
            macd_values = []
            for i in range(max(self.fast, self.slow), len(self.candles)):
                temp_closes = closes[:i+1]
                temp_fast = self._ema(temp_closes, self.fast)
                temp_slow = self._ema(temp_closes, self.slow)
                macd_values.append(temp_fast - temp_slow)
            
            if len(macd_values) >= self.signal:
                self.macd_signal = self._ema(macd_values, self.signal)

    def on_new_candle(self, date, slot, candle):
        """Process completed candle"""
        self.candles.append(candle)
        
        # Store previous MACD values
        self.prev_macd = self.macd
        self.prev_signal = self.macd_signal
        
        # Need enough candles for MACD calculation
        required_candles = max(self.fast, self.slow, self.signal) + 5
        if len(self.candles) < required_candles:
            print(f"⚠️ Need {required_candles} candles, have {len(self.candles)}")
            return
            
        if not self.ready:
            self.ready = True
            print(f"✅ MACD Strategy ready with {len(self.candles)} candles")
        
        # Calculate MACD
        self._calculate_macd()
        
        # Check for signals
        self._check_macd_signals(candle)
        
        print(f"📊 MACD: {self.macd:.4f} | Signal: {self.macd_signal:.4f} | Position: {self.position}")

    def _check_macd_signals(self, candle):
        """Check for MACD crossover/crossunder signals"""
        if not self.ready or self.prev_macd == 0 or self.prev_signal == 0:
            return
            
        now = datetime.now()
        
        # Avoid duplicate signals (15-minute cooldown)
        if (self.last_signal_time and 
            (now - self.last_signal_time).total_seconds() < 900):
            return
        
        # MACD Crossover (MACD crosses above Signal) - BUY signal
        if (self.macd > self.macd_signal and self.prev_macd <= self.prev_signal):
            if self.position != 'BUY':
                print(f"🟢 MACD Crossover detected @ ₹{candle['close']:.2f}")
                self._place_order("BUY", candle['close'])
                
        # MACD Crossunder (MACD crosses below Signal) - SELL signal
        elif (self.macd < self.macd_signal and self.prev_macd >= self.prev_signal):
            if self.position != 'SELL':
                print(f"🔴 MACD Crossunder detected @ ₹{candle['close']:.2f}")
                self._place_order("SELL", candle['close'])

    def on_live_tick(self, ltp, timestamp):
        """Process live ticks - minimal implementation"""
        # Market hours check
        current_time = timestamp.time()
        if not (time(9, 30) <= current_time <= time(23, 30)):
            return

    def _place_order(self, side, price):
        """Place order with Sharekhan API"""
        if self.position == side:
            print(f"⚠️ Already in {side} position, skipping order")
            return
            
        order_params = {
            "customerId": self.customer_id,
            "scripCode": self.scripcode,
            "tradingSymbol": "GOLDM",
            "exchange": "MX",
            "transactionType": "B" if side == "BUY" else "S",
            "quantity": self.quantity,
            "disclosedQty": 0,
            "price": 0.0,
            "triggerPrice": 0.0,
            "rmsCode": "ANY",
            "afterHour": "N",
            "orderType": "MKT",
            "expiry": "05/09/2025",
            "instrumentType": "FS",
            "optionType": "XX",
            "strikePrice": "-1",
            "channelUser": self.customer_id,
            "validity": "GFD",
            "requestType": "NEW",
            "productType": "INVESTMENT"
        }
        
        try:
            resp = self.sharekhan.placeOrder(order_params)
            print(f"✅ {side} order @ ₹{price:.2f} | Response: {resp}")
            
            # Update tracking
            self.last_signal_time = datetime.now()
            self.position = side
            self.trades.append({
                'time': self.last_signal_time,
                'side': side,
                'price': price,
                'response': resp
            })
            
        except Exception as e:
            print(f"❌ Order failed: {e}")

    def summary(self):
        """Print trading summary"""
        print("\n" + "="*50)
        print("         MACD STRATEGY SUMMARY")
        print("="*50)
        print(f"Total Trades: {len(self.trades)}")
        print(f"Current Position: {self.position or 'None'}")
        print(f"Last MACD: {self.macd:.4f}")
        print(f"Last Signal: {self.macd_signal:.4f}")
        print("="*50)


# ------------------------------
# Main Execution
# ------------------------------
if __name__ == "__main__":
    api_key = "GIaiFf8eFa1gOfdC2W4m6RUb9YOvVYIu"
    with open('access.txt', 'r') as f:
        access_token = f.read().strip()

    # Fetch historical data
    fetcher = HistoricalDataFetcher(api_key, access_token)
    historical_candles = fetcher.fetch_candles(7)

    # Initialize components
    processor = FifteenMinOHLCProcessor()
    strategy = SimpleMACDStrategy(
        api_key=api_key,
        access_token=access_token,
        quantity=100,
        customer_id="4036182",
        scripcode="463393"
    )
    
    # Connect components
    processor.on_candle_complete = strategy.on_new_candle
    processor.on_live_tick = strategy.on_live_tick
    
    # Load historical data
    processor.preload_historical(historical_candles)

    # Start live trading
    client = SharekhanClient() 
    client.set_processor(processor)
    print("🚀 Starting live MACD trading...")
    print("📊 Strategy: MACD Crossover (12,26,9) for BUY/SELL signals")
    
    try:
        client.connect()
    except KeyboardInterrupt:
        print("\n⛔ Trading stopped by user")

    # Save results
    if strategy.trades:
        pd.DataFrame(strategy.trades).to_csv("macd_live_trades.csv", index=False)
        print("💾 Trades saved to macd_live_trades.csv")
    
    strategy.summary()