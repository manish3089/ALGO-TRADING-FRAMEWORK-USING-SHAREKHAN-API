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
        # subscribe feed
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
            # Get master list
            order = self.sharekhan.master("MX")
            df = pd.DataFrame(order.get('data', []))
            if df.empty:
                raise ValueError("Master API returned no data")

            # GOLDM FUT only
            gold_fut = df[
                df['tradingSymbol'].str.contains("GOLDM", na=False) &
                (df['optionType'] == "FUT")
            ].copy()

            if gold_fut.empty:
                raise ValueError("No GOLDM FUT in master data")

            gold_fut['expiry'] = pd.to_datetime(gold_fut['expiry'], errors='coerce', dayfirst=True)
            gold_fut = gold_fut.dropna(subset=['expiry'])

            nearest = gold_fut[gold_fut['expiry'] >= pd.Timestamp.today()] \
                .sort_values('expiry').iloc[0]

            scripcode = str(nearest['scripCode'])
            print(f"📌 Using {nearest['tradingSymbol']} (scripCode {scripcode})")

            # Historical
            history = self.sharekhan.historicaldata("MX", scripcode, "15minute")
            if not history or 'data' not in history:
                raise ValueError("No historical data in response")

            hist_df = pd.DataFrame(history['data'])
            if hist_df.empty:
                raise ValueError("Historical rows empty")

            hist_df['tradeDate'] = pd.to_datetime(
                hist_df['tradeDate'], format="%d/%m/%Y", errors="coerce"
            )
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
            slot = (ts.hour * 60 + ts.minute) // 15   # 15-minute slots
            self.data[date][slot] = {**c, 'slot': slot}

    def add_tick(self, tick):
        # Normalize tick format
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

        # Update current candle with live tick
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
        
        # Process live tick
        if self.on_live_tick:
            self.on_live_tick(ltp, ts)
        
        self.last_slot[date] = slot

    def _finalize(self, date, slot):
        if slot in self.data[date] and self.on_candle_complete:
            self.on_candle_complete(date, slot, self.data[date][slot])


# ------------------------------
# Updated Keltner Channel Strategy
# ------------------------------
class KeltnerChannelStrategy:
    def __init__(self, initial_balance=1000000, size_pct=0.1, 
                 daily_start_time="09:15", daily_end_time="23:30",
                 ema_length=10, keltner_period=10, atr_period=10, keltner_multiplier=3):
        
        # Trading parameters
        self.cash = initial_balance
        self.position = None
        self.size_pct = size_pct
        
        # Time parameters
        self.daily_start_time = datetime.strptime(daily_start_time, "%H:%M").time()
        self.daily_end_time = datetime.strptime(daily_end_time, "%H:%M").time()
        
        # Technical parameters
        self.ema_length = ema_length
        self.keltner_period = keltner_period
        self.atr_period = atr_period
        self.keltner_multiplier = keltner_multiplier
        
        # Data storage
        self.candles = []
        self.trades = []
        self.ready = False
        
        # Current indicators (for last completed candle)
        self.current_ema = 0
        self.previous_ema = 0
        self.upper_keltner = 0
        self.lower_keltner = 0
        self.long_entry_level = 0
        self.short_entry_level = 0
        
        # Initialize Sharekhan connection for order placement
        self.api_key = None
        self.sharekhan = None

    def set_sharekhan_connection(self, api_key, access_token):
        """Set up Sharekhan connection for order placement"""
        self.api_key = api_key
        self.sharekhan = SharekhanConnect(api_key, access_token=access_token)
        
        # Initialize Sharekhan connection for order placement
        self.api_key = None
        self.sharekhan = None

    def set_sharekhan_connection(self, api_key, access_token):
        """Set up Sharekhan connection for order placement"""
        self.api_key = api_key
        self.sharekhan = SharekhanConnect(api_key, access_token=access_token)

    def _ema(self, values, period):
        if len(values) < period:
            return sum(values) / len(values)
        mult = 2.0 / (period + 1)
        ema = sum(values[:period]) / period
        for v in values[period:]:
            ema = (v * mult) + (ema * (1 - mult))
        return ema

    def _atr(self, highs, lows, closes, period):
        trs = []
        for i in range(1, len(highs)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            )
            trs.append(tr)
        return sum(trs[-period:]) / min(period, len(trs)) if trs else 0

    def _is_trading_time(self, current_time):
        """Check if current time is within trading hours"""
        current_time_only = current_time.time()
        return self.daily_start_time <= current_time_only <= self.daily_end_time

    def on_new_candle(self, date, slot, candle):
        """Process completed candle and update indicators"""
        self.candles.append(candle)
        
        # Need enough candles for indicators
        required_candles = max(self.ema_length, self.keltner_period, self.atr_period) + 1
        if len(self.candles) < required_candles:
            return
            
        if not self.ready:
            self.ready = True
            print(f"✅ Strategy ready with {len(self.candles)} candles")
        
        # Calculate indicators for last completed candle
        self._update_indicators()
        
        # Check for position exits on completed candle
        if self.position:
            self._check_exit_conditions(candle['timestamp'])

    def _update_indicators(self):
        """Update all indicators based on completed candles"""
        if len(self.candles) < max(self.ema_length, self.keltner_period, self.atr_period) + 1:
            return
            
        closes = [c['close'] for c in self.candles]
        highs = [c['high'] for c in self.candles]
        lows = [c['low'] for c in self.candles]
        
        # Calculate EMAs for last two candles
        self.previous_ema = self._ema(closes[:-1], self.ema_length)
        self.current_ema = self._ema(closes, self.ema_length)
        
        # Keltner Channel calculation
        typical_prices = [(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)]
        keltner_ma = self._ema(typical_prices, self.keltner_period)
        atr = self._atr(highs, lows, closes, self.atr_period)
        
        self.upper_keltner = keltner_ma + (atr * self.keltner_multiplier)
        self.lower_keltner = keltner_ma - (atr * self.keltner_multiplier)
        
        # Calculate entry limits (keeping original logic)
        self.long_entry_level = self.current_ema + (1/8) * (self.upper_keltner - self.current_ema)
        self.short_entry_level = self.current_ema - (1/8) * (self.current_ema - self.lower_keltner)
        
        print(f"📊 EMA: {self.current_ema:.2f} (prev: {self.previous_ema:.2f})")
        print(f"📊 Keltner: Upper {self.upper_keltner:.2f}, Lower {self.lower_keltner:.2f}")
        print(f"🎯 Entry Levels: Long {self.long_entry_level:.2f}, Short {self.short_entry_level:.2f}")

    def on_live_tick(self, ltp, timestamp):
        """Process live ticks for entry/exit decisions"""
        if not self.ready:
            return
            
        # Check if within trading hours
        if not self._is_trading_time(timestamp):
            # Close position if time exceeded
            if self.position:
                self._close_position(ltp, timestamp, "Time Exceeded")
            return
        
        # Check for entries (only if no position)
        if not self.position:
            self._check_entry_conditions(ltp, timestamp)
        else:
            # Check tick-based exits (Keltner breakouts)
            self._check_tick_exits(ltp, timestamp)

    def _check_entry_conditions(self, ltp, timestamp):
        """Check for long/short entry conditions"""
        
        # Long entry conditions
        if (self.current_ema > self.previous_ema and  # EMA trending up
            ltp < self.long_entry_level):  # Price below long entry level
            self._open_position("LONG", ltp, timestamp)
            return
            
        # Short entry conditions  
        if (self.current_ema < self.previous_ema and  # EMA trending down
            ltp > self.short_entry_level):  # Price above short entry level
            self._open_position("SHORT", ltp, timestamp)
            return

        print(f"[DEBUG] LTP={ltp}, EMA={self.current_ema:.2f}, "
              f"PrevEMA={self.previous_ema:.2f}, "
              f"LongEntry={self.long_entry_level:.2f}, "
              f"ShortEntry={self.short_entry_level:.2f}, "
              f"Position={self.position}")

    def _check_exit_conditions(self, timestamp):
        """Check EMA trend reversal exits on completed candles"""
        if not self.position:
            return
            
        if self.position['type'] == "LONG":
            # Exit long if EMA turns down
            if self.current_ema < self.previous_ema:
                # Use last candle's close price for exit
                exit_price = self.candles[-1]['close']
                self._close_position(exit_price, timestamp, "EMA Trend Reversal")
                return
                
        elif self.position['type'] == "SHORT":
            # Exit short if EMA turns up
            if self.current_ema > self.previous_ema:
                # Use last candle's close price for exit
                exit_price = self.candles[-1]['close']
                self._close_position(exit_price, timestamp, "EMA Trend Reversal")
                return

    def _check_tick_exits(self, ltp, timestamp):
        """Check Keltner band breakout exits on live ticks"""
        if not self.position:
            return
            
        if self.position['type'] == "LONG":
            # Exit long if price hits or exceeds upper Keltner
            if ltp >= self.upper_keltner:
                self._close_position(ltp, timestamp, "Upper Keltner Hit")
                return
                
        elif self.position['type'] == "SHORT":
            # Exit short if price hits or goes below lower Keltner
            if ltp <= self.lower_keltner:
                self._close_position(ltp, timestamp, "Lower Keltner Hit")
                return

    def _open_position(self, side, price, timestamp):
        """Opens a new position"""
        # Calculate quantity based on available cash
        qty = max(1, int((self.cash * self.size_pct) / price))
        
        # Handle cash allocation for both LONG and SHORT positions
        if side == "LONG":
            cost = qty * price
            if self.cash < cost:
                print(f"❌ Insufficient cash for LONG: Need {cost:.2f}, Have {self.cash:.2f}")
                return
            self.cash -= cost
        elif side == "SHORT":
            # For short positions, we might need margin requirements
            # This is a simplified model - in reality you'd need margin calculations
            margin_requirement = qty * price * 0.1  # Assume 10% margin
            if self.cash < margin_requirement:
                print(f"❌ Insufficient margin for SHORT: Need {margin_requirement:.2f}, Have {self.cash:.2f}")
                return
            self.cash -= margin_requirement  # Block margin amount

        # Record the position
        self.position = {
            "type": side, 
            "qty": qty, 
            "entry": price,
            "margin_blocked": margin_requirement if side == "SHORT" else 0
        }

        # Log the trade
        self.trades.append({
            "time": timestamp, 
            "action": f"OPEN_{side}", 
            "price": price, 
            "qty": qty,
            "reason": "Entry Signal"
        })

        # Place order with Sharekhan (only if connection is available)
        if self.sharekhan:
            self._place_sharekhan_order(side, qty, price)
        else:
            print("⚠️ Sharekhan connection not available - simulating order only")

        print(f"🚀 OPENED {side}: {qty} @ {price:.2f} | Cash: {self.cash:.2f}")

    def _place_sharekhan_order(self, side, qty, price):
        """Place actual order with Sharekhan API"""
        orderparams = {
            "customerId": 4036182,
            "scripCode": 458697,
            "tradingSymbol": "GOLDM",
            "exchange": "MX",
            "transactionType": "B" if side == "LONG" else "S",
            "quantity": qty,
            "disclosedQty": 0,
            "price": float(price),
            "triggerPrice": 0.0,
            "rmsCode": "ANY",
            "afterHour": "N",
            "orderType": "LIMIT",  # Changed from NORMAL to LIMIT for price specification
            "expiry": "05/09/2025",
            "instrumentType": "FS",
            "optionType": "XX",
            "strikePrice": "-1",
            "channelUser": "4036182",
            "validity": "GFD",
            "requestType": "NEW",
            "productType": "INVESTMENT"
        }
        
        try:
            order = self.sharekhan.placeOrder(orderparams)
            print(f"✅ Sharekhan Order Response: {order}")
        except Exception as e:
            print(f"⚠️ Sharekhan order placement failed: {e}")

    def _close_position(self, price, timestamp, reason):
        """Closes current position"""
        if not self.position:
            return
            
        pos = self.position
        
        # Calculate P&L and update cash
        if pos['type'] == "LONG":
            pnl = pos['qty'] * (price - pos['entry'])
            self.cash += pos['qty'] * price  # Get back the full value
        else:  # SHORT
            pnl = pos['qty'] * (pos['entry'] - price)
            self.cash += pos.get('margin_blocked', 0)  # Release blocked margin
            self.cash += pnl  # Add profit or subtract loss
            
        self.trades.append({
            "time": timestamp, 
            "action": "CLOSE", 
            "price": price,
            "qty": pos['qty'], 
            "pnl": pnl, 
            "reason": reason,
            "position_type": pos['type']
        })
        
        # Place closing order with Sharekhan
        if self.sharekhan:
            self._place_closing_order(pos, price)
        
        print(f"💰 CLOSED {pos['type']}: {pos['qty']} @ {price:.2f} | PnL: {pnl:.2f} ({reason}) | Cash: {self.cash:.2f}")
        self.position = None

    def _place_closing_order(self, position, price):
        """Place closing order with Sharekhan"""
        # Opposite transaction type for closing
        close_transaction = "S" if position['type'] == "LONG" else "B"
        
        orderparams = {
            "customerId": 4036182,
            "scripCode": 458697,
            "tradingSymbol": "GOLDM",
            "exchange": "MX",
            "transactionType": close_transaction,
            "quantity": position['qty'],
            "disclosedQty": 0,
            "price": float(price),
            "triggerPrice": 0.0,
            "rmsCode": "ANY",
            "afterHour": "N",
            "orderType": "LIMIT",
            "expiry": "05/09/2025",
            "instrumentType": "FS",
            "optionType": "XX",
            "strikePrice": "-1",
            "channelUser": "4036182",
            "validity": "GFD",
            "requestType": "NEW",
            "productType": "INVESTMENT"
        }
        
        try:
            order = self.sharekhan.placeOrder(orderparams)
            print(f"✅ Closing Order Response: {order}")
        except Exception as e:
            print(f"⚠️ Closing order placement failed: {e}")

    def summary(self):
        """Print trading summary"""
        total_pnl = sum(t.get('pnl', 0) for t in self.trades)
        wins = len([t for t in self.trades if t.get('pnl', 0) > 0])
        losses = len([t for t in self.trades if t.get('pnl', 0) < 0])
        total_trades = wins + losses
        win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
        
        print("\n" + "="*50)
        print("            TRADING SUMMARY")
        print("="*50)
        print(f"Final Cash Balance: ₹{self.cash:.2f}")
        print(f"Total P&L: ₹{total_pnl:.2f}")
        print(f"Total Trades: {len(self.trades)}")
        print(f"Wins: {wins} | Losses: {losses}")
        print(f"Win Rate: {win_rate:.1f}%")
        if self.position:
            print(f"Open Position: {self.position['type']} @ {self.position['entry']:.2f}")
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
    print(f"📈 Loaded {len(historical_candles)} historical candles")

    # Initialize components with custom parameters
    processor = FifteenMinOHLCProcessor()
    strategy = KeltnerChannelStrategy(
        initial_balance=1000000, 
        size_pct=0.1,
        daily_start_time="09:00",  # Trading start time
        daily_end_time="23:30",   # Trading end time
        ema_length=10,            # EMA period
        keltner_period=10,        # Keltner MA period
        atr_period=10,            # ATR period
        keltner_multiplier=3      # Keltner multiplier
    )
    
    # Set up Sharekhan connection for the strategy
    strategy.set_sharekhan_connection(api_key, access_token)
    
    # Set up Sharekhan connection for the strategy
    strategy.set_sharekhan_connection(api_key, access_token)
    processor.on_candle_complete = strategy.on_new_candle
    processor.on_live_tick = strategy.on_live_tick
    
    # Load historical data
    processor.preload_historical(historical_candles)

    # Start live trading
    client = SharekhanClient()
    client.set_processor(processor)
    print("🚀 Starting live trading with new strategy rules...")
    print("📍 Entries based on EMA trend + price limits, Exits on trend reversal or Keltner breakout!")
    
    try:
        client.connect()
    except KeyboardInterrupt:
        print("\n⛔ Trading stopped by user")

    # Save results
    if strategy.trades:
        pd.DataFrame(strategy.trades).to_csv("keltner_strategy_trades.csv", index=False)
        print("💾 Trades saved to keltner_strategy_trades.csv")
    
    strategy.summary()