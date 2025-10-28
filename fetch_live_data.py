import pandas as pd
import json
from datetime import datetime, timedelta
import threading
import time
from SharekhanApi.sharekhanWebsocket import SharekhanWebSocket

class SharekhanLiveDataProcessor:
    def __init__(self, access_token):
        self.access_token = access_token
        self.sws = SharekhanWebSocket(access_token)
        self.live_data = pd.DataFrame()
        self.lock = threading.Lock()
        self.is_connected = False
        self.connection_thread = None
        
        # Define the 5-hour time slots
        self.time_slots = [
            "09:00-14:00",
            "14:00-19:00", 
            "19:00-23:30"
        ]
        
        # Initialize DataFrame with proper columns
        self.init_dataframe()
        
        # Setup callbacks
        self.sws.on_open = self.on_open
        self.sws.on_data = self.on_data
        self.sws.on_message = self.on_message
        self.sws.on_error = self.on_error
        self.sws.on_close = self.on_close
    
    def init_dataframe(self):
        """Initialize the DataFrame with proper columns"""
        columns = [
            'timestamp', 'symbol', 'token', 'ltp', 'volume', 'open', 'high', 
            'low', 'close', 'change', 'change_percent', 'bid', 'ask',
            'bid_qty', 'ask_qty', 'total_buy_qty', 'total_sell_qty', 'oi', 'oi_change'
        ]
        self.live_data = pd.DataFrame(columns=columns)
        print("DataFrame initialized with columns:", columns)
    
    def get_time_slot(self, timestamp):
        """Determine which 5-hour slot a timestamp belongs to"""
        hour = timestamp.hour
        if 9 <= hour < 14:
            return "09:00-14:00"
        elif 14 <= hour < 19:
            return "14:00-19:00"
        else:
            return "19:00-24:00"
    
    def parse_sharekhan_message(self, message):
        """Parse incoming message from Sharekhan WebSocket"""
        try:
            # Handle different message types
            if isinstance(message, str):
                if message in ['ping', 'pong']:
                    return []
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    print(f"Could not parse JSON: {message}")
                    return []
            else:
                data = message
            
            # Handle list of ticks
            if isinstance(data, list):
                parsed_data = []
                for item in data:
                    parsed_item = self.extract_tick_fields(item)
                    if parsed_item:
                        parsed_data.append(parsed_item)
                return parsed_data
            
            # Handle single tick
            elif isinstance(data, dict):
                parsed_item = self.extract_tick_fields(data)
                return [parsed_item] if parsed_item else []
            
            return []
                
        except Exception as e:
            print(f"Error parsing message: {e}")
            return []
    
    def extract_tick_fields(self, data):
        """Extract relevant fields from tick data based on Sharekhan format"""
        try:
            # Common field mappings for Sharekhan data
            parsed_data = {
                'timestamp': datetime.now(),
                'symbol': str(data.get('tk', data.get('symbol', ''))),  # Token or Symbol
                'token': str(data.get('tk', '')),  # Token
                'ltp': self.safe_float(data.get('lp', data.get('ltp', 0))),  # Last Price
                'volume': self.safe_float(data.get('v', data.get('volume', 0))),  # Volume
                'open': self.safe_float(data.get('o', data.get('open', 0))),   # Open
                'high': self.safe_float(data.get('h', data.get('high', 0))),   # High
                'low': self.safe_float(data.get('l', data.get('low', 0))),    # Low
                'close': self.safe_float(data.get('c', data.get('close', 0))),  # Close
                'change': self.safe_float(data.get('nc', data.get('change', 0))),  # Net Change
                'change_percent': self.safe_float(data.get('pc', data.get('change_percent', 0))),  # Percentage Change
                'bid': self.safe_float(data.get('bp1', data.get('bid', 0))),  # Best Bid
                'ask': self.safe_float(data.get('sp1', data.get('ask', 0))),  # Best Ask
                'bid_qty': self.safe_float(data.get('bq1', data.get('bid_qty', 0))),  # Bid Quantity
                'ask_qty': self.safe_float(data.get('sq1', data.get('ask_qty', 0))),  # Ask Quantity
                'total_buy_qty': self.safe_float(data.get('tbq', data.get('total_buy_qty', 0))),  # Total Buy Qty
                'total_sell_qty': self.safe_float(data.get('tsq', data.get('total_sell_qty', 0))),  # Total Sell Qty
                'oi': self.safe_float(data.get('oi', 0)),  # Open Interest
                'oi_change': self.safe_float(data.get('poi', 0)),  # OI Change
            }
            
            # Only return if we have a valid symbol or token
            if parsed_data['symbol'] or parsed_data['token']:
                return parsed_data
            return None
            
        except Exception as e:
            print(f"Error extracting fields: {e}")
            return None
    
    def safe_float(self, value):
        """Safely convert value to float"""
        try:
            if value is None or value == '':
                return 0.0
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def append_to_dataframe(self, data_list):
        """Append new data to the live dataframe"""
        if not data_list:
            return
            
        with self.lock:
            try:
                new_rows = pd.DataFrame(data_list)
                if self.live_data.empty:
                    self.live_data = new_rows
                else:
                    self.live_data = pd.concat([self.live_data, new_rows], ignore_index=True)
                
                # Keep only recent data to prevent memory issues (last 10000 records)
                if len(self.live_data) > 10000:
                    self.live_data = self.live_data.tail(10000).reset_index(drop=True)
                    
            except Exception as e:
                print(f"Error appending to dataframe: {e}")
    
    def on_open(self, wsapp):
        """Callback for WebSocket open"""
        print("✅ WebSocket connection established")
        self.is_connected = True
        
        # Subscribe to data feeds
        self.subscribe_to_feeds()
    
    def on_data(self, wsapp, data):
        """Callback for receiving binary data"""
        try:
            print(f"📊 Received binary data: {data}")
            parsed_data = self.parse_sharekhan_message(data)
            if parsed_data:
                self.append_to_dataframe(parsed_data)
                print(f"➕ Added {len(parsed_data)} records. Total: {len(self.live_data)}")
        except Exception as e:
            print(f"❌ Error processing binary data: {e}")
    
    def on_message(self, wsapp, message):
        """Callback for receiving text messages"""
        try:
            print(f"📨 Received message: {message}")
            parsed_data = self.parse_sharekhan_message(message)
            if parsed_data:
                self.append_to_dataframe(parsed_data)
                print(f"➕ Added {len(parsed_data)} records. Total: {len(self.live_data)}")
        except Exception as e:
            print(f"❌ Error processing message: {e}")
    
    def on_error(self, wsapp, error):
        """Callback for WebSocket errors"""
        print(f"❌ WebSocket error: {error}")
        self.is_connected = False
    
    def on_close(self, wsapp):
        """Callback for WebSocket close"""
        print("🔴 WebSocket connection closed")
        self.is_connected = False
    
    def subscribe_to_feeds(self):
        """Subscribe to market data feeds using Sharekhan format"""
        try:
            # Example subscription - modify tokens as needed
            # You can get these tokens from Sharekhan API documentation or instrument master
            token_list = {
                "action": "subscribe",
                "key": ["feed"],
                "value": [""]  # Empty for all feeds or specific tokens
            }
            
            print(f"📤 Subscribing to feeds: {token_list}")
            self.sws.subscribe(token_list)
            
            # Optional: Subscribe to specific instruments
            # Uncomment and modify as needed
            specific_feed = {
                 "action": "feed",
                 "key": ["depth"],
                 "value": ["MX458697"]  # Example tokens
             }
            self.sws.fetchData(specific_feed)
            
        except Exception as e:
            print(f"❌ Error subscribing to feeds: {e}")
    
    def start_connection(self):
        """Start the WebSocket connection in a separate thread"""
        if self.connection_thread and self.connection_thread.is_alive():
            print("⚠️ Connection already running")
            return
            
        self.connection_thread = threading.Thread(target=self._connect_worker)
        self.connection_thread.daemon = True
        self.connection_thread.start()
        print("🚀 Starting WebSocket connection...")
    
    def _connect_worker(self):
        """Worker function to run the WebSocket connection"""
        try:
            self.sws.connect()
        except Exception as e:
            print(f"❌ Connection error: {e}")
            self.is_connected = False
    
    def stop_connection(self):
        """Stop the WebSocket connection"""
        try:
            self.is_connected = False
            if self.sws:
                self.sws.close_connection()
            print("🔴 WebSocket connection stopped")
        except Exception as e:
            print(f"❌ Error stopping connection: {e}")
    
    def get_current_data(self):
        """Get current dataframe (thread-safe)"""
        with self.lock:
            return self.live_data.copy()
    
    def reshape_to_5hour_groups(self, symbol_filter=None):
        """
        Reshape data into 5-hour groups by day
        
        Parameters:
        - symbol_filter: Optional symbol/token to filter data
        
        Returns:
        - DataFrame with 5-hour grouped data
        """
        with self.lock:
            df = self.live_data.copy()
        
        if df.empty:
            print("📋 No data available for grouping")
            return pd.DataFrame()
        
        # Filter by symbol or token if specified
        if symbol_filter:
            mask = (df['symbol'].str.contains(str(symbol_filter), na=False) | 
                   df['token'].str.contains(str(symbol_filter), na=False))
            df = df[mask]
            if df.empty:
                print(f"📋 No data found for filter: {symbol_filter}")
                return pd.DataFrame()
        
        # Add date and time slot columns
        df['date'] = df['timestamp'].dt.date
        df['time_slot'] = df['timestamp'].apply(self.get_time_slot)
        
        try:
            # Group by date, symbol, and time slot
            grouped = df.groupby(['date', 'symbol', 'token', 'time_slot']).agg({
                'ltp': 'last',           # Last traded price
                'volume': 'sum',         # Total volume
                'open': 'first',         # First open price in the slot
                'high': 'max',           # Highest price in the slot
                'low': 'min',            # Lowest price in the slot
                'close': 'last',         # Last close price in the slot
                'timestamp': ['first', 'last'],  # First and last timestamp
                'bid': 'last',           # Last bid
                'ask': 'last',           # Last ask
                'total_buy_qty': 'sum',  # Sum of buy quantities
                'total_sell_qty': 'sum', # Sum of sell quantities
                'oi': 'last',            # Last open interest
                'oi_change': 'sum'       # Sum of OI changes
            }).reset_index()
            
            # Flatten column names
            grouped.columns = [
                'date', 'symbol', 'token', 'time_slot', 'ltp', 'total_volume', 
                'open', 'high', 'low', 'close', 'start_time', 'end_time',
                'bid', 'ask', 'total_buy_qty', 'total_sell_qty', 'oi', 'oi_change'
            ]
            
            # Calculate additional metrics
            grouped['price_change'] = grouped['close'] - grouped['open']
            grouped['price_change_percent'] = ((grouped['close'] - grouped['open']) / grouped['open'] * 100).round(2)
            grouped['price_range'] = grouped['high'] - grouped['low']
            grouped['spread'] = grouped['ask'] - grouped['bid']
            grouped['buy_sell_ratio'] = (grouped['total_buy_qty'] / (grouped['total_sell_qty'] + 0.01)).round(2)
            grouped['vwap'] = ((grouped['high'] + grouped['low'] + grouped['close']) / 3).round(2)  # Simple VWAP approximation
            
            # Sort by date and time slot
            slot_order = {slot: i for i, slot in enumerate(self.time_slots)}
            grouped['slot_order'] = grouped['time_slot'].map(slot_order)
            grouped = grouped.sort_values(['date', 'symbol', 'slot_order']).drop('slot_order', axis=1)
            
            print(f"📊 Created {len(grouped)} 5-hour groups")
            return grouped
            
        except Exception as e:
            print(f"❌ Error grouping data: {e}")
            return pd.DataFrame()
    
    def get_daily_summary(self, symbol_filter=None):
        """Get daily summary across all 5-hour slots"""
        grouped_data = self.reshape_to_5hour_groups(symbol_filter)
        
        if grouped_data.empty:
            return pd.DataFrame()
        
        try:
            # Group by date and symbol to get daily summary
            daily_summary = grouped_data.groupby(['date', 'symbol', 'token']).agg({
                'total_volume': 'sum',
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'start_time': 'min',
                'end_time': 'max',
                'total_buy_qty': 'sum',
                'total_sell_qty': 'sum',
                'oi': 'last',
                'oi_change': 'sum'
            }).reset_index()
            
            # Calculate daily metrics
            daily_summary['daily_change'] = daily_summary['close'] - daily_summary['open']
            daily_summary['daily_change_percent'] = ((daily_summary['close'] - daily_summary['open']) / daily_summary['open'] * 100).round(2)
            daily_summary['daily_range'] = daily_summary['high'] - daily_summary['low']
            daily_summary['daily_buy_sell_ratio'] = (daily_summary['total_buy_qty'] / (daily_summary['total_sell_qty'] + 0.01)).round(2)
            daily_summary['daily_vwap'] = ((daily_summary['high'] + daily_summary['low'] + daily_summary['close']) / 3).round(2)
            
            print(f"📈 Created daily summary with {len(daily_summary)} records")
            return daily_summary
            
        except Exception as e:
            print(f"❌ Error creating daily summary: {e}")
            return pd.DataFrame()
    
    def save_data_to_csv(self, filename="sharekhan_live_data.csv"):
        """Save current data to CSV"""
        with self.lock:
            if not self.live_data.empty:
                self.live_data.to_csv(filename, index=False)
                print(f"💾 Raw data saved to {filename} ({len(self.live_data)} records)")
            else:
                print("📋 No raw data to save")
    
    def save_grouped_data_to_csv(self, filename="sharekhan_5hour_data.csv", symbol_filter=None):
        """Save 5-hour grouped data to CSV"""
        grouped_data = self.reshape_to_5hour_groups(symbol_filter)
        if not grouped_data.empty:
            grouped_data.to_csv(filename, index=False)
            print(f"💾 5-hour grouped data saved to {filename} ({len(grouped_data)} records)")
        else:
            print("📋 No grouped data to save")
    
    def save_daily_summary_to_csv(self, filename="sharekhan_daily_summary.csv", symbol_filter=None):
        """Save daily summary to CSV"""
        daily_data = self.get_daily_summary(symbol_filter)
        if not daily_data.empty:
            daily_data.to_csv(filename, index=False)
            print(f"💾 Daily summary saved to {filename} ({len(daily_data)} records)")
        else:
            print("📋 No daily summary to save")
    
    def print_data_summary(self):
        """Print summary of current data"""
        with self.lock:
            if self.live_data.empty:
                print("📋 No data collected yet")
                return
            
            print(f"\n📊 --- Data Summary ---")
            print(f"📈 Total records: {len(self.live_data)}")
            print(f"🏢 Unique symbols: {self.live_data['symbol'].nunique()}")
            print(f"🎯 Unique tokens: {self.live_data['token'].nunique()}")
            print(f"⏰ Time range: {self.live_data['timestamp'].min()} to {self.live_data['timestamp'].max()}")
            
            # Show top symbols by activity
            if not self.live_data.empty:
                top_symbols = self.live_data['symbol'].value_counts().head(5)
                print(f"🔥 Top 5 active symbols:")
                for symbol, count in top_symbols.items():
                    print(f"   {symbol}: {count} ticks")


# Main execution function
def main():
    # Replace with your actual Sharekhan access tokenapi_key = "GIaiFf8eFa1gOfdC2W4m6RUb9YOvVYIu"
    with open('access.txt', 'r') as file:
        access_token = file.read().strip()
    
    if access_token == "your_actual_access_token_here":
        print("❌ Please replace 'your_actual_access_token_here' with your actual Sharekhan access token")
        return
    
    # Initialize the processor
    processor = SharekhanLiveDataProcessor(access_token)
    
    print("🚀 Starting Sharekhan Live Data Collection with 5-Hour Grouping")
    print("=" * 60)
    
    # Start the WebSocket connection
    processor.start_connection()
    
    # Wait for connection to establish
    print("⏳ Waiting for connection to establish...")
    time.sleep(5)
    
    if not processor.is_connected:
        print("❌ Failed to establish connection. Please check your access token and network.")
        return
    
    try:
        print("✅ Connection established! Collecting data...")
        print("📝 Press Ctrl+C to stop and save data")
        print("-" * 60)
        
        # Monitor data collection
        iteration = 0
        while processor.is_connected:
            time.sleep(30)  # Check every 30 seconds
            iteration += 1
            
            print(f"\n🔄 Iteration {iteration}")
            processor.print_data_summary()
            
            # Process data if available
            current_data = processor.get_current_data()
            if len(current_data) > 0:
                
                # Show 5-hour grouped data
                grouped_data = processor.reshape_to_5hour_groups()
                if not grouped_data.empty:
                    print(f"\n⏰ 5-hour groups created: {len(grouped_data)}")
                    print("📊 Sample 5-hour grouped data:")
                    print(grouped_data[['symbol', 'time_slot', 'open', 'high', 'low', 'close', 'total_volume']].head(3))
                
                # Show daily summary
                daily_summary = processor.get_daily_summary()
                if not daily_summary.empty:
                    print(f"\n📈 Daily summary records: {len(daily_summary)}")
                    print("📋 Daily Summary:")
                    print(daily_summary[['symbol', 'daily_change_percent', 'total_volume', 'daily_range']].head(2))
            
            print("=" * 60)
            
            # Save data periodically (every 5 iterations)
            if iteration % 5 == 0:
                print("💾 Periodic save...")
                processor.save_data_to_csv(f"periodic_data_{iteration}.csv")
                processor.save_grouped_data_to_csv(f"periodic_5hour_{iteration}.csv")
    
    except KeyboardInterrupt:
        print("\n\n⏹️ Stopping data collection...")
    
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
    
    finally:
        print("\n💾 Saving final data...")
        
        # Save all data formats
        processor.save_data_to_csv("final_sharekhan_raw_data.csv")
        processor.save_grouped_data_to_csv("final_sharekhan_5hour_data.csv")
        processor.save_daily_summary_to_csv("final_sharekhan_daily_summary.csv")
        
        # Stop connection
        processor.stop_connection()
        
        # Final summary
        final_data = processor.get_current_data()
        print(f"\n📊 Final Statistics:")
        print(f"   📈 Total records collected: {len(final_data)}")
        print(f"   ⏰ Collection duration: Complete")
        print(f"   💾 Files saved: 3 (raw, 5-hour grouped, daily summary)")
        
        print("\n✅ Data collection completed successfully!")
        print("🎉 Thank you for using Sharekhan Live Data Processor!")


if __name__ == "__main__":
    main()