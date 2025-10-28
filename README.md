# 🧠 Algo Trading Framework using Sharekhan API

An end-to-end algorithmic trading system built in Python for **NIFTY-based stock trading**.  
This project integrates with the **Sharekhan API** to fetch historical and live market data, generate tokens, backtest strategies, and execute trades automatically.

---

## 🚀 Features

- **API Integration:** Connects with Sharekhan’s trading API for real-time and historical market data.  
- **Authentication:** Secure token-based login and session management.  
- **Data Handling:** Fetches and stores live and historical data (CSV & Pandas DataFrames).  
- **Strategy Module:** Includes multiple trading strategies such as Keltner Channel and candlestick-based signals.  
- **Backtesting Engine:** Tests trading strategies on historical datasets before live execution.  
- **Live Execution:** Places and manages live orders automatically through the broker API.  
- **Modular Design:** Separate scripts for data fetching, strategy generation, and order placement.

---

## 🧩 Project Structure

SharekhanApi/              # API handling and integration scripts
templates/                 # Frontend templates (if web interface is used)
static/                    # Static assets
15min_gold_candles.csv     # Sample historical dataset
15min_keltner.py           # Strategy implementation
Historical_Data.csv        # Historical stock data
Token Gen.ipynb            # Notebook for token generation and API testing
fetch_live_data.py         # Fetches real-time data from Sharekhan API
placeorder.py              # Handles live order placement
setup.py                   # Project setup script
requirements.txt           # Project dependencies
README.md                  # Project documentation



---

## ⚙️ Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/manish3089/ALGO-TRADING-FRAMEWORK-USING-SHAREKHAN-API
   cd ALGO-TRADING-FRAMEWORK-USING-SHAREKHAN-API

⚠️ Disclaimer

This project is for educational and research purposes only.
Live trading involves risk — use at your own discretion and test thoroughly before deploying in a production environment.
