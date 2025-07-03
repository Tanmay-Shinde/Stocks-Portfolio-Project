# 📊 Stocks Portfolio Project

A modular, end-to-end data engineering project for stock market analytics using **Python**, **PostgreSQL (NeonDB)**, and **SQLAlchemy**. The project ingests raw NSE (National Stock Exchange) data, processes it through **Bronze** → **Silver** → **Gold** layered pipelines, and generates **technical indicators** like **moving averages**, **RSI**, and **buy/sell signals**.

This project simulates a **real-world data warehouse** and trading signal generator pipeline, complete with fact/dimension modeling and historical market analytics.

## 🚀 Features
- Bronze/Silver/Gold architecture following data lakehouse principles.
- Technical indicators: 7/14/21/28-day Moving Averages and RSI.
- Trading strategy simulation based on moving average crossovers.
- Buy/Sell signal generation and client trade simulations.
- Clean, reproducible, modular Python codebase.

## 📁 File Structure
```
database/
├── bronze/                # Staging layer scripts for raw data ingestion
│   ├── stock_daily_staging.py
│   └── symbol_staging.py
├── silver/                # Cleaned and normalized fact/dimension tables
│   ├── calendar_dim.py
│   ├── holiday_dim.py
│   ├── member_dim.py
│   ├── member_symbol_fact.py
│   ├── stock_daily_fact.py
│   ├── symbols_dim.py
│   └── trading_dim.py
├── gold/                  # Analytics-ready tables and signal logic
│   ├── moving_avg_fact.py
│   ├── rsi_index_fact.py
│   ├── buy_sell_moving_avg_fact.py
│   ├── gainers_losers.py
│   └── member_buy_sell.py
raw_files/
├── nse_data.csv           # NSE tickers data 
├── nse_holidays_data.csv  # Market holiday dates
.env                       # Contains DB credentials (not version-controlled)
append.py                  # Utility to append or manage table data
connection.py              # PostgreSQL connection logic via SQLAlchemy
setup.py                   # One-step script to build the entire pipeline
README.md
```

## 🔑 Setup Instructions

### 1. Clone the repository
Run the following commands in bash/cmd:
```
git clone https://github.com/your-username/StocksPortfolioProject.git
cd StocksPortfolioProject
```

### 2. Set up a NeonDB PostgreSQL instance
1. Go to [https://neon.tech](https://neon.tech) and create a free account.
2. Create a new project and get your connection string (host, db, username, password, etc.).
3. Open [connection.py](./connection.py) and update your connection string, leaving out the password

### 3. Configure your .env file
Create a `.env` file in the root directory and include your database connection password as follows: `DB_PASSWORD=your_neon_password`

⚠️ *Make sure .env is included in .gitignore for security.*

## 🧪 Replicating the Project
Once your .env and other files are in place, run the full pipeline setup:
```
python setup.py
```
This will:
- Connect to your NeonDB database
- Create and populate all bronze, silver, and gold layer tables
- Output logs for each transformation step

## 🙏 Acknowledgements:
| Type                       | Tool / Library                                                                                                                            |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| **Dataset**             | [NSE Tickers Data](https://www.kaggle.com/datasets/ianalyticsgeek/nse-tickers-their-yahoo-finance-equivalent-codes?resource=download) – Base stock market data containing ticker information. |
| **Dataset**             | [NSE Holidays Data](https://www.niftyindices.com/resources/holiday-calendar) – Public market holiday data used for trading calendar alignment.                                                       |
| **API Library**         | [yfinance](https://github.com/ranaroussi/yfinance) – Fetches historical stock price data via Yahoo Finance.                               |
| **Database**           | [NeonDB](https://neon.tech/) – Serverless PostgreSQL platform used to host the project database.                                          |
| **ORM/DB Tool**         | [SQLAlchemy](https://www.sqlalchemy.org/) – SQL toolkit and ORM for database interaction.                                                 |
| **Data Analysis**       | [pandas](https://pandas.pydata.org/) – For data transformation and analysis.                                                              |
| **Numerical Computing** | [NumPy](https://numpy.org/) – Used for array operations and numerical logic.                                                              |
| **Date Handling**        | [datetime](https://docs.python.org/3/library/datetime.html) – Built-in Python module for date manipulation.                               |
| **File Paths**          | [pathlib](https://docs.python.org/3/library/pathlib.html) – Clean and platform-independent file paths.                                    |
| **IDE**                 | [PyCharm](https://www.jetbrains.com/pycharm/) – Development environment used to build and manage this project.                            |

## 📄 License
This project is open-source and available under the [MIT License](./LICENSE.md).

**Disclaimer:** This project is for educational and personal learning purposes only. I am not a financial advisor, and this project **does not constitute financial advice**. The trading signals, analysis, or tools presented here **should not be used for real-world investment decisions**.
