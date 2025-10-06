# Stock Market ETL Pipeline
_Note: this is still under development_

A comprehensive demonstration of common data engineering practices through the use of Python, Apache Spark & Airflow, SQL and Flask.

## Project Overview

This project implements a complete ETL pipeline for stock market data. 
We showcase:
- Data Extraction (using Alpha Vantage API)
- Data Transformation (Apache Spark / PySpark)
- Data Loading (Into a PostgreSQL database)
- Workflow Orchestration (Apache Airflow)
- Data Visualization (via Flask web dashboard)

## Architecture
1. Access data from ALpha Vantage API
2. Construct the raw data as CSV/JSON
3. Transform with PySpark
4. Load into a PostgreSQL database
5. Display with a Flask Dashboard

We can also wrap this with Apache Airflow for orchestration

# Quick Start
## Prerequisites
- Python 3.8+ 
- PostgreSQL 12+
- Apache Spark 3.5+
- Apache Airflow 2.7+
- Alpha Vantage API key (free tier optional)

### 1. Environment Setup
```bash
# Clone the repository
git clone <repository-url>

# Create a virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration
```bash
# Copy environment template
cp .env.example .env

# Edit .env file with your local settings
ALPHA_VANTAGE_API_KEY=your_individual_api_key_here
DB_HOST=localhost
DB_PORT=1234
DB_NAME=stock_etl
DB_USER=postgres
DB_PASSWORD=password
```

### 3. Database Setup
```bash
# Start PostgreSQL (if using Docker)
docker run --name postgres-stock \
    -e POSTGRES_DB=stock_etl \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=password \
    -p 1234:1234 -d postgres:13

# Create database schema
psql -h localhost -U postgres -d stock_etl -f sql/create_tables.sql
```

### 4. Run the Pipeline
#### A) Run individual components
```bash
# 1. Extract the stock data
python scripts/fetch_stock_data.py

# 2. Transform with Spark
python scripts/transform_spark.py --input data/raw/ --output data/processed/

# 3. Load to SQL database
python scripts/load_to_db.py --daily-prices data/processed/daily_prices --indicators data/processed/stock_indicators
```

#### B) Airflow Orchestration
```bash
# Initialize Airflow database
airflow db init

# Start the Airflow webserver and scheduler
airflow webserver --port 8080 &
airflow schedular &

# You can then access the UI at http://localhost:8080
# Then trigger the 'stock_market_etl_pipeline' DAG 
```

### 5. Launch Web Dash
```bash
# Start the Flask application
python webapp/app.py

# Access the dashboard at http://localhost:5000
```

# Features
## Portfolio Overview
- __Total Symbols Tracked__: Real-time count of monitored stocks
- __Average Daily Return__: Performance metric on portfolio
- __Success Rate__: Percentage of stocks with positive returns
- __Market Status__: Connection and recent data indicators

## Interactive Charts
- __Price Analysis__: Candlestick charts with volume indicators
- __Technical Indicators__: Moving averages (SMA 5, 20, 50, 200)
- __Volatility Metrics__: 30-day rolling volatility calculations
- __Trend Analysis__: Algorithmic trend strength assessment

## Real-time Data
- __Auto-refresh__: Updates every 5 minutes
- __Symbol Selection__: Dynamic switching between stocks
- __Time Periods__: 30, 90, 180-day views
- __Performance Metrics__: Top performers and high-volume activity


