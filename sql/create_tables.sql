-- ===================================================
-- Stock ETL Pipeline - Database Schema Definition
-- ===================================================
-- 
-- This SQL script creates the complete database schema for the stock ETL pipeline.
-- It demonstrates best practices for financial data modeling including:
-- 
-- 1. Proper data types for financial data (Float for prices)
-- 2. Indexing strategies for time-series data
-- 3. Data integrity constraints
-- 4. Partitioning considerations for large datasets
-- 5. Audit fields for data lineage tracking

-- Drop existing tables if they exist (for development)
-- In production, use proper migration scripts instead
DROP TABLE IF EXISTS stock_indicators CASCADE;
DROP TABLE IF EXISTS daily_stock_prices CASCADE;
DROP TABLE IF EXISTS stock_symbols CASCADE;
DROP TABLE IF EXISTS pipeline_runs CASCADE;

-- ===================================================
-- Core Reference Tables
-- ===================================================

-- Stock symbols master table
-- This stores metadata about tracked stocks
CREATE TABLE stock_symbols (
    symbol VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    sector VARCHAR(100),
    industry VARCHAR(100),
    market_cap_category VARCHAR(20), -- 'large', 'mid', 'small'
    currency VARCHAR(3) DEFAULT 'USD',
    exchange VARCHAR(10) DEFAULT 'NASDAQ',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample stock symbols
-- This provides us with reference data for our pipeline
INSERT INTO stock_symbols (symbol, company_name, sector, industry, market_cap_category) VALUES
    ('AAPL',  'Apple Inc.',            'Technology',             'Consumer Electronics', 'large'),
    ('GOOGL', 'Alphabet Inc.',         'Technology',             'Internet Services',    'large'),
    ('MSFT',  'Microsoft Corporation', 'Technology',             'Software',             'large'),
    ('AMZN',  'Amazon.com Inc.',       'Consumer Discretionary', 'E-commerce',           'large'),
    ('TSLA',  'Tesla Inc.',            'Consumer Discretionary', 'Electric Vehicles',    'large'),
    ('META',  'Meta Platforms Inc.',   'Technology',             'Social Media',         'large'),
    ('NVDA',  'NVIDIA Corporation',    'Technology',             'Semiconductors',       'large'),
    ('NFLX',  'Netflix Inc.',          'Communication Services', 'Streaming',            'large');

-- ===================================================
-- Raw Stock Price Data Table
-- ===================================================

-- Daily stock prices table (raw data from API)
-- This table stores the fundamental OHLCV (open-high-low-close value) data
CREATE TABLE daily_stock_prices (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    trade_date DATE NOT NULL,

    -- OHLCV data - using DECIMAL for precision in financial calculations
    open_price     DECIMAL(10,2) NOT NULL,
    high_price     DECIMAL(10,2) NOT NULL,
    low_price      DECIMAL(10,2) NOT NULL,
    close_price    DECIMAL(10,2) NOT NULL,
    volume BIGINT NOT NULL,

    -- Data quality and lineage fields
    data_source VARCHAR(50) DEFAULT 'alpha_vantage',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Ensure no duplicate records for same symbol/date
    UNIQUE(symbol, trade_date),

    -- Foreign key to stock symbols
    FOREIGN KEY (symbol) REFERENCES stock_symbols(symbol)
);

-- =====================================================
-- Processed Technical Indicators Table
-- =====================================================

-- Stock technical indicators (processed data)
-- This table stores calculated metrics and moving averages
CREATE TABLE stock_indicators (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    trade_date DATE NOT NULL,

    -- Price-based indicators
    daily_return DECIMAL(8,4), -- Daily percentage return 
    log_return DECIMAL(8,4), -- Log return for statistical analysis

    -- Moving averages (various windows)
    sma_5   DECIMAL(10,2), -- 5-day   Simple Moving Average 
    sma_10  DECIMAL(10,2), -- 10-day  Simple Moving Average 
    sma_20  DECIMAL(10,2), -- 20-day  Simple Moving Average 
    sma_50  DECIMAL(10,2), -- 50-day  Simple Moving Average 
    sma_100 DECIMAL(10,2), -- 200-day Simple Moving Average 
    sma_200 DECIMAL(10,2),

    -- Volatility measures
    volatility_30d DECIMAL(8,4), -- 30-day rolling volatility

    -- Volume indicators
    volume_sma_20 BIGINT,      -- 20-day average volume
    volume_ratio DECIMAL(6,2), -- Current volume vs average

    -- Price position indicators
    price_vs_sma20_pct DECIMAL(6,2), -- % above/below 20-day MA
    price_vs_sma50_pct DECIMAL(6,2), -- % above/below 50-day MA

    -- Trend indicators
    is_above_sma20 BOOLEAN,     -- Price above 20-day MA
    is_above_sma50 BOOLEAN,     -- Price above 50-day MA
    is_above_sma200 BOOLEAN,    -- Price above 200-day MA
    trend_strength VARCHAR(20), -- 'strong_up', 'up', 'sideways', 'down', 'strong_down'

    -- Processing metadata
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Ensure no duplicates
    UNIQUE(symbol, trade_date),

    -- Foreign Key relationships
    FOREIGN KEY (symbol) REFERENCES stock_symbols(symbol),
    FOREIGN KEY (symbol, trade_date) REFERENCES daily_stock_prices(symbol, trade_date)
);

-- =====================================================
-- Pipeline Execution Tracking
-- =====================================================

-- Pipeline runs table for monitoring and debugging
-- Tracks each execution of the ETL pipeline
CREATE TABLE pipeline_runs (
    id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(100) UNIQUE NOT NULL, -- Airflow run_id or custom identifier

    -- Execution details
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) DEFAULT 'running', -- 'running', 'success', 'failed', 'partial'

    -- Processing statistics
    symbols_processed INTEGER DEFAULT 0,
    records_extracted INTEGER DEFAULT 0,
    records_transformed INTEGER DEFAULT 0,
    records_loaded INTEGER DEFAULT 0,

    -- Error handling
    error_message TEXT,
    error_details JSONB, -- Store structured error information

    -- Processing metadata
    pipeline_version VARCHAR(20),
    config_used JSONB, -- Store congfiguration used for this run

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- Performance Indexes
-- =====================================================

-- Indexes for optimal query performance
-- These indexes are designed for common query patterns in financial data

-- Time-series queries (most common pattern)
CREATE INDEX idx_daily_prices_symbol_date ON daily_stock_prices(symbol, trade_date DESC);
CREATE INDEX idx_indicators_symbol_date ON stock_indicators(symbol, trade_date DESC);

-- Date range queries
CREATE INDEX idx_daily_prices_date ON daily_stock_prices(trade_date DESC);
CREATE INDEX idx_indicators_date ON stock_indicators(trade_date DESC);

-- Symbol-based queries
CREATE INDEX idx_daily_price_symbol ON daily_stock_prices(symbol);
CREATE INDEX idx_indicators_symbol ON stock_indicators(symbol);

-- Volume-based analysis
CREATE INDEX idx_daily_prices_volume ON daily_stock_prices(volume DESC);

-- Pipeline monitoring
CREATE INDEX idx_pipeline_runs_status ON pipeline_runs(status, start_time DESC);
CREATE INDEX idx_pipeline_runs_date ON pipeline_runs(start_time DESC);

-- =====================================================
-- Database Functions and Views
-- =====================================================

-- View for latest stock prices with indicators
-- This materialized view provides quick access to current stock status
CREATE VIEW latest_stock_data AS
SELECT
    p.symbol,
    s.company_name,
    p.trade_date,
    p.close_price,
    p.volume,
    i.daily_return,
    i.sma_20,
    i.sma_50,
    i.volatility_30d,
    i.is_above_sma20,
    i.is_above_sma50,
    i.trend_strength
FROM daily_stock_prices p
JOIN stock_symbols s ON p.symbol = s.symbol
LEFT JOIN stock_indicators i ON p.symbol = i.symbol AND p.trade_date = i.trade_date
WHERE p.trade_date = (
    SELECT MAX(trade_date)
    FROM daily_stock_prices p2
    WHERE p2.symbol = p.symbol
);

-- Function to calculate porfolio performance
-- Demonstrates advanced SQL for financial calculations
CREATE OR REPLACE FUNCTION calculate_portfolio_performance(
    start_date DATE,
    end_date DATE,
    symbols TEXT[] DEFAULT ARRAY['AAPL', 'GOOGL', 'MSFT']
) RETURNS TABLE(
    symbol VARCHAR(10),
    start_price DECIMAL(10,2),
    end_price DECIMAL(10,2),
    total_return DECIMAL(8,4),
    avg_daily_return DECIMAL(8,4),
    volatility DECIMAL(8,4)
) AS $$ 
BEGIN
    RETURN QUERY
    WITH price_data AS (
        SELECT
            p.symbol,
            p.trade_date,
            p.close_price,
            LAG(p.close_price) OVER (PARTITION BY p.symbol ORDER BY p.trade_date) as prev_price
        FROM daily_stock_prices p
        WHERE p.symbol = ANY(symbols)
          AND p.trade_date BETWEEN start_date AND end_date
    ),
    returns_data AS (
        SELECT
            symbol,
            trade_date,
            close_price,
            CASE
                WHEN prev_price IS NOT NULL THEN
                    (close_price - prev_price) / prev_price
                ELSE 0
            END as daily_return
        FROM price_data
    )
    SELECT
        r.symbol,
        (SELECT close_price FROM returns_data r2
         WHERE r2.symbol = r.symbol AND r2.trade_date = start_date),
        (SELECT close_price FROM returns_data r2
         WHERE r2.symbol = r.symbol
         ORDER BY r2.trade_date DESC LIMIT 1),
        ((SELECT close_price FROM returns_data r2
          WHERE r2.symbol = r.symbol
          ORDER BY r2.trade_date DESC LIMIT 1) - 
         (SELECT close_price FROM returns_data r2
          WHERE r2.symbol = r.symbol AND r2.trade_date = start_date)) / 
         (SELECT close_price FROM returns_data r2
          WHERE r2.symbol = r.symbol AND r2.trade_date = start_date),
        AVG(r.daily_return),
        STDDEV(r.daily_return)
    FROM returns_data r
    WHERE r.daily_return != 0
    GROUP BY r.symbol;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Data Constraints and Triggers
-- =====================================================

-- Trigger to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

-- Apply update triggers to relevant tables
CREATE TRIGGER update_stock_symbols_modtime 
    BEFORE UPDATE ON stock_symbols 
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_daily_prices_modtime 
    BEFORE UPDATE ON daily_stock_prices 
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- Add check constraints for data quality
ALTER TABLE daily_stock_prices
ADD CONSTRAINT chk_positive_prices
CHECK (open_price > 0 AND high_price > 0 AND low_price > 0 AND close_price > 0),
ADD CONSTRAINT chk_high_low_range
CHECK (high_price >= low_price),
ADD CONSTRAINT chk_ohlc_range
CHECK (open_price BETWEEN low_price AND high_price
       AND close_price BETWEEN low_price AND high_price),
ADD CONSTRAINT chk_positive_volume
CHECK (volume >= 0);

-- Comments for documentation
COMMENT ON TABLE stock_symbols IS 'Master table for stock symbol metadata';
COMMENT ON TABLE daily_stock_prices IS 'Raw daily OHLCV stock price data from external APIs';
COMMENT ON TABLE stock_indicators IS 'Calculated technical indicators and metrics';
COMMENT ON TABLE pipeline_runs IS 'ETL pipeline execution tracking and monitoring';

COMMENT ON COLUMN stock_indicators.volatility_30d IS '30-day rolling standard deviation of returns';
COMMENT ON COLUMN stock_indicators.trend_strength IS 'Qualitative trend assessment based on moving agerages';

