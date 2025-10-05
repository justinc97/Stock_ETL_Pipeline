"""
Stock Data Extraction Module
============================

This module handes data extraction from the Alpha Vantage API, demonstrating: 

1. API integration patterns and error handling
2. Rate limiting and retry logic
3. Data validation and cleaning
4. JSON/CSV data processing with pandas
5. Logging and monitoring best practices

The module is designed to be robust and production-ready, handling various edge cases that occur when working with external financial data APIs.
"""

import os, time, json, logging, requests, sys
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path

# Add project root to Python path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import Settings

# Configure logging for this module
logging.basicConfig(
    level = getattr(logging, Settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers = [
        logging.FileHandler(os.path.join(Settings.paths.LOGS_DIR, 'fetch_stock_data.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class StockDataFetcher:
    """
    A robust stock data fetcher that handles API interactions with Alpha Vantage.

    This class encapsulates all the complexity of working with external APIs:
    - Authentication and API key management
    - Rate limiting compliance
    - Error handling and retreis
    - Data validation and format conversion

    Example usage:
        fetcher = StockDataFetcher()
        data    = fetcher.fetch_daily_data(['AAPL', 'GOOGL'])
    """

    def __init__(self, api_key: str = None):
        """
        Initialize the stock data fetcher.

        Args:
            api_key: ALpha Vantage API key. If None, uses config default.
        """
        self.api_key = api_key or Settings.api.ALPHA_VANTAGE_API_KEY
        self.base_url = Settings.api.ALPHA_VANTAGE_BASE_URL
        self.session = requests.Session() # Reuse connections for efficiency

        # Rate limiting state
        self.last_request_time = 0
        self.requests_made = 0
        self.request_times = []

        logger.info(f"StockDataFetcher initialized with base URL: {self.base_url}")

    def _respect_rate_limit(self) -> None:
        """
        Implement rate limiting to comply with API restrictions.

        Alpha Vantage free tier allows 5 requests per minute.
        This method ensures we don't exceed that limit.
        """
        current_time = time.time()

        # Remove request times older than 1 minute
        one_minute_ago = current_time - 60
        self.request_times = [t for t in self.request_times if t > one_minute_ago]

        # If we've made too many requests in the last minute, wait
        if len(self.request_times) >= Settings.api.REQUESTS_PER_MINUTE:
            sleep_time = 60 - (current_time - self.request_times[0])
            if sleep_time > 0:
                logger.info(f"Rate limit reached. Waiting {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)
        
        # Record this request time
        self.request_times.append(current_time)
        self.last_request_time = current_time

    def _make_api_request(self, params: Dict[str,str]) -> Dict:
        """
        Make a single API request with error handling and retries

        Args:
            params: Dictionary of API parameters

        Returns:
            JSON response as a dictionary
        
        Raises: 
            requests.RequestException: if all retry attempts fail
        """
        # Add API key to parameters
        params['apikey'] = self.api_key

        for attempt in range(Settings.api.RETRY_ATTEMPTS):
            try:
                # Respect rate limiting
                self._respect_rate_limit()

                logger.debug(f"Making API request (attempt {attempt + 1}): {params}")

                # Make the request
                response = self.session.get(self.base_url, params = params, timeout = 30)
                response.raise_for_status() # Raise an error for bad status codes

                # Parse JSON response
                data = response.json()

                # Check for API -specific error messages
                if 'Error Message' in data:
                    raise ValueError(f"API Error: {data['Error Message']}")
                
                if 'Note' in data:
                    logger.warning(f"API Note: {data['None']}")
                    # This usually means rate limit hit, wait and retry
                    time.sleep(Settings.api.RETRY_DELAY)
                    continue

                logger.debug("API request successful")
                return data
            except (requests.RequestException, ValueError, KeyError) as e:
                logger.warning(f"API request failed (attempt {attempt + 1}): {str(e)}")

                if attempt < Settings.api.RETRY_ATTEMPTS - 1:
                    wait_time = Settings.api.RETRY_DELAY * (2**attempt) # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All retry attempts failed for params: {params}")
                    raise
        # This should never be reached, but just in case
        raise requests.RequestException('Unexpected error in API request')
    
    def fetch_daily_data(self, symbol: str, outputsize: str = 'compact') -> pd.DataFrame:
        """
        Fetch daily stock data for a single symbol.
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            outputsize: 'compact' (100 days) or 'full' (all available)
        
        Returns:
            pandas DataFrame with columns: date, open, high, low, close, volume    
        """
        logger.info(f"Fetching daily data for {symbol}")

        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'outputsize': outputsize
        }

        try:
            data = self._make_api_request(params)

            # Extract the time series data
            time_series_key = 'Time Series (Daily)'
            if time_series_key not in data:
                raise KeyError(f"Expected key '{time_series_key} not found in API response")
            
            time_series = data[time_series_key]

            # Convert to pandas DataFrame
            df = pd.DataFrame.from_dict(time_series, orient = 'index')

            # Clean column names and data types
            df = self._clean_daily_data(df, symbol)

            logger.info(f"Successfully fetched {len(df)} days of data for {symbol}")
            return df
        
        except Exception as e:
            logger.error(f"Failed to fetch data for {symbol}: {str(e)}")
            # Return empty DataFrame with correct columns for consistency
            return pd.DataFrame(columns = ['symbol','data','open','high','low','close','volume'])
        
    def _clean_daily_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Clean and standardize the raw API data.

        This method demonstrates important data cleaning patterns:
        1. Column renaming and standardization
        2. Data type conversion
        3. Index handling
        4. Data validation

        Args:
            df: Raw DataFrame from API
            symbol: Stock symbol for metadata

        Returns:
            Cleaned DataFrame
        """

        # Rename columns to remove API previxes
        column_mapping = {
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close',
            '5. volume': 'volume'
        }
        df = df.rename(columns=column_mapping)

        # Convert index (dates) to datetime
        df.index = pd.to_datetime(df.index)
        df.index.name = 'date'

        # Reset index to make date a column
        df = df.reset_index()

        # Add symbol column
        df['symbol'] = symbol

        # Convert price columns to numeric
        price_columns = ['open', 'high', 'low', 'close']
        for col in price_columns:
            df[col] = pd.to_numeric(df[col], errors = 'coerce')
        
        # Convert volume to integer
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype(int)

        # Sort by date (oldest first)
        df = df.sort_values('date').reset_index(drop=True)

        # Data validation
        self._validate_price_data(df,symbol)

        # Reorder columns for consistency
        df = df[['symbol','date','open','high','low','close','volume']]

        return df
    
    def _validate_price_data(self, df: pd.DataFrame, symbol: str) -> None:
        """
        Validate the cleaned price data for common issues.

        Args:
            df: DataFrame to validate
            symbol: Stock symbol for error messages
        """
        if df.empty:
            logger.warning(f"No data available for {symbol}")
            return

        # Check for missing values in critical columns
        critical_columns = ['open','high','low','close']
        missing_data = df[critical_columns].isnull().sum()
        if missing_data.any():
            logger.warning(f"Missing data in {symbol}: {missing_data[missing_data>0].to_dict()}")

        # Check for impossible price relationships
        invalid_ohlc = df[
            (df['high'] < df['low']) | 
            (df['open'] < df['low']) | 
            (df['open'] > df['high']) | 
            (df['close'] < df['low']) | 
            (df['close'] > df['high'])
        ]

        if not invalid_ohlc.empty:
            logger.warning(f"Invalid OHLC relationships found for {symbol} on dates: {invalid_ohlc['date'].tolist()}")

        # Check for negative prices
        negative_prices = df[(df[critical_columns] <= 0).any(axis=1)]
        if not negative_prices.empty:
            logger.warning(f"Negative or zero prices found for {symbol} on dates: {negative_prices['date'].tolist()}")
        
        logger.debug(f"Data validation completed for {symbol}")

    def fetch_multiple_symbols(self, symbols: List[str], save_individual: bool = True) -> pd.DataFrame:
        """
        Fetch data for multiple stock symbols.

        This method demonstrates batch processing patterns and error handling
        when working with multiple data sources.

        Args:
            symbols: List of stock symbols
            save_individual: Whether to save each symbol's data separately

        Returns:
            Combined DataFrame with all symbols' data
        """
        logger.info(f"Fetching data for {len(symbols)} symbols: {symbols}")

        all_data = []
        failed_symbols = []

        for i, symbol in enumerate(symbols):
            logger.info(f"Processing {symbol} ({i+1}/{len(symbols)})")

            try:
                df = self.fetch_daily_data(symbol)

                if not df.empty:
                    all_data.append(df)

                    # Save individual symbol data if requested
                    if save_individual: 
                        self._save_symbol_data(df,symbol)
                else:
                    failed_symbols.append(symbol)
            
            except Exception as e:
                logger.error(f"Failed to process {symbol}: {str(e)}")
                failed_symbols.append(symbol)

        if failed_symbols:
            logger.warning(f"Failed to fetch data for symbols: {failed_symbols}")

        # Combine all data
        if all_data:
            combined_df = pd.concat(all_data, ignore_index = True)
            logger.info(f"Successfully fetched data for {len(all_data)} symbols, total records: {len(combined_df)}")

        else:
            logger.warning("No data was sucessfully fetched")
            combined_df = pd.DataFrame()
        
        return combined_df
    
    def _save_symbol_data(self, df: pd.DataFrame, symbol: str) -> None:
        """
        Save individual symbol data to files.

        Args:
            df: DataFrame to save
            symbol: Stock symbol
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Save as CSV
        csv_filename = f"{symbol}_daily_{timestamp}.csv"
        csv_path = os.path.join(Settings.paths.RAW_DATA_DIR, csv_filename)
        df.to_csv(csv_path, index = False)

        # Save as JSON for backup
        json_filename = f"{symbol}_daily_{timestamp}.json"
        json_path = os.path.join(Settings.paths.RAW_DATA_DIR, json_filename)
        df.to_json(json_path, orient = 'records', date_format = 'iso', indent = 2)

        logger.debug(f"Saved {symbol} data to {csv_path} and {json_path}")

    def save_combined_data(self, df: pd.DataFrame, filename_prefix: str = 'stock_data') -> str:
        """
        Save combined stock data with timestamp.

        Args:
            df: DataFrame to save
            filename_prefix: Prefix for the filename

        Returns:
            Path to saved file
        """ 
        if df.empty:
            logger.warning("No data to save")
            return ""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{filename_prefix}_{timestamp}.csv"
        filepath = os.path.join(Settings.paths.RAW_DATA_DIR, filename)

        # Save with metadata header
        with open(filepath, 'w') as f:
            f.write(f"# Stock data extracted at {datetime.now().isoformat()}\n")
            f.write(f"# Symbols: {sorted(df['symbol'].unique().tolist())}\n")
            f.write(f"# Records: {len(df)}\n")
            f.write(f"# Date range: {df['date'].min()} to {df['date'].max()}\n")
            f.write("# Columns: symbol, date, open, high, low, close, volume\n")

        # Append the actual data
        df.to_csv(filepath, mode = 'a', index = False)

        logger.info(f"Combined data saved to {filepath}")
        return filepath
    
def main():
    """
    Main function demonstrating the StockDataFetcher usage.

    This function shows how to use the fetcher in different scenarios:
    1. Single symbol fetch
    2. Multiple symbol batch fetch
    3. Error handling and data validation
    """
    logger.info("Starting stock data extraction process")

    fetcher = StockDataFetcher()

    # Get symbols from configuration
    symbols = Settings.stock.DEFAULT_SYMBOLS
    logger.info(f"Will fetch data for symbols: {symbols}")

    try:
        # Fetch data for all symbols
        combined_data = fetcher.fetch_multiple_symbols(symbols, save_individual=True)

        if not combined_data.empty:
            # Save combined data
            output_file = fetcher.save_combined_data(combined_data)

            # Display summary statistics
            print("\n" + "="*60)
            print("STOCK DATA EXCTRACTION SUMMARY")
            print('='*60)
            print(f"Total records fetched: {len(combined_data):,}")
            print(f"Symbols processed: {combined_data['symbol'].nunique()}")
            print(f"Date range: {combined_data['date'].min()} to {combined_data['date'].max()}")
            print(f"Output file: {output_file}")

            # Show sample data
            print("\nSample Data:")
            print(combined_data.head(10).to_string(index = False))

            # Show data quality summary
            print("\nData quality summary:")
            for symbol in sorted(combined_data['symbol'].unique()):
                symbol_data = combined_data[combined_data['symbol'] == symbol]
                print(f"  {symbol}: {len(symbol_data)} records, "
                      f"latest: {symbol_data['date'].max()}")
        else:
            logger.error("No data was successfully exctracted")
            return 1
    
    except Exception as e:
        logger.error(f"Extraction process failed: {str(e)}")
        return 1
    
    logger.info("Stock data extraction completed successfully")
    return 0

if __name__ == "__main__":
    exit(main())

