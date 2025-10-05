"""
Database Loading Module
=======================

This module handles loading processed stock data into PostgreSQL database.
It demonstrates best practices for:

1. Database connection management with SQLAlchemy
2. Bulk data loading strategies
3. Data integrity and constraint handling
4. Error handling and transaction management
5. Performance optimization for time-series data
6. Upsert patterns for incremental updates

Key learning objectives:
- SQLAlchemy ORM and Core usage
- PostgreSQL-specific optimizations
- Batch processing for large datasets
- Data validation before database insertion
- Connection pooling and resource management
"""

import os, sys, logging, pandas as pd, numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any

from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Date,
    Numeric, Integer, Boolean, DateTime, text, func, select, and_
)
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.pool import StaticPool

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import Settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, Settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseLoader:
    """
    Database loader for stock market data with PostgreSQL optimizations.

    This class provides comprehensive database operations for te stock ETL pipeline:
    - Connection management with proper error handling
    - Bulk insert operations with conflict resolution
    - Data validation and type conversaion
    - Performance monitoring and optimization
    - Transaction management for data consistency

    The impleentation uses SQLAlchemy for database abstraction while leveraging
    PostgreSQL-specific features for optimal performance.
    """

    def __init__(self, connection_string: str = None):
        """
        Initialize database loader with connection configuration.

        Args:
            connection_string: Database connection string. If None, uses config.
        """
        self.connection_string = connection_string or Settings.db.get_connection_string()
        self.engine = None
        self.SessionLocal = None
        self.metadata = None

        # Table references
        self.stock_symbols_table = None
        self.daily_prices_table = None
        self.indicators_table = None
        self.pipeline_runs_table = None

        self._initialize_database_connection()
        self._initialize_table_definitions()

    def _initialize_database_connection(self) -> None:
        """
        Initialize database engine and session factory.

        This method sets up the database connection with optimized settings:
        - Connection pooling for concurrent access
        - Proper timeout and retry configurations
        - PostgreSQL-specific optimizations
        """
        logger.info("Initializing database connection...")

        try:
            # Create engine with connection pooling
            print(self.connection_string)
            self.engine = create_engine(
                self.connection_string,
                pool_size = 10,          # Number of connections to maintain 
                max_overflow = 20,       # Additional connections under load
                pool_timeout = 30,       # Seconds to wait for connection
                pool_recycle = 3600,     # Recycle connections after 1 hour
                echo = Settings.LOG_LEVEL == "DEBUG" # Log SQL statements in debug mode
            )
        
            # Test connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                logger.info(f"Connected to PostgreSQL: {version[:50]}...")

            # Create session factory
            self.SessionLocal = sessionmaker(bind = self.engine)

            logger.info("Database connection initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database connection: {str(e)}")
            raise

    def _initialize_table_definitions(self) -> None:
        """
        Initialize SQLAlchemy table definitions matching our schema.

        This method creates table objects that match the SQL schema,
        allowing for type-safe database operations and better performance.
        """
        logger.info("Initializing table definitions...")

        self.metadata = MetaData()

        # Stock symbols table
        self.stock_symbols_table = Table(
            'stock_symbols', self.metadata,
            Column('symbol', String(10), primary_key = True),
            Column('company_name', String(255), nullable=False),
            Column('sector', String(1000)),
            Column('industry', String(100)),
            Column('market_cap_category', String(20)),
            Column('currency', String(3)),
            Column('exchange', String(10)),
            Column('is_active', Boolean),
            Column('created_at', DateTime),
            Column('updated_at', DateTime)
        )

        # Daily stock prices table
        self.daily_prices_table = Table(
            'daily_stock_prices', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('symbol', String(10), nullable=False),
            Column('trade_date', Date, nullable=False),
            Column('open_price', Numeric(10, 2), nullable=False),
            Column('high_price', Numeric(10, 2), nullable=False),
            Column('low_price', Numeric(10, 2), nullable=False),
            Column('close_price', Numeric(10, 2), nullable=False),
            Column('volume', Integer, nullable=False),
            Column('data_source', String(50)),
            Column('created_at', DateTime),
            Column('updated_at', DateTime)
        )

        # Stock indicators table
        self.indicators_table = Table(
            'stock_indicators', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('symbol', String(10), nullable=False),
            Column('trade_date', Date, nullable=False),
            Column('daily_return', Numeric(8, 4)),
            Column('log_return', Numeric(8, 4)),
            Column('sma_5', Numeric(10, 2)),
            Column('sma_10', Numeric(10, 2)),
            Column('sma_20', Numeric(10, 2)),
            Column('sma_50', Numeric(10, 2)),
            Column('sma_200', Numeric(10, 2)),
            Column('volatility_30d', Numeric(8, 4)),
            Column('volume_sma_20', Integer),
            Column('volume_ratio', Numeric(6, 2)),
            Column('price_vs_sma20_pct', Numeric(6, 2)),
            Column('price_vs_sma50_pct', Numeric(6, 2)),
            Column('is_above_sma20', Boolean),
            Column('is_above_sma50', Boolean),
            Column('is_above_sma200', Boolean),
            Column('trend_strength', String(10)),
            Column('calculated_at', DateTime)
        )

        # Pipeline runs table
        self.pipeline_runs_table = Table(
            'pipeline_runs', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('run_id', String(100), unique=True, nullable=False),
            Column('start_time', DateTime, nullable=False),
            Column('end_time', DateTime),
            Column('status', String(20)),
            Column('symbols_processed', Integer),
            Column('records_extracted', Integer),
            Column('records_transformed', Integer),
            Column('records_loaded', Integer),
            Column('error_message', String),
            Column('pipeline_version', String(20)),
            Column('created_at', DateTime)
        )
        
        logger.info("Table definitions initialized")

    def validate_data_before_load(self, df: pd.DataFrame, table_type: str) -> pd.DataFrame:
        """
        Validate and clean data before database insertion.

        This method performs comprehensive data validation and cleaning:
        - Data type validation and conversion
        - Constraint validation (e.g., non-null, ranges)
        - Data sanitization
        - Duplicate detection and handling

        Args:
            df: DataFrame to validate
            table_type: Type of table ('daily_prices' or 'indicators')

        Returns:
            Cleaned and validated DataFrame
        """
        logger.info(f"Validating {table_type} data before database load...")

        if df.empty:
            logger.warning("Empty DataFrame provided for validation")
            return df
        
        original_count = len(df)

        # Common validation for both table types
        if 'symbol' in df.columns:
            # Remove rows with missing symbols
            df = df.dropna(subset=['symbol'])
            # Clean symbol column (uppercase, strip whitespace)
            df['symbol'] = df['symbol'].astype(str).str.strip().str.upper()
        
        if 'trade_date' in df.columns:
            # Ensure trade_date is datetime
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            # Remove future dates(data quality check)
            df = df[df['trade_date'] <= datetime.now()]

        # Table-specific validation
        if table_type == 'daily_prices':
            df = self._validate_daily_prices_data(df)
        elif table_type == 'indicators':
            df = self._validate_indicators_data(df)

        # Remove exact duplicates
        before_dedup = len(df)
        if table_type == 'daily_prices':
            df = df.drop_duplicates(subset=['symbol','trade_date'])
        elif table_type == 'indicators':
            df = df.drop_duplicates(subset=['symbol','trade_date'])
        
        duplicates_removed = before_dedup - len(df)
        if duplicates_removed > 0:
            logger.info(f"Removed {duplicates_removed} duplicate records")

        records_removed = original_count - len(df)
        if records_removed > 0:
            logger.info(f"Validation removed {records_removed} invalid records")
        
        logger.info(f"Data validation completed. {len(df)} records ready for load")
        return df
    
    def _validate_daily_prices_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate daily prices data specifically.

        Args:
            df: Daily prices DataFrame

        Returns:
            Validated DataFrame
        """
        # Required columns for daily prices
        required_columns = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
        missing_columns  = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Remove rows with missing critical price data
        df = df.dropna(subset=required_columns)

        # Validate price constraints
        price_columns = ['open_price', 'high_price', 'low_price', 'close_price']

        # Remove rows with non-positive prices
        for col in price_columns:
            df = df[df[col]>0]

        # Validate OHLC relationships
        df = df[
            (df['high_price'] >= df['low_price']) &
            (df['open_price'] >= df['low_price']) & (df['open_price'] <= df['high_price']) & 
            (df['close_price'] >= df['low_price']) & (df['close_price'] <= df['high_price'])
        ]

        # Validate volume (non-negative)
        df = df[df['volume'] >= 0]

        # Convert to appropriate data types
        for col in price_columns: 
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors = 'coerce')
        
        df['volume'] = pd.to_numeric(df['volume'], errors = 'coerce').fillna(0).astype(int)

        return df
    
    def _validate_indicators_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate indicators data specifically.

        Args:
            df: Indicators DataFrame
        
        Returns:
            Validated DataFrame
        """
        # Convert numeric columns
        numeric_columns = [
            'daily_return', 'log_return', 'sma_5', 'sma_10', 'sma_20', 'sma_50', 'sma_200',
            'volatility_30d', 'volume_ratio', 'price_vs_sma20_pct', 'price_vs_sma50_pct'
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors = 'coerce')
        
        # Convert integer columns
        integer_columns = ['volume_sma_20']
        for col in integer_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors = 'coerce').fillna(0).astype(int)
        
        # Convert boolean columns
        boolean_columns = ['is_above_sma20', 'is_above_sma50', 'is_above_sma200']
        for col in boolean_columns:
            if col in df.columns:
                df[col] = df[col].astype(bool)

        # Validate return values (should be reasonable)
        if 'daily_return' in df.columns:
            # Remove extreme returns (likely data errors)
            df = df[abs(df['daily_return']) <= 1.0] # Maxx 100% daily change

        return df
    
    def upsert_daily_prices(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Insert or update daily price records using PostgreSQL UPSERT.

        This method demonstrates efficient bulk loading with conflict resolution:
        - Uses PostgreSQL's ON CONFLICT for upserts
        - Batch processing for large datasets
        - Transaction management for data consistency

        Args:
            df: DataFrame with daily price data

        Returns:
            Dictionary with operation statistics
        """
        logger.info(f"Upserting {len(df)} daily price records...")

        if df.empty:
            return {"inserted": 0, "updated": 0, "errors": 0}
        
        # Validate data
        df = self.validate_data_before_load(df, 'daily_prices')

        stats = {"inserted": 0, "updated": 0, "errors": 0}

        with self.engine.begin() as conn:
            try:
                # Convert DataFrame to list of dictionaries
                records = df.to_dict('records')

                # Use PostgreSQL-specific upsert
                stmt = insert(self.daily_prices_table).values(records)
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements=['symbol', 'trade_date'],
                    set_ = {
                        'open_price': stmt.excluded.open_price,
                        'high_price': stmt.excluded.high_price,
                        'low_price':  stmt.excluded.low_price,
                        'close_price':stmt.excluded.close_price,
                        'volume':     stmt.excluded.volume,
                        'updated_at': func.now()
                    }
                )

                result = conn.execute(upsert_stmt)
                stats["inserted"] = result.rowcount

                logger.info(f"Successfully upserted {stats['inserted']} daily price records")

            except Exception as e:
                logger.error(f"Error upserting daily prices: {str(e)}")
                stats["errors"] = len(df)
                raise
        return stats
    
    def upsert_indicators(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Insert or update stock indicator records.

        Args:
            df: DataFrame with indicators data

        Returns:
            Dictionary with operation statistics
        """
        logger.info(f"Upserting {len(df)} indicator records...")

        if df.empty:
            return {"inserted": 0, "updated": 0, "errors": 0}
        
        # Validate data
        df = self.validate_data_before_load(df, "indicators")

        stats = {"inserted": 0, "updated": 0, "errors": 0}

        with self.engine.begin() as conn:
            try:
                records = df.to_dict('records')

                # PostgreSQL upsert for indicators
                stmt = insert(self.indicators_table).values(records)
                upsert_stmt = stmt.on_conflict_do_update(
                    index_elements = ["symbol", "trade_date"],
                    set_ = {col.name: getattr(stmt.excluded, col.name)
                           for col in self.indicators_table.columns
                           if col.name not in ["id", "symbol", "trade_date"]}
                )

                result = conn.execute(upsert_stmt)
                stats["inserted"] = result.rowcount

                logger.info(f"Successfully upserted {stats["inserted"]} indicator records")

            except Exception as e:
                logger.error(f"Error upserting indicators: {str(e)}")
                stats["errors"] = len(df)
                raise
        
        return stats
    
    def load_parquet_files(self, daily_prices_path: str, indicators_path: str) -> Dict[str, Any]:
        """
        Load processed data from Parquet files into database.

        This method demonstrates loading Spark-generated Parquet files:
        - Handling partitioned Parquet directories
        - Batch processing for memory efficiency
        - Progress tracking and error handling

        Args:
            daily_prices_path: Path to daily prices Parquet files
            indicators_path: Path to indicators Parquet files

        Returns:
            Dictionary with loading statistics
        """
        logger.info("Loading data from Parquet files...")

        total_stats = {
            "daily_prices": {"inserted": 0, "updated": 0, "errors": 0},
            "indicators": {"inserted": 0, "updated": 0, "errors": 0},
            "start_time": datetime.now(),
            "symbols_processed": set()
        }

        try:
            # Load daily prices
            if os.path.exists(daily_prices_path):
                logger.info(f"Loading daily prices from {daily_prices_path}")
                daily_prices_df = pd.read_parquet(daily_prices_path)

                if not daily_prices_df.empty:
                    # Track symbols
                    total_stats["symbols_processed"].update(daily_prices_df['symbol'].unique())

                    # Load in batches for memory efficiency
                    batch_size = 10000
                    for i in range(0, len(daily_prices_df), batch_size):
                        batch = daily_prices_df.iloc[i:i+batch_size]
                        batch_stats = self.upsert_daily_prices(batch)

                        total_stats["daily_prices"]["inserted"] += batch_stats["inserted"]
                        total_stats["daily_prices"]["errors"]   += batch_stats["errors"]

                        logger.info(f"Loaded daily prices batch {i//batch_size + 1}, "
                                    f"records: {len(batch)}")
                
                else: 
                    logger.warning("No daily prices data found")
            else:
                logger.warning(f"Daily prices path not found: {daily_prices_path}")
            

            # Load indicators
            if os.path.exists(indicators_path):
                logger.info(f"Loading indicators from {indicators_path}")
                indicators_df = pd.read_parquet(indicators_path)

                if not indicators_df.empty:
                    # Track symbols
                    total_stats["symbols_processed"].update(indicators_df['symbol'].unique())

                    # load in batches
                    batch_size = 10000
                    for i in range(0, len(indicators_df), batch_size):
                        batch = indicators_df.iloc[i:i+batch_size]
                        batch_stats = self.upsert_indicators(batch)

                        total_stats["indicators"]["inserted"] += batch_stats["inserted"]
                        total_stats["indicators"]["errors"]   += batch_stats["errors"]

                        logger.info(f"Loaded indicators batch {i//batch_size + 1}, "
                                    f"records: {len(batch)}")
                        
                else:
                    logger.warning("No indicators data found")
            else:
                logger.warning(f"Indicators path not found: {indicators_path}")


            # Finalize statistics
            total_stats["end_time"] = datetime.now()
            total_stats["duration"] = total_stats["end_time"] - total_stats["start_time"]
            total_stats["symbols_processed"] = list(total_stats["symbols_processed"])

            logger.info("Data loading completed successfully")
            return total_stats
        
        except Exception as e:
            logger.error(f"Error loading Parquet files: {str(e)}")
            total_stats["error"] = str(e)
            raise

    def create_pipeline_run_record(self, run_id: str, start_time: datetime,
                                   status: str = 'running', **kwargs) -> int:
        """
        Create a pipeline run record for tracking ETL execution.

        Args:
            run_id: Unique identifier for the pipeline run
            start_time: Pipeline start timestamp
            status: Pipeline status ('running', 'success', 'failed', 'partial')
            **kwargs: Additional fields (symbols_processed, records_exctracted, etc.)
        
        Returns:
            Pipeline run record ID
        """
        logger.info(f"Creating pipeline run record: {run_id}")

        record = {
            'run_id': run_id,
            'start_time': start_time,
            'status': status,
            'created_at': datetime.now(),
            **kwargs
        }

        with self.engine.begin() as conn:
            stmt = insert(self.pipeline_runs_table).values(record)
            result = conn.execute(stmt)

            # Get the inserted record ID
            record_id = result.lastrowid or result.rowcount

        logger.info(f"Pipeline run record created with ID: {record_id}")
        return record_id
    
    def update_pipeline_run_record(self, run_id: str, **kwargs) -> None:
        """
        Update an existing pipeline run record.

        Args:
            run_id: Pipeline run identifier
            **kwargs: Fields to update
        """
        logger.info(f"Updating pipeline run record: {run_id}")

        with self.engine.begin() as conn:
            stmt = (
                self.pipeline_runs_table.update()
                .where(self.pipeline_runs_table.c.run_id == run_id)
                .values(**kwargs)
            )
            result = conn.execute(stmt)

            if result.rowcount == 0:
                logger.warning(f"No pipeline run record found for ID: {run_id}")
            else:
                logger.info(f"Pipeline run record updated: {run_id}")

    def get_latest_data_date(self, symbol: str = None) -> Optional[datetime]:
        """
        Get the latest date for which we have data.

        Args:
            symbol: Optional symbol filter

        Returns:
            Latest data date or None if no data exists
        """
        with self.engine.connect() as conn:
            query = select(func.max(self.daily_prices_table.c.trade_date))

            if symbol:
                query = query.where(self.daily_prices_table.c.symbol == symbol)

            result = conn.execute(query)
            latest_date = result.scalar()

        return latest_date
    
    def get_data_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the loaded data.

        Returns:
            Dictionary with data statistics
        """
        logger.info("Generating data statistics...")

        stats = {}

        with self.engine.connect() as conn:
            # Daily prices statistics
            prices_query = select(
                func.count().label('total_records'),
                func.count(func.distinct(self.daily_prices_table.c.symbol)).label('unique_symbols'),
                func.min(self.daily_prices_table.c.trade_date).label('earliest_date'),
                func.max(self.daily_prices_table.c.trade_date).label('latest_date')
            )

            prices_result = conn.execute(prices_query).fetchone()
            stats['daily_prices'] = {
                'total_records': prices_result[0],
                'unique_symbols': prices_result[1],
                'earliest_date': str(prices_result.earliest_date) if prices_result.earliest_date else None,
                'latest_date': str(prices_result.latest_date) if prices_result.latest_date else None
            }

            # Indicators statistics
            indicators_query = select(
                func.count().label('total_records'),
                func.count(func.distinct(self.indicators_table.c.symbol)).label('unique_symbol'),
                func.min(self.indicators_table.c.trade_date).label('earliest_date'),
                func.max(self.indicators_table.c.trade_date).label('latest_date')
            )

            indicators_result = conn.execute(indicators_query).fetchone()
            stats['indicators'] = {
                'total_records': prices_result[0],
                'unique_symbols': prices_result[1],
                'earliest_date': str(indicators_result.earliest_date) if indicators_result.earliest_date else None,
                'latest_date': str(indicators_result.latest_date) if indicators_result.latest_date else None
            }

            # Symbol-level statistics
            symbol_stats_query = select(
                self.daily_prices_table.c.symbol,
                func.count().label('record_count'),
                func.min(self.daily_prices_table.c.trade_date).label('start_date'),
                func.max(self.daily_prices_table.c.trade_date).label('end_date'),
                func.max(self.daily_prices_table.c.close_price).label('max_date'),
                func.min(self.daily_prices_table.c.close_price).label('min_date')
            ).group_by(self.daily_prices_table.c.symbol).order_by(self.daily_prices_table.c.symbol)

            symbol_results = conn.execute(symbol_stats_query).fetchall()
            print(symbol_results)
            stats['by_symbol'] = [
                {
                    'symbol': row.symbol,
                    'record_count': row.record_count,
                    'start_date': str(row.start_date),
                    'end_date': str(row.end_date),
                    'max_price': float(row[-2]),
                    'min_price': float(row[-1])
                }
                for row in symbol_results
            ]

            # Recent pipeline runs
            pipeline_runs_query = select(
                self.pipeline_runs_table.c.run_id,
                self.pipeline_runs_table.c.start_time,
                self.pipeline_runs_table.c.end_time,
                self.pipeline_runs_table.c.status,
                self.pipeline_runs_table.c.records_loaded
            ).order_by(self.pipeline_runs_table.c.start_time.desc()).limit(5)

            pipeline_results = conn.execute(pipeline_runs_query).fetchall()
            stats['recent_pipeline_runs'] = [
                {
                    'run_id': row.run_id,
                    'start_time': str(row.start_time),
                    'end_time': str(row.end_time) if row.end_time else None,
                    'status': row.status,
                    'records_loaded': row.records_loaded
                }
                for row in pipeline_results
            ]
        return stats
    
    def cleanup_old_data(self, days_to_keep: int = 730) -> Dict[str, int]:
        """
        Clean up old data beyond retention period.

        Args:
            days_to_keep: Number of days of data to retain

        Returns:
            Dictionary with cleanup statistics
        """
        cutoff_date = datetime.now().date() - timedelta(days = days_to_keep)
        logger.info(f"Cleaning up data older than {cutoff_date}")

        cleanup_stats = {}

        with self.engine.begin() as conn:
            # Clean up old daily prices
            prices_delete = (
                self.daily_prices_table.delete()
                .where(self.daily_prices_table.c.trade_date < cutoff_date)
            )
            prices_result = conn.execute(prices_delete)
            cleanup_stats['daily_prices_deleted'] = prices_result.rowcount

            # Clean up old indicators
            indicators_delete = (
                self.indicators_table.delete()
                .where(self.indicators_table.c.trade_date < cutoff_date)
            )
            indicators_result = conn.execute(indicators_delete)
            cleanup_stats['indicators_deleted'] = indicators_result.rowcount

            # Clean up old pipeline runs (keep more history)
            pipeline_cuttoff = datetime.now() - timedelta(days = 90) # Keep 90 days of runs
            pipeline_delete = (
                self.pipeline_runs_table.delete()
                .where(self.pipeline_runs_table.c.start_time < pipeline_cuttoff)
            )
            pipeline_result = conn.execute(pipeline_delete)
            cleanup_stats['pipeline_runs_deleted'] = pipeline_result.rowcount

        logger.info(f"Cleanup completed: {cleanup_stats}")
        return cleanup_stats
    
    def close(self) -> None:
        """
        Close database connections and clean up resources.
        """
        if self.engine:
            logger.info("Closing database connections...")
            self.engine.dispose()
            logger.info("Database connections closed")
        

def load_stock_data(daily_prices_path: str, indicators_path: str,
                    run_id: str = None) -> Dict[str, Any]:
    """
    Main function to load processed stock data into the database.

    This function orchestrates the complete database loading process:
    1. Initialize database loader
    2. Create pipeline run tracking record
    3. Load daily prices and indicators data
    4. Update pipeline run status
    5. Generate loading statistics

    Args:
        daily_prices_path: Path to daily prices Parquet files
        indicators_path: Path to indicators Parquet files
        run_id: Optional pipeline run identifier

    Returns:
        Dictionary with loading results and statistics
    """

    # Generate run ID if not provided
    if not run_id:
        run_id = f"load_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    loader = DatabaseLoader()
    start_time = datetime.now()

    try:
        # Create pipeline run record
        loader.create_pipeline_run_record(
            run_id = run_id,
            start_time = start_time,
            status = "running", 
            pipeline_version = "1.0"
        )

        # Load data from Parquet files
        loading_stats = loader.load_parquet_files(daily_prices_path, indicators_path)

        # Calculate total records loaded
        total_loaded = (
            loading_stats['daily_prices']['inserted'] + 
            loading_stats['indicators']['inserted']
        )

        # Update pipeline run record
        loader.update_pipeline_run_record(
            run_id = run_id,
            end_time = datetime.now(),
            status = "success",
            symbols_processed = len(loading_stats['symbols_processed']),
            records_loaded = total_loaded
        )

        # Get data statistics
        data_stats = loader.get_data_statistics()

        # Prepare results
        results = {
            "run_id": run_id,
            "status": "success",
            "loading_stats": loading_stats,
            "data_stats": data_stats,
            "duration": loading_stats["duration"].total_seconds(),
            "symbols_processed": loading_stats["symbols_processed"]
        }

        # Display Summary
        print("\n" + "="*60)
        print("DATABASE LOADING SUMMARY")
        print("="*60)
        print(f"Run ID: {run_id}")
        print(f"Duration: {loading_stats['duration']}")
        print(f"Symbols processed: {len(loading_stats['symbols_processed'])}")
        print(f"Daily prices loaded: {loading_stats['daily_prices']['inserted']:,}")
        print(f"Indicators loaded: {loading_stats['indicators']['inserted']:,}")
        print(f"Tota records: {total_loaded:,}")

        if loading_stats["symbols_processed"]:
            print(f"Symbols: {', '.join(sorted(loading_stats['symbols_processed']))}")

        print(f"\nDatabase Statistics:")
        print(f"  Total daily price records: {data_stats['daily_prices']['total_records']:,}")
        print(f"  Total indicator records: {data_stats['indicators']['total_records']:,}")
        print(f"  Date range: {data_stats['daily_prices']['earliest_date']} to {data_stats['daily_prices']['latest_date']}")

        logger.info("Database loading completed successfully")
        return results
    
    except Exception as e:
        logger.error(f"Database loading failed: {str(e)}")

        # Update pipeline run record with error
        try:
            loader.update_pipeline_run_record(
                run_id = run_id,
                end_time = datetime.now(),
                status = 'failed',
                error_message = str(e)[:500] # Truncate long error messages
            )
        except Exception as update_error:
            logger.error(f"Failed to update pipeline run record: {str(update_error)}")

        raise

    finally:
        # Always cleanup database connections
        loader.close()

def main():
    """
    Main function for running the databse loader as a standalone script.

    This demonstrates command-line usage of the database loading functionality.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Load processed stock data into database")
    parser.add_argument("--daily-prices", required = True, help = "Path to daily prices Parquet fiiles")
    parser.add_argument("--indicators", required = True, help = "Path to indicators Parquet files")
    parser.add_argument("--run-id", help="Pipeline run identifier")
    parser.add_argument("--log-level", default = "INFO", choices = ["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()

    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    try:
        # Run database loading
        results = load_stock_data(
            daily_prices_path=args.daily_prices,
            indicators_path=args.indicators,
            run_id=args.run_id
        )

        print(f"\nDatabase loading completed successfully!")
        print(f"Run ID: {results['run_id']}")
        print(f"Total records loaded: {results['loading_stats']['daily_prices']['inserted'] + results['loading_stats']['indicators']['inserted']:,}")

        return 0
    
    except Exception as e:
        logger.error(f"Database loading failed: {str(e)}")
        return 1
    
if __name__ == "__main__":
    exit(main())