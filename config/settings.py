"""
Configuration settings for the Stock ETL Pipeline
=================================================

This module contains all configuration constants and settings used throughout the pipeline.
It demonstrates best practices for managing configuration in data engineering projects:
1. Environment-based configuration
2. Centralized settings mangement
3. Type hints for better code documentation
4. Sensible defaults with override capabilities
"""

import os
from typing import List, Dict
from dotenv import load_dotenv

# Load environment variables from .env file

load_dotenv()

class DatabaseConfig:
    """
    Database configuration class

    Centralizes all database-related settings and connection parameters.
    Uses environment variables for sensitive information like passwords.
    """
    HOST: str = os.getenv('DB_HOST', 'localhost')
    PORT: int = int(os.getenv('DB_PORT', '5432'))
    NAME: str = os.getenv('DB_NAME', 'stock_etl')
    USER: str = os.getenv('DB_USER', 'postgres')
    PASSWORD: str = os.getenv('DB_PASSWORD', 'password')

    @classmethod
    def get_connection_string(cls) -> str:
        """Generate SQLAlchemy connection string"""
        return f"postgresql://{cls.USER}:{cls.PASSWORD}@{cls.HOST}:{cls.PORT}/{cls.NAME}"
    
class APIConfig:
    """
    External API configuration

    Contains settings for financial data APIs. 
    Alpha Vantage is used as the primary data source, 
    but the structure allows for easy extension to other providers like Yahoo Finance, 
    IEX Cloud, etc.
    """
    ALPHA_VANTAGE_API_KEY:  str = os.getenv('ALPHA_VANTAGE_API_KEY', 'demo') 
    ALPHA_VANTAGE_BASE_URL: str = 'https://www.alphavantage.co/query'

    # API rate limiting settings
    REQUESTS_PER_MINUTE: int = 5 # ALPHA Vantage free tier limit
    RETRY_ATTEMPTS:      int = 3
    RETRY_DELAY:         int = 60 # Seconds

class StockConfig:
    """
    Stock-specific configuration

    Defines which stocks to track and analysis parameters.
    This modular approach makes it easy to modify the stock universe 
    without changing core pipeline logic.
    """
    # Default stock symbols to track (Major tech stocks)
    DEFAULT_SYMBOLS: List[str] = [
        'AAPL',  # Apple Inc.
        'GOOGL', # Alphabet Inc.
        'MSFT',  # Microsoft Corp.
        'AMZN',  # Amazon.com Inc.
        'TSLA',  # Tesla Inc.
        'META',  # Meta Platforms Inc.
        'NVDA',  # NVIDIA Corp.
        'NFLX',  # Netflix Inc.
    ]

    # Technical analysis parameters
    MOVING_AVERAGE_WINDOWS: List[int] = [5,10,20,50,200] # Days
    VOLATILITY_WINDOW: int = 30 # Days for rolling volatility calculation

    # Data retention settings
    HISTORICAL_DAYS: int = 365 * 2 # 2 years of historical data

class SparkConfig:
    """
    PySpark configuration settings
    
    Optimizes Spark performance for our specific use case.
    These settings are tuned for local development but can be 
    adjusted for production cluster deployments.
    """
    APP_NAME: str = 'StockETL'
    MASTER:   str = os.getenv('SPARK_MASTER', 'local[*]')

    # Spark SQL warehouse directory
    WAREHOUSE_DIR: str = os.path.join(os.getcwd(), 'spark-warehouse')

    # Memory and performance tuning
    DRIVER_MEMORY:   str = '2g'
    EXECUTOR_MEMORY: str = '2g'

    # Spark configuration dictionary
    SPARK_CONF: Dict[str,str] = {
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.sql.warehouse.dir': WAREHOUSE_DIR,
        'spark.driver.memory': DRIVER_MEMORY,
        'spark.executor.memory': EXECUTOR_MEMORY
    }

class AirflowConfig:
    """
    Apache Airflow DAG configuration

    Defines scheduling and execution parameters for the ETL pipeline.
    These settings control when and how often the pipeline runs.
    """
    # DAG scheduling
    SCHEDULE_INTERVAL: str = '0 22 * * 1-5' # 10 PM UTC, weekdays (after market close)
    START_DATE_DAYS_AGO: int = 1
    MAX_ACTIVE_RUNS: int = 1
    CATCHUP: bool = False
    
    # Task execution settings
    RETRIES: int = 2
    RETRY_DELAY_MINUTES: int = 5
    EXECUTION_TIMEOUT_MINUTES: int = 30

class WebConfig:
    """
    Flask web application configuration

    Settings for the dashboard web application including 
    security, debugging, and display preferences.
    """
    SECRET_KEY: str = os.getenv('FLASK_SECRET_KEY', 'dev-key-change-in-production')
    DEBUG:     bool = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    HOST:       str = os.getenv('FLASK_HOST', '0.0.0.0')
    PORT:       int = int(os.getenv('FLASK_PORT', '5555'))

    # Dashboard display settings
    DEFAULT_CHART_DAYS:       int = 30
    MAX_CHART_POINTS:         int = 1000
    REFRESH_INTERVAL_SECONDS: int = 300 # 5 minutes

class PathConfig:
    """
    File system paths configuration

    Centralizes all file and directory paths used by the pipeline.
    Uses absolute paths to avoid issues with different execution contexts.
    """
    PROJECT_ROOT: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR: str = os.path.join(PROJECT_ROOT, 'data')
    RAW_DATA_DIR: str = os.path.join(DATA_DIR, 'raw')
    PROCESSED_DATA_DIR: str = os.path.join(DATA_DIR, 'processed')
    LOGS_DIR: str = os.path.join(PROJECT_ROOT, 'logs')

    # Ensure directories exist
    @classmethod
    def create_directories(cls) -> None:
        """Create necessary directories if they don't exist"""
        for dir_path in [cls.DATA_DIR, cls.RAW_DATA_DIR, cls.PROCESSED_DATA_DIR, cls.LOGS_DIR]:
            os.makedirs(dir_path, exist_ok = True)

# Application-wide settings
class Settings:
    """
    Main settings class that combines all configuration classes

    Provides a single point of access to all application settings.
    This pattern makes it easy to manage complex configurations
    across different components of the system.
    """
    db = DatabaseConfig()
    api = APIConfig()
    stock = StockConfig()
    spark = SparkConfig()
    airflow = AirflowConfig()
    web = WebConfig()
    paths = PathConfig()

    # Envivronment
    ENVIRONMENT: str = os.getenv('ENVIRONMENT', 'development')
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')

    @classmethod
    def initialize(cls) -> None:
        """Initialize application settings and create required directories"""
        cls.paths.create_directories()
        print(f"Stock ETL Pipeline initialized in {cls.ENVIRONMENT} environment")

# Initialize settings when module is imported
Settings.initialize()