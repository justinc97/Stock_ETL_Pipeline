"""
PySpark Data Transformation Module
==================================

This module demonstrates advanced data transformation patterns using Apache Spark:

1. Distributed data processing with PySpark
2. Window functions for time-series analysis
3. Technical indicator calculations
4. Data quality validation at scale
5. Performance optimization techniques
6. Structured streaming concepts

Key learning objectives:
- Understanding Spark DataFrames vs Pandas
- Window functions and partitioning
- User-defined functions (UDFs)
- Catalyst optimizer utilization
- Memory management in spark
"""

import os, sys, logging
from datetime import datetime
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import(
    col, when, lag, avg, stddev, max as spark_max, min as spark_min,
    count, lit, round as spark_round,
    log as spark_log, abs as spark_abs, current_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    DecimalType, IntegerType
)

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import Settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, Settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockDataTransformer:
    """
    PySpark-based stock data transformer for technical analysis.

    This class encapsulates all data transformation logic using Spark,
    demonstrating scalable approaches to financial data processing:

    - Moving averages calculation
    - Volatility metrics
    - Return calculations
    - Technical indicators
    - Data quality checks

    The implementation leverages Spark's distributed computing capabilities to 
    handle large voluems of historical stock data efficiently.
    """

    def __init__(self, app_name: str = None):
        """
        Initialize the Spark-based tranformer.
        
        Args:
            app_name: Spark application name
        """
        self.app_name = app_name or Settings.spark.APP_NAME
        self.spark    = None
        self._initialize_spark_session()

    def _initialize_spark_session(self) -> None:
        """
        Initialize Spark session with opimitzed configuration.

        This config is tuned for financial time-series processing:
        - Adaptive query execution for dynamic optimization
        - Coalescing partitions to reduce small file problems
        - Memory settings appropriate for stock data volumes
        """
        logger.info("Initializing Spark session...")

        # Build Spark session with custom config
        builder = SparkSession.builder.appName(self.app_name)

        # Apply configuration from settings
        for key, value in Settings.spark.SPARK_CONF.items():
            builder = builder.config(key, value)
        
        # Create session
        self.spark = builder.getOrCreate()

        # Set log level to reduce verbosity
        self.spark.sparkContext.setLogLevel("WARN")

        logger.info(f"Spark session initialized: {self.spark.version}")
        logger.info(f"Spark UI available at: {self.spark.sparkContext.uiWebUrl}")

    def load_raw_data(self, file_path: str) -> DataFrame:
        """
        Load raw stock data from CSV into Spark DataFrame.

        This method demonstrates Spark's schema inference and data loading 
        patterns, with explicit schema definition for better performance.

        Args:
            file_path: Path to CSV file containing stock data

        Returns:
            Spark DataFrame with loaded stock data
        """
        logger.info(f"Loading raw data from: {file_path}")

        # Define explicit schema for better perfromance and type safety
        schema = StructType([
            StructField("symbol", StringType(), True),
            StructField("date", DateType(), True),
            StructField("open", DecimalType(10, 2), True),
            StructField("high", DecimalType(10, 2), True),
            StructField("low", DecimalType(10, 2), True),
            StructField("close", DecimalType(10, 2), True),
            StructField("volume", IntegerType(), True),
        ])

        try:
            # Load data with schema
            df = self.spark.read.csv(
                file_path,
                header = True,
                schema = schema,
                timestampFormat = "yyyy-MM-dd",
                comment = '#'
            )

            # Basic data validation
            record_count = df.count()
            logger.info(f"Loaded {record_count:,} records")

            if record_count == 0:
                raise ValueError("No data loaded from file")
            
            # Show sample data for verification
            logger.info("Sample data:")
            df.show(5, truncate=False)

            return df
        
        except Exception as e:
            logger.error(f"Failed to load data from {file_path}: {str(e)}")
            raise

    def calculate_returns(self, df: DataFrame) -> DataFrame:
        """
        Calculate daily returns and log returns for each stock.

        Returns are fundamental to financial analysis. This method demonstrates:
        - Window functions with partitioning
        - Lag operations for time-series
        - Mathematical transformations
        - Null handling in financial calculations

        Args:
            df: Input DataFrame with stock prices

        Returns:
            DataFrame with return columns added
        """
        logger.info("Calculating daily returns...""")

        # Define window specificaiton: partition by symbol, order by date
        # This ensures calculations are done per strock in chronological order
        window_spec = Window.partitionBy("symbol").orderBy("date")

        # Calculate previous day's closing price using lag function
        df_with_prev = df.withColumn(
            "prev_close",
            lag("close", 1).over(window_spec)
        )

        # Calculate daily return: (close - prev_close) / prev_close
        df_with_returns = df_with_prev.withColumn(
            "daily_return",
            when(
                col("prev_close").isNull() | (col("prev_close") == 0),
                lit(0.0)
            ).otherwise(
                (col("close") - col("prev_close")) / col("prev_close")
            )
        )

        # Calculate log returns: ln(close/prev_close)
        # Log returns are preferred for statistical analysis
        df_with_log_returns = df_with_returns.withColumn(
            "log_return",
            when(
                col("prev_close").isNull() | (col("prev_close") == 0) | (col("close") == 0),
                lit(0.0)
            ).otherwise(
                spark_log(col("close")/col("prev_close"))
            )
        )

        # Drop intermediate collumn
        df_final = df_with_log_returns.drop("prev_close")

        logger.info("Returns calculation completed")
        return df_final

    def calculate_moving_averages(self, df: DataFrame) -> DataFrame:
        """
        Calculate multiple moving averages for technical analysis.

        Moving averages are crucial technical indicators. This method shows:
        - Rolling window calculations
        - Multiple window sizes
        - Performance optimization with window frames

        Args:
            df: DataFrame with stock price data

        Returns:
            DataFrame with moving average columns added
        """ 
        logger.info("Calculating moving averages...")

        result_df = df
        
        # Calculate moving averages for different window sizes
        for window_size in Settings.stock.MOVING_AVERAGE_WINDOWS:
            logger.debug(f"Calculating {window_size}-day SMA")

            # Define window with row-based frame for exact N-day calculation
            window_spec = (
                Window.partitionBy("symbol")
                .orderBy("date")
                .rowsBetween(-window_size - 1, 0) # Include current row and N-1 previous
            )

            # Calculate simple moving average
            result_df = result_df.withColumn(
                f"sma_{window_size}",
                spark_round(avg("close").over(window_spec), 2)
            )

            # Calculate volume moving average for the 20-day window
            if window_size == 20:
                result_df = result_df.withColumn(
                    "volume_sma_20",
                    spark_round(avg("volume").over(window_spec), 0).cast(IntegerType())
                )

                # Volume ratio (current volume vs 20-day average)
                result_df = result_df.withColumn(
                    "volume_ratio",
                    when(
                        col("volume_sma_20") == 0,
                        lit(0.0)
                    ).otherwise(
                        spark_round(col("volume") / col("volume_sma_20"), 2)
                    )
                )

        logger.info(f"Moving averages calculated for windows: {Settings.stock.MOVING_AVERAGE_WINDOWS}")
        return result_df
    
    def calculate_volatility(self, df: DataFrame) -> DataFrame:
        """
        Calculate rolling volatility (standard deviation of returns).

        Volatility is a key risk measure in finance. This method demonstrates:
        - Rolling standard deviation calculations
        - Annualized volatility computation
        - Statistical window functions

        Args:
            df: DataFrame with returns data

        Returns:
            DataFrame with volatility columns added
        """
        logger.info("Calculating volatility metrics...")

        # Define window for volatility calculation
        volatility_window = Settings.stock.VOLATILITY_WINDOW
        window_spec = (
            Window.partitionBy("symbol")
            .orderBy("date")
            .rowsBetween(-volatility_window - 1, 0)
        )

        # Calculate rolling standard deviation of returns
        df_with_volatility = df.withColumn(
            f"volatility_{volatility_window}d",
            spark_round(stddev("daily_return").over(window_spec), 4)
        )

        # Calculate annualized volatility (multiply by sqrt(252) for trading days)
        df_with_volatility = df_with_volatility.withColumn(
            f"annualized_volatility",
            spark_round(col(f"volatility_{volatility_window}d") * (252 ** 0.5), 4)
        ) 

        logger.info(f"Volatility calculated using {volatility_window}-day window")
        return df_with_volatility
    
    def calculate_price_indicators(self, df: DataFrame) -> DataFrame:
        """
        Calculate price-based technical indicators and signals.

        This method creates trading signals and position indicators:
        - Price vs moving average comparisons
        - Trend strength assessment
        - Boolean indicators for technical analysis

        Args:
            df: DataFrame with prices and moving averages
        
        Returns:
            DataFrame with indicator columns added
        """
        logger.info("Calculating price indicators...")

        # Price vs Moving Average percentages
        df_with_indicators = df.withColumn(
            "price_vs_sma20_pct",
            when(
                col("sma_20").isNull() | (col("sma_20") == 0),
                    lit(0.0)
            ).otherwise(
                spark_round(((col("close") - col("sma_20")) / col("sma_20")) * 100, 2)
            )
        ).withColumn(
            "price_vs_sma50_pct",
            when(
                col("sma_50").isNull() | (col("sma_50") == 0),
                lit(0.0)
            ).otherwise(
                spark_round(((col("close") - col("sma_50")) / col("sma_50")) * 100, 2)
            )
        )

        # Boolean indicators for price position
        df_with_indicators = df_with_indicators.withColumn(
            "is_above_sma20",
            col("close") > col("sma_20")
        ).withColumn(
            "is_above_sma50",
            col("close") > col("sma_50")
        ).withColumn(
            "is_above_sma200",
            col("close") > col("sma_200")
        )

        # Trend strength assessment based on multiple MA relationships
        df_with_indicators = df_with_indicators.withColumn(
            "trend_strength",
            when(
                col("is_above_sma20") & col("is_above_sma50") & col("is_above_sma200") &
                (col("sma_20") > col("sma_50")) & (col("sma_50") > col("sma_200")),
                lit("strong_up")
            ).when(
                col("is_above_sma20") & col("is_above_sma50"),
                lit("up")
            ).when(
                (~col("is_above_sma20")) & (~col("is_above_sma50")) & (~col("is_above_sma200")) &
                (col("sma_20") < col("sma_50")) & (col("sma_50") < col("sma_200")),
                lit("strong_down")
            ).when(
                (~col("is_above_sma20")) & (~col("is_above_sma50")),
                lit("down")
            ).otherwise(
                lit("sideways")
            )
        )

        logger.info("Price indicators calculation completed")
        return df_with_indicators
    
    def validate_data_quality(self, df: DataFrame) -> Dict[str, int]:
        """
        Perform comprehensive data quality validation.

        Data quality is crucial in financial analysis. This method checks:
        - Missing values in critical columns
        - Logical consistency of OHLC data
        - Outliers and anomalies
        - Data completeness

        Args: 
            df: DataFrame to validate

        Returns:
            Dictionary with validation results
        """
        logger.info("Performing data quality validation...")

        validation_results = {}

        # Chcek for null values in critical columns
        critical_columns = ["symbol", "date", "open", "high", "low", "close", "volume"]
        for column in critical_columns:
            null_count = df.filter(col(column).isNull()).count()
            validation_results[f"{column}_nulls"] = null_count
            if null_count > 0:
                logger.warning(f"Found {null_count} null values in {column}")
        
        # Check for invalid OHLC relationships
        invalid_ohlc = df.filter(
            (col("high") < col("low")) | 
            (col("open") < col("low")) | (col("open") > col("high")) | 
            (col("close") < col("low")) | (col("close") > col("high"))
        ).count()
        validation_results["invalid_ohlc"] = invalid_ohlc

        if invalid_ohlc > 0:
            logger.warning(f"Found {invalid_ohlc} records with invalid OHLC relationships")

        # Check for negative or zero prices
        negative_prices = df.filter(
            (col("open") <= 0) | (col("high") <= 0) | 
            (col("low") <= 0) | (col("close") <= 0)
        ).count()
        validation_results["negative_prices"] = negative_prices

        if negative_prices > 0:
            logger.warning(f"Found {negative_prices} records with negative or zero prices")
        
        # Check for extreme returns (potential data errors)
        extreme_returns = df.filter(
            (spark_abs(col("daily_return")) > 0.5) # > 50% daily move
        ).count()
        validation_results["extreme_returns"] = extreme_returns

        if extreme_returns > 0:
            logger.warning(f"Found {extreme_returns} records with extreme daily returns (>50%)")

        # Data completeness by symbol
        symbol_completeness = df.groupBy("symbol").agg(
            count("*").alias("record_count"),
            spark_min("date").alias("start_date"),
            spark_max("date").alias("end_date")
        )

        logger.info("Data completeness by symbol:")
        symbol_completeness.show()

        total_records = df.count()
        validation_results["total_records"] = total_records

        logger.info(f"Data quality validation completed. Total records; {total_records:,}")
        return validation_results
    
    def create_indicators_dataset(self, df: DataFrame) -> DataFrame:
        """
        Create the final technical indicators dataset for database loading.

        This method prepares the final dataset with all calculated indicators,
        properly formatted for database insertion.

        Args:  
            df: DataFrame with all calculated features

        Returns:
            DataFrame ready for database loading
        """
        logger.info("Creating final indicators dataset...")

        # Select and rename columns for database schema compatibility
        indicators_df = df.select(
            col("symbol"),
            col("date").alias("trade_date"),
            col("daily_return"),
            col("log_return"),
            col("sma_5"),
            col("sma_10"),
            col("sma_20"),
            col("sma_50"),
            col("sma_200"),
            col(f"volatility_{Settings.stock.VOLATILITY_WINDOW}d").alias("volatility_30d"),
            col("volume_sma_20"),
            col("volume_ratio"),
            col("price_vs_sma20_pct"),
            col("price_vs_sma50_pct"),
            col("is_above_sma20"),
            col("is_above_sma50"),
            col("is_above_sma200"),
            col("trend_strength"),
            current_timestamp().alias("calculated_at")
        )

        # Filter out records where we don't have enough data for calculations
        # (e.g., first 200 days won't have SMA_200)
        indicators_df = indicators_df.filter(
            col("sma_20").isNotNull() & col("volatility_30d").isNotNull()
        )

        logger.info(f"Final indicators dataset created with {indicators_df.count():,} records")
        return indicators_df
    
    def create_daily_prices_dataset(self, df: DataFrame) -> DataFrame:
        """
        Create the daily prices dataset for database loading.

        Args:
            df: DataFrame with stock price data

        Returns:
            DataFrame formatted for daily_stock_prices table
        """
        logger.info("Creating daily prices dataset...")

        # Select columns for daily prices table
        daily_prices_df = df.select(
            col("symbol"),
            col("date").alias("trade_date"),
            col("open").alias("open_price"),
            col("high").alias("high_price"),
            col("low").alias("low_price"),
            col("close").alias("close_price"),
            col("volume"),
            lit("alpha_vantage").alias("data_source"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )

        logger.info(f"Daily prices dataset created with {daily_prices_df.count():,} records")
        return daily_prices_df
    
    def save_processed_data(self, df: DataFrame, output_path: str, format: str = "parquet") -> None:
        """
        Save processed data to disk in specified format.

        Demonstrates Spark's data saving capabilities and format options:
        - Parquet for efficient columnar storage
        - Partitioning strategies for time-series data
        - Compression options

        Args:
            df: DataFrame to save
            output_path: Output file path
            format: Output format ("parquet", "csv", "json")
        """
        logger.info(f"Saving processed data to {output_path} in {format} format")

        try:
            writer = df.write.mode("overwrite")

            if format.lower() == "parquet":
                # Save as Parquet with partitioning by symbol for efficient querying
                writer.partitionBy("symbol").parquet(output_path)
            elif format.lower() == "csv":
                # Save as CSV with header
                writer.option("header", "true").csv(output_path)
            elif format.lower() == "json":
                # Save as JSON
                writer.json(output_path)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            logger.info(f"Data successfully saved to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save data: {str(e)}")
            raise

    def get_processing_summary(self, original_df: DataFrame, processed_df: DataFrame) -> Dict:
        """
        Generate processing summary statistics.

        Args:
            original_df: Original input DataFrame
            processed_df: Processed output DataFrame

        Returns:
            Dictionary with processing statistics
        """
        summary = {}

        # Record counts
        summary["input_records"] = original_df.count()
        summary["output_records"] = processed_df.count()
        summary["records_processed"] = summary["output_records"]

        # Symbol information
        input_symbols = original_df.select("symbol").distinct().collect()
        output_symbols = processed_df.select("symbol").distinct().collect()

        summary["input_symbols"] = len(input_symbols)
        summary["output_symbols"] = len(output_symbols)
        summary["symbols_list"] = sorted([row["symbol"] for row in output_symbols])

        # Date ranges
        date_stats = processed_df.agg(
            spark_min("trade_date").alias("start_date"),
            spark_max("trade_date").alias("end_date"),
        ).collect()[0]

        summary["date_range"] = {
            "start": str(date_stats["start_date"]),
            "end":   str(date_stats["end_date"])
        }

        return summary
    
    def cleanup(self) -> None:
        """
        Clean up Spark session and resources.
        """
        if self.spark:
            logger.info("Cleaning up Spark session...")
            self.spark.stop()
            self.spark = None
            logger.info("Spark session stopped")

def transform_stock_data(input_path: str, output_dir: str = None) -> Dict:
    """
    Main transormation function that orchestrates the entire process.

    This function demonstrates the complete ETL transformation pipeline:
    1. Data loading and validation
    2. Feature engineering and calculations
    3. Data quality checks
    4. Output generation

    Args: 
        input_path: Path to input data file
        output_dir: Directory for output files
    
    Returns:
        Dictionary with processing results and statistics
    """
    logger.info("Starting stock data transformation process")

    # Initialize transformer
    transformer = StockDataTransformer()

    try:
        # Load raw data
        raw_df = transformer.load_raw_data(input_path)
        logger.info("Raw data loaded successfully")

        # Data transformation pipeline
        logger.info("Starting transformation pipeline...")

        # Step 1: Calculate returns
        df_with_returns = transformer.calculate_returns(raw_df)
        df_with_returns.cache() # Cache for reuse in multiple calculations

        # Step 2: Calculate moving averages
        df_with_ma = transformer.calculate_moving_averages(df_with_returns)

        # Step 3: Calculate volatility
        df_with_volatility = transformer.calculate_volatility(df_with_ma)
        
        # Step 4: Calculate price indicators
        df_with_indicators = transformer.calculate_price_indicators(df_with_volatility)
        
        # Step 5: Data Quality Validation
        validation_results = transformer.validate_data_quality(df_with_indicators)

        # Create final datasets
        indicators_dataset   = transformer.create_indicators_dataset(df_with_indicators)
        daily_prices_dataset = transformer.create_daily_prices_dataset(df_with_indicators)

        # Save processed data
        if output_dir:
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok = True)

            # Save indicators data
            indicator_path = os.path.join(output_dir, "stock_indicators")
            transformer.save_processed_data(indicators_dataset, indicator_path, "parquet")

            # Save daily prices data
            prices_path = os.path.join(output_dir, "daily_prices")
            transformer.save_processed_data(daily_prices_dataset, prices_path, "parquet")

            # Save as CSV for easy inspection
            csv_path = os.path.join(output_dir, "indicators_sample.csv")
            indicators_dataset.coalesce(1).write.mode("overwrite").option("header","true").csv(csv_path)

        # Generate processing summary
        processing_summary = transformer.get_processing_summary(raw_df, indicators_dataset)
        processing_summary["valudation_results"] = validation_results
        processing_summary["transformation_timestamp"] = datetime.now().isoformat()

        # Display summary
        print("\n" + "="*60)
        print("SPARK TRANSFORMATION SUMMARY")
        print("="*60)
        print(f"Input records: {processing_summary['input_records']:,}")
        print(f"Output records: {processing_summary["output_records"]:,}")
        print(f"Symbols processed: {processing_summary["output_symbols"]}")
        print(f"Date range: {processing_summary['date_range']['start']} to {processing_summary['date_range']['end']}")

        print("\nData Quality Results:")
        for key, value in validation_results.items():
            if isinstance(value, int) and value > 0:
                print(f" {key}: {value:,}")
        
        print(f"\nSymbols: {', '.join(processing_summary['symbols_list'])}")

        # Show sample of transformed data
        print("\nSample of transformed indicators data:")
        indicators_dataset.show(5,truncate=False)

        logger.info("Transformation completed successfully")
        return processing_summary
    
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise

    finally:
        # Always cleanup resources
        transformer.cleanup()

def main():
    """
    Main function for running the transformation as a standalone script.

    This demonstrates how the transformation module would be called from command line or other applications.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Transform stock data using PySpark")
    parser.add_argument("--input", required=True, help="Input data file path")
    parser.add_argument("--output", help="Output directory path")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()

    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    try:
        # Run transformation
        results = transform_stock_data(
            input_path = args.input,
            output_dir = args.output or Settings.paths.PROCESSED_DATA_DIR
        )

        print(f"\nTransformation completed successfully!")
        print(f"Processed {results['output_records']:,} records for {results['output_symbols']} symbols")

        return 0
    
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        return 1
    
if __name__ == "__main__":
    exit(main())