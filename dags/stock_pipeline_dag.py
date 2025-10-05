"""
Stock Market ETL Pipeline - Airflow DAG
=======================================

This DAG orchestrates the complete stock market data pipeline, demonstrating:

1. Apache Airflow workflow management
2. Task dependencies and data flow
3. Error handling and retry strategies
4. Dynamic task generation
5. XCom for inter-task communication
6. Custom operators and sensors
7. Email notifications and monitoring

The pipeline follows this workflow:
1. Extract stock data from Alpha Vantage API
2. Transform data using PySpark for technical indicators
3. Load processed data into PostgreSQL database
4. Send notification on completion/failure

Key Airflow concepts demonstrated:
- DAG definition and scheduling
- PythonOperator and BashOperator usage
- Task dependencies with >> operator
- XCom for passing data between tasks
- Custom functions as Airflow tasks
- Error handling and alerting
"""

import os, sys
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.utils.email import send_email

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import our custom modules
from config.settings import Settings
from scripts.fetch_stock_data import StockDataFetcher
from scripts.transform_spark import transform_stock_data
from scripts.load_to_db import load_stock_data

# DAG configuration
DAG_ID = 'stock_market_etl_pipeline'
DESCRIPTION = 'Complete ETL pipeline for stock market data processing'

# Default arguments for all tasks in the DAG
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': days_ago(Settings.airflow.START_DATE_DAYS_AGO),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': Settings.airflow.RETRIES,
    'retry_delay': timedelta(minutes=Settings.airflow.RETRY_DELAY_MINUTES),
    'execution_timeout': timedelta(minutes=Settings.airflow.EXECUTION_TIMEOUT_MINUTES),
}

def extract_stock_data_task(**context) -> str:
    """
    Airflow task function for stock data extraction.

    This function demonstrates:
    - How to wrap existing python code as Airflow tasks
    - XCom usage for passing data between tasks
    - Error handling in Airflow tasks
    - Context usage for accessing Airflow metadata

    Args:
        **context: Airflow context dictionary
    
    Returns:
        Path to the extracted data file
    """
    print("Starting stock data extraction task...")

    # Get execution context
    ds = context['ds'] # Date of execution
    run_id = context['run_id']

    print(f"Execution date: {ds}")
    print(f"Run ID: {run_id}")

    # Initialize data fetcher
    fetcher = StockDataFetcher()

    try:
        # Get symbols from Airflow Variables or use defaults
        symbols_var = Variable.get("stock_symbols", default_var = None)
        if symbols_var:
            symbols = [s.strip().upper() for s in symbols_var.splot(',')]
            print(f"Using symbols from Airflow Variables: {symbols}")
        else:
            symbols = Settings.stock.DEFAULT_SYMBOLS
            print("Using default symbols: {symbols}")

        # Fetch data for all symbols
        combined_data = fetcher.fetch_multiple_symbols(symbols, save_individual = True)

        if combined_data.empty:
            raise AirflowException("No stock data was successfully extracted")
        
        # Save combined data
        output_file = fetcher.save_combined_data(combined_data, f"extracted_data_{ds}")

        # Log extraction statistics
        extraction_stats = {
            'total_records': len(combined_data),
            'symbols_count': combined_data['symbol'].nunique(),
            'symbols_list': sorted(combined_data['symbol'].unique().tolist()),
            'date_range': {
                'start': str(combined_data['date'].min()),
                'end': str(combined_data['date'].max())
            },
            'output_file': output_file
        }

        print(f"Extraction completed: {extraction_stats}")

        # Store stats in XCom for downstream tasks
        context['task_instance'].xcom_push(key='extraction_stats', value = extraction_stats)

        # Return output file path for next task
        return output_file

    except Exception as e:
        print(f"Stock data extraction failed: {str(e)}")
        raise AirflowException(f"Extraction task failed: {str(e)}")
    
def transform_stock_data_task(**context) -> Dict[str, str]:
    """
    Airflow task function for PySpark data transformation.

    This function demonstrates:
    - Retrieving data from pervious tasks using XCom
    - Error handling for Spark jobs in Airflow
    - Returning structured data for downstream tasks

    Args:
        **context: Airflow context dictionary

    Returns:
        Dictionary with paths to transformed data files
    """
    print("Starting stock data transformation task...")

    # Get input file from previous task
    task_instance = context['task_instance']
    input_file = task_instance.xcom_pull(task_ids='extract_stock_data')

    if not input_file:
        raise AirflowException("No input file received from extraction task")
    
    print(f"Input file: {input_file}")

    # Get execution context
    ds = context['ds']

    # Define output directory
    output_dir = os.path.join(Settings.paths.PROCESSED_DATA_DIR, f"transformed_{ds}")

    try:
        # Run Spark transformation
        transformation_results = transform_stock_data(
            input_path=input_file,
            output_dir=output_dir
        )

        print(f"Transformation completed: {transformation_results}")

        # Define expected output paths
        output_paths = {
            'daily_prices_path': os.path.join(output_dir, 'daily_prices'),
            'indicators_path': os.path.join(output_dir, 'stock_indicators'),
            'transformation_stats': transformation_results
        }

        # Store transformation stats in XCom 
        task_instance.xcom_push(key = 'transformation_stats', 
                                value = transformation_results)
        
        return output_paths
    
    except Exception as e:
        print(f"Stock data transformation failed: {str(e)}")
        raise AirflowException(f"Transformation task failed: {str(e)}")
    
def load_to_database_task(**context) -> Dict[str, Any]:
    """
    Airflow task function for databse loading.

    This function demonstrates:
    - Accessing transformed data from previous task
    - Database operations in Airflow tasks
    - Comprehensive error handling and logging

    Args:
        **context: Airflow context dictionary

    Returns:
        Dictionary with loading statistics
    """
    print("Starting database loading task...")

    # Get transformed data paths from previous task
    task_instance = context['task_instance']
    output_paths = task_instance.xcom_pull(task_ids='transform_stock_data')

    if not output_paths:
        raise AirflowException("No output paths received from transformation task")

    print(f"Data paths: {output_paths}")

    # Get execution context
    run_id = context['run_id']

    try:
        # Load data into database
        loading_results = load_stock_data(
            daily_prices_path=output_paths['daily_prices_path'],
            indicators_path=output_paths['indicators_path'],
            run_id=f"airflow_{run_id}"
        )

        print(f"Database loading completed: {loading_results}")

        # Store loading stats in XCom
        task_instance.xcom_push(key='loading_stats', value=loading_results)

        return loading_results
    
    except Exception as e:
        print(f"Database loading failed: {str(e)}")
        raise AirflowException(f"Database loading task failed: {str(e)}")
    
def data_quality_check_task(**context) -> bool:
    """
    Airflow task for data quality validation.
    
    This function demonstrates:
    - Data quality checks in ETL pipelines
    - Conditional task execution based on results
    - Using database queries for validation

    Args:
        **context: Airflow context dictionary

    Returns:
        Boolean indicating if data quality checks passed
    """
    print("Starting data quality checks...")

    from scripts.load_to_db import DatabaseLoader

    try:
        loader = DatabaseLoader()

        # Get data statistics
        stats = loader.get_data_statistics()

        # Define quality checks
        checks_passed = True
        issues = []

        # Check 1: Ensure we have data for all expected symbols
        expected_symbols = set(Settings.stock.DEFAULT_SYMBOLS)
        actual_symbols   = set()

        for symbol_stat in stats['by_symbol']:
            actual_symbols.add(symbol_stat['symbol'])
        
        missing_symbols = expected_symbols - actual_symbols
        if missing_symbols:
            checks_passed = False
            issues.append(f"Missing data for symbols: {missing_symbols}")

        # Check 2: Ensure data is recent (within last 7 days for weekdays)
        latest_date = stats['daily_prices']['latest_date']
        if latest_date:
            latest_date_obj = datetime.strptime(latest_date, '%-%m-%d').date()
            days_old = (datetime.now().date() - latest_date_obj).days

            # Allow for weekends and holidays (up to 5 days)
            if days_old > 5:
                checks_passed = False
                issues.append(f"Data is {days_old} days old, latest: {latest_date}")
        
        # Check 3: Ensure reasonable number of records per symbol
        min_expected_records = 50 # At least 50 days of data
        for symbol_stat in stats['by_symbol']:
            if symbol_stat['record_count'] < min_expected_records:
                checks_passed = False
                issues.append(f"Insufficient data for symbol {symbol_stat['symbol']}: {symbol_stat['record_count']} records")

        # Store quality check results
        quality_results = {
            'checks_passed': checks_passed,
            'issues': issues,
            'stats': stats
        }

        context['task_instance'].xcom_push(key='quality_results', value=quality_results)

        # Log results
        if checks_passed:
            print("All data quality checks passed!")
        else:
            print("Data quality issues found:")
            for issue in issues:
                print(f"   - {issue}")
        
        loader.close()
        return checks_passed
    
    except Exception as e:
        print(f"Data quality check failed: {str(e)}")
        raise AirflowException(f"Data quality check failed: {str(e)}")
    
def send_notification_task(**context) -> None:
    """
    Send pipeline completion notification.

    This function demonstrates:
    - Email notifications in Airflow
    - Aggregating results from multiple tasks
    - Conditional messaging based on pipeline results

    Args:
        **context: Airflow context dictionary
    """
    print("Sending pipeline completion notification...")

    task_instance = context['task_instance']
    ds = context['ds']

    try:
        # Gather results from all tasks
        extraction_stats = task_instance.xcom_pull(key='extraction_stats', task_ids='extract_stock_data')
        transformation_stats = task_instance.xcom_pull(key='transformation_stats', task_ids='transform_stock_data')
        loading_stats = task_instance.xcom_pull(key='loading_stats', task_ids='load_to_database')
        quality_results = task_instance.xcom_pull(key='quality_results', task_ids='data_quality_check')

        # Prepare email content
        subject = f"Stock ETL Pipeline Completed - {ds}"
        
        if quality_results and quality_results['checks_passed']:
            subject += " SUCCESS"
            status = "SUCCESS"
        else:
            subject += " WITH ISSUES"
            status = "COMPLETED WITH ISSEUS"
        
        # Build email body
        body = f"""
Stock Market ETL Pipeline Report
================================

Execution Date: {ds}
Status: {status}

EXTRACTION RESULTS:
- Records extracted: {extraction_stats.get('total_records', 0):,}
- Symbols processed: {extraction_stats.get('symbols_count', 0)}
- Date range: {extraction_stats.get('date_range', {}).get('start')} to {extraction_stats.get('date_range', {}).get('end')}

TRANSFORMATION RESULTS:
- Input records: {transformation_stats.get('input_records', 0):,}
- Output records: {transformation_stats.get('output_records', 0):,}
- Symbols transformed: {transformation_stats.get('output_symbols', 0)}

DATABASE LOADING:
- Daily prices loaded: {loading_stats.get('loading_stats', {}).get('daily_prices', {}).get('inserted', 0):,}
- Indicators loaded: {loading_stats.get('loading_stats', {}).get('indicators', {}).get('inserted', 0):,}
- Duration: {loading_stats.get('duration', 0):.1f} seconds

DATA QUALITY:
"""
        if quality_results:
            if quality_results['checks_passed']:
                body += "ALL quality checks passed!\n"
            else:
                body += "Quality issues found:\n"
                for issue in quality_results['issues']:
                    body += f"  - {issue}\n"
        
        body += f"\SYMBOLS PROCESSED:\n{', '.join(extraction_stats.get('symbols_list', []))}"

        print("Email notification prepared:")
        print(f"Subject: {subject}")
        print(f"Body: {body}")

        # In production, configure email settings and send actual emails
        # send_email(to=['data-team@company.com'],subject=subject,html_content=body)
        print("Notification sent successfully")

    except Exception as e:
        print(f"Failed to send notification: {str(e)}")
        # Don't fail the entire pipeline for notification issue

def cleanup_old_files_task(**context) -> None:
    """
    Clean up old data files to manage disk space.

    Args:
        **context: Airflow context dictionary
    """
    print("Cleaning up old files...")

    import shutil
    from pathlib import Path

    try:
        # Clean up files older than 30 days
        cutoff_date = datetime.now() - timedelta(days = 30)

        directories_to_clean = [
            Settings.paths.RAW_DATA_DIR,
            Settings.paths.PROCESSED_DATA_DIR
        ]

        files_deleted = 0
        for directory in directories_to_clean:
            if os.path.exists(directory):
                for file_path in Path(directory).rglob('*'):
                    if file_path.is_file():
                        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        if file_mtime < cutoff_date:
                            file_path.unlink()
                            files_deleted += 1
                            print(f"Deleted old file: {file_path}")
        
        print(f"Cleanup completed. Deleted {files_deleted} old files.")
    
    except Exception as e:
        print(f"File cleanup failed: {str(e)}")


# ===============================================================
# DAG DEFINITION
# ===============================================================

# Create the DAG instance
dag = DAG(
    dag_id = DAG_ID,
    description = DESCRIPTION,
    default_args = default_args,
    schedule_interval = Settings.airflow.SCHEDULE_INTERVAL,
    max_active_runs = Settings.airflow.MAX_ACTIVE_RUNS,
    catchup = Settings.airflow.CATCHUP,
    tags = ['stock_market','etl','finance','pyspark'],
    doc_md = __doc__,
)

# ===============================================================
# TASK DEFINITIONS
# ===============================================================

# Start task (dummy operator for clear workflow visualization)
start_task = DummyOperator(
    task_id = 'start_pipeline',
    dag = dag,
    doc_md = "Pipeline start marker"
)

# Data extraction task
extract_data_task = PythonOperator(
    task_id = 'extract_stock_data',
    python_callable = extract_stock_data_task,
    dag = dag,
    doc_md = """
    Extract stock data from Alpha Vantage API
    
    This task:
    - Fetches OHLCV data from configured stock symbols
    - Handles API rate limiting and retries
    - Validates and cleans raw data
    - Saves data to local storage for processing
    """
)

# Data transformation task group
with TaskGroup("transform_data", dag = dag) as transform_group:

    # File sensor to ensure data is available
    wait_for_extracted_data = FileSensor(
        task_id = 'wait_for_extracted_data',
        filepath = os.path.join(Settings.paths.RAW_DATA_DIR, 'stock_data_{{ ds }}.csv'),
        poke_interval = 30,
        timeout = 300,
        dag = dag,
        doc_md = "Wait for extracted data file to be available"
    )

    # PySpark transformation task
    transform_data_task = PythonOperator(
        task_id = 'transform_stock_data',
        python_callable = transform_stock_data_task,
        dag = dag,
        doc_md = """
        Transform stock data using PySpark.
        
        This task:
        - Loads raw stock data into Spark DataFrames
        - Calculates technical indicators and moving averages
        - Computes volatility and return metrics
        - Validates data quality
        - Saves processed data in Parquet format
        """
    )

    # Task dependencies within the group
    wait_for_extracted_data >> transform_data_task

# Database loading task
load_database_task = PythonOperator(
    task_id = 'load_to_database',
    python_callable = load_to_database_task,
    dag = dag,
    doc_md = """
    Load processed data into PostgreSQL database.
    
    This task:
    - Loads Parquet files from Spark transformation
    - Performs data validation and type conversion
    - Uses PostgreSQL UPSERT for efficient loading
    - Tracks pipeline execution in audit tables
    """
)

# Data quality validation task
quality_check_task = PythonOperator(
    task_id = 'data_quality_check',
    python_callable = data_quality_check_task,
    dag = dag,
    doc_md = """
    Perform data quality validation checks.
    
    This task:
    - Validates data completeness and accuracy
    - Checks for data freshness and consistency
    - Ensures all expected symbols have data
    - Reports any quality issues found
    """
)

# Notification task
notification_task = PythonOperator(
    task_id = 'send_notification',
    python_callable = send_notification_task,
    dag = dag,
    trigger_rule = 'all_done',
    doc_md = "Send pipeline completion notification via email"
)

# Cleanup task
cleanup_task = PythonOperator(
    task_id = 'cleanup_old_files',
    python_callable = cleanup_old_files_task,
    dag = dag,
    trigger_rule = 'all_done',
    doc_md = "Clean up old data files to manage disk space"
)

# End task (dummy operator for clear workflow visualization)
end_task = DummyOperator(
    task_id = 'end_pipeline',
    dag = dag,
    trigger_rule = 'all_done',
    doc_md = 'Pipeline end marker'
)

# ===============================================================
# TASK DEPENDENCIES 
# ===============================================================

# Define the task flow using the >> operator
# This creates a clear, linear pipeline flow:

start_task >> extract_data_task >> transform_group >> load_database_task >> quality_check_task

# Parallel completion tasks
quality_check_task >> [notification_task, cleanup_task] >> end_task

# ===============================================================
# ADDITIONAL CONFIG
# ===============================================================

# Add custom failure callback
def task_failure_callback(context):
    """
    Custom callback function for task failures.

    This function is called whenever a task fails and can be used for:
    - Custom alerting
    - Slack notifications
    - PagerDuty integration
    - Custom logging
    """
    task_instance = context['task_instance']
    dag_run = context['dag_run']

    print(f"Task failed: {task_instance.task_id}")
    print(f"DAG: {task_instance.dag_id}")
    print(f"Execution dateL {dag_run.execution_date}")
    print(f"Log URL: {task_instance.log_url}")

    # Here add custom alerting logic
    # e.g., send slack message

# Apply failue callback to critical tasks
extract_data_task.on_failure_callback = task_failure_callback
transform_data_task.on_failure_callback = task_failure_callback
load_database_task.on_failure_callback = task_failure_callback

# ===============================================================
# DAG DOCUMENTATION
# ===============================================================

# Add comprehensive DAG documentation
dag.doc_md = """
# Stock Market ETL Pipeline

This DAG implements a complete ETL (Extract, Transform, Load) pipeline for stock market data processing.

## Pipeline Overview

The pipeline consists of the folling stages:

1. **Data Extraction**: Fetch stock price data from Alpha Vantage API
2. **Data Transformation**:  Process data using Apache Spark to calculate technical indicators
3. **Data Loading**: Load the processed data into a PostgreSQL database
4. **Quality Validation**: Perform data quality checks and validation
5. **Notification**: Send completion notificaion and reports

## Schedule
- **Frequency**: Daily at 22:00 UTC (weekdays only)
- **Start Date**: {{ (ds_add(ds, -1))  }}
- **Catchup**: Disabled
- **Max Active Runs**: 1

## Data Sources
- **Primary**: Alpha Vantage API for real-time stock data
- **Symbols**: Configurable via Airflow Variables ('stock_symbols')
- **Default Symbols**: AAPL, GOOGL, MSFT, AMZN, TSLA, META, NVDA, NFLX

## Technical Stack

- **Orchestration**: Apache Airflow
- **Data Processing**: Apache Spark (PySpark)
- **Database**: PostgreSQL
- **Data Formats**: CSV (raw), Parquet (processed)
- **APIs**: Alpha Vantage Financial Data API

## Configuration

### Airflow Variables
- `stock_symbols`: Comma-separated list of stock symbols to process
- `email_notificaions`: Email address for notifications

### Connections
- `postgres_default`: PostgreSQL database connection
- `alpha_vantage_api`: API connection with authentication

## Monitoring

- **Success/Failure Notifications**: Email alerts
- **Data Quality Checks**: Automated validation
- **Pipeline Metrics**: Execution time, record counts, error rates
- **Logs**: Comprehensive logging at all stages

## Error Handling

- **Retries**: 2 attempts with 5-minute delays
- **Timeout**: 30-minute task timeout
- **Graceful Degradation**: Pipeline continues if some symbols fail
- **Data Validation**: Comprehensive checks before database loading

## Maintenance

- **File Cleanup**: Automatic cleanup of files older than 30 days
- **Database Cleanup**: Configurable data retention periods
- **Monitoring**: Pipeline execution tracking in audit tables

## Dependencies

See `requirements.txt` for complete dependency list.

## Support

For issues or questions, contact me.
"""

# ===============================================================
# UTILITY FUNCTIONS FOR DAG MANAGEMENT 
# ===============================================================

def get_dag_config():
    """
    Get DAG configuration for external monitoring or management tools.

    Returns:
        Dictionary with DAG configuration details
    """
    return {
        'dag_id': DAG_ID,
        'description': DESCRIPTION,
        'schedule_interval': Settings.airflow.SCHEDULE_INTERVAL,
        'max_active_runs': Settings.airflow.MAX_ACTIVE_RUNS,
        'catchup': Settings.airflow.CATCHUP,
        'start_date': days_ago(Settings.airflow.START_DATE_DAYS_AGO),
        'task_count': len(dag.task_dict),
        'task_ids': list(dag.task_dict.keys())
    }

def validate_dag_configuration():
    """
    Validate DAG configuration and dependencies.

    This function can be used to check DAG health and configuration 
    before deployment or during troubleshooting.

    Returns:
        Dictionary with validation results
    """
    validation_results = {
        'valid': True,
        'issues': [],
        'warnings': []
    }
    
    # Check if required Airflow connections exist
    from airflow.models import Connection
    from airflow.utils.db import provide_session

    @provide_session
    def check_connections(session):
        required_connections = ['postgres_default']
        existing_connections = session.query(Connection.conn_id).all()
        existing_conn_ids    = [conn[0] for conn in existing_connections]

        for conn_id in required_connections:
            if conn_id not in existing_conn_ids:
                validation_results['issues'].append(f"Missing connection: {conn_id}")
                validation_results['valud'] = False

    try:
        check_connections()
    except Exception as e:
        validation_results['warnings'].append(f"Could not validate connections: {str(e)}")

    # Check if required directories exist
    required_dirs = [
        Settings.paths.RAW_DATA_DIR,
        Settings.paths.PROCESSED_DATA_DIR,
        Settings.paths.LOGS_DIR
    ]

    for dir_path in required_dirs:
        if not os.path.exist(dir_path):
            validation_results['warnings'].append(f"Directory will be created: {dir_path}")

    return validation_results

# Export DAG instance for Airflow
# This is required for Airflow to discover and load the DAG
globals()[DAG_ID] = dag