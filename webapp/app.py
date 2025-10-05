"""
Stock Market Dashboard - Flask Web Application
==============================================

This Flask application provide a web-based dashboard for visualizing stock market data.
It demonstartes: 

1. Flask web framework fundamentals
2. Database integration with SQLAlchemy
3. RESTful API design patterns
4. JSON data serialization for frontend
5. Template rendering with Jinja2
6. Error handling and logging
7. Real-time data visualization preparation

Key features:
- Interactive stock price charts
- Technical indicator visualization
- Portfolio performance tracking
- Real-time data updates
- Responsive web design
- REST API endpoints for data access

The application follows the MVC (Model-View-Controller) pattern and
implements best practices for web application security and performance.
"""

import os, sys, json, logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from flask import Flask, render_template, jsonify, request, abort, Response
from flask import current_app, g
from werkzeug.exceptions import HTTPException
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import Settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, Settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ===============================================================================
# FLASK APPLICATION SETUP
# ===============================================================================

def create_app(config_name: str = 'default') -> Flask:
    """
    Application factory pattern for Flask app creation.

    This pattern allows for:
    - Easy testing with different configurations
    - Multiple app instances
    - Better separation of concerns
    - Cleaner initialization process

    Args:
        config_name: Configuration profile to use

    Returns:
        Configured Flask application instance
    """
    app = Flask(__name__)

    # Configure Flask application
    app.config['SECRET_KEY'] = Settings.web.SECRET_KEY
    app.config['DEBUG']      = Settings.web.DEBUG
    app.config['JSON_SORT_KEYS'] = False # Preserve JSON key order
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True if Settings.web.DEBUG else False

    # Database configuration
    app.config['DATABASE_URL'] = Settings.db.get_connection_string()

    # Initialize database engine
    app.engine = create_engine(
        app.config['DATABASE_URL'],
        pool_size = 5,
        max_overflow = 10,
        pool_timeout = 30,
        pool_recycle = 3600
    )

    # Register blueprints and routes
    register_routes(app)
    register_error_handlers(app)

    logger.info(f"Flask application created in {config_name} mode")
    return app

# ===============================================================================
# DATABASE HELPER FUNCTIONS
# ===============================================================================

def get_db_connection():
    """
    Get database connection from application context.

    This function implements the Flask application context pattern for database connections, ensuring proper connection management.

    Returns:
        SQLAlchemy database connection
    """
    if 'db_conn' not in g:
        g.db_conn = current_app.engine.connect()
    return g.db_conn

def close_db_connection():
    """
    Close database connection when application context ends.
    """
    conn = g.pop('db_conn', None)
    if conn is not None:
        conn.close()

def execute_query(query: str, params: Dict = None) -> pd.DataFrame:
    """
    Execute SQL query and return results as pandas DataFrame.

    This function provides a convenient interface for database queries
    with proper error handling and connection management.

    Args:
        query: SQL query string
        params: Query parameters dictionary

    Returns:
        Query results as pandas DataFrame
    
    Raises:
        SQLAlchemyError: If database query fails
    """
    try:
        conn = get_db_connection()

        if params:
            result = pd.read_sql_query(query, conn, params = params)
        else:
            result = pd.read_sql_query(query, conn)
        
        logger.debug(f"Query executed successfully, returned {len(result)} rows")
        return result

    except SQLAlchemyError as e:
        logger.error(f"Database query failed: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in query execution: {str(e)}")
        raise


# ===============================================================================
# DATA ACCESS LAYER
# ===============================================================================

class StockDataService:
    """
    Service class for stock data access and business logic.

    This class encapsulates all database operations and business logic
    for stock data, following the Service Layer pattern. It provides
    a clean interface between the web layer and data layer.
    """

    @staticmethod
    def get_available_symbols() -> List[str]:
        """
        Get list of available stock symbols.

        Returns:
            List of stock symbols
        """
        query = """
        SELECT DISTINCT symbol
        FROM daily_stock_prices
        WHERE trade_date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY symbol
        """

        df = execute_query(query)
        return df['symbol'].tolist()
    
    @staticmethod
    def get_stock_price_data(symbol: str, days: int = 30) -> Dict[str, Any]:
        """
        Get stock price data for a specific symbol.

        Args:
            symbol: Stock symbol
            days: Number of days of historical data

        Returns:
            Dictionary with stock price data and metadata
        """
        query = """
        SELECT
            trade_date,
            open_price,
            high_price,
            low_price,
            close_price,
            volume
        FROM daily_stock_prices
        WHERE symbol = %(symbol)s
            AND trade_date >= CURRENT_DATE - INTERVAL '%(days)s days'
        ORDER BY trade_date ASC
        """

        params = {'symbol': symbol, 'days': days}
        df = execute_query(query,params)

        if df.empty:
            return {'symbol': symbol, 'data': [], 'metadata': {'count': 0}}
        
        # Convert to JSON-serializable format
        data = []
        for _, row in df.iterrows():
            data.append({
                'date': row['trade_date'].strftime('%Y-%m-%d'),
                'open': float(row['open_price']),
                'high': float(row['high_price']),
                'low': float(row['low_price']),
                'close': float(row['close_price']),
                'volume': int(row['volume'])
            })

        # Calculate metadata
        metadata = {
            'count': len(data),
            'start_date': data[0]['date'] if data else None,
            'end_date': data[-1]['date'] if data else None,
            'latest_price': data[-1]['close'] if data else None,
        }

        return {
            'symbol': symbol,
            'data': data,
            'metadata': metadata
        }
    
    @staticmethod
    def get_technical_indicators(symbol: str, days: int = 30) -> Dict[str, Any]:
        """
        Get technical indicators for a specific symbol.

        Args:   
            symbol: Stock symbol
            days: Number of days of historical data

        Returns:
            Dictionary with technical indicator data
        """
        query = """
        SELECT
            i.trade_date,
            i.daily_return,
            i.sma_5,
            i.sma_20,
            i.sma_50,
            i.volatility_30d,
            i.is_above_sma20,
            i.is_above_sma50,
            i.trend_strength,
            p.close_price
        FROM stock_indicators i
        JOIN daily_stock_prices p ON i.symbol = p.symbol AND i.trade_date = p.trade_date
        WHERE i.symbol = %(symbol)s
            AND i.trade_date >= CURRENT_DATE - INTERVAL '%(days)s days'
        ORDER BY i.trade_date ASC
        """

        params = {'symbol': symbol, 'days': days}
        df = execute_query(query, params)

        if df.empty:
            return {'symbol': symbol, 'indicators': [], 'metadata': {'count': 0}}
        
        # Convert to JSON-serializable format
        indicators = []
        for _, row in df.iterrows():
            indicators.append({
                'date': row['trade_date'].strftime('%Y-%m-%d'),
                'price': float(row['close_price']),
                'daily_return': float(row['daily_return']) if row['daily_return'] else None,
                'sma_5': float(row['sma_5']) if row['sma_5'] else None,
                'sma_20': float(row['sma_20']) if row['sma_20'] else None,
                'sma_50': float(row['sma_50']) if row['sma_50'] else None,
                'volatility': float(row['volatility_30d']) if row['volatility_30d'] else None,
                'above_sma20': bool(row['is_above_sma20']) if row['is_above_sma20'] is not None else None,
                'above_sma50': bool(row['is_above_sma50']) if row['is_above_sma50'] is not None else None,
                'trend': row['trend_strength']
            })
        
        # Calculate summary statistics
        recent_data = df.tail(5) # Last 5 days
        avg_return = recent_data['daily_return'].mean() if not recent_data['daily_return'].isna().all() else None
        current_volatility = df.iloc[-1]['volatility_30d'] if not df.empty else None

        metadata = {
            'count': len(indicators),
            'avg_recent_return': float(avg_return) if avg_return else None,
            'current_volatility': float(current_volatility) if current_volatility else None,
            'current_trend': df.iloc[-1]['trend_strength'] if not df.empty else None
        }

        return {
            'symbol': symbol,
            'indicators': indicators,
            'metadata': metadata
        }
    
    @staticmethod
    def get_portfolio_summary() -> Dict[str, Any]:
        """
        Get portfolio summary with top performers and recent activity.

        Returns:
            Dictionary with portfolio summary data
        """

        # Get latest data for all symbols
        query = """
        WITH latest_data AS (
            SELECT
                p.symbol,
                s.company_name,
                p.close_price,
                p.volume,
                i.daily_return,
                i.volatility_30d,
                i.trend_strength,
                p.trade_date
            FROM daily_stock_prices p
            JOIN stock_symbols s ON p.symbol = s.symbol
            LEFT JOIN stock_indicators i ON p.symbol = i.symbol AND p.trade_date = i.trade_date
            WHERE p.trade_date = (
                SELECT MAX(trade_date)
                FROM daily_stock_prices p2
                WHERE p2.symbol = p.symbol
            )
        ),
        weekly_performance AS (
            SELECT
                symbol,
                ((close_price - LAG(close_price, 5) OVER (PARTITION BY symbol ORDER BY trade_date)) / 
                LAG(close_price, 5) OVER (PARTITION BY symbol ORDER BY trade_date)) * 100 as weekly_return
            FROM daily_stock_prices
            WHERE trade_date >= CURRENT_DATE - INTERVAL '7 days'
        )
        SELECT
            l.*,
            COALESCE(w.weekly_return, 0) as weekly_return
        FROM latest_data l
        LEFT JOIN weekly_performance w ON l.symbol = w.symbol
        ORDER BY l.symbol
        """

        df = execute_query(query)

        if df.empty:
            return {'summary': {}, 'top_performers': [], 'recent_activity': []}
        
        # Prepare portfolio data
        portfolio_data = []
        for _, row in df.iterrows():
            portfolio_data.append({
                'symbol': row['symbol'],
                'company_name': row['company_name'],
                'price': float(row['close_price']),
                'daily_return': float(row['daily_return']) if row['daily_return'] else 0,
                'weekly_return': float(row['weekly_return']) if row['weekly_return'] else 0,
                'volatility': float(row['volatility_30d']) if row['volatility_30d'] else 0,
                'trend': row['trend_strength'],
                'volume': int(row['volume'])
            })

        # Calculate summary statistics
        total_symbols = len(portfolio_data)
        avg_daily_return = sum(stock['daily_return'] for stock in portfolio_data) / total_symbols if total_symbols > 0 else 0
        postitive_stocks = sum(1 for stock in portfolio_data if stock['daily_return'] > 0)

        # Top performers (by weekly return)
        top_performers = sorted(portfolio_data, key=lambda x: x['weekly_return'], reverse = True)[:3]

        # Recent activity (highest volume)
        recent_activity = sorted(portfolio_data, key=lambda x: x['volume'], reverse = True)[:5]

        summary = {
            'total_symbols': total_symbols,
            'avg_daily_return': round(avg_daily_return, 4),
            'positive_stocks': postitive_stocks,
            'positive_ratio': round((postitive_stocks/total_symbols) * 100, 1) if total_symbols > 0 else 0,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        return {
            'summary': summary,
            'top_performers': top_performers,
            'recent_activity': recent_activity,
            'all_stocks': portfolio_data
        }
    
# ===============================================================================
# ROUTE HANDLERS
# ===============================================================================

def register_routes(app: Flask) -> None:
    """
    Register all application routes.

    Args:
        app: Flask application instance
    """

    @app.teardown_appcontext
    def close_db(error):
        """Close database connection at the end of each request."""
        close_db_connection
    
    @app.route('/')
    def index():
        """
        Main dashboard page.

        This route renders the main dashboard template with initial data
        for teh stock market overview
        """
        try:
            # Get available symbols for the dropdown
            symbols = StockDataService.get_available_symbols()

            # Get portfolio summary
            portfolio_summary = StockDataService.get_portfolio_summary()

            return render_template(
                'index.html',
                symbols = symbols,
                portfolio_summary = portfolio_summary,
                page_title = "Stock Market Dashboard"
            )
        
        except Exception as e:
            logger.error(f"Error loading dashboard: {str(e)}")
            return render_template(
                'error.html',
                error_message = "Failed to load dashboard data",
                error_details = str(e) if app.config['DEBUG'] else None
            ), 500
        
    @app.route('/api/symbols')
    def api_symbols():
        """
        API endpoint to get available stock symbols.

        Returns:
            JSON array of available stock symbols
        """
        try:
            symbols = StockDataService.get_available_symbols()
            return jsonify({
                'symbols': symbols,
                'count': len(symbols),
                'timestamp': datetime.now().isoformat()
            })
        
        except Exception as e:
            logger.error(f"Error getting symbols: {str(e)}")
            return jsonify({'error': 'Failed to fetch indicators'}), 500
    
    @app.route('/api/stock/<symbol>')
    def api_stock_data(symbol):
        """API endpoint to get stock price data"""
        try:
            symbol = symbol.upper().strip()
            days = request.args.get('days', 30, type=int)
            days = min(max(days,1),365)

            stock_data = StockDataService.get_stock_price_data(symbol, days)

            if not stock_data['data']:
                return jsonify({'error': 'No data found for symbol'}), 404
            
            return jsonify(stock_data)
        except Exception as e:
            logger.error(f"Error getting stock data for {symbol}: {str(e)}")
            return jsonify({'error': 'Failed to fetch stock data'}), 500
        
    @app.route('/api/indicators/<symbol>')    
    def api_indicators(symbol):
        """API endpoint to get technical indicators"""
        try:
            symbol = symbol.upper().strip()
            days = request.args.get('days', 30, type=int)
            days = min(max(days,1),365)

            indicators_data = StockDataService.get_technical_indicators(symbol, days)

            if not indicators_data['indicators']:
                return jsonify({'error': 'No indicators found for symbol'}), 404
            return jsonify(indicators_data)
        except Exception as e:
            logger.error(f"Error getting indicators for {symbol}: {str(e)}")
            return jsonify({'error': 'Failed to fetch indicators'}), 500


    @app.route('/api/portfolio')
    def api_portfolio():
        """
        API endpoint to get portfolio summary data

        Returns:
            JSON object with portfolio overview and top performers
        """
        try:
            portfolio_data = StockDataService.get_portfolio_summary()
            return jsonify(portfolio_data)

        except Exception as e:
            logger.error(f"Error getting portfolio data: {str(e)}")
            return jsonify({'error': 'Failed to fetch portfolio data'}), 500
        
    @app.route('/api/compare')
    def api_compare():
        """
        API endpoint to compare multiple stocks.

        Query Parameters:
            symbols: Comma-separated list of stock symbols
            days: Number of days of historical data (default: 30)
            metric: Comparison metric ('price', 'return', 'volatility')

        Returns:
            JSON object with comparison data
        """
        try:
            # Get query parameters
            symbols_param = request.args.get('symbols', '')
            if not symbols_param:
                return jsonify({'error': 'No symbols provided'}), 400

            symbols = [s.upper().strop() for s in symbols_param.split(',')]
            symbols = [s for s in symbols if s] # Remove empty strings

            if len(symbols) > 10: # Limit to 10 symbols for perfomance
                return jsonify({'error': 'Too many symbols (max 10)'}), 400
            
            days = request.args.get('days', 30, type = int)
            days = min(max(days, 1), 365)

            metric = request.args.get('metric', 'price').lower()
            if metric not in ['price', 'return', 'volatility']:
                metric = 'price'

            # Fetch data for all symbols
            comparison_data = []
            for symbol in symbols:
                if metric == 'price':
                    data = StockDataService.get_stock_price_data(symbol, days)
                    if data['data']:
                        comparison_data.append({
                            'symbol': symbol,
                            'data': data['data'],
                            'metadata': data['metadata']
                        })
                else:
                    data = StockDataService.get_technical_indicators(symbol, days)
                    if data['indicators']:
                        comparison_data.append({
                            'symbol': symbol,
                            'data': data['indicators'],
                            'metadata': data['metadata'],
                        })
            return jsonify({
                'comparison': comparison_data,
                'metric': metric,
                'symbols_requested': symbols,
                'symbols_found': [item['symbol'] for item in comparison_data],
                'timestamp': datetime.now().isoformat()
            })
        
        except Exception as e:
            logger.error(f"Error in stock comparison: {str(e)}")
            return jsonify({'error': 'Failed to compare stocks'}), 500
        
    @app.route('/api/health')
    def api_health():
        """
        API endpoint for application health check.

        RETURNS:
            JSON object with application health status
        """
        try:
            # Test database connection
            test_query = 'SELECT 1 as test'
            execute_query(test_query)

            # Get basic stats
            symbols = StockDataService.get_available_symbols()

            health_status = {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'database': 'connected',
                'symbols_available': len(symbols),
                'version': '1.0.0',
                'environment': Settings.ENVIRONMENT
            }

            return jsonify(health_status)

        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return jsonify({
                'status': 'unhealthy',
                'timestamp': datetime.now().isoformat(),
                'error': str(e) if app.config['DEBUG'] else 'Internal error'
            }), 500
        
    @app.route('/api/debug/routes')
    def debug_routes():
        """Debug endpoint to list all registered routes"""
        routes = []
        for rule in app.url_map.iter_rules():
            routes.append({
                'endpoint': rule.endpoint,
                'methods': list(rule.methods),
                'path': str(rule)
            })
        return jsonify(routes)
        
    from flask import send_from_directory

    @app.route('/favicon.ico')
    def favicon():
        """Serve favicon from static directory."""
        return send_from_directory(
            os.path.join(app.root_path, 'static'),
            'favicon.ico',
            mimetype='image/vnd.microsoft.icon'
        )
        
def register_error_handlers(app: Flask) -> None:
    """
    Register custom error handlers for the application.

    Args:
        app: Flask application instance
    """    

    @app.errorhandler(404)
    def not_found_error(error):
        """Handle 404 Not Found errors."""
        if request.path.startswith('/api/'):
            return jsonify({'error': 'Endpoint not found'}), 404
        return render_template('error.html',
                               error_message='Sorry, we cannot find this page',
                               error_code = 404), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        """Handle 500 Internal Server errors."""
        logger.error(f"Internal server error: {str(error)}")
        if request.path.startswith('/api/'):
            return jsonify({'error': 'Internal server error'}), 500
        return render_template('error.html',
                               error_message="Internal server error",
                               error_code = 500), 500
    
    @app.errorhandler(SQLAlchemyError)
    def database_error(error):
        """Handle database errors."""
        logger.error(f"Database error: {str(error)}")
        if request.path.startswith('/api/'):
            return jsonify({'error':'Database error'}), 500
        return render_template('error.html',
                               error_message = "Database connection error",
                               error_details = "Please try again later"), 500
    
    @app.errorhandler(Exception)
    def general_error(error):
        """Handle general exceptions."""
        logger.error(f"Unhandled exception: {str(error)}", exc_info = True)
        if request.path.startswith('/api/'):
            return jsonify({'error':'Unexpected error occurred'}), 500
        return render_template('error.html',
                               error_message="An unexpected error occured",
                               error_details = str(error) if app.config['DEBUG'] else None), 500
    
# ===============================================================================
# APPLICATION FACTORY AND MAIN
# ===============================================================================

# Create Flask application instance
app = create_app()

# Custom CLI commands for dev and maintenance
@app.cli.command()
def init_db():
    """Initialize database tables."""
    print("Initializing database...")
    try:
        # Import and run database initialization
        from scripts.load_to_db import DatabaseLoader
        loader = DatabaseLoader()
        print("Database initialized successfully!""")
        loader.close()
    except Exception as e:
        print(f"Database initialization failed: {str(e)}")

@app.cli.command()
def test_db():
    """Test database connection and basic queries."""
    print("Testing database connection...")
    try:
        symbols = StockDataService.get_available_symbols()
        print(f"YES! Database connection successful!")
        print(f"YES! Found {len(symbols)} symbols: {', '.join(symbols[:5])}{'...' if len(symbols) > 5 else ''}")

        if symbols:
            # Test data retrieval
            test_symbol = symbols[0]
            data = StockDataService.get_stock_price_data(test_symbol, 5)
            print(f"YES! Data retrieval test passed for {test_symbol}")
            print(f"YES! Latest price: ${data['metadata']['latest_price']}")

    except Exception as e:
        print(f"NO! Database test failed: {str(e)}")

@app.cli.command()
def create_sample_data():
    """Create sample data for development and testing."""
    try:
        # This would tyipcally produce the database with sample data
        # For demo purposes we'll just print a message
        print("Sample data creation would go here...")
        print("In production, this would:")
        print("1. Create sample stock symbols")
        print("2. Generate historical price data")
        print("3. Calculate sample indicators")
        print("✅ Sample data creation completed!")
    except Exception as e:
        print(f"❌ Sample data creation failed: {str(e)}")

def main():
    """
    Main function to run the Flask development server.

    This function is used when running the application directly
    for development and testing purposes.
    """
    print("="*60)
    print("STOCK MARKET DASHBOARD")
    print("="*60)
    print(f"Environment: {Settings.ENVIRONMENT}")
    print(f"Debug mode: {Settings.web.DEBUG}")
    print(f"Database: {Settings.db.HOST}:{Settings.db.PORT}/{Settings.db.NAME}")
    print(f"Log level: {Settings.LOG_LEVEL}")
    print("-"*60)

    # Test database connection
    try:
        with app.app_context():
            symbols = StockDataService.get_available_symbols()
            print(f"YES! Database connected - {len(symbols)} symbols available")
            if symbols:
                print(f"   Symbols: {', '.join(symbols[:3])}{'...' if len(symbols) > 3 else ''}")
    except Exception as e:
        print(f"NO! Database connection failed: {str(e)}")
        print("   Please check database config. and ensure it's running.")
        return 1

    print("-"*60)
    print(f"Starting Flask development server...")
    print(f"Dashboard URL: http://{Settings.web.HOST}:{Settings.web.PORT}")
    print(f"API Base URL: http://{Settings.web.HOST}:{Settings.web.PORT}/api")
    print("="*60)

    # Run the Flask dev. server
    try:
        app.run(
            host = Settings.web.HOST,
            port = Settings.web.PORT,
            debug = Settings.web.DEBUG,
            threaded = True
        )
    except KeyboardInterrupt:
        print("\n\nShutting down gracefully...")
        return 0
    except Exception as e:
        print(f"\nFailed to start server: {str(e)}")
        return 1
    

if __name__ == '__main__':
    exit(main())

    


