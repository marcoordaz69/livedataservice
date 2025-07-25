#!/usr/bin/env python3
import databento as db
import asyncio
import os
import logging
import sys
from datetime import datetime, timezone, timedelta
import asyncpg
from dotenv import load_dotenv
from decimal import Decimal
import time

# Import SetupMonitor and TradingDB
from setup_monitor import SetupMonitor
from database.trading_db import TradingDB

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "simple_ohlcv.log")),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Add some startup diagnostic logging
logger.info(f"simple_ohlcv.py started")
logger.info(f"Log file path: {os.path.join(os.path.dirname(os.path.abspath(__file__)), 'simple_ohlcv.log')}")
logger.info(f"Current working directory: {os.getcwd()}")

# Load environment variables
load_dotenv()

# Global variables
minute_data_cache = {}

# Setup monitor instance (global)
setup_monitor = None

# Remove global db_lock, rely on connection pool instead
# db_lock = asyncio.Lock()

# Database connection pool (global)
db_pool = None

# Latest price data cache (to reduce database queries)
latest_price_cache = {
    'timestamp': None,
    'data': None,
    'last_update': 0
}

# Setup check throttling
last_setup_check = 0
setup_check_interval = 5  # Check setups every 5 seconds

# Add a global event loop
main_loop = None

# Price formatting helper
def format_price(price):
    """Format the raw price value to a proper format."""
    if isinstance(price, (int, float)):
        return price / 1000000000
    return price

async def create_db_pool():
    """Create and return a database connection pool."""
    global db_pool
    
    if db_pool is not None:
        return db_pool
        
    host = os.getenv('host')
    user = os.getenv('user')
    password = os.getenv('password')
    port = os.getenv('port', '5432')
    dbname = os.getenv('dbname', 'postgres')
    
    if not all([host, user, password]):
        raise ValueError("Missing required database environment variables (host, user, password)")
    
    # Create a connection pool with a reasonable min and max size
    db_pool = await asyncpg.create_pool(
        user=user,
        password=password,
        database=dbname,
        host=host,
        port=int(port),
        ssl='require',  # Supabase requires SSL
        min_size=2,     # Minimum connections in pool
        max_size=10     # Maximum connections in pool
    )
    
    logger.info("Database connection pool created")
    return db_pool

async def get_db_connection():
    """Get a connection from the pool, creating the pool if needed."""
    start_time = time.time()
    try:
        pool = await create_db_pool()
        connection_start = time.time()
        conn = await pool.acquire()
        connection_time = time.time() - connection_start
        
        if connection_time > 1.0:
            logger.warning(f"[PERF] Database connection acquisition took {connection_time:.3f}s")
            
        return conn
    except Exception as e:
        total_time = time.time() - start_time
        logger.error(f"[PERF] Failed to get database connection after {total_time:.3f}s: {e}")
        raise
    finally:
        elapsed = time.time() - start_time
        if elapsed > 2.0:
            logger.warning(f"[PERF] Total get_db_connection time: {elapsed:.3f}s")

async def release_db_connection(conn):
    """Release a connection back to the pool."""
    start_time = time.time()
    if db_pool is not None:
        try:
            await db_pool.release(conn)
            elapsed = time.time() - start_time
            if elapsed > 0.5:
                logger.warning(f"[PERF] Slow connection release: {elapsed:.3f}s")
        except Exception as e:
            logger.error(f"[PERF] Error releasing connection: {e}")
            elapsed = time.time() - start_time
            logger.error(f"[PERF] Failed connection release took {elapsed:.3f}s")
    else:
        logger.warning("[PERF] Tried to release connection but pool is None")

async def insert_ohlcv_data(data_record):
    """Insert a single OHLCV record into the database."""
    if not data_record:
        return
    
    # No longer using global lock
    conn = None
    try:
        # Get a connection from the pool
        conn = await get_db_connection()
        
        async with conn.transaction():
            await conn.execute("""
                INSERT INTO raw_ohlcv (
                    symbol, timestamp, open, high, low, close, volume, source_contract
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (symbol, timestamp) DO UPDATE
                SET open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    source_contract = EXCLUDED.source_contract
            """, 
            data_record['symbol'],
            data_record['timestamp'],
            str(data_record['open']),
            str(data_record['high']),
            str(data_record['low']),
            str(data_record['close']),
            data_record['volume'],
            data_record['source_contract']
            )
            
            # Update cache after successful insert
            if data_record['symbol'] == 'NQ':
                latest_price_cache['timestamp'] = data_record['timestamp']
                latest_price_cache['data'] = {
                    'timestamp': data_record['timestamp'],
                    'open': data_record['open'],
                    'high': data_record['high'],
                    'low': data_record['low'],
                    'close': data_record['close'],
                    'volume': data_record['volume'],
                    'source_contract': data_record['source_contract']
                }
                latest_price_cache['last_update'] = time.time()
            
            logger.info(f"Inserted/Updated record for {data_record['timestamp'].isoformat()} UTC")
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        raise
    finally:
        # Return the connection to the pool
        if conn:
            await release_db_connection(conn)

def process_record(record):
    """Process and format an OHLCV record."""
    global minute_data_cache
    
    try:
        # Format timestamp with UTC timezone
        timestamp = datetime.fromtimestamp(record.ts_event / 1e9, tz=timezone.utc)
        
        # Get minute key for consistent tracking
        minute_key = timestamp.strftime('%Y-%m-%d %H:%M:00')
        
        # Scale prices from nanoseconds
        open_price = format_price(record.open)
        high_price = format_price(record.high)
        low_price = format_price(record.low)
        close_price = format_price(record.close)
        
        # Get volume and symbol
        volume = getattr(record, 'volume', 0)
        symbol = getattr(record, 'symbol', 'NQ.n.0')
        
        # For front month contract tracking, always use NQ.n.0
        # This is the most accurate way to track front month
        source_contract = 'NQ.n.0'
        
        # Prepare data record
        data_record = {
            'symbol': 'NQ',  # Always use NQ as the normalized symbol
            'timestamp': timestamp,
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': volume,
            'source_contract': source_contract
        }
        
        # Track data by minute
        if minute_key not in minute_data_cache:
            minute_data_cache[minute_key] = data_record
            logger.info(f"New entry for {minute_key}: {symbol} with volume {volume}")
        elif volume > minute_data_cache[minute_key]['volume'] * 1.5:
            # Update if this has 50% more volume
            minute_data_cache[minute_key] = data_record
            logger.info(f"Updated highest volume entry for {minute_key}: {symbol} with volume {volume}")
        
        # Log the data
        logger.info(f"\n{timestamp.isoformat()} UTC | Front month contract: {source_contract}")
        logger.info(f"Open:   ${open_price:.2f}")
        logger.info(f"High:   ${high_price:.2f}")
        logger.info(f"Low:    ${low_price:.2f}")
        logger.info(f"Close:  ${close_price:.2f}")
        logger.info(f"Volume: {volume}")
        logger.info("-" * 50)
        
        # Cleanup minute_data_cache - remove entries older than 10 minutes
        current_minute = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:00')
        for key in list(minute_data_cache.keys()):
            # If this isn't the current minute and we have a stored record
            if key != current_minute and key in minute_data_cache:
                entry = minute_data_cache[key]
                
                # Check if enough time has passed to consider it complete (at least 30 seconds)
                if (datetime.now(timezone.utc) - entry['timestamp']).total_seconds() > 30:
                    logger.info(f"Processing completed minute data for {key}...")
                    
                    # Return the entry to be inserted into database
                    record_to_insert = {
                        'symbol': entry['symbol'],
                        'timestamp': entry['timestamp'],
                        'open': entry['open'],
                        'high': entry['high'],
                        'low': entry['low'],
                        'close': entry['close'],
                        'volume': entry['volume'],
                        'source_contract': entry['source_contract']
                    }
                    
                    # Remove from cache
                    del minute_data_cache[key]
                    
                    # Return the entry
                    return record_to_insert
        
        # Return None if we're not ready to insert anything yet
        return None
                    
    except Exception as e:
        logger.error(f"Error processing OHLCV record: {e}")
        return None

def error_callback(exception):
    """Handle stream errors."""
    logger.error(f"Stream error: {exception}")

async def process_minute_data_cache():
    """Periodically process and insert completed minute data from the cache."""
    global minute_data_cache, setup_monitor
    
    while True:
        try:
            # Wait for 30 seconds
            await asyncio.sleep(30)
            
            current_minute = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:00')
            inserted_count = 0
            
            # Process all entries except current minute
            for key in list(minute_data_cache.keys()):
                if key != current_minute:
                    entry = minute_data_cache[key]
                    
                    # Check if enough time has passed
                    if (datetime.now(timezone.utc) - entry['timestamp']).total_seconds() > 30:
                        logger.info(f"Processing completed minute data for {key}...")
                        
                        # Create record for insertion
                        record_to_insert = {
                            'symbol': entry['symbol'],
                            'timestamp': entry['timestamp'],
                            'open': entry['open'],
                            'high': entry['high'],
                            'low': entry['low'],
                            'close': entry['close'],
                            'volume': entry['volume'],
                            'source_contract': entry['source_contract']
                        }
                        
                        # Insert into database
                        try:
                            await insert_ohlcv_data(record_to_insert)
                            inserted_count += 1
                        except Exception as e:
                            logger.error(f"Error inserting data in process_minute_data_cache: {e}", exc_info=True)
                        
                        # Validate setups against this price data, just like in handle_record
                        if setup_monitor and entry['symbol'] == 'NQ':
                            try:
                                # Ensure DB connection is valid
                                await setup_monitor.db.ensure_connected()
                                
                                # Get the latest data directly from the database instead of using just the cached data
                                # This ensures we're always using the most recent data for validation
                                latest_data = await get_latest_ohlcv_from_db()
                                
                                if latest_data:
                                    # Use the latest data from the database
                                    close_price = Decimal(str(latest_data['close']))
                                    high_price = Decimal(str(latest_data['high']))
                                    low_price = Decimal(str(latest_data['low']))
                                    
                                    logger.info(f"Using latest database data for setup validation:")
                                    logger.info(f"  Timestamp: {latest_data['timestamp'].isoformat()} UTC")
                                    logger.info(f"  Price range: ${float(low_price):.2f}-${float(high_price):.2f}")
                                    logger.info(f"  Close: ${float(close_price):.2f}")
                                else:
                                    # Fall back to the cached entry if latest data retrieval fails
                                    close_price = Decimal(str(entry['close']))
                                    high_price = Decimal(str(entry['high']))
                                    low_price = Decimal(str(entry['low']))
                                    
                                    logger.info(f"Falling back to cached data for setup validation:")
                                    logger.info(f"  Timestamp: {entry['timestamp'].isoformat()} UTC")
                                    logger.info(f"  Price range: ${float(low_price):.2f}-${float(high_price):.2f}")
                                    logger.info(f"  Close: ${float(close_price):.2f}")
                                
                                logger.info(f"Checking cached minute data against price range: ${float(low_price):.2f}-${float(high_price):.2f}")
                                
                                # Use full price range for validation
                                results = await setup_monitor.check_price(
                                    symbol='NQ.FUT',
                                    close_price=close_price,
                                    high_price=high_price,
                                    low_price=low_price
                                )
                                
                                # Log any triggered setups
                                if results:
                                    logger.info(f"Found {len(results)} setup triggers in cached data!")
                                    for result in results:
                                        if result.matched:
                                            logger.info(f"SETUP TRIGGERED (cached): {result.message}")
                                            logger.info(f"Setup ID: {result.setup_id}")
                                            logger.info(f"Status: {result.status}")
                                            
                                            # Log trade completion details for exits
                                            if result.status == 'closed':
                                                if "Take profit" in result.message:
                                                    logger.info(f"TRADE COMPLETED: PROFIT ✓ at ${float(result.close_price):.2f}")
                                                elif "Stop loss" in result.message:
                                                    logger.info(f"TRADE COMPLETED: LOSS ✗ at ${float(result.close_price):.2f}")
                                                else:
                                                    logger.info(f"TRADE COMPLETED: MANUAL EXIT at ${float(result.close_price):.2f}")
                            except Exception as setup_error:
                                logger.error(f"Error checking setups with cached data: {setup_error}", exc_info=True)
                        
                        # Remove from cache
                        del minute_data_cache[key]
            
            if inserted_count:
                logger.info(f"Inserted {inserted_count} completed minute records")
                    
        except Exception as e:
            logger.error(f"Error in minute data processing: {e}", exc_info=True)

async def handle_record(record):
    """Asynchronous handler for processing records."""
    global setup_monitor, last_setup_check
    
    start_time = time.time()
    record_info = None
    
    try:
        # Only process OHLCV records
        if hasattr(record, 'open') and hasattr(record, 'high') and hasattr(record, 'low') and hasattr(record, 'close'):
            processing_start = time.time()
            data_record = process_record(record)
            processing_time = time.time() - processing_start
            
            if data_record:
                # Record info for logging
                record_info = f"timestamp={data_record['timestamp'].isoformat()}, symbol={data_record['symbol']}"
                logger.info(f"[PERF] Record processing took {processing_time:.3f}s for {record_info}")
                
                # Insert data without global lock
                db_start = time.time()
                await insert_ohlcv_data(data_record)
                db_time = time.time() - db_start
                
                if db_time > 2.0:
                    logger.warning(f"[PERF] Database operations took {db_time:.3f}s for {record_info}")
                
                # Only check setups periodically to reduce overhead
                current_time = time.time()
                if setup_monitor and data_record['symbol'] == 'NQ' and (current_time - last_setup_check) >= setup_check_interval:
                    try:
                        # Update last check timestamp
                        last_setup_check = current_time
                        
                        # Use cached data if available, otherwise query the database
                        if latest_price_cache['data'] and latest_price_cache['timestamp']:
                            latest_data = latest_price_cache['data']
                            logger.info(f"Using cached price data for setup validation (age: {current_time - latest_price_cache['last_update']:.1f}s)")
                        else:
                            latest_data = await get_latest_ohlcv_from_db()
                        
                        if latest_data:
                            # Use the price data for validation
                            close_price = Decimal(str(latest_data['close']))
                            high_price = Decimal(str(latest_data['high']))
                            low_price = Decimal(str(latest_data['low']))
                            
                            logger.info(f"Checking trade setups against price range: ${float(low_price):.2f}-${float(high_price):.2f} (close: ${float(close_price):.2f})")
                            
                            # Check the setups
                            setup_check_start = time.time()
                            results = await setup_monitor.check_price(
                                symbol='NQ.FUT',
                                close_price=close_price,
                                high_price=high_price,
                                low_price=low_price
                            )
                            setup_check_time = time.time() - setup_check_start
                            
                            if setup_check_time > 2.0:
                                logger.warning(f"[PERF] Setup check took {setup_check_time:.3f}s for {len(setup_monitor.active_setups.get('NQ.FUT', {}))} setups")
                            
                            # Log any triggered setups
                            if results:
                                logger.info(f"Found {len(results)} setup triggers!")
                                for result in results:
                                    if result.matched:
                                        logger.info(f"SETUP TRIGGERED: {result.message}")
                                        logger.info(f"Setup ID: {result.setup_id}")
                                        logger.info(f"Status: {result.status}")
                                        
                                        # Log trade completion details for exits
                                        if result.status == 'closed':
                                            if "Take profit" in result.message:
                                                logger.info(f"TRADE COMPLETED: PROFIT ✓ at ${float(result.close_price):.2f}")
                                            elif "Stop loss" in result.message:
                                                logger.info(f"TRADE COMPLETED: LOSS ✗ at ${float(result.close_price):.2f}")
                                            else:
                                                logger.info(f"TRADE COMPLETED: MANUAL EXIT at ${float(result.close_price):.2f}")
                    except Exception as setup_error:
                        logger.error(f"Error checking setups: {setup_error}", exc_info=True)
    except Exception as e:
        logger.error(f"Error in handle_record: {e}")
    finally:
        total_time = time.time() - start_time
        if total_time > 5.0:
            if record_info:
                logger.warning(f"[PERF] handle_record took {total_time:.3f}s for {record_info}")
            else:
                logger.warning(f"[PERF] handle_record took {total_time:.3f}s for unknown record")

async def get_latest_ohlcv_from_db():
    """Fetch the latest OHLCV data from the database for the NQ symbol."""
    start_time = time.time()
    conn = None
    
    try:
        # Get a connection from the pool
        conn_start = time.time()
        conn = await get_db_connection()
        conn_time = time.time() - conn_start
        
        if conn_time > 1.0:
            logger.warning(f"[PERF] Connection acquisition in get_latest_ohlcv_from_db took {conn_time:.3f}s")
        
        try:
            # Query to get the most recent record
            query = """
                SELECT timestamp, open, high, low, close, volume, source_contract 
                FROM raw_ohlcv 
                WHERE symbol = 'NQ' 
                ORDER BY timestamp DESC 
                LIMIT 1
            """
            
            # Execute the query
            query_start = time.time()
            row = await conn.fetchrow(query)
            query_time = time.time() - query_start
            
            if query_time > 1.0:
                logger.warning(f"[PERF] Query execution in get_latest_ohlcv_from_db took {query_time:.3f}s")
            
            if not row:
                logger.warning("No OHLCV data found in database for NQ")
                return None
                
            # Convert row to dictionary
            return {
                'timestamp': row['timestamp'],
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close'],
                'volume': row['volume'],
                'source_contract': row['source_contract']
            }
            
        except Exception as e:
            logger.error(f"Error fetching latest OHLCV data: {e}", exc_info=True)
            return None
        finally:
            # Return the connection to the pool
            release_start = time.time()
            await release_db_connection(conn)
            release_time = time.time() - release_start
            
            if release_time > 0.5:
                logger.warning(f"[PERF] Connection release in get_latest_ohlcv_from_db took {release_time:.3f}s")
            
    except Exception as e:
        logger.error(f"Database connection error in get_latest_ohlcv_from_db: {e}", exc_info=True)
        return None
    finally:
        total_time = time.time() - start_time
        if total_time > 2.0:
            logger.warning(f"[PERF] get_latest_ohlcv_from_db took {total_time:.3f}s total")

# Create a synchronous wrapper function that places the task on the main event loop
def handle_record_sync(record):
    """Synchronous wrapper for handle_record that ensures it runs on the main event loop."""
    global main_loop
    if main_loop:
        main_loop.call_soon_threadsafe(
            lambda: asyncio.create_task(handle_record(record))
        )

async def main():
    """Main function to run the OHLCV data collection."""
    global setup_monitor, db_pool, main_loop
    
    # Store the main event loop for use in callbacks
    main_loop = asyncio.get_running_loop()
    
    # Verify environment variables
    if not all([os.getenv("DATABENTO_API_KEY"),
                os.getenv("host"),
                os.getenv("user"),
                os.getenv("password")]):
        logger.error("Missing required environment variables")
        return

    try:
        logger.info("=== STARTING SIMPLE_OHLCV.PY ===")
        
        # Initialize database connection pool
        logger.info("Initializing database connection pool...")
        await create_db_pool()
        logger.info("Database connection pool established")
        
        # Initialize database connection
        logger.info("Initializing database connection...")
        db_conn = TradingDB()
        await db_conn.connect()
        logger.info("Database connection established")
        
        # Initialize setup monitor
        logger.info("Initializing setup monitor...")
        setup_monitor = SetupMonitor(db_conn)
        logger.info("Setup monitor initialized")
        
        # No need for db_lock, directly refresh setups
        await setup_monitor.refresh_setups('NQ.FUT')
        setup_count = setup_monitor.get_active_setup_count('NQ.FUT')
        
        # Get detailed counts of open and active setups
        open_count, active_count = await get_setup_status_counts(setup_monitor, 'NQ.FUT')
        logger.info(f"Loaded {setup_count} trade setups for NQ.FUT (Open: {open_count}, Active: {active_count})")
        
        logger.info("Initializing Databento client...")
        client = db.Live(key=os.getenv("DATABENTO_API_KEY"))
        
        # Add callbacks - use the synchronous wrapper instead of creating tasks directly
        client.add_callback(
            record_callback=handle_record_sync,
            exception_callback=error_callback
        )
        
        # Use the smart symbology NQ.n.0 which always resolves to the front month contract
        # n = highest open interest, 0 = first contract (front month)
        symbol = "NQ.n.0"
        logger.info(f"Subscribing to {symbol} 1-minute OHLCV data using SMART SYMBOLOGY...")
        
        # Start from 10 minutes ago
        start_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        start_ts = int(start_time.timestamp() * 1e9)
        
        try:
            # Subscribe to OHLCV data with stype_in='continuous' for smart symbology
            logger.info("Subscribing to OHLCV schema with SMART SYMBOLOGY...")
            client.subscribe(
                dataset="GLBX.MDP3",
                schema="ohlcv-1m",
                symbols=[symbol],
                stype_in="continuous",  # KEY PARAMETER: Use continuous symbology
                start=start_ts,
            )
            logger.info("Successfully subscribed to OHLCV schema")
        except Exception as e:
            logger.error(f"Error subscribing to OHLCV schema: {e}")
            return  # Exit if we can't subscribe
            
        # Start the periodic task to process minute data
        asyncio.create_task(process_minute_data_cache())
        
        logger.info(f"Starting data stream...")
        try:
            client.start()
            logger.info("Client started successfully")
        except Exception as e:
            logger.error(f"Error starting client: {e}")
            return  # Exit if we can't start the client
        
        # Keep the script running and monitor connection
        count = 0
        last_update = datetime.now()
        try:
            while True:
                await asyncio.sleep(5)
                count += 1
                if count % 3 == 0:  # Every 15 seconds
                    current_time = datetime.now()
                    logger.info(f"Stream active - Last update: {(current_time - last_update).seconds}s ago")
                    logger.info(f"Current minute data cache size: {len(minute_data_cache)}")
                    logger.info(f"Using front month contract: NQ.n.0 (smart symbology)")
                    
                    # Refresh setup monitor periodically 
                    if count % 12 == 0:  # Every 60 seconds
                        try:
                            # Remove db_lock and directly refresh setups
                            logger.info("Refreshing trade setups...")
                            # Ensure DB connection is valid
                            await setup_monitor.db.ensure_connected()
                            
                            # Use NQ.FUT as symbol for trade setups
                            await setup_monitor.refresh_setups('NQ.FUT')
                            setup_count = setup_monitor.get_active_setup_count('NQ.FUT')
                            
                            # Get detailed counts of open and active setups
                            open_count, active_count = await get_setup_status_counts(setup_monitor, 'NQ.FUT')
                            logger.info(f"Now tracking {setup_count} setups for NQ.FUT (Open: {open_count}, Active: {active_count})")
                        except Exception as refresh_error:
                            logger.error(f"Error refreshing setups: {refresh_error}")
                    
                    # If no updates for a long time, try to reconnect
                    if (current_time - last_update).seconds > 300:  # 5 minutes
                        logger.warning("No updates for 5 minutes, attempting to reconnect...")
                        
                        try:
                            # Stop current client gracefully
                            logger.info("Stopping current client...")
                            try:
                                client.stop()
                            except Exception as stop_error:
                                logger.warning(f"Error stopping client: {stop_error}")
                            
                            await asyncio.sleep(3)  # Wait longer for cleanup
                            
                            # Create new client instance 
                            logger.info("Creating new Databento client...")
                            client = db.Live(key=os.getenv("DATABENTO_API_KEY"))
                            
                            # Add callbacks using the main event loop
                            client.add_callback(
                                record_callback=handle_record_sync,
                                exception_callback=error_callback
                            )
                            
                            # Resubscribe to OHLCV with fresh timestamp
                            start_ts = int((datetime.now(timezone.utc) - timedelta(minutes=10)).timestamp() * 1e9)
                            logger.info(f"Resubscribing to OHLCV from timestamp {start_ts}...")
                            
                            client.subscribe(
                                dataset="GLBX.MDP3",
                                schema="ohlcv-1m",
                                symbols=[symbol],
                                stype_in="continuous",  # KEY PARAMETER: Use continuous symbology
                                start=start_ts
                            )
                            
                            # Start the client
                            logger.info("Starting reconnected client...")
                            client.start()
                            
                            # Reset the timer and log success
                            last_update = datetime.now()
                            logger.info("✅ Reconnected successfully - data stream restored")
                            
                        except Exception as reconnect_error:
                            logger.error(f"❌ Failed to reconnect: {reconnect_error}")
                            logger.error("Will retry on next cycle...")
                            # Don't crash - let it try again in the next cycle
                    
        except KeyboardInterrupt:
            logger.info("\nShutting down...")
            client.stop()
            logger.info("Stream stopped")
            
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)
        raise
    finally:
        # Close database connection if it exists
        if 'db_conn' in locals() and db_conn is not None:
            try:
                await db_conn.disconnect()
                logger.info("Database connection closed")
            except Exception as db_error:
                logger.error(f"Error closing database connection: {db_error}")
        
        # Close the connection pool
        if db_pool is not None:
            try:
                await db_pool.close()
                logger.info("Database connection pool closed")
            except Exception as pool_error:
                logger.error(f"Error closing connection pool: {pool_error}")

async def get_setup_status_counts(setup_monitor, symbol):
    """Get counts of open and active setups for a symbol.
    
    Args:
        setup_monitor: SetupMonitor instance
        symbol: Symbol to get counts for
        
    Returns:
        Tuple of (open_count, active_count)
    """
    open_count = 0
    active_count = 0
    
    try:
        if symbol in setup_monitor.active_setups:
            for setup in setup_monitor.active_setups[symbol].values():
                status = setup.get('status')
                if status == 'open':
                    open_count += 1
                elif status == 'active':
                    active_count += 1
    except Exception as e:
        logger.error(f"Error getting setup status counts: {e}")
    
    return open_count, active_count

if __name__ == "__main__":
    # Set explicit event loop policy for consistent behavior
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # Run the main function with a single event loop
    asyncio.run(main())