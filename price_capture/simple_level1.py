#!/usr/bin/env python3
"""
Level 1 Data Service - Real-time Top of Book (TBBO) data collection
Replaces OHLCV 1-minute bars with real-time bid/ask updates
"""
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

# Symbol configuration - using continuous contracts to avoid rollover issues
SYMBOL_CONFIG = {
    'NQ': {
        'databento_symbol': 'NQ.c.0',  # Continuous contract (front month)
        'db_symbol': 'NQ',
        'price_range': {'min': 10000, 'max': 50000},
        'description': 'NASDAQ 100 E-mini (Continuous)'
    },
    'ES': {
        'databento_symbol': 'ES.c.0',  # Continuous contract (front month)
        'db_symbol': 'ES',
        'price_range': {'min': 2000, 'max': 10000},
        'description': 'S&P 500 E-mini (Continuous)'
    }
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "simple_level1.log")),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Global variables
db_pool = None
main_loop = None
latest_level1_cache = {}
last_update_time = datetime.now()

def format_price(price):
    """Format the raw price value from nanoseconds to dollars."""
    if isinstance(price, (int, float)):
        return price / 1000000000
    return price

def validate_price_range(symbol, price):
    """Validate if a price is within the expected range for a symbol."""
    if symbol not in SYMBOL_CONFIG:
        logger.warning(f"Unknown symbol {symbol}, allowing price {price}")
        return True
    
    config = SYMBOL_CONFIG[symbol]
    min_price = config['price_range']['min']
    max_price = config['price_range']['max']
    
    if min_price <= price <= max_price:
        return True
    else:
        logger.warning(f"Price {price:.2f} for {symbol} outside expected range {min_price}-{max_price}")
        return False

def determine_symbol_from_price(price):
    """Determine symbol from price range."""
    for symbol, config in SYMBOL_CONFIG.items():
        if validate_price_range(symbol, price):
            return config['db_symbol']
    return 'NQ'  # Default fallback

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
    
    db_pool = await asyncpg.create_pool(
        user=user,
        password=password,
        database=dbname,
        host=host,
        port=int(port),
        ssl='require',
        min_size=2,
        max_size=10
    )
    
    logger.info("Level 1 database connection pool created")
    return db_pool

async def get_db_connection():
    """Get a connection from the pool."""
    start_time = time.time()
    try:
        pool = await create_db_pool()
        conn = await pool.acquire()
        return conn
    except Exception as e:
        total_time = time.time() - start_time
        logger.error(f"Failed to get database connection after {total_time:.3f}s: {e}")
        raise

async def release_db_connection(conn):
    """Release a connection back to the pool."""
    if db_pool is not None:
        try:
            await db_pool.release(conn)
        except Exception as e:
            logger.error(f"Error releasing connection: {e}")

async def insert_level1_data(tbbo_record):
    """Insert Level 1 TBBO record into database."""
    global last_update_time
    
    if not tbbo_record:
        return
    
    conn = None
    try:
        conn = await get_db_connection()
        
        async with conn.transaction():
            await conn.execute("""
                INSERT INTO raw_level1 (
                    symbol, timestamp, bid_price, bid_size, ask_price, ask_size,
                    sequence, ts_recv, ts_event, publisher_id, instrument_id, source_contract
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (symbol, timestamp, sequence) DO UPDATE
                SET bid_price = EXCLUDED.bid_price,
                    bid_size = EXCLUDED.bid_size,
                    ask_price = EXCLUDED.ask_price,
                    ask_size = EXCLUDED.ask_size,
                    ts_recv = EXCLUDED.ts_recv,
                    ts_event = EXCLUDED.ts_event,
                    publisher_id = EXCLUDED.publisher_id,
                    instrument_id = EXCLUDED.instrument_id,
                    source_contract = EXCLUDED.source_contract
            """, 
            tbbo_record['symbol'],
            tbbo_record['timestamp'],
            str(tbbo_record['bid_price']),
            tbbo_record['bid_size'],
            str(tbbo_record['ask_price']),
            tbbo_record['ask_size'],
            tbbo_record['sequence'],
            tbbo_record['ts_recv'],
            tbbo_record['ts_event'],
            tbbo_record['publisher_id'],
            tbbo_record['instrument_id'],
            tbbo_record['source_contract']
            )
            
            # Update cache
            symbol = tbbo_record['symbol']
            latest_level1_cache[symbol] = {
                'timestamp': tbbo_record['timestamp'],
                'bid_price': tbbo_record['bid_price'],
                'bid_size': tbbo_record['bid_size'],
                'ask_price': tbbo_record['ask_price'],
                'ask_size': tbbo_record['ask_size'],
                'mid_price': (tbbo_record['bid_price'] + tbbo_record['ask_price']) / 2,
                'spread': tbbo_record['ask_price'] - tbbo_record['bid_price']
            }
            
            # Update last successful update time
            last_update_time = datetime.now()
            
            logger.debug(f"Level 1 update: {symbol} ${tbbo_record['bid_price']:.2f}x{tbbo_record['bid_size']} / ${tbbo_record['ask_price']:.2f}x{tbbo_record['ask_size']}")
            
    except Exception as e:
        logger.error(f"Error inserting Level 1 data: {e}")
        raise
    finally:
        if conn:
            await release_db_connection(conn)

def process_tbbo_record(record):
    """Process Level 1 TBBO record (MBP1Msg format)."""
    try:
        # Format timestamp
        timestamp = datetime.fromtimestamp(record.ts_event / 1e9, tz=timezone.utc)
        
        # Extract TBBO fields from MBP1Msg levels array
        if not hasattr(record, 'levels') or not record.levels:
            return None
            
        # Get the first level (Level 1 = top of book)
        level = record.levels[0]
        
        # Extract bid/ask data from the BidAskPair (use pretty formatted values)
        bid_price = getattr(level, 'pretty_bid_px', 0)
        bid_size = getattr(level, 'bid_sz', 0)
        ask_price = getattr(level, 'pretty_ask_px', 0)
        ask_size = getattr(level, 'ask_sz', 0)
        
        # Skip if no valid prices
        if bid_price <= 0 or ask_price <= 0:
            return None
        
        # Get instrument ID from record
        instrument_id = getattr(record, 'instrument_id', 0)
        
        # Determine symbol based on price ranges AND use historical instrument knowledge
        mid_price = (bid_price + ask_price) / 2
        
        # Determine symbol based on price ranges (ES is much lower than NQ)
        if 2000 <= mid_price <= 10000:
            normalized_symbol = 'ES'
        elif 10000 <= mid_price <= 50000:
            normalized_symbol = 'NQ'
        else:
            # Log unexpected price and skip
            logger.warning(f"Unexpected price {mid_price:.2f} for instrument {instrument_id} - skipping record")
            return None
        
        # Validate prices for the determined symbol
        if not validate_price_range(normalized_symbol, mid_price):
            return None
        
        # Log periodic updates (every 100th record to avoid spam)
        sequence = getattr(record, 'sequence', 0)
        if sequence % 100 == 0:
            spread = ask_price - bid_price
            logger.info(f"Level 1 {normalized_symbol}: ${bid_price:.2f}x{bid_size} / ${ask_price:.2f}x{ask_size} (spread: ${spread:.2f})")
        
        return {
            'symbol': normalized_symbol,
            'timestamp': timestamp,
            'bid_price': bid_price,
            'bid_size': bid_size,
            'ask_price': ask_price,
            'ask_size': ask_size,
            'sequence': sequence,
            'ts_recv': getattr(record, 'ts_recv', 0),
            'ts_event': getattr(record, 'ts_event', 0),
            'publisher_id': getattr(record, 'publisher_id', 0),
            'instrument_id': instrument_id,
            'source_contract': SYMBOL_CONFIG[normalized_symbol]['databento_symbol']
        }
        
    except Exception as e:
        logger.error(f"Error processing TBBO record: {e}")
        return None

def error_callback(exception):
    """Handle stream errors."""
    logger.error(f"Level 1 Stream error: {exception}")

async def handle_tbbo_record(record):
    """Asynchronous handler for processing TBBO records."""
    start_time = time.time()
    
    try:
        # Only process MBP1Msg records (Level 1 market data)
        if hasattr(record, 'levels') and record.levels:
            tbbo_record = process_tbbo_record(record)
            
            if tbbo_record:
                # Insert data into database
                await insert_level1_data(tbbo_record)
                    
    except Exception as e:
        logger.error(f"Error in handle_tbbo_record: {e}")
    finally:
        total_time = time.time() - start_time
        if total_time > 2.0:
            logger.warning(f"[PERF] handle_tbbo_record took {total_time:.3f}s")

def handle_tbbo_record_sync(record):
    """Synchronous wrapper for handle_tbbo_record."""
    global main_loop
    if main_loop:
        main_loop.call_soon_threadsafe(
            lambda: asyncio.create_task(handle_tbbo_record(record))
        )

async def main():
    """Main function to run Level 1 data collection."""
    global main_loop
    
    main_loop = asyncio.get_running_loop()
    
    # Verify environment variables
    if not all([os.getenv("DATABENTO_API_KEY"),
                os.getenv("host"),
                os.getenv("user"),
                os.getenv("password")]):
        logger.error("Missing required environment variables")
        return

    try:
        logger.info("=== STARTING LEVEL 1 DATA SERVICE ===")
        
        # Initialize database connection pool
        logger.info("Initializing database connection pool...")
        await create_db_pool()
        logger.info("Database connection pool established")
        
        logger.info("Initializing Databento client...")
        client = db.Live(key=os.getenv("DATABENTO_API_KEY"))
        
        # Add callbacks
        client.add_callback(
            record_callback=handle_tbbo_record_sync,
            exception_callback=error_callback
        )
        
        # Get enabled symbols
        enabled_symbols = os.getenv('ENABLED_SYMBOLS', 'NQ').split(',')
        enabled_symbols = [s.strip() for s in enabled_symbols]
        
        # Build list of databento symbols to subscribe to
        databento_symbols = []
        for symbol in enabled_symbols:
            if symbol in SYMBOL_CONFIG:
                databento_symbols.append(SYMBOL_CONFIG[symbol]['databento_symbol'])
                logger.info(f"Added {symbol} -> {SYMBOL_CONFIG[symbol]['databento_symbol']} to Level 1 subscription")
            else:
                logger.warning(f"Unknown symbol {symbol}, skipping")
        
        if not databento_symbols:
            logger.error("No valid symbols to subscribe to for Level 1")
            return
        
        logger.info(f"Subscribing to Level 1 TBBO data for symbols: {databento_symbols}")
        
        # Start from current time for live data
        start_time = datetime.now(timezone.utc)
        start_ts = int(start_time.timestamp() * 1e9)
        
        try:
            # Subscribe to TBBO (Level 1) data
            logger.info(f"Subscribing to TBBO schema for symbols: {databento_symbols}")
            client.subscribe(
                dataset="GLBX.MDP3",     # CME Globex MDP 3.0 dataset
                schema="tbbo",           # Top of Book (Level 1) schema
                symbols=databento_symbols,
                stype_in="continuous",   # Use continuous contracts to avoid rollover issues
                start=start_ts,
            )
            logger.info("Successfully subscribed to TBBO (Level 1) schema")
        except Exception as e:
            logger.error(f"Error subscribing to TBBO schema: {e}")
            return
            
        logger.info("Starting Level 1 data stream...")
        try:
            client.start()
            logger.info("Level 1 client started successfully")
        except Exception as e:
            logger.error(f"Error starting Level 1 client: {e}")
            return
        
        # Keep the script running and monitor connection
        count = 0
        global last_update_time
        try:
            while True:
                await asyncio.sleep(10)
                count += 1
                if count % 6 == 0:  # Every 60 seconds
                    current_time = datetime.now()
                    logger.info(f"Level 1 stream active - monitoring {len(databento_symbols)} symbols")
                    logger.info(f"Last update: {(current_time - last_update_time).seconds}s ago")
                    
                    # Log current market data
                    for symbol in enabled_symbols:
                        if symbol in latest_level1_cache:
                            data = latest_level1_cache[symbol]
                            logger.info(f"  {symbol}: ${data['bid_price']:.2f}x{data['bid_size']} / ${data['ask_price']:.2f}x{data['ask_size']} (mid: ${data['mid_price']:.2f}, spread: ${data['spread']:.2f})")
                    
                    # If no updates for a long time, try to reconnect (reduced from 5 minutes to 2 minutes)
                    if (current_time - last_update_time).seconds > 120:  # 2 minutes
                        logger.warning("No Level 1 updates for 2 minutes, attempting to reconnect...")
                        
                        try:
                            client.stop()
                            await asyncio.sleep(2)
                            client = db.Live(key=os.getenv("DATABENTO_API_KEY"))
                            client.add_callback(
                                record_callback=handle_tbbo_record_sync,
                                exception_callback=error_callback
                            )
                            
                            start_ts = int(datetime.now(timezone.utc).timestamp() * 1e9)
                            client.subscribe(
                                dataset="GLBX.MDP3",
                                schema="tbbo",
                                symbols=databento_symbols,
                                stype_in="continuous",
                                start=start_ts
                            )
                            
                            client.start()
                            last_update_time = datetime.now()
                            logger.info("Level 1 reconnected successfully")
                        except Exception as reconnect_error:
                            logger.error(f"Failed to reconnect Level 1: {reconnect_error}")
                    
        except KeyboardInterrupt:
            logger.info("\nShutting down Level 1 service...")
            client.stop()
            logger.info("Level 1 stream stopped")
            
    except Exception as e:
        logger.error(f"Critical error in Level 1 service: {e}", exc_info=True)
        raise
    finally:
        # Close the connection pool
        if db_pool is not None:
            try:
                await db_pool.close()
                logger.info("Level 1 database connection pool closed")
            except Exception as pool_error:
                logger.error(f"Error closing Level 1 connection pool: {pool_error}")

if __name__ == "__main__":
    # Set explicit event loop policy for consistent behavior
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())