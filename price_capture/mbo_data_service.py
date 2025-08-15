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

# Note: Using asyncpg directly instead of db_factory for MBO service

# MBO symbol configuration for CME Globex futures (matching OHLCV configuration)
MBO_SYMBOL_CONFIG = {
    'NQ': {
        'databento_symbol': 'NQ.n.0',
        'db_symbol': 'NQ',
        'description': 'NASDAQ 100 E-mini Futures'
    },
    'ES': {
        'databento_symbol': 'ES.n.0',
        'db_symbol': 'ES',
        'description': 'S&P 500 E-mini Futures'
    },
    'YM': {
        'databento_symbol': 'YM.n.0',
        'db_symbol': 'YM',
        'description': 'Dow Jones E-mini Futures'
    },
    'RTY': {
        'databento_symbol': 'RTY.n.0',
        'db_symbol': 'RTY',
        'description': 'Russell 2000 E-mini Futures'
    }
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "mbo_data_service.log")),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Global variables
db_pool = None
mbo_cache = {}
last_sequence_by_symbol = {}
instrument_id_to_symbol = {}  # Maps instrument IDs to normalized symbols

def format_price(price):
    """Format the raw price value from nanoseconds to dollars."""
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
    
    logger.info("MBO database connection pool created")
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

async def insert_mbo_data(mbo_record):
    """Insert a single MBO record into the database."""
    if not mbo_record:
        return
    
    conn = None
    try:
        conn = await get_db_connection()
        
        async with conn.transaction():
            await conn.execute("""
                INSERT INTO raw_mbo (
                    symbol, timestamp, sequence, order_id, side, action, 
                    price, size, flags, ts_recv, ts_event, ts_in_delta,
                    publisher_id, instrument_id, channel_id, rtype, source_contract
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                ON CONFLICT (symbol, timestamp, sequence, order_id) DO UPDATE
                SET side = EXCLUDED.side,
                    action = EXCLUDED.action,
                    price = EXCLUDED.price,
                    size = EXCLUDED.size,
                    flags = EXCLUDED.flags,
                    ts_recv = EXCLUDED.ts_recv,
                    ts_event = EXCLUDED.ts_event,
                    ts_in_delta = EXCLUDED.ts_in_delta,
                    publisher_id = EXCLUDED.publisher_id,
                    instrument_id = EXCLUDED.instrument_id,
                    channel_id = EXCLUDED.channel_id,
                    rtype = EXCLUDED.rtype,
                    source_contract = EXCLUDED.source_contract
            """, 
            mbo_record['symbol'],
            mbo_record['timestamp'],
            mbo_record['sequence'],
            mbo_record['order_id'],
            mbo_record['side'],
            mbo_record['action'],
            str(mbo_record['price']),
            mbo_record['size'],
            mbo_record['flags'],
            mbo_record['ts_recv'],
            mbo_record['ts_event'],
            mbo_record['ts_in_delta'],
            mbo_record['publisher_id'],
            mbo_record['instrument_id'],
            mbo_record['channel_id'],
            mbo_record['rtype'],
            mbo_record['source_contract']
            )
            
            logger.debug(f"Inserted MBO record for {mbo_record['symbol']} - {mbo_record['action']} order {mbo_record['order_id']}")
            
    except Exception as e:
        logger.error(f"Error inserting MBO data: {e}")
        raise
    finally:
        if conn:
            await release_db_connection(conn)

def process_mbo_record(record):
    """Process and format an MBO record."""
    try:
        # Format timestamp with UTC timezone
        timestamp = datetime.fromtimestamp(record.ts_event / 1e9, tz=timezone.utc)
        
        # Debug: Log ALL available attributes in the MBO record
        all_attrs = []
        for attr in dir(record):
            if not attr.startswith('_'):
                try:
                    value = getattr(record, attr)
                    if not callable(value):
                        all_attrs.append(f"{attr}={value}")
                except:
                    all_attrs.append(f"{attr}=<error>")
        
        # Log every 100th record to see full structure
        sequence = getattr(record, 'sequence', 0)
        if sequence % 100 == 0:
            logger.info(f"FULL MBO RECORD STRUCTURE: {', '.join(all_attrs)}")
        
        # Try different ways to get the symbol from MBO record
        raw_symbol = None
        possible_symbol_fields = ['symbol', 'instrument_id', 'raw_symbol', 'underlying']
        
        for field in possible_symbol_fields:
            if hasattr(record, field):
                raw_symbol = getattr(record, field)
                logger.debug(f"Found symbol field '{field}': {raw_symbol}")
                break
        
        # Handle instrument_id based symbol mapping
        if raw_symbol and str(raw_symbol).isdigit():
            # This is an instrument ID, check our mapping
            instrument_id = int(raw_symbol)
            if instrument_id in instrument_id_to_symbol:
                normalized_symbol = instrument_id_to_symbol[instrument_id]
                raw_symbol = MBO_SYMBOL_CONFIG[normalized_symbol]['databento_symbol']
                logger.debug(f"Mapped instrument_id {instrument_id} to {normalized_symbol}")
            else:
                # For now, assume any numeric instrument_id for our subscription is NQ
                # This will be updated when we receive the symbol mapping
                normalized_symbol = 'NQ'
                raw_symbol = 'NQ.n.0'
                instrument_id_to_symbol[instrument_id] = normalized_symbol
                logger.info(f"New instrument mapping: {instrument_id} -> {normalized_symbol}")
        elif not raw_symbol or raw_symbol == 'UNKNOWN':
            # For continuous contracts, we know we subscribed to NQ.n.0, so default to NQ
            normalized_symbol = 'NQ'  # Default to NQ since that's what we subscribed to
            raw_symbol = 'NQ.n.0'
            logger.debug(f"Using default symbol mapping: {normalized_symbol}")
        else:
            # Try to map the symbol string
            normalized_symbol = None
            for symbol, config in MBO_SYMBOL_CONFIG.items():
                if config['databento_symbol'] == raw_symbol or symbol == raw_symbol:
                    normalized_symbol = config['db_symbol']
                    break
            
            if not normalized_symbol:
                logger.warning(f"Unknown MBO symbol: {raw_symbol}, skipping")
                return None
        
        # Format price from nanoseconds
        price = format_price(getattr(record, 'price', 0))
        
        # Extract ALL MBO fields
        order_id = getattr(record, 'order_id', 0)
        side = getattr(record, 'side', 'A')  # A=Ask, B=Bid
        action = getattr(record, 'action', 'A')  # A=Add, M=Modify, D=Delete, etc.
        size = getattr(record, 'size', 0)
        sequence = getattr(record, 'sequence', 0)
        flags = getattr(record, 'flags', 0)
        ts_recv = getattr(record, 'ts_recv', 0)
        ts_event = getattr(record, 'ts_event', 0)
        ts_in_delta = getattr(record, 'ts_in_delta', 0)
        publisher_id = getattr(record, 'publisher_id', 0)
        instrument_id = getattr(record, 'instrument_id', 0)
        channel_id = getattr(record, 'channel_id', 0)
        rtype = getattr(record, 'rtype', 160)  # MBO record type
        
        # Check for sequence number continuity
        if normalized_symbol in last_sequence_by_symbol:
            expected_seq = last_sequence_by_symbol[normalized_symbol] + 1
            if sequence != expected_seq and sequence > expected_seq:
                logger.warning(f"Sequence gap detected for {normalized_symbol}: expected {expected_seq}, got {sequence}")
        
        last_sequence_by_symbol[normalized_symbol] = sequence
        
        # Prepare COMPLETE MBO data record
        mbo_record = {
            'symbol': normalized_symbol,
            'timestamp': timestamp,
            'sequence': sequence,
            'order_id': order_id,
            'side': side,
            'action': action,
            'price': price,
            'size': size,
            'flags': flags,
            'ts_recv': ts_recv,
            'ts_event': ts_event,
            'ts_in_delta': ts_in_delta,
            'publisher_id': publisher_id,
            'instrument_id': instrument_id,
            'channel_id': channel_id,
            'rtype': rtype,
            'source_contract': raw_symbol
        }
        
        # Log sample records (every 1000th record to avoid spam)
        if sequence % 1000 == 0:
            logger.info(f"MBO {normalized_symbol}: {action} order {order_id} - {side} ${price:.4f} size {size} (seq: {sequence})")
        
        return mbo_record
                    
    except Exception as e:
        logger.error(f"Error processing MBO record: {e}")
        return None

def error_callback(exception):
    """Handle stream errors."""
    logger.error(f"MBO Stream error: {exception}")

async def handle_mbo_record(record):
    """Asynchronous handler for processing MBO records."""
    start_time = time.time()
    
    try:
        # Only process MBO records
        if hasattr(record, 'order_id') and hasattr(record, 'action'):
            processing_start = time.time()
            mbo_record = process_mbo_record(record)
            processing_time = time.time() - processing_start
            
            if mbo_record:
                # Insert data into database
                db_start = time.time()
                await insert_mbo_data(mbo_record)
                db_time = time.time() - db_start
                
                if db_time > 1.0:
                    logger.warning(f"[PERF] MBO database operation took {db_time:.3f}s")
                    
    except Exception as e:
        logger.error(f"Error in handle_mbo_record: {e}")
    finally:
        total_time = time.time() - start_time
        if total_time > 2.0:
            logger.warning(f"[PERF] handle_mbo_record took {total_time:.3f}s")

# Synchronous wrapper for callback
main_loop = None

def handle_mbo_record_sync(record):
    """Synchronous wrapper for handle_mbo_record."""
    global main_loop
    if main_loop:
        main_loop.call_soon_threadsafe(
            lambda: asyncio.create_task(handle_mbo_record(record))
        )

async def main():
    """Main function to run MBO data collection."""
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
        logger.info("=== STARTING MBO DATA SERVICE ===")
        
        # Initialize database connection pool
        logger.info("Initializing database connection pool...")
        await create_db_pool()
        logger.info("Database connection pool established")
        
        logger.info("Initializing Databento client...")
        client = db.Live(key=os.getenv("DATABENTO_API_KEY"))
        
        # Add callbacks
        client.add_callback(
            record_callback=handle_mbo_record_sync,
            exception_callback=error_callback
        )
        
        # Get enabled symbols for MBO
        enabled_symbols = os.getenv('MBO_SYMBOLS', 'NQ,ES').split(',')
        enabled_symbols = [s.strip() for s in enabled_symbols]
        
        # Build list of databento symbols to subscribe to
        databento_symbols = []
        for symbol in enabled_symbols:
            if symbol in MBO_SYMBOL_CONFIG:
                databento_symbols.append(MBO_SYMBOL_CONFIG[symbol]['databento_symbol'])
                logger.info(f"Added {symbol} -> {MBO_SYMBOL_CONFIG[symbol]['databento_symbol']} to MBO subscription")
            else:
                logger.warning(f"Unknown MBO symbol {symbol}, skipping")
        
        if not databento_symbols:
            logger.error("No valid symbols to subscribe to for MBO")
            return
        
        logger.info(f"Subscribing to MBO data for symbols: {databento_symbols}")
        
        # Start from current time for live data
        start_time = datetime.now(timezone.utc)
        start_ts = int(start_time.timestamp() * 1e9)
        
        try:
            # Subscribe to MBO data from CME Globex dataset (same as OHLCV)
            logger.info(f"Subscribing to MBO schema for symbols: {databento_symbols}")
            client.subscribe(
                dataset="GLBX.MDP3",     # CME Globex MDP 3.0 dataset
                schema="mbo",            # Market By Order schema
                symbols=databento_symbols,
                stype_in="continuous",   # Use continuous symbology (same as OHLCV)
                start=start_ts,
            )
            logger.info("Successfully subscribed to MBO schema")
        except Exception as e:
            logger.error(f"Error subscribing to MBO schema: {e}")
            return
            
        logger.info("Starting MBO data stream...")
        try:
            client.start()
            logger.info("MBO client started successfully")
        except Exception as e:
            logger.error(f"Error starting MBO client: {e}")
            return
        
        # Keep the script running and monitor connection
        count = 0
        last_update = datetime.now()
        try:
            while True:
                await asyncio.sleep(10)
                count += 1
                if count % 6 == 0:  # Every 60 seconds
                    current_time = datetime.now()
                    logger.info(f"MBO stream active - monitoring {len(databento_symbols)} symbols")
                    logger.info(f"Last update: {(current_time - last_update).seconds}s ago")
                    
                    # Log sequence number status
                    for symbol in databento_symbols:
                        if symbol in last_sequence_by_symbol:
                            logger.info(f"  {symbol}: last sequence {last_sequence_by_symbol[symbol]}")
                    
                    # If no updates for a long time, try to reconnect
                    if (current_time - last_update).seconds > 300:  # 5 minutes
                        logger.warning("No MBO updates for 5 minutes, attempting to reconnect...")
                        
                        try:
                            client.stop()
                            await asyncio.sleep(2)
                            client = db.Live(key=os.getenv("DATABENTO_API_KEY"))
                            client.add_callback(
                                record_callback=handle_mbo_record_sync,
                                exception_callback=error_callback
                            )
                            
                            start_ts = int(datetime.now(timezone.utc).timestamp() * 1e9)
                            client.subscribe(
                                dataset="GLBX.MDP3",     # CME Globex MDP 3.0 dataset
                                schema="mbo",
                                symbols=databento_symbols,
                                stype_in="continuous",   # Use continuous symbology
                                start=start_ts
                            )
                            
                            client.start()
                            last_update = datetime.now()
                            logger.info("MBO reconnected successfully")
                        except Exception as reconnect_error:
                            logger.error(f"Failed to reconnect MBO: {reconnect_error}")
                    
        except KeyboardInterrupt:
            logger.info("\nShutting down MBO service...")
            client.stop()
            logger.info("MBO stream stopped")
            
    except Exception as e:
        logger.error(f"Critical error in MBO service: {e}", exc_info=True)
        raise
    finally:
        # Close the connection pool
        if db_pool is not None:
            try:
                await db_pool.close()
                logger.info("MBO database connection pool closed")
            except Exception as pool_error:
                logger.error(f"Error closing MBO connection pool: {pool_error}")

if __name__ == "__main__":
    # Set explicit event loop policy for consistent behavior
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())