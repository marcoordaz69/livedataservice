#!/usr/bin/env python3
"""
Level 1 Historical Backfill Service - Fills gaps in Level 1 TBBO data
Similar pattern to OHLCV backfill for data continuity
"""
import databento as db
import asyncio
import os
import sys
import logging
import argparse
from datetime import datetime, timezone, timedelta
import asyncpg
from dotenv import load_dotenv
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "level1_backfill.log")),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Global variables
db_pool = None
records_processed = 0
records_inserted = 0
primary_instruments = {}  # Track the primary instrument ID for each symbol

async def get_db_connection(max_retries=3, retry_delay=2):
    """Get a connection to the database with retry logic."""
    host = os.getenv('host')
    user = os.getenv('user')
    password = os.getenv('password')
    port = os.getenv('port', '5432')
    dbname = os.getenv('dbname', 'postgres')
    
    if not all([host, user, password]):
        raise ValueError("Missing required database environment variables (host, user, password)")
    
    last_error = None
    for attempt in range(max_retries):
        try:
            conn = await asyncpg.connect(
                user=user,
                password=password,
                database=dbname,
                host=host,
                port=int(port),
                ssl='require',  # Supabase requires SSL
                command_timeout=30,  # 30 second timeout
                server_settings={
                    'application_name': 'level1_backfill'
                }
            )
            if attempt > 0:
                logger.info(f"Database connection successful on attempt {attempt + 1}")
            return conn
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                logger.warning(f"Database connection attempt {attempt + 1} failed: {e}")
                logger.info(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"All {max_retries} database connection attempts failed")
    
    raise last_error

async def check_last_level1_timestamp():
    """Check the database for the most recent timestamp in the raw_level1 table."""
    logger.info("Checking for existing Level 1 data in the database...")
    
    try:
        conn = await get_db_connection()
        
        # Query the latest timestamp for NQ symbol (most active)
        query = "SELECT MAX(timestamp) FROM raw_level1 WHERE symbol = 'NQ'"
        latest_timestamp = await conn.fetchval(query)
        await conn.close()
        
        if latest_timestamp:
            # Format timestamp for logging
            latest_timestamp_str = latest_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')
            logger.info(f"Found existing Level 1 data with latest timestamp: {latest_timestamp_str}")
            
            # Return timestamp plus one second to avoid duplication (Level 1 is much higher frequency)
            start_from_timestamp = latest_timestamp + timedelta(seconds=1)
            start_from_timestamp_str = start_from_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"Will start Level 1 backfill from: {start_from_timestamp_str}")
            
            return start_from_timestamp
        else:
            logger.info("No existing Level 1 data found in database, will perform full backfill")
            return None
            
    except Exception as e:
        logger.error(f"Error checking existing Level 1 data: {e}")
        return None

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
        min_size=10,
        max_size=30
    )
    
    logger.info("Level 1 backfill database connection pool created")
    return db_pool

async def insert_batch_level1_data(records_batch):
    """Insert a batch of Level 1 records into database."""
    if not records_batch:
        return 0
    
    pool = await create_db_pool()
    
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Prepare batch insert
                insert_query = """
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
                """
                
                batch_data = []
                for record in records_batch:
                    batch_data.append((
                        record['symbol'],
                        record['timestamp'],
                        str(record['bid_price']),
                        record['bid_size'],
                        str(record['ask_price']),
                        record['ask_size'],
                        record['sequence'],
                        record['ts_recv'],
                        record['ts_event'],
                        record['publisher_id'],
                        record['instrument_id'],
                        record['source_contract']
                    ))
                
                await conn.executemany(insert_query, batch_data)
                return len(batch_data)
                
    except Exception as e:
        logger.error(f"Error inserting batch Level 1 data: {e}")
        raise

def process_historical_tbbo_record(record):
    """Process historical Level 1 TBBO record (MBP1Msg format)."""
    global records_processed, primary_instruments
    records_processed += 1
    
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
        
        # Determine symbol based on price ranges
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
        
        # Track primary instrument
        if normalized_symbol not in primary_instruments:
            primary_instruments[normalized_symbol] = instrument_id
            logger.info(f"Continuous {normalized_symbol} contract instrument: {instrument_id} (price: ${mid_price:.2f})")
        
        # Validate prices for the determined symbol
        if not validate_price_range(normalized_symbol, mid_price):
            return None
        
        # Log progress every 25000 records for backfill
        if records_processed % 25000 == 0:
            spread = ask_price - bid_price
            logger.info(f"Processed {records_processed} records - Latest: {normalized_symbol} ${bid_price:.2f}x{bid_size} / ${ask_price:.2f}x{ask_size} (instrument: {instrument_id})")
        
        return {
            'symbol': normalized_symbol,
            'timestamp': timestamp,
            'bid_price': bid_price,
            'bid_size': bid_size,
            'ask_price': ask_price,
            'ask_size': ask_size,
            'sequence': getattr(record, 'sequence', 0),
            'ts_recv': getattr(record, 'ts_recv', 0),
            'ts_event': getattr(record, 'ts_event', 0),
            'publisher_id': getattr(record, 'publisher_id', 0),
            'instrument_id': instrument_id,
            'source_contract': SYMBOL_CONFIG[normalized_symbol]['databento_symbol']
        }
        
    except Exception as e:
        logger.error(f"Error processing historical TBBO record: {e}")
        return None

async def run_level1_backfill(days=4, start_time=None, end_time=None):
    """Run Level 1 historical backfill process.
    
    Args:
        days: Number of days to backfill (default: 4)
        start_time: Unix timestamp to start from (optional)
        end_time: Unix timestamp to end at (optional)
    """
    global records_inserted, primary_instruments
    
    # Reset primary instruments for each download
    primary_instruments = {}
    
    logger.info("=== STARTING LEVEL 1 BACKFILL SERVICE ===")
    
    try:
        # Determine time range
        if start_time and end_time:
            # Use both provided times
            start_datetime = datetime.fromtimestamp(start_time, tz=timezone.utc)
            end_datetime = datetime.fromtimestamp(end_time, tz=timezone.utc)
            logger.info(f"Backfill from: {start_datetime} to: {end_datetime}")
        elif start_time:
            # Use provided start time with current end time
            start_datetime = datetime.fromtimestamp(start_time, tz=timezone.utc)
            end_datetime = datetime.now(timezone.utc)
            logger.info(f"Backfill from: {start_datetime} to: {end_datetime}")
        else:
            # Use days parameter
            end_datetime = datetime.now(timezone.utc)
            start_datetime = end_datetime - timedelta(days=days)
            logger.info(f"Backfill last {days} days: {start_datetime} to: {end_datetime}")
        
        # Only backfill during trading hours (6:00 AM - 4:00 PM ET)
        # Convert ET to UTC (assuming summer time: UTC-4)
        
        # Initialize database connection pool
        await create_db_pool()
        logger.info("Database connection pool established")
        
        # Initialize Databento client for historical data
        logger.info("Initializing Databento client for historical data...")
        client = db.Historical(key=os.getenv("DATABENTO_API_KEY"))
        
        # Get symbols to download - use continuous contracts
        symbols = ['NQ.c.0', 'ES.c.0']  # Continuous futures contracts
        
        logger.info(f"Downloading TBBO data for symbols: {symbols}")
        logger.info(f"Using continuous contracts to avoid rollover issues")
        
        # Download historical data
        logger.info("Starting Level 1 historical data download...")
        data = client.timeseries.get_range(
            dataset="GLBX.MDP3",
            schema="tbbo",
            start=start_datetime,
            end=end_datetime,
            symbols=symbols,
            stype_in="continuous"  # Use continuous symbology for automatic rollover handling
        )
        
        logger.info("Historical Level 1 data download started...")
        
        # Process records in batches
        batch_size = 2000  # Larger batches for better performance
        batch_records = []
        
        start_processing = time.time()
        
        for record in data:
            # Process the record
            processed_record = process_historical_tbbo_record(record)
            
            if processed_record:
                batch_records.append(processed_record)
                
                # Insert batch when it reaches batch_size
                if len(batch_records) >= batch_size:
                    inserted = await insert_batch_level1_data(batch_records)
                    records_inserted += inserted
                    
                    # Log progress
                    elapsed = time.time() - start_processing
                    rate = records_inserted / elapsed if elapsed > 0 else 0
                    logger.info(f"Inserted {records_inserted} Level 1 records ({rate:.1f} records/sec)")
                    
                    batch_records = []
        
        # Insert remaining records
        if batch_records:
            inserted = await insert_batch_level1_data(batch_records)
            records_inserted += inserted
            
        # Final statistics
        total_time = time.time() - start_processing
        avg_rate = records_inserted / total_time if total_time > 0 else 0
        
        logger.info("=== LEVEL 1 BACKFILL COMPLETE ===")
        logger.info(f"Records processed: {records_processed:,}")
        logger.info(f"Records inserted: {records_inserted:,}")
        logger.info(f"Total time: {total_time:.1f} seconds")
        logger.info(f"Average rate: {avg_rate:.1f} records/sec")
        logger.info("Historical Level 1 backfill completed successfully")
        
        return True
        
    except Exception as e:
        error_msg = str(e)
        if "data_start_after_available_end" in error_msg:
            logger.info("Database is already up-to-date with latest available historical data")
            logger.info("Level 1 backfill not needed - data is current")
            return True
        else:
            logger.error(f"Error during Level 1 backfill: {e}", exc_info=True)
            return False
    finally:
        # Close the connection pool
        if db_pool is not None:
            try:
                await db_pool.close()
                logger.info("Database connection pool closed")
            except Exception as pool_error:
                logger.error(f"Error closing connection pool: {pool_error}")

async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Level 1 Historical Backfill Service")
    parser.add_argument("--days", type=float, default=4, help="Number of days to backfill (default: 4)")
    parser.add_argument("--start-time", type=int, help="Unix timestamp to start backfill from")
    parser.add_argument("--end-time", type=int, help="Unix timestamp to end backfill at")
    parser.add_argument("--backfill-only", action="store_true", help="Run backfill only (for compatibility)")
    args = parser.parse_args()
    
    # Verify environment variables
    required_vars = ["DATABENTO_API_KEY", "host", "user", "password"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        sys.exit(1)
    
    # Check for existing data to determine start time if not provided
    if not args.start_time:
        last_timestamp = await check_last_level1_timestamp()
        if last_timestamp:
            args.start_time = int(last_timestamp.timestamp())
    
    # Run backfill
    success = await run_level1_backfill(days=args.days, start_time=args.start_time, end_time=args.end_time)
    
    if success:
        logger.info("Level 1 backfill service completed successfully")
        sys.exit(0)
    else:
        logger.error("Level 1 backfill service failed")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())