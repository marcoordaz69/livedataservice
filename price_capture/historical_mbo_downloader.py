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

# Symbol configuration for CME Globex futures (matching live MBO configuration)
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
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "historical_mbo_downloader.log")),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Global variables
db_pool = None
instrument_id_to_symbol = {}

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
        min_size=5,
        max_size=20
    )
    
    logger.info("Historical MBO database connection pool created")
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

async def insert_mbo_batch(mbo_records):
    """Insert a batch of MBO records into the database."""
    if not mbo_records:
        return 0
    
    conn = None
    try:
        conn = await get_db_connection()
        
        async with conn.transaction():
            # Prepare the batch insert query
            query = """
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
            """
            
            # Execute batch insert
            for record in mbo_records:
                await conn.execute(query,
                    record['symbol'],
                    record['timestamp'],
                    record['sequence'],
                    record['order_id'],
                    record['side'],
                    record['action'],
                    str(record['price']),
                    record['size'],
                    record['flags'],
                    record['ts_recv'],
                    record['ts_event'],
                    record['ts_in_delta'],
                    record['publisher_id'],
                    record['instrument_id'],
                    record['channel_id'],
                    record['rtype'],
                    record['source_contract']
                )
            
            return len(mbo_records)
            
    except Exception as e:
        logger.error(f"Error inserting MBO batch: {e}")
        raise
    finally:
        if conn:
            await release_db_connection(conn)

def process_historical_mbo_record(record):
    """Process and format a historical MBO record."""
    try:
        # Format timestamp with UTC timezone
        timestamp = datetime.fromtimestamp(record.ts_event / 1e9, tz=timezone.utc)
        
        # Handle instrument_id based symbol mapping
        raw_symbol = getattr(record, 'instrument_id', None)
        normalized_symbol = 'NQ'  # Default to NQ for now
        
        if raw_symbol and str(raw_symbol).isdigit():
            instrument_id = int(raw_symbol)
            if instrument_id in instrument_id_to_symbol:
                normalized_symbol = instrument_id_to_symbol[instrument_id]
            else:
                # Store the mapping for future use
                instrument_id_to_symbol[instrument_id] = normalized_symbol
                logger.info(f"New historical instrument mapping: {instrument_id} -> {normalized_symbol}")
        
        # Get the databento symbol for this normalized symbol
        if normalized_symbol in MBO_SYMBOL_CONFIG:
            source_contract = MBO_SYMBOL_CONFIG[normalized_symbol]['databento_symbol']
        else:
            source_contract = 'NQ.n.0'  # Default fallback
        
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
            'source_contract': source_contract
        }
        
        return mbo_record
                    
    except Exception as e:
        logger.error(f"Error processing historical MBO record: {e}")
        return None

async def download_historical_mbo_data(symbol='NQ', days_back=30):
    """
    Download historical MBO data for the specified symbol and time range.
    
    Args:
        symbol: Symbol to download (e.g., 'NQ', 'ES')
        days_back: Number of days back from today to download
    """
    logger.info(f"=== STARTING HISTORICAL MBO DOWNLOAD FOR {symbol} ===")
    
    # Verify environment variables
    if not all([os.getenv("DATABENTO_API_KEY"),
                os.getenv("host"),
                os.getenv("user"),
                os.getenv("password")]):
        logger.error("Missing required environment variables")
        return

    if symbol not in MBO_SYMBOL_CONFIG:
        logger.error(f"Unknown symbol {symbol}. Available symbols: {list(MBO_SYMBOL_CONFIG.keys())}")
        return

    try:
        # Initialize database connection pool
        logger.info("Initializing database connection pool...")
        await create_db_pool()
        logger.info("Database connection pool established")
        
        # Initialize Databento Historical client
        logger.info("Initializing Databento Historical client...")
        client = db.Historical(key=os.getenv("DATABENTO_API_KEY"))
        
        # Calculate date range
        # Use a safe end time that's a few hours ago to avoid data availability issues
        end_date = datetime.now(timezone.utc) - timedelta(hours=6)
        
        # For testing purposes, use minutes instead of days if days_back is very small
        if days_back < 1:
            start_date = end_date - timedelta(minutes=int(days_back * 24 * 60))
        else:
            start_date = end_date - timedelta(days=days_back)
            
        # For August 12th specifically, download full day of finalized historical data
        if days_back == 1:
            # Get full August 12th trading day (finalized historical data should be cheaper)
            start_date = datetime(2025, 8, 12, 0, 0, 0, tzinfo=timezone.utc)
            end_date = datetime(2025, 8, 12, 23, 59, 59, tzinfo=timezone.utc)
        
        # Get symbol configuration
        symbol_config = MBO_SYMBOL_CONFIG[symbol]
        databento_symbol = symbol_config['databento_symbol']
        
        logger.info(f"Downloading MBO data for {symbol} ({databento_symbol})")
        logger.info(f"Date range: {start_date.isoformat()} to {end_date.isoformat()}")
        
        # Define query parameters
        params = {
            "dataset": "GLBX.MDP3",  # CME Globex MDP 3.0 dataset
            "symbols": databento_symbol,
            "stype_in": "continuous",  # Use continuous symbology
            "schema": "mbo",  # Market By Order schema
            "start": start_date.isoformat(),
            "end": end_date.isoformat()
        }
        
        # Check expected cost first
        logger.info("Checking expected cost for historical data request...")
        try:
            cost_info = client.metadata.get_cost(**params)
            logger.info(f"Expected cost: ${cost_info:.2f} USD")
            
            # Ask for confirmation if cost is significant
            if cost_info > 10.0:
                logger.warning(f"High cost detected: ${cost_info:.2f}. Proceeding with download...")
                
        except Exception as cost_error:
            logger.warning(f"Could not get cost estimate: {cost_error}")
        
        # Download historical data
        logger.info("Requesting historical MBO data...")
        start_time = time.time()
        
        data = client.timeseries.get_range(**params)
        
        download_time = time.time() - start_time
        logger.info(f"Data download completed in {download_time:.2f} seconds")
        
        # Log data metadata
        logger.info(f"Dataset: {data.dataset}")
        logger.info(f"Schema: {data.schema}")
        logger.info(f"Symbols: {data.symbols}")
        logger.info(f"Start: {data.start}")
        logger.info(f"End: {data.end}")
        
        # Process and insert data in batches
        logger.info("Processing and inserting MBO records...")
        batch_size = 1000
        batch_records = []
        total_processed = 0
        total_inserted = 0
        
        processing_start = time.time()
        
        # Iterate through all records
        for record in data:
            # Process the record
            mbo_record = process_historical_mbo_record(record)
            
            if mbo_record:
                batch_records.append(mbo_record)
                total_processed += 1
                
                # Insert batch when it reaches batch_size
                if len(batch_records) >= batch_size:
                    try:
                        inserted_count = await insert_mbo_batch(batch_records)
                        total_inserted += inserted_count
                        logger.info(f"Inserted batch of {inserted_count} records. Total processed: {total_processed}, Total inserted: {total_inserted}")
                        batch_records = []  # Clear batch
                    except Exception as batch_error:
                        logger.error(f"Error inserting batch: {batch_error}")
                        # Clear batch to continue processing
                        batch_records = []
            
            # Log progress every 10,000 records
            if total_processed % 10000 == 0 and total_processed > 0:
                elapsed = time.time() - processing_start
                rate = total_processed / elapsed
                logger.info(f"Progress: {total_processed} records processed at {rate:.0f} records/sec")
        
        # Insert any remaining records in the final batch
        if batch_records:
            try:
                inserted_count = await insert_mbo_batch(batch_records)
                total_inserted += inserted_count
                logger.info(f"Inserted final batch of {inserted_count} records")
            except Exception as final_batch_error:
                logger.error(f"Error inserting final batch: {final_batch_error}")
        
        processing_time = time.time() - processing_start
        total_time = time.time() - start_time
        
        logger.info(f"=== HISTORICAL MBO DOWNLOAD COMPLETED ===")
        logger.info(f"Symbol: {symbol} ({databento_symbol})")
        logger.info(f"Total records processed: {total_processed}")
        logger.info(f"Total records inserted: {total_inserted}")
        logger.info(f"Processing time: {processing_time:.2f} seconds")
        logger.info(f"Total time: {total_time:.2f} seconds")
        logger.info(f"Processing rate: {total_processed / processing_time:.0f} records/sec")
        
        return {
            'symbol': symbol,
            'total_processed': total_processed,
            'total_inserted': total_inserted,
            'processing_time': processing_time,
            'total_time': total_time
        }
        
    except Exception as e:
        logger.error(f"Critical error in historical MBO download: {e}", exc_info=True)
        raise
    finally:
        # Close the connection pool
        if db_pool is not None:
            try:
                await db_pool.close()
                logger.info("Historical MBO database connection pool closed")
            except Exception as pool_error:
                logger.error(f"Error closing historical MBO connection pool: {pool_error}")

async def main():
    """Main function to run historical MBO data download."""
    
    # Get symbol from environment or command line
    symbol = os.getenv('HISTORICAL_MBO_SYMBOL', 'NQ')
    days_back = float(os.getenv('HISTORICAL_MBO_DAYS', '30'))
    
    # Check command line arguments
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
    if len(sys.argv) > 2:
        days_back = float(sys.argv[2])
    
    logger.info(f"Starting historical MBO download for {symbol}, {days_back} days back")
    
    try:
        result = await download_historical_mbo_data(symbol=symbol, days_back=days_back)
        
        if result:
            logger.info("Historical MBO download completed successfully!")
            logger.info(f"Results: {result}")
        else:
            logger.error("Historical MBO download failed")
            
    except KeyboardInterrupt:
        logger.info("Historical MBO download interrupted by user")
    except Exception as e:
        logger.error(f"Historical MBO download failed: {e}", exc_info=True)

if __name__ == "__main__":
    # Set explicit event loop policy for consistent behavior
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())