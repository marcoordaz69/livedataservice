#!/usr/bin/env python3
import asyncio
import os
import logging
import signal
import sys
import argparse
from datetime import datetime, timedelta, timezone
import databento as db
from dotenv import load_dotenv
import asyncpg
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

# Configure basic logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("live_data_service.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Load API key and database environment variables
load_dotenv()
api_key = os.getenv('DATABENTO_API_KEY')
if not api_key:
    raise ValueError("DATABENTO_API_KEY not found in environment variables")

# Control flags
running = True
reconnect_requested = False

# Price formatting helper
def format_price(price):
    """Format the raw price value to a proper format."""
    if isinstance(price, (int, float)):
        return price / 1000000000
    return price

# Database connection settings
async def get_db_connection():
    """Get a connection to the database."""
    host = os.getenv('host')
    user = os.getenv('user')
    password = os.getenv('password')
    port = os.getenv('port', '5432')
    dbname = os.getenv('dbname', 'postgres')
    
    if not all([host, user, password]):
        raise ValueError("Missing required database environment variables (host, user, password)")
    
    conn = await asyncpg.connect(
        user=user,
        password=password,
        database=dbname,
        host=host,
        port=int(port),
        ssl='require'  # Supabase requires SSL
    )
    return conn



async def load_data_to_db(data_records):
    """Insert the collected data into the database."""
    if not data_records:
        logger.warning("No data records to insert")
        return 0
    
    logger.info(f"=== INSERTING {len(data_records)} RECORDS INTO DATABASE ===")
    
    # Log sample of the data being inserted (first 3 records)
    sample_size = min(3, len(data_records))
    if sample_size > 0:
        logger.info(f"Sample of data being inserted (showing {sample_size} records):")
        for i, record in enumerate(data_records[:sample_size], 1):
            logger.info(f"  RECORD {i}/{sample_size}:")
            logger.info(f"    Symbol:        {record['symbol']}")
            logger.info(f"    Timestamp:     {record['timestamp'].isoformat()} (UTC)")
            logger.info(f"    Open:          ${record['open']:.2f}")
            logger.info(f"    High:          ${record['high']:.2f}")
            logger.info(f"    Low:           ${record['low']:.2f}")
            logger.info(f"    Close:         ${record['close']:.2f}")
            logger.info(f"    Volume:        {record['volume']}")
            logger.info(f"    Source:        {record['source_contract']}")
    
    conn = await get_db_connection()
    
    try:
        # Insert data in batches
        batch_size = 100
        total_inserted = 0
        
        for i in range(0, len(data_records), batch_size):
            batch = data_records[i:i+batch_size]
            
            # Create a transaction for this batch
            async with conn.transaction():
                prepared_stmt = await conn.prepare("""
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
                """)
                
                for record in batch:
                    await prepared_stmt.fetchval(
                        record['symbol'],
                        record['timestamp'],
                        record['open'],
                        record['high'],
                        record['low'],
                        record['close'],
                        record['volume'],
                        record['source_contract']
                    )
            
            total_inserted += len(batch)
            logger.info(f"Inserted batch {i//batch_size + 1}/{(len(data_records)-1)//batch_size + 1} ({len(batch)} records)")
    
        logger.info(f"Successfully inserted {total_inserted} records into database")
        return total_inserted
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        raise
    finally:
        await conn.close()

class ContractTracker:
    """Track and determine the most reliable contract based on volume patterns."""
    
    def __init__(self, lookback_periods=20):
        self.volume_history: Dict[str, List[int]] = {}
        self.price_history: Dict[str, List[float]] = {}
        self.lookback = lookback_periods
        self.primary_contract: Optional[str] = None
        self.primary_contract_avg_volume = 0
        self.last_update = datetime.now()
        self.avg_price_map = {}  # Track average prices for consistency checking

    def add_record(self, instrument_id: str, symbol: str, timestamp: datetime, prices: Tuple[float, ...], volume: int):
        if instrument_id not in self.volume_history:
            self.volume_history[instrument_id] = []
            self.price_history[instrument_id] = []
        
        self.volume_history[instrument_id].append(volume)
        avg_price = sum(prices) / len(prices)
        self.price_history[instrument_id].append(avg_price)
        
        # Track average price for this instrument for consistency checking
        if instrument_id not in self.avg_price_map:
            self.avg_price_map[instrument_id] = []
        
        # Add to price history for consistency checking
        self.avg_price_map[instrument_id].append(avg_price)
        if len(self.avg_price_map[instrument_id]) > 30:  # Keep 30 periods for consistency checks
            self.avg_price_map[instrument_id] = self.avg_price_map[instrument_id][-30:]
        
        # Keep only last N periods for volume tracking
        if len(self.volume_history[instrument_id]) > self.lookback:
            self.volume_history[instrument_id] = self.volume_history[instrument_id][-self.lookback:]
            self.price_history[instrument_id] = self.price_history[instrument_id][-self.lookback:]
        
        self._update_primary_contract()
        self.last_update = datetime.now()

    def _update_primary_contract(self):
        max_avg_volume = 0
        selected_contract = None
        
        for contract_id, volumes in self.volume_history.items():
            if len(volumes) < 3:  # Require minimum data points
                continue
                
            avg_volume = sum(volumes) / len(volumes)
            if avg_volume > max_avg_volume:
                max_avg_volume = avg_volume
                selected_contract = contract_id
        
        if selected_contract:
            if self.primary_contract != selected_contract:
                logger.info(f"Setting primary contract to {selected_contract} with avg volume {max_avg_volume:.1f}")
            self.primary_contract = selected_contract
            self.primary_contract_avg_volume = max_avg_volume

    def is_primary_contract(self, instrument_id: str) -> bool:
        return instrument_id == self.primary_contract
        
    def is_price_consistent(self, price: float, tolerance: float = 0.03) -> bool:
        """Check if a price is consistent with recent history.
        
        Args:
            price: The price to check
            tolerance: Allowed deviation as a percentage (default 3%)
            
        Returns:
            True if price is within tolerance of recent average, False otherwise
        """
        if not self.primary_contract or self.primary_contract not in self.avg_price_map:
            return True  # No history to compare against
            
        # Get price history for primary contract
        price_history = self.avg_price_map.get(self.primary_contract, [])
        if not price_history:
            return True  # No history to compare against
            
        # Calculate average of recent prices
        recent_avg = sum(price_history) / len(price_history)
        
        # Check if price is within tolerance
        deviation = abs(price - recent_avg) / recent_avg
        return deviation <= tolerance

    def get_primary_contract_info(self) -> Tuple[Optional[str], float]:
        return self.primary_contract, self.primary_contract_avg_volume

    def time_since_last_update(self) -> float:
        return (datetime.now() - self.last_update).total_seconds()

async def historical_load(start_timestamp=None):
    """Load historical data using both Live and Historical APIs to support up to 4 days of history.
    
    For data within the last ~24 hours: Uses the Live API
    For older data (up to 4 days): Uses the Historical API
    
    Args:
        start_timestamp: Optional UNIX timestamp to start from
    """
    try:
        # Create live client and historical client
        logger.info("=== INITIALIZING HISTORICAL DATA LOAD ===")
        live_client = db.Live(key=api_key)
        historical_client = db.Historical(key=api_key)
        
        # Dictionary to store symbol mappings
        symbol_mappings = {}
        
        # Dictionary to track highest volume entries by timestamp
        highest_volume_entries = {}
        
        # Initialize contract tracker
        contract_tracker = ContractTracker(lookback_periods=20)
        
        # Calculate end time using UTC
        end_time = datetime.now(timezone.utc)
        
        # Calculate cutoff for Live API (24 hours ago)
        live_api_cutoff = end_time - timedelta(hours=24)
        
        if start_timestamp:
            # Convert UNIX timestamp to datetime
            start_time = datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
            logger.info(f"Using provided start time: {start_time.isoformat()} (UTC)")
        else:
            # Default to 4 days ago
            start_time = end_time - timedelta(days=4)
            logger.info(f"Using default start time (4 days ago): {start_time.isoformat()} (UTC)")
            
        # Ensure we don't go back more than 4 days
        max_lookback = end_time - timedelta(days=4)
        if start_time < max_lookback:
            logger.info(f"Limiting start time to 4 days ago: {max_lookback.isoformat()} (UTC)")
            start_time = max_lookback
        
        # ===== STAGE 1: Historical API for older data =====
        if start_time < live_api_cutoff:
            logger.info(f"===== STAGE 1: HISTORICAL API (from {start_time.isoformat()} to {live_api_cutoff.isoformat()}) =====")
            
            try:
                # Convert to nanoseconds for Databento API
                historical_start_ts = int(start_time.timestamp() * 1e9)
                historical_end_ts = int(live_api_cutoff.timestamp() * 1e9)
                
                logger.info(f"Fetching historical data from {start_time.isoformat()} to {live_api_cutoff.isoformat()} (UTC)")
                
                # Use the historical API to get data
                history = historical_client.timeseries.get_range(
                    dataset="GLBX.MDP3",
                    schema="ohlcv-1m",
                    stype_in="continuous",  # Use continuous symbology like live streaming
                    symbols=["NQ.n.0"],     # Use smart symbology for front month contract
                    start=historical_start_ts,
                    end=historical_end_ts
                )
                
                logger.info(f"Received historical data: {len(history)} records")
                
                # Process the historical records
                for record in history:
                    # Get the record type
                    record_type = record.__class__.__name__
                    
                    # Process OHLCV data
                    if "OHLCV" in record_type:
                        # Convert timestamp to datetime with UTC timezone
                        timestamp = datetime.fromtimestamp(record.ts_event / 1e9, tz=timezone.utc)
                        # Round to the minute for consistent keys
                        minute_key = timestamp.strftime('%Y-%m-%d %H:%M:00')
                        
                        # Get instrument ID and look up symbol
                        instrument_id = getattr(record, 'instrument_id', 'Unknown')
                        symbol = symbol_mappings.get(instrument_id, 'Unknown')
                        volume = getattr(record, 'volume', 0)
                        
                        # Format prices correctly
                        prices = (format_price(getattr(record, 'open', 0)),
                                  format_price(getattr(record, 'high', 0)),
                                  format_price(getattr(record, 'low', 0)),
                                  format_price(getattr(record, 'close', 0)))
                        
                        # Validate NQ price range (should be around 18000-25000)
                        avg_price = sum(prices) / len(prices)
                        if avg_price < 10000 or avg_price > 50000:
                            logger.warning(f"Skipping record with invalid NQ price: {avg_price:.2f} at {timestamp.isoformat()}")
                            continue
                        
                        # Add to contract tracker
                        contract_tracker.add_record(instrument_id, symbol, timestamp, prices, volume)
                        
                        # Track the entry for this timestamp
                        if minute_key not in highest_volume_entries:
                            highest_volume_entries[minute_key] = {
                                'timestamp': timestamp,
                                'symbol': 'NQ',  # Always use NQ as the symbol for consistency
                                'instrument_id': instrument_id,
                                'open': prices[0],
                                'high': prices[1],
                                'low': prices[2],
                                'close': prices[3],
                                'volume': volume,
                                'source_contract': symbol if symbol != 'Unknown' else 'NQ.n.0'
                            }
                        else:
                            existing = highest_volume_entries[minute_key]
                            
                            # Update if this is the primary contract or has higher volume
                            if contract_tracker.is_primary_contract(instrument_id) or volume > existing['volume']:
                                highest_volume_entries[minute_key] = {
                                    'timestamp': timestamp,
                                    'symbol': 'NQ',  # Always use NQ as the symbol for consistency
                                    'instrument_id': instrument_id,
                                    'open': prices[0],
                                    'high': prices[1],
                                    'low': prices[2],
                                    'close': prices[3],
                                    'volume': volume,
                                    'source_contract': symbol if symbol != 'Unknown' else 'NQ.n.0'
                                }
                    
                    # Process symbol mappings
                    elif "SymbolMapping" in record_type:
                        symbol = getattr(record, 'symbol', 'Unknown')
                        instrument_id = getattr(record, 'instrument_id', 'Unknown')
                        symbol_mappings[instrument_id] = symbol
                        logger.info(f"Symbol mapping: {symbol} -> {instrument_id}")
                    
                # Log progress after processing historical data
                logger.info(f"Processed {len(highest_volume_entries)} unique minute bars from historical data")
                
            except Exception as historical_error:
                logger.error(f"Error fetching historical data: {historical_error}")
                logger.warning("Continuing with live data only")
        
        # ===== STAGE 2: Live API for recent data =====
        # Start time for live API is either:
        # 1. The live_api_cutoff time (if we're continuing from historical data)
        # 2. The original start_time (if it's within live API range)
        live_start_time = max(start_time, live_api_cutoff)
        logger.info(f"===== STAGE 2: LIVE API (from {live_start_time.isoformat()} to now) =====")
        
        # Convert to nanoseconds for Databento API
        live_start_ts = int(live_start_time.timestamp() * 1e9)
        
        logger.info(f"Subscribing to NQ.n.0 OHLCV data starting from {live_start_time.isoformat()} (UTC)...")
        live_client.subscribe(
            dataset="GLBX.MDP3",
            schema="ohlcv-1m",
            stype_in="continuous",  # Use continuous symbology like live streaming
            symbols=["NQ.n.0"],     # Use smart symbology for front month contract
            start=live_start_ts
        )
        
        # Process data for a limited time to collect recent history
        logger.info("Starting to collect recent historical data...")
        end_collection_time = datetime.now() + timedelta(minutes=3)  # Give it 3 minutes to collect data
        
        for record in live_client:
            # Check if we've processed enough data
            if datetime.now() > end_collection_time:
                logger.info("Reached collection time limit, stopping historical data collection")
                break
                
            # Get the record type
            record_type = record.__class__.__name__
            
            # Process OHLCV data
            if "OHLCV" in record_type:
                # Convert timestamp to datetime with UTC timezone
                timestamp = datetime.fromtimestamp(record.ts_event / 1e9, tz=timezone.utc)
                # Round to the minute for consistent keys
                minute_key = timestamp.strftime('%Y-%m-%d %H:%M:00')
                
                # Get instrument ID and look up symbol
                instrument_id = getattr(record, 'instrument_id', 'Unknown')
                symbol = symbol_mappings.get(instrument_id, 'Unknown')
                volume = getattr(record, 'volume', 0)
                
                # Format prices correctly
                prices = (format_price(getattr(record, 'open', 0)),
                          format_price(getattr(record, 'high', 0)),
                          format_price(getattr(record, 'low', 0)),
                          format_price(getattr(record, 'close', 0)))
                
                # Validate NQ price range (should be around 18000-25000)
                avg_price = sum(prices) / len(prices)
                if avg_price < 10000 or avg_price > 50000:
                    logger.warning(f"Skipping record with invalid NQ price: {avg_price:.2f} at {timestamp.isoformat()}")
                    continue
                
                # Add to contract tracker
                contract_tracker.add_record(instrument_id, symbol, timestamp, prices, volume)
                
                # Track the entry for this timestamp
                if minute_key not in highest_volume_entries:
                    highest_volume_entries[minute_key] = {
                        'timestamp': timestamp,
                        'symbol': 'NQ',  # Always use NQ as the symbol for consistency
                        'instrument_id': instrument_id,
                        'open': prices[0],
                        'high': prices[1],
                        'low': prices[2],
                        'close': prices[3],
                        'volume': volume,
                        'source_contract': symbol if symbol != 'Unknown' else 'NQ.n.0'
                    }
                else:
                    existing = highest_volume_entries[minute_key]
                    
                    # Update if this is the primary contract or has higher volume and is price-consistent
                    if (contract_tracker.is_primary_contract(instrument_id) or 
                        (volume > existing['volume'] * 1.2)):  # Must be 20% higher volume
                        highest_volume_entries[minute_key] = {
                            'timestamp': timestamp,
                            'symbol': 'NQ',  # Always use NQ as the symbol for consistency
                            'instrument_id': instrument_id,
                            'open': prices[0],
                            'high': prices[1],
                            'low': prices[2],
                            'close': prices[3],
                            'volume': volume,
                            'source_contract': symbol if symbol != 'Unknown' else 'NQ.n.0'
                        }
                
                # Log progress periodically
                if len(highest_volume_entries) % 100 == 0:
                    primary = contract_tracker.get_primary_contract_info()
                    primary_info = f" (Primary: {primary[0]}, Avg Vol: {primary[1]:.1f})" if primary[0] else ""
                    logger.info(f"Collected {len(highest_volume_entries)} unique minute bars{primary_info}")
            
            # Process symbol mappings
            elif "SymbolMapping" in record_type:
                symbol = getattr(record, 'symbol', 'Unknown')
                instrument_id = getattr(record, 'instrument_id', 'Unknown')
                symbol_mappings[instrument_id] = symbol
                logger.info(f"Symbol mapping: {symbol} -> {instrument_id}")
        
        # Convert the collected data to a list for database insertion
        data_records = list(highest_volume_entries.values())
        logger.info(f"Collected {len(data_records)} unique minute bars in total")
        
        # Sort by timestamp for cleaner insertion
        data_records.sort(key=lambda x: x['timestamp'])
        
        # If we have a primary contract, prefer those records
        primary_contract = contract_tracker.get_primary_contract_info()
        if primary_contract:
            logger.info(f"Primary contract identified: {primary_contract[0]} with avg volume {primary_contract[1]:.1f}")
            
            # Additional logging for diagnostics
            contracts_used = {}
            for record in data_records:
                if record.get('instrument_id') not in contracts_used:
                    contracts_used[record.get('instrument_id')] = 0
                contracts_used[record.get('instrument_id')] += 1
            
            for contract_id, count in contracts_used.items():
                percent = (count / len(data_records)) * 100 if data_records else 0
                logger.info(f"Contract {contract_id}: {count} records ({percent:.1f}%)")
        
        return data_records
        
    except Exception as e:
        logger.error(f"Error collecting historical data: {e}")
        raise

async def live_data_stream(start_time: datetime):
    """Run a continuous live data stream that updates minute by minute."""
    global running, reconnect_requested
    
    try:
        # Get the most recent timestamp from the database
        conn = await get_db_connection()
        latest_timestamp = await conn.fetchval(
            "SELECT MAX(timestamp) FROM raw_ohlcv WHERE symbol = 'NQ'"
        )
        await conn.close()
        
        # Calculate start time for live data (use the latest timestamp + 1 minute or now - 5 minutes)
        if latest_timestamp:
            start_time = latest_timestamp + timedelta(minutes=1)
            # If the latest timestamp is more than 10 minutes old, use a more recent start time
            if datetime.now(timezone.utc) - start_time > timedelta(minutes=10):
                start_time = datetime.now(timezone.utc) - timedelta(minutes=5)
        else:
            start_time = datetime.now(timezone.utc) - timedelta(minutes=5)
            
        start_ts = int(start_time.replace(tzinfo=None).timestamp() * 1e9)  # Convert to nanoseconds
        
        logger.info(f"=== STARTING LIVE DATA STREAM FROM {start_time.isoformat()} (UTC) ===")
        logger.info(f"Subscribing to OHLCV-1m data for NQ.FUT starting from {start_time.isoformat()} (UTC)")
        client = db.Live(key=api_key)
        
        # Dictionary to store symbol mappings
        symbol_mappings = {}
        
        # Initialize the contract tracker
        contract_tracker = ContractTracker(lookback_periods=15)
        
        # Dictionary to track per-minute data
        current_minute_data = {}
        last_processed_minute = None
        
        # Subscribe to live data
        client.subscribe(
            dataset="GLBX.MDP3",
            schema="ohlcv-1m",
            stype_in="parent",
            symbols=["NQ.FUT"],
            start=start_ts
        )
        
        # Set up a heartbeat task to check for activity
        heartbeat_time = datetime.now()
        last_update_time = datetime.now()
        updates_received = 0
        
        # Start time for logging intervals
        start_loop_time = datetime.now()
        
        # Create an iterator from the client
        client_iter = iter(client)
        
        # Process live data indefinitely
        while running:
            if reconnect_requested:
                logger.info("Reconnect requested, restarting live data stream...")
                reconnect_requested = False
                break
                
            # Heartbeat check - log status every 30 seconds
            current_time = datetime.now()
            if (current_time - heartbeat_time).total_seconds() > 30:
                elapsed_seconds = (current_time - start_loop_time).total_seconds()
                logger.info(f"Status after {elapsed_seconds:.1f} seconds: "
                            f"Received {updates_received} OHLCV updates, "
                            f"Last update: {(current_time - last_update_time).total_seconds():.1f} seconds ago")
                
                # Check if we haven't received updates in over 2 minutes and request reconnection
                if updates_received > 0 and (current_time - last_update_time).total_seconds() > 120:
                    logger.warning("No updates received in over 2 minutes, requesting reconnection...")
                    reconnect_requested = True
                    break
                
                # If we haven't received any updates after 3 minutes since startup, try reconnecting
                if updates_received == 0 and (current_time - start_loop_time).total_seconds() > 180:
                    logger.warning("No OHLCV updates received after 3 minutes, requesting reconnection...")
                    reconnect_requested = True
                    break
                
                heartbeat_time = current_time
            
            # Check if we need to flush any pending data
            current_minute_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:00')
            for minute_key in list(current_minute_data.keys()):
                # If this is a past minute (not the current one), we should insert it
                if minute_key != current_minute_str and minute_key != last_processed_minute:
                    records_to_insert = [current_minute_data[minute_key]]
                    
                    # Log what we're inserting
                    logger.info(f"Flushing stale data for {minute_key}:")
                    for record in records_to_insert:
                        logger.info(f"  Flushing record for {record['timestamp'].isoformat()} (UTC):")
                        logger.info(f"    Symbol: {record['symbol']}")
                        logger.info(f"    OHLC: ${record['open']:.2f}/${record['high']:.2f}/${record['low']:.2f}/${record['close']:.2f}")
                        logger.info(f"    Volume: {record['volume']}")
                        logger.info(f"    Source: {record['source_contract']}")
                    
                    # Insert into the database
                    await load_data_to_db(records_to_insert)
                    
                    # Remove from current data to avoid reprocessing
                    del current_minute_data[minute_key]
                    
                    # Update last processed minute
                    last_processed_minute = minute_key
            
            try:
                # Use a non-blocking approach with a timeout
                # Wait for up to 1 second to get the next record
                try:
                    record = await asyncio.wait_for(asyncio.to_thread(next, client_iter, None), timeout=1.0)
                except asyncio.TimeoutError:
                    # No record available within timeout, continue with next iteration
                    continue
                
                # Check if we got a valid record
                if record is None:
                    logger.warning("Received None record from client iterator")
                    continue
                
                # Get the record type
                record_type = record.__class__.__name__
                
                # Process OHLCV data
                if "OHLCV" in record_type:
                    updates_received += 1
                    last_update_time = datetime.now()
                    
                    # Convert timestamp to datetime with UTC timezone
                    timestamp = datetime.fromtimestamp(record.ts_event / 1e9, tz=timezone.utc)
                    
                    # Get the minute key for consistent tracking
                    minute_key = timestamp.strftime('%Y-%m-%d %H:%M:00')
                    
                    # Get instrument ID and look up symbol
                    instrument_id = getattr(record, 'instrument_id', 'Unknown')
                    symbol = symbol_mappings.get(instrument_id, 'Unknown')
                    volume = getattr(record, 'volume', 0)
                    
                    # Format prices correctly
                    prices = (format_price(getattr(record, 'open', 0)),
                              format_price(getattr(record, 'high', 0)),
                              format_price(getattr(record, 'low', 0)),
                              format_price(getattr(record, 'close', 0)))
                    
                    # Add to contract tracker
                    contract_tracker.add_record(instrument_id, symbol, timestamp, prices, volume)
                    
                    # Log the update
                    logger.info(f"OHLCV Update at {timestamp.isoformat()} (UTC):")
                    logger.info(f"  Open:   ${prices[0]:.2f}")
                    logger.info(f"  High:   ${prices[1]:.2f}")
                    logger.info(f"  Low:    ${prices[2]:.2f}")
                    logger.info(f"  Close:  ${prices[3]:.2f}")
                    logger.info(f"  Volume: {volume}")
                    logger.info(f"  ID:     {instrument_id}")
                    logger.info(f"  Symbol: {symbol}")
                    
                    # Check if this is the primary contract or has consistent price
                    is_primary = contract_tracker.is_primary_contract(instrument_id)
                    is_consistent = contract_tracker.is_price_consistent(prices[3])
                    
                    # Only update if this is primary contract or significantly higher volume with consistent price
                    update_record = is_primary or (
                        minute_key in current_minute_data and 
                        volume > current_minute_data[minute_key]['volume'] * 1.2 and
                        is_consistent
                    )
                    
                    if minute_key not in current_minute_data or update_record:
                        # Store/update data for this minute
                        current_minute_data[minute_key] = {
                            'timestamp': timestamp,
                            'symbol': 'NQ',  # Always use NQ as the symbol for consistency
                            'instrument_id': instrument_id,
                            'open': prices[0],
                            'high': prices[1],
                            'low': prices[2],
                            'close': prices[3],
                            'volume': volume,
                            'source_contract': symbol,
                            'is_primary': is_primary
                        }
                        
                        if is_primary:
                            logger.info(f"Highest volume entry for {minute_key} updated (PRIMARY contract): Volume={volume}")
                        else:
                            logger.info(f"Highest volume entry for {minute_key} updated: Volume={volume}")
                        
                        if not is_consistent:
                            logger.warning(f"Price ${prices[3]:.2f} may be inconsistent with recent history")
                    
                    # If we've moved to a new minute, store the previous minute's data
                    current_minute = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:00')
                    if last_processed_minute and last_processed_minute != minute_key and minute_key != current_minute:
                        # Get data for completed minutes
                        completed_minutes = []
                        for key in list(current_minute_data.keys()):
                            if key != current_minute:
                                completed_minutes.append(current_minute_data[key])
                                # Remove after adding to avoid reprocessing
                                del current_minute_data[key]
                        
                        if completed_minutes:
                            # Log what we're inserting
                            logger.info(f"Inserting {len(completed_minutes)} complete records into database:")
                            for record in completed_minutes:
                                logger.info(f"  Complete record for {record['timestamp'].isoformat()} (UTC):")
                                logger.info(f"    Symbol: {record['symbol']}")
                                logger.info(f"    Open:   ${record['open']:.2f}")
                                logger.info(f"    High:   ${record['high']:.2f}")
                                logger.info(f"    Low:    ${record['low']:.2f}")
                                logger.info(f"    Close:  ${record['close']:.2f}")
                                logger.info(f"    Volume: {record['volume']}")
                                logger.info(f"    Source: {record['source_contract']}")
                                if record.get('is_primary'):
                                    logger.info(f"    Status: PRIMARY contract")
                            
                            # Store these completed minutes in the database
                            await load_data_to_db(completed_minutes)
                        
                        # Update the last processed minute
                        last_processed_minute = minute_key
                        
                    # Initialize last_processed_minute if this is our first record
                    if last_processed_minute is None:
                        last_processed_minute = minute_key
                
                # Process symbol mappings
                elif "SymbolMapping" in record_type:
                    symbol = getattr(record, 'symbol', 'Unknown')
                    instrument_id = getattr(record, 'instrument_id', 'Unknown')
                    symbol_mappings[instrument_id] = symbol
                    logger.info(f"Symbol mapping: {symbol} -> {instrument_id}")
                
                # Process system messages
                elif "SystemMsg" in record_type:
                    msg_type = getattr(record, 'type', 'Unknown')
                    msg_text = getattr(record, 'text', 'No message text')
                    logger.info(f"SystemMsg: Type={msg_type}, Text={msg_text}")
            
            except StopIteration:
                logger.info("Client iterator stopped, reconnecting...")
                reconnect_requested = True
                break
            except Exception as e:
                logger.error(f"Error processing record: {e}", exc_info=True)
                # Continue with the next record
                
        logger.info("Live data stream stopped")
        
    except Exception as e:
        logger.error(f"Error in live data stream: {e}", exc_info=True)
        # Request reconnection if there was an error
        reconnect_requested = True
        await asyncio.sleep(5)  # Wait a bit before reconnecting
        
    finally:
        # Ensure we attempt to reconnect
        if running and not reconnect_requested:
            reconnect_requested = True

def signal_handler(sig, frame):
    """Handle Ctrl+C and other termination signals."""
    global running
    logger.info("Shutdown signal received, stopping live data service...")
    running = False

async def check_db_status():
    """Check the database status and print summary information."""
    try:
        conn = await get_db_connection()
        
        # Get count of records
        count = await conn.fetchval("SELECT COUNT(*) FROM raw_ohlcv WHERE symbol = 'NQ'")
        
        # Get time range
        range_result = await conn.fetchrow("""
            SELECT 
                MIN(timestamp) AS min_time,
                MAX(timestamp) AS max_time
            FROM raw_ohlcv 
            WHERE symbol = 'NQ'
        """)
        
        min_time = range_result['min_time'] if range_result else None
        max_time = range_result['max_time'] if range_result else None
        
        logger.info("=== DATABASE STATUS ===")
        logger.info(f"Total NQ records: {count}")
        
        if min_time and max_time:
            logger.info(f"Time range: {min_time} to {max_time} (UTC)")
            
            # Get most recent records to check price consistency
            recent_records = await conn.fetch("""
                SELECT timestamp AT TIME ZONE 'UTC' as ts_utc, open, high, low, close, volume, source_contract
                FROM raw_ohlcv
                WHERE symbol = 'NQ'
                ORDER BY timestamp DESC
                LIMIT 5
            """)
            
            if recent_records:
                logger.info("Most recent records:")
                for i, record in enumerate(recent_records, 1):
                    logger.info(f"  RECORD {i}/5:")
                    logger.info(f"    Timestamp: {record['ts_utc']}")
                    logger.info(f"    OHLC: ${record['open']:.2f}/${record['high']:.2f}/${record['low']:.2f}/${record['close']:.2f}")
                    logger.info(f"    Volume: {record['volume']}")
                    logger.info(f"    Contract: {record['source_contract']}")
                    
                # Check for price anomalies
                prices = [record['close'] for record in recent_records]
                if prices and max(prices) / min(prices) > 1.03:  # 3% difference
                    logger.warning("⚠️ PRICE ANOMALY DETECTED: Recent prices vary by more than 3%")
                    logger.warning(f"  Recent prices: {', '.join([f'${p:.2f}' for p in prices])}")
        else:
            logger.info("No records found in database")
            
        await conn.close()
    except Exception as e:
        logger.error(f"Error checking database status: {e}")

async def main():
    """Main function that runs the live data service."""
    global running, reconnect_requested
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Live market data service")
    parser.add_argument("--backfill-only", action="store_true", help="Run only historical backfill")
    parser.add_argument("--start-time", type=int, help="Start timestamp for historical backfill (UNIX timestamp)")
    parser.add_argument("--days", type=int, default=4, help="Number of days to backfill (default: 4)")
    args = parser.parse_args()
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Check database status before starting
        logger.info("Checking database status before starting...")
        await check_db_status()
        
        # Calculate start time based on days parameter if no explicit start time provided
        if args.start_time is None and args.days != 4:  # If days is specified but not start_time
            now = datetime.now(timezone.utc)
            start_time = now - timedelta(days=args.days)
            args.start_time = int(start_time.timestamp())
            logger.info(f"Calculated start time based on {args.days} days lookback: {start_time.isoformat()} (UTC)")
        
        # Load historical data first
        logger.info(f"Starting historical data load (lookback period: {args.days} days)...")
        historical_data = await historical_load(args.start_time)
        
        # Insert historical data
        if historical_data:
            records_inserted = await load_data_to_db(historical_data)
            logger.info(f"Inserted {records_inserted} historical records")
            
        # If backfill-only mode, exit here
        if args.backfill_only:
            logger.info("Historical backfill completed")
            return
        
        # Now start the live data stream in a loop to handle reconnections
        logger.info("Starting live data stream mode...")
        
        while running:
            reconnect_requested = False
            await live_data_stream(datetime.now(timezone.utc))
            
            if running and reconnect_requested:
                logger.info("Reconnecting to live data stream in 5 seconds...")
                await asyncio.sleep(5)  # Wait before reconnecting
            else:
                break
                
        # Final database status check
        logger.info("Checking final database status...")
        await check_db_status()
        
        logger.info("Live data service shutdown complete")
        
    except Exception as e:
        logger.error(f"Critical error in main process: {e}", exc_info=True)
        raise
    finally:
        running = False

if __name__ == "__main__":
    logger.info("=== STARTING LIVE DATA SERVICE ===")
    asyncio.run(main()) 