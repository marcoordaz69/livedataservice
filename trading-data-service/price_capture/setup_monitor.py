# setup_monitor.py
from decimal import Decimal
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List, Any, Tuple
from dataclasses import dataclass
from uuid import UUID
from database.utils import format_display_price
import os
import json
from pathlib import Path
import time

# Create a directory for trade trigger logs
LOGS_DIR = Path("logs/trade_triggers")
LOGS_DIR.mkdir(parents=True, exist_ok=True)

@dataclass
class SetupCheck:
    """Results of checking a close price against a setup"""
    setup_id: UUID
    symbol: str
    status: str
    close_price: Decimal  # Changed from price to close_price
    matched: bool
    message: str

class SetupMonitor:
    """Monitors OHLCV close price updates against trading setups"""
    def __init__(self, db):
        self.db = db
        self.active_setups: Dict[str, Dict[str, Dict]] = {}
        self.last_refresh: Dict[str, float] = {}
        self.refresh_interval = 60  # Refresh setups every 60 seconds
        self.pending_trades: set[UUID] = set()  # Track setups being processed
        
        # Cache for setup check results
        self.check_results_cache = {}
        self.cache_expiry = 5  # Cache expires after 5 seconds
        self.last_cache_time = 0
        
    def _safe_decimal(self, value: Any) -> Optional[Decimal]:
        """Safely convert a value to Decimal"""
        try:
            return Decimal(str(value)) if value is not None else None
        except Exception as e:
            logging.error(f"Error converting to Decimal: {value} - {e}")
            return None

    def _safe_uuid(self, value: Any) -> Optional[UUID]:
        """Safely convert a value to UUID"""
        try:
            return UUID(str(value)) if value is not None else None
        except Exception as e:
            logging.error(f"Error converting to UUID: {value} - {e}")
            return None

    async def refresh_setups(self, symbol: str) -> None:
        """Refresh setups for a symbol from database
        
        Args:
            symbol: Trading symbol to refresh setups for
        """
        try:
            # Ensure DB connection is valid before proceeding
            await self.db.ensure_connected()
            
            # First, expire old setups that are over 24 hours old
            await self.expire_old_setups(symbol)
            
            # Get fresh setups from database
            setups = await self.db.get_symbol_setups(symbol)
            
            # Index setups by ID for quick lookup
            self.active_setups[symbol] = {
                str(setup['id']): setup for setup in setups
            }
            
            self.last_refresh[symbol] = datetime.now().timestamp()
            
            logging.info(f"Refreshed {len(setups)} setups for {symbol}")
            
        except Exception as e:
            logging.error(f"Failed to refresh setups for {symbol}: {e}")
            raise

    async def expire_old_setups(self, symbol: str) -> None:
        """Mark setups older than 24 hours as closed (expired)
        
        Args:
            symbol: Trading symbol to expire setups for
        """
        try:
            # Ensure DB connection is valid
            await self.db.ensure_connected()
            
            # Log current time for reference
            current_time = datetime.now(timezone.utc)
            cutoff_time = current_time - timedelta(hours=24)
            logging.info(f"Expiring setups older than {cutoff_time.isoformat()} (current time: {current_time.isoformat()})")
            
            # Query to find and update old setups
            async with self.db.pool.acquire() as conn:
                # First check how many total setups there are for the symbol
                total_setups = await conn.fetchval("""
                    SELECT COUNT(*) FROM trade_setups WHERE symbol = $1
                """, symbol)
                
                logging.info(f"Total setups for {symbol}: {total_setups}")
                
                # Now check how many are old
                old_setups = await conn.fetch("""
                    SELECT id, status, setup_time, created_at
                    FROM trade_setups
                    WHERE 
                        symbol = $1 AND
                        status IN ('open', 'active') AND
                        (
                            (setup_time IS NOT NULL AND setup_time < NOW() - INTERVAL '24 hours') OR
                            (setup_time IS NULL AND created_at < NOW() - INTERVAL '24 hours')
                        )
                """, symbol)
                
                if old_setups:
                    logging.info(f"Found {len(old_setups)} setups older than 24 hours to expire")
                    
                    # Log sample of old setups
                    for i, setup in enumerate(old_setups[:5]):  # Log first 5 only
                        setup_time = setup['setup_time'].isoformat() if setup['setup_time'] else "None"
                        created_at = setup['created_at'].isoformat() if setup['created_at'] else "None" 
                        logging.info(f"  Old Setup {i+1}: ID={setup['id']}, Status={setup['status']}, Setup Time={setup_time}, Created={created_at}")
                    
                    # Update each setup to 'closed' status instead of 'expired'
                    updated_count = 0
                    for setup in old_setups:
                        setup_id = setup['id']
                        old_status = setup['status']
                        
                        # For active setups, we need to close any open trades first
                        if old_status == 'active':
                            try:
                                # Get the active trade
                                active_trade = await self.db.get_active_trade(setup_id)
                                
                                if active_trade:
                                    # Record a manual exit if there's an open trade
                                    await self.db.record_trade_exit(
                                        trade_id=active_trade['id'],
                                        exit_price=float(active_trade['entry_price']),  # Use entry price for minimal PnL impact
                                        exit_time=datetime.now(timezone.utc)
                                    )
                                    logging.info(f"Closed active trade for expired setup {setup_id}")
                            except Exception as e:
                                logging.error(f"Error closing trade for expired setup {setup_id}: {e}")
                        
                        try:
                            # Update the setup status to 'closed' (allowed value in constraint)
                            result = await conn.execute("""
                                UPDATE trade_setups
                                SET 
                                    status = 'closed',
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = $1
                            """, setup_id)
                            
                            updated_count += 1
                            logging.info(f"Closed old setup {setup_id} (previous status: {old_status})")
                        except Exception as update_error:
                            logging.error(f"Error updating setup {setup_id} to closed: {update_error}")
                    
                    logging.info(f"✅ Closed {updated_count}/{len(old_setups)} old setups for {symbol}")
                    
                    # Verify the change
                    remaining_old = await conn.fetchval("""
                        SELECT COUNT(*)
                        FROM trade_setups
                        WHERE 
                            symbol = $1 AND
                            status IN ('open', 'active') AND
                            (
                                (setup_time IS NOT NULL AND setup_time < NOW() - INTERVAL '24 hours') OR
                                (setup_time IS NULL AND created_at < NOW() - INTERVAL '24 hours')
                            )
                    """, symbol)
                    
                    logging.info(f"Remaining old setups after closing: {remaining_old}")
                else:
                    logging.info(f"No old setups to expire for {symbol}")
        
        except Exception as e:
            logging.error(f"Error expiring old setups: {e}")
            # Don't raise the exception - we want the refresh to continue even if expiration fails

    def _needs_refresh(self, symbol: str) -> bool:
        """Check if setups for a symbol need to be refreshed"""
        now = datetime.now().timestamp()
        if symbol not in self.last_refresh:
            return True
        return now - self.last_refresh[symbol] > self.refresh_interval
    
    def get_active_setup_count(self, symbol: str) -> int:
        """Get count of active setups for a symbol"""
        return len(self.active_setups.get(symbol, {}))

    async def get_latest_ohlcv(self, symbol: str) -> Optional[Dict]:
        """Get the most recent OHLCV data for a symbol from the database
        
        Args:
            symbol: The trading symbol (e.g., 'NQ')
            
        Returns:
            Dictionary with the latest OHLCV data or None if not found
        """
        try:
            # Ensure DB connection is valid
            await self.db.ensure_connected()
            
            # Query to get the latest OHLCV record for the symbol
            query = """
                SELECT timestamp, open, high, low, close, volume, source_contract 
                FROM raw_ohlcv 
                WHERE symbol = $1 
                ORDER BY timestamp DESC 
                LIMIT 1
            """
            
            async with self.db.pool.acquire() as conn:
                row = await conn.fetchrow(query, symbol)
                
            if not row:
                logging.warning(f"No OHLCV data found for symbol {symbol}")
                return None
                
            # Convert row to dictionary
            return {
                'timestamp': row['timestamp'],
                'open': self._safe_decimal(row['open']),
                'high': self._safe_decimal(row['high']),
                'low': self._safe_decimal(row['low']),
                'close': self._safe_decimal(row['close']),
                'volume': row['volume'],
                'source_contract': row['source_contract']
            }
        
        except Exception as e:
            logging.error(f"Error fetching latest OHLCV data: {e}", exc_info=True)
            return None

    def _is_in_range(self, setup: Dict, high_price: Decimal, low_price: Decimal) -> bool:
        """Quickly check if a setup is potentially triggerable within the current price range
        
        This is a fast pre-filter to avoid doing expensive checks on setups that are 
        nowhere near the current price range.
        """
        try:
            status = setup.get('status')
            
            # For open setups, check if price range overlaps entry zone
            if status == 'open':
                entry_zone_high = self._safe_decimal(setup.get('entry_zone_high', setup.get('entry_high')))
                entry_zone_low = self._safe_decimal(setup.get('entry_zone_low', setup.get('entry_low')))
                
                # Entry zone buffer (20% of the zone width or at least 10 points)
                if entry_zone_high and entry_zone_low:
                    zone_width = abs(entry_zone_high - entry_zone_low)
                    buffer = max(zone_width * Decimal('0.2'), Decimal('10'))
                    
                    # Expand entry zone by buffer
                    expanded_high = entry_zone_high + buffer
                    expanded_low = entry_zone_low - buffer
                    
                    # Check if current price range overlaps expanded entry zone
                    return (
                        (low_price <= expanded_high and low_price >= expanded_low) or
                        (high_price <= expanded_high and high_price >= expanded_low) or
                        (low_price <= expanded_low and high_price >= expanded_high)
                    )
            
            # For active setups, always check (they're already in a trade)
            elif status == 'active':
                return True
                
            return False
        except Exception as e:
            logging.error(f"Error in _is_in_range: {e}")
            return True  # Default to checking the setup if we can't determine range

    async def check_price(self, symbol: str, close_price: Decimal, high_price: Decimal = None, low_price: Decimal = None) -> List[SetupCheck]:
        """Check if price range triggers any setup conditions
        
        Args:
            symbol: Trading symbol
            close_price: Current bar's closing price
            high_price: Current bar's high price (optional)
            low_price: Current bar's low price (optional)
            
        Returns:
            List of SetupCheck results
        """
        import time
        start_time = time.time()
        
        # Check if we need to refresh setups
        if self._needs_refresh(symbol):
            refresh_start = time.time()
            await self.refresh_setups(symbol)
            refresh_time = time.time() - refresh_start
            if refresh_time > 2.0:
                logging.warning(f"[PERF] Refreshing setups took {refresh_time:.3f}s")
        
        # Generate a cache key based on price inputs
        cache_key = f"{symbol}:{close_price}:{high_price}:{low_price}"
        current_time = time.time()
        
        # Check if we have a valid cached result
        if (cache_key in self.check_results_cache and 
            current_time - self.check_results_cache[cache_key]['time'] < self.cache_expiry):
            
            cached_results = self.check_results_cache[cache_key]['results']
            logging.info(f"Using cached results for {cache_key} (age: {current_time - self.check_results_cache[cache_key]['time']:.1f}s)")
            return cached_results
            
        results: List[SetupCheck] = []
        processed_setups = set()  # Track which setups we've handled
        
        # Get the latest OHLCV data from the database to ensure we're using the most up-to-date information
        # Use the normalized symbol 'NQ' for database queries
        db_symbol = 'NQ' if symbol.startswith('NQ') else symbol
        
        ohlcv_start = time.time()
        latest_ohlcv = await self.get_latest_ohlcv(db_symbol)
        ohlcv_time = time.time() - ohlcv_start
        if ohlcv_time > 1.0:
            logging.warning(f"[PERF] get_latest_ohlcv took {ohlcv_time:.3f}s")
        
        if latest_ohlcv:
            # Override provided prices with the latest data from the database
            db_high = latest_ohlcv['high']
            db_low = latest_ohlcv['low']
            db_close = latest_ohlcv['close']
            
            # Update the price range if the database values are more recent
            high_price = db_high
            low_price = db_low
            close_price = db_close
            
            logging.info(f"Using latest OHLCV data from database for validation (timestamp: {latest_ohlcv['timestamp'].isoformat()} UTC)")
            logging.info(f"Latest price range: ${float(db_low):.2f} (Low) to ${float(db_high):.2f} (High), Close: ${float(db_close):.2f}")
        else:
            logging.warning(f"No latest OHLCV data found in database, using provided values")
        
        # Log the price range we're checking
        if high_price is not None and low_price is not None:
            logging.info(f"Checking setups against price range: {format_display_price(low_price)}-{format_display_price(high_price)} (close: {format_display_price(close_price)})")
        else:
            logging.info(f"Checking setups against close price: {format_display_price(close_price)}")
        
        setups = self.active_setups.get(symbol, {})
        setup_count = len(setups)
        filtered_count = 0
        processed_count = 0
        
        # First, filter setups by price range to avoid checking all of them
        relevant_setups = {}
        for setup_id, setup in setups.items():
            # Pre-filter setups that are not in the price range
            if high_price is not None and low_price is not None:
                if self._is_in_range(setup, high_price, low_price):
                    relevant_setups[setup_id] = setup
                else:
                    filtered_count += 1
            else:
                # If no price range provided, include all setups
                relevant_setups[setup_id] = setup
        
        logging.info(f"Filtered out {filtered_count}/{setup_count} setups based on price range")
        
        # Now process only the relevant setups
        for setup_id, setup in relevant_setups.items():
            setup_start = time.time()
            try:
                # Skip if we've already processed this setup for this price
                if setup_id in processed_setups:
                    continue
                
                # Only process if status is either 'open' (for entries) or 'active' (for exits)
                status = setup.get('status')
                if status not in ('open', 'active'):
                    logging.debug(f"Skipping setup {setup_id} with status: {status}")
                    continue
                
                result = await self._check_single_setup(setup, symbol, close_price, high_price, low_price)
                if result:
                    results.append(result)
                    processed_setups.add(setup_id)
                    logging.debug(f"Processed setup {setup_id} for price range")
                
                processed_count += 1
                setup_time = time.time() - setup_start
                if setup_time > 1.0:
                    logging.warning(f"[PERF] Processing setup {setup_id} took {setup_time:.3f}s (status: {status})")
                
            except Exception as e:
                logging.error(f"Error checking setup {setup_id}: {e}", exc_info=True)
            
        total_time = time.time() - start_time
        if total_time > 2.0 or processed_count > 10:
            logging.warning(f"[PERF] check_price processed {processed_count}/{setup_count} setups in {total_time:.3f}s")
        
        # Cache the results for future use
        self.check_results_cache[cache_key] = {
            'time': current_time,
            'results': results
        }
        
        # Clear old cache entries periodically
        if current_time - self.last_cache_time > 60:  # Clear every minute
            self.last_cache_time = current_time
            old_keys = [k for k, v in self.check_results_cache.items() 
                      if current_time - v['time'] > self.cache_expiry]
            for k in old_keys:
                del self.check_results_cache[k]
            
        return results

    async def _check_single_setup(self, setup: Dict, symbol: str, close_price: Decimal, high_price: Decimal = None, low_price: Decimal = None) -> Optional[SetupCheck]:
        """Check a single setup against a price range"""
        import time
        start_time = time.time()
        
        try:
            status = setup.get('status')
            
            # Check if the setup is open (waiting for entry)
            if status == 'open':
                entry_start = time.time()
                result = await self._check_entry(setup, symbol, close_price, high_price, low_price)
                entry_time = time.time() - entry_start
                if entry_time > 0.5:
                    logging.warning(f"[PERF] _check_entry took {entry_time:.3f}s for setup {setup.get('id')}")
                return result
                
            # Check if the setup is active (waiting for exit)
            elif status == 'active':
                exit_start = time.time()
                result = await self._check_exit(setup, symbol, close_price, high_price, low_price)
                exit_time = time.time() - exit_start
                if exit_time > 0.5:
                    logging.warning(f"[PERF] _check_exit took {exit_time:.3f}s for setup {setup.get('id')}")
                return result
                
            # Otherwise, skip the setup
            else:
                logging.debug(f"Setup {setup.get('id')} has status {status}, skipping")
                return None
                
        except Exception as e:
            logging.error(f"Error in _check_single_setup: {e}", exc_info=True)
            return None
        finally:
            total_time = time.time() - start_time
            if total_time > 1.0:
                logging.warning(f"[PERF] _check_single_setup took {total_time:.3f}s for setup {setup.get('id')}")

    def log_trade_trigger(self, setup_id: UUID, trade_id: UUID, setup: Dict, 
                         trigger_type: str, entry_price: Decimal, 
                         close_price: Decimal, high_price: Optional[Decimal] = None, 
                         low_price: Optional[Decimal] = None) -> None:
        """Log detailed information about a trade trigger to a file
        
        Args:
            setup_id: The UUID of the setup
            trade_id: The UUID of the trade
            setup: The trade setup dictionary
            trigger_type: Type of trigger (range_overlap or close_price)
            entry_price: The price at which the trade was entered
            close_price: The close price of the current bar
            high_price: Optional high price of the current bar
            low_price: Optional low price of the current bar
        """
        try:
            # Create a timestamp for the log file name
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            log_file = LOGS_DIR / f"trade_trigger_{timestamp}_{setup_id}_{trade_id}.json"
            
            # Create detailed log data
            log_data = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "setup_id": str(setup_id),
                "trade_id": str(trade_id),
                "symbol": setup.get('symbol', 'Unknown'),
                "trigger_type": trigger_type,
                "entry_price": float(entry_price),
                "close_price": float(close_price),
                "high_price": float(high_price) if high_price is not None else None,
                "low_price": float(low_price) if low_price is not None else None,
                "entry_zone_low": float(setup.get('entry_zone_low', 0)),
                "entry_zone_high": float(setup.get('entry_zone_high', 0)),
                "take_profit": float(setup.get('take_profit', 0)),
                "stop_loss": float(setup.get('stop_loss', 0)),
                "direction": setup.get('direction', 'Unknown'),
                "price_range_overlap": None,
                "in_entry_zone": None,
                "setup_data": {k: str(v) if isinstance(v, (UUID, Decimal)) else v 
                               for k, v in setup.items() if k not in ('id', 'entry_zone_low', 'entry_zone_high', 'take_profit', 'stop_loss', 'direction')}
            }
            
            # Add trigger-specific details
            if trigger_type == 'range_overlap':
                entry_low = self._safe_decimal(setup.get('entry_zone_low'))
                entry_high = self._safe_decimal(setup.get('entry_zone_high'))
                log_data["price_range_overlap"] = bool(low_price <= entry_high and high_price >= entry_low)
                log_data["overlap_details"] = {
                    "entry_zone": f"{float(entry_low)}-{float(entry_high)}",
                    "price_range": f"{float(low_price)}-{float(high_price)}",
                    "overlap_exists": bool(low_price <= entry_high and high_price >= entry_low),
                    "overlap_low": float(max(low_price, entry_low)) if low_price <= entry_high and high_price >= entry_low else None,
                    "overlap_high": float(min(high_price, entry_high)) if low_price <= entry_high and high_price >= entry_low else None
                }
            elif trigger_type == 'close_price':
                entry_low = self._safe_decimal(setup.get('entry_zone_low'))
                entry_high = self._safe_decimal(setup.get('entry_zone_high'))
                log_data["in_entry_zone"] = bool(entry_low <= close_price <= entry_high)
            
            # Write to log file
            with open(log_file, 'w') as f:
                json.dump(log_data, f, indent=2)
                
            logging.info(f"Trade trigger details logged to {log_file}")
            
        except Exception as e:
            logging.error(f"Error logging trade trigger details: {e}", exc_info=True)

    async def _check_entry(self, setup: Dict, symbol: str, close_price: Decimal, high_price: Decimal = None, low_price: Decimal = None) -> Optional[SetupCheck]:
        """Check if price range triggers setup entry conditions
        
        Args:
            setup: Trade setup dictionary
            symbol: Trading symbol
            close_price: Current bar's closing price
            high_price: Current bar's high price (optional)
            low_price: Current bar's low price (optional)
            
        Returns:
            SetupCheck if entry triggered, None otherwise
        """
        try:
            setup_id = self._safe_uuid(setup.get('id'))
            
            # Skip if already being processed
            if setup_id in self.pending_trades:
                logging.debug(f"Setup {setup_id} is already being processed")
                return None
            
            # Get entry zone bounds
            entry_low = self._safe_decimal(setup.get('entry_zone_low'))
            entry_high = self._safe_decimal(setup.get('entry_zone_high'))
            
            # Get or infer trade direction
            direction = setup.get('direction')
            take_profit = self._safe_decimal(setup.get('take_profit'))
            stop_loss = self._safe_decimal(setup.get('stop_loss'))
            
            # Infer direction if not explicitly set
            if not direction and take_profit is not None and stop_loss is not None:
                direction = 'long' if take_profit > stop_loss else 'short'
                logging.info(f"Inferred {direction} direction for setup {setup_id}")
            
            # Validate entry zone
            if not all([entry_low, entry_high]):
                logging.error(f"Invalid entry zone for setup {setup_id}: {entry_low}-{entry_high}")
                return None
            
            # If high and low prices are provided, use them for entry validation
            if high_price is not None and low_price is not None:
                # Price range overlaps with entry zone when:
                # 1. The bar's low is below entry high AND the bar's high is above entry low
                # (meaning some part of the price action was within the entry zone)
                price_range_overlaps_entry = (low_price <= entry_high and high_price >= entry_low)
                
                logging.info(f"Price range check for setup {setup_id}:")
                logging.info(f"  - Entry zone: {format_display_price(entry_low)} to {format_display_price(entry_high)}")
                logging.info(f"  - Price range: {format_display_price(low_price)} (Low) to {format_display_price(high_price)} (High), Close: {format_display_price(close_price)}")
                logging.info(f"  - Direction: {direction or 'Unknown'}")
                logging.info(f"  - Range overlaps entry zone: {price_range_overlaps_entry}")
                
                if price_range_overlaps_entry:
                    try:
                        # Determine best entry price based on direction
                        if direction == 'long':
                            # For long, use lowest price in entry zone (best entry)
                            entry_price = max(low_price, entry_low)
                        else:  # direction == 'short' or None
                            # For short, use highest price in entry zone (best entry)
                            entry_price = min(high_price, entry_high)
                            
                        # If still no direction, use the standard check
                        if not direction:
                            # If entry price is in the entry zone, use it
                            if entry_low <= close_price <= entry_high:
                                entry_price = close_price
                            else:
                                # Otherwise, use mid-point of overlapping range
                                overlap_low = max(low_price, entry_low)
                                overlap_high = min(high_price, entry_high)
                                entry_price = (overlap_low + overlap_high) / Decimal('2.0')
                            
                        # Mark setup as pending before processing
                        self.pending_trades.add(setup_id)
                        
                        # Ensure DB connection is valid before recording trade
                        await self.db.ensure_connected()
                        
                        # Record trade entry using determined entry price
                        trade_id = await self.db.record_trade_from_setup(
                            setup_id=setup_id,
                            entry_price=float(entry_price)
                        )
                        
                        # Verify database status was updated correctly
                        await self._verify_status_update(setup_id, 'active')
                        
                        # Update local state
                        setup['status'] = 'active'
                        setup['current_price'] = close_price
                        
                        # Log detailed trade trigger information
                        self.log_trade_trigger(
                            setup_id=setup_id,
                            trade_id=trade_id,
                            setup=setup,
                            trigger_type='range_overlap',
                            entry_price=entry_price,
                            close_price=close_price,
                            high_price=high_price,
                            low_price=low_price
                        )
                        
                        # Remove from pending trades
                        self.pending_trades.discard(setup_id)
                        
                        logging.info(
                            f"Entry triggered for setup {setup_id}:\n"
                            f"- Trade ID: {trade_id}\n"
                            f"- Direction: {direction or 'Unknown'}\n"
                            f"- Entry Price: {format_display_price(entry_price)}\n"
                            f"- Zone: {format_display_price(entry_low)}-{format_display_price(entry_high)}\n"
                            f"- Bar Range: {format_display_price(low_price)}-{format_display_price(high_price)}"
                        )
                        
                        return SetupCheck(
                            setup_id=setup_id,
                            symbol=symbol,
                            status='active',
                            close_price=entry_price,  # Use actual entry price
                            matched=True,
                            message=f"Entry triggered at {format_display_price(entry_price)} (range: {format_display_price(low_price)}-{format_display_price(high_price)})"
                        )
                        
                    except Exception as e:
                        # Ensure setup is removed from pending on error
                        self.pending_trades.discard(setup_id)
                        logging.error(f"Error processing entry for setup {setup_id}: {e}", exc_info=True)
                        raise
                
            else:
                # Fall back to the original close-price-only check
                logging.info(f"Price check for setup {setup_id} (close price only):")
                logging.info(f"  - Entry zone: {format_display_price(entry_low)} to {format_display_price(entry_high)}")
                logging.info(f"  - Close price: {format_display_price(close_price)}")
                logging.info(f"  - In entry zone: {entry_low <= close_price <= entry_high}")
                
                # Check if close price is in entry zone
                if entry_low <= close_price <= entry_high:
                    try:
                        # Mark setup as pending before processing
                        self.pending_trades.add(setup_id)
                        
                        # Ensure DB connection is valid before recording trade
                        await self.db.ensure_connected()
                        
                        # Record trade entry using close price
                        trade_id = await self.db.record_trade_from_setup(
                            setup_id=setup_id,
                            entry_price=float(close_price)
                        )
                        
                        # Update local state
                        setup['status'] = 'active'
                        setup['current_price'] = close_price
                        
                        # Log detailed trade trigger information
                        self.log_trade_trigger(
                            setup_id=setup_id,
                            trade_id=trade_id,
                            setup=setup,
                            trigger_type='close_price',
                            entry_price=close_price,
                            close_price=close_price
                        )
                        
                        # Remove from pending trades
                        self.pending_trades.discard(setup_id)
                        
                        logging.info(
                            f"Entry triggered for setup {setup_id}:\n"
                            f"- Trade ID: {trade_id}\n"
                            f"- Close Price: {format_display_price(close_price)}\n"
                            f"- Zone: {format_display_price(entry_low)}-{format_display_price(entry_high)}"
                        )
                        
                        return SetupCheck(
                            setup_id=setup_id,
                            symbol=symbol,
                            status='active',
                            close_price=close_price,
                            matched=True,
                            message=f"Entry triggered at {format_display_price(close_price)}"
                        )
                        
                    except Exception as e:
                        # Ensure setup is removed from pending on error
                        self.pending_trades.discard(setup_id)
                        logging.error(f"Error processing entry for setup {setup_id}: {e}", exc_info=True)
                        raise
                
        except Exception as e:
            logging.error(f"Error checking entry conditions: {e}", exc_info=True)
            
        return None

    async def _check_exit(self, setup: Dict, symbol: str, close_price: Decimal, high_price: Decimal = None, low_price: Decimal = None) -> Optional[SetupCheck]:
        """Check if price range triggers setup exit conditions
        
        Args:
            setup: Trade setup dictionary
            symbol: Trading symbol
            close_price: Current bar's closing price
            high_price: Current bar's high price (optional)
            low_price: Current bar's low price (optional)
        """
        try:
            setup_id = self._safe_uuid(setup.get('id'))
            take_profit = self._safe_decimal(setup.get('take_profit'))
            stop_loss = self._safe_decimal(setup.get('stop_loss'))
            direction = setup.get('direction')

            # Infer and update direction if not set
            if not direction:
                direction = 'long' if take_profit > stop_loss else 'short'
                logging.info(f"Inferred {direction} direction for setup {setup_id} (TP: {take_profit}, SL: {stop_loss})")
                
                async with self.db.pool.acquire() as conn:
                    await conn.execute("""
                        UPDATE trade_setups 
                        SET direction = $2,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = $1
                    """, setup_id, direction)
                
                setup['direction'] = direction

            if not all([setup_id, take_profit, stop_loss, direction]):
                logging.error(
                    f"Missing required values for exit check:\n"
                    f"ID: {setup_id}, TP: {take_profit}, SL: {stop_loss}, Direction: {direction}"
                )
                return None

            # Variables to determine if targets were hit
            tp_hit = False
            sl_hit = False
            
            # If high and low prices are provided, use them for more accurate exit validation
            if high_price is not None and low_price is not None:
                # For long positions:
                # - Take profit is hit if the high price reached or exceeded take profit
                # - Stop loss is hit if the low price reached or fell below stop loss
                if direction == 'long':
                    tp_hit = high_price >= take_profit
                    sl_hit = low_price <= stop_loss
                # For short positions:
                # - Take profit is hit if the low price reached or fell below take profit
                # - Stop loss is hit if the high price reached or exceeded stop loss
                else:  # direction == 'short'
                    tp_hit = low_price <= take_profit
                    sl_hit = high_price >= stop_loss
                
                logging.debug(
                    f"Range exit check for {setup_id}:\n"
                    f"- Direction: {direction}\n"
                    f"- Take Profit: {format_display_price(take_profit)} (Hit: {tp_hit})\n"
                    f"- Stop Loss: {format_display_price(stop_loss)} (Hit: {sl_hit})\n"
                    f"- Price Range: {format_display_price(low_price)}-{format_display_price(high_price)}"
                )
            else:
                # Fall back to using close price only
                tp_hit = (close_price >= take_profit) if direction == 'long' else (close_price <= take_profit)
                sl_hit = (close_price <= stop_loss) if direction == 'long' else (close_price >= stop_loss)

            if tp_hit or sl_hit:
                try:
                    # Ensure DB connection is valid before checking trade
                    await self.db.ensure_connected()
                    
                    active_trade = await self.db.get_active_trade(setup_id)
                    if not active_trade:
                        logging.error(f"No active trade found for setup {setup_id}")
                        return None

                    entry_price = self._safe_decimal(active_trade.get('entry_price'))
                    if not entry_price:
                        logging.error(f"Invalid entry price for trade {active_trade['id']}")
                        return None

                    # Determine exit price based on what was hit
                    is_long = direction == 'long'
                    
                    if high_price is not None and low_price is not None:
                        # Use the exact target price that was hit for more accurate PnL
                        if tp_hit:
                            exit_price = take_profit
                            outcome = 'profit'
                            message = f"Take profit hit at {format_display_price(take_profit)} (range: {format_display_price(low_price)}-{format_display_price(high_price)})"
                        else:  # sl_hit
                            exit_price = stop_loss
                            outcome = 'loss'
                            message = f"Stop loss hit at {format_display_price(stop_loss)} (range: {format_display_price(low_price)}-{format_display_price(high_price)})"
                    else:
                        # Validate outcome based on entry price, exit price, and levels using close price
                        if (is_long and close_price >= take_profit) or (not is_long and close_price <= take_profit):
                            outcome = 'profit'
                            exit_price = close_price
                            message = f"Take profit hit at {format_display_price(close_price)}"
                        elif (is_long and close_price <= stop_loss) or (not is_long and close_price >= stop_loss):
                            outcome = 'loss'
                            exit_price = close_price
                            message = f"Stop loss hit at {format_display_price(close_price)}"
                        else:
                            outcome = 'manual'
                            exit_price = close_price
                            message = f"Manual exit at {format_display_price(close_price)}"

                    # Record exit and update status
                    await self.db.ensure_connected()
                    await self.db.record_trade_exit(
                        trade_id=active_trade['id'],
                        exit_price=float(exit_price),
                        exit_time=datetime.now(timezone.utc)
                    )

                    # Verify database status was updated correctly
                    await self._verify_status_update(setup_id, 'closed')

                    # Update local state
                    setup['status'] = 'closed'
                    setup['current_price'] = close_price
                    setup['exit_price'] = exit_price
                    setup['outcome'] = outcome

                    logging.info(
                        f"Trade exit for setup {setup_id}:\n"
                        f"- Direction: {direction}\n"
                        f"- Entry: {format_display_price(entry_price)}\n"
                        f"- Exit: {format_display_price(exit_price)}\n"
                        f"- Outcome: {outcome}\n"
                        f"- Price Range: {format_display_price(low_price) if low_price else 'N/A'}-{format_display_price(high_price) if high_price else 'N/A'}"
                    )

                    return SetupCheck(
                        setup_id=setup_id,
                        symbol=symbol,
                        status='closed',
                        close_price=exit_price,
                        matched=True,
                        message=message
                    )

                except Exception as e:
                    logging.error(f"Error processing exit for setup {setup_id}: {e}", exc_info=True)
                    
        except Exception as e:
            logging.error(f"Error checking exit conditions: {e}", exc_info=True)
            
        return None

    async def _verify_status_update(self, setup_id: UUID, expected_status: str) -> bool:
        """Verify that a setup's status was updated correctly in the database
        
        Args:
            setup_id: UUID of the setup to verify
            expected_status: Expected status ('active', 'closed', etc.)
            
        Returns:
            True if status matches expected, False otherwise
        """
        try:
            await self.db.ensure_connected()
            async with self.db.pool.acquire() as conn:
                actual_status = await conn.fetchval("""
                    SELECT status FROM trade_setups WHERE id = $1
                """, setup_id)
                
                if actual_status == expected_status:
                    logging.debug(f"✅ Status verification passed: Setup {setup_id} status = '{actual_status}'")
                    return True
                else:
                    logging.error(
                        f"❌ Status verification FAILED: Setup {setup_id}\n"
                        f"   Expected: '{expected_status}'\n"
                        f"   Actual: '{actual_status}'\n"
                        f"   This indicates a potential database update issue!"
                    )
                    
                    # Attempt to fix the status inconsistency
                    if expected_status in ('active', 'closed'):
                        logging.info(f"🔧 Attempting to fix status inconsistency for setup {setup_id}")
                        await conn.execute("""
                            UPDATE trade_setups 
                            SET status = $2, updated_at = CURRENT_TIMESTAMP 
                            WHERE id = $1
                        """, setup_id, expected_status)
                        
                        # Verify the fix
                        fixed_status = await conn.fetchval("""
                            SELECT status FROM trade_setups WHERE id = $1
                        """, setup_id)
                        
                        if fixed_status == expected_status:
                            logging.info(f"✅ Status fix successful: Setup {setup_id} now has status '{fixed_status}'")
                            return True
                        else:
                            logging.error(f"❌ Status fix FAILED: Setup {setup_id} still has status '{fixed_status}'")
                            return False
                    else:
                        return False
                        
        except Exception as e:
            logging.error(f"Error verifying status update for setup {setup_id}: {e}", exc_info=True)
            return False

    async def verify_and_fix_status_consistency(self, symbol: str) -> Dict[str, int]:
        """Verify and fix status consistency for all setups of a symbol
        
        This method performs a comprehensive check of status consistency and attempts
        to fix any inconsistencies found. It should be called periodically.
        
        Args:
            symbol: Trading symbol to check
            
        Returns:
            Dictionary with counts of issues found and fixed
        """
        results = {
            'total_checked': 0,
            'inconsistencies_found': 0,
            'inconsistencies_fixed': 0,
            'orphaned_trades': 0,
            'missing_trades': 0
        }
        
        try:
            logging.info(f"🔍 Starting status consistency check for {symbol}")
            await self.db.ensure_connected()
            
            async with self.db.pool.acquire() as conn:
                # Check for setups with 'active' status but no corresponding trade
                active_setups_no_trade = await conn.fetch("""
                    SELECT ts.id, ts.status, ts.symbol
                    FROM trade_setups ts
                    LEFT JOIN trades t ON ts.id = t.setup_id
                    WHERE ts.symbol = $1 
                    AND ts.status = 'active' 
                    AND t.id IS NULL
                """, symbol.replace('.FUT', ''))
                
                results['missing_trades'] = len(active_setups_no_trade)
                if active_setups_no_trade:
                    logging.warning(f"Found {len(active_setups_no_trade)} 'active' setups without trades")
                    for setup in active_setups_no_trade:
                        # Reset these setups to 'open' status
                        await conn.execute("""
                            UPDATE trade_setups 
                            SET status = 'open', updated_at = CURRENT_TIMESTAMP 
                            WHERE id = $1
                        """, setup['id'])
                        logging.info(f"Reset setup {setup['id']} from 'active' to 'open' (missing trade)")
                        results['inconsistencies_fixed'] += 1
                
                # Check for trades without exit data but setup status is 'closed'
                incomplete_trades = await conn.fetch("""
                    SELECT t.id as trade_id, t.setup_id, ts.status, ts.symbol
                    FROM trades t
                    JOIN trade_setups ts ON t.setup_id = ts.id
                    WHERE ts.symbol = $1 
                    AND ts.status = 'closed'
                    AND (t.exit_price IS NULL OR t.exit_time IS NULL)
                """, symbol.replace('.FUT', ''))
                
                if incomplete_trades:
                    logging.warning(f"Found {len(incomplete_trades)} incomplete trades with 'closed' setups")
                    for trade in incomplete_trades:
                        # Reset these setups to 'active' status
                        await conn.execute("""
                            UPDATE trade_setups 
                            SET status = 'active', updated_at = CURRENT_TIMESTAMP 
                            WHERE id = $1
                        """, trade['setup_id'])
                        logging.info(f"Reset setup {trade['setup_id']} from 'closed' to 'active' (incomplete trade)")
                        results['inconsistencies_fixed'] += 1
                
                # Check for completed trades but setup status is still 'active'
                completed_trades_active_setups = await conn.fetch("""
                    SELECT t.id as trade_id, t.setup_id, ts.status, ts.symbol
                    FROM trades t
                    JOIN trade_setups ts ON t.setup_id = ts.id
                    WHERE ts.symbol = $1 
                    AND ts.status = 'active'
                    AND t.exit_price IS NOT NULL 
                    AND t.exit_time IS NOT NULL
                """, symbol.replace('.FUT', ''))
                
                if completed_trades_active_setups:
                    logging.warning(f"Found {len(completed_trades_active_setups)} completed trades with 'active' setups")
                    for trade in completed_trades_active_setups:
                        # Update these setups to 'closed' status
                        await conn.execute("""
                            UPDATE trade_setups 
                            SET status = 'closed', updated_at = CURRENT_TIMESTAMP 
                            WHERE id = $1
                        """, trade['setup_id'])
                        logging.info(f"Updated setup {trade['setup_id']} from 'active' to 'closed' (completed trade)")
                        results['inconsistencies_fixed'] += 1
                
                # Count total inconsistencies found
                results['inconsistencies_found'] = (
                    results['missing_trades'] + 
                    len(incomplete_trades) + 
                    len(completed_trades_active_setups)
                )
                
                # Count total setups checked
                total_setups = await conn.fetchval("""
                    SELECT COUNT(*) FROM trade_setups WHERE symbol = $1
                """, symbol.replace('.FUT', ''))
                results['total_checked'] = total_setups
                
            logging.info(
                f"✅ Status consistency check complete for {symbol}:\n"
                f"   Total setups checked: {results['total_checked']}\n"
                f"   Inconsistencies found: {results['inconsistencies_found']}\n"
                f"   Inconsistencies fixed: {results['inconsistencies_fixed']}\n"
                f"   Missing trades: {results['missing_trades']}\n"
                f"   Orphaned trades: {results['orphaned_trades']}"
            )
            
        except Exception as e:
            logging.error(f"Error in status consistency check for {symbol}: {e}", exc_info=True)
            
        return results