# database/trading_db.py
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timezone
from uuid import UUID
import json
import os
from .db import init_db_pool, close_db_pool, get_connection_string, pool, pool_lock
from .utils import get_utc_now, format_price, format_display_price
from decimal import Decimal
import logging
from database.tenant_utils import get_current_user_id
import asyncio

# Global connection reference counter
connection_counter = 0
connection_counter_lock = asyncio.Lock()

class TradingDB:
    def __init__(self):
        """Initialize database connection and logger"""
        # Initialize logger first
        self.logger = logging.getLogger(__name__)
        self.pool = None
        self._is_connecting = False  # Add connection lock flag
        self._connection_count = 0   # Track active connections for this instance
        self._safe_to_disconnect = False  # Default to not close pool on disconnect
    
    async def ensure_connected(self):
        """Ensure database is connected, reconnecting if necessary"""
        global connection_counter
        
        try:
            if self._is_connecting:
                self.logger.debug("Connection already in progress, waiting...")
                return
                
            self._is_connecting = True
            
            # Use the global pool
            if pool is None or getattr(pool, '_closed', True):
                self.logger.info("Reconnecting to database...")
                self.pool = await init_db_pool()
            else:
                self.pool = pool
                
            # Increment both local and global connection counts
            self._connection_count += 1
            async with connection_counter_lock:
                connection_counter += 1
                
            self.logger.debug(f"Database connection count: local={self._connection_count}, global={connection_counter}")
            
        except Exception as e:
            self.logger.error(f"Failed to reconnect to database: {e}")
            raise
        finally:
            self._is_connecting = False
    
    async def connect(self):
        """Initialize database connection pool and ensure schema is up to date"""
        try:
            # Check if we already have a valid connection
            if self.pool and not getattr(self.pool, '_closed', True):
                self.logger.debug("Database already connected")
                return
                
            # Initialize pool
            self.pool = await init_db_pool()
            
            # Run schema updates
            try:
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        # Note: Price columns are already NUMERIC type
                        # No need to alter column types that are already correct
                        
                        # Add timestamp columns to trade_setups
                        await conn.execute("""
                            ALTER TABLE trade_setups 
                            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE 
                            DEFAULT CURRENT_TIMESTAMP;
                            
                            ALTER TABLE trade_setups 
                            ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE 
                            DEFAULT CURRENT_TIMESTAMP;
                        """)
                        
                        # Add timestamp columns to trades
                        await conn.execute("""
                            ALTER TABLE trades 
                            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE 
                            DEFAULT CURRENT_TIMESTAMP;
                            
                            ALTER TABLE trades 
                            ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE 
                            DEFAULT CURRENT_TIMESTAMP;
                        """)
                        
                        # Add indexes
                        await conn.execute("""
                            CREATE INDEX IF NOT EXISTS idx_trade_setups_symbol_status 
                            ON trade_setups(symbol, status);
                            
                            CREATE INDEX IF NOT EXISTS idx_trade_setups_updated_at 
                            ON trade_setups(updated_at);
                            
                            CREATE INDEX IF NOT EXISTS idx_trades_setup_id 
                            ON trades(setup_id);
                        """)
                        
                        # Add market-state descriptor columns to trade_setups
                        await conn.execute("""
                            ALTER TABLE trade_setups
                            ADD COLUMN IF NOT EXISTS atr_pct              NUMERIC,
                            ADD COLUMN IF NOT EXISTS true_range_pctile    NUMERIC,
                            ADD COLUMN IF NOT EXISTS daily_sigma          NUMERIC,
                            ADD COLUMN IF NOT EXISTS vix_level            NUMERIC,
                            ADD COLUMN IF NOT EXISTS adx                  NUMERIC,
                            ADD COLUMN IF NOT EXISTS ema20_slope          NUMERIC,
                            ADD COLUMN IF NOT EXISTS ma50_200_distance    NUMERIC,
                            ADD COLUMN IF NOT EXISTS va_shift             NUMERIC,
                            ADD COLUMN IF NOT EXISTS bid_ask_spread_ticks NUMERIC,
                            ADD COLUMN IF NOT EXISTS depth_top5           NUMERIC,
                            ADD COLUMN IF NOT EXISTS ob_imbalance         NUMERIC,
                            ADD COLUMN IF NOT EXISTS vwap_distance        NUMERIC,
                            ADD COLUMN IF NOT EXISTS econ_flag            TEXT,
                            ADD COLUMN IF NOT EXISTS earnings_window      BOOLEAN,
                            ADD COLUMN IF NOT EXISTS fomc_window          BOOLEAN,
                            ADD COLUMN IF NOT EXISTS hour_bucket          TEXT,
                            ADD COLUMN IF NOT EXISTS dow                  INT,
                            ADD COLUMN IF NOT EXISTS option_expiry_week   BOOLEAN,
                            ADD COLUMN IF NOT EXISTS month_end            BOOLEAN,
                            ADD COLUMN IF NOT EXISTS vol_regime           TEXT;
                        """)
                        
                        # Add execution/risk descriptors to trades table
                        await conn.execute("""
                            ALTER TABLE trades
                            ADD COLUMN IF NOT EXISTS quoted_entry_price NUMERIC,
                            ADD COLUMN IF NOT EXISTS slippage_ticks     NUMERIC,
                            ADD COLUMN IF NOT EXISTS holding_seconds    INT,
                            ADD COLUMN IF NOT EXISTS mae_points         NUMERIC,
                            ADD COLUMN IF NOT EXISTS mfe_points         NUMERIC,
                            ADD COLUMN IF NOT EXISTS overnight_gap_pts  NUMERIC;
                        """)
                        
                        # Create outcome & calibration tables
                        await conn.execute("""
                            CREATE TABLE IF NOT EXISTS trade_predictions (
                                id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                trade_id      UUID REFERENCES trades(id) ON DELETE CASCADE,
                                predicted_win NUMERIC,
                                kelly_frac    NUMERIC,
                                created_at    TIMESTAMPTZ DEFAULT now()
                            );

                            CREATE TABLE IF NOT EXISTS calibration_stats (
                                bucket_low    NUMERIC,
                                bucket_high   NUMERIC,
                                total         INT,
                                wins          INT,
                                brier_sum     NUMERIC,
                                last_updated  TIMESTAMPTZ DEFAULT now(),
                                PRIMARY KEY (bucket_low, bucket_high)
                            );
                        """)
                
                self.logger.info("✅ Database pool connected and schema updated!")
            except Exception as schema_error:
                # Check if it's the specific error about the setup_training_snapshot view
                if "setup_training_snapshot" in str(schema_error) and "depends on column" in str(schema_error):
                    self.logger.warning(f"Schema update failed but continuing: {schema_error}")
                    self.logger.info("✅ Database pool connected (without schema updates)!")
                else:
                    # Re-raise any other error
                    raise schema_error
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}", exc_info=True)
            # Do NOT close the global pool here - just release our reference
            self.pool = None
            raise
    
    async def connect_without_schema_update(self):
        """Initialize database connection pool without running schema updates"""
        try:
            # Check if we already have a valid connection
            if self.pool and not getattr(self.pool, '_closed', True):
                self.logger.debug("Database already connected")
                return
                
            # Initialize pool
            self.pool = await init_db_pool()
            self.logger.info("✅ Database pool connected (without schema updates)!")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}", exc_info=True)
            # Do NOT close the global pool here - just release our reference
            self.pool = None
            raise
    
    async def disconnect(self):
        """Decrement connection count and only close pool if needed"""
        global connection_counter
        
        try:
            # Decrement connection counts
            if self._connection_count > 0:
                self._connection_count -= 1
                
            async with connection_counter_lock:
                if connection_counter > 0:
                    connection_counter -= 1
                
            self.logger.debug(f"Database connection count after decrement: local={self._connection_count}, global={connection_counter}")
            
            # Only actually close the pool if specifically requested AND global count is zero
            if self._safe_to_disconnect and connection_counter <= 0:
                self.logger.info("No active database connections, initiating pool close")
                async with pool_lock:
                    # Double-check inside the lock to avoid race conditions
                    if connection_counter <= 0 and pool is not None and not getattr(pool, '_closed', True):
                        await close_db_pool()
                        self.pool = None
            else:
                # Just clear our reference without closing
                self.pool = None
                
        except Exception as e:
            self.logger.error(f"Error in database disconnect: {e}", exc_info=True)
            
    def prevent_disconnect(self):
        """Prevent the pool from being disconnected (for long-running services)"""
        self._safe_to_disconnect = False
        self.logger.info("Database disconnect prevention enabled")
        
    def allow_disconnect(self):
        """Allow the pool to be disconnected (for scripts)"""
        self._safe_to_disconnect = True
        self.logger.info("Database disconnect prevention disabled")
    
    async def save_analysis(self, raw_text: str, processed_data: Dict = None, user_id: Optional[str] = None) -> UUID:
        """Save market analysis with optional user_id
        
        Args:
            raw_text: The raw analysis text
            processed_data: Optional processed data dictionary
            user_id: Optional user ID, if not provided will try to get from context
        """
        await self.ensure_connected()
        
        # Use passed user_id first, fallback to context
        if user_id is None:
            user_id = get_current_user_id()
        
        async with self.pool.acquire() as conn:
            try:
                async with conn.transaction():
                    # Set tenant context for this transaction
                    if user_id:
                        await conn.execute(f"SELECT set_config('app.current_user', '{user_id}', TRUE)")
                        return await conn.fetchval("""
                            INSERT INTO analyses (raw_text, processed_data, user_id)
                            VALUES ($1, $2::jsonb, $3)
                            RETURNING id
                        """, raw_text, json.dumps(processed_data) if processed_data else None, user_id)
                    else:
                        # No user authenticated, directly insert with explicit NULL user_id
                        return await conn.fetchval("""
                            INSERT INTO analyses (raw_text, processed_data, user_id)
                            VALUES ($1, $2::jsonb, NULL)
                            RETURNING id
                        """, raw_text, json.dumps(processed_data) if processed_data else None)
            except Exception as e:
                self.logger.error(f"Error saving analysis: {e}")
                raise
    
    async def create_trade_setup(
        self,
        symbol: str,
        entry_zone_low: float,
        entry_zone_high: float,
        take_profit: float,
        stop_loss: float,
        characteristics: Optional[Dict] = None,
        analysis_id: Optional[UUID] = None,
        # Market-state descriptors
        atr_pct: Optional[float] = None,
        true_range_pctile: Optional[float] = None,
        daily_sigma: Optional[float] = None,
        vix_level: Optional[float] = None,
        adx: Optional[float] = None,
        ema20_slope: Optional[float] = None,
        ma50_200_distance: Optional[float] = None,
        va_shift: Optional[float] = None,
        bid_ask_spread_ticks: Optional[float] = None,
        depth_top5: Optional[float] = None,
        ob_imbalance: Optional[float] = None,
        vwap_distance: Optional[float] = None,
        econ_flag: Optional[str] = None,
        earnings_window: Optional[bool] = None,
        fomc_window: Optional[bool] = None,
        hour_bucket: Optional[str] = None,
        dow: Optional[int] = None,
        option_expiry_week: Optional[bool] = None,
        month_end: Optional[bool] = None,
        vol_regime: Optional[str] = None
    ) -> UUID:
        """Create a new trade setup using regular price values
        
        This matches the format used in manual SQL queries, storing prices
        as regular numbers without nanosecond conversion.
        
        Args:
            symbol: Trading symbol (e.g. 'NQ.FUT')
            entry_zone_low: Lower bound of entry zone
            entry_zone_high: Upper bound of entry zone
            take_profit: Take profit price level
            stop_loss: Stop loss price level
            characteristics: Optional dict of setup characteristics
            analysis_id: Optional UUID of the analysis that generated this setup
            
            # Market-state descriptors
            atr_pct: ATR as percentage of price
            true_range_pctile: Percentile of current true range vs historical
            daily_sigma: Daily standard deviation
            vix_level: VIX index level
            adx: Average Directional Index
            ema20_slope: Slope of 20-period EMA
            ma50_200_distance: Distance between 50 and 200 MA
            va_shift: Market profile value area shift
            bid_ask_spread_ticks: Spread in ticks
            depth_top5: Market depth at top 5 levels
            ob_imbalance: Order book imbalance
            vwap_distance: Distance from VWAP
            econ_flag: Economic calendar flag (red, orange, none)
            earnings_window: Whether in earnings release window
            fomc_window: Whether in FOMC window
            hour_bucket: Trading hour bucket
            dow: Day of week (0-6)
            option_expiry_week: Whether in option expiry week
            month_end: Whether near month end
            vol_regime: Volatility regime (low, neutral, high)
            
        Returns:
            UUID of created setup
            
        Raises:
            ValueError: If inputs are invalid
        """
        await self.ensure_connected()
        async with self.pool.acquire() as conn:
            try:
                # Get current user context and set it for this transaction
                user_id = get_current_user_id()
                if user_id:
                    await conn.execute(f"SELECT set_config('app.current_user', '{user_id}', TRUE)")
                    self.logger.info(f"Set tenant context for user {user_id} in create_trade_setup")
                
                # Validate inputs
                if any(v is None for v in [entry_zone_low, entry_zone_high, take_profit, stop_loss]):
                    raise ValueError("All price values must be non-null")
                    
                # Ensure entry zone is ordered correctly
                if entry_zone_low > entry_zone_high:
                    entry_zone_low, entry_zone_high = entry_zone_high, entry_zone_low
                    
                # Store prices directly without conversion
                setup_id = await conn.fetchval("""
                    INSERT INTO trade_setups (
                        symbol, status, entry_zone,
                        entry_zone_low, entry_zone_high,
                        take_profit, stop_loss,
                        characteristics, analysis_id,
                        setup_time, created_at, updated_at,
                        user_id, -- Explicitly set user_id
                        -- Market-state descriptors
                        atr_pct, true_range_pctile, daily_sigma, vix_level,
                        adx, ema20_slope, ma50_200_distance, va_shift,
                        bid_ask_spread_ticks, depth_top5, ob_imbalance, vwap_distance,
                        econ_flag, earnings_window, fomc_window, hour_bucket,
                        dow, option_expiry_week, month_end, vol_regime
                    ) VALUES (
                        $1, 'open', numrange($2, $3),
                        $2, $3, $4, $5, $6, $7,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        CURRENT_TIMESTAMP,
                        $8, -- user_id parameter
                        $9, $10, $11, $12, $13, $14, $15, $16,
                        $17, $18, $19, $20, $21, $22, $23, $24,
                        $25, $26, $27, $28
                    )
                    RETURNING id
                """,
                    symbol,
                    entry_zone_low,    # Regular price without conversion
                    entry_zone_high,   # Regular price without conversion
                    take_profit,       # Regular price without conversion
                    stop_loss,         # Regular price without conversion
                    json.dumps(characteristics) if characteristics else None,
                    analysis_id,       # Store the analysis ID
                    user_id,           # Explicitly pass user_id
                    atr_pct, true_range_pctile, daily_sigma, vix_level,
                    adx, ema20_slope, ma50_200_distance, va_shift,
                    bid_ask_spread_ticks, depth_top5, ob_imbalance, vwap_distance,
                    econ_flag, earnings_window, fomc_window, hour_bucket,
                    dow, option_expiry_week, month_end, vol_regime
                )
                
                self.logger.info(
                    f"Created trade setup:\n"
                    f"Symbol: {symbol}\n"
                    f"Entry Zone: {entry_zone_low}-{entry_zone_high}\n"
                    f"Take Profit: {take_profit}\n"
                    f"Stop Loss: {stop_loss}\n"
                    f"Analysis ID: {analysis_id}\n"
                    f"ATR%: {atr_pct}, VIX: {vix_level}\n"
                    f"Vol Regime: {vol_regime}"
                )
                
                return setup_id
                
            except Exception as e:
                self.logger.error(
                    f"Failed to create trade setup:\n"
                    f"Symbol: {symbol}\n"
                    f"Entry: {entry_zone_low}-{entry_zone_high}\n"
                    f"TP: {take_profit}, SL: {stop_loss}\n"
                    f"Error: {str(e)}"
                )
                raise
    
    async def record_trade_entry(self, 
                               setup_id: UUID, 
                               entry_price: float,
                               entry_time: datetime = None,
                               quoted_entry_price: Optional[float] = None,
                               slippage_ticks: Optional[float] = None,
                               tick_size: float = 0.25) -> UUID:
        """Record a trade entry
        
        Args:
            setup_id: UUID of the trade setup
            entry_price: Entry price of the trade (actual fill price)
            entry_time: Optional entry timestamp, defaults to now
            quoted_entry_price: Price that was quoted/expected before execution
            slippage_ticks: Pre-calculated slippage in ticks, or None to auto-calculate
            tick_size: Size of one tick for the instrument, default 0.25 for NQ
            
        Returns:
            UUID of the new trade record
        """
        await self.ensure_connected()
        if entry_time is None:
            entry_time = datetime.now(timezone.utc)
            
        # Calculate slippage if quoted price provided but slippage not explicitly given
        if quoted_entry_price is not None and slippage_ticks is None:
            # Convert to float to avoid decimal precision issues
            entry = float(entry_price)
            quoted = float(quoted_entry_price)
            # Calculate slippage in ticks
            slippage_points = quoted - entry  # Positive if filled better than quoted
            slippage_ticks = slippage_points / tick_size
            
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Get user_id from the setup record (since livedataservice has no user context)
                setup_data = await conn.fetchrow("""
                    SELECT user_id FROM trade_setups WHERE id = $1
                """, setup_id)
                
                if not setup_data:
                    self.logger.error(f"No setup found with ID {setup_id}")
                    raise ValueError(f"No setup found with ID {setup_id}")
                
                user_id = setup_data['user_id']
                if user_id:
                    await conn.execute(f"SELECT set_config('app.current_user', '{user_id}', TRUE)")
                    self.logger.info(f"Set tenant context for user {user_id} in record_trade_entry (from setup)")
                
                # Create trade record with execution quality metrics
                trade_id = await conn.fetchval("""
                    INSERT INTO trades (
                        setup_id,
                        entry_price,
                        entry_time,
                        quoted_entry_price,
                        slippage_ticks,
                        user_id
                    )
                    VALUES ($1, $2, $3, $4, $5, $6)
                    RETURNING id
                """, setup_id, format_price(entry_price), entry_time, 
                    quoted_entry_price, slippage_ticks, user_id)
                
                # Update setup status
                await conn.execute("""
                    UPDATE trade_setups 
                    SET status = 'active'
                    WHERE id = $1
                """, setup_id)
                
                # Log execution quality metrics
                if quoted_entry_price is not None:
                    self.logger.info(
                        f"Trade execution quality:\n"
                        f"Quoted price: {quoted_entry_price}\n"
                        f"Fill price: {entry_price}\n"
                        f"Slippage: {slippage_ticks} ticks"
                    )
                
                return trade_id
    
    def validate_price_movement(self, base_price: float, new_price: float, max_percent_move: float = 10.0) -> bool:
        """
        Validate if a price movement is within reasonable bounds
        
        Args:
            base_price: Reference price (like entry price or last known good price)
            new_price: New price to validate
            max_percent_move: Maximum allowed percentage move (default 10%)
            
        Returns:
            bool: True if price movement is valid, False otherwise
        """
        try:
            # Convert to float to ensure consistent math
            base = float(base_price)
            new = float(new_price)
            
            if base <= 0 or new <= 0:
                return False
            
            # Calculate percentage change
            percent_change = abs((new - base) / base * 100)
            
            # Validate the change is within bounds
            return percent_change <= max_percent_move
            
        except (ValueError, TypeError, ZeroDivisionError) as e:
            self.logger.error(f"Price validation error: {e}")
            return False

    async def record_trade_exit(self, 
                             trade_id: UUID, 
                             exit_price: float, 
                             exit_time: datetime = None,
                             mae_points: Optional[float] = None,
                             mfe_points: Optional[float] = None,
                             overnight_gap_pts: Optional[float] = None) -> None:
        """Record a trade exit with price validation and execution metrics
        
        Args:
            trade_id: UUID of the trade
            exit_price: Exit price of the trade
            exit_time: Optional exit timestamp, defaults to now
            mae_points: Maximum adverse excursion in price points
            mfe_points: Maximum favorable excursion in price points
            overnight_gap_pts: Price gap from previous close to next open if held overnight
        """
        await self.ensure_connected()
        if exit_time is None:
            exit_time = datetime.now(timezone.utc)
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Get trade and setup details including user_id
                trade_data = await conn.fetchrow("""
                    SELECT t.*, ts.take_profit, ts.stop_loss, ts.direction,
                           ts.entry_zone_low, ts.entry_zone_high, t.entry_time, ts.user_id
                    FROM trades t
                    JOIN trade_setups ts ON t.setup_id = ts.id
                    WHERE t.id = $1
                """, trade_id)
                
                if not trade_data:
                    self.logger.error(f"No trade found with ID {trade_id}")
                    return
                
                # Get user_id from setup record (since livedataservice has no user context)
                user_id = trade_data['user_id']
                if user_id:
                    await conn.execute(f"SELECT set_config('app.current_user', '{user_id}', TRUE)")
                    self.logger.info(f"Set tenant context for user {user_id} in record_trade_exit (from setup)")
                
                # Convert all numeric values to Decimal for consistent calculation
                entry_price = Decimal(str(trade_data['entry_price']))
                new_exit_price = Decimal(str(exit_price))
                take_profit = Decimal(str(trade_data['take_profit']))
                stop_loss = Decimal(str(trade_data['stop_loss']))
                entry_zone_low = Decimal(str(trade_data['entry_zone_low']))
                entry_zone_high = Decimal(str(trade_data['entry_zone_high']))
                direction = trade_data['direction']
                entry_time = trade_data['entry_time']
                
                # Calculate holding time in seconds
                holding_seconds = int((exit_time - entry_time).total_seconds())
                
                # Determine price range based on setup values
                max_price = max(entry_zone_high, take_profit, stop_loss)
                min_price = min(entry_zone_low, take_profit, stop_loss)
                avg_price = (max_price + min_price) / 2
                
                # Validate exit price is within reasonable range
                if not self.validate_price_movement(float(avg_price), float(new_exit_price)):
                    error_msg = (
                        f"Invalid exit price {new_exit_price} for trade {trade_id}.\n"
                        f"Entry: {entry_price}, TP: {take_profit}, SL: {stop_loss}\n"
                        f"Price appears to be outside reasonable range."
                    )
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)
                
                # Additional validation: price should be in same general range
                price_magnitude = len(str(int(avg_price)))
                exit_magnitude = len(str(int(new_exit_price)))
                if abs(price_magnitude - exit_magnitude) > 1:
                    error_msg = (
                        f"Exit price {new_exit_price} magnitude differs significantly "
                        f"from average price {avg_price}"
                    )
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)
                
                # Determine outcome based on direction and levels
                if direction == 'long':
                    if new_exit_price >= take_profit:
                        outcome = 'profit'
                    elif new_exit_price <= stop_loss:
                        outcome = 'loss'
                    else:
                        outcome = 'manual'
                else:  # short
                    if new_exit_price <= take_profit:
                        outcome = 'profit'
                    elif new_exit_price >= stop_loss:
                        outcome = 'loss'
                    else:
                        outcome = 'manual'
                
                # Calculate PnL with consistent decimal types
                pnl = (new_exit_price - entry_price) if direction == 'long' else (entry_price - new_exit_price)
                
                # Validate outcome determination with cross-verification
                validated_outcome = self._validate_outcome_determination(
                    direction, new_exit_price, entry_price, take_profit, stop_loss, pnl
                )
                
                if validated_outcome != outcome:
                    self.logger.error(
                        f"❌ OUTCOME VALIDATION FAILED for trade {trade_id}:\n"
                        f"   Direction: {direction}\n"
                        f"   Entry: {entry_price}, Exit: {new_exit_price}\n"
                        f"   Take Profit: {take_profit}, Stop Loss: {stop_loss}\n"
                        f"   PnL: {pnl}\n"
                        f"   Initial outcome: '{outcome}'\n"
                        f"   Validated outcome: '{validated_outcome}'\n"
                        f"   Using validated outcome for safety."
                    )
                    outcome = validated_outcome
                else:
                    self.logger.debug(f"✅ Outcome validation passed: {outcome} for trade {trade_id}")
                
                # Format values for database storage
                formatted_exit_price = format_price(float(new_exit_price))
                formatted_pnl = format_price(float(pnl))
                
                # Update trade record and setup status in one transaction
                await conn.execute("""
                    WITH trade_update AS (
                        UPDATE trades
                        SET exit_price = $2,
                            exit_time = $3,
                            outcome = $4,
                            pnl = $5,
                            updated_at = CURRENT_TIMESTAMP,
                            holding_seconds = $6,
                            mae_points = $7,
                            mfe_points = $8,
                            overnight_gap_pts = $9
                        WHERE id = $1
                    )
                    UPDATE trade_setups
                    SET status = 'closed',
                        current_price = $2,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = (SELECT setup_id FROM trades WHERE id = $1)
                """, 
                    trade_id, 
                    formatted_exit_price, 
                    exit_time, 
                    outcome, 
                    formatted_pnl,
                    holding_seconds,
                    mae_points,
                    mfe_points,
                    overnight_gap_pts
                )
                
                self.logger.info(
                    f"Trade {trade_id} exited:\n"
                    f"Price: {new_exit_price}\n"
                    f"Outcome: {outcome}\n"
                    f"PnL: {pnl}\n"
                    f"Holding time: {holding_seconds // 3600}h {(holding_seconds % 3600) // 60}m {holding_seconds % 60}s\n"
                    f"MAE: {mae_points} points, MFE: {mfe_points} points"
                )
    
    async def get_active_setups(self, symbol: str) -> List[Dict[str, Any]]:
        """Fetch open trade setups for the given symbol
        
        Args:
            symbol: The trading symbol to fetch setups for
            
        Returns:
            List of active trade setup dictionaries
        """
        await self.ensure_connected()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT ts.*, 
                       lower(entry_zone) as entry_low,
                       upper(entry_zone) as entry_high,
                       take_profit,
                       stop_loss,
                       entry_buffer
                FROM trade_setups ts
                WHERE symbol = $1 
                AND status = 'open'
                AND setup_time > NOW() - INTERVAL '24 hours'
            """, symbol)
            return [dict(row) for row in rows]

    async def get_active_trade(self, setup_id: UUID) -> Optional[Dict[str, Any]]:
        """Get the active trade for a setup
        
        Args:
            setup_id: UUID of the setup
            
        Returns:
            Dictionary with trade details or None if no active trade found
        """
        await self.ensure_connected()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    id,
                    entry_price,
                    entry_time,
                    exit_price,
                    exit_time
                FROM trades
                WHERE 
                    setup_id = $1 AND 
                    exit_time IS NULL
                ORDER BY entry_time DESC
                LIMIT 1
            """, setup_id)
            
            if not row:
                return None
            
            return {
                'id': row['id'],
                'entry_price': row['entry_price'],
                'entry_time': row['entry_time'],
                'exit_price': row['exit_price'],
                'exit_time': row['exit_time']
            }

    async def get_trade_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """Get trading statistics for a time period"""
        await self.ensure_connected()
        async with self.pool.acquire() as conn:
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_trades,
                    COUNT(*) FILTER (WHERE outcome = 'profit') as winning_trades,
                    COALESCE(SUM(pnl), 0) as total_pnl
                FROM trades
                WHERE created_at > NOW() - INTERVAL '1 hour' * $1
            """, hours)  # Changed the query to use multiplication
            
            total_trades = stats['total_trades']
            winning_trades = stats['winning_trades']
            
            return {
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'win_rate': (winning_trades / total_trades * 100 
                           if total_trades > 0 else 0),
                'total_pnl': float(stats['total_pnl'])
            }

    async def save_question(
        self, 
        question: str, 
        answer: str, 
        previous_analysis_id: Optional[UUID] = None
    ) -> UUID:
        """Save a Q&A interaction to the database
        
        Args:
            question: The user's question
            answer: The system's answer
            previous_analysis_id: Optional UUID of related previous analysis
            
        Returns:
            UUID: The ID of the newly created analysis record
        """
        await self.ensure_connected()
        
        # Create a new analysis record for the Q&A without depending on tenant context
        async with self.pool.acquire() as conn:
            # Try a more direct approach - bypass RLS by using a system column insert
            analysis_id = await conn.fetchval("""
                -- Skip RLS by using system column
                INSERT INTO analyses (
                    raw_text,
                    processed_data,
                    user_id
                )
                VALUES (
                    $1,
                    $2::jsonb,
                    NULL  -- Explicitly set user_id to NULL for system operations
                )
                RETURNING id
            """,
                answer,
                json.dumps({
                    'type': 'qa',
                    'question': question,
                    'previous_analysis_id': str(previous_analysis_id) if previous_analysis_id else None,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                })
            )
            
            return analysis_id

    async def get_qa_history(
        self,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Retrieve Q&A history from the database
        
        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            List of Q&A interactions with timestamps
        """
        await self.ensure_connected()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, raw_text, processed_data, created_at
                FROM analyses 
                WHERE processed_data->>'type' = 'qa'
                ORDER BY created_at DESC
                LIMIT $1 OFFSET $2
            """, limit, offset)
            
            return [{
                'id': str(row['id']),
                'answer': row['raw_text'],
                'question': row['processed_data'].get('question'),
                'timestamp': row['created_at'].isoformat(),
                'previous_analysis_id': row['processed_data'].get('previous_analysis_id')
            } for row in rows]

    async def verify_and_fix_setups(self, symbol: str) -> None:
        """Verify all setups have proper entry zones and fix if needed.
        
        This method properly handles decimal.Decimal values from PostgreSQL
        when calculating entry zones.
        """
        await self.ensure_connected()
        async with self.pool.acquire() as conn:
            self.logger.info(f"\nVerifying setups for {symbol}...")
            
            # Get all setups for the symbol
            setups = await conn.fetch("""
                SELECT * FROM trade_setups WHERE symbol = $1
            """, symbol)
            
            for setup in setups:
                self.logger.info(f"\nChecking setup {setup['id']}:")
                needs_update = False
                
                # Convert values to Decimal for precise calculations
                take_profit = setup['take_profit']
                stop_loss = setup['stop_loss']
                
                # Check for missing entry zones
                if setup['entry_zone_low'] is None or setup['entry_zone_high'] is None:
                    self.logger.info("Entry zone bounds are missing")
                    needs_update = True
                    
                    # Calculate sensible entry zone if we have take profit and stop loss
                    if take_profit is not None and stop_loss is not None:
                        # Convert percentages to Decimal
                        d_20 = Decimal('0.20')
                        d_30 = Decimal('0.30')
                        
                        # For long setups
                        if take_profit > stop_loss:
                            range_size = take_profit - stop_loss
                            entry_low = stop_loss + (range_size * d_20)
                            entry_high = stop_loss + (range_size * d_30)
                        # For short setups
                        else:
                            range_size = stop_loss - take_profit
                            entry_high = stop_loss - (range_size * d_20)
                            entry_low = stop_loss - (range_size * d_30)
                        
                        self.logger.info(f"Calculated entry zone: {entry_low} - {entry_high}")
                        
                        try:
                            # Update the setup
                            await conn.execute("""
                                UPDATE trade_setups
                                SET 
                                    entry_zone_low = $2,
                                    entry_zone_high = $3
                                WHERE id = $1
                            """, setup['id'], entry_low, entry_high)
                            
                            self.logger.info(f"✅ Fixed entry zone: {entry_low:.1f} - {entry_high:.1f}")
                            
                        except Exception as e:
                            self.logger.error(f"❌ Error updating setup: {e}")
                            continue
                
                # Print current setup state
                self.logger.info(f"Status: {setup['status']}")
                if setup['entry_zone_low'] is not None:
                    self.logger.info(f"Entry Zone: {setup['entry_zone_low']:.1f} - {setup['entry_zone_high']:.1f}")
                else:
                    self.logger.info("Entry Zone: Not Set")
                self.logger.info(f"Take Profit: {setup['take_profit']:.1f}")
                self.logger.info(f"Stop Loss: {setup['stop_loss']:.1f}")

    async def get_symbol_setups(self, symbol: str) -> List[Dict[str, Any]]:
        """Get active trade setups with proper decimal handling.
        
        This method will handle symbol aliases, so 'NQ.FUT' will match 'NQ' and vice versa.
        """
        await self.ensure_connected()
        
        # Handle symbol aliases: Extract the base symbol (e.g., 'NQ' from 'NQ.FUT')
        base_symbol = symbol.split('.')[0] if '.' in symbol else symbol
        
        self.logger.info(f"Getting setups for symbol {symbol} (base: {base_symbol})")
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    id,
                    symbol,
                    status,
                    entry_zone_low,
                    entry_zone_high,
                    take_profit,
                    stop_loss,
                    direction,
                    setup_time,
                    created_at
                FROM trade_setups
                WHERE 
                    (symbol = $1 OR symbol LIKE $2 OR symbol = $3) AND
                    status IN ('open', 'active') AND
                    (
                        (setup_time IS NOT NULL AND setup_time > NOW() - INTERVAL '24 hours') OR
                        (setup_time IS NULL AND created_at > NOW() - INTERVAL '24 hours')
                    )
                ORDER BY 
                    COALESCE(setup_time, created_at) DESC
            """, symbol, f"{base_symbol}%", base_symbol)
            
            # Log what we found
            self.logger.info(f"Found {len(rows)} setups for {symbol}")
            
            # Convert to dict and ensure proper decimal handling
            setups = []
            for row in rows:
                setup = dict(row)
                # Keep numeric values as Decimal
                setups.append(setup)
            
            return setups

    async def get_active_setups_for_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """Get active trade setups for a specific symbol
        
        Args:
            symbol: Trading symbol to get setups for
            
        Returns:
            List of dictionaries containing setup details
        """
        await self.ensure_connected()
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    ts.id, 
                    ts.entry_zone, 
                    ts.take_profit, 
                    ts.stop_loss,
                    ts.status,
                    ts.entry_buffer
                FROM trade_setups ts
                WHERE 
                    ts.symbol = $1 AND 
                    ts.status IN ('open', 'active', 'in_progress') AND
                    ts.setup_time > NOW() - INTERVAL '24 hours'
                ORDER BY ts.setup_time DESC
            """, symbol)
            
            return [
                {
                    'id': str(row['id']),
                    'entry_zone_low': float(row['entry_zone'].lower),
                    'entry_zone_high': float(row['entry_zone'].upper),
                    'take_profit': float(row['take_profit']),
                    'stop_loss': float(row['stop_loss']),
                    'entry_buffer': float(row['entry_buffer']),
                    'status': row['status']
                } for row in rows
            ]

    async def update_setup_status(
        self, 
        setup_id: UUID, 
        status: str,
        current_price: Optional[float] = None
    ) -> None:
        """Update a setup's status and current price."""
        await self.ensure_connected()
        async with self.pool.acquire() as conn:
            # Update with the new values
            await conn.execute("""
                UPDATE trade_setups 
                SET 
                    status = $2,
                    current_price = $3
                WHERE id = $1
            """,
                setup_id,
                status,
                format_price(current_price) if current_price is not None else None
            )

    async def record_trade_from_setup(
        self,
        setup_id: UUID,
        entry_price: float,
        exit_price: Optional[float] = None,
        outcome: Optional[str] = None
    ) -> UUID:
        """Record a complete trade based on a setup
        
        Args:
            setup_id: UUID of the setup that triggered the trade
            entry_price: Entry price in dollars
            exit_price: Optional exit price in dollars
            outcome: Optional outcome ('profit', 'loss', 'manual')
            
        Returns:
            UUID of the new trade record
            
        Raises:
            ValueError: If setup doesn't exist or has invalid status
        """
        await self.ensure_connected()
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    # Get setup details for validation and user_id
                    setup = await conn.fetchrow("""
                        SELECT status, take_profit, stop_loss, user_id
                        FROM trade_setups
                        WHERE id = $1
                    """, setup_id)
                    
                    if not setup:
                        raise ValueError(f"No setup found with ID {setup_id}")
                    
                    # Get user_id from setup record (since livedataservice has no user context)
                    user_id = setup['user_id']
                    if user_id:
                        await conn.execute(f"SELECT set_config('app.current_user', '{user_id}', TRUE)")
                        self.logger.info(f"Set tenant context for user {user_id} in record_trade_from_setup (from setup)")
                    
                    if setup['status'] != 'open':
                        raise ValueError(f"Setup {setup_id} is not open (status: {setup['status']})")
                    
                    # Format prices for storage
                    formatted_price = format_price(entry_price)
                    if formatted_price is None:
                        raise ValueError(f"Invalid entry price: {entry_price}")
                    
                    # Update setup status and current price
                    await conn.execute("""
                        UPDATE trade_setups 
                        SET 
                            status = 'active',
                            current_price = $2,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = $1
                    """,
                        setup_id,
                        formatted_price
                    )
                    
                    # Create trade record
                    trade_id = await conn.fetchval("""
                        INSERT INTO trades (
                            setup_id, 
                            entry_price,
                            entry_time,
                            created_at,
                            updated_at,
                            user_id
                        ) VALUES (
                            $1, $2, $3,
                            CURRENT_TIMESTAMP,
                            CURRENT_TIMESTAMP,
                            $4
                        ) RETURNING id
                    """,
                        setup_id,
                        formatted_price,
                        get_utc_now(),
                        user_id
                    )
                    
                    # Format prices for logging
                    display_entry = format_display_price(entry_price)
                    display_tp = format_display_price(setup['take_profit'])
                    display_sl = format_display_price(setup['stop_loss'])
                    
                    self.logger.info(
                        f"Trade recorded successfully:\n"
                        f"- Setup ID: {setup_id}\n"
                        f"- Trade ID: {trade_id}\n"
                        f"- Entry Price: {display_entry}\n"
                        f"- Take Profit: {display_tp}\n"
                        f"- Stop Loss: {display_sl}\n"
                        f"- Status: active"
                    )
                    
                    return trade_id
                    
                except Exception as e:
                    self.logger.error(
                        f"Failed to record trade:\n"
                        f"- Setup ID: {setup_id}\n"
                        f"- Entry Price: {entry_price}\n"
                        f"Error: {str(e)}",
                        exc_info=True
                    )
                    raise

    def format_decimal(self, value: Optional[Union[Decimal, float]]) -> Optional[str]:
        """Format a decimal value for display, handling None values."""
        if value is None:
            return "Not Set"
        return f"{value:.1f}"

    async def get_instrument_mappings(self):
        """Get mappings between continuous contract symbols and instrument IDs.
        
        Returns:
            Dict mapping continuous contract symbols (e.g. "NQ.FUT") to instrument IDs
        """
        await self.ensure_connected()
        try:
            async with self.pool.acquire() as conn:
                try:
                    # First try to create the table if it doesn't exist
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS instrument_mappings (
                            id SERIAL PRIMARY KEY,
                            symbol VARCHAR(16) NOT NULL,
                            instrument_id VARCHAR(32) NOT NULL,
                            active BOOLEAN DEFAULT TRUE,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                        );
                        
                        CREATE INDEX IF NOT EXISTS idx_instrument_mappings_symbol 
                            ON instrument_mappings(symbol);
                        
                        CREATE INDEX IF NOT EXISTS idx_instrument_mappings_active 
                            ON instrument_mappings(active);
                    """)
                    
                    # Now try to get mappings
                    rows = await conn.fetch("""
                        SELECT symbol, instrument_id 
                        FROM instrument_mappings
                        WHERE active = TRUE
                    """)
                    
                    mappings = {}
                    for row in rows:
                        # Assume that your table stores continuous contract symbols (like "NQ.FUT", "ES.FUT")
                        mappings[row['symbol']] = str(row['instrument_id'])
                        
                    if mappings:
                        self.logger.info(f"Retrieved {len(mappings)} instrument mappings from database")
                        return mappings
                        
                except Exception as e:
                    self.logger.warning(f"Could not fetch mappings from database: {e}")
                
                # If the table doesn't exist or returns no rows, fall back
                self.logger.info("No mappings found in database; using fallback continuous mappings")
                
        except Exception as e:
            self.logger.error(f"Error retrieving instrument mappings: {e}", exc_info=True)
            
        # Fallback mappings for continuous contracts
        fallback_mappings = {
            "NQ.FUT": "CONTINUOUS",  # Indicates that the continuous front-month is handled by the API.
            "ES.FUT": "CONTINUOUS",
            "RTY.FUT": "CONTINUOUS",
            "YM.FUT": "CONTINUOUS"
        }
        
        self.logger.info(f"Using fallback instrument mappings: {fallback_mappings}")
        return fallback_mappings

    def get_dsn(self):
        """Get the database connection string (DSN).
        
        Returns:
            str: The database connection string or None if not available
        """
        try:
            # Use the existing function to get the connection string
            return get_connection_string()
        except Exception as e:
            self.logger.error(f"Error generating DSN: {e}")
            return None
    
    async def fetch_all(self, query, *args):
        """Execute a query and return all rows as a list of dictionaries
        
        Args:
            query: SQL query to execute
            *args: Query parameters
            
        Returns:
            List of dictionaries with query results
        """
        await self.ensure_connected()
        async with self.pool.acquire() as conn:
            try:
                rows = await conn.fetch(query, *args)
                # Convert Row objects to dictionaries
                return [dict(row) for row in rows]
            except Exception as e:
                self.logger.error(f"Error executing query: {e}", exc_info=True)
                raise
    
    async def fetch_one(self, query, *args):
        """Execute a query and return a single row as a dictionary
        
        Args:
            query: SQL query to execute
            *args: Query parameters
            
        Returns:
            Dictionary with query result or None if no rows found
        """
        await self.ensure_connected()
        async with self.pool.acquire() as conn:
            try:
                row = await conn.fetchrow(query, *args)
                # Convert Row object to dictionary if it exists
                return dict(row) if row else None
            except Exception as e:
                self.logger.error(f"Error executing query: {e}", exc_info=True)
                raise
    
    async def execute(self, query, *args):
        """Execute a query without returning rows
        
        Args:
            query: SQL query to execute
            *args: Query parameters
            
        Returns:
            Query execution result
        """
        await self.ensure_connected()
        async with self.pool.acquire() as conn:
            try:
                return await conn.execute(query, *args)
            except Exception as e:
                self.logger.error(f"Error executing query: {e}", exc_info=True)
                raise
    
    async def update_trade_risk_metrics(self, 
                                     trade_id: UUID, 
                                     mae_points: Optional[float] = None, 
                                     mfe_points: Optional[float] = None,
                                     overnight_gap_pts: Optional[float] = None) -> None:
        """Update Maximum Adverse Excursion and Maximum Favorable Excursion for a trade
        
        This method is used to continuously update risk metrics during a trade's lifecycle.
        It can be called on each candle close to track the trade's performance.
        
        Args:
            trade_id: UUID of the trade
            mae_points: Maximum adverse excursion in price points (worst drawdown from entry)
            mfe_points: Maximum favorable excursion in price points (best unrealized profit)
            overnight_gap_pts: Price gap from previous close to next open if held overnight
        """
        await self.ensure_connected()
        
        async with self.pool.acquire() as conn:
            # First, get the current MAE/MFE to only update if the new values are more extreme
            current = await conn.fetchrow("""
                SELECT t.mae_points, t.mfe_points, t.overnight_gap_pts, t.entry_price, 
                       ts.direction
                FROM trades t
                JOIN trade_setups ts ON t.setup_id = ts.id
                WHERE t.id = $1 AND t.exit_time IS NULL
            """, trade_id)
            
            if not current:
                self.logger.warning(f"No open trade found with ID {trade_id} for risk metric update")
                return
                
            direction = current['direction']
            current_mae = current['mae_points']
            current_mfe = current['mfe_points']
            update_needed = False
            update_fields = []
            
            # For MAE, we want the most negative value (worst case)
            if mae_points is not None:
                if current_mae is None or (direction == 'long' and mae_points < current_mae) or \
                   (direction == 'short' and mae_points < current_mae):
                    update_fields.append(("mae_points", mae_points))
                    update_needed = True
            
            # For MFE, we want the most positive value (best case)
            if mfe_points is not None:
                if current_mfe is None or (direction == 'long' and mfe_points > current_mfe) or \
                   (direction == 'short' and mfe_points > current_mfe):
                    update_fields.append(("mfe_points", mfe_points))
                    update_needed = True
            
            # For overnight gap, we store the most recent value
            if overnight_gap_pts is not None:
                if current['overnight_gap_pts'] != overnight_gap_pts:
                    update_fields.append(("overnight_gap_pts", overnight_gap_pts))
                    update_needed = True
            
            # Only update if necessary
            if update_needed:
                # Build SQL update statement dynamically based on which fields need updating
                fields_sql = ", ".join([f"{field} = ${i+2}" for i, (field, _) in enumerate(update_fields)])
                values = [trade_id] + [value for _, value in update_fields]
                
                await conn.execute(f"""
                    UPDATE trades
                    SET {fields_sql}, updated_at = CURRENT_TIMESTAMP
                    WHERE id = $1
                """, *values)
                
                self.logger.info(
                    f"Updated risk metrics for trade {trade_id}:\n" + 
                    "\n".join([f"{field}: {value}" for field, value in update_fields])
                )

    async def save_trade_prediction(self, 
                                 trade_id: UUID, 
                                 predicted_win: float, 
                                 kelly_frac: Optional[float] = None) -> UUID:
        """Save a prediction for a trade's outcome with probability
        
        If a prediction already exists for this trade, it will be updated with the new values.
        
        Args:
            trade_id: UUID of the trade
            predicted_win: Probability of win (0.0 to 1.0)
            kelly_frac: Kelly criterion fraction for position sizing (optional)
            
        Returns:
            UUID of the created or updated prediction record
        """
        await self.ensure_connected()
        
        # Validate inputs
        if not 0 <= predicted_win <= 1:
            raise ValueError(f"Predicted win probability must be between 0 and 1, got {predicted_win}")
            
        if kelly_frac is not None and not -1 <= kelly_frac <= 1:
            raise ValueError(f"Kelly fraction must be between -1 and 1, got {kelly_frac}")
        
        async with self.pool.acquire() as conn:
            # Check if trade exists
            trade_exists = await conn.fetchval(
                "SELECT 1 FROM trades WHERE id = $1", trade_id
            )
            
            if not trade_exists:
                raise ValueError(f"No trade found with ID {trade_id}")
            
            # Save or update prediction using ON CONFLICT
            prediction_id = await conn.fetchval("""
                INSERT INTO trade_predictions (
                    trade_id, predicted_win, kelly_frac, created_at
                ) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
                ON CONFLICT (trade_id) DO UPDATE SET
                    predicted_win = EXCLUDED.predicted_win,
                    kelly_frac = EXCLUDED.kelly_frac,
                    created_at = CURRENT_TIMESTAMP
                RETURNING id
            """, trade_id, predicted_win, kelly_frac)
            
            self.logger.info(
                f"Saved/updated prediction for trade {trade_id}:\n"
                f"Win probability: {predicted_win:.2f}\n"
                f"Kelly fraction: {kelly_frac:.2f}" if kelly_frac is not None else "Kelly fraction: N/A"
            )
            
            return prediction_id
    
    async def update_calibration_statistics(self) -> None:
        """Update calibration statistics based on completed trades with predictions
        
        This function should be called periodically (e.g., nightly) to update the
        calibration statistics for model evaluation.
        """
        await self.ensure_connected()
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Create buckets of 10% each
                buckets = [(i/10, (i+1)/10) for i in range(10)]
                
                # For 0-0.1 bucket, include exact 0
                buckets[0] = (0, 0.1)
                
                # For 0.9-1 bucket, include exact 1
                buckets[9] = (0.9, 1.0)
                
                # Process each bucket
                for low, high in buckets:
                    # Get completed trades in this bucket
                    results = await conn.fetch("""
                        WITH latest_predictions AS (
                            SELECT DISTINCT ON (p.trade_id) 
                                p.trade_id, 
                                p.predicted_win,
                                t.outcome
                            FROM trade_predictions p
                            JOIN trades t ON p.trade_id = t.id
                            WHERE 
                                t.exit_time IS NOT NULL
                                AND p.predicted_win >= $1 
                                AND p.predicted_win < $2
                            ORDER BY p.trade_id, p.created_at DESC
                        )
                        SELECT 
                            COUNT(*) as total,
                            COUNT(CASE WHEN outcome = 'profit' THEN 1 END) as wins,
                            SUM(POW(predicted_win - CASE WHEN outcome = 'profit' THEN 1 ELSE 0 END, 2)) as brier_sum
                        FROM latest_predictions;
                    """, low, high)
                    
                    if results and results[0]['total'] > 0:
                        total = results[0]['total']
                        wins = results[0]['wins']
                        brier_sum = results[0]['brier_sum'] or 0
                        
                        # Update calibration stats for this bucket
                        await conn.execute("""
                            INSERT INTO calibration_stats (bucket_low, bucket_high, total, wins, brier_sum, last_updated)
                            VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
                            ON CONFLICT (bucket_low, bucket_high) DO UPDATE
                            SET total = calibration_stats.total + EXCLUDED.total,
                                wins = calibration_stats.wins + EXCLUDED.wins,
                                brier_sum = calibration_stats.brier_sum + EXCLUDED.brier_sum,
                                last_updated = CURRENT_TIMESTAMP
                        """, low, high, total, wins, brier_sum)
                        
                        self.logger.info(
                            f"Updated calibration for bucket {low:.1f}-{high:.1f}:\n"
                            f"Total trades: {total}\n"
                            f"Wins: {wins}\n"
                            f"Actual win rate: {wins/total:.2f}\n"
                            f"Brier score: {brier_sum/total:.4f}"
                        )
    
    async def get_calibration_curve(self) -> List[Dict[str, Any]]:
        """Get calibration curve data for model evaluation
        
        Returns:
            List of dictionaries with calibration statistics per bucket
        """
        await self.ensure_connected()
        
        async with self.pool.acquire() as conn:
            results = await conn.fetch("""
                SELECT 
                    bucket_low, 
                    bucket_high, 
                    total, 
                    wins, 
                    CASE WHEN total > 0 THEN wins::float / total ELSE 0 END as actual_win_rate,
                    CASE WHEN total > 0 THEN brier_sum / total ELSE NULL END as brier_score,
                    (bucket_high + bucket_low) / 2 as predicted_win_rate,
                    last_updated
                FROM calibration_stats
                ORDER BY bucket_low
            """)
            
            return [dict(r) for r in results]

    async def save_trade_setup(self, setup: Dict[str, Any]) -> Optional[UUID]:
        """Save a trade setup from a dictionary format
        
        This is an adapter method that maps the dictionary format used by the
        Claude analyzer to the parameters expected by create_trade_setup.
        
        Args:
            setup: Dictionary containing setup data with the following expected structure:
                {
                    'symbol': str,
                    'entry_low': float,
                    'entry_high': float,
                    'take_profit': float,
                    'stop_loss': float,
                    'characteristics': {
                        'direction': str,
                        'reasons': List[str],
                        'management': List[str],
                        'likelihood': int,
                        'setup_type': str,
                        'momentum': Optional[str],
                        'market_condition': Optional[str],
                        'timeframe': Optional[str],
                        'risk_reward': Optional[float],
                        'confidence': Optional[int]
                    },
                    'analysis_id': Optional[UUID]
                }
                
        Returns:
            UUID of the created setup or None if creation failed
        """
        self.logger.info(f"Saving trade setup via adapter method: {setup['symbol']}")
        try:
            # Extract required fields with validation
            symbol = setup.get('symbol', 'NQ.FUT')  # Default to NQ.FUT if not specified
            entry_low = setup.get('entry_low')
            entry_high = setup.get('entry_high')
            take_profit = setup.get('take_profit')
            stop_loss = setup.get('stop_loss')
            characteristics = setup.get('characteristics', {})
            analysis_id = setup.get('analysis_id')  # Get analysis_id from setup
            
            # Validate that analysis_id exists if provided
            if analysis_id:
                # Check if the analysis_id exists in the analyses table
                async with self.pool.acquire() as conn:
                    analysis_exists = await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM analyses WHERE id = $1)", 
                        analysis_id
                    )
                    if not analysis_exists:
                        self.logger.warning(f"Analysis ID {analysis_id} not found in analyses table, setting to NULL")
                        analysis_id = None
            
            # Link to analysis context if feature flag is enabled
            analysis_context_id = None
            analysis_features = {}
            
            if os.getenv('ENABLE_ANALYSIS_CONTEXT', 'false').lower() == 'true':
                try:
                    async with self.pool.acquire() as conn:
                        # Find most recent analysis context within 2 hour window
                        context_row = await conn.fetchrow("""
                            SELECT id, extracted_features
                            FROM analysis_contexts
                            WHERE symbol = $1
                            AND timestamp BETWEEN (CURRENT_TIMESTAMP - INTERVAL '2 hours') 
                                               AND (CURRENT_TIMESTAMP + INTERVAL '5 minutes')
                            ORDER BY timestamp DESC
                            LIMIT 1
                        """, symbol.replace('.FUT', ''))
                        
                        if context_row:
                            analysis_context_id = context_row['id']
                            analysis_features = context_row['extracted_features'] or {}
                            self.logger.info(f"Linked analysis context {analysis_context_id} with {len(analysis_features)} features")
                        else:
                            self.logger.warning(f"No recent analysis context found for {symbol}")
                except Exception as e:
                    self.logger.error(f"Failed to link analysis context: {e}")
                    # Continue without analysis context
            
            # Validate required fields
            if any(x is None for x in [entry_low, entry_high, take_profit, stop_loss]):
                self.logger.error(f"Missing required fields in setup: {setup}")
                return None
                
            # Ensure symbol is 'NQ.FUT' for NQ futures
            if symbol == 'NQ':
                symbol = 'NQ.FUT'
                
            # Extract direction from characteristics (critical field)
            direction = characteristics.get('direction')
            if not direction:
                self.logger.warning(f"No direction specified in trade setup: {setup}")
            
            # Get current market price for validation
            current_price = None
            try:
                async with self.pool.acquire() as conn:
                    current_price_row = await conn.fetchrow("""
                        SELECT close FROM ohlcv_data 
                        WHERE symbol = $1 
                        ORDER BY timestamp DESC 
                        LIMIT 1
                    """, symbol.replace('.FUT', ''))
                    
                    if current_price_row:
                        current_price = float(current_price_row['close'])
                        self.logger.info(f"Current market price for {symbol}: {current_price}")
                    else:
                        self.logger.warning(f"No current price found for {symbol}")
            except Exception as e:
                self.logger.error(f"Failed to get current price for {symbol}: {e}")
            
            # Validate direction logic before creating setup
            if current_price and direction:
                try:
                    self._validate_setup_direction(direction, current_price, entry_low, entry_high)
                except ValueError as ve:
                    self.logger.error(f"Setup validation failed: {ve}")
                    return None
            
            # Extract market state descriptors from characteristics
            market_condition = characteristics.get('market_condition')
            setup_type = characteristics.get('setup_type')
            momentum = characteristics.get('momentum')
            
            # Map characteristics to market state descriptors
            vol_regime = None
            if momentum == 'strong':
                vol_regime = 'high'
            elif momentum == 'weak':
                vol_regime = 'low'
            elif momentum == 'neutral':
                vol_regime = 'neutral'
            
            # Extract market metrics if provided
            market_metrics = setup.get('market_metrics', {})
            atr_pct = market_metrics.get('atr_pct')
            true_range_pctile = market_metrics.get('true_range_pctile')
            daily_sigma = market_metrics.get('daily_sigma')
            ema20_slope = market_metrics.get('ema20_slope') 
            vwap_distance = market_metrics.get('vwap_distance')
            hour_bucket = market_metrics.get('hour_bucket')
            adx = market_metrics.get('adx')
            
            # Override vol_regime from market metrics if available
            if market_metrics.get('vol_regime'):
                vol_regime = market_metrics.get('vol_regime')
            
            self.logger.info(f"Using market metrics: atr_pct={atr_pct}, true_range_pctile={true_range_pctile}, daily_sigma={daily_sigma}, adx={adx}, vol_regime={vol_regime}")
            
            # Insert the trade setup with all fields correctly set
            await self.ensure_connected()
            async with self.pool.acquire() as conn:
                try:
                    # Get user_id from setup dict first, fallback to context
                    user_id = setup.get('user_id') or get_current_user_id()
                    if user_id:
                        await conn.execute(f"SELECT set_config('app.current_user', '{user_id}', TRUE)")
                        self.logger.info(f"Set tenant context for user {user_id} in trade setup creation")
                    
                    # Validate inputs
                    if any(v is None for v in [entry_low, entry_high, take_profit, stop_loss]):
                        raise ValueError("All price values must be non-null")
                    # Ensure entry zone is ordered correctly
                    if entry_low is not None and entry_high is not None and entry_low > entry_high:
                        entry_low, entry_high = entry_high, entry_low
                        
                    # Store prices directly without conversion, but ensure the direction field is also set
                    setup_id = await conn.fetchval("""
                        INSERT INTO trade_setups (
                            symbol, status, entry_zone,
                            entry_zone_low, entry_zone_high,
                            take_profit, stop_loss,
                            characteristics, analysis_id,
                            setup_time, created_at, updated_at,
                            direction, -- Set direction explicitly
                            user_id, -- Explicitly set user_id
                            current_price, -- Set current market price for validation
                            -- Analysis context fields
                            analysis_context_id,
                            analysis_features,
                            -- Market-state descriptors
                            atr_pct, true_range_pctile, daily_sigma, vix_level,
                            adx, ema20_slope, ma50_200_distance, va_shift,
                            bid_ask_spread_ticks, depth_top5, ob_imbalance, vwap_distance,
                            econ_flag, earnings_window, fomc_window, hour_bucket,
                            dow, option_expiry_week, month_end, vol_regime
                        ) VALUES (
                            $1, 'open', $2::jsonb,
                            $3, $4, $5, $6, $7, $8,
                            CURRENT_TIMESTAMP,
                            CURRENT_TIMESTAMP,
                            CURRENT_TIMESTAMP,
                            $9, -- Direction parameter
                            $10, -- user_id parameter
                            $11, -- current_price parameter
                            $12, -- analysis_context_id
                            $13::jsonb, -- analysis_features
                            $14, $15, $16, $17, $18, $19, $20, $21,
                            $22, $23, $24, $25, $26, $27, $28, $29,
                            $30, $31, $32, $33
                        )
                        RETURNING id
                    """,
                        symbol,
                        json.dumps({"low": entry_low, "high": entry_high}),  # JSONB entry_zone
                        entry_low,    # entry_zone_low
                        entry_high,   # entry_zone_high
                        take_profit,  # Regular price without conversion
                        stop_loss,    # Regular price without conversion
                        json.dumps(characteristics) if characteristics else None,
                        analysis_id,  # Use the analysis_id from the setup
                        direction,    # Set direction explicitly
                        user_id,      # Explicitly pass user_id
                        current_price,  # Set current market price for validation
                        analysis_context_id,  # Link to analysis context
                        json.dumps(analysis_features) if analysis_features else '{}',  # Copy extracted features
                        atr_pct, true_range_pctile, daily_sigma, None,  # atr_pct, true_range_pctile, daily_sigma, vix_level
                        adx, ema20_slope, None, None,  # adx, ema20_slope, ma50_200_distance, va_shift
                        None, None, None, vwap_distance,  # bid_ask_spread_ticks, depth_top5, ob_imbalance, vwap_distance
                        None, None, None, hour_bucket,  # econ_flag, earnings_window, fomc_window, hour_bucket
                        None, None, None, vol_regime  # dow, option_expiry_week, month_end, vol_regime
                    )
                    
                    self.logger.info(
                        f"Created trade setup:\n"
                        f"Symbol: {symbol}\n"
                        f"Direction: {direction}\n"
                        f"Entry Zone: {entry_low}-{entry_high}\n"
                        f"Take Profit: {take_profit}\n"
                        f"Stop Loss: {stop_loss}\n"
                        f"Analysis ID: {analysis_id}\n"  # Log the analysis ID
                        f"Analysis Context ID: {analysis_context_id}\n"
                        f"Analysis Features: {len(analysis_features)} features\n"
                        f"Vol Regime: {vol_regime}"
                    )
                    
                    return setup_id
                    
                except Exception as e:
                    self.logger.error(
                        f"Failed to create trade setup:\n"
                        f"Symbol: {symbol}\n"
                        f"Entry: {entry_low}-{entry_high}\n"
                        f"TP: {take_profit}, SL: {stop_loss}\n"
                        f"Error: {str(e)}"
                    )
                    raise
            
        except Exception as e:
            self.logger.error(f"Failed to save trade setup: {e}", exc_info=True)
            self.logger.error(f"Setup data that caused error: {setup}")
            return None
    
    def _validate_setup_direction(self, direction: str, current_price: float, entry_low: float, entry_high: float) -> None:
        """Validate that setup direction logic is correct relative to current market price
        
        Args:
            direction: 'long' or 'short'
            current_price: Current market price
            entry_low: Lower bound of entry zone
            entry_high: Upper bound of entry zone
            
        Raises:
            ValueError: If direction logic is invalid
        """
        if direction.lower() == 'long':
            # LONG setups: entry zone must be BELOW current price
            # (price needs to drop to enter the position)
            if entry_low > current_price:
                raise ValueError(
                    f"INVALID LONG SETUP: Entry zone {entry_low}-{entry_high} is ABOVE current price {current_price}. "
                    f"LONG setups require entry zones BELOW current price to allow proper entry on pullbacks."
                )
            # Allow some tolerance for near-current price setups
            if entry_low > (current_price - (current_price * 0.001)):  # Within 0.1%
                self.logger.warning(
                    f"RISKY LONG SETUP: Entry zone {entry_low}-{entry_high} very close to current price {current_price}. "
                    f"May trigger immediately without proper pullback."
                )
                
        elif direction.lower() == 'short':
            # SHORT setups: entry zone must be ABOVE current price  
            # (price needs to rise to enter the position)
            if entry_high < current_price:
                raise ValueError(
                    f"INVALID SHORT SETUP: Entry zone {entry_low}-{entry_high} is BELOW current price {current_price}. "
                    f"SHORT setups require entry zones ABOVE current price to allow proper entry on bounces."
                )
            # Allow some tolerance for near-current price setups
            if entry_high < (current_price + (current_price * 0.001)):  # Within 0.1%
                self.logger.warning(
                    f"RISKY SHORT SETUP: Entry zone {entry_low}-{entry_high} very close to current price {current_price}. "
                    f"May trigger immediately without proper bounce."
                )
        else:
            self.logger.warning(f"Unknown direction '{direction}' - cannot validate setup logic")
            
        # Additional sanity checks
        if entry_low >= entry_high:
            raise ValueError(f"Invalid entry zone: entry_low ({entry_low}) must be less than entry_high ({entry_high})")
            
        # Check for reasonable entry zone size (should be between 0.1% and 2% of current price)
        zone_size = entry_high - entry_low
        price_pct = (zone_size / current_price) * 100
        
        if price_pct < 0.05:  # Less than 0.05%
            self.logger.warning(f"Very narrow entry zone: {zone_size} points ({price_pct:.3f}% of price)")
        elif price_pct > 5.0:  # More than 5%
            self.logger.warning(f"Very wide entry zone: {zone_size} points ({price_pct:.1f}% of price)")
            
        self.logger.info(f"✅ Setup direction validation passed: {direction.upper()} setup with entry {entry_low}-{entry_high} vs current {current_price}")

    def _validate_outcome_determination(self, direction: str, exit_price: float, entry_price: float, 
                                      take_profit: float, stop_loss: float, pnl: float) -> str:
        """Cross-validate outcome determination using multiple approaches
        
        Args:
            direction: Trade direction ('long' or 'short')
            exit_price: Price at which trade was exited
            entry_price: Price at which trade was entered
            take_profit: Take profit target price
            stop_loss: Stop loss price
            pnl: Calculated profit/loss
            
        Returns:
            Validated outcome ('profit', 'loss', or 'manual')
        """
        try:
            # Method 1: Standard level-based determination
            if direction.lower() == 'long':
                if exit_price >= take_profit:
                    level_outcome = 'profit'
                elif exit_price <= stop_loss:
                    level_outcome = 'loss'
                else:
                    level_outcome = 'manual'
            else:  # short
                if exit_price <= take_profit:
                    level_outcome = 'profit'
                elif exit_price >= stop_loss:
                    level_outcome = 'loss'
                else:
                    level_outcome = 'manual'
            
            # Method 2: PnL-based validation
            if pnl > 0:
                pnl_outcome = 'profit'
            elif pnl < 0:
                pnl_outcome = 'loss'
            else:
                pnl_outcome = 'manual'  # Breakeven
                
            # Method 3: Price movement validation
            if direction.lower() == 'long':
                favorable_move = exit_price > entry_price
            else:  # short
                favorable_move = exit_price < entry_price
                
            movement_outcome = 'profit' if favorable_move else 'loss'
            
            # Cross-validation logic
            outcomes = [level_outcome, pnl_outcome, movement_outcome]
            
            # For profits and losses, level-based outcome should match PnL outcome
            if level_outcome in ('profit', 'loss'):
                if level_outcome != pnl_outcome:
                    self.logger.warning(
                        f"Outcome inconsistency detected:\n"
                        f"   Level-based: {level_outcome}\n"
                        f"   PnL-based: {pnl_outcome}\n"
                        f"   Direction: {direction}, Entry: {entry_price}, Exit: {exit_price}\n"
                        f"   TP: {take_profit}, SL: {stop_loss}, PnL: {pnl}"
                    )
                    
                    # Additional validation for edge cases
                    if abs(pnl) < 0.01:  # Very small PnL, likely breakeven
                        return 'manual'
                    
                    # If there's a significant PnL discrepancy, trust the levels
                    return level_outcome
                else:
                    # Level and PnL outcomes match - high confidence
                    return level_outcome
            
            # For manual outcomes, verify it's not actually hitting levels
            elif level_outcome == 'manual':
                # Check if we're very close to levels (within 0.25 points for NQ)
                tp_distance = abs(exit_price - take_profit)
                sl_distance = abs(exit_price - stop_loss)
                
                if tp_distance <= 0.25:  # Very close to take profit
                    self.logger.info(f"Manual exit very close to TP (distance: {tp_distance}), classifying as profit")
                    return 'profit'
                elif sl_distance <= 0.25:  # Very close to stop loss
                    self.logger.info(f"Manual exit very close to SL (distance: {sl_distance}), classifying as loss")
                    return 'loss'
                else:
                    return 'manual'
                    
            # Default fallback
            return level_outcome
            
        except Exception as e:
            self.logger.error(f"Error in outcome validation: {e}", exc_info=True)
            # Fallback to simple level-based determination
            if direction.lower() == 'long':
                return 'profit' if exit_price >= take_profit else ('loss' if exit_price <= stop_loss else 'manual')
            else:
                return 'profit' if exit_price <= take_profit else ('loss' if exit_price >= stop_loss else 'manual')

    async def audit_trade_outcomes(self, symbol: str = None, limit: int = 100) -> Dict[str, Any]:
        """Audit existing trade outcomes for accuracy
        
        This method validates existing trade outcomes against the current logic
        to ensure historical data integrity.
        
        Args:
            symbol: Optional symbol to limit audit to (e.g., 'NQ')
            limit: Maximum number of trades to audit
            
        Returns:
            Dictionary with audit results and statistics
        """
        results = {
            'total_audited': 0,
            'correct_outcomes': 0,
            'incorrect_outcomes': 0,
            'discrepancies': [],
            'accuracy_percentage': 0.0,
            'summary': {}
        }
        
        try:
            self.logger.info(f"🔍 Starting trade outcome audit for {symbol or 'all symbols'}")
            await self.ensure_connected()
            
            async with self.pool.acquire() as conn:
                # Build query based on symbol filter
                query = """
                    SELECT 
                        t.id, t.setup_id, t.entry_price, t.exit_price, t.outcome, t.pnl,
                        ts.direction, ts.take_profit, ts.stop_loss, ts.symbol
                    FROM trades t
                    JOIN trade_setups ts ON t.setup_id = ts.id
                    WHERE t.exit_price IS NOT NULL 
                    AND t.outcome IS NOT NULL
                """
                params = []
                
                if symbol:
                    query += " AND ts.symbol = $1"
                    params.append(symbol.replace('.FUT', ''))
                
                query += f" ORDER BY t.updated_at DESC LIMIT {limit}"
                
                trades = await conn.fetch(query, *params)
                results['total_audited'] = len(trades)
                
                if not trades:
                    self.logger.info("No completed trades found for audit")
                    return results
                
                self.logger.info(f"Auditing {len(trades)} completed trades...")
                
                for trade in trades:
                    trade_id = trade['id']
                    direction = trade['direction']
                    entry_price = float(trade['entry_price'])
                    exit_price = float(trade['exit_price'])
                    take_profit = float(trade['take_profit'])
                    stop_loss = float(trade['stop_loss'])
                    recorded_outcome = trade['outcome']
                    recorded_pnl = float(trade['pnl']) if trade['pnl'] else 0.0
                    
                    # Calculate expected PnL
                    expected_pnl = (exit_price - entry_price) if direction == 'long' else (entry_price - exit_price)
                    
                    # Validate outcome using our validation method
                    expected_outcome = self._validate_outcome_determination(
                        direction, exit_price, entry_price, take_profit, stop_loss, expected_pnl
                    )
                    
                    # Check if outcome matches
                    outcome_correct = (recorded_outcome == expected_outcome)
                    pnl_correct = abs(recorded_pnl - expected_pnl) < 0.01  # Allow small rounding differences
                    
                    if outcome_correct and pnl_correct:
                        results['correct_outcomes'] += 1
                    else:
                        results['incorrect_outcomes'] += 1
                        
                        discrepancy = {
                            'trade_id': str(trade_id),
                            'setup_id': str(trade['setup_id']),
                            'symbol': trade['symbol'],
                            'direction': direction,
                            'entry_price': entry_price,
                            'exit_price': exit_price,
                            'take_profit': take_profit,
                            'stop_loss': stop_loss,
                            'recorded_outcome': recorded_outcome,
                            'expected_outcome': expected_outcome,
                            'recorded_pnl': recorded_pnl,
                            'expected_pnl': expected_pnl,
                            'outcome_match': outcome_correct,
                            'pnl_match': pnl_correct,
                            'pnl_difference': abs(recorded_pnl - expected_pnl)
                        }
                        results['discrepancies'].append(discrepancy)
                        
                        self.logger.warning(
                            f"Trade outcome discrepancy found:\n"
                            f"   Trade ID: {trade_id}\n"
                            f"   Direction: {direction}\n"
                            f"   Entry: {entry_price}, Exit: {exit_price}\n"
                            f"   TP: {take_profit}, SL: {stop_loss}\n"
                            f"   Recorded: {recorded_outcome} (PnL: {recorded_pnl})\n"
                            f"   Expected: {expected_outcome} (PnL: {expected_pnl})\n"
                            f"   Outcome Match: {outcome_correct}, PnL Match: {pnl_correct}"
                        )
                
                # Calculate accuracy percentage
                if results['total_audited'] > 0:
                    results['accuracy_percentage'] = (results['correct_outcomes'] / results['total_audited']) * 100
                
                # Generate summary
                results['summary'] = {
                    'total_trades_audited': results['total_audited'],
                    'accuracy_rate': f"{results['accuracy_percentage']:.2f}%",
                    'discrepancies_found': results['incorrect_outcomes'],
                    'symbol_filter': symbol or 'all',
                    'audit_timestamp': datetime.now().isoformat()
                }
                
                self.logger.info(
                    f"✅ Trade outcome audit complete:\n"
                    f"   Total audited: {results['total_audited']}\n"
                    f"   Correct outcomes: {results['correct_outcomes']}\n"
                    f"   Incorrect outcomes: {results['incorrect_outcomes']}\n"
                    f"   Accuracy: {results['accuracy_percentage']:.2f}%\n"
                    f"   Discrepancies: {len(results['discrepancies'])}"
                )
                
        except Exception as e:
            self.logger.error(f"Error in trade outcome audit: {e}", exc_info=True)
            
        return results

    async def get_trade_setups_by_user(self, user_id: str, limit: int = 50) -> List[Dict]:
        """Get trade setups for a specific user with proper tenant context
        
        Args:
            user_id: User ID to get trade setups for
            limit: Maximum number of records to return
            
        Returns:
            List of trade setup dictionaries
        """
        await self.ensure_connected()
        
        try:
            # Convert string to UUID if needed
            user_uuid = user_id if isinstance(user_id, str) else str(user_id)
            
            async with self.pool.acquire() as conn:
                # Set tenant context for this query
                await conn.execute(f"SELECT set_config('app.current_user', '{user_uuid}', TRUE)")
                
                # Query trade setups for the user
                rows = await conn.fetch("""
                    SELECT 
                        id,
                        symbol,
                        status,
                        entry_zone_low,
                        entry_zone_high,
                        take_profit,
                        stop_loss,
                        direction,
                        setup_time,
                        created_at,
                        updated_at,
                        characteristics,
                        user_id
                    FROM trade_setups
                    WHERE user_id = $1
                    ORDER BY created_at DESC
                    LIMIT $2
                """, user_uuid, limit)
                
                # Convert to dictionaries
                setups = []
                for row in rows:
                    setup = dict(row)
                    # Convert UUID to string for JSON serialization
                    setup['id'] = str(setup['id'])
                    setup['user_id'] = str(setup['user_id'])
                    setups.append(setup)
                
                self.logger.info(f"Retrieved {len(setups)} trade setups for user: {user_id}")
                return setups
                
        except Exception as e:
            self.logger.error(f"Failed to get trade setups for user {user_id}: {str(e)}")
            raise