# db.py - This file handles connecting to our database
import os
import asyncpg
import asyncio
import logging
from dotenv import load_dotenv

# Configure logging
logger = logging.getLogger(__name__)

# Load our environment variables (like database URL)
load_dotenv()

# Build connection string - prioritize DATABASE_URL/SUPABASE_DB_URL over individual parts
def get_connection_string():
    # First, try to use a complete DATABASE_URL or SUPABASE_DB_URL
    database_url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL") or os.getenv("TRADING_DB_URI")
    
    if database_url:
        logger.info("Using DATABASE_URL/SUPABASE_DB_URL for connection")
        return database_url
    
    # Fallback to building from individual parts
    logger.info("Building connection string from individual environment variables")
    user = os.getenv("user", "postgres")
    password = os.getenv("password", "postgres")
    host = os.getenv("host", "localhost")
    port = os.getenv("port")
    dbname = os.getenv("dbname", "trading")
    
    # Handle port separately - it must be an integer
    if port is not None:
        try:
            port = int(port)
        except (ValueError, TypeError):
            # If port is not a valid integer, use the default PostgreSQL port
            logger.warning(f"Invalid port value: {port}, using default port 5432")
            port = 5432
    else:
        # If port is None, use the default PostgreSQL port
        port = 5432
    
    # Build the connection string with the validated values
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

# We'll use this to store our database connection
pool = None
pool_lock = asyncio.Lock()
pool_init_in_progress = False

async def init_db_pool():
    """Create a connection pool to the database"""
    global pool, pool_init_in_progress
    
    # Use lock to prevent concurrent pool creation
    async with pool_lock:
        try:
            # Check if we have a valid pool
            if pool and not getattr(pool, '_closed', True):
                logger.debug("Reusing existing database pool")
                return pool
                
            # Set flag to indicate initialization is happening
            pool_init_in_progress = True
            logger.info("Creating new database connection pool")
            
            # Connection settings for Supabase compatibility
            server_settings = {
                'application_name': 'trading-app-analysis',
                'jit': 'off'  # Disable JIT for stability with pooled connections
            }
            
            # Retry logic for connection establishment
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    logger.info(f"Database connection attempt {attempt + 1}/{max_retries}")
                    
                    # Create a new pool with proper timeout settings
                    pool = await asyncpg.create_pool(
                        dsn=get_connection_string(),
                        min_size=2,                    # Minimum connections
                        max_size=10,                   # Maximum connections
                        # Connection health settings
                        max_queries=50000,             # Max queries per connection before recycling
                        max_inactive_connection_lifetime=300.0,  # 5 minutes
                        # Individual connection parameters
                        timeout=30.0,                  # Connection establishment timeout
                        command_timeout=60.0,          # Query execution timeout
                        server_settings=server_settings
                    )
                    
                    # Test the pool with a simple query
                    async with pool.acquire() as conn:
                        await conn.fetchval("SELECT 1")
                    
                    logger.info("✅ Database pool connected!")
                    print("✅ Database pool connected!")
                    return pool
                    
                except asyncio.TimeoutError as e:
                    logger.warning(f"Connection attempt {attempt + 1} timed out: {e}")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        raise
                except Exception as e:
                    logger.error(f"Connection attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        raise
            
        except Exception as e:
            logger.error(f"❌ Failed to create database pool: {str(e)}")
            print(f"❌ Failed to create database pool: {str(e)}")
            # Clean up partial pool if exists
            if pool and not getattr(pool, '_closed', True):
                try:
                    await pool.close()
                except:
                    pass
                pool = None
            raise
        finally:
            pool_init_in_progress = False

async def close_db_pool():
    """Close the database connection pool"""
    global pool
    
    # Use lock to prevent concurrent pool operations
    async with pool_lock:
        if pool:
            logger.info("Closing database connection pool")
            await pool.close()
            pool = None
            print("Database pool closed")
            logger.info("Database pool closed")

async def ensure_connected():
    """Ensure we have a valid database connection"""
    global pool
    
    if not pool or getattr(pool, '_closed', True):
        await init_db_pool()
    
    return pool

async def test_db_connection():
    """Test database connectivity with detailed logging"""
    logger.info("Testing database connection...")
    try:
        # Test connection string construction
        connection_string = get_connection_string()
        logger.info(f"Connection string format: postgresql://user:***@host:port/dbname")
        
        # Test basic connectivity
        test_pool = await asyncpg.create_pool(
            dsn=connection_string,
            min_size=1,
            max_size=2,
            timeout=15.0,                # Connection establishment timeout
            command_timeout=30.0         # Query execution timeout
        )
        
        # Test query execution
        async with test_pool.acquire() as conn:
            result = await conn.fetchval("SELECT current_timestamp")
            logger.info(f"✅ Database connectivity test passed! Server time: {result}")
        
        await test_pool.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Database connectivity test failed: {e}")
        return False