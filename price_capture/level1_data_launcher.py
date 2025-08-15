#!/usr/bin/env python3
"""
Level 1 Data Collection Launcher with Smart Backfill
Ensures data continuity by filling gaps before starting live stream
"""
import asyncio
import argparse
import subprocess
import sys
import time
import os
import logging
import signal
from datetime import datetime, timedelta, timezone
import asyncpg
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "level1_launcher.log")),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get the path to the price_capture directory
PRICE_CAPTURE_DIR = os.path.dirname(os.path.abspath(__file__))

def ensure_env_vars():
    """Ensure all required environment variables are set."""
    required_vars = [
        "DATABENTO_API_KEY",
        "host",
        "user",
        "password"
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        return False
    return True

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
                    'application_name': 'level1_launcher'
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
            
            # Return timestamp plus one second to avoid duplication
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

async def run_level1_backfill(days=4):
    """Run the Level 1 historical backfill process using level1_backfill_service.py.
    
    Args:
        days: Number of days to backfill (default: 4)
    """
    logger.info(f"Starting Level 1 historical backfill process for the last {days} days...")
    
    try:
        # Check for existing data
        start_timestamp = await check_last_level1_timestamp()
        
        # Build command with optional start_time parameter
        backfill_service_path = os.path.join(PRICE_CAPTURE_DIR, "level1_backfill_service.py")
        command = [sys.executable, backfill_service_path, "--backfill-only"]
        
        # Add days parameter
        if days != 4:  # Only pass if different from default
            command.extend(["--days", str(days)])
            
        if start_timestamp:
            timestamp_unix = int(start_timestamp.timestamp())
            command.extend(["--start-time", str(timestamp_unix)])
            logger.info(f"Starting Level 1 backfill from timestamp {timestamp_unix} ({start_timestamp})")
        else:
            logger.info(f"Starting full {days}-day Level 1 backfill (no timestamp provided)")
        
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        
        backfill_complete = False
        
        # Monitor the process output
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                line = output.strip()
                logger.info(f"[BACKFILL] {line}")
                # Check for completion markers
                if ("Historical Level 1 backfill completed" in line or 
                    "Level 1 backfill service completed" in line or
                    "Level 1 backfill not needed - data is current" in line):
                    backfill_complete = True
                    logger.info("Detected Level 1 backfill completion marker")
                    break
        
        # If we detected completion, terminate the process
        if backfill_complete:
            terminate_process(process)
            logger.info("Level 1 historical backfill completed successfully")
            return True
        else:
            logger.error("Level 1 backfill process ended without completion marker")
            return False
            
    except Exception as e:
        logger.error(f"Error during Level 1 historical backfill: {e}")
        return False

async def run_level1_live_stream():
    """Run the Level 1 live data streaming process using simple_level1.py."""
    logger.info("Starting Level 1 live data streaming process...")
    
    try:
        # Get the path to simple_level1.py in the price_capture directory
        simple_level1_path = os.path.join(PRICE_CAPTURE_DIR, "simple_level1.py")
        
        # Check if the script exists
        if not os.path.exists(simple_level1_path):
            logger.error(f"simple_level1.py not found at {simple_level1_path}!")
            return None
            
        # Make simple_level1.py executable if it's not
        if not os.access(simple_level1_path, os.X_OK):
            logger.info("Making simple_level1.py executable...")
            os.chmod(simple_level1_path, 0o755)
        
        # Set up environment for Level 1 process
        env = os.environ.copy()
        env["ENABLED_SYMBOLS"] = "NQ,ES"  # Set symbols
        
        # Set PYTHONPATH to ensure modules can be found
        parent_dir = os.path.dirname(os.getcwd())
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = f"{parent_dir}:{os.getcwd()}:{env['PYTHONPATH']}"
        else:
            env["PYTHONPATH"] = f"{parent_dir}:{os.getcwd()}"
        
        # Set asyncio debug mode
        env["PYTHONASYNCIODEBUG"] = "1"
        env["PYTHONASYNCIOMETHODNAME"] = "loop"
        
        # Start Level 1 live stream process
        process = subprocess.Popen(
            [sys.executable, "-u", simple_level1_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            env=env
        )
        
        # Start a task to monitor the process output
        async def monitor_output():
            while True:
                try:
                    output = process.stdout.readline()
                    if output == '' and process.poll() is not None:
                        break
                    if output:
                        logger.info(f"[LIVE] {output.strip()}")
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error monitoring Level 1 live output: {e}")
                    await asyncio.sleep(1)
        
        # Create the task to monitor output
        monitor_task = asyncio.create_task(monitor_output())
        logger.info("Level 1 live data streaming process started")
        
        return process
        
    except Exception as e:
        logger.error(f"Error starting Level 1 live stream: {e}")
        return None

def terminate_process(process):
    """Safely terminate a process."""
    if process and process.poll() is None:
        logger.info("Terminating process...")
        process.terminate()
        try:
            process.wait(timeout=5)  # Wait up to 5 seconds for graceful shutdown
        except subprocess.TimeoutExpired:
            logger.warning("Process did not terminate gracefully, forcing...")
            process.kill()

async def main():
    parser = argparse.ArgumentParser(description="Level 1 data collection launcher")
    parser.add_argument("--backfill-only", action="store_true", help="Run only Level 1 historical backfill")
    parser.add_argument("--live-only", action="store_true", help="Run only Level 1 live data streaming")
    parser.add_argument("--days", type=float, default=4, help="Number of days to backfill (default: 4)")
    args = parser.parse_args()
    
    # Check environment variables
    if not ensure_env_vars():
        sys.exit(1)
    
    live_process = None
    shutdown_event = asyncio.Event()
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(signame):
        logger.info(f"Received {signame}, initiating graceful shutdown...")
        shutdown_event.set()
    
    # Register signal handlers
    if sys.platform != 'win32':
        for sig in [signal.SIGTERM, signal.SIGINT]:
            asyncio.get_event_loop().add_signal_handler(
                sig, lambda s=sig: signal_handler(signal.Signals(s).name)
            )
    
    try:
        if args.backfill_only:
            # Run only Level 1 backfill
            success = await run_level1_backfill(days=args.days)
            sys.exit(0 if success else 1)
            
        elif args.live_only:
            # Start only Level 1 live streaming
            live_process = await run_level1_live_stream()
            if not live_process:
                logger.error("Failed to start Level 1 live streaming")
                sys.exit(1)
            
            # Keep running until shutdown signal or process failure
            while not shutdown_event.is_set():
                await asyncio.sleep(1)
                
                # Check if live process is still running
                if live_process.poll() is not None:
                    exit_code = live_process.poll()
                    logger.error(f"Level 1 live stream process ended unexpectedly with exit code {exit_code}")
                    break
                
        else:
            # Run both backfill and live streaming (default behavior)
            logger.info("Starting full Level 1 data collection process...")
            
            # Run backfill first with specified days
            success = await run_level1_backfill(days=args.days)
            if not success:
                logger.error("Level 1 backfill failed, not starting live streaming")
                sys.exit(1)
            
            logger.info("Level 1 backfill complete, waiting 5 seconds before starting live stream...")
            await asyncio.sleep(5)  # Add a small delay between backfill and live stream
            
            # Start Level 1 live streaming
            live_process = await run_level1_live_stream()
            if not live_process:
                logger.error("Failed to start Level 1 live streaming")
                sys.exit(1)
            
            logger.info("Level 1 live streaming process started successfully")
            
            # Keep running until shutdown signal or process failure
            while not shutdown_event.is_set():
                await asyncio.sleep(1)
                
                # Check if live process is still running
                if live_process.poll() is not None:
                    exit_code = live_process.poll()
                    logger.error(f"Level 1 live stream process ended unexpectedly with exit code {exit_code}")
                    break
                    
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Terminate processes on shutdown
        if live_process:
            terminate_process(live_process)
            logger.info("Level 1 live stream process terminated")
            
        logger.info("Level 1 launcher shutdown complete")

if __name__ == "__main__":
    # Set explicit event loop policy for consistent behavior
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Level 1 launcher terminated by user")