#!/usr/bin/env python3
import asyncio
import argparse
import subprocess
import sys
import time
import os
import logging
from datetime import datetime, timedelta, timezone
import asyncpg
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "launcher.log")),
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

async def check_last_data_timestamp():
    """Check the database for the most recent timestamp in the raw_ohlcv table."""
    logger.info("Checking for existing data in the database...")
    
    try:
        conn = await get_db_connection()
        
        # Query the latest timestamp for NQ symbol
        query = "SELECT MAX(timestamp) FROM raw_ohlcv WHERE symbol = 'NQ'"
        latest_timestamp = await conn.fetchval(query)
        await conn.close()
        
        if latest_timestamp:
            # Format timestamp for logging
            latest_timestamp_str = latest_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')
            logger.info(f"Found existing data with latest timestamp: {latest_timestamp_str}")
            
            # Return timestamp plus one minute to avoid duplication
            start_from_timestamp = latest_timestamp + timedelta(minutes=1)
            start_from_timestamp_str = start_from_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"Will start backfill from: {start_from_timestamp_str}")
            
            return start_from_timestamp
        else:
            logger.info("No existing data found in database, will perform full backfill")
            return None
            
    except Exception as e:
        logger.error(f"Error checking existing data: {e}")
        return None

async def run_backfill(days=4):
    """Run the historical backfill process using live_data_service.py.
    
    Args:
        days: Number of days to backfill (default: 4)
    """
    logger.info(f"Starting historical backfill process for the last {days} days...")
    
    try:
        # Check for existing data
        start_timestamp = await check_last_data_timestamp()
        
        # Build command with optional start_time parameter
        live_data_service_path = os.path.join(PRICE_CAPTURE_DIR, "live_data_service.py")
        command = [sys.executable, live_data_service_path, "--backfill-only"]
        
        # Add days parameter
        if days != 4:  # Only pass if different from default
            command.extend(["--days", str(days)])
            
        if start_timestamp:
            timestamp_unix = int(start_timestamp.timestamp())
            command.extend(["--start-time", str(timestamp_unix)])
            logger.info(f"Starting backfill from timestamp {timestamp_unix} ({start_timestamp})")
        else:
            logger.info(f"Starting full {days}-day backfill (no timestamp provided)")
        
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
                logger.info(line)
                # Check for completion markers
                if "Inserted historical records" in line or "Historical backfill completed" in line:
                    backfill_complete = True
                    logger.info("Detected backfill completion marker")
                    break
        
        # If we detected completion, terminate the process
        if backfill_complete:
            terminate_process(process)
            logger.info("Historical backfill completed successfully")
            return True
        else:
            logger.error("Backfill process ended without completion marker")
            return False
            
    except Exception as e:
        logger.error(f"Error during historical backfill: {e}")
        return False

async def run_live_stream():
    """Run the live data streaming process using simple_ohlcv.py."""
    logger.info("Starting live data streaming process with setup validation...")
    
    try:
        # Get the path to simple_ohlcv.py in the price_capture directory
        simple_ohlcv_path = os.path.join(PRICE_CAPTURE_DIR, "simple_ohlcv.py")
        
        # Check if the script exists
        if not os.path.exists(simple_ohlcv_path):
            logger.error(f"simple_ohlcv.py not found at {simple_ohlcv_path}!")
            return None
            
        # Make simple_ohlcv.py executable if it's not
        if not os.access(simple_ohlcv_path, os.X_OK):
            logger.info("Making simple_ohlcv.py executable...")
            os.chmod(simple_ohlcv_path, 0o755)
        
        # Use a dedicated Python process with environment variables properly set up
        # This isolates the asyncio event loop and prevents cross-loop issues
        env = os.environ.copy()
        
        # Set PYTHONPATH to ensure modules can be found
        # Add both current directory and parent directory to Python path
        parent_dir = os.path.dirname(os.getcwd())
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = f"{parent_dir}:{os.getcwd()}:{env['PYTHONPATH']}"
        else:
            env["PYTHONPATH"] = f"{parent_dir}:{os.getcwd()}"
        
        # Set asyncio debug mode to help identify any remaining issues
        env["PYTHONASYNCIODEBUG"] = "1"
        
        # Add PYTHONPATH and PYTHONASYNCIOMETHODNAME environment variables
        # to ensure proper event loop isolation and handling
        env["PYTHONASYNCIOMETHODNAME"] = "loop"
        
        # Start a new process with the environment properly set
        process = subprocess.Popen(
            [sys.executable, "-u", simple_ohlcv_path],  # Use sys.executable to ensure we use the right Python
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
                        logger.info(output.strip())
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error monitoring output: {e}")
                    await asyncio.sleep(1)
        
        # Create the task to monitor output
        monitor_task = asyncio.create_task(monitor_output())
        logger.info("Live data streaming process started")
        
        return process
        
    except Exception as e:
        logger.error(f"Error starting live stream: {e}")
        return None

async def run_setup_monitor():
    """Run the setup monitoring process using monitor_setup_validator.py."""
    logger.info("Starting trade setup monitor...")
    
    try:
        # Check if colorama is installed
        try:
            import colorama
        except ImportError:
            logger.info("Installing colorama package for monitor...")
            subprocess.run([sys.executable, "-m", "pip", "install", "colorama"], check=True)
            
        # Get the path to monitor_setup_validator.py in the price_capture directory
        monitor_path = os.path.join(PRICE_CAPTURE_DIR, "monitor_setup_validator.py")
        
        # Check if the monitoring script exists
        if not os.path.exists(monitor_path):
            logger.error(f"monitor_setup_validator.py not found at {monitor_path}!")
            return None
            
        # Make monitor_setup_validator.py executable if it's not
        if not os.access(monitor_path, os.X_OK):
            logger.info("Making monitor_setup_validator.py executable...")
            os.chmod(monitor_path, 0o755)
        
        # Define the absolute path to the log file
        log_file = os.path.join(PRICE_CAPTURE_DIR, "simple_ohlcv.log")
        logger.info(f"Looking for log file at: {log_file}")
        
        # Wait a moment for the log file to be created
        max_wait = 10  # Maximum wait time in seconds
        wait_time = 0
        
        while not os.path.exists(log_file) and wait_time < max_wait:
            logger.info(f"Waiting for log file to be created... ({wait_time}/{max_wait}s)")
            await asyncio.sleep(1)
            wait_time += 1
            
        if not os.path.exists(log_file):
            logger.warning(f"Log file {log_file} not found after waiting {max_wait}s. Monitor may not work correctly.")
        
        # Set up the environment for the monitor process
        env = os.environ.copy()
        # Add both current directory and parent directory to Python path
        parent_dir = os.path.dirname(os.getcwd())
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = f"{parent_dir}:{os.getcwd()}:{env['PYTHONPATH']}"
        else:
            env["PYTHONPATH"] = f"{parent_dir}:{os.getcwd()}"
            
        # Add event loop isolation settings
        env["PYTHONASYNCIODEBUG"] = "1"
        env["PYTHONASYNCIOMETHODNAME"] = "loop"
        
        # Start the monitor process with proper Python executable and explicitly pass the log file path
        process = subprocess.Popen(
            [sys.executable, "-u", monitor_path, "--log-file", log_file],
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
                        logger.info(output.strip())
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error monitoring output: {e}")
                    await asyncio.sleep(1)
        
        # Create the task to monitor output
        monitor_task = asyncio.create_task(monitor_output())
        logger.info("Trade setup monitor started")
        logger.info(f"Monitoring trade setup validator log file: {log_file}")
        
        return process
        
    except Exception as e:
        logger.error(f"Error starting setup monitor: {e}")
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
    parser = argparse.ArgumentParser(description="Market data collection launcher")
    parser.add_argument("--backfill-only", action="store_true", help="Run only historical backfill")
    parser.add_argument("--live-only", action="store_true", help="Run only live data streaming")
    parser.add_argument("--days", type=int, default=4, help="Number of days to backfill (default: 4)")
    parser.add_argument("--no-monitor", action="store_true", help="Disable the trade setup monitor")
    args = parser.parse_args()
    
    # Check environment variables
    if not ensure_env_vars():
        sys.exit(1)
    
    live_process = None
    monitor_process = None
    
    try:
        if args.backfill_only:
            # Pass the days parameter to backfill
            success = await run_backfill(days=args.days)
            sys.exit(0 if success else 1)
            
        elif args.live_only:
            # Start live streaming
            live_process = await run_live_stream()
            if not live_process:
                logger.error("Failed to start live streaming")
                sys.exit(1)
                
            # Start setup monitor if not disabled
            if not args.no_monitor:
                # Wait a moment for the live stream to initialize
                await asyncio.sleep(5)  # Increased from 2 to 5 seconds for better stability
                
                # Start the setup monitor
                monitor_process = await run_setup_monitor()
                if not monitor_process:
                    logger.warning("Failed to start trade setup monitor, continuing without it")
            
            # Keep running until interrupted
            while True:
                await asyncio.sleep(1)
                
                # Check if live process is still running
                if live_process.poll() is not None:
                    exit_code = live_process.poll()
                    logger.error(f"Live stream process ended unexpectedly with exit code {exit_code}")
                    sys.exit(1)
                
                # Check if monitor process is still running (if it was started)
                if monitor_process and monitor_process.poll() is not None:
                    exit_code = monitor_process.poll()
                    logger.warning(f"Setup monitor process ended with exit code {exit_code}, restarting...")
                    monitor_process = await run_setup_monitor()
                
        else:
            # Run both backfill and live streaming
            logger.info("Starting full data collection process...")
            
            # Run backfill first with specified days
            success = await run_backfill(days=args.days)
            if not success:
                logger.error("Backfill failed, not starting live streaming")
                sys.exit(1)
            
            logger.info("Backfill complete, waiting 5 seconds before starting live stream...")
            await asyncio.sleep(5)  # Add a small delay between backfill and live stream
            
            # Start live streaming
            live_process = await run_live_stream()
            if not live_process:
                logger.error("Failed to start live streaming")
                sys.exit(1)
            
            logger.info("Live streaming process started successfully")
            
            # Start setup monitor if not disabled
            if not args.no_monitor:
                # Wait a moment for the live stream to initialize
                await asyncio.sleep(5)  # Increased from 2 to 5 seconds for better stability
                
                # Start the setup monitor
                monitor_process = await run_setup_monitor()
                if not monitor_process:
                    logger.warning("Failed to start trade setup monitor, continuing without it")
            
            # Keep running until interrupted
            while True:
                await asyncio.sleep(1)
                
                # Check if live process is still running
                if live_process.poll() is not None:
                    exit_code = live_process.poll()
                    logger.error(f"Live stream process ended unexpectedly with exit code {exit_code}")
                    sys.exit(1)
                
                # Check if monitor process is still running (if it was started)
                if monitor_process and monitor_process.poll() is not None:
                    exit_code = monitor_process.poll()
                    logger.warning(f"Setup monitor process ended with exit code {exit_code}, restarting...")
                    monitor_process = await run_setup_monitor()
                    
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Terminate processes on shutdown
        if monitor_process:
            terminate_process(monitor_process)
            logger.info("Setup monitor process terminated")
            
        if live_process:
            terminate_process(live_process)
            logger.info("Live stream process terminated")
            
        logger.info("Launcher shutdown complete")

if __name__ == "__main__":
    # Set explicit event loop policy for consistent behavior
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Launcher terminated by user") 