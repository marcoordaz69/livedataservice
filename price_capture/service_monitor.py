#!/usr/bin/env python3
"""
Service Monitor - Monitors and restarts data collection services
Ensures continuous operation even if individual services fail
"""
import asyncio
import asyncpg
import os
import logging
import sys
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import subprocess
import signal
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "service_monitor.log")),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get the path to the price_capture directory
PRICE_CAPTURE_DIR = os.path.dirname(os.path.abspath(__file__))

class ServiceMonitor:
    def __init__(self):
        self.services = {}
        self.db_pool = None
        self.running = True
        self.last_data_check = {}
        self.restart_counts = {}
        self.max_restarts = 10
        self.check_interval = 30  # Check every 30 seconds
        self.data_timeout = 300  # 5 minutes without data triggers restart
        
    async def create_db_pool(self):
        """Create database connection pool."""
        if self.db_pool:
            return self.db_pool
            
        host = os.getenv('host')
        user = os.getenv('user')
        password = os.getenv('password')
        port = os.getenv('port', '5432')
        dbname = os.getenv('dbname', 'postgres')
        
        if not all([host, user, password]):
            raise ValueError("Missing required database environment variables")
        
        self.db_pool = await asyncpg.create_pool(
            user=user,
            password=password,
            database=dbname,
            host=host,
            port=int(port),
            ssl='require',
            min_size=2,
            max_size=5,
            command_timeout=30
        )
        
        logger.info("Database connection pool created")
        return self.db_pool
        
    async def check_recent_data(self, table_name, symbol):
        """Check if we have recent data in the database."""
        try:
            pool = await self.create_db_pool()
            async with pool.acquire() as conn:
                query = f"""
                    SELECT MAX(timestamp) as last_update,
                           COUNT(*) as recent_count
                    FROM {table_name}
                    WHERE symbol = $1
                      AND timestamp > NOW() - INTERVAL '10 minutes'
                """
                result = await conn.fetchrow(query, symbol)
                
                if result and result['last_update']:
                    seconds_ago = (datetime.now(timezone.utc) - result['last_update']).total_seconds()
                    return {
                        'has_recent_data': seconds_ago < self.data_timeout,
                        'seconds_since_update': seconds_ago,
                        'recent_count': result['recent_count']
                    }
                    
                return {
                    'has_recent_data': False,
                    'seconds_since_update': float('inf'),
                    'recent_count': 0
                }
                
        except Exception as e:
            logger.error(f"Error checking recent data: {e}")
            return {
                'has_recent_data': True,  # Assume OK if can't check
                'seconds_since_update': 0,
                'recent_count': 0
            }
            
    def start_service(self, service_name, script_path):
        """Start a service subprocess."""
        try:
            if service_name in self.services and self.services[service_name].poll() is None:
                logger.info(f"{service_name} is already running")
                return self.services[service_name]
                
            env = os.environ.copy()
            
            # Ensure Python can find modules
            parent_dir = os.path.dirname(PRICE_CAPTURE_DIR)
            if "PYTHONPATH" in env:
                env["PYTHONPATH"] = f"{parent_dir}:{PRICE_CAPTURE_DIR}:{env['PYTHONPATH']}"
            else:
                env["PYTHONPATH"] = f"{parent_dir}:{PRICE_CAPTURE_DIR}"
            
            # Start the service
            process = subprocess.Popen(
                [sys.executable, "-u", script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                env=env,
                preexec_fn=os.setsid if sys.platform != 'win32' else None
            )
            
            self.services[service_name] = process
            self.restart_counts[service_name] = self.restart_counts.get(service_name, 0) + 1
            
            logger.info(f"Started {service_name} (PID: {process.pid}, restart #{self.restart_counts[service_name]})")
            return process
            
        except Exception as e:
            logger.error(f"Failed to start {service_name}: {e}")
            return None
            
    def stop_service(self, service_name):
        """Stop a service subprocess."""
        if service_name not in self.services:
            return
            
        process = self.services[service_name]
        if process and process.poll() is None:
            try:
                if sys.platform != 'win32':
                    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                else:
                    process.terminate()
                    
                process.wait(timeout=5)
                logger.info(f"Stopped {service_name}")
            except Exception as e:
                logger.error(f"Error stopping {service_name}: {e}")
                try:
                    process.kill()
                except:
                    pass
                    
        del self.services[service_name]
        
    async def monitor_service_output(self, service_name):
        """Monitor output from a service subprocess."""
        if service_name not in self.services:
            return
            
        process = self.services[service_name]
        
        try:
            while process.poll() is None and self.running:
                try:
                    output = process.stdout.readline()
                    if output:
                        logger.info(f"[{service_name}] {output.strip()}")
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error reading {service_name} output: {e}")
                    await asyncio.sleep(1)
                    
        except Exception as e:
            logger.error(f"Monitor error for {service_name}: {e}")
            
    async def check_and_restart_services(self):
        """Check services health and restart if needed."""
        # Import market hours helper
        try:
            from market_hours_helper import is_market_open, get_next_market_open
            market_hours_available = True
        except ImportError:
            logger.warning("Market hours helper not available, assuming market is always open")
            market_hours_available = False
            is_market_open = lambda: True
            
        while self.running:
            try:
                await asyncio.sleep(self.check_interval)
                
                # Check if market is open (skip restart logic if closed)
                market_open = is_market_open() if market_hours_available else True
                
                if not market_open:
                    # Market is closed, don't restart for stale data
                    if int(time.time()) % 300 < self.check_interval:
                        logger.info("Market is closed - services on standby")
                    # Still check if processes are alive
                    for service_name in list(self.services.keys()):
                        process = self.services[service_name]
                        if process.poll() is not None:
                            logger.warning(f"{service_name} process died during market close")
                            # Restart even during market close to be ready
                            if self.restart_counts.get(service_name, 0) < self.max_restarts:
                                await asyncio.sleep(2)
                                if service_name == 'level1':
                                    script_path = os.path.join(PRICE_CAPTURE_DIR, "simple_level1.py")
                                else:
                                    script_path = os.path.join(PRICE_CAPTURE_DIR, "simple_ohlcv.py")
                                self.start_service(service_name, script_path)
                                asyncio.create_task(self.monitor_service_output(service_name))
                    continue
                
                # Check Level 1 service
                level1_status = await self.check_recent_data('raw_level1', 'NQ')
                
                if not level1_status['has_recent_data']:
                    logger.warning(f"Level 1 data stale ({level1_status['seconds_since_update']:.0f}s old)")
                    
                    if self.restart_counts.get('level1', 0) < self.max_restarts:
                        logger.info("Restarting Level 1 service...")
                        self.stop_service('level1')
                        await asyncio.sleep(2)
                        level1_path = os.path.join(PRICE_CAPTURE_DIR, "simple_level1.py")
                        self.start_service('level1', level1_path)
                        asyncio.create_task(self.monitor_service_output('level1'))
                    else:
                        logger.error(f"Level 1 service exceeded max restarts ({self.max_restarts})")
                        
                # Check OHLCV service
                ohlcv_status = await self.check_recent_data('raw_ohlcv', 'NQ')
                
                if not ohlcv_status['has_recent_data']:
                    logger.warning(f"OHLCV data stale ({ohlcv_status['seconds_since_update']:.0f}s old)")
                    
                    if self.restart_counts.get('ohlcv', 0) < self.max_restarts:
                        logger.info("Restarting OHLCV service...")
                        self.stop_service('ohlcv')
                        await asyncio.sleep(2)
                        ohlcv_path = os.path.join(PRICE_CAPTURE_DIR, "simple_ohlcv.py")
                        self.start_service('ohlcv', ohlcv_path)
                        asyncio.create_task(self.monitor_service_output('ohlcv'))
                    else:
                        logger.error(f"OHLCV service exceeded max restarts ({self.max_restarts})")
                        
                # Check if processes are still running
                for service_name in list(self.services.keys()):
                    process = self.services[service_name]
                    if process.poll() is not None:
                        logger.warning(f"{service_name} process died (exit code: {process.returncode})")
                        
                        if self.restart_counts.get(service_name, 0) < self.max_restarts:
                            await asyncio.sleep(2)
                            if service_name == 'level1':
                                script_path = os.path.join(PRICE_CAPTURE_DIR, "simple_level1.py")
                            else:
                                script_path = os.path.join(PRICE_CAPTURE_DIR, "simple_ohlcv.py")
                                
                            self.start_service(service_name, script_path)
                            asyncio.create_task(self.monitor_service_output(service_name))
                            
                # Log status every 5 minutes
                if int(time.time()) % 300 < self.check_interval:
                    logger.info(f"Monitor Status - Level1: {level1_status['recent_count']} records, "
                              f"OHLCV: {ohlcv_status['recent_count']} records")
                    logger.info(f"Restart counts - Level1: {self.restart_counts.get('level1', 0)}, "
                              f"OHLCV: {self.restart_counts.get('ohlcv', 0)}")
                    
            except Exception as e:
                logger.error(f"Error in service monitor loop: {e}")
                await asyncio.sleep(10)
                
    async def start(self):
        """Start the service monitor."""
        logger.info("=== SERVICE MONITOR STARTING ===")
        
        # Initialize database pool
        await self.create_db_pool()
        
        # Start services
        level1_path = os.path.join(PRICE_CAPTURE_DIR, "simple_level1.py")
        ohlcv_path = os.path.join(PRICE_CAPTURE_DIR, "simple_ohlcv.py")
        
        # Check if scripts exist
        if os.path.exists(level1_path):
            self.start_service('level1', level1_path)
            asyncio.create_task(self.monitor_service_output('level1'))
        else:
            logger.warning(f"Level 1 script not found: {level1_path}")
            
        if os.path.exists(ohlcv_path):
            self.start_service('ohlcv', ohlcv_path)
            asyncio.create_task(self.monitor_service_output('ohlcv'))
        else:
            logger.warning(f"OHLCV script not found: {ohlcv_path}")
            
        # Start monitoring loop
        monitor_task = asyncio.create_task(self.check_and_restart_services())
        
        try:
            await monitor_task
        except KeyboardInterrupt:
            logger.info("Shutting down service monitor...")
            self.running = False
            
        finally:
            # Stop all services
            for service_name in list(self.services.keys()):
                self.stop_service(service_name)
                
            # Close database pool
            if self.db_pool:
                await self.db_pool.close()
                
            logger.info("Service monitor stopped")
            
async def main():
    """Main entry point."""
    monitor = ServiceMonitor()
    await monitor.start()
    
if __name__ == "__main__":
    # Set up signal handlers
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        sys.exit(0)
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run the monitor
    asyncio.run(main())