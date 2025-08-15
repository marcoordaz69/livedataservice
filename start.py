#!/usr/bin/env python3
"""
Simple start script for Railway deployment
"""
import sys
import os
import asyncio
import logging

# Configure logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def check_environment():
    """Check and log environment variables for debugging."""
    logger.info("=== RAILWAY ENVIRONMENT CHECK ===")
    
    required_vars = ["DATABENTO_API_KEY", "host", "user", "password"]
    optional_vars = ["port", "dbname", "PORT"]
    
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if value:
            logger.info(f"✅ {var}: {'*' * 8} (set)")
        else:
            logger.error(f"❌ {var}: NOT SET")
            missing_vars.append(var)
    
    for var in optional_vars:
        value = os.getenv(var)
        if value:
            logger.info(f"ℹ️  {var}: {value}")
        else:
            logger.info(f"ℹ️  {var}: not set (will use default)")
    
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these in Railway dashboard: Settings → Environment Variables")
        return False
    
    logger.info("✅ All required environment variables are set")
    return True

async def run_with_health_check(use_monitor=False):
    """Run services alongside health check server"""
    from health_check import run_health_server
    
    # Start health check server as background task FIRST
    health_task = asyncio.create_task(run_health_server())
    
    # Give health server time to start
    await asyncio.sleep(2)
    logger.info("Health check server should be running...")
    
    try:
        if use_monitor:
            # Use the new service monitor for better reliability
            logger.info("Starting service monitor for improved reliability...")
            from price_capture.service_monitor import ServiceMonitor
            monitor = ServiceMonitor()
            await monitor.start()
        else:
            # Use the original launcher
            from price_capture.live_data_launcher import main
            await main()
    except Exception as e:
        logger.error(f"Service failed: {e}")
        # Keep health server running even if main fails
        logger.info("Keeping health server running despite service failure...")
        try:
            while True:
                await asyncio.sleep(60)
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
    finally:
        # Cleanup health server
        logger.info("Shutting down health server...")
        health_task.cancel()
        try:
            await health_task
        except asyncio.CancelledError:
            pass

if __name__ == "__main__":
    # Check environment first, but don't exit on Railway if some vars missing
    env_check_passed = check_environment()
    if not env_check_passed:
        if os.getenv('RAILWAY_DEPLOYMENT'):
            logger.warning("Environment check failed on Railway - continuing anyway to start health server...")
        else:
            logger.error("Environment check failed - exiting")
            sys.exit(1)
    
    # Set default symbols for Railway deployment if not already set
    if os.getenv('RAILWAY_DEPLOYMENT') and not os.getenv('ENABLED_SYMBOLS'):
        os.environ['ENABLED_SYMBOLS'] = 'NQ,ES'
        logger.info("🎯 Railway deployment: Enabled symbols NQ,ES")
    
    # Set explicit event loop policy for consistent behavior
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        logger.info("Starting live data service...")
        
        # For Railway, use the service monitor for better reliability
        use_monitor = os.getenv('RAILWAY_DEPLOYMENT') or os.getenv('USE_SERVICE_MONITOR')
        
        if use_monitor:
            logger.info("🚂 Railway/Monitor mode - using service monitor for improved reliability")
            asyncio.run(run_with_health_check(use_monitor=True))
        else:
            # Use original launcher for local development
            logger.info("Local mode - using standard launcher")
            # Modify sys.argv to add --live-only and --enable-level1 flags
            sys.argv.extend(['--live-only', '--enable-level1'])
            asyncio.run(run_with_health_check(use_monitor=False))
    except KeyboardInterrupt:
        logger.info("Launcher terminated by user")
    except Exception as e:
        logger.error(f"Fatal error in launcher: {e}", exc_info=True)
        sys.exit(1)