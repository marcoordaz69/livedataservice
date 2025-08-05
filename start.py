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

async def run_with_health_check(main_coro):
    """Run main coroutine alongside health check server"""
    from health_check import run_health_server
    
    # Start health check server as background task
    health_task = asyncio.create_task(run_health_server())
    
    try:
        # Run main launcher
        await main_coro()
    except Exception as e:
        logger.error(f"Main launcher failed: {e}")
        raise
    finally:
        # Cleanup health server
        health_task.cancel()
        try:
            await health_task
        except asyncio.CancelledError:
            pass

if __name__ == "__main__":
    # Check environment first
    if not check_environment():
        logger.error("Environment check failed - exiting")
        sys.exit(1)
    
    # Set explicit event loop policy for consistent behavior
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        logger.info("Starting live data launcher...")
        
        # Import and run the main launcher
        from price_capture.live_data_launcher import main
        
        # For Railway, check if we should run in live-only mode
        if os.getenv('RAILWAY_DEPLOYMENT'):
            logger.info("🚂 Railway deployment detected - running in live-only mode")
            # Modify sys.argv to add --live-only flag
            sys.argv.extend(['--live-only'])
        
        asyncio.run(run_with_health_check(main()))
    except KeyboardInterrupt:
        logger.info("Launcher terminated by user")
    except Exception as e:
        logger.error(f"Fatal error in launcher: {e}", exc_info=True)
        sys.exit(1)