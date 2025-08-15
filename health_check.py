#!/usr/bin/env python3
"""
Simple HTTP health check server for Railway deployment monitoring
"""
import asyncio
import logging
from aiohttp import web
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class HealthCheckServer:
    def __init__(self, port=8080):
        self.port = port
        self.app = web.Application()
        self.setup_routes()
        self.start_time = datetime.utcnow()
        
    def setup_routes(self):
        self.app.router.add_get('/health', self.health_check)
        self.app.router.add_get('/status', self.status_check)
        
    async def health_check(self, request):
        """Simple health check endpoint"""
        return web.json_response({
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "uptime_seconds": (datetime.utcnow() - self.start_time).total_seconds()
        })
        
    async def status_check(self, request):
        """Detailed status check with service information"""
        try:
            # Check database connectivity
            from price_capture.live_data_launcher import get_db_connection
            try:
                conn = await get_db_connection()
                await conn.close()
                db_status = "connected"
            except Exception as e:
                db_status = f"error: {str(e)}"
            
            # Check if Level 1 script is available
            level1_path = os.path.join(os.path.dirname(__file__), "price_capture", "simple_level1.py")
            level1_available = os.path.exists(level1_path)
                
            return web.json_response({
                "status": "running",
                "database": db_status,
                "level1_script": "available" if level1_available else "missing",
                "services": {
                    "ohlcv": "enabled",
                    "level1": "enabled" if level1_available else "disabled",
                    "monitor": "enabled"
                },
                "environment": "railway" if os.getenv('RAILWAY_DEPLOYMENT') else "local",
                "uptime_seconds": (datetime.utcnow() - self.start_time).total_seconds(),
                "timestamp": datetime.utcnow().isoformat()
            })
        except Exception as e:
            return web.json_response({
                "status": "error",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            }, status=500)
            
    async def start_server(self):
        """Start the health check server"""
        runner = web.AppRunner(self.app)
        await runner.setup()
        
        site = web.TCPSite(runner, '0.0.0.0', self.port)
        await site.start()
        
        logger.info(f"Health check server started on port {self.port}")
        return runner
        
async def run_health_server():
    """Run the health check server as a background task"""
    port = int(os.getenv('PORT', 8080))  # Railway provides PORT env var
    server = HealthCheckServer(port)
    runner = await server.start_server()
    
    try:
        # Keep server running
        while True:
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        logger.info("Health check server cancelled")
    finally:
        await runner.cleanup()