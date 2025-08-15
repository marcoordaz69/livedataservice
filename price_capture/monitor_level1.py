#!/usr/bin/env python3
"""
Monitor Level 1 live data stream - Quick status check
"""
import os
import asyncio
import asyncpg
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def check_live_data_status():
    """Check the status of live Level 1 data stream."""
    print("=" * 60)
    print("LIVE LEVEL 1 DATA STREAM MONITOR")
    print("=" * 60)
    
    try:
        # Connect to database
        host = os.getenv('host')
        user = os.getenv('user')
        password = os.getenv('password')
        port = os.getenv('port', '5432')
        dbname = os.getenv('dbname', 'postgres')
        
        conn = await asyncpg.connect(
            user=user,
            password=password,
            database=dbname,
            host=host,
            port=int(port),
            ssl='require'
        )
        
        # Check recent activity (last 5 minutes)
        recent_query = """
        SELECT 
            symbol,
            COUNT(*) as records_last_5min,
            MIN(bid_price::numeric) as min_bid,
            MAX(bid_price::numeric) as max_bid,
            MIN(ask_price::numeric) as min_ask,
            MAX(ask_price::numeric) as max_ask,
            MAX(timestamp) as latest_update,
            EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) as seconds_ago
        FROM raw_level1 
        WHERE timestamp >= NOW() - INTERVAL '5 minutes'
        GROUP BY symbol 
        ORDER BY latest_update DESC;
        """
        
        recent_data = await conn.fetch(recent_query)
        
        if recent_data:
            print("📊 RECENT ACTIVITY (Last 5 minutes):")
            print("-" * 60)
            for row in recent_data:
                mid_price = (row['min_bid'] + row['max_bid']) / 2
                spread_avg = ((row['max_ask'] - row['min_bid']) + (row['min_ask'] - row['max_bid'])) / 2
                status = "🟢 ACTIVE" if row['seconds_ago'] < 60 else "🟡 SLOW" if row['seconds_ago'] < 300 else "🔴 STALE"
                
                print(f"{status} {row['symbol']}: {row['records_last_5min']:,} records")
                print(f"   Price Range: ${row['min_bid']:.2f} - ${row['max_bid']:.2f}")
                print(f"   Latest: {row['latest_update'].strftime('%H:%M:%S')} ({row['seconds_ago']:.0f}s ago)")
                print()
        else:
            print("🔴 NO RECENT DATA - Service may be stopped or markets closed")
            print()
        
        # Check today's total activity
        today_query = """
        SELECT 
            symbol,
            COUNT(*) as records_today,
            MIN(timestamp) as first_update,
            MAX(timestamp) as last_update
        FROM raw_level1 
        WHERE DATE(timestamp) = CURRENT_DATE
        GROUP BY symbol 
        ORDER BY symbol;
        """
        
        today_data = await conn.fetch(today_query)
        
        if today_data:
            print("📈 TODAY'S TOTAL ACTIVITY:")
            print("-" * 60)
            total_records = 0
            for row in today_data:
                total_records += row['records_today']
                duration = row['last_update'] - row['first_update']
                hours = duration.total_seconds() / 3600
                rate = row['records_today'] / hours if hours > 0 else 0
                
                print(f"{row['symbol']}: {row['records_today']:,} records ({rate:.0f}/hour)")
            
            print(f"\nTotal today: {total_records:,} records")
            print()
        
        # Check historical data summary
        history_query = """
        SELECT 
            symbol,
            COUNT(*) as total_records,
            MIN(DATE(timestamp)) as first_date,
            MAX(DATE(timestamp)) as last_date,
            COUNT(DISTINCT DATE(timestamp)) as trading_days
        FROM raw_level1 
        GROUP BY symbol 
        ORDER BY symbol;
        """
        
        history_data = await conn.fetch(history_query)
        
        if history_data:
            print("🗄️  HISTORICAL DATA SUMMARY:")
            print("-" * 60)
            for row in history_data:
                avg_per_day = row['total_records'] / row['trading_days'] if row['trading_days'] > 0 else 0
                print(f"{row['symbol']}: {row['total_records']:,} records across {row['trading_days']} days")
                print(f"   Date Range: {row['first_date']} to {row['last_date']}")
                print(f"   Average: {avg_per_day:,.0f} records/day")
                print()
        
        await conn.close()
        
    except Exception as e:
        print(f"❌ Error checking data status: {e}")
    
    print("=" * 60)
    print("Monitor completed at", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(check_live_data_status())