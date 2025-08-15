#!/usr/bin/env python3
"""
Market Hours Helper - Determines if markets are open for trading
"""
from datetime import datetime, timezone, time, timedelta
import pytz

def is_market_open():
    """
    Check if futures markets are currently open.
    
    CME Globex Trading Hours (for NQ and ES):
    - Sunday: 6:00 PM ET to Friday 5:00 PM ET
    - Daily break: 5:00 PM - 6:00 PM ET
    
    Returns:
        bool: True if market is open, False otherwise
    """
    # Get current time in ET
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    
    # Get day of week (0=Monday, 6=Sunday)
    weekday = now_et.weekday()
    current_time = now_et.time()
    
    # Market closed times
    if weekday == 5:  # Saturday
        return False
    elif weekday == 6:  # Sunday
        # Market opens at 6:00 PM ET on Sunday
        return current_time >= time(18, 0)
    elif weekday == 4:  # Friday
        # Market closes at 5:00 PM ET on Friday
        return current_time < time(17, 0)
    else:  # Monday through Thursday
        # Daily break from 5:00 PM to 6:00 PM ET
        if time(17, 0) <= current_time < time(18, 0):
            return False
        return True

def get_next_market_open():
    """
    Get the next market open time.
    
    Returns:
        datetime: Next market open time in UTC
    """
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    weekday = now_et.weekday()
    current_time = now_et.time()
    
    # If it's during the daily break (5-6 PM ET)
    if weekday != 5 and weekday != 6:  # Not Saturday or Sunday
        if time(17, 0) <= current_time < time(18, 0):
            # Market reopens at 6 PM today
            next_open = now_et.replace(hour=18, minute=0, second=0, microsecond=0)
            return next_open.astimezone(timezone.utc)
    
    # If it's Friday after 5 PM or Saturday
    if weekday == 4 and current_time >= time(17, 0):
        # Next open is Sunday 6 PM
        days_ahead = 2
        next_open = now_et + timedelta(days=days_ahead)
        next_open = next_open.replace(hour=18, minute=0, second=0, microsecond=0)
        return next_open.astimezone(timezone.utc)
    elif weekday == 5:  # Saturday
        # Next open is Sunday 6 PM
        days_ahead = 1
        next_open = now_et + timedelta(days=days_ahead)
        next_open = next_open.replace(hour=18, minute=0, second=0, microsecond=0)
        return next_open.astimezone(timezone.utc)
    elif weekday == 6 and current_time < time(18, 0):
        # Market opens at 6 PM today
        next_open = now_et.replace(hour=18, minute=0, second=0, microsecond=0)
        return next_open.astimezone(timezone.utc)
    
    # Market is currently open or will open at 6 PM today
    if current_time < time(18, 0):
        next_open = now_et.replace(hour=18, minute=0, second=0, microsecond=0)
    else:
        # Next break is at 5 PM tomorrow, reopens at 6 PM
        next_open = (now_et + timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0)
    
    return next_open.astimezone(timezone.utc)

def is_extended_hours():
    """
    Check if we're in extended/overnight trading hours.
    Regular hours: 9:30 AM - 4:00 PM ET
    Extended includes overnight and pre/post market
    
    Returns:
        bool: True if in extended hours, False if regular hours
    """
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    current_time = now_et.time()
    
    # Regular trading hours are 9:30 AM to 4:00 PM ET
    regular_start = time(9, 30)
    regular_end = time(16, 0)
    
    return not (regular_start <= current_time < regular_end)

if __name__ == "__main__":
    from datetime import timedelta
    
    print(f"Market open: {is_market_open()}")
    print(f"Extended hours: {is_extended_hours()}")
    print(f"Next market open: {get_next_market_open()}")