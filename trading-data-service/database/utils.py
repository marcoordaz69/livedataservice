# database/utils.py
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID
from typing import Dict, Any, Union, Optional

def get_utc_now() -> datetime:
    """Get current UTC time with timezone info"""
    return datetime.now(timezone.utc)

def format_price(price: float) -> float:
    """Format price for database storage - now just returns the number as-is"""
    if price is None:
        return None
    return float(price)

def format_display_price(price: float) -> str:
    """Format price for display"""
    if price is None:
        return "N/A"
    return f"{float(price):.2f}"

def format_trade_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Format trade data for display"""
    return {
        'id': str(data['id']) if isinstance(data['id'], UUID) else data['id'],
        'entry_time': data['entry_time'].isoformat() if data['entry_time'] else None,
        'exit_time': data['exit_time'].isoformat() if data['exit_time'] else None,
        'outcome': data['outcome'],
        'pnl': float(data['pnl']) if data['pnl'] else None,
        'entry_zone': {
            'low': float(data['entry_zone'].lower),
            'high': float(data['entry_zone'].upper)
        } if 'entry_zone' in data else None,
        'take_profit': float(data['take_profit']) if 'take_profit' in data else None,
        'stop_loss': float(data['stop_loss']) if 'stop_loss' in data else None
    }