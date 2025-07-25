# Trade User Sync Implementation Plan

## Executive Summary
This document outlines the issue with user authentication not properly linking to trade setups and trades in your trading application, and provides a comprehensive plan to fix the synchronization between your authentication system and trading data.

## The Problem (Explained Simply)

### What's Happening:
Think of your app like a school where:
- **Authentication** = The student ID card system (knows who you are)
- **Trade Setups/Trades** = Your homework assignments
- **The Problem** = Your homework isn't getting your name written on it!

Right now, when you log in and create trade setups, the system knows who you are, BUT when it saves your trades to the database, it's forgetting to write your name on them. This means your trades aren't showing up in your history because the system can't find trades with your name on them.

### Visual Flow of the Problem:
```
USER LOGS IN → System knows you (✓)
     ↓
ANALYZE MARKETS → System still knows you (✓)
     ↓
CREATE TRADE SETUP → System still knows you (✓)
     ↓
SAVE TO DATABASE → System forgets to write your name (✗) ← THE PROBLEM!
     ↓
VIEW HISTORY → Can't find trades without your name (✗)
```

## Root Cause Analysis

### 1. **Database Connection Context Loss**
- The authentication middleware sets the user context in the main database connection
- When the Claude analyzer creates a new database connection, it doesn't inherit this context
- The new connection doesn't know who the current user is

### 2. **Missing User ID Propagation**
- While `user_id` is passed to `analyze_screenshots()` (line 1285 in api/analysis.py)
- And `user_id` is passed to `save_trade_setup()` (line 556 in claude_analyzer.py)
- The database connection used by `save_trade_setup()` doesn't have the tenant context set

### 3. **Tenant Context Not Set in Analysis Database**
- The `TradingDB` instance in `claude_analyzer.py` (line 103) creates a fresh connection
- This connection doesn't go through the middleware that sets tenant context
- PostgreSQL session variables (app.current_user) aren't set for this connection

## Implementation Plan

### Phase 1: Fix Database Connection Context (Priority: HIGH)

#### Step 1.1: Update claude_analyzer.py to Accept Tenant Context
```python
# In claude_analyzer.py, modify the analyze_screenshots method
async def analyze_screenshots(self, screenshot_paths: List[str], prompt_template: str, user_id: Optional[str] = None) -> str:
    # ... existing code ...
    
    # Before database operations, set tenant context
    if user_id:
        from database.tenant_context import set_current_user_id, set_tenant_context
        from uuid import UUID
        
        # Convert string to UUID if needed
        user_uuid = UUID(user_id) if isinstance(user_id, str) else user_id
        
        # Set tenant context for this operation
        set_current_user_id(user_uuid)
        await set_tenant_context(self.db, user_uuid)
```

#### Step 1.2: Ensure Tenant Context in save_trade_setup
```python
# In database/trading_db.py, modify save_trade_setup method
async def save_trade_setup(self, setup: Dict[str, Any]) -> Optional[UUID]:
    # Extract user_id from setup
    user_id = setup.get('user_id')
    
    if user_id:
        # Set tenant context for this specific connection
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Set the session variable
                await conn.execute(f"SELECT set_config('app.current_user', '{user_id}', TRUE)")
                
                # Now execute the INSERT with proper context
                # ... rest of the save_trade_setup code ...
```

### Phase 2: Add Verification and Logging (Priority: MEDIUM)

#### Step 2.1: Add User ID Verification in save_trade_setup
```python
# Before saving, verify user_id is present
if not user_id:
    self.logger.warning("No user_id provided for trade setup, using system default")
    # You could either:
    # 1. Reject the setup
    # 2. Use a default system user
    # 3. Try to get from current context
    from database.tenant_context import get_current_user_id
    user_id = get_current_user_id()
    if not user_id:
        raise ValueError("Cannot save trade setup without user_id")
```

#### Step 2.2: Add Logging for Debugging
```python
# Add comprehensive logging
self.logger.info(f"Saving trade setup for user: {user_id}")
self.logger.info(f"Current database tenant context: {await self.get_current_tenant()}")
```

### Phase 3: Create Helper Functions (Priority: LOW)

#### Step 3.1: Create a Context-Aware Database Factory
```python
# In database/db_factory.py, add:
async def get_db_with_context(user_id: Optional[str] = None):
    """Get a database instance with tenant context set"""
    db = get_db()
    await db.ensure_connected()
    
    if user_id:
        from database.tenant_context import set_tenant_context
        from uuid import UUID
        user_uuid = UUID(user_id) if isinstance(user_id, str) else user_id
        await set_tenant_context(db, user_uuid)
    
    return db
```

## Testing Plan

### 1. Verify User Context is Set
```python
# Test script to verify user context
async def test_user_context():
    # 1. Create a user
    # 2. Set tenant context
    # 3. Save a trade setup
    # 4. Query to verify user_id is saved
    # 5. Query using RLS to verify only that user's trades are returned
```

### 2. End-to-End Test
1. Log in as User A
2. Create a trade setup
3. Verify it saves with User A's ID
4. Log in as User B
5. Verify User B cannot see User A's trades
6. Create a trade for User B
7. Verify each user only sees their own trades

## Monitoring and Verification

### SQL Queries to Check Data
```sql
-- Check if trades have user_ids
SELECT COUNT(*) as total_trades,
       COUNT(user_id) as trades_with_user,
       COUNT(*) - COUNT(user_id) as trades_without_user
FROM trades;

-- Check recent trade setups
SELECT id, user_id, created_at, symbol
FROM trade_setups
ORDER BY created_at DESC
LIMIT 10;

-- Verify RLS is working
SET app.current_user = 'your-user-uuid-here';
SELECT * FROM trades; -- Should only show that user's trades
```

## Quick Fix (Temporary)

If you need a quick fix while implementing the full solution:

1. **Add Manual User ID Setting** in claude_analyzer.py:
```python
# In save_trade_setup call
setup['user_id'] = user_id or 'default-system-user-uuid'
```

2. **Update Existing Data**:
```sql
-- Update existing trades without user_id (be careful!)
UPDATE trades 
SET user_id = 'your-user-uuid'
WHERE user_id IS NULL 
  AND created_at > '2024-01-01'; -- Adjust date as needed
```

## Summary for Your Cloud Version

When you share this with your cloud version of the price capture code:

1. **The Issue**: User authentication context is lost when creating new database connections
2. **The Fix**: Set tenant context explicitly when saving trade setups
3. **Key Files to Update**:
   - `Analysis/claude_analyzer.py` - Set tenant context before DB operations
   - `database/trading_db.py` - Ensure user_id is used in save_trade_setup
   - `api/analysis.py` - Already passing user_id correctly

4. **Critical Code Changes**:
   - Always set `app.current_user` session variable before database operations
   - Verify user_id is present before saving any trade data
   - Add logging to track user_id flow through the system

This plan ensures that every trade setup and trade is properly associated with the logged-in user, enabling your history section to display the correct user-specific data.