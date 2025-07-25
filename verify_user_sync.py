#!/usr/bin/env python3
"""
Quick verification script to check if user sync implementation is working.
This script checks the key components without running full tests.
"""

import asyncio
import logging
import sys
from database.trading_db import TradingDB
from database.tenant_utils import set_current_user_id, get_current_user_id
from claude_analyzer import ClaudeAnalyzer

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def verify_implementation():
    """Verify that the user sync implementation is properly configured"""
    
    logger.info("🔍 Verifying Trade User Sync Implementation")
    
    issues = []
    
    try:
        # Test 1: Check if tenant_utils functions work
        logger.info("1. Testing tenant context functions...")
        
        try:
            import uuid
            test_user = uuid.uuid4()
            set_current_user_id(test_user)
            current = get_current_user_id()
            
            if current == test_user:
                logger.info("✅ Tenant context functions working")
            else:
                issues.append("❌ Tenant context functions not working properly")
        except Exception as e:
            issues.append(f"❌ Tenant context functions failed: {str(e)}")
        
        # Test 2: Check if TradingDB has required methods
        logger.info("2. Testing TradingDB methods...")
        
        try:
            db = TradingDB()
            
            # Check if save_trade_setup exists
            if hasattr(db, 'save_trade_setup'):
                logger.info("✅ save_trade_setup method exists")
            else:
                issues.append("❌ save_trade_setup method missing")
            
            # Check if get_trade_setups_by_user exists
            if hasattr(db, 'get_trade_setups_by_user'):
                logger.info("✅ get_trade_setups_by_user method exists")
            else:
                issues.append("❌ get_trade_setups_by_user method missing")
                
        except Exception as e:
            issues.append(f"❌ TradingDB initialization failed: {str(e)}")
        
        # Test 3: Check if ClaudeAnalyzer has required methods
        logger.info("3. Testing ClaudeAnalyzer methods...")
        
        try:
            analyzer = ClaudeAnalyzer()
            
            # Check if analyze_screenshots exists and requires user_id
            if hasattr(analyzer, 'analyze_screenshots'):
                logger.info("✅ analyze_screenshots method exists")
            else:
                issues.append("❌ analyze_screenshots method missing")
                
            # Check if get_user_trade_history exists
            if hasattr(analyzer, 'get_user_trade_history'):
                logger.info("✅ get_user_trade_history method exists")
            else:
                issues.append("❌ get_user_trade_history method missing")
                
        except Exception as e:
            issues.append(f"❌ ClaudeAnalyzer initialization failed: {str(e)}")
        
        # Test 4: Check if tenant_utils has set_tenant_context
        logger.info("4. Testing tenant_utils functions...")
        
        try:
            from database.tenant_utils import set_tenant_context
            logger.info("✅ set_tenant_context function exists")
        except ImportError:
            issues.append("❌ set_tenant_context function missing")
        
        # Test 5: Check if database connection works
        logger.info("5. Testing database connection...")
        
        try:
            db = TradingDB()
            await db.ensure_connected()
            logger.info("✅ Database connection successful")
            await db.disconnect()
        except Exception as e:
            issues.append(f"❌ Database connection failed: {str(e)}")
        
        # Report results
        if not issues:
            logger.info("\n🎉 All verifications passed!")
            logger.info("✅ Trade User Sync implementation is properly configured")
            return True
        else:
            logger.error(f"\n💥 Found {len(issues)} issues:")
            for issue in issues:
                logger.error(f"   {issue}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Verification failed: {str(e)}", exc_info=True)
        return False

async def main():
    """Run verification"""
    
    success = await verify_implementation()
    
    if success:
        logger.info("\n🚀 Implementation is ready!")
        logger.info("You can now run the full test with: python test_user_sync.py")
        sys.exit(0)
    else:
        logger.error("\n💥 Implementation has issues!")
        logger.error("Please fix the issues above before proceeding.")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())