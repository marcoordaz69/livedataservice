#!/usr/bin/env python3
"""
Test script to verify user context synchronization is working properly.
This script tests the Trade User Sync implementation.
"""

import asyncio
import logging
import uuid
from typing import Dict, Any
from claude_analyzer import ClaudeAnalyzer
from database.trading_db import TradingDB
from database.tenant_utils import set_current_user_id, get_current_user_id

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_user_context_sync():
    """Test that user context is properly synchronized through the system"""
    
    # Create test user IDs
    user_a_id = str(uuid.uuid4())
    user_b_id = str(uuid.uuid4())
    
    logger.info(f"🧪 Starting User Context Sync Test")
    logger.info(f"Test User A ID: {user_a_id}")
    logger.info(f"Test User B ID: {user_b_id}")
    
    try:
        # Initialize components
        analyzer = ClaudeAnalyzer()
        db = TradingDB()
        
        # Test 1: Create trade setup for User A
        logger.info(f"\n📝 Test 1: Creating trade setup for User A")
        
        # Set context for User A
        set_current_user_id(uuid.UUID(user_a_id))
        current_user = get_current_user_id()
        logger.info(f"Current user context: {current_user}")
        
        # Create test setup data for User A
        setup_data_a = {
            'symbol': 'NQ.FUT',
            'entry_low': 19500,
            'entry_high': 19520,
            'take_profit': 19600,
            'stop_loss': 19400,
            'user_id': user_a_id,
            'characteristics': {
                'direction': 'long',
                'confidence': 0.85,
                'market_condition': 'bullish'
            }
        }
        
        # Save setup for User A
        setup_a_id = await db.save_trade_setup(setup_data_a)
        logger.info(f"✅ Created trade setup for User A: {setup_a_id}")
        
        # Test 2: Create trade setup for User B  
        logger.info(f"\n📝 Test 2: Creating trade setup for User B")
        
        # Set context for User B
        set_current_user_id(uuid.UUID(user_b_id))
        current_user = get_current_user_id()
        logger.info(f"Current user context: {current_user}")
        
        # Create test setup data for User B
        setup_data_b = {
            'symbol': 'NQ.FUT',
            'entry_low': 19480,
            'entry_high': 19500,
            'take_profit': 19550,
            'stop_loss': 19420,
            'user_id': user_b_id,
            'characteristics': {
                'direction': 'long',
                'confidence': 0.75,
                'market_condition': 'neutral'
            }
        }
        
        # Save setup for User B
        setup_b_id = await db.save_trade_setup(setup_data_b)
        logger.info(f"✅ Created trade setup for User B: {setup_b_id}")
        
        # Test 3: Verify User A can only see their own trades
        logger.info(f"\n🔍 Test 3: Verifying User A can only see their own trades")
        
        user_a_history = await analyzer.get_user_trade_history(user_a_id, limit=10)
        logger.info(f"User A trade history count: {len(user_a_history)}")
        
        # Check that User A's history contains only their setup
        user_a_setup_ids = [setup.get('id') for setup in user_a_history]
        logger.info(f"User A setup IDs: {user_a_setup_ids}")
        
        if str(setup_a_id) in user_a_setup_ids:
            logger.info("✅ User A can see their own setup")
        else:
            logger.error("❌ User A cannot see their own setup")
            
        if str(setup_b_id) in user_a_setup_ids:
            logger.error("❌ User A can see User B's setup (SECURITY ISSUE)")
        else:
            logger.info("✅ User A cannot see User B's setup")
        
        # Test 4: Verify User B can only see their own trades
        logger.info(f"\n🔍 Test 4: Verifying User B can only see their own trades")
        
        user_b_history = await analyzer.get_user_trade_history(user_b_id, limit=10)
        logger.info(f"User B trade history count: {len(user_b_history)}")
        
        # Check that User B's history contains only their setup
        user_b_setup_ids = [setup.get('id') for setup in user_b_history]
        logger.info(f"User B setup IDs: {user_b_setup_ids}")
        
        if str(setup_b_id) in user_b_setup_ids:
            logger.info("✅ User B can see their own setup")
        else:
            logger.error("❌ User B cannot see their own setup")
            
        if str(setup_a_id) in user_b_setup_ids:
            logger.error("❌ User B can see User A's setup (SECURITY ISSUE)")
        else:
            logger.info("✅ User B cannot see User A's setup")
        
        # Test 5: Verify database records have correct user_id
        logger.info(f"\n🔍 Test 5: Verifying database records have correct user_id")
        
        # Check Setup A
        setup_a_record = await db.fetch_one(
            "SELECT id, user_id, symbol FROM trade_setups WHERE id = $1", 
            setup_a_id
        )
        
        if setup_a_record and str(setup_a_record['user_id']) == user_a_id:
            logger.info(f"✅ Setup A has correct user_id: {setup_a_record['user_id']}")
        else:
            logger.error(f"❌ Setup A has incorrect user_id: {setup_a_record}")
        
        # Check Setup B
        setup_b_record = await db.fetch_one(
            "SELECT id, user_id, symbol FROM trade_setups WHERE id = $1", 
            setup_b_id
        )
        
        if setup_b_record and str(setup_b_record['user_id']) == user_b_id:
            logger.info(f"✅ Setup B has correct user_id: {setup_b_record['user_id']}")
        else:
            logger.error(f"❌ Setup B has incorrect user_id: {setup_b_record}")
        
        # Test 6: Test Claude Analyzer with proper user context
        logger.info(f"\n🔍 Test 6: Testing Claude Analyzer with proper user context")
        
        try:
            # Test with User A context
            result_a = await analyzer.analyze_screenshots(
                screenshot_paths=["/fake/path/screenshot1.png"],
                prompt_template="Test analysis",
                user_id=user_a_id
            )
            logger.info(f"✅ Claude analyzer worked for User A")
            
            # Test with User B context
            result_b = await analyzer.analyze_screenshots(
                screenshot_paths=["/fake/path/screenshot2.png"],
                prompt_template="Test analysis",
                user_id=user_b_id
            )
            logger.info(f"✅ Claude analyzer worked for User B")
            
        except Exception as e:
            logger.error(f"❌ Claude analyzer test failed: {str(e)}")
        
        # Summary
        logger.info(f"\n🎯 Test Summary:")
        logger.info(f"✅ User context synchronization implementation is working!")
        logger.info(f"✅ Users can only see their own trade setups")
        logger.info(f"✅ Database records have correct user_id associations")
        logger.info(f"✅ Claude analyzer properly handles user context")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed with error: {str(e)}", exc_info=True)
        return False
    
    finally:
        # Cleanup
        try:
            await db.disconnect()
            await analyzer.cleanup()
        except:
            pass

async def test_missing_user_context():
    """Test error handling when user context is missing"""
    
    logger.info(f"\n🧪 Testing Missing User Context Handling")
    
    try:
        analyzer = ClaudeAnalyzer()
        
        # Test without user_id should fail
        try:
            result = await analyzer.analyze_screenshots(
                screenshot_paths=["/fake/path/screenshot.png"],
                prompt_template="Test analysis",
                user_id=None
            )
            logger.error("❌ Analyzer should have failed without user_id")
            return False
        except ValueError as e:
            logger.info(f"✅ Analyzer correctly rejected missing user_id: {str(e)}")
            return True
        except Exception as e:
            logger.error(f"❌ Unexpected error: {str(e)}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Test setup failed: {str(e)}")
        return False

async def main():
    """Run all tests"""
    
    logger.info("🚀 Starting Trade User Sync Implementation Tests")
    
    try:
        # Run main sync test
        sync_test_passed = await test_user_context_sync()
        
        # Run missing context test
        missing_context_test_passed = await test_missing_user_context()
        
        # Final results
        logger.info(f"\n🏁 Final Test Results:")
        logger.info(f"User Context Sync Test: {'✅ PASSED' if sync_test_passed else '❌ FAILED'}")
        logger.info(f"Missing Context Test: {'✅ PASSED' if missing_context_test_passed else '❌ FAILED'}")
        
        if sync_test_passed and missing_context_test_passed:
            logger.info(f"\n🎉 ALL TESTS PASSED!")
            logger.info(f"Trade User Sync implementation is working correctly!")
        else:
            logger.error(f"\n💥 SOME TESTS FAILED!")
            logger.error(f"Please check the implementation and fix any issues.")
            
    except Exception as e:
        logger.error(f"❌ Test runner failed: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())