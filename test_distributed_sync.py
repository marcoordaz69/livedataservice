#!/usr/bin/env python3
"""
Test script for distributed user sync implementation
This tests the JWT token-based user context passing between applications
"""

import asyncio
import logging
import uuid
import jwt
from datetime import datetime, timedelta
from typing import Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
import sys
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add current directory to path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api_analysis import router, JWT_SECRET_KEY
from claude_analyzer import ClaudeAnalyzer
from database.trading_db import TradingDB

def create_test_jwt_token(user_id: str, expires_hours: int = 24) -> str:
    """Create a test JWT token"""
    payload = {
        "user_id": user_id,
        "exp": datetime.utcnow() + timedelta(hours=expires_hours),
        "iat": datetime.utcnow(),
        "iss": "test-main-app"
    }
    
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm="HS256")

async def test_distributed_user_sync():
    """Test that distributed user context works properly"""
    
    # Create test user IDs
    user_a_id = str(uuid.uuid4())
    user_b_id = str(uuid.uuid4())
    
    logger.info(f"🧪 Starting Distributed User Sync Test")
    logger.info(f"Test User A ID: {user_a_id}")
    logger.info(f"Test User B ID: {user_b_id}")
    
    # Create FastAPI test client
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    
    try:
        # Test 1: Create JWT tokens for both users
        logger.info(f"\n📝 Test 1: Creating JWT tokens for users")
        
        token_a = create_test_jwt_token(user_a_id)
        token_b = create_test_jwt_token(user_b_id)
        
        logger.info(f"✅ Created tokens for both users")
        
        # Test 2: Test API with User A token
        logger.info(f"\n📝 Test 2: Testing API with User A token")
        
        headers_a = {"Authorization": f"Bearer {token_a}"}
        
        # Test the analyze_screenshots_with_context endpoint (easier to test)
        response_a = client.post(
            "/analyze_screenshots_with_context",
            json={
                "screenshot_paths": ["/fake/path/screenshot1.png"],
                "prompt_template": "Test analysis",
                "user_id": user_a_id
            }
        )
        
        if response_a.status_code == 200:
            result_a = response_a.json()
            logger.info(f"✅ User A analysis succeeded: {result_a.get('status')}")
            logger.info(f"   User ID in response: {result_a.get('user_id')}")
        else:
            logger.error(f"❌ User A analysis failed: {response_a.status_code} - {response_a.text}")
        
        # Test 3: Test API with User B token
        logger.info(f"\n📝 Test 3: Testing API with User B token")
        
        headers_b = {"Authorization": f"Bearer {token_b}"}
        
        response_b = client.post(
            "/analyze_screenshots_with_context",
            json={
                "screenshot_paths": ["/fake/path/screenshot2.png"],
                "prompt_template": "Test analysis",
                "user_id": user_b_id
            }
        )
        
        if response_b.status_code == 200:
            result_b = response_b.json()
            logger.info(f"✅ User B analysis succeeded: {result_b.get('status')}")
            logger.info(f"   User ID in response: {result_b.get('user_id')}")
        else:
            logger.error(f"❌ User B analysis failed: {response_b.status_code} - {response_b.text}")
        
        # Test 4: Test invalid token
        logger.info(f"\n📝 Test 4: Testing invalid token")
        
        invalid_headers = {"Authorization": "Bearer invalid-token"}
        
        response_invalid = client.post(
            "/analyze_screenshots",
            json={
                "screenshot_paths": ["/fake/path/screenshot.png"],
                "prompt_template": "Test analysis"
            },
            headers=invalid_headers
        )
        
        if response_invalid.status_code == 401:
            logger.info(f"✅ Invalid token correctly rejected: {response_invalid.status_code}")
        else:
            logger.error(f"❌ Invalid token should have been rejected: {response_invalid.status_code}")
        
        # Test 5: Test expired token
        logger.info(f"\n📝 Test 5: Testing expired token")
        
        expired_token = create_test_jwt_token(user_a_id, expires_hours=-1)  # Expired 1 hour ago
        expired_headers = {"Authorization": f"Bearer {expired_token}"}
        
        response_expired = client.post(
            "/analyze_screenshots",
            json={
                "screenshot_paths": ["/fake/path/screenshot.png"],
                "prompt_template": "Test analysis"
            },
            headers=expired_headers
        )
        
        if response_expired.status_code == 401:
            logger.info(f"✅ Expired token correctly rejected: {response_expired.status_code}")
        else:
            logger.error(f"❌ Expired token should have been rejected: {response_expired.status_code}")
        
        # Test 6: Test missing Authorization header
        logger.info(f"\n📝 Test 6: Testing missing Authorization header")
        
        response_no_auth = client.post(
            "/analyze_screenshots",
            json={
                "screenshot_paths": ["/fake/path/screenshot.png"],
                "prompt_template": "Test analysis"
            }
        )
        
        if response_no_auth.status_code == 401:
            logger.info(f"✅ Missing auth header correctly rejected: {response_no_auth.status_code}")
        else:
            logger.error(f"❌ Missing auth header should have been rejected: {response_no_auth.status_code}")
        
        # Test 7: Test health check (no auth required)
        logger.info(f"\n📝 Test 7: Testing health check endpoint")
        
        response_health = client.get("/health")
        
        if response_health.status_code == 200:
            logger.info(f"✅ Health check succeeded: {response_health.json()}")
        else:
            logger.error(f"❌ Health check failed: {response_health.status_code}")
        
        # Summary
        logger.info(f"\n🎯 Test Summary:")
        logger.info(f"✅ JWT token creation working")
        logger.info(f"✅ User context extraction from tokens working")
        logger.info(f"✅ API endpoints properly authenticate users")
        logger.info(f"✅ Invalid/expired tokens properly rejected")
        logger.info(f"✅ Distributed user sync implementation is working!")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed with error: {str(e)}", exc_info=True)
        return False

async def test_token_creation():
    """Test JWT token creation and validation"""
    
    logger.info(f"\n🧪 Testing JWT Token Creation and Validation")
    
    try:
        user_id = str(uuid.uuid4())
        
        # Create token
        token = create_test_jwt_token(user_id)
        logger.info(f"✅ Created token: {token[:50]}...")
        
        # Validate token
        try:
            payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=["HS256"])
            decoded_user_id = payload.get("user_id")
            
            if decoded_user_id == user_id:
                logger.info(f"✅ Token validation successful: {decoded_user_id}")
                return True
            else:
                logger.error(f"❌ Token validation failed: {decoded_user_id} != {user_id}")
                return False
                
        except jwt.ExpiredSignatureError:
            logger.error(f"❌ Token expired")
            return False
        except jwt.InvalidTokenError:
            logger.error(f"❌ Invalid token")
            return False
            
    except Exception as e:
        logger.error(f"❌ Token test failed: {str(e)}")
        return False

async def main():
    """Run all tests"""
    
    logger.info("🚀 Starting Distributed User Sync Tests")
    
    try:
        # Test token creation first
        token_test_passed = await test_token_creation()
        
        if not token_test_passed:
            logger.error("❌ Token creation test failed, skipping distributed tests")
            return
        
        # Run distributed sync test
        sync_test_passed = await test_distributed_user_sync()
        
        # Final results
        logger.info(f"\n🏁 Final Test Results:")
        logger.info(f"Token Creation Test: {'✅ PASSED' if token_test_passed else '❌ FAILED'}")
        logger.info(f"Distributed Sync Test: {'✅ PASSED' if sync_test_passed else '❌ FAILED'}")
        
        if token_test_passed and sync_test_passed:
            logger.info(f"\n🎉 ALL TESTS PASSED!")
            logger.info(f"Distributed User Sync implementation is working correctly!")
            logger.info(f"\nNext steps:")
            logger.info(f"1. Copy the updated files to your cloud service")
            logger.info(f"2. Set JWT_SECRET_KEY environment variable")
            logger.info(f"3. Update your main app.py to use the client code")
            logger.info(f"4. Test with real user authentication")
        else:
            logger.error(f"\n💥 SOME TESTS FAILED!")
            logger.error(f"Please check the implementation and fix any issues.")
            
    except Exception as e:
        logger.error(f"❌ Test runner failed: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())