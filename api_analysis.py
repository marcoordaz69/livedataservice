"""
API Analysis Layer for Cloud Service (Distributed Architecture)
Copy this file to your cloud service as api/analysis.py
"""
from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Optional, Dict, Any
import logging
import jwt
from uuid import UUID
from pydantic import BaseModel
from claude_analyzer import ClaudeAnalyzer
from database.tenant_utils import set_current_user_id

router = APIRouter()
logger = logging.getLogger(__name__)
security = HTTPBearer()

# Configuration - set these in your environment
JWT_SECRET_KEY = "your-shared-secret-key"  # MUST match your main app
JWT_ALGORITHM = "HS256"

class AnalysisRequest(BaseModel):
    """Request model for screenshot analysis"""
    screenshot_paths: List[str]
    prompt_template: str
    user_id: Optional[str] = None  # Optional fallback if not in token

class UserContextRequest(BaseModel):
    """Request model with explicit user context"""
    screenshot_paths: List[str]
    prompt_template: str
    user_id: str  # Required user_id in request body

def extract_user_from_token(credentials: HTTPAuthorizationCredentials) -> str:
    """Extract user ID from JWT token sent by main app"""
    try:
        # Decode JWT token
        payload = jwt.decode(credentials.credentials, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        user_id = payload.get("user_id") or payload.get("sub")
        
        if not user_id:
            raise HTTPException(status_code=401, detail="User ID not found in token")
        
        # Set tenant context for this request
        user_uuid = UUID(user_id)
        set_current_user_id(user_uuid)
        
        logger.info(f"Extracted user_id from token: {user_id}")
        return user_id
        
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid user ID format")
    except Exception as e:
        logger.error(f"Token validation error: {str(e)}")
        raise HTTPException(status_code=401, detail="Authentication failed")

@router.post("/analyze_screenshots")
async def analyze_screenshots(
    request: AnalysisRequest,
    user_id: str = Depends(extract_user_from_token)
):
    """
    Analyze screenshots with user context from JWT token
    
    This endpoint is called by your main app with a JWT token in the Authorization header.
    The token contains the user_id which is extracted and used for tenant context.
    """
    try:
        logger.info(f"Starting analysis for user: {user_id}")
        
        # Initialize analyzer with user context
        analyzer = ClaudeAnalyzer()
        
        # Pass user_id explicitly to analyzer
        result = await analyzer.analyze_screenshots(
            screenshot_paths=request.screenshot_paths,
            prompt_template=request.prompt_template,
            user_id=user_id  # KEY: Pass user_id explicitly
        )
        
        logger.info(f"Analysis completed for user: {user_id}")
        return {
            "result": result, 
            "user_id": user_id,
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Analysis failed for user {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/analyze_screenshots_with_context")
async def analyze_screenshots_with_context(request: UserContextRequest):
    """
    Alternative endpoint that accepts user_id in request body
    
    Use this if you prefer to pass user_id directly instead of JWT tokens.
    Make sure to secure this endpoint with API keys or other authentication.
    """
    try:
        user_id = request.user_id
        
        if not user_id:
            raise HTTPException(status_code=400, detail="user_id is required")
        
        logger.info(f"Starting analysis for user: {user_id}")
        
        # Set tenant context
        user_uuid = UUID(user_id)
        set_current_user_id(user_uuid)
        
        # Initialize analyzer with user context
        analyzer = ClaudeAnalyzer()
        
        # Pass user_id explicitly to analyzer
        result = await analyzer.analyze_screenshots(
            screenshot_paths=request.screenshot_paths,
            prompt_template=request.prompt_template,
            user_id=user_id  # KEY: Pass user_id explicitly
        )
        
        logger.info(f"Analysis completed for user: {user_id}")
        return {
            "result": result, 
            "user_id": user_id,
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Analysis failed for user {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/user_trade_history")
async def get_user_trade_history(
    limit: int = 50,
    user_id: str = Depends(extract_user_from_token)
):
    """
    Get trade history for authenticated user
    """
    try:
        logger.info(f"Getting trade history for user: {user_id}")
        
        # Initialize analyzer with user context
        analyzer = ClaudeAnalyzer()
        
        # Get user's trade history
        history = await analyzer.get_user_trade_history(user_id, limit)
        
        logger.info(f"Retrieved {len(history)} trade records for user: {user_id}")
        return {
            "history": history,
            "user_id": user_id,
            "count": len(history),
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Failed to get trade history for user {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}