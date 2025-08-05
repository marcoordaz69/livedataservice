"""
Authentication Middleware for Cloud Service
Copy this file to your cloud service as middleware/auth.py
"""
import jwt
import logging
from typing import Optional
from uuid import UUID
from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from database.tenant_utils import set_current_user_id, get_current_user_id

logger = logging.getLogger(__name__)
security = HTTPBearer()

# Configure these in your environment
JWT_SECRET_KEY = "your-jwt-secret-key"  # Replace with actual secret
JWT_ALGORITHM = "HS256"

class AuthMiddleware:
    """Authentication middleware for handling user context"""
    
    def __init__(self, secret_key: str = JWT_SECRET_KEY):
        self.secret_key = secret_key
        self.algorithm = JWT_ALGORITHM
    
    async def __call__(self, request: Request, call_next):
        """
        Middleware to extract and set user context for each request
        """
        try:
            # Extract user_id from JWT token
            user_id = await self._extract_user_from_request(request)
            
            # Set tenant context if user is authenticated
            if user_id:
                set_current_user_id(user_id)
                logger.debug(f"Set tenant context for user: {user_id}")
            
            # Add user_id to request state for easy access
            request.state.user_id = user_id
            
            # Process the request
            response = await call_next(request)
            
            return response
            
        except Exception as e:
            logger.error(f"Auth middleware error: {str(e)}")
            # Continue without authentication for non-protected routes
            request.state.user_id = None
            return await call_next(request)
    
    async def _extract_user_from_request(self, request: Request) -> Optional[UUID]:
        """Extract user ID from JWT token in request headers"""
        try:
            # Get Authorization header
            auth_header = request.headers.get("Authorization")
            if not auth_header:
                return None
            
            # Extract token from "Bearer <token>"
            if not auth_header.startswith("Bearer "):
                return None
            
            token = auth_header.split(" ")[1]
            
            # Decode JWT token
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            # Extract user_id from payload
            user_id_str = payload.get("user_id") or payload.get("sub")
            if not user_id_str:
                return None
            
            # Convert to UUID
            user_id = UUID(user_id_str)
            logger.debug(f"Extracted user_id from token: {user_id}")
            
            return user_id
            
        except jwt.ExpiredSignatureError:
            logger.warning("JWT token has expired")
            return None
        except jwt.InvalidTokenError:
            logger.warning("Invalid JWT token")
            return None
        except ValueError:
            logger.warning("Invalid user_id format in token")
            return None
        except Exception as e:
            logger.error(f"Error extracting user from token: {str(e)}")
            return None

# FastAPI dependency functions
async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """
    FastAPI dependency to get current authenticated user
    Use this in your API endpoints: user_id: str = Depends(get_current_user)
    """
    try:
        # Decode the token
        payload = jwt.decode(credentials.credentials, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        user_id = payload.get("user_id") or payload.get("sub")
        
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid authentication credentials")
        
        # Set tenant context
        user_uuid = UUID(user_id)
        set_current_user_id(user_uuid)
        
        return user_id
        
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid user ID format")
    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        raise HTTPException(status_code=401, detail="Authentication failed")

async def get_optional_user(request: Request) -> Optional[str]:
    """
    FastAPI dependency to get current user if authenticated, None otherwise
    Use this for endpoints that work with or without authentication
    """
    try:
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return None
        
        token = auth_header.split(" ")[1]
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        user_id = payload.get("user_id") or payload.get("sub")
        
        if user_id:
            # Set tenant context
            user_uuid = UUID(user_id)
            set_current_user_id(user_uuid)
        
        return user_id
        
    except Exception:
        return None

def create_jwt_token(user_id: str, expires_delta: Optional[int] = None) -> str:
    """
    Create a JWT token for a user
    
    Args:
        user_id: User ID to encode in token
        expires_delta: Token expiration time in seconds (default: 24 hours)
    
    Returns:
        JWT token string
    """
    import datetime
    
    if expires_delta is None:
        expires_delta = 24 * 60 * 60  # 24 hours
    
    payload = {
        "user_id": user_id,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(seconds=expires_delta),
        "iat": datetime.datetime.utcnow()
    }
    
    return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)

# Example usage in your FastAPI app:
"""
from fastapi import FastAPI
from middleware.auth import AuthMiddleware

app = FastAPI()

# Add auth middleware
app.add_middleware(AuthMiddleware)

# Example protected endpoint
@app.get("/protected")
async def protected_endpoint(user_id: str = Depends(get_current_user)):
    return {"message": f"Hello user {user_id}"}

# Example optional auth endpoint
@app.get("/optional-auth")
async def optional_auth_endpoint(user_id: Optional[str] = Depends(get_optional_user)):
    if user_id:
        return {"message": f"Hello authenticated user {user_id}"}
    else:
        return {"message": "Hello anonymous user"}
"""