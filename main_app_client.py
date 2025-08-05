"""
Client code for your main app.py to call the live data service
Copy this code into your main application
"""

import requests
import jwt
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)

class LiveDataServiceClient:
    """
    Client for calling the live data service with proper user context
    """
    
    def __init__(self, 
                 service_url: str, 
                 jwt_secret: str,
                 jwt_algorithm: str = "HS256"):
        """
        Initialize the live data service client
        
        Args:
            service_url: URL of your live data service (e.g., "https://your-service.railway.app")
            jwt_secret: Shared secret key for JWT tokens (must match live data service)
            jwt_algorithm: JWT algorithm to use
        """
        self.service_url = service_url.rstrip('/')
        self.jwt_secret = jwt_secret
        self.jwt_algorithm = jwt_algorithm
    
    def create_jwt_token(self, user_id: str, expires_hours: int = 24) -> str:
        """
        Create a JWT token for the user
        
        Args:
            user_id: The user's ID
            expires_hours: Token expiration time in hours
            
        Returns:
            JWT token string
        """
        payload = {
            "user_id": user_id,
            "exp": datetime.utcnow() + timedelta(hours=expires_hours),
            "iat": datetime.utcnow(),
            "iss": "main-app"  # Issuer identification
        }
        
        return jwt.encode(payload, self.jwt_secret, algorithm=self.jwt_algorithm)
    
    def get_auth_headers(self, user_id: str) -> Dict[str, str]:
        """
        Get authentication headers for API calls
        
        Args:
            user_id: The user's ID
            
        Returns:
            Headers dictionary with Authorization header
        """
        token = self.create_jwt_token(user_id)
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    async def analyze_screenshots(self, 
                                user_id: str,
                                screenshot_paths: List[str], 
                                prompt_template: str) -> Dict[str, Any]:
        """
        Analyze screenshots via the live data service
        
        Args:
            user_id: The authenticated user's ID
            screenshot_paths: List of screenshot file paths
            prompt_template: Analysis prompt template
            
        Returns:
            Analysis result dictionary
        """
        try:
            headers = self.get_auth_headers(user_id)
            
            payload = {
                "screenshot_paths": screenshot_paths,
                "prompt_template": prompt_template
            }
            
            logger.info(f"Calling live data service for user: {user_id}")
            
            response = requests.post(
                f"{self.service_url}/analyze_screenshots",
                json=payload,
                headers=headers,
                timeout=300  # 5 minute timeout for analysis
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Analysis completed for user: {user_id}")
                return result
            else:
                logger.error(f"Live data service error: {response.status_code} - {response.text}")
                raise Exception(f"Analysis failed: {response.status_code} - {response.text}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error calling live data service: {str(e)}")
            raise Exception(f"Network error: {str(e)}")
        except Exception as e:
            logger.error(f"Error calling live data service: {str(e)}")
            raise
    
    async def analyze_screenshots_with_context(self, 
                                             user_id: str,
                                             screenshot_paths: List[str], 
                                             prompt_template: str) -> Dict[str, Any]:
        """
        Alternative method that passes user_id in request body instead of JWT token
        
        Args:
            user_id: The authenticated user's ID
            screenshot_paths: List of screenshot file paths
            prompt_template: Analysis prompt template
            
        Returns:
            Analysis result dictionary
        """
        try:
            payload = {
                "screenshot_paths": screenshot_paths,
                "prompt_template": prompt_template,
                "user_id": user_id
            }
            
            logger.info(f"Calling live data service (with context) for user: {user_id}")
            
            response = requests.post(
                f"{self.service_url}/analyze_screenshots_with_context",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=300  # 5 minute timeout for analysis
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Analysis completed for user: {user_id}")
                return result
            else:
                logger.error(f"Live data service error: {response.status_code} - {response.text}")
                raise Exception(f"Analysis failed: {response.status_code} - {response.text}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error calling live data service: {str(e)}")
            raise Exception(f"Network error: {str(e)}")
        except Exception as e:
            logger.error(f"Error calling live data service: {str(e)}")
            raise
    
    async def get_user_trade_history(self, 
                                   user_id: str, 
                                   limit: int = 50) -> Dict[str, Any]:
        """
        Get trade history for a user
        
        Args:
            user_id: The authenticated user's ID
            limit: Maximum number of records to return
            
        Returns:
            Trade history dictionary
        """
        try:
            headers = self.get_auth_headers(user_id)
            
            logger.info(f"Getting trade history for user: {user_id}")
            
            response = requests.get(
                f"{self.service_url}/user_trade_history",
                params={"limit": limit},
                headers=headers,
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Retrieved {result.get('count', 0)} trade records for user: {user_id}")
                return result
            else:
                logger.error(f"Live data service error: {response.status_code} - {response.text}")
                raise Exception(f"Failed to get trade history: {response.status_code} - {response.text}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error calling live data service: {str(e)}")
            raise Exception(f"Network error: {str(e)}")
        except Exception as e:
            logger.error(f"Error calling live data service: {str(e)}")
            raise
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Check if the live data service is healthy
        
        Returns:
            Health status dictionary
        """
        try:
            response = requests.get(
                f"{self.service_url}/health",
                timeout=10
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"Health check failed: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Health check network error: {str(e)}")
            raise Exception(f"Health check network error: {str(e)}")

# Example usage in your main app.py:

"""
# Add this to your main app.py

# Configuration
LIVE_DATA_SERVICE_URL = "https://your-live-data-service.railway.app"
JWT_SECRET = "your-shared-secret-key"  # Must match live data service

# Initialize client
live_data_client = LiveDataServiceClient(
    service_url=LIVE_DATA_SERVICE_URL,
    jwt_secret=JWT_SECRET
)

# Example route in your main app
@app.route("/analyze_market")
async def analyze_market():
    # Get current user (however you do this in your app)
    user_id = get_current_user_id()  # Your existing auth logic
    
    # Call live data service with user context
    try:
        result = await live_data_client.analyze_screenshots(
            user_id=user_id,
            screenshot_paths=["/path/to/screenshot1.png", "/path/to/screenshot2.png"],
            prompt_template="Analyze these market screenshots for trading opportunities"
        )
        
        return jsonify({
            "success": True,
            "analysis": result['result'],
            "user_id": user_id
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

# Example route to get trade history
@app.route("/trade_history")
async def get_trade_history():
    user_id = get_current_user_id()  # Your existing auth logic
    
    try:
        result = await live_data_client.get_user_trade_history(
            user_id=user_id,
            limit=50
        )
        
        return jsonify({
            "success": True,
            "history": result['history'],
            "count": result['count'],
            "user_id": user_id
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
"""