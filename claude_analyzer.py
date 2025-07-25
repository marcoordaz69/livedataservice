"""
Claude Analyzer for Cloud Service
Copy this file to your cloud service as claude_analyzer.py
"""
import asyncio
import logging
from typing import List, Optional, Dict, Any
from uuid import UUID
from database.trading_db import TradingDB
from database.tenant_utils import set_current_user_id, set_tenant_context

logger = logging.getLogger(__name__)

class ClaudeAnalyzer:
    """
    Enhanced Claude Analyzer with proper user context propagation
    """
    
    def __init__(self):
        self.db = TradingDB()
        
    async def analyze_screenshots(
        self, 
        screenshot_paths: List[str], 
        prompt_template: str, 
        user_id: Optional[str] = None
    ) -> str:
        """
        Analyze screenshots with proper user context
        
        Args:
            screenshot_paths: List of screenshot file paths
            prompt_template: Analysis prompt template
            user_id: User ID for tenant context (REQUIRED)
            
        Returns:
            Analysis result string
        """
        try:
            # Validate user_id is provided
            if not user_id:
                raise ValueError("user_id is required for analysis")
            
            logger.info(f"Starting analysis for user: {user_id}")
            
            # Set tenant context at start of analysis
            await self._set_tenant_context(user_id)
            
            # Ensure DB connection has tenant context
            await self.db.ensure_connected()
            
            # Perform the actual analysis
            analysis_result = await self._perform_analysis(
                screenshot_paths, 
                prompt_template, 
                user_id
            )
            
            logger.info(f"Analysis completed for user: {user_id}")
            return analysis_result
            
        except Exception as e:
            logger.error(f"Analysis failed for user {user_id}: {str(e)}")
            raise
    
    async def _set_tenant_context(self, user_id: str):
        """Set tenant context for database operations"""
        try:
            # Convert string to UUID if needed
            user_uuid = UUID(user_id) if isinstance(user_id, str) else user_id
            
            # Set tenant context for this operation
            set_current_user_id(user_uuid)
            await set_tenant_context(self.db, user_uuid)
            
            logger.debug(f"Tenant context set for user: {user_id}")
            
        except Exception as e:
            logger.error(f"Failed to set tenant context for user {user_id}: {str(e)}")
            raise
    
    async def _perform_analysis(
        self, 
        screenshot_paths: List[str], 
        prompt_template: str, 
        user_id: str
    ) -> str:
        """
        Perform the actual Claude analysis
        Replace this with your actual Claude API calls
        """
        # TODO: Implement actual Claude API integration
        # This is a placeholder for your Claude analysis logic
        
        # Simulate analysis processing
        logger.info(f"Analyzing {len(screenshot_paths)} screenshots for user {user_id}")
        
        # Example analysis result - replace with actual Claude response
        analysis_result = {
            "market_condition": "bullish",
            "entry_signal": "long",
            "confidence": 0.85,
            "notes": "Strong upward momentum with good volume"
        }
        
        # Save trade setup with proper user context
        await self._save_trade_setup(analysis_result, user_id)
        
        return str(analysis_result)
    
    async def _save_trade_setup(self, analysis_result: Dict[str, Any], user_id: str):
        """Save trade setup with proper user context"""
        try:
            # Create trade setup data with user_id
            setup_data = {
                'symbol': 'NQ.FUT',  # Replace with actual symbol
                'entry_low': 19500,  # Replace with actual values
                'entry_high': 19520,
                'take_profit': 19600,
                'stop_loss': 19400,
                'user_id': user_id,  # KEY: Include user_id in setup
                'analysis_confidence': analysis_result.get('confidence', 0.0),
                'market_condition': analysis_result.get('market_condition', 'unknown'),
                'entry_signal': analysis_result.get('entry_signal', 'none'),
                'notes': analysis_result.get('notes', ''),
                'characteristics': analysis_result
            }
            
            logger.info(f"Saving trade setup for user: {user_id}")
            
            # This will work correctly with existing save_trade_setup method
            # The database layer already handles user_id properly
            setup_id = await self.db.save_trade_setup(setup_data)
            
            if setup_id:
                logger.info(f"Trade setup saved successfully with ID: {setup_id} for user: {user_id}")
            else:
                logger.warning(f"Trade setup save returned None for user: {user_id}")
                
            return setup_id
            
        except Exception as e:
            logger.error(f"Failed to save trade setup for user {user_id}: {str(e)}")
            raise
    
    async def get_user_trade_history(self, user_id: str, limit: int = 50) -> List[Dict]:
        """
        Get trade history for a specific user
        """
        try:
            # Set tenant context
            await self._set_tenant_context(user_id)
            
            # Query user's trade history
            # The RLS (Row Level Security) will automatically filter by user_id
            history = await self.db.get_trade_setups_by_user(user_id, limit)
            
            logger.info(f"Retrieved {len(history)} trade setups for user: {user_id}")
            return history
            
        except Exception as e:
            logger.error(f"Failed to get trade history for user {user_id}: {str(e)}")
            raise
    
    async def cleanup(self):
        """Cleanup resources"""
        try:
            if self.db:
                await self.db.close()
                logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")