# Cloud Service Deployment Instructions

## Files to Copy to Your Cloud Service

Copy these files to your cloud service in the correct locations:

### 1. API Layer
- **Source:** `cloud_service_templates/api_analysis.py`
- **Destination:** `api/analysis.py`
- **Purpose:** Handles screenshot analysis requests with proper user context

### 2. Claude Analyzer  
- **Source:** `cloud_service_templates/claude_analyzer.py`
- **Destination:** `claude_analyzer.py`
- **Purpose:** Enhanced analyzer with user context propagation

### 3. Authentication Middleware
- **Source:** `cloud_service_templates/auth_middleware.py`
- **Destination:** `middleware/auth.py`
- **Purpose:** JWT authentication and user context management

## Configuration Required

### Environment Variables
Add these to your cloud service environment:

```bash
# JWT Configuration
JWT_SECRET_KEY=your-super-secret-jwt-key-here
JWT_ALGORITHM=HS256

# Database Configuration (already exists)
DATABASE_URL=your-database-url
```

### FastAPI App Integration
Update your main FastAPI app file:

```python
from fastapi import FastAPI
from middleware.auth import AuthMiddleware
from api.analysis import router as analysis_router

app = FastAPI()

# Add authentication middleware
app.add_middleware(AuthMiddleware)

# Add analysis router
app.include_router(analysis_router, prefix="/api/v1")
```

## Key Changes Summary

### What These Files Fix:

1. **User Context Loss Problem** ✅
   - Sets tenant context before database operations
   - Ensures user_id flows through the entire pipeline

2. **Missing API Layer** ✅
   - Provides `/analyze_screenshots` endpoint
   - Handles authentication and user context

3. **Authentication Integration** ✅
   - JWT token validation
   - Automatic user context setting
   - Protected endpoints

### Critical Implementation Points:

1. **Always Pass user_id:** Every analysis request now includes user_id
2. **Set Tenant Context:** Database operations set PostgreSQL session variables
3. **Verify User Context:** Logging and error handling for user context issues

## Testing Your Implementation

### 1. Test Authentication
```bash
# Test endpoint without auth (should fail)
curl -X POST "http://your-service/api/v1/analyze_screenshots" \
  -H "Content-Type: application/json" \
  -d '{"screenshot_paths": ["test.png"], "prompt_template": "analyze this"}'

# Test with valid JWT token (should work)
curl -X POST "http://your-service/api/v1/analyze_screenshots" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"screenshot_paths": ["test.png"], "prompt_template": "analyze this"}'
```

### 2. Verify User Context in Database
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
```

### 3. Test User Isolation
1. Create trade setup with User A's token
2. Query trades with User B's token
3. Verify User B cannot see User A's trades

## Troubleshooting

### Common Issues:

1. **Import Errors**
   - Ensure all database imports are correct
   - Check that `tenant_utils.py` exists in your database package

2. **JWT Token Issues**
   - Verify JWT_SECRET_KEY is set
   - Check token format is "Bearer <token>"

3. **Database Connection Issues**
   - Ensure database pool is properly configured
   - Check PostgreSQL session variables are being set

### Debug Logging:
Add this to your logging configuration:
```python
import logging
logging.getLogger('claude_analyzer').setLevel(logging.DEBUG)
logging.getLogger('auth_middleware').setLevel(logging.DEBUG)
```

## Next Steps After Deployment

1. **Update Frontend:** Ensure your frontend sends JWT tokens in Authorization headers
2. **Test End-to-End:** Verify complete user flow from login to trade history
3. **Monitor Logs:** Check for any user context issues in production
4. **Database Cleanup:** Update any existing trades without user_id if needed

## Security Notes

- Keep JWT_SECRET_KEY secure and rotate periodically
- Use HTTPS in production
- Consider token expiration times
- Monitor for authentication failures