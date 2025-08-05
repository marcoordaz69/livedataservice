# Railway Deployment Guide for Live Data Service

## Overview
This guide covers deploying your live data service to Railway with optimizations for 24/7 operation.

## Environment Variables Required

Set these in your Railway dashboard under **Settings → Environment Variables**:

### Required Variables
```
DATABENTO_API_KEY=your_databento_api_key
host=your_supabase_host
user=your_supabase_user  
password=your_supabase_password
```

### Optional Variables
```
port=5432
dbname=postgres
RAILWAY_DEPLOYMENT=true
```

## Deployment Features

### ✅ Health Check Endpoint
- **URL**: `https://your-service.railway.app/health`
- **Purpose**: Railway monitors service health automatically
- **Response**: JSON with status, uptime, and timestamp

### ✅ Database Connection Resilience
- **Retry Logic**: 3 attempts with 2-second delays
- **Timeout**: 30-second connection timeout
- **SSL**: Required for Supabase connections

### ✅ Graceful Shutdown
- **Signal Handling**: Responds to SIGTERM/SIGINT
- **Process Cleanup**: Safely terminates all child processes
- **Resource Cleanup**: Closes database connections properly

### ✅ Auto-restart Policy
- **Policy**: ON_FAILURE with 10 max retries
- **Health Checks**: 30-second timeout
- **Process Monitoring**: Automatic restart of failed subprocesses

## Railway Configuration Files

### `railway.json`
```json
{
  "deploy": {
    "startCommand": "python start.py",
    "restartPolicyType": "ON_FAILURE", 
    "restartPolicyMaxRetries": 10,
    "healthcheckPath": "/health",
    "healthcheckTimeout": 30
  }
}
```

### `nixpacks.toml`
```toml
[phases.setup]
nixPkgs = ['python3', 'gcc', 'pkg-config']

[phases.install]
cmds = [
    'python -m venv --copies /opt/venv',
    '. /opt/venv/bin/activate && pip install -r requirements.txt'
]

[start]
cmd = 'python start.py'
```

## Service Architecture

```
start.py
├── Health Check Server (port from $PORT)
│   ├── /health (basic health check)
│   └── /status (detailed status with DB check)
└── Live Data Launcher
    ├── Database Connection (with retry)
    ├── Live Data Stream Process  
    └── Trade Setup Monitor Process
```

## Deployment Steps

1. **Create Railway Project**
   ```bash
   railway login
   railway init
   ```

2. **Set Environment Variables**
   - Go to Railway dashboard
   - Navigate to your project → Settings → Environment Variables
   - Add all required variables

3. **Deploy**
   ```bash
   railway up
   ```

4. **Monitor Deployment**
   - Check logs in Railway dashboard
   - Verify health endpoint: `https://your-service.railway.app/health`
   - Monitor database connections

## Monitoring & Troubleshooting

### Health Check Endpoints
- **Basic Health**: `/health` - Returns service status
- **Detailed Status**: `/status` - Includes database connectivity

### Common Issues

#### Database Connection Failures
- Check environment variables are set correctly
- Verify Supabase credentials and SSL requirements
- Review connection retry logs

#### Process Crashes
- Railway auto-restarts on failure (up to 10 times)
- Check logs for error patterns
- Monitor memory/CPU usage

#### Health Check Failures
- Verify health endpoint responds within 30 seconds
- Check if main process is responsive
- Review database connectivity issues

### Log Monitoring
```bash
railway logs --follow
```

### Service Scaling
Railway automatically handles:
- Memory scaling (up to plan limits)
- CPU allocation
- Network traffic

## Best Practices

1. **Resource Management**
   - Monitor memory usage through Railway dashboard
   - Use connection pooling for database
   - Implement proper error handling

2. **Reliability**
   - Health checks every 30 seconds
   - Graceful shutdown handling
   - Process monitoring and restart

3. **Security**
   - SSL required for database connections
   - Environment variables for secrets
   - Application name identification

4. **Monitoring**
   - Regular health check monitoring
   - Database connection status
   - Process uptime tracking

## Railway-Specific Optimizations

### Automatic Features
- **PORT Environment Variable**: Railway provides this automatically
- **Health Checks**: Configured to use `/health` endpoint
- **Restart Policy**: Handles temporary failures gracefully
- **SSL Certificates**: Automatic HTTPS for your service

### Service URL
Your service will be available at:
`https://your-service-name.railway.app`

### Cost Optimization
- Service runs only when needed
- Automatic scaling based on usage
- Resource allocation optimization

## Troubleshooting Commands

```bash
# Check service status
railway status

# View real-time logs  
railway logs --follow

# Check environment variables
railway variables

# Restart service
railway up --detach
```

## Support

For Railway-specific issues:
- Railway Documentation: https://docs.railway.app
- Railway Discord: https://discord.gg/railway

For service-specific issues:
- Check application logs in Railway dashboard
- Verify database connectivity
- Review health check endpoint responses