# Trading Data Service - Cloud Deployment Package

This is the standalone live data service that captures real-time market data and validates trade setups. It connects to your Supabase database and runs independently from your main trading application.

## 🎯 What This Service Does

- **Live Market Data**: Captures real-time NQ futures data from Databento
- **Trade Validation**: Monitors trade setups and triggers entries/exits
- **Database Updates**: Stores OHLCV data and trade events in Supabase
- **24/7 Operation**: Designed to run continuously in the cloud

## 📦 Package Contents

```
trading-data-service/
├── price_capture/               # Live data components
│   ├── live_data_launcher.py   # Main orchestrator
│   ├── simple_ohlcv.py         # Live data streaming
│   ├── setup_monitor.py        # Trade validation logic
│   ├── live_data_service.py    # Historical backfill
│   └── monitor_setup_validator.py  # Real-time monitoring
├── database/                    # Database utilities
│   ├── trading_db.py           # Database operations
│   ├── db.py                   # Connection management
│   ├── utils.py                # Helper functions
│   └── __init__.py
├── requirements.txt            # Python dependencies
├── config.example             # Environment variables template
└── README.md                  # This file
```

## 🚀 Quick Deployment

### Option 1: Railway (Recommended)
1. Push this folder to a Git repository
2. Connect Railway to your repository
3. Set environment variables (see below)
4. Deploy as a worker service

### Option 2: Render
1. Connect your repository to Render
2. Create a new Background Worker
3. Set build command: `pip install -r requirements.txt`
4. Set start command: `python price_capture/live_data_launcher.py`

### Option 3: VPS Deployment
```bash
# Install dependencies
pip install -r requirements.txt

# Create systemd service
sudo cp live-data.service /etc/systemd/system/
sudo systemctl enable live-data
sudo systemctl start live-data
```

## ⚙️ Environment Variables

Required environment variables (copy from config.example):

```bash
# Databento API Key
DATABENTO_API_KEY=your_api_key_here

# Supabase Database Connection
host=your_supabase_host
user=your_db_user
password=your_db_password
port=5432
dbname=postgres
```

## 🔧 Local Testing

Before deploying, test the service locally:

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
cp config.example .env
# Edit .env with your actual values

# Test the service
python price_capture/live_data_launcher.py --live-only
```

## 📊 Monitoring

The service provides logs for monitoring:
- Market data ingestion status
- Trade setup validations
- Database connection health
- Error reporting

## 🔄 Integration with Main App

This service communicates with your main trading application through the shared Supabase database:

- **Writes**: Live OHLCV data, trade status updates
- **Reads**: Trade setups created by your main app
- **No direct API calls** between services

## 💰 Cost Estimates

- **Railway**: $5-10/month
- **Render**: $7/month  
- **VPS**: $3-5/month

## 🛠️ Troubleshooting

Common issues:
1. **Database connection**: Check Supabase credentials
2. **API limits**: Verify Databento API key and quota
3. **Missing data**: Check market hours and symbol availability
4. **Import errors**: Ensure all dependencies are installed

## 📋 Service Commands

```bash
# Full service (backfill + live data + monitoring)
python price_capture/live_data_launcher.py

# Live data only
python price_capture/live_data_launcher.py --live-only

# Backfill only
python price_capture/live_data_launcher.py --backfill-only --days 7

# Disable monitoring
python price_capture/live_data_launcher.py --no-monitor
```

Your main trading application continues to run locally while this service handles 24/7 data capture in the cloud.