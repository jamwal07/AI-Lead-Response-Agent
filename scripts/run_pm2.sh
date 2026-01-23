#!/bin/bash
# Patch 3: Deployment Safety with PM2
# Usage: ./scripts/run_pm2.sh

# Ensure PM2 is installed
if ! command -v pm2 &> /dev/null
then
    echo "PM2 could not be found. Installing via npm..."
    npm install pm2 -g
fi

echo "🚀 Starting PlumberAI with PM2..."

# 1. Start the Flask App (Webhook Server)
pm2 start execution/run_app.py --interpreter python3 --name plumber-web --restart-delay=3000

# 2. Start the SMS Worker (Background Job)
pm2 start execution/utils/sms_engine.py --interpreter python3 --name plumber-worker --restart-delay=3000

echo "✅ System Online. View logs with: pm2 logs"
pm2 save
