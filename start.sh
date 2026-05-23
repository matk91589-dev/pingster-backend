#!/bin/bash
cd /app

echo "🚀 Starting bot only..."
pkill -9 -f "bot.py" 2>/dev/null
sleep 1

# Только бот, gunicorn запускает Render сам!
nohup python3 bot.py > /app/bot.log 2>&1 &
echo "✅ Bot started (PID: $!)"
