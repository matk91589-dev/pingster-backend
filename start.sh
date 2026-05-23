#!/bin/bash
cd /app

echo "=========================================="
echo "🚀 PINGSTER STARTUP"
echo "=========================================="

# Убиваем старые процессы бота
pkill -9 -f "bot.py" 2>/dev/null
sleep 1

# 🔥 Только бот! Gunicorn запускает Render через Start Command
nohup python3 bot.py > /app/bot.log 2>&1 &
echo "✅ Bot started (PID: $!)"

echo ""
echo "📋 Bot logs: tail -f /app/bot.log"
echo "=========================================="
