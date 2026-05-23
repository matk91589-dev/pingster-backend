#!/bin/bash
cd /app

echo "=========================================="
echo "🚀 PINGSTER STARTUP"
echo "=========================================="

# Убиваем старые процессы
pkill -9 -f "bot.py" 2>/dev/null
sleep 1

# 🔥 Запускаем сервер через waitress (работает с Python 3.14)
nohup waitress-serve --port=8000 app:application > /app/server.log 2>&1 &
echo "✅ Server started (PID: $!)"

sleep 2

# 🔥 Запускаем бота
nohup python3 bot.py > /app/bot.log 2>&1 &
echo "✅ Bot started (PID: $!)"

echo ""
echo "📋 Server logs: tail -f /app/server.log"
echo "📋 Bot logs:    tail -f /app/bot.log"
echo "=========================================="
