#!/bin/bash
cd /app

echo "=========================================="
echo "🚀 PINGSTER STARTUP"
echo "=========================================="

# Убиваем старые процессы
pkill -9 -f "flask run" 2>/dev/null
pkill -9 -f "bot.py" 2>/dev/null
sleep 2

# Запускаем Flask на порту 5001
nohup python3 -m flask run --host=0.0.0.0 --port=5001 > flask.log 2>&1 &
echo "✅ Flask запущен (PID: $!)"

sleep 3

# Запускаем бота
nohup python3 bot.py > bot.log 2>&1 &
echo "✅ Бот запущен (PID: $!)"

echo ""
echo "📋 Смотреть логи Flask:   tail -f /app/flask.log"
echo "📋 Смотреть логи бота:    tail -f /app/bot.log"
echo "=========================================="
