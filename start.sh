#!/bin/bash
cd /app

echo "=========================================="
echo "🚀 PINGSTER STARTUP"
echo "=========================================="

# Убиваем старые процессы
pkill -9 -f "gunicorn" 2>/dev/null
pkill -9 -f "bot.py" 2>/dev/null
sleep 2

# 🔥 Запускаем Flask через gunicorn
nohup gunicorn app:application \
    --bind 0.0.0.0:5000 \
    --workers 4 \
    --timeout 120 \
    --log-level info \
    --error-logfile /app/gunicorn-error.log \
    --access-logfile /app/gunicorn-access.log \
    > /app/gunicorn.log 2>&1 &
echo "✅ Gunicorn запущен (PID: $!)"

sleep 3

# 🔥 Запускаем бота
nohup python3 bot.py > /app/bot.log 2>&1 &
echo "✅ Бот запущен (PID: $!)"

echo ""
echo "📋 Логи Gunicorn:  tail -f /app/gunicorn.log"
echo "📋 Логи Gunicorn ошибки: tail -f /app/gunicorn-error.log"
echo "📋 Логи бота:      tail -f /app/bot.log"
echo "=========================================="
