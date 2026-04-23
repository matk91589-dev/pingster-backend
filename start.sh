#!/bin/bash
cd /app

echo "=========================================="
echo "🚀 PINGSTER STARTUP"
echo "=========================================="

# Убиваем старые процессы
pkill -9 -f "app.py" 2>/dev/null
pkill -9 -f "bot.py" 2>/dev/null
pkill -9 -f "flask" 2>/dev/null
sleep 2

# Запускаем Flask через python3 app.py (чтобы воркеры работали!)
nohup python3 app.py > flask.log 2>&1 &
echo "✅ Flask запущен (PID: $!)"

sleep 3

# Запускаем бота
nohup python3 bot.py > bot.log 2>&1 &
echo "✅ Бот запущен (PID: $!)"

echo ""
echo "📋 Смотреть логи Flask:   tail -f /app/flask.log"
echo "📋 Смотреть логи бота:    tail -f /app/bot.log"
echo "=========================================="
