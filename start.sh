#!/bin/bash
cd /app

# Жёстко убиваем ВСЁ
pkill -9 -f python3 2>/dev/null
sleep 2

# Запускаем Flask на другом порту (5001)
nohup python3 -m flask run --host=0.0.0.0 --port=5001 > flask.log 2>&1 &
echo "✅ Flask запущен на порту 5001 (PID: $!)"

sleep 3

# Запускаем бота
nohup python3 bot.py > bot.log 2>&1 &
echo "✅ Бот запущен (PID: $!)"
