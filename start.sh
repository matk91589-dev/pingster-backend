#!/bin/bash
cd /app

echo "=========================================="
echo "🚀 PINGSTER STARTUP (GUNICORN ONLY)"
echo "=========================================="

# ==========================================
# 🔥 УБИВАЕМ СТАРЫЙ GUNICORN (через /proc)
# ==========================================
echo "🧹 Stopping old gunicorn processes..."

for pid in /proc/[0-9]*; do
  cmd=$(cat $pid/cmdline 2>/dev/null | tr '\0' ' ')
  echo "$cmd" | grep -q "gunicorn" >/dev/null && kill -9 ${pid##*/}
done

sleep 2

# ==========================================
# 🔥 УБИВАЕМ СТАРЫЕ БОТЫ
# ==========================================
echo "🧹 Stopping old bot processes..."

for pid in /proc/[0-9]*; do
  cmd=$(cat $pid/cmdline 2>/dev/null | tr '\0' ' ')
  echo "$cmd" | grep -q "bot.py" >/dev/null && kill -9 ${pid##*/}
done

sleep 1

# ==========================================
# 🔥 ЗАПУСК GUNICORN
# ==========================================
echo "🚀 Starting Gunicorn..."

nohup gunicorn \
  -w 4 \
  -b 0.0.0.0:8000 \
  app:app \
  --access-logfile /app/access.log \
  --error-logfile /app/server.log \
  --log-level info \
  > /dev/null 2>&1 &

echo "✅ Server started (PID: $!)"

sleep 2

# ==========================================
# 🔥 ЗАПУСК БОТА
# ==========================================
echo "🤖 Starting bot..."

nohup python3 bot.py > /app/bot.log 2>&1 &

echo "✅ Bot started (PID: $!)"

echo ""
echo "📋 Server logs: tail -f /app/server.log"
echo "📋 Bot logs:    tail -f /app/bot.log"
echo "=========================================="
