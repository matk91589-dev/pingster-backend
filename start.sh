#!/bin/bash
cd /app

echo "🚀 STARTING PINGSTER"

# убиваем старые gunicorn процессы
for pid in /proc/[0-9]*; do
  cmd=$(cat $pid/cmdline 2>/dev/null | tr '\0' ' ')
  echo "$cmd" | grep -q gunicorn && kill -9 ${pid##*/} 2>/dev/null
done

sleep 2

echo "🚀 RUN GUNICORN"

exec gunicorn \
  -w 1 \
  -b 0.0.0.0:8000 \
  --timeout 120 \
  --worker-tmp-dir /dev/shm \
  app:app \
  --access-logfile /app/access.log \
  --error-logfile /app/server.log
