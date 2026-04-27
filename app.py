import os
import sys
import time
import random
import threading
import logging
import re
import json
from datetime import datetime, timedelta
from contextlib import contextmanager
from functools import wraps
from collections import defaultdict
from typing import Optional, Dict, List, Tuple, Any
import signal
import requests

import psycopg2
import psycopg2.extras
from flask import Flask, request, jsonify
from flask_cors import CORS

os.environ['PGSSLMODE'] = 'require'
os.environ['PGCONNECT_TIMEOUT'] = '30'

# ============================================
# КОНФИГУРАЦИЯ
# ============================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)

# 🔥 Фикс CORS для OPTIONS запросов
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    return response

@app.route('/api/<path:path>', methods=['OPTIONS'])
def handle_options(path):
    return '', 200

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")
BOT_TOKEN = os.getenv("BOT_TOKEN")

required_env_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD", "BOT_TOKEN"]
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "60"))
RATE_LIMIT_WINDOW = int(os.getenv("RATE_LIMIT_WINDOW", "60"))

# ============================================
# КАСТОМНЫЕ ИСКЛЮЧЕНИЯ
# ============================================
class AppError(Exception):
    def __init__(self, message: str, status_code: int = 400, error_code: str = None):
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code

class ValidationError(AppError):
    def __init__(self, message: str, details: List[str] = None):
        super().__init__(message, 400, "VALIDATION_ERROR")
        self.details = details or []

class NotFoundError(AppError):
    def __init__(self, message: str):
        super().__init__(message, 404, "NOT_FOUND")

# ============================================
# RATE LIMITING
# ============================================
class RateLimiter:
    def __init__(self):
        self.requests = defaultdict(list)
    
    def is_allowed(self, key: str, limit: int, window: int) -> Tuple[bool, int]:
        now = time.time()
        self.requests[key] = [t for t in self.requests[key] if t > now - window]
        if len(self.requests[key]) >= limit:
            return False, int(window - (now - min(self.requests[key])))
        self.requests[key].append(now)
        return True, 0

rate_limiter = RateLimiter()

def rate_limit(limit: int = None, window: int = None):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            _limit, _window = (limit or RATE_LIMIT_REQUESTS), (window or RATE_LIMIT_WINDOW)
            key = f"{request.remote_addr}:{request.endpoint}"
            allowed, wait = rate_limiter.is_allowed(key, _limit, _window)
            if not allowed:
                return jsonify({"error": "Rate limit exceeded", "retry_after": wait, "error_code": "RATE_LIMIT_EXCEEDED"}), 429
            return f(*args, **kwargs)
        return wrapper
    return decorator

# ============================================
# ПУЛ СОЕДИНЕНИЙ
# ============================================
from psycopg2 import pool

db_pool = None

def init_db_pool():
    global db_pool
    try:
        db_pool = pool.SimpleConnectionPool(2, 15, host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT, connect_timeout=30, keepalives=1, keepalives_idle=5, keepalives_interval=2, keepalives_count=2)
        logger.info("✅ Пул соединений создан")
        test_conn = db_pool.getconn()
        db_pool.putconn(test_conn)
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка создания пула: {e}")
        return False

def get_db_connection():
    if not db_pool: raise AppError("Database pool not initialized", 500)
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur: cur.execute("SELECT 1")
        return conn
    except Exception as e:
        logger.error(f"Ошибка получения соединения: {e}")
        raise

@contextmanager
def get_db_cursor():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        yield cursor
        conn.commit()
    except Exception as e:
        if conn: conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        if conn:
            try: db_pool.putconn(conn)
            except: pass

# ============================================
# КЭШ
# ============================================
class SimpleCache:
    def __init__(self):
        self.cache = {}
    def get(self, key):
        if key in self.cache:
            val, ts = self.cache[key]
            if time.time() - ts < 300: return val
            del self.cache[key]
        return None
    def set(self, key, value):
        self.cache[key] = (value, time.time())
    def delete(self, key):
        self.cache.pop(key, None)

cache = SimpleCache()

# ============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================
def update_user_activity(telegram_id: str):
    try:
        with get_db_cursor() as c:
            c.execute("UPDATE users SET last_active=NOW() AT TIME ZONE 'UTC', is_online=TRUE WHERE telegram_id=%s", (telegram_id,))
    except: pass

def get_player_id(telegram_id: str) -> Optional[str]:
    cached = cache.get(f"pid:{telegram_id}")
    if cached: return cached
    try:
        with get_db_cursor() as c:
            c.execute("SELECT player_id FROM users WHERE telegram_id=%s", (telegram_id,))
            r = c.fetchone()
            if r:
                cache.set(f"pid:{telegram_id}", r[0])
                return r[0]
    except: pass
    return None

def get_profile_cached(player_id: str) -> Optional[Dict]:
    cached = cache.get(f"prof:{player_id}")
    if cached: return cached
    try:
        with get_db_cursor() as c:
            c.execute("SELECT nick, age, steam_link, faceit_link, avatar FROM profiles WHERE player_id=%s", (player_id,))
            r = c.fetchone()
            if r:
                p = dict(r)
                cache.set(f"prof:{player_id}", p)
                return p
    except: pass
    return None

def generate_player_id() -> str:
    return str(random.randint(10000000, 99999999))

def generate_random_nick() -> str:
    return ''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789') for _ in range(8))

# ============================================
# ЭНДПОИНТЫ
# ============================================

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "ok", "version": "2.0.0"})

# ---------- ПОЛЬЗОВАТЕЛЬ ----------
@app.route('/api/user/init', methods=['POST'])
@rate_limit(10, 60)
def user_init():
    data = request.json
    if not data or 'telegram_id' not in data:
        raise ValidationError("Missing telegram_id")
    tid = data['telegram_id']
    username = data.get('username', '')
    
    with get_db_cursor() as c:
        c.execute("SELECT player_id FROM users WHERE telegram_id=%s", (tid,))
        r = c.fetchone()
        if r:
            update_user_activity(tid)
            cache.set(f"pid:{tid}", r[0])
            return jsonify({"status": "ok", "player_id": r[0], "is_new": False})
    
    pid = generate_player_id()
    nick = username if username and len(username) <= 32 else generate_random_nick()
    
    with get_db_cursor() as c:
        c.execute("INSERT INTO users (telegram_id, player_id, created_at, last_active, is_online, leadercoins) VALUES (%s,%s,NOW(),NOW(),TRUE,1000)", (tid, pid))
        c.execute("INSERT INTO profiles (player_id, telegram_id, nick, created_at) VALUES (%s,%s,%s,NOW())", (pid, tid, nick))
    cache.set(f"pid:{tid}", pid)
    return jsonify({"status": "ok", "player_id": pid, "is_new": True, "nick": nick})

@app.route('/api/user/update-username', methods=['POST'])
def update_username():
    data = request.json
    if not data or 'telegram_id' not in data: raise ValidationError("Missing telegram_id")
    tid = data['telegram_id']
    username = data.get('username', '')
    with get_db_cursor() as c:
        c.execute("UPDATE users SET username=%s WHERE telegram_id=%s RETURNING username", (username, tid))
        r = c.fetchone()
        if r: return jsonify({"status": "ok", "username": r[0]})
        raise NotFoundError("User not found")

@app.route('/api/user/rating', methods=['POST'])
def get_user_rating():
    data = request.json
    if not data or 'telegram_id' not in data: raise ValidationError("Missing telegram_id")
    with get_db_cursor() as c:
        c.execute("SELECT COALESCE(rating,0) as rating FROM users WHERE telegram_id=%s", (data['telegram_id'],))
        r = c.fetchone()
    return jsonify({"status": "ok", "rating": r['rating'] if r else 0})

# ---------- ПРОФИЛЬ ----------
@app.route('/api/profile/get', methods=['POST'])
def get_profile():
    data = request.json
    if not data or 'telegram_id' not in data: raise ValidationError("Missing telegram_id")
    update_user_activity(data['telegram_id'])
    pid = get_player_id(data['telegram_id'])
    if not pid: raise NotFoundError("User not found")
    p = get_profile_cached(pid)
    if not p: raise NotFoundError("Profile not found")
    return jsonify({"status": "ok", "nick": p['nick'], "age": p['age'], "steam_link": p['steam_link'], "faceit_link": p['faceit_link']})

@app.route('/api/profile/update', methods=['POST'])
def update_profile():
    data = request.json
    if not data or 'telegram_id' not in data: raise ValidationError("Missing telegram_id")
    pid = get_player_id(data['telegram_id'])
    if not pid: raise NotFoundError("User not found")
    
    fields = []
    vals = []
    for k, col in [('nick','nick'), ('age','age'), ('steam_link','steam_link'), ('faceit_link','faceit_link')]:
        if k in data and data[k] is not None:
            fields.append(f"{col}=%s")
            vals.append(data[k])
    if not fields: raise ValidationError("No fields")
    vals.append(pid)
    
    with get_db_cursor() as c:
        c.execute(f"UPDATE profiles SET {', '.join(fields)} WHERE player_id=%s RETURNING nick, age, steam_link, faceit_link", vals)
        u = c.fetchone()
    cache.delete(f"prof:{pid}")
    return jsonify({"status": "ok", "nick": u[0], "age": u[1], "steam_link": u[2], "faceit_link": u[3]})

# ---------- АВАТАР ----------
@app.route('/api/profile/avatar', methods=['POST'])
def get_avatar():
    data = request.json
    if not data or 'telegram_id' not in data: raise ValidationError("Missing telegram_id")
    pid = get_player_id(data['telegram_id'])
    if not pid: raise NotFoundError("User not found")
    with get_db_cursor() as c:
        c.execute("SELECT avatar FROM profiles WHERE player_id=%s", (pid,))
        r = c.fetchone()
    return jsonify({"status": "ok", "avatar": r[0] if r and r[0] else None})

@app.route('/api/profile/avatar/update', methods=['POST'])
def update_avatar():
    data = request.json
    if not data or 'telegram_id' not in data: raise ValidationError("Missing telegram_id")
    avatar_data = data.get('avatar_url') or data.get('avatar')
    if not avatar_data: raise ValidationError("Missing avatar data")
    pid = get_player_id(data['telegram_id'])
    if not pid: raise NotFoundError("User not found")
    with get_db_cursor() as c:
        c.execute("UPDATE profiles SET avatar=%s WHERE player_id=%s RETURNING avatar", (avatar_data, pid))
        u = c.fetchone()
    cache.delete(f"prof:{pid}")
    return jsonify({"status": "ok", "avatar": u[0] if u else None})

# ---------- АНКЕТЫ (profiles_extra) ----------
@app.route('/api/anketa/create', methods=['POST'])
@rate_limit(10, 60)
def create_anketa():
    data = request.json
    if not data or 'telegram_id' not in data or 'mode' not in data: raise ValidationError("Missing fields")
    pid = get_player_id(data['telegram_id'])
    if not pid: raise NotFoundError("User not found")
    
    mode = data['mode'].lower()
    if mode not in ('faceit','premier','prime','public'): raise ValidationError("Invalid mode")
    
    rank = data.get('rank', '')
    about = data.get('about', '')
    
    # Обновляем профиль если переданы
    if data.get('age'):
        with get_db_cursor() as c:
            c.execute("UPDATE profiles SET age=%s WHERE player_id=%s", (int(data['age']), pid))
    for link_type in ('steam_link', 'faceit_link'):
        if data.get(link_type):
            with get_db_cursor() as c:
                c.execute(f"UPDATE profiles SET {link_type}=%s WHERE player_id=%s", (data[link_type], pid))
    cache.delete(f"prof:{pid}")
    
    with get_db_cursor() as c:
        c.execute("SELECT id FROM profiles_extra WHERE player_id=%s AND mode=%s AND is_active=TRUE", (pid, mode))
        existing = c.fetchone()
        if existing:
            c.execute("UPDATE profiles_extra SET rank=%s, about=%s, updated_at=NOW() WHERE id=%s RETURNING id", (rank, about, existing[0]))
        else:
            c.execute("INSERT INTO profiles_extra (player_id, mode, rank, about) VALUES (%s,%s,%s,%s) RETURNING id", (pid, mode, rank, about))
        anketa_id = c.fetchone()[0]
    
    return jsonify({"status": "ok", "anketa_id": anketa_id})

@app.route('/api/anketa/next', methods=['POST'])
def get_next_anketa():
    data = request.json
    if not data or 'telegram_id' not in data: raise ValidationError("Missing telegram_id")
    mode = data.get('mode', '').lower()
    pid = get_player_id(data['telegram_id'])
    if not pid: raise NotFoundError("User not found")
    
    with get_db_cursor() as c:
        if mode and mode != 'all':
            c.execute("""
                SELECT pe.*, p.nick, p.age, p.steam_link, p.faceit_link, p.avatar
                FROM profiles_extra pe
                JOIN profiles p ON pe.player_id = p.player_id
                WHERE pe.is_active=TRUE AND pe.mode=%s AND pe.player_id!=%s
                AND pe.player_id NOT IN (SELECT liked_player_id FROM likes WHERE liker_player_id=%s)
                ORDER BY pe.created_at DESC LIMIT 1
            """, (mode, pid, pid))
        else:
            c.execute("""
                SELECT pe.*, p.nick, p.age, p.steam_link, p.faceit_link, p.avatar
                FROM profiles_extra pe
                JOIN profiles p ON pe.player_id = p.player_id
                WHERE pe.is_active=TRUE AND pe.player_id!=%s
                AND pe.player_id NOT IN (SELECT liked_player_id FROM likes WHERE liker_player_id=%s)
                ORDER BY pe.created_at DESC LIMIT 1
            """, (pid, pid))
        r = c.fetchone()
    
    if not r:
        return jsonify({"status": "empty", "message": "Анкеты закончились"})
    
    return jsonify({"status": "ok", "anketa": dict(r)})

# ---------- ЛАЙКИ ----------
@app.route('/api/like', methods=['POST'])
@rate_limit(30, 60)
def like_player():
    data = request.json
    if not data or 'telegram_id' not in data or 'liked_player_id' not in data: raise ValidationError("Missing fields")
    pid = get_player_id(data['telegram_id'])
    if not pid: raise NotFoundError("User not found")
    liked = data['liked_player_id']
    
    with get_db_cursor() as c:
        try:
            c.execute("INSERT INTO likes (liker_player_id, liked_player_id) VALUES (%s,%s)", (pid, liked))
        except:
            return jsonify({"status": "already_liked"})
        
        c.execute("SELECT id FROM likes WHERE liker_player_id=%s AND liked_player_id=%s", (liked, pid))
        is_match = c.fetchone() is not None
    
    if is_match:
        with get_db_cursor() as c:
            c.execute("SELECT telegram_id FROM users WHERE player_id=%s", (pid,))
            u1 = c.fetchone()
            c.execute("SELECT telegram_id, username FROM users WHERE player_id=%s", (liked,))
            u2 = c.fetchone()
            c.execute("SELECT nick FROM profiles WHERE player_id=%s", (pid,))
            n1 = c.fetchone()
            c.execute("SELECT nick FROM profiles WHERE player_id=%s", (liked,))
            n2 = c.fetchone()
        
        # Добавляем в друзья
        with get_db_cursor() as c:
            c.execute("SELECT 1 FROM friends WHERE (player1_id=%s AND player2_id=%s) OR (player1_id=%s AND player2_id=%s)", (pid, liked, liked, pid))
            if not c.fetchone():
                c.execute("INSERT INTO friends (player1_id, player2_id, created_at) VALUES (%s,%s,NOW())", (pid, liked))
        
        for uid, partner_nick, partner_uname in [(u1[0], n2[0], u2[1]), (u2[0], n1[0], u1[1])]:
            try:
                msg = f"❤️ Взаимный мэтч!\n\nТы и {partner_nick} лайкнули друг друга!\n\nНапиши ему: @{partner_uname}" if partner_uname else f"❤️ Взаимный мэтч!\n\nТы и {partner_nick} лайкнули друг друга!"
                requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={"chat_id": uid, "text": msg}, timeout=3)
            except: pass
        
        return jsonify({"status": "match", "message": "Взаимный лайк!"})
    
    # Уведомление тому кого лайкнули
    with get_db_cursor() as c:
        c.execute("SELECT telegram_id, username FROM users WHERE player_id=%s", (liked,))
        u = c.fetchone()
        c.execute("SELECT nick FROM profiles WHERE player_id=%s", (pid,))
        n = c.fetchone()
    try:
        requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={"chat_id": u[0], "text": f"👍 {n[0]} лайкнул твою анкету!\n\nЗапусти бота чтобы посмотреть: @pingster_team_bot"}, timeout=3)
    except: pass
    
    return jsonify({"status": "liked"})

@app.route('/api/likes/list', methods=['POST'])
def likes_list():
    data = request.json
    if not data or 'telegram_id' not in data: raise ValidationError("Missing telegram_id")
    pid = get_player_id(data['telegram_id'])
    if not pid: raise NotFoundError("User not found")
    
    with get_db_cursor() as c:
        c.execute("SELECT l.liked_player_id, p.nick, p.avatar, pe.mode, pe.rank FROM likes l JOIN profiles p ON l.liked_player_id=p.player_id LEFT JOIN profiles_extra pe ON p.player_id=pe.player_id AND pe.is_active=TRUE WHERE l.liker_player_id=%s AND l.liked_player_id IN (SELECT liker_player_id FROM likes WHERE liked_player_id=%s)", (pid, pid))
        mutual = [dict(r) for r in c.fetchall()]
        
        c.execute("SELECT l.liker_player_id, p.nick, p.avatar, pe.mode, pe.rank FROM likes l JOIN profiles p ON l.liker_player_id=p.player_id LEFT JOIN profiles_extra pe ON p.player_id=pe.player_id AND pe.is_active=TRUE WHERE l.liked_player_id=%s AND l.liker_player_id NOT IN (SELECT liked_player_id FROM likes WHERE liker_player_id=%s)", (pid, pid))
        liked_me = [dict(r) for r in c.fetchall()]
        
        c.execute("SELECT l.liked_player_id, p.nick, p.avatar, pe.mode, pe.rank FROM likes l JOIN profiles p ON l.liked_player_id=p.player_id LEFT JOIN profiles_extra pe ON p.player_id=pe.player_id AND pe.is_active=TRUE WHERE l.liker_player_id=%s AND l.liked_player_id NOT IN (SELECT liker_player_id FROM likes WHERE liked_player_id=%s)", (pid, pid))
        i_liked = [dict(r) for r in c.fetchall()]
    
    return jsonify({"status": "ok", "mutual": mutual, "liked_me": liked_me, "i_liked": i_liked})

# ---------- ДРУЗЬЯ / ЛИДЕРБОРД ----------
@app.route('/api/friends/list', methods=['POST'])
def friends_list():
    data = request.json
    if not data or 'telegram_id' not in data: raise ValidationError("Missing telegram_id")
    pid = get_player_id(data['telegram_id'])
    if not pid: raise NotFoundError("User not found")
    with get_db_cursor() as c:
        c.execute("SELECT CASE WHEN player1_id=%s THEN player2_id ELSE player1_id END as fid FROM friends WHERE player1_id=%s OR player2_id=%s", (pid, pid, pid))
        friends = []
        for r in c.fetchall():
            p = get_profile_cached(r[0])
            if p: friends.append({"player_id": r[0], "nick": p['nick'], "avatar": p.get('avatar')})
    return jsonify({"status": "ok", "friends": friends})

@app.route('/api/users/leaderboard', methods=['POST'])
def leaderboard():
    with get_db_cursor() as c:
        c.execute("SELECT u.player_id, p.nick, p.avatar, COALESCE(u.leadercoins,0) as coins FROM users u JOIN profiles p ON u.player_id=p.player_id ORDER BY coins DESC LIMIT 20")
        lb = [dict(r) for r in c.fetchall()]
    return jsonify({"status": "ok", "leaderboard": lb})

# ============================================
# ОБРАБОТЧИКИ ОШИБОК
# ============================================
@app.errorhandler(AppError)
def handle_app_error(e):
    return jsonify({"error": str(e), "error_code": e.error_code}), e.status_code

@app.errorhandler(404)
def handle_404(e):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def handle_500(e):
    logger.error(f"500: {e}")
    return jsonify({"error": "Internal server error"}), 500

# ============================================
# ДЛЯ GUNICORN (PRODUCTION)
# ============================================
if db_pool is None:
    init_db_pool()

application = app

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f"🔥 PINGSTER v2.0 на порту {port}")
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
