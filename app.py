import sys
import os
import threading
import time
import hashlib
import hmac
import base64
from datetime import datetime, timedelta
import random
import logging
import requests
from functools import lru_cache, wraps
from contextlib import contextmanager
from cachetools import TTLCache
from psycopg2 import pool
import psycopg2.extras
from flask import Flask, request, jsonify
from flask_cors import CORS

# ============================================
# КОНФИГУРАЦИЯ
# ============================================
BUILD_VERSION = int(time.time())

sys.path.append('/app/.local/lib/python3.14/site-packages')
sys.path.append(os.path.expanduser('~/.local/lib/python3.14/site-packages'))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Конфигурация БД
DB_HOST = os.getenv("DB_HOST", "85.239.33.182")
DB_NAME = os.getenv("DB_NAME", "pingster_db")
DB_USER = os.getenv("DB_USER", "gen_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "{,@~:5my>jvOAj")
DB_PORT = os.getenv("DB_PORT", 5432)

# Токен бота
BOT_TOKEN = "8484054850:AAGwAcn1URrcKtikJKclqP8Z8oYs0wbIYY8"
BOT_USERNAME = "PingsterBot"
FORUM_GROUP_ID = -1003753772298
RULES_TOPIC_ID = 5
SECRET_KEY = os.getenv("SECRET_KEY", "pingster_super_secret_key_2026_change_this")

# ============================================
# ПУЛ СОЕДИНЕНИЙ
# ============================================
db_pool = None

def init_db_pool():
    global db_pool
    try:
        db_pool = pool.ThreadedConnectionPool(
            minconn=5,
            maxconn=30,
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            connect_timeout=3,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=5,
            keepalives_count=2
        )
        logger.info(f"✅ Пул соединений создан: min=5, max=30")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка создания пула: {e}")
        return False

@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = db_pool.getconn()
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            db_pool.putconn(conn)

@contextmanager
def get_db_cursor(commit=True):
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()

# ============================================
# КЭШИРОВАНИЕ
# ============================================
player_id_cache = TTLCache(maxsize=5000, ttl=300)
profile_cache = TTLCache(maxsize=5000, ttl=300)
forum_cache = TTLCache(maxsize=1000, ttl=600)

def get_player_id_cached(telegram_id):
    cache_key = f"player_id_{telegram_id}"
    if cache_key in player_id_cache:
        return player_id_cache[cache_key]
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT player_id FROM users WHERE telegram_id = %s", (telegram_id,))
            result = cursor.fetchone()
            if result:
                player_id_cache[cache_key] = result[0]
                return result[0]
    except Exception as e:
        logger.error(f"Ошибка get_player_id: {e}")
    return None

def get_profile_cached(player_id):
    cache_key = f"profile_{player_id}"
    if cache_key in profile_cache:
        return profile_cache[cache_key]
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT nick, age, steam_link, faceit_link, avatar, created_at
                FROM profiles WHERE player_id = %s
            """, (player_id,))
            result = cursor.fetchone()
            if result:
                profile_cache[cache_key] = dict(result)
                return dict(result)
    except Exception as e:
        logger.error(f"Ошибка get_profile: {e}")
    return None

def invalidate_user_cache(telegram_id=None, player_id=None):
    if telegram_id:
        player_id_cache.pop(f"player_id_{telegram_id}", None)
    if player_id:
        profile_cache.pop(f"profile_{player_id}", None)

# ============================================
# ДЕКОРАТОР ДЛЯ ЗАМЕРА ВРЕМЕНИ
# ============================================
def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = (time.time() - start) * 1000
        if elapsed > 100:
            logger.warning(f"⚠️ {func.__name__} занял {elapsed:.0f}ms")
        return result
    return wrapper

# ============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================
def generate_player_id():
    return str(random.randint(10000000, 99999999))

def generate_random_nick():
    chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    return ''.join(random.choice(chars) for _ in range(6))

RANK_TO_VALUE = {
    'Silver 1': 1000, 'Silver 2': 1100, 'Silver 3': 1200, 'Silver 4': 1300,
    'Silver Elite': 1400, 'Gold Nova 1': 1500, 'Gold Nova 2': 1600,
    'Gold Nova 3': 1700, 'Gold Nova Master': 1800, 'Master Guardian 1': 1900,
    'Master Guardian 2': 2000, 'Master Guardian Elite': 2100,
    'Distinguished Master Guardian': 2200, 'Legendary Eagle': 2300,
    'Legendary Eagle Master': 2400, 'Supreme Master First Class': 2500,
    'Global Elite': 2600
}

RANK_LIST = list(RANK_TO_VALUE.keys())

def get_rank_index(rank):
    return RANK_LIST.index(rank) if rank in RANK_LIST else 0

def calculate_score(player, candidate, mode):
    player_rating = int(player['rank']) if player.get('rank', '').isdigit() else 0
    cand_rating = int(candidate.get('rank', '')) if candidate.get('rank', '').isdigit() else 0
    
    if mode in ['faceit', 'premier']:
        rating_diff = abs(player_rating - cand_rating)
    else:
        rating_diff = abs(get_rank_index(player.get('rank', '')) - get_rank_index(candidate.get('rank', ''))) * 100
    
    age_diff = abs((player.get('age') or 0) - (candidate.get('age') or 0))
    return rating_diff + age_diff * 100

def get_range_buckets(mode, style, bucket):
    if mode == 'faceit':
        return bucket - 4, bucket + 4
    elif mode == 'premier' and style == 'tryhard':
        return bucket - 5, bucket + 5
    return None, None

# ============================================
# ФУНКЦИИ ДЛЯ ЗАЩИЩЕННЫХ ССЫЛОК
# ============================================
def generate_match_token(match_id, user_id, expires_at):
    data = f"{match_id}:{user_id}:{expires_at}"
    signature = hmac.new(SECRET_KEY.encode(), data.encode(), hashlib.sha256).hexdigest()[:16]
    token = f"{match_id}:{user_id}:{expires_at}:{signature}"
    return base64.urlsafe_b64encode(token.encode()).decode().rstrip('=')

def verify_match_token(token):
    try:
        padded_token = token + '=' * (4 - len(token) % 4) if len(token) % 4 else token
        decoded = base64.urlsafe_b64decode(padded_token.encode()).decode()
        parts = decoded.split(':')
        
        if len(parts) != 4:
            return None, None
            
        match_id, user_id, expires_at, signature = parts
        expires_at = int(expires_at)
        
        if int(time.time()) > expires_at:
            return None, None
        
        expected_data = f"{match_id}:{user_id}:{expires_at}"
        expected_signature = hmac.new(SECRET_KEY.encode(), expected_data.encode(), hashlib.sha256).hexdigest()[:16]
        
        if hmac.compare_digest(signature, expected_signature):
            return match_id, user_id
        return None, None
    except Exception as e:
        logger.error(f"Ошибка проверки токена: {e}")
        return None, None

def check_user_in_forum_cached(user_id):
    cache_key = f"forum_{user_id}"
    if cache_key in forum_cache:
        return forum_cache[cache_key]
    
    try:
        response = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember",
            params={"chat_id": FORUM_GROUP_ID, "user_id": int(user_id)},
            timeout=3
        )
        data = response.json()
        status = data.get('result', {}).get('status') if data.get('ok') else None
        result = status in ['member', 'administrator', 'creator']
        forum_cache[cache_key] = result
        return result
    except Exception as e:
        logger.error(f"Ошибка проверки форума: {e}")
        return False

def eject_intruder(user_id, topic_id):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/banChatMember",
            json={"chat_id": FORUM_GROUP_ID, "user_id": int(user_id), "until_date": int(time.time()) + 30},
            timeout=3
        )
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/unbanChatMember",
            json={"chat_id": FORUM_GROUP_ID, "user_id": int(user_id), "only_if_banned": True},
            timeout=3
        )
        rules_link = f"https://t.me/c/{str(FORUM_GROUP_ID).replace('-100', '')}/{RULES_TOPIC_ID}"
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": int(user_id),
                "text": f"⛔ **Вы перешли в чужой матч!**\n\nПожалуйста, ознакомьтесь с [правилами]({rules_link}).",
                "parse_mode": "Markdown"
            },
            timeout=3
        )
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO intrusions (user_id, topic_id, created_at)
                VALUES (%s, %s, (NOW() AT TIME ZONE 'UTC'))
            """, (str(user_id), str(topic_id)))
        return True
    except Exception as e:
        logger.error(f"Ошибка при выкидывании чужака: {e}")
        return False

# ============================================
# ФОНОВЫЕ ПРОЦЕССЫ
# ============================================
def close_topic(topic_id):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/closeForumTopic",
            json={"chat_id": FORUM_GROUP_ID, "message_thread_id": topic_id},
            timeout=3
        )
        with get_db_cursor() as cursor:
            cursor.execute("UPDATE games SET status = 'closed' WHERE telegram_chat_id = %s", (str(topic_id),))
        logger.info(f"✅ Тема {topic_id} закрыта")
    except Exception as e:
        logger.error(f"Ошибка закрытия темы: {e}")

def background_worker():
    logger.info("🚀 Фоновый поток для закрытия тем запущен")
    while True:
        try:
            with get_db_cursor() as cursor:
                cursor.execute("""
                    SELECT telegram_chat_id FROM games 
                    WHERE expires_at < (NOW() AT TIME ZONE 'UTC') AND status = 'active'
                """)
                for topic in cursor.fetchall():
                    close_topic(topic[0])
                    time.sleep(0.3)
        except Exception as e:
            logger.error(f"Ошибка фонового процесса: {e}")
        time.sleep(60)

def queue_cleaner_worker():
    logger.info("🧹 Очистка очереди поиска запущена")
    while True:
        try:
            with get_db_cursor() as cursor:
                cursor.execute("DELETE FROM search_queue WHERE expires_at < (NOW() AT TIME ZONE 'UTC')")
                if cursor.rowcount:
                    logger.info(f"🧹 Очищено {cursor.rowcount} записей из очереди")
        except Exception as e:
            logger.error(f"Ошибка очистки очереди: {e}")
        time.sleep(5)

# ============================================
# ЭНДПОИНТЫ
# ============================================
@app.route('/', methods=['GET'])
def home():
    return "Pingster backend is running!"

@app.route('/api', methods=['GET'])
def api_root():
    return jsonify({"message": "Pingster API is running!", "status": "ok"})

# ---------- ПРОФИЛЬ ----------
@app.route('/api/profile/get', methods=['POST'])
@timing_decorator
def get_profile():
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    telegram_id = request.json['telegram_id']
    player_id = get_player_id_cached(telegram_id)
    
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    profile = get_profile_cached(player_id)
    if not profile:
        return jsonify({"error": "Profile not found"}), 404
    
    return jsonify({"status": "ok", **profile})

@app.route('/api/profile/update', methods=['POST'])
@timing_decorator
def update_profile():
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    data = request.json
    telegram_id = data['telegram_id']
    player_id = get_player_id_cached(telegram_id)
    
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    update_fields = []
    update_values = []
    
    for field in ['nick', 'age', 'steam_link', 'faceit_link', 'avatar']:
        if field in data and data[field] is not None:
            update_fields.append(f"{field} = %s")
            update_values.append(data[field])
    
    if not update_fields:
        return jsonify({"error": "No fields to update"}), 400
    
    update_values.append(player_id)
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute(f"""
                UPDATE profiles SET {', '.join(update_fields)}
                WHERE player_id = %s
                RETURNING nick, age, steam_link, faceit_link, avatar
            """, update_values)
            updated = cursor.fetchone()
        
        invalidate_user_cache(telegram_id=telegram_id, player_id=player_id)
        
        return jsonify({
            "status": "ok",
            "nick": updated[0] if updated else None,
            "age": updated[1] if updated else None,
            "steam_link": updated[2] if updated else None,
            "faceit_link": updated[3] if updated else None,
            "avatar": updated[4] if updated else None
        })
    except Exception as e:
        logger.error(f"Ошибка update_profile: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/profile/avatar/update', methods=['POST'])
@timing_decorator
def update_avatar():
    if not request.json or 'telegram_id' not in request.json or 'avatar_url' not in request.json:
        return jsonify({"error": "Missing fields"}), 400
    
    telegram_id = request.json['telegram_id']
    avatar_url = request.json['avatar_url']
    player_id = get_player_id_cached(telegram_id)
    
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("UPDATE profiles SET avatar = %s WHERE player_id = %s RETURNING avatar", (avatar_url, player_id))
            updated = cursor.fetchone()
        
        invalidate_user_cache(player_id=player_id)
        
        return jsonify({"status": "ok", "avatar_url": updated[0] if updated else None})
    except Exception as e:
        logger.error(f"Ошибка update_avatar: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/profile/avatar/get', methods=['POST'])
@timing_decorator
def get_avatar():
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    player_id = get_player_id_cached(request.json['telegram_id'])
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    profile = get_profile_cached(player_id)
    return jsonify({"status": "ok", "avatar_url": profile.get('avatar') if profile else None})

# ---------- ПОЛЬЗОВАТЕЛЬ ----------
@app.route('/api/user/init', methods=['POST'])
@timing_decorator
def user_init():
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    data = request.json
    telegram_id = data['telegram_id']
    username = data.get('username', '')
    
    player_id = get_player_id_cached(telegram_id)
    
    if player_id:
        return jsonify({"status": "ok", "player_id": player_id})
    
    player_id = generate_player_id()
    nick = username if username else generate_random_nick()
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO users (telegram_id, player_id, created_at)
                VALUES (%s, %s, (NOW() AT TIME ZONE 'UTC'))
            """, (telegram_id, player_id))
            cursor.execute("""
                INSERT INTO profiles (player_id, nick, avatar, created_at)
                VALUES (%s, %s, %s, (NOW() AT TIME ZONE 'UTC'))
            """, (player_id, nick, None))
        
        invalidate_user_cache(telegram_id=telegram_id)
        
        return jsonify({"status": "ok", "player_id": player_id})
    except Exception as e:
        logger.error(f"Ошибка user_init: {e}")
        return jsonify({"error": str(e)}), 500

# ---------- ПОИСК ----------
@app.route('/api/search/start', methods=['POST'])
@timing_decorator
def start_search():
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    data = request.json
    telegram_id = data['telegram_id']
    player_id = get_player_id_cached(telegram_id)
    
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    mode = data.get('mode', '').lower()
    rank_value = data.get('rating_value', '0')
    
    rating_number = int(rank_value) if mode in ['faceit', 'premier'] else RANK_TO_VALUE.get(rank_value, 1000)
    rating_bucket = rating_number // 100
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT id FROM matches 
                WHERE (player1_id = %s OR player2_id = %s) 
                AND status IN ('pending', 'accepted')
                AND expires_at > (NOW() AT TIME ZONE 'UTC')
            """, (player_id, player_id))
            if cursor.fetchone():
                return jsonify({"error": "Уже в матче"}), 400
            
            cursor.execute("DELETE FROM search_queue WHERE player_id = %s", (player_id,))
            
            cursor.execute("""
                INSERT INTO search_queue 
                (player_id, mode, rank, rating_bucket, style, age, steam_link, faceit_link, comment, joined_at, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, (NOW() AT TIME ZONE 'UTC'), (NOW() AT TIME ZONE 'UTC' + INTERVAL '2 minutes'))
            """, (
                player_id, mode, str(rating_number), rating_bucket,
                data.get('style'), data.get('age'),
                data.get('steam_link'), data.get('faceit_link'), data.get('comment')
            ))
        
        return jsonify({"status": "searching", "message": "В очереди"})
    except Exception as e:
        logger.error(f"Ошибка start_search: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/match/check', methods=['POST'])
@timing_decorator
def check_match():
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    telegram_id = request.json['telegram_id']
    player_id = get_player_id_cached(telegram_id)
    
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT id, player1_id, player2_id, expires_at, status
                FROM matches
                WHERE (player1_id = %s OR player2_id = %s)
                AND status IN ('pending', 'accepted')
                AND expires_at > (NOW() AT TIME ZONE 'UTC')
                ORDER BY id DESC LIMIT 1
            """, (player_id, player_id))
            match = cursor.fetchone()
            
            if match:
                other_id = match['player2_id'] if match['player1_id'] == player_id else match['player1_id']
                profile = get_profile_cached(other_id)
                if profile:
                    return jsonify({
                        "match_found": True,
                        "match_id": match['id'],
                        "opponent": {
                            "player_id": other_id,
                            "nick": profile['nick'],
                            "age": profile['age'],
                            "style": "fan",
                            "rating": "0",
                            "steam_link": profile['steam_link'] or "Не указана",
                            "faceit_link": profile['faceit_link'] or "Не указана",
                            "avatar": profile['avatar'],
                            "comment": "Нет комментария"
                        },
                        "expires_at": match['expires_at'].isoformat() + "Z"
                    })
            
            cursor.execute("""
                SELECT * FROM search_queue 
                WHERE player_id = %s AND expires_at > (NOW() AT TIME ZONE 'UTC')
                FOR UPDATE SKIP LOCKED
            """, (player_id,))
            current = cursor.fetchone()
            
            if not current:
                return jsonify({"match_found": False})
            
            min_bucket, max_bucket = get_range_buckets(current['mode'], current['style'], current['rating_bucket'])
            
            query = """
                SELECT sq.*, p.nick, p.steam_link, p.faceit_link, p.avatar
                FROM search_queue sq
                JOIN profiles p ON sq.player_id = p.player_id
                WHERE sq.mode = %s AND sq.player_id != %s
                AND sq.expires_at > (NOW() AT TIME ZONE 'UTC')
            """
            params = [current['mode'], player_id]
            
            if min_bucket is not None:
                query += " AND sq.rating_bucket BETWEEN %s AND %s"
                params.extend([min_bucket, max_bucket])
            
            query += " FOR UPDATE SKIP LOCKED"
            cursor.execute(query, params)
            candidates = cursor.fetchall()
            
            if not candidates:
                return jsonify({"match_found": False})
            
            candidates.sort(key=lambda x: calculate_score(current, x, current['mode']))
            best = candidates[0]
            
            expires_at = datetime.utcnow() + timedelta(seconds=30)
            cursor.execute("""
                INSERT INTO matches (player1_id, player2_id, mode, created_at, expires_at, status)
                VALUES (%s, %s, %s, (NOW() AT TIME ZONE 'UTC'), %s, 'pending')
                RETURNING id
            """, (player_id, best['player_id'], current['mode'], expires_at))
            match_id = cursor.fetchone()['id']
            
            cursor.execute("DELETE FROM search_queue WHERE player_id IN (%s, %s)", (player_id, best['player_id']))
            
            return jsonify({
                "match_found": True,
                "match_id": match_id,
                "opponent": {
                    "player_id": best['player_id'],
                    "nick": best['nick'],
                    "age": best['age'],
                    "style": best['style'],
                    "rating": best['rank'],
                    "steam_link": best['steam_link'] or "Не указана",
                    "faceit_link": best['faceit_link'] or "Не указана",
                    "avatar": best['avatar'],
                    "comment": best['comment'] or "Нет комментария"
                },
                "expires_at": expires_at.isoformat() + "Z"
            })
    except Exception as e:
        logger.error(f"Ошибка check_match: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/match/status/<int:match_id>', methods=['GET'])
@timing_decorator
def match_status(match_id):
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT status, player1_response, player2_response, expires_at
                FROM matches WHERE id = %s
            """, (match_id,))
            match = cursor.fetchone()
            
            if not match:
                return jsonify({"status": "not_found"})
            
            if match['player1_response'] == 'accept' and match['player2_response'] == 'accept':
                return jsonify({"status": "both_accepted"})
            
            return jsonify({
                "status": match['status'],
                "expires_at": match['expires_at'].isoformat() + "Z" if match['expires_at'] else None
            })
    except Exception as e:
        logger.error(f"Ошибка match_status: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/match/respond', methods=['POST'])
@timing_decorator
def respond_match():
    if not request.json or 'telegram_id' not in request.json or 'match_id' not in request.json or 'response' not in request.json:
        return jsonify({"error": "Missing fields"}), 400
    
    data = request.json
    telegram_id = data['telegram_id']
    player_id = get_player_id_cached(telegram_id)
    
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT player1_id, player2_id, player1_response, player2_response, expires_at, status
                FROM matches WHERE id = %s FOR UPDATE
            """, (data['match_id'],))
            match = cursor.fetchone()
            
            if not match:
                return jsonify({"error": "Match not found"}), 404
            
            if datetime.utcnow() > match['expires_at']:
                cursor.execute("UPDATE matches SET status = 'expired' WHERE id = %s", (data['match_id'],))
                return jsonify({"status": "expired"})
            
            if str(match['player1_id']) == str(player_id):
                if match['player1_response'] is not None:
                    return jsonify({"status": "already_responded"})
                cursor.execute("UPDATE matches SET player1_response = %s WHERE id = %s", (data['response'], data['match_id']))
            elif str(match['player2_id']) == str(player_id):
                if match['player2_response'] is not None:
                    return jsonify({"status": "already_responded"})
                cursor.execute("UPDATE matches SET player2_response = %s WHERE id = %s", (data['response'], data['match_id']))
            else:
                return jsonify({"error": "User not in this match"}), 403
            
            cursor.execute("SELECT player1_response, player2_response FROM matches WHERE id = %s", (data['match_id'],))
            r1, r2 = cursor.fetchone()
            
            if r1 == 'accept' and r2 == 'accept':
                cursor.execute("UPDATE matches SET status = 'accepted' WHERE id = %s", (data['match_id'],))
                return jsonify({"status": "accepted", "both_accepted": True})
            elif r1 == 'reject' or r2 == 'reject':
                cursor.execute("DELETE FROM matches WHERE id = %s", (data['match_id'],))
                return jsonify({"status": "rejected", "both_accepted": False})
            else:
                time_left = max(0, int((match['expires_at'] - datetime.utcnow()).total_seconds()))
                return jsonify({"status": "waiting", "both_accepted": False, "time_left": time_left})
    except Exception as e:
        logger.error(f"Ошибка respond_match: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/search/stop', methods=['POST'])
@timing_decorator
def stop_search():
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    telegram_id = request.json['telegram_id']
    player_id = get_player_id_cached(telegram_id)
    
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("DELETE FROM search_queue WHERE player_id = %s", (player_id,))
            deleted = cursor.rowcount
        return jsonify({"status": "stopped", "deleted": deleted})
    except Exception as e:
        logger.error(f"Ошибка stop_search: {e}")
        return jsonify({"error": str(e)}), 500

# ---------- ДРУЗЬЯ ----------
@app.route('/api/friends/list', methods=['POST'])
@timing_decorator
def friends_list():
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    telegram_id = request.json['telegram_id']
    player_id = get_player_id_cached(telegram_id)
    
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    CASE WHEN player1_id = %s THEN player2_id ELSE player1_id END as friend_id
                FROM friends
                WHERE player1_id = %s OR player2_id = %s
            """, (player_id, player_id, player_id))
            friend_ids = [row[0] for row in cursor.fetchall()]
            
            friends = []
            for fid in friend_ids:
                profile = get_profile_cached(fid)
                if profile:
                    friends.append({
                        "player_id": fid,
                        "nick": profile['nick'],
                        "avatar": profile['avatar'],
                        "age": profile['age'],
                        "steam_link": profile['steam_link'],
                        "faceit_link": profile['faceit_link'],
                        "added_at": None
                    })
        
        return jsonify({"status": "ok", "friends": friends})
    except Exception as e:
        logger.error(f"Ошибка friends_list: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/friends/add', methods=['POST'])
@timing_decorator
def add_friend():
    if not request.json or 'telegram_id' not in request.json or 'friend_player_id' not in request.json:
        return jsonify({"error": "Missing fields"}), 400
    
    telegram_id = request.json['telegram_id']
    friend_player_id = request.json['friend_player_id']
    player_id = get_player_id_cached(telegram_id)
    
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO friends (player1_id, player2_id, created_at)
                VALUES (%s, %s, (NOW() AT TIME ZONE 'UTC'))
                ON CONFLICT DO NOTHING
            """, (player_id, friend_player_id))
        return jsonify({"status": "ok", "message": "Friend added"})
    except Exception as e:
        logger.error(f"Ошибка add_friend: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/friends/remove', methods=['POST'])
@timing_decorator
def remove_friend():
    if not request.json or 'telegram_id' not in request.json or 'friend_player_id' not in request.json:
        return jsonify({"error": "Missing fields"}), 400
    
    telegram_id = request.json['telegram_id']
    friend_player_id = request.json['friend_player_id']
    player_id = get_player_id_cached(telegram_id)
    
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                DELETE FROM friends 
                WHERE (player1_id = %s AND player2_id = %s) OR (player1_id = %s AND player2_id = %s)
            """, (player_id, friend_player_id, friend_player_id, player_id))
        return jsonify({"status": "ok", "message": "Friend removed"})
    except Exception as e:
        logger.error(f"Ошибка remove_friend: {e}")
        return jsonify({"error": str(e)}), 500

# ---------- ИСТОРИЯ МАТЧЕЙ ----------
@app.route('/api/my-matches', methods=['POST'])
@timing_decorator
def my_matches():
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    telegram_id = request.json['telegram_id']
    player_id = get_player_id_cached(telegram_id)
    
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT m.id, p1.nick as player1, p2.nick as player2, 
                       g.telegram_chat_link, g.status,
                       EXTRACT(EPOCH FROM (g.expires_at - (NOW() AT TIME ZONE 'UTC'))) as time_left
                FROM matches m
                JOIN games g ON m.id = g.match_id
                JOIN profiles p1 ON m.player1_id = p1.player_id
                JOIN profiles p2 ON m.player2_id = p2.player_id
                WHERE (m.player1_id = %s OR m.player2_id = %s)
                AND g.status = 'active'
                AND g.expires_at > (NOW() AT TIME ZONE 'UTC')
                ORDER BY m.id DESC
            """, (player_id, player_id))
            active = cursor.fetchall()
            
            cursor.execute("""
                SELECT m.id, p1.nick as player1, p2.nick as player2, 
                       g.telegram_chat_link, g.created_at
                FROM matches m
                JOIN games g ON m.id = g.match_id
                JOIN profiles p1 ON m.player1_id = p1.player_id
                JOIN profiles p2 ON m.player2_id = p2.player_id
                WHERE (m.player1_id = %s OR m.player2_id = %s)
                AND g.status = 'closed'
                ORDER BY m.id DESC
                LIMIT 50
            """, (player_id, player_id))
            history = cursor.fetchall()
        
        return jsonify({
            "status": "ok",
            "active": [dict(match) for match in active],
            "history": [dict(match) for match in history]
        })
    except Exception as e:
        logger.error(f"Ошибка my_matches: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/active-match', methods=['POST'])
@timing_decorator
def active_match():
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    player_id = get_player_id_cached(request.json['telegram_id'])
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT g.telegram_chat_link, m.id
                FROM games g
                JOIN matches m ON g.match_id = m.id
                WHERE (m.player1_id = %s OR m.player2_id = %s)
                AND g.status = 'active'
                AND g.expires_at > (NOW() AT TIME ZONE 'UTC')
                LIMIT 1
            """, (player_id, player_id))
            game = cursor.fetchone()
            
            if game:
                return jsonify({"active": True, "match_id": game[1], "chat_link": game[0]})
            return jsonify({"active": False})
    except Exception as e:
        logger.error(f"Ошибка active_match: {e}")
        return jsonify({"error": str(e)}), 500

# ---------- ИГРЫ ----------
@app.route('/api/game/create', methods=['POST'])
@timing_decorator
def create_game():
    if not request.json or 'match_id' not in request.json:
        return jsonify({"error": "Missing match_id"}), 400
    
    match_id = request.json['match_id']
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT id, telegram_chat_link FROM games WHERE match_id = %s", (match_id,))
            existing = cursor.fetchone()
            if existing:
                return jsonify({"status": "ok", "game_id": existing[0], "chat_link": existing[1]})
            
            for i in range(5):
                cursor.execute("""
                    SELECT player1_id, player2_id, mode FROM matches
                    WHERE id = %s AND status = 'accepted'
                """, (match_id,))
                match = cursor.fetchone()
                if match:
                    break
                time.sleep(0.3)
            
            if not match:
                return jsonify({"error": "Match not accepted"}), 400
            
            player1_id, player2_id, mode = match
            
            cursor.execute("SELECT u.telegram_id, p.nick FROM users u JOIN profiles p ON u.player_id = p.player_id WHERE u.player_id = %s", (player1_id,))
            user1 = cursor.fetchone()
            cursor.execute("SELECT u.telegram_id, p.nick FROM users u JOIN profiles p ON u.player_id = p.player_id WHERE u.player_id = %s", (player2_id,))
            user2 = cursor.fetchone()
            
            if not user1 or not user2:
                return jsonify({"error": "Users not found"}), 404
            
            telegram_id1, nick1 = user1
            telegram_id2, nick2 = user2
            
            create_topic_url = f"https://api.telegram.org/bot{BOT_TOKEN}/createForumTopic"
            topic_response = requests.post(create_topic_url, json={
                "chat_id": FORUM_GROUP_ID,
                "name": f"#{match_id} | {nick1} & {nick2}",
                "icon_color": 0x6FB9F0
            }, timeout=5)
            
            if not topic_response.ok:
                return jsonify({"error": "Failed to create topic"}), 500
            
            topic_data = topic_response.json()
            topic_id = topic_data['result']['message_thread_id']
            
            clean_chat_id = str(FORUM_GROUP_ID).replace('-100', '')
            public_link = f"https://t.me/c/{clean_chat_id}/{topic_id}"
            expires_at = datetime.utcnow() + timedelta(minutes=30)
            
            cursor.execute("""
                INSERT INTO games (match_id, player1_id, player2_id, telegram_chat_id, telegram_chat_link, status, created_at, expires_at)
                VALUES (%s, %s, %s, %s, %s, 'active', (NOW() AT TIME ZONE 'UTC'), %s)
                RETURNING id
            """, (match_id, player1_id, player2_id, topic_id, public_link, expires_at))
            game_id = cursor.fetchone()[0]
            
            try:
                requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json={
                        "chat_id": int(telegram_id1),
                        "text": f"✅ **Матч #{match_id} создан!**\nСоперник: {nick2}\n\n🔗 [Перейти в чат]({public_link})",
                        "parse_mode": "Markdown"
                    },
                    timeout=3
                )
                requests.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json={
                        "chat_id": int(telegram_id2),
                        "text": f"✅ **Матч #{match_id} создан!**\nСоперник: {nick1}\n\n🔗 [Перейти в чат]({public_link})",
                        "parse_mode": "Markdown"
                    },
                    timeout=3
                )
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления: {e}")
            
            return jsonify({"status": "ok", "game_id": game_id, "chat_link": public_link})
    except Exception as e:
        logger.error(f"Ошибка create_game: {e}")
        return jsonify({"error": str(e)}), 500

# ---------- ПОИСК ИГРОКОВ ----------
@app.route('/api/users/all', methods=['POST'])
@timing_decorator
def get_all_users():
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    player_id = get_player_id_cached(request.json['telegram_id'])
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT player_id, nick, avatar, age, steam_link, faceit_link
                FROM profiles
                WHERE player_id != %s
                ORDER BY created_at DESC
                LIMIT 100
            """, (player_id,))
            users = [dict(row) for row in cursor.fetchall()]
        return jsonify({"status": "ok", "users": users})
    except Exception as e:
        logger.error(f"Ошибка get_all_users: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/users/search', methods=['POST'])
@timing_decorator
def search_users():
    if not request.json or 'telegram_id' not in request.json or 'query' not in request.json:
        return jsonify({"error": "Missing fields"}), 400
    
    telegram_id = request.json['telegram_id']
    query = request.json['query'].strip()
    
    player_id = get_player_id_cached(telegram_id)
    if not player_id or not query:
        return jsonify({"error": "Invalid request"}), 400
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT player_id, nick, avatar, age, steam_link, faceit_link
                FROM profiles
                WHERE player_id != %s AND (nick ILIKE %s OR player_id::text ILIKE %s)
                ORDER BY CASE WHEN nick ILIKE %s THEN 1 ELSE 2 END
                LIMIT 50
            """, (player_id, f'%{query}%', f'%{query}%', f'{query}%'))
            users = [dict(row) for row in cursor.fetchall()]
        return jsonify({"status": "ok", "users": users, "query": query})
    except Exception as e:
        logger.error(f"Ошибка search_users: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/user/profile/<player_id>', methods=['POST'])
@timing_decorator
def get_user_profile(player_id):
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    profile = get_profile_cached(player_id)
    if not profile:
        return jsonify({"error": "Profile not found"}), 404
    
    return jsonify({"status": "ok", "player_id": player_id, **profile})

# ---------- ВЕРИФИКАЦИЯ ТОКЕНА ----------
@app.route('/api/verify-token', methods=['POST'])
@timing_decorator
def verify_token():
    if not request.json or 'token' not in request.json or 'user_id' not in request.json:
        return jsonify({"error": "Missing fields"}), 400
    
    token = request.json['token']
    user_id = str(request.json['user_id'])
    
    match_id, token_user_id = verify_match_token(token)
    
    if match_id and token_user_id == user_id:
        return jsonify({"valid": True, "match_id": match_id})
    return jsonify({"valid": False})

# ============================================
# ЗАПУСК
# ============================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    
    print("🔥 PINGSTER BACKEND - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ")
    print(f"🚀 Запуск на порту {port}")
    
    if not init_db_pool():
        print("❌ Не удалось подключиться к БД")
        sys.exit(1)
    
    bg_thread = threading.Thread(target=background_worker, daemon=True)
    bg_thread.start()
    
    clean_thread = threading.Thread(target=queue_cleaner_worker, daemon=True)
    clean_thread.start()
    
    print("✅ Готов к работе (300+ пользователей)")
    print("✅ Пул соединений: 5-30")
    print("✅ Кэш: player_id, profile, forum")
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
