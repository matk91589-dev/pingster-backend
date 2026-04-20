import os
import sys
import time
import random
import hashlib
import hmac
import base64
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
from flask import Flask, request, jsonify, g
from flask_cors import CORS
from dotenv import load_dotenv

# В самом начале, после load_dotenv(), добавь:
os.environ['PGSSLMODE'] = 'require'
os.environ['PGCONNECT_TIMEOUT'] = '30'

# Загрузка переменных окружения
load_dotenv()

# ============================================
# КОНФИГУРАЦИЯ
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)

# Конфигурация из переменных окружения (обязательные)
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")
BOT_TOKEN = os.getenv("BOT_TOKEN")
FORUM_GROUP_ID = os.getenv("FORUM_GROUP_ID")
SECRET_KEY = os.getenv("SECRET_KEY", "pingster_super_secret_key_2026_change_this")

# Проверка обязательных переменных
required_env_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD", "BOT_TOKEN", "FORUM_GROUP_ID"]
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Конвертация FORUM_GROUP_ID в int
try:
    FORUM_GROUP_ID = int(FORUM_GROUP_ID)
except ValueError:
    raise ValueError("FORUM_GROUP_ID must be an integer")

# Rate limiting конфигурация
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

class ConflictError(AppError):
    def __init__(self, message: str):
        super().__init__(message, 409, "CONFLICT")

# ============================================
# RATE LIMITING
# ============================================
class RateLimiter:
    def __init__(self):
        self.requests = defaultdict(list)
    
    def is_allowed(self, key: str, limit: int, window: int) -> Tuple[bool, int]:
        now = time.time()
        window_start = now - window
        
        # Очищаем старые запросы
        self.requests[key] = [req_time for req_time in self.requests[key] if req_time > window_start]
        
        if len(self.requests[key]) >= limit:
            # Возвращаем время до следующего доступного слота
            oldest = min(self.requests[key])
            wait_time = int(window - (now - oldest))
            return False, wait_time
        
        self.requests[key].append(now)
        return True, 0

rate_limiter = RateLimiter()

def rate_limit(limit: int = None, window: int = None):
    """Декоратор для ограничения частоты запросов"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if limit is None or window is None:
                _limit, _window = RATE_LIMIT_REQUESTS, RATE_LIMIT_WINDOW
            else:
                _limit, _window = limit, window
            
            # Используем IP + эндпоинт как ключ
            key = f"{request.remote_addr}:{request.endpoint}"
            allowed, wait_time = rate_limiter.is_allowed(key, _limit, _window)
            
            if not allowed:
                return jsonify({
                    "error": "Rate limit exceeded",
                    "retry_after": wait_time,
                    "error_code": "RATE_LIMIT_EXCEEDED"
                }), 429
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

# ============================================
# ПУЛ СОЕДИНЕНИЙ
# ============================================
from psycopg2 import pool

db_pool = None

def init_db_pool():
    global db_pool
    try:
        db_pool = pool.SimpleConnectionPool(
            minconn=2,
            maxconn=15,
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            connect_timeout=30,
            keepalives=1,
            keepalives_idle=5,
            keepalives_interval=2,
            keepalives_count=2
        )
        logger.info(f"✅ Пул соединений создан (min=2, max=15) с keepalive")
        
        # Проверяем пул при старте
        try:
            test_conn = db_pool.getconn()
            db_pool.putconn(test_conn)
            logger.info("✅ Проверка пула успешна")
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке пула: {e}")
            raise
        
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка создания пула: {e}")
        return False

def get_db_connection():
    if not db_pool:
        raise AppError("Database pool not initialized", 500, "DB_POOL_ERROR")
    try:
        conn = db_pool.getconn()
        # Проверяем что соединение живое
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return conn
    except Exception as e:
        logger.error(f"Ошибка получения соединения: {e}")
        raise

def put_db_connection(conn):
    if db_pool:
        db_pool.putconn(conn)

@contextmanager
def get_db_cursor():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        # Проверяем что соединение живое
        with conn.cursor() as test_cursor:
            test_cursor.execute("SELECT 1")
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        yield cursor
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error: {e}")
        # Если SSL ошибка - пробуем переподключиться
        if "SSL connection has been closed" in str(e) or "connection already closed" in str(e):
            logger.info("🔄 Переподключение к БД...")
            if db_pool:
                db_pool.closeall()
                time.sleep(1)
                init_db_pool()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            try:
                db_pool.putconn(conn)
                logger.debug("Соединение возвращено в пул")
            except Exception as e:
                logger.error(f"Ошибка при возврате соединения: {e}")

# ============================================
# REDIS КЭШ (опционально)
# ============================================
class RedisCache:
    """Кэш с поддержкой Redis (если доступен) или fallback на память"""
    def __init__(self):
        self.cache = {}
        self.use_redis = False
        try:
            import redis
            redis_url = os.getenv("REDIS_URL")
            if redis_url:
                self.redis_client = redis.from_url(redis_url)
                self.redis_client.ping()
                self.use_redis = True
                logger.info("✅ Redis подключен")
            else:
                logger.info("⚠️ Redis не настроен, использую in-memory кэш")
        except ImportError:
            logger.info("⚠️ redis-py не установлен, использую in-memory кэш")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка подключения к Redis: {e}, использую in-memory кэш")
    
    def get(self, key: str):
        if self.use_redis:
            value = self.redis_client.get(key)
            return json.loads(value) if value else None
        
        # In-memory fallback
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < 300:  # 5 минут TTL
                return value
            del self.cache[key]
        return None
    
    def set(self, key: str, value: Any, ttl: int = 300):
        if self.use_redis:
            self.redis_client.setex(key, ttl, json.dumps(value))
        else:
            self.cache[key] = (value, time.time())
    
    def delete(self, key: str):
        if self.use_redis:
            self.redis_client.delete(key)
        else:
            self.cache.pop(key, None)
    
    def clear_pattern(self, pattern: str):
        """Очистка по паттерну (только для Redis)"""
        if self.use_redis:
            for key in self.redis_client.scan_iter(f"*{pattern}*"):
                self.redis_client.delete(key)

cache = RedisCache()

# ============================================
# ВАЛИДАЦИЯ ДАННЫХ
# ============================================
def validate_nick(nick: str) -> Tuple[bool, Optional[str]]:
    if not nick or len(nick) < 2 or len(nick) > 32:
        return False, "Nick must be 2-32 characters"
    if not re.match(r'^[a-zA-Z0-9_\u0410-\u044F]+$', nick):
        return False, "Nick can only contain letters, numbers, and underscore"
    return True, None

def validate_age(age: Any) -> Tuple[bool, Optional[str], Optional[int]]:
    try:
        age_int = int(age)
        if age_int < 13 or age_int > 120:
            return False, "Age must be 13-120", None
        return True, None, age_int
    except (ValueError, TypeError):
        return False, "Invalid age format", None

def validate_steam_link(link: str) -> Tuple[bool, Optional[str]]:
    if not link:
        return True, None
    if not link.startswith(('https://steamcommunity.com/', 'https://s.team/')):
        return False, "Invalid Steam profile link"
    return True, None

def validate_faceit_link(link: str) -> Tuple[bool, Optional[str]]:
    if not link:
        return True, None
    if not link.startswith('https://www.faceit.com/'):
        return False, "Invalid FaceIT profile link"
    return True, None

def validate_avatar_url(url: str) -> Tuple[bool, Optional[str]]:
    if not url:
        return True, None
    
    # ПРОВЕРЯЕМ: ЭТО BASE64 ИЛИ URL?
    if url.startswith('data:image'):
        # Это base64 — всегда валидно
        return True, None
    
    # Это URL — проверяем формат
    pattern = r'^https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+\.(?:jpg|jpeg|png|gif|webp)(?:\?.*)?$'
    if not re.match(pattern, url):
        return False, "Invalid avatar URL format"
    return True, None

# ---------- ОТЛАДКА: РУЧНОЕ ЗАКРЫТИЕ ТЕМ ----------
@app.route('/api/debug/close-expired', methods=['GET'])
def debug_close_expired():
    """Ручное закрытие просроченных тем (для отладки)"""
    try:
        closed = 0
        errors = []
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT telegram_chat_id FROM games 
                WHERE expires_at < (NOW() AT TIME ZONE 'UTC') 
                AND status = 'active'
            """)
            topics = cursor.fetchall()
            
            logger.info(f"🔧 [DEBUG] Найдено {len(topics)} тем для закрытия")
            
            for topic in topics:
                topic_id = topic[0]
                try:
                    logger.info(f"🔧 [DEBUG] Закрываем тему {topic_id}")
                    close_topic(topic_id)
                    closed += 1
                except Exception as e:
                    errors.append({"topic_id": topic_id, "error": str(e)})
                    
        return jsonify({
            "status": "ok", 
            "closed": closed, 
            "total_found": len(topics),
            "errors": errors
        })
    except Exception as e:
        logger.error(f"❌ [DEBUG] Ошибка: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500

# ============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================
def update_user_activity(telegram_id: str):
    """Обновляет last_active и is_online для пользователя"""
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                UPDATE users 
                SET last_active = (NOW() AT TIME ZONE 'UTC'),
                    is_online = TRUE
                WHERE telegram_id = %s
            """, (telegram_id,))
    except Exception as e:
        logger.error(f"Ошибка update_user_activity: {e}")

def set_user_offline(telegram_id: str):
    """Устанавливает is_online = FALSE"""
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                UPDATE users 
                SET is_online = FALSE
                WHERE telegram_id = %s
            """, (telegram_id,))
    except Exception as e:
        logger.error(f"Ошибка set_user_offline: {e}")

def get_player_id(telegram_id: str) -> Optional[str]:
    cached = cache.get(f"player_id:{telegram_id}")
    if cached:
        return cached
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT player_id FROM users WHERE telegram_id = %s", (telegram_id,))
            result = cursor.fetchone()
            if result:
                player_id = result[0]
                cache.set(f"player_id:{telegram_id}", player_id)
                return player_id
            return None
    except Exception as e:
        logger.error(f"Ошибка get_player_id: {e}")
        return None

def get_profile_cached(player_id: str) -> Optional[Dict]:
    cached = cache.get(f"profile:{player_id}")
    if cached:
        return cached
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT nick, age, steam_link, faceit_link, avatar, created_at
                FROM profiles WHERE player_id = %s
            """, (player_id,))
            result = cursor.fetchone()
            if result:
                profile = dict(result)
                if profile.get('created_at'):
                    profile['created_at'] = profile['created_at'].isoformat()
                cache.set(f"profile:{player_id}", profile)
                return profile
            return None
    except Exception as e:
        logger.error(f"Ошибка get_profile: {e}")
        return None

def invalidate_cache(telegram_id: str = None, player_id: str = None):
    if telegram_id:
        cache.delete(f"player_id:{telegram_id}")
    if player_id:
        cache.delete(f"profile:{player_id}")

def generate_player_id() -> str:
    return str(random.randint(10000000, 99999999))

def generate_random_nick() -> str:
    chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    return ''.join(random.choice(chars) for _ in range(8))

# Ранги CS2
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

def get_rank_index(rank: str) -> int:
    return RANK_LIST.index(rank) if rank in RANK_LIST else 0

def calculate_score(player: Dict, candidate: Dict, mode: str) -> int:
    """Расчет совместимости игроков (меньше = лучше)"""
    if mode in ['faceit', 'premier']:
        player_rating = int(player.get('rank', 0))
        cand_rating = int(candidate.get('rank', 0))
        rating_diff = abs(player_rating - cand_rating)
    else:
        rating_diff = abs(
            get_rank_index(player.get('rank', '')) - 
            get_rank_index(candidate.get('rank', ''))
        ) * 100
    
    age_diff = abs((player.get('age') or 0) - (candidate.get('age') or 0))
    
    return rating_diff + (age_diff * 50)

def get_range_buckets(mode: str, style: str, bucket: int) -> Tuple[Optional[int], Optional[int]]:
    """Получение диапазона поиска по бакетам"""
    if mode == 'faceit':
        # Faceit: ±4 бакета = ±400 ELO
        return bucket - 4, bucket + 4
    elif mode == 'premier':
        # Premier: ±1 бакет = ±5000 рейтинга
        return bucket - 1, bucket + 1
    elif mode == 'prime':
        # Prime: ±8 бакетов = ±800 рейтинга
        return bucket - 8, bucket + 8
    elif mode == 'public':
        # Public: без фильтра
        return None, None
    return None, None

def generate_match_token(match_id: int, user_id: str, expires_at: int) -> str:
    """Генерация защищенного токена для матча"""
    data = f"{match_id}:{user_id}:{expires_at}"
    signature = hmac.new(
        SECRET_KEY.encode(), 
        data.encode(), 
        hashlib.sha256
    ).hexdigest()[:16]
    token = f"{match_id}:{user_id}:{expires_at}:{signature}"
    return base64.urlsafe_b64encode(token.encode()).decode().rstrip('=')

def verify_match_token(token: str) -> Tuple[Optional[int], Optional[str]]:
    """Проверка токена матча"""
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
        expected_signature = hmac.new(
            SECRET_KEY.encode(),
            expected_data.encode(),
            hashlib.sha256
        ).hexdigest()[:16]
        
        if hmac.compare_digest(signature, expected_signature):
            return int(match_id), user_id
        
        return None, None
    except Exception as e:
        logger.error(f"Ошибка проверки токена: {e}")
        return None, None

def check_user_in_forum(user_id: str) -> bool:
    """Проверка, состоит ли пользователь в Telegram форуме"""
    try:
        user_id_int = int(user_id)
        
        response = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember",
            params={"chat_id": FORUM_GROUP_ID, "user_id": user_id_int},
            timeout=5
        )
        data = response.json()
        
        if not data.get('ok'):
            logger.error(f"Telegram API error: {data}")
            return False
        
        status = data.get('result', {}).get('status')
        is_member = status in ['member', 'administrator', 'creator']
        
        logger.info(f"Проверка форума: user_id={user_id_int}, status={status}, result={is_member}")
        return is_member
        
    except ValueError:
        logger.error(f"Некорректный user_id: {user_id}")
        return False
    except Exception as e:
        logger.error(f"Ошибка проверки форума: {e}")
        return False

# ============================================
# РЕПУТАЦИЯ - ОТПРАВКА УВЕДОМЛЕНИЙ
# ============================================
def send_match_notification(telegram_id, match_id, teammate_nick, chat_link):
    """Отправляет личное сообщение игроку с кнопками оценки"""
    try:
        keyboard = {
            "inline_keyboard": [
                [{"text": "👉 Перейти в чат", "url": chat_link}],
                [
                    {"text": "👍", "callback_data": f"vote_up_{telegram_id}_{match_id}"},
                    {"text": "👎", "callback_data": f"vote_down_{telegram_id}_{match_id}"}
                ]
            ]
        }
        
        message = f"🎮 У вас создан мэтч #{match_id} с игроком {teammate_nick}\n\nОцените тиммейта:"
        
        response = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": telegram_id,
                "text": message,
                "reply_markup": keyboard
            },
            timeout=5
        )
        
        if response.ok:
            logger.info(f"✅ Уведомление отправлено пользователю {telegram_id}")
        else:
            logger.error(f"❌ Ошибка отправки уведомления: {response.text}")
            
    except Exception as e:
        logger.error(f"❌ Ошибка send_match_notification: {e}")

def update_reputation(telegram_id, delta):
    """Обновляет репутацию (rating) и leadercoins пользователя"""
    try:
        # За положительную оценку +75 leadercoins, за отрицательную -50
        coins_delta = 75 if delta > 0 else -50
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                UPDATE users 
                SET rating = COALESCE(rating, 0) + %s,
                    leadercoins = COALESCE(leadercoins, 0) + %s
                WHERE telegram_id = %s
                RETURNING rating, leadercoins
            """, (delta, coins_delta, telegram_id))
            result = cursor.fetchone()
            if result:
                logger.info(f"✅ Рейтинг: {result[0]}, Leadercoins: {result[1]} для {telegram_id}")
            else:
                logger.warning(f"⚠️ Пользователь {telegram_id} не найден")
    except Exception as e:
        logger.error(f"❌ Ошибка update_reputation: {e}")

# ============================================
# ОБРАБОТЧИКИ ОШИБОК
# ============================================
@app.errorhandler(AppError)
def handle_app_error(error: AppError):
    response = {
        "error": str(error),
        "error_code": error.error_code
    }
    if hasattr(error, 'details') and error.details:
        response["details"] = error.details
    return jsonify(response), error.status_code

@app.errorhandler(404)
def handle_404(error):
    return jsonify({"error": "Endpoint not found", "error_code": "NOT_FOUND"}), 404

@app.errorhandler(500)
def handle_500(error):
    logger.error(f"Internal server error: {error}")
    return jsonify({"error": "Internal server error", "error_code": "INTERNAL_ERROR"}), 500

# ============================================
# ФОНОВЫЕ ПРОЦЕССЫ
# ============================================
def close_topic(topic_id: int):
    """Закрытие темы в Telegram форуме"""
    try:
        logger.info(f"🔒 [close_topic] Закрываем тему {topic_id} в форуме {FORUM_GROUP_ID}")
        
        response = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/closeForumTopic",
            json={"chat_id": FORUM_GROUP_ID, "message_thread_id": topic_id},
            timeout=5
        )
        
        if response.ok:
            logger.info(f"✅ [close_topic] Тема {topic_id} закрыта")
            with get_db_cursor() as cursor:
                cursor.execute(
                    "UPDATE games SET status = 'closed' WHERE telegram_chat_id = %s",
                    (str(topic_id),)
                )
        else:
            logger.error(f"❌ [close_topic] Ошибка API: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"❌ [close_topic] Исключение: {e}")

def background_worker():
    logger.info("🚀 Фоновый поток запущен")
    
    while True:
        try:
            logger.info("⏰ [ФОН] Проверка просроченных тем...")
            with get_db_cursor() as cursor:
                cursor.execute("""
                    SELECT telegram_chat_id FROM games 
                    WHERE expires_at < (NOW() AT TIME ZONE 'UTC') 
                    AND status = 'active'
                """)
                topics = cursor.fetchall()
                logger.info(f"📋 [ФОН] Найдено тем: {len(topics)}")
                
                for topic in topics:
                    try:
                        close_topic(topic[0])
                        time.sleep(0.3)
                    except Exception as e:
                        logger.error(f"❌ [ФОН] Ошибка закрытия темы {topic[0]}: {e}")
                        
        except Exception as e:
            logger.error(f"❌ [ФОН] КРИТИЧЕСКАЯ ОШИБКА: {e}")
            # Записываем в файл на всякий случай
            with open("background_worker_errors.log", "a") as f:
                f.write(f"{datetime.now()} - {e}\n")
                
        time.sleep(60)
def queue_cleaner_worker():
    """Очистка устаревших записей в очереди поиска"""
    logger.info("🧹 Очистка очереди запущена")
    while True:
        try:
            with get_db_cursor() as cursor:
                cursor.execute(
                    "DELETE FROM search_queue WHERE expires_at < (NOW() AT TIME ZONE 'UTC')"
                )
                if cursor.rowcount:
                    logger.info(f"🧹 Очищено {cursor.rowcount} записей из очереди")
        except Exception as e:
            logger.error(f"Ошибка очистки очереди: {e}")
        time.sleep(5)

def online_cleaner_worker():
    """Сбрасывает is_online для неактивных пользователей"""
    logger.info("🟢 Очистка онлайн-статуса запущена")
    while True:
        try:
            with get_db_cursor() as cursor:
                cursor.execute("""
                    UPDATE users 
                    SET is_online = FALSE 
                    WHERE last_active < (NOW() AT TIME ZONE 'UTC' - INTERVAL '5 minutes')
                """)
                if cursor.rowcount:
                    logger.info(f"🟢 Сброшен онлайн для {cursor.rowcount} пользователей")
        except Exception as e:
            logger.error(f"Ошибка online_cleaner: {e}")
        time.sleep(60)

# ============================================
# HEALTH CHECK
# ============================================
@app.route('/health', methods=['GET'])
def health_check():
    """Health check эндпоинт для мониторинга"""
    status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "2.0.0",
        "services": {}
    }
    
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT 1")
            status["services"]["database"] = "connected"
    except Exception as e:
        status["status"] = "unhealthy"
        status["services"]["database"] = f"error: {str(e)}"
    
    if db_pool:
        status["services"]["db_pool"] = {
            "min": db_pool.minconn,
            "max": db_pool.maxconn,
            "closed": db_pool.closed
        }
    
    status["services"]["cache"] = "redis" if cache.use_redis else "memory"
    
    try:
        response = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getMe", timeout=3)
        status["services"]["telegram"] = "connected" if response.ok else "error"
    except:
        status["services"]["telegram"] = "unavailable"
    
    return jsonify(status), 200 if status["status"] == "healthy" else 503

# ============================================
# ЭНДПОИНТЫ
# ============================================
@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "name": "Pingster Backend",
        "version": "2.0.0",
        "status": "running",
        "endpoints": [
            "/health", "/api", "/api/user/init",
            "/api/profile/get", "/api/profile/update",
            "/api/search/start", "/api/match/check",
            "/api/reputation/vote"
        ]
    })

@app.route('/api', methods=['GET'])
def api_root():
    return jsonify({
        "message": "Pingster API is running!",
        "version": "2.0.0",
        "status": "ok"
    })

# ---------- СТАТУС ПОЛЬЗОВАТЕЛЯ ----------
@app.route('/api/user/status', methods=['POST'])
@rate_limit(limit=30, window=60)
def get_user_status():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id", ["telegram_id is required"])
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT is_online, 
                       EXTRACT(EPOCH FROM (NOW() AT TIME ZONE 'UTC' - last_active)) as seconds_ago
                FROM users 
                WHERE telegram_id = %s
            """, (data['telegram_id'],))
            result = cursor.fetchone()
            
            if not result:
                raise NotFoundError("User not found")
            
            is_online = result['is_online'] and (result['seconds_ago'] or 0) < 300
            return jsonify({
                "is_online": is_online,
                "last_active_seconds_ago": int(result['seconds_ago'] or 0),
                "status": "ok"
            })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка get_user_status: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

# ---------- ЕЖЕДНЕВНЫЙ БОНУС ----------
@app.route('/api/user/daily-bonus', methods=['POST'])
@rate_limit(limit=5, window=60)
def daily_bonus():
    try:
        data = request.json
        telegram_id = data.get('telegram_id')
        if not telegram_id:
            raise ValidationError("Missing telegram_id")
        
        with get_db_cursor() as cursor:
            cursor.execute("SELECT last_daily_bonus FROM users WHERE telegram_id = %s", (telegram_id,))
            result = cursor.fetchone()
            
            if not result:
                raise NotFoundError("User not found")
            
            last_bonus = result[0]
            now = datetime.utcnow()
            
            if not last_bonus or (now - last_bonus).total_seconds() > 12 * 3600:
                cursor.execute("""
                    UPDATE users 
                    SET leadercoins = COALESCE(leadercoins, 0) + 50,
                        last_daily_bonus = (NOW() AT TIME ZONE 'UTC')
                    WHERE telegram_id = %s
                    RETURNING leadercoins
                """, (telegram_id,))
                new_coins = cursor.fetchone()[0]
                
                return jsonify({"status": "ok", "bonus": True, "leadercoins": new_coins})
            else:
                time_left = 12 * 3600 - int((now - last_bonus).total_seconds())
                return jsonify({"status": "ok", "bonus": False, "time_left": time_left})
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка daily_bonus: {e}")
        raise AppError(str(e), 500)

# ---------- РЕЙТИНГ ПОЛЬЗОВАТЕЛЯ ----------
@app.route('/api/user/rating', methods=['POST'])
@rate_limit(limit=30, window=60)
def get_user_rating():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id")
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT COALESCE(rating, 0) as rating
                FROM users WHERE telegram_id = %s
            """, (data['telegram_id'],))
            result = cursor.fetchone()
            
            if result:
                return jsonify({"status": "ok", "rating": result['rating']})
            else:
                return jsonify({"status": "ok", "rating": 0})
    except Exception as e:
        logger.error(f"Ошибка get_user_rating: {e}")
        return jsonify({"status": "error"}), 500

# ---------- ПРОФИЛЬ ----------
@app.route('/api/profile/get', methods=['POST'])
@rate_limit()
def get_profile():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id", ["telegram_id is required"])
        
        update_user_activity(data['telegram_id'])
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            raise NotFoundError("User not found")
        
        profile = get_profile_cached(player_id)
        if not profile:
            raise NotFoundError("Profile not found")
        
        return jsonify({
            "status": "ok",
            "nick": profile['nick'],
            "age": profile['age'],
            "steam_link": profile['steam_link'],
            "faceit_link": profile['faceit_link'],
            "created_at": profile['created_at']
        })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка get_profile: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

@app.route('/api/profile/update', methods=['POST'])
@rate_limit(limit=20, window=60)
def update_profile():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id", ["telegram_id is required"])
        
        update_user_activity(data['telegram_id'])
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            raise NotFoundError("User not found")
        
        validation_errors = []
        update_fields = []
        update_values = []
        
        if 'nick' in data and data['nick'] is not None:
            is_valid, error = validate_nick(data['nick'])
            if not is_valid:
                validation_errors.append(error)
            else:
                update_fields.append("nick = %s")
                update_values.append(data['nick'])
        
        if 'age' in data and data['age'] is not None:
            is_valid, error, age_int = validate_age(data['age'])
            if not is_valid:
                validation_errors.append(error)
            else:
                update_fields.append("age = %s")
                update_values.append(age_int)
        
        if 'steam_link' in data:
            is_valid, error = validate_steam_link(data['steam_link'])
            if not is_valid:
                validation_errors.append(error)
            else:
                update_fields.append("steam_link = %s")
                update_values.append(data['steam_link'])
        
        if 'faceit_link' in data:
            is_valid, error = validate_faceit_link(data['faceit_link'])
            if not is_valid:
                validation_errors.append(error)
            else:
                update_fields.append("faceit_link = %s")
                update_values.append(data['faceit_link'])
        
        if validation_errors:
            raise ValidationError("Validation failed", validation_errors)
        
        if not update_fields:
            raise ValidationError("No fields to update", ["At least one field is required"])
        
        update_values.append(player_id)
        
        with get_db_cursor() as cursor:
            cursor.execute(f"""
                UPDATE profiles SET {', '.join(update_fields)}
                WHERE player_id = %s
                RETURNING nick, age, steam_link, faceit_link, avatar
            """, update_values)
            updated = cursor.fetchone()
            
            # 🔥 ПРОВЕРЯЕМ ЗАПОЛНЕНИЕ ПРОФИЛЯ (БОНУС +200 leadercoins)
            if updated:
                nick_ok = updated[0] is not None and updated[0] != ''
                age_ok = updated[1] is not None and updated[1] != ''
                steam_ok = updated[2] is not None and updated[2] != ''
                faceit_ok = updated[3] is not None and updated[3] != ''
                
                # Профиль считается заполненным, если есть ник, возраст и хотя бы одна ссылка
                profile_filled = nick_ok and age_ok and (steam_ok or faceit_ok)
                
                if profile_filled:
                    # Проверяем, получал ли уже бонус
                    cursor.execute("""
                        SELECT profile_completed FROM users WHERE player_id = %s
                    """, (player_id,))
                    result = cursor.fetchone()
                    
                    if not result or not result[0]:  # Ещё не получал бонус
                        cursor.execute("""
                            UPDATE users 
                            SET leadercoins = COALESCE(leadercoins, 0) + 200,
                                profile_completed = TRUE
                            WHERE player_id = %s
                            RETURNING leadercoins
                        """, (player_id,))
                        bonus_result = cursor.fetchone()
                        if bonus_result:
                            logger.info(f"🎉 Профиль заполнен для {player_id}! +200 leadercoins (всего: {bonus_result[0]})")
        
        invalidate_cache(telegram_id=data['telegram_id'], player_id=player_id)
        
        return jsonify({
            "status": "ok",
            "nick": updated[0] if updated else None,
            "age": updated[1] if updated else None,
            "steam_link": updated[2] if updated else None,
            "faceit_link": updated[3] if updated else None,
            "message": "Profile updated successfully"
        })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка update_profile: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

@app.route('/api/profile/avatar/update', methods=['POST'])
@rate_limit(limit=10, window=60)
def update_avatar():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing fields", ["telegram_id is required"])
        
        # 🔥 ПРИНИМАЕМ И avatar_url И avatar
        avatar_data = data.get('avatar_url') or data.get('avatar')
        if not avatar_data:
            raise ValidationError("Missing avatar data", ["avatar_url or avatar is required"])
        
        is_valid, error = validate_avatar_url(avatar_data)
        if not is_valid:
            raise ValidationError(error, ["avatar"])
        
        update_user_activity(data['telegram_id'])
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            raise NotFoundError("User not found")
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                UPDATE profiles SET avatar = %s 
                WHERE player_id = %s 
                RETURNING avatar
            """, (avatar_data, player_id))
            updated = cursor.fetchone()
        
        invalidate_cache(player_id=player_id)
        
        return jsonify({
            "status": "ok",
            "avatar": updated[0] if updated else None
        })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка update_avatar: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

# ---------- АВАТАР (ПОЛУЧЕНИЕ) ----------
@app.route('/api/profile/avatar', methods=['POST', 'OPTIONS'])
@rate_limit(limit=30, window=60)
def get_avatar():
    if request.method == 'OPTIONS':
        return '', 200
    
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id", ["telegram_id is required"])
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            raise NotFoundError("User not found")
        
        with get_db_cursor() as cursor:
            cursor.execute("SELECT avatar FROM profiles WHERE player_id = %s", (player_id,))
            result = cursor.fetchone()
            
        return jsonify({
            "status": "ok",
            "avatar": result[0] if result and result[0] else None
        })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка get_avatar: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

# ---------- ПОЛЬЗОВАТЕЛЬ ----------
@app.route('/api/user/init', methods=['POST'])
@rate_limit(limit=10, window=60)
def user_init():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id", ["telegram_id is required"])
        
        telegram_id = data['telegram_id']
        username = data.get('username', '')
        
        player_id = get_player_id(telegram_id)
        
        if player_id:
            update_user_activity(telegram_id)
            return jsonify({"status": "ok", "player_id": player_id, "is_new": False})
        
        player_id = generate_player_id()
        nick = username if username and len(username) <= 32 else generate_random_nick()
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO users (telegram_id, player_id, created_at, last_active, is_online, leadercoins)
                VALUES (%s, %s, (NOW() AT TIME ZONE 'UTC'), (NOW() AT TIME ZONE 'UTC'), TRUE, 1000)
            """, (telegram_id, player_id))
            cursor.execute("""
                INSERT INTO profiles (player_id, nick, avatar, created_at)
                VALUES (%s, %s, %s, (NOW() AT TIME ZONE 'UTC'))
            """, (player_id, nick, None))
        
        cache.set(f"player_id:{telegram_id}", player_id)
        
        return jsonify({
            "status": "ok",
            "player_id": player_id,
            "is_new": True,
            "nick": nick
        })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка user_init: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

# ---------- ПОИСК ----------
@app.route('/api/search/start', methods=['POST'])
@rate_limit(limit=5, window=60)
def start_search():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id", ["telegram_id is required"])
        
        update_user_activity(data['telegram_id'])
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            raise NotFoundError("User not found")
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT id FROM matches 
                WHERE (player1_id = %s OR player2_id = %s) 
                AND status IN ('pending', 'accepted')
                AND expires_at > (NOW() AT TIME ZONE 'UTC')
            """, (player_id, player_id))
            if cursor.fetchone():
                raise ConflictError("User already in a match")
            
            cursor.execute("DELETE FROM search_queue WHERE player_id = %s", (player_id,))
            
            mode = data.get('mode', '').lower()
            # 🔥 ВСЕ РЕЖИМЫ РАЗРЕШЕНЫ
            if mode not in ['faceit', 'premier', 'prime', 'public']:
                raise ValidationError("Invalid mode", ["mode must be faceit, premier, prime, or public"])
            
            rank_value = data.get('rating_value', '0')
            
            # 🔥 ОПРЕДЕЛЯЕМ ЧТО СОХРАНЯТЬ В ПОЛЕ rank
            if mode in ['faceit', 'premier']:
                try:
                    rating_number = int(rank_value)
                    max_rating = 5000 if mode == 'faceit' else 40000
                    if rating_number < 0 or rating_number > max_rating:
                        raise ValueError
                except ValueError:
                    error_msg = f"rating_value must be between 0-{max_rating}"
                    raise ValidationError("Invalid rating value", [error_msg])
                rank_display = str(rating_number)  # Для FACEIT/PREMIER сохраняем число
            else:
                # Для prime/public сохраняем НАЗВАНИЕ ранга
                if rank_value not in RANK_TO_VALUE:
                    raise ValidationError("Invalid rank", ["rank must be valid CS2 rank"])
                rating_number = RANK_TO_VALUE[rank_value]
                rank_display = rank_value  # 🔥 СОХРАНЯЕМ СТРОКОВОЕ НАЗВАНИЕ РАНГА
            
            # 🔥 РАЗНЫЙ ДЕЛИТЕЛЬ ДЛЯ РАЗНЫХ РЕЖИМОВ
            if mode == 'premier':
                rating_bucket = rating_number // 5000  # Бакет = 0, 1, 2... 8 (для 0-40000)
            elif mode == 'faceit':
                rating_bucket = rating_number // 100   # Бакет = 0-50 (для 0-5000)
            else:
                rating_bucket = rating_number // 100   # Prime/Public
            
            # 🔥 СОХРАНЯЕМ ОРИГИНАЛЬНЫЙ РЕЖИМ — КАЖДЫЙ В СВОЕЙ ОЧЕРЕДИ
            cursor.execute("""
                INSERT INTO search_queue 
                (player_id, mode, rank, rating_bucket, style, age, steam_link, faceit_link, comment, joined_at, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, (NOW() AT TIME ZONE 'UTC'), (NOW() AT TIME ZONE 'UTC' + INTERVAL '2 minutes'))
            """, (
                player_id, mode, rank_display, rating_bucket,
                data.get('style', 'fan'), data.get('age'),
                data.get('steam_link'), data.get('faceit_link'), data.get('comment')
            ))

        logger.info(f"✅ Поиск добавлен: player_id={player_id}, mode={mode}, rank={rank_display}, steam={data.get('steam_link')}, faceit={data.get('faceit_link')}, style={data.get('style')}")
        
        return jsonify({
            "status": "searching",
            "message": "Added to search queue",
            "expires_in": 120
        })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка start_search: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

@app.route('/api/match/check', methods=['POST'])
@rate_limit(limit=30, window=60)
def check_match():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id", ["telegram_id is required"])
        
        logger.info(f"🔍 check_match для telegram_id={data['telegram_id']}")
        
        update_user_activity(data['telegram_id'])
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            raise NotFoundError("User not found")
        
        logger.info(f"🔍 player_id={player_id}")
        
        with get_db_cursor() as cursor:
            lock_key = hash(player_id) % 2**31
            cursor.execute("SELECT pg_try_advisory_xact_lock(%s)", (lock_key,))
            
            # Проверяем, есть ли уже активный матч
            cursor.execute("""
                SELECT id, player1_id, player2_id, mode,
                       player1_age, player1_rank, player1_style, player1_comment, player1_steam_link, player1_faceit_link,
                       player2_age, player2_rank, player2_style, player2_comment, player2_steam_link, player2_faceit_link,
                       expires_at, status
                FROM matches
                WHERE (player1_id = %s OR player2_id = %s)
                AND status IN ('pending', 'accepted')
                AND expires_at > (NOW() AT TIME ZONE 'UTC')
                ORDER BY id DESC LIMIT 1
            """, (player_id, player_id))
            
            match = cursor.fetchone()
            
            if match:
                logger.info(f"📋 Найден существующий матч id={match['id']}")
                
                other_id = match['player2_id'] if match['player1_id'] == player_id else match['player1_id']
                is_player1 = (str(match['player1_id']) == str(player_id))
                
                profile = get_profile_cached(other_id)
                if profile:
                    # Получаем репутацию
                    cursor.execute("SELECT COALESCE(rating, 0) as trust_rating FROM users WHERE player_id = %s", (other_id,))
                    trust_data = cursor.fetchone()
                    trust_rating = trust_data['trust_rating'] if trust_data else 0
                    
                    # Берём данные из матча
                    if is_player1:
                        opponent_age = match['player2_age'] or profile['age'] or 0
                        opponent_rank = match['player2_rank'] or "0"
                        opponent_style = match['player2_style'] or "fan"
                        opponent_comment = match['player2_comment'] or "Нет комментария"
                        opponent_steam_link = match.get('player2_steam_link') or profile.get('steam_link') or "Не указана"
                        opponent_faceit_link = match.get('player2_faceit_link') or profile.get('faceit_link') or "Не указана"
                    else:
                        opponent_age = match['player1_age'] or profile['age'] or 0
                        opponent_rank = match['player1_rank'] or "0"
                        opponent_style = match['player1_style'] or "fan"
                        opponent_comment = match['player1_comment'] or "Нет комментария"
                        opponent_steam_link = match.get('player1_steam_link') or profile.get('steam_link') or "Не указана"
                        opponent_faceit_link = match.get('player1_faceit_link') or profile.get('faceit_link') or "Не указана"
                    
                    return jsonify({
                        "match_found": True,
                        "match_id": match['id'],
                        "opponent": {
                            "player_id": other_id,
                            "nick": profile['nick'],
                            "age": opponent_age,
                            "style": opponent_style,
                            "rating": opponent_rank,
                            "rank": opponent_rank,
                            "trust_rating": trust_rating,
                            "steam_link": opponent_steam_link,
                            "faceit_link": opponent_faceit_link,
                            "avatar": profile.get('avatar'),
                            "comment": opponent_comment
                        },
                        "expires_at": match['expires_at'].isoformat() + "Z"
                    })
            
            # Ищем текущего игрока в очереди
            cursor.execute("""
                SELECT * FROM search_queue 
                WHERE player_id = %s AND expires_at > (NOW() AT TIME ZONE 'UTC')
                FOR UPDATE SKIP LOCKED
            """, (player_id,))
            current = cursor.fetchone()
            
            if not current:
                logger.info("❌ Игрок не в очереди")
                return jsonify({"match_found": False, "in_queue": False})
            
            logger.info(f"📋 Текущий игрок: mode={current['mode']}, style={current['style']}, rating_bucket={current['rating_bucket']}, rank={current['rank']}")
            
            # 🔥 ВКЛЮЧАЕМ ФИЛЬТР ПО БАКЕТАМ
            min_bucket, max_bucket = get_range_buckets(
                current['mode'], 
                current['style'], 
                current['rating_bucket']
            )
            
            # Логируем диапазон поиска
            if min_bucket is not None and max_bucket is not None:
                logger.info(f"🎯 Диапазон поиска: бакеты {min_bucket}-{max_bucket}")
            else:
                logger.info(f"🎯 Режим без фильтрации: ищем всех")
            
            # Базовый запрос
            query = """
                SELECT sq.*, p.nick, p.avatar
                FROM search_queue sq
                JOIN profiles p ON sq.player_id = p.player_id
                WHERE sq.mode = %s 
                AND sq.player_id != %s
                AND sq.expires_at > (NOW() AT TIME ZONE 'UTC')
            """
            params = [current['mode'], player_id]
            
            # 🔥 ПРИМЕНЯЕМ ФИЛЬТР ПО БАКЕТАМ, ЕСЛИ ОН ЕСТЬ
            if min_bucket is not None and max_bucket is not None:
                query += " AND sq.rating_bucket BETWEEN %s AND %s"
                params.extend([min_bucket, max_bucket])
            
            query += " FOR UPDATE SKIP LOCKED"
            
            cursor.execute(query, params)
            candidates = cursor.fetchall()
            
            logger.info(f"🔍 Найдено кандидатов в диапазоне: {len(candidates)}")
            
            if not candidates:
                logger.info("❌ Кандидатов не найдено в заданном диапазоне")
                return jsonify({
                    "match_found": False, 
                    "in_queue": True,
                    "search_range": {
                        "min_bucket": min_bucket,
                        "max_bucket": max_bucket
                    } if min_bucket is not None else None
                })
            
            # 🔥 СОРТИРУЕМ ПО СОВМЕСТИМОСТИ
            candidates_list = []
            for cand in candidates:
                # Подготавливаем данные для расчета скора
                player_data = {
                    'rank': current['rank'],
                    'age': current['age']
                }
                cand_data = {
                    'rank': cand['rank'],
                    'age': cand['age']
                }
                score = calculate_score(player_data, cand_data, current['mode'])
                candidates_list.append((score, cand))
            
            # Сортируем по скору (меньше = лучше)
            candidates_list.sort(key=lambda x: x[0])
            
            # 🔥 Берем лучшего кандидата
            best_score, best = candidates_list[0]
            
            logger.info(f"✅ Найден кандидат: player_id={best['player_id']}, score={best_score}, rank={best['rank']}, bucket={best['rating_bucket']}")
            
            expires_at = datetime.utcnow() + timedelta(seconds=40)
            
            # Сохраняем матч
            cursor.execute("""
                INSERT INTO matches (
                    player1_id, player2_id, mode,
                    player1_age, player1_rank, player1_style, player1_comment, player1_steam_link, player1_faceit_link,
                    player2_age, player2_rank, player2_style, player2_comment, player2_steam_link, player2_faceit_link,
                    created_at, expires_at, status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, (NOW() AT TIME ZONE 'UTC'), %s, 'pending')
                RETURNING id
            """, (
                player_id, best['player_id'], current['mode'],
                current['age'], current['rank'], current['style'] or 'fan', current.get('comment') or '',
                current.get('steam_link'), current.get('faceit_link'),
                best['age'], best['rank'], best['style'] or 'fan', best.get('comment') or '',
                best.get('steam_link'), best.get('faceit_link'),
                expires_at
            ))
            match_id = cursor.fetchone()['id']
            
            logger.info(f"✅ Матч создан: id={match_id}")
            
            # Получаем репутацию второго игрока
            cursor.execute("SELECT COALESCE(rating, 0) FROM users WHERE player_id = %s", (best['player_id'],))
            trust_data = cursor.fetchone()
            trust_rating = trust_data[0] if trust_data else 0
            
            # Данные для ответа
            rank_value = best['rank'] if best['rank'] and best['rank'] != '0' else "—"
            opponent_data = {
                "player_id": best['player_id'],
                "nick": best['nick'],
                "age": best['age'] or 0,
                "style": best['style'] or "fan",
                "rating": rank_value,
                "rank": rank_value,
                "trust_rating": trust_rating,
                "steam_link": best.get('steam_link') or "Не указана",
                "faceit_link": best.get('faceit_link') or "Не указана",
                "avatar": best.get('avatar'),
                "comment": best.get('comment') or "Нет комментария"
            }
            
            logger.info(f"📤 Отправляем оппонента: steam={opponent_data['steam_link']}, style={opponent_data['style']}")
            
            # Удаляем обоих из очереди
            cursor.execute("DELETE FROM search_queue WHERE player_id IN (%s, %s)", 
                          (player_id, best['player_id']))
            
            # 🔥 ДОБАВЛЯЕМ ИНФУ О ДИАПАЗОНЕ ПОИСКА В ОТВЕТ
            search_range = None
            if min_bucket is not None and max_bucket is not None:
                search_range = {
                    "min_bucket": min_bucket,
                    "max_bucket": max_bucket
                }
            
            return jsonify({
                "match_found": True,
                "match_id": match_id,
                "opponent": opponent_data,
                "expires_at": expires_at.isoformat() + "Z",
                "in_queue": True,
                "search_range": search_range,  # 🔥 Чтобы фронт понимал, в каком диапазоне искали
                "compatibility_score": best_score  # 🔥 Показываем насколько хорошо подходит (меньше = лучше)
            })
            
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка check_match: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

@app.route('/api/match/status/<int:match_id>', methods=['GET'])
@rate_limit(limit=60, window=60)
def match_status(match_id: int):
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
        raise AppError(str(e), 500, "INTERNAL_ERROR")

@app.route('/api/match/respond', methods=['POST'])
@rate_limit(limit=10, window=60)
def respond_match():
    try:
        data = request.json
        if not data or 'telegram_id' not in data or 'match_id' not in data or 'response' not in data:
            raise ValidationError("Missing fields", ["telegram_id, match_id, response are required"])
        
        if data['response'] not in ['accept', 'reject']:
            raise ValidationError("Invalid response", ["response must be 'accept' or 'reject'"])
        
        update_user_activity(data['telegram_id'])
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            raise NotFoundError("User not found")
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT player1_id, player2_id, player1_response, player2_response, expires_at, status
                FROM matches WHERE id = %s FOR UPDATE
            """, (data['match_id'],))
            match = cursor.fetchone()
            
            if not match:
                raise NotFoundError("Match not found")
            
            if datetime.utcnow() > match['expires_at']:
                cursor.execute("UPDATE matches SET status = 'expired' WHERE id = %s", (data['match_id'],))
                return jsonify({"status": "expired", "message": "Match expired"})
            
            if str(match['player1_id']) == str(player_id):
                if match['player1_response'] is not None:
                    return jsonify({"status": "already_responded", "message": "Already responded"})
                cursor.execute("UPDATE matches SET player1_response = %s WHERE id = %s", 
                              (data['response'], data['match_id']))
            elif str(match['player2_id']) == str(player_id):
                if match['player2_response'] is not None:
                    return jsonify({"status": "already_responded", "message": "Already responded"})
                cursor.execute("UPDATE matches SET player2_response = %s WHERE id = %s", 
                              (data['response'], data['match_id']))
            else:
                raise ValidationError("User not in this match", ["user_id"])
            
            cursor.execute("SELECT player1_response, player2_response FROM matches WHERE id = %s", 
                          (data['match_id'],))
            r1, r2 = cursor.fetchone()
            
            if r1 == 'accept' and r2 == 'accept':
                cursor.execute("UPDATE matches SET status = 'accepted' WHERE id = %s", (data['match_id'],))
                
                player1_id = match['player1_id']
                player2_id = match['player2_id']
                
                cursor.execute("""
                    SELECT 1 FROM friends 
                    WHERE (player1_id = %s AND player2_id = %s) 
                       OR (player1_id = %s AND player2_id = %s)
                """, (player1_id, player2_id, player2_id, player1_id))
                already_friends = cursor.fetchone()
                
                if not already_friends:
                    cursor.execute("""
                        INSERT INTO friends (player1_id, player2_id, created_at)
                        VALUES (%s, %s, (NOW() AT TIME ZONE 'UTC'))
                    """, (player1_id, player2_id))
                    logger.info(f"✅ Добавлены в друзья: {player1_id} и {player2_id}")
                else:
                    logger.info(f"⏭️ Пользователи {player1_id} и {player2_id} уже в друзьях")
                
                return jsonify({"status": "accepted", "both_accepted": True})
                
            elif r1 == 'reject' or r2 == 'reject':
                # Удаляем матч
                cursor.execute("DELETE FROM matches WHERE id = %s", (data['match_id'],))
                
                # 🔥 УДАЛЯЕМ ОБОИХ ИГРОКОВ ИЗ ОЧЕРЕДИ ПОИСКА
                cursor.execute("DELETE FROM search_queue WHERE player_id IN (%s, %s)", 
                               (match['player1_id'], match['player2_id']))
                
                logger.info(f"❌ Матч {data['match_id']} отклонён. Игроки {match['player1_id']} и {match['player2_id']} удалены из очереди.")
                
                return jsonify({"status": "rejected", "both_accepted": False})
                
            else:
                time_left = max(0, int((match['expires_at'] - datetime.utcnow()).total_seconds()))
                return jsonify({
                    "status": "waiting",
                    "both_accepted": False,
                    "time_left": time_left
                })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка respond_match: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")
        
@app.route('/api/search/stop', methods=['POST'])
@rate_limit(limit=10, window=60)
def stop_search():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id", ["telegram_id is required"])
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            raise NotFoundError("User not found")
        
        set_user_offline(data['telegram_id'])
        
        with get_db_cursor() as cursor:
            cursor.execute("DELETE FROM search_queue WHERE player_id = %s", (player_id,))
            deleted = cursor.rowcount
        
        return jsonify({
            "status": "stopped",
            "deleted": deleted,
            "message": "Search stopped"
        })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка stop_search: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

# ---------- ДРУЗЬЯ ----------
@app.route('/api/friends/list', methods=['POST'])
@rate_limit()
def friends_list():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id", ["telegram_id is required"])
        
        update_user_activity(data['telegram_id'])
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            raise NotFoundError("User not found")
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    CASE WHEN player1_id = %s THEN player2_id ELSE player1_id END as friend_id,
                    created_at
                FROM friends
                WHERE player1_id = %s OR player2_id = %s
                ORDER BY created_at DESC
            """, (player_id, player_id, player_id))
            friend_records = cursor.fetchall()
            
            friends = []
            for record in friend_records:
                profile = get_profile_cached(record['friend_id'])
                if profile:
                    cursor.execute("SELECT username, telegram_id FROM users WHERE player_id = %s", (record['friend_id'],))
                    user_data = cursor.fetchone()
                    
                    friends.append({
                        "player_id": record['friend_id'],
                        "nick": profile['nick'],
                        "avatar": profile.get('avatar'),
                        "age": profile.get('age'),
                        "steam_link": profile.get('steam_link'),
                        "faceit_link": profile.get('faceit_link'),
                        "username": user_data['username'] if user_data else None,
                        "telegram_id": user_data['telegram_id'] if user_data else None,
                        "added_at": record['created_at'].isoformat() if record['created_at'] else None
                    })
        
        return jsonify({
            "status": "ok",
            "friends": friends,
            "count": len(friends)
        })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка friends_list: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")
        
@app.route('/api/friends/add', methods=['POST'])
@rate_limit(limit=20, window=60)
def add_friend():
    try:
        data = request.json
        if not data or 'telegram_id' not in data or 'friend_player_id' not in data:
            raise ValidationError("Missing fields", ["telegram_id and friend_player_id are required"])
        
        update_user_activity(data['telegram_id'])
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            raise NotFoundError("User not found")
        
        if str(player_id) == str(data['friend_player_id']):
            raise ValidationError("Cannot add yourself as friend", ["friend_player_id"])
        
        friend_profile = get_profile_cached(data['friend_player_id'])
        if not friend_profile:
            raise NotFoundError("Friend not found")
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO friends (player1_id, player2_id, created_at)
                VALUES (%s, %s, (NOW() AT TIME ZONE 'UTC'))
                ON CONFLICT (player1_id, player2_id) DO NOTHING
            """, (player_id, data['friend_player_id']))
            
            if cursor.rowcount == 0:
                return jsonify({
                    "status": "already_friends",
                    "message": "Already friends"
                })
        
        return jsonify({
            "status": "ok",
            "message": "Friend added successfully"
        })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка add_friend: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

@app.route('/api/friends/remove', methods=['POST'])
@rate_limit(limit=20, window=60)
def remove_friend():
    try:
        data = request.json
        if not data or 'telegram_id' not in data or 'friend_player_id' not in data:
            raise ValidationError("Missing fields", ["telegram_id and friend_player_id are required"])
        
        update_user_activity(data['telegram_id'])
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            raise NotFoundError("User not found")
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                DELETE FROM friends 
                WHERE (player1_id = %s AND player2_id = %s) 
                   OR (player1_id = %s AND player2_id = %s)
            """, (player_id, data['friend_player_id'], data['friend_player_id'], player_id))
            
            deleted = cursor.rowcount
        
        return jsonify({
            "status": "ok",
            "deleted": deleted,
            "message": "Friend removed successfully" if deleted else "Friend not found"
        })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка remove_friend: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

# ---------- ЛИДЕРБОРД ----------
@app.route('/api/users/leaderboard', methods=['POST'])
@rate_limit()
def get_leaderboard():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id", ["telegram_id is required"])
        
        update_user_activity(data['telegram_id'])
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            raise NotFoundError("User not found")
        
        page = int(data.get('page', 1))
        per_page = min(int(data.get('per_page', 20)), 100)
        offset = (page - 1) * per_page
        
        with get_db_cursor() as cursor:
            # Получаем общее количество
            cursor.execute("SELECT COUNT(*) FROM users WHERE leadercoins IS NOT NULL")
            total = cursor.fetchone()[0]
            
            # Получаем топ игроков по leadercoins
            cursor.execute("""
                SELECT 
                    u.player_id,
                    p.nick,
                    p.avatar,
                    COALESCE(u.leadercoins, 0) as leadercoins,
                    ROW_NUMBER() OVER (ORDER BY COALESCE(u.leadercoins, 0) DESC) as rank
                FROM users u
                JOIN profiles p ON u.player_id = p.player_id
                WHERE u.leadercoins IS NOT NULL
                ORDER BY u.leadercoins DESC
                LIMIT %s OFFSET %s
            """, (per_page, offset))
            top_players = cursor.fetchall()
            
            # Получаем ранг текущего пользователя по leadercoins
            cursor.execute("""
                SELECT COUNT(*) + 1 as rank
                FROM users
                WHERE leadercoins > (SELECT COALESCE(leadercoins, 0) FROM users WHERE player_id = %s)
            """, (player_id,))
            user_rank = cursor.fetchone()['rank'] if cursor.rowcount > 0 else None
            
            leaderboard = []
            for row in top_players:
                leaderboard.append({
                    "rank": row['rank'],
                    "player_id": row['player_id'],
                    "nick": row['nick'],
                    "avatar": row['avatar'],
                    "leadercoins": row['leadercoins']
                })
            
            return jsonify({
                "status": "ok",
                "leaderboard": leaderboard,
                "pagination": {
                    "page": page,
                    "per_page": per_page,
                    "total": total,
                    "total_pages": (total + per_page - 1) // per_page
                },
                "user_rank": user_rank
            })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка leaderboard: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

# ---------- ИСТОРИЯ МАТЧЕЙ ----------
@app.route('/api/my-matches', methods=['POST'])
@rate_limit()
def my_matches():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id", ["telegram_id is required"])
        
        update_user_activity(data['telegram_id'])
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            raise NotFoundError("User not found")
        
        page = int(data.get('page', 1))
        per_page = min(int(data.get('per_page', 20)), 50)
        offset = (page - 1) * per_page
        
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
                       g.telegram_chat_link, g.created_at, g.status
                FROM matches m
                JOIN games g ON m.id = g.match_id
                JOIN profiles p1 ON m.player1_id = p1.player_id
                JOIN profiles p2 ON m.player2_id = p2.player_id
                WHERE (m.player1_id = %s OR m.player2_id = %s)
                AND g.status = 'closed'
                ORDER BY m.id DESC
                LIMIT %s OFFSET %s
            """, (player_id, player_id, per_page, offset))
            history = cursor.fetchall()
            
            cursor.execute("""
                SELECT COUNT(*)
                FROM matches m
                JOIN games g ON m.id = g.match_id
                WHERE (m.player1_id = %s OR m.player2_id = %s)
                AND g.status = 'closed'
            """, (player_id, player_id))
            total_history = cursor.fetchone()[0]
        
        return jsonify({
            "status": "ok",
            "active": [dict(match) for match in active],
            "history": [dict(match) for match in history],
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total_history,
                "total_pages": (total_history + per_page - 1) // per_page
            }
        })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка my_matches: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

@app.route('/api/active-match', methods=['POST'])
@rate_limit()
def active_match():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id", ["telegram_id is required"])
        
        update_user_activity(data['telegram_id'])
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            raise NotFoundError("User not found")
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT g.telegram_chat_link, m.id, g.expires_at
                FROM games g
                JOIN matches m ON g.match_id = m.id
                WHERE (m.player1_id = %s OR m.player2_id = %s)
                AND g.status = 'active'
                AND g.expires_at > (NOW() AT TIME ZONE 'UTC')
                LIMIT 1
            """, (player_id, player_id))
            game = cursor.fetchone()
            
            if game:
                time_left = max(0, int((game['expires_at'] - datetime.utcnow()).total_seconds()))
                return jsonify({
                    "active": True,
                    "match_id": game['id'],
                    "chat_link": game['telegram_chat_link'],
                    "time_left_seconds": time_left
                })
            return jsonify({"active": False})
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка active_match: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

@app.route('/api/game/create', methods=['POST'])
@rate_limit(limit=5, window=60)
def create_game():
    try:
        data = request.json
        if not data or 'match_id' not in data:
            raise ValidationError("Missing match_id", ["match_id is required"])
        
        match_id = data['match_id']
        logger.info(f"🎮 Создание игры для матча {match_id}")
        
        with get_db_cursor() as cursor:
            match = None
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
                raise ValidationError("Match not accepted yet", ["match_id"])
            
            player1_id, player2_id, mode = match
            logger.info(f"👥 Игроки: {player1_id} и {player2_id}, режим: {mode}")
            
            cursor.execute("""
                SELECT u.telegram_id, p.nick 
                FROM users u 
                JOIN profiles p ON u.player_id = p.player_id 
                WHERE u.player_id = %s
            """, (player1_id,))
            user1 = cursor.fetchone()
            
            cursor.execute("""
                SELECT u.telegram_id, p.nick 
                FROM users u 
                JOIN profiles p ON u.player_id = p.player_id 
                WHERE u.player_id = %s
            """, (player2_id,))
            user2 = cursor.fetchone()
            
            if not user1 or not user2:
                raise NotFoundError("Users not found")
            
            telegram_id1, nick1 = user1
            telegram_id2, nick2 = user2
            
            # Создаём запись в games
            expires_at = datetime.utcnow() + timedelta(minutes=30)
            cursor.execute("""
                INSERT INTO games (match_id, player1_id, player2_id, status, created_at, expires_at)
                VALUES (%s, %s, %s, 'pending', (NOW() AT TIME ZONE 'UTC'), %s)
                RETURNING id
            """, (match_id, player1_id, player2_id, expires_at))
            game_id = cursor.fetchone()[0]
            logger.info(f"📝 Создана запись в БД, game_id = {game_id}")
            
            # Создаём тему в форуме
            import requests
            create_topic_url = f"https://api.telegram.org/bot{BOT_TOKEN}/createForumTopic"
            topic_name = f"🎮 Мэтч #{game_id} | {nick1} & {nick2}"
            
            logger.info(f"📡 Отправляем запрос на создание темы: {topic_name}")
            
            topic_response = requests.post(create_topic_url, json={
                "chat_id": FORUM_GROUP_ID,
                "name": topic_name,
                "icon_color": 0x6FB9F0
            }, timeout=5)
            
            if not topic_response.ok:
                cursor.execute("DELETE FROM games WHERE id = %s", (game_id,))
                logger.error(f"❌ Ошибка создания темы: {topic_response.text}")
                raise AppError("Failed to create game topic", 500, "TELEGRAM_ERROR")
            
            topic_data = topic_response.json()
            topic_id = topic_data['result']['message_thread_id']
            logger.info(f"✅ Тема создана, topic_id = {topic_id}")
            
            clean_chat_id = str(FORUM_GROUP_ID).replace('-100', '')
            public_link = f"https://t.me/c/{clean_chat_id}/{topic_id}"
            
            cursor.execute("""
                UPDATE games 
                SET telegram_chat_id = %s, 
                    telegram_chat_link = %s, 
                    status = 'active'
                WHERE id = %s
            """, (topic_id, public_link, game_id))
            
            welcome_message = (
                f"🎮 **Мэтч #{game_id} создан!**\n\n"
                f"👤 {nick1} & {nick2}\n"
                f"🎯 Режим: {mode.upper()}\n"
                f"💬 Чат активен 30 минут или перейдите лс"
            )
            
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": FORUM_GROUP_ID,
                    "message_thread_id": topic_id,
                    "text": welcome_message,
                    "parse_mode": "Markdown"
                },
                timeout=3
            )
            
            send_match_notification(telegram_id1, game_id, nick2, public_link)
            send_match_notification(telegram_id2, game_id, nick1, public_link)
            
            for pid in [player1_id, player2_id]:
                cursor.execute("""
                    SELECT COUNT(*) FROM games 
                    WHERE (player1_id = %s OR player2_id = %s) AND status = 'active'
                """, (pid, pid))
                games_count = cursor.fetchone()[0]
                
                if games_count == 1:
                    cursor.execute("""
                        UPDATE users 
                        SET leadercoins = COALESCE(leadercoins, 0) + 150
                        WHERE player_id = %s
                    """, (pid,))
                    logger.info(f"🎉 Первый мэтч для игрока {pid}! +150 leadercoins")
            
            logger.info(f"✅ Игра {game_id} успешно создана!")
            
            return jsonify({
                "status": "ok",
                "game_id": game_id,
                "chat_link": public_link,
                "expires_in": 1800
            })
            
    except AppError:
        raise
    except Exception as e:
        logger.error(f"❌ Ошибка create_game: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

# ---------- ПОИСК ИГРОКОВ ----------
@app.route('/api/users/all', methods=['POST'])
@rate_limit()
def get_all_users():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id", ["telegram_id is required"])
        
        update_user_activity(data['telegram_id'])
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            raise NotFoundError("User not found")
        
        page = int(data.get('page', 1))
        per_page = min(int(data.get('per_page', 20)), 100)
        offset = (page - 1) * per_page
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT player_id, nick, avatar, age, steam_link, faceit_link
                FROM profiles
                WHERE player_id != %s
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
            """, (player_id, per_page, offset))
            users = [dict(row) for row in cursor.fetchall()]
            
            cursor.execute("SELECT COUNT(*) FROM profiles WHERE player_id != %s", (player_id,))
            total = cursor.fetchone()[0]
        
        return jsonify({
            "status": "ok",
            "users": users,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "total_pages": (total + per_page - 1) // per_page
            }
        })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка get_all_users: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

@app.route('/api/users/search', methods=['POST'])
@rate_limit(limit=30, window=60)
def search_users():
    try:
        data = request.json
        if not data or 'telegram_id' not in data or 'query' not in data:
            raise ValidationError("Missing fields", ["telegram_id and query are required"])
        
        update_user_activity(data['telegram_id'])
        
        player_id = get_player_id(data['telegram_id'])
        query = data['query'].strip()
        
        if not player_id:
            raise NotFoundError("User not found")
        
        if len(query) < 2:
            return jsonify({
                "status": "ok",
                "users": [],
                "query": query,
                "message": "Query too short (minimum 2 characters)"
            })
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT player_id, nick, avatar, age, steam_link, faceit_link
                FROM profiles
                WHERE player_id != %s 
                AND (nick ILIKE %s OR player_id::text ILIKE %s)
                ORDER BY 
                    CASE 
                        WHEN nick ILIKE %s THEN 1 
                        WHEN nick ILIKE %s THEN 2 
                        ELSE 3 
                    END,
                    nick
                LIMIT 50
            """, (
                player_id, 
                f'%{query}%', 
                f'%{query}%',
                f'{query}%',
                f'%{query}%'
            ))
            users = [dict(row) for row in cursor.fetchall()]
        
        return jsonify({
            "status": "ok",
            "users": users,
            "query": query,
            "count": len(users)
        })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка search_users: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

@app.route('/api/user/profile/<player_id>', methods=['POST'])
@rate_limit()
def get_user_profile(player_id: str):
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id", ["telegram_id is required"])
        
        update_user_activity(data['telegram_id'])
        
        profile = get_profile_cached(player_id)
        if not profile:
            raise NotFoundError("Profile not found")
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT is_online, 
                       EXTRACT(EPOCH FROM (NOW() AT TIME ZONE 'UTC' - last_active)) as seconds_ago
                FROM users 
                WHERE player_id = %s
            """, (player_id,))
            user_data = cursor.fetchone()
        
        is_online = user_data['is_online'] and (user_data['seconds_ago'] or 0) < 300 if user_data else False
        
        return jsonify({
            "status": "ok",
            "player_id": player_id,
            "nick": profile['nick'],
            "age": profile.get('age'),
            "steam_link": profile.get('steam_link'),
            "faceit_link": profile.get('faceit_link'),
            "avatar": profile.get('avatar'),
            "is_online": is_online,
            "last_active_seconds_ago": int(user_data['seconds_ago'] or 0) if user_data else None
        })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка get_user_profile: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

# ---------- ВЕРИФИКАЦИЯ ----------
@app.route('/api/verify-token', methods=['POST'])
@rate_limit(limit=20, window=60)
def verify_token():
    try:
        data = request.json
        if not data or 'token' not in data or 'user_id' not in data:
            raise ValidationError("Missing fields", ["token and user_id are required"])
        
        match_id, token_user_id = verify_match_token(data['token'])
        
        if match_id and token_user_id == str(data['user_id']):
            return jsonify({
                "valid": True,
                "match_id": match_id
            })
        return jsonify({"valid": False})
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка verify_token: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

# ---------- ФОРУМ ----------
@app.route('/api/check-forum', methods=['POST'])
@rate_limit(limit=10, window=60)
def check_forum():
    try:
        data = request.json
        if not data or 'user_id' not in data:
            raise ValidationError("Missing user_id", ["user_id is required"])
        
        try:
            user_id_int = int(data['user_id'])
        except (ValueError, TypeError):
            raise ValidationError("Invalid user_id", ["user_id must be an integer"])
        
        in_forum = check_user_in_forum(user_id_int)
        return jsonify({
            "in_forum": in_forum,
            "forum_link": f"https://t.me/c/{str(FORUM_GROUP_ID).replace('-100', '')}"
        })
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка check_forum: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

@app.route('/api/user/update-username', methods=['POST'])
@rate_limit(limit=30, window=60)
def update_username():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            raise ValidationError("Missing telegram_id", ["telegram_id is required"])
        
        telegram_id = data['telegram_id']
        username = data.get('username', '')
        
        with get_db_cursor() as cursor:
            cursor.execute("""
                UPDATE users 
                SET username = %s 
                WHERE telegram_id = %s
                RETURNING username
            """, (username, telegram_id))
            result = cursor.fetchone()
            
            if result:
                return jsonify({"status": "ok", "username": result[0]})
            else:
                raise NotFoundError("User not found")
    except AppError:
        raise
    except Exception as e:
        logger.error(f"Ошибка update_username: {e}")
        raise AppError(str(e), 500, "INTERNAL_ERROR")

# ---------- СТАТИСТИКА ----------
@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Публичная статистика (без авторизации)"""
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM users")
            total_users = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM users WHERE is_online = TRUE")
            online_users = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM matches WHERE status = 'accepted'")
            total_matches = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) FROM search_queue 
                WHERE expires_at > (NOW() AT TIME ZONE 'UTC')
            """)
            searching_users = cursor.fetchone()[0]
        
        return jsonify({
            "status": "ok",
            "stats": {
                "total_users": total_users,
                "online_users": online_users,
                "total_matches": total_matches,
                "searching_users": searching_users
            }
        })
    except Exception as e:
        logger.error(f"Ошибка get_stats: {e}")
        return jsonify({"error": str(e)}), 500

# ---------- РЕПУТАЦИЯ (CALLBACK ОТ БОТА) ----------
@app.route('/api/reputation/vote', methods=['POST'])
def reputation_vote():
    """Обработка голосов за репутацию от бота"""
    try:
        data = request.json
        if not data or 'callback_data' not in data:
            raise ValidationError("Missing callback_data")
        
        callback_data = data['callback_data']
        message = data.get('message', {})
        chat_id = message.get('chat', {}).get('id')
        message_id = message.get('message_id')
        
        # Парсим callback_data: vote_up_12345_42 или vote_down_12345_42
        # Формат: vote_{up/down}_{voter_telegram_id}_{match_id}
        parts = callback_data.split('_')
        if len(parts) != 4:
            return jsonify({"status": "error", "message": "Invalid callback_data"}), 400
        
        vote_type = parts[1]  # up или down
        voter_telegram_id = parts[2]  # 👈 ТОТ, КТО ГОЛОСУЕТ
        match_id = parts[3]
        
        # 🔥 НАХОДИМ ТИММЕЙТА (того, КОМУ ставим оценку)
        with get_db_cursor() as cursor:
            cursor.execute("""
                SELECT player1_id, player2_id 
                FROM matches 
                WHERE id = %s
            """, (match_id,))
            match = cursor.fetchone()
            
            if not match:
                return jsonify({"status": "error", "message": "Match not found"}), 404
            
            # Находим telegram_id тиммейта (не голосующего)
            cursor.execute("""
                SELECT telegram_id FROM users WHERE player_id IN (%s, %s)
            """, (match['player1_id'], match['player2_id']))
            players = cursor.fetchall()
            
            player_ids = [str(p[0]) for p in players]
            
            if voter_telegram_id not in player_ids:
                return jsonify({"status": "error", "message": "User not in match"}), 403
            
            # Берем другого игрока (тиммейта)
            teammate_telegram_id = player_ids[0] if player_ids[1] == voter_telegram_id else player_ids[1]
        
        # Обновляем репутацию (rating) ТИММЕЙТА
        delta = 1 if vote_type == 'up' else -1
        update_reputation(teammate_telegram_id, delta)
        
        logger.info(f"✅ Голос: {vote_type} | voter={voter_telegram_id} | target={teammate_telegram_id} | delta={delta}")
        
        # Обновляем сообщение в боте (убираем кнопки, показываем оценку)
        if chat_id and message_id:
            vote_emoji = "👍" if vote_type == 'up' else "👎"
            new_text = message.get('text', '').replace('Оцените тиммейта:', f'✅ Вы оценили тиммейта: {vote_emoji}')
            
            # Получаем ссылку на чат из старого сообщения
            chat_link = None
            if message.get('reply_markup') and message['reply_markup'].get('inline_keyboard'):
                for row in message['reply_markup']['inline_keyboard']:
                    for btn in row:
                        if btn.get('url'):
                            chat_link = btn['url']
                            break
            
            new_keyboard = {"inline_keyboard": []}
            if chat_link:
                new_keyboard["inline_keyboard"].append([{"text": "👉 Перейти в чат", "url": chat_link}])
            
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
                json={
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "text": new_text,
                    "reply_markup": new_keyboard if new_keyboard["inline_keyboard"] else None
                },
                timeout=5
            )
        
        return jsonify({"status": "ok"})
        
    except Exception as e:
        logger.error(f"Ошибка reputation_vote: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
# ============================================
# GRACEFUL SHUTDOWN
# ============================================
def shutdown_handler(signum, frame):
    """Обработчик сигналов для graceful shutdown"""
    logger.info("📴 Получен сигнал завершения, закрываем соединения...")
    if db_pool:
        db_pool.closeall()
        logger.info("✅ Пул соединений закрыт")
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

# ============================================
# ДЛЯ GUNICORN (PRODUCTION)
# ============================================
if db_pool is None:
    init_db_pool()

application = app
app = app

# ============================================
# ЗАПУСК ДЛЯ РАЗРАБОТКИ
# ============================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    
    print("=" * 50)
    print("🔥 PINGSTER BACKEND v2.1.0")
    print("=" * 50)
    print(f"🚀 Запуск на порту {port}")
    print(f"✅ Пул соединений: активен")
    print(f"✅ Кэш: Redis/Memory")
    print(f"✅ Репутация: включена")
    print("=" * 50)
    
    # Запускаем фоновые потоки
    bg_thread = threading.Thread(target=background_worker, daemon=True)
    bg_thread.start()
    
    clean_thread = threading.Thread(target=queue_cleaner_worker, daemon=True)
    clean_thread.start()
    
    online_thread = threading.Thread(target=online_cleaner_worker, daemon=True)
    online_thread.start()
    
    print("✅ Фоновые процессы запущены")
    
    # Запускаем Flask (только для разработки)
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
