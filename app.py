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

BUILD_VERSION = int(time.time())

sys.path.append('/app/.local/lib/python3.14/site-packages')
sys.path.append(os.path.expanduser('~/.local/lib/python3.14/site-packages'))

from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
import psycopg2
import psycopg2.extras

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Настройка CORS для всех доменов (для разработки)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Конфигурация из переменных окружения
DB_HOST = os.getenv("DB_HOST", "85.239.33.182")
DB_NAME = os.getenv("DB_NAME", "pingster_db")
DB_USER = os.getenv("DB_USER", "gen_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "{,@~:5my>jvOAj")
DB_PORT = os.getenv("DB_PORT", 5432)

# Токен бота
BOT_TOKEN = "8484054850:AAGwAcn1URrcKtikJKclqP8Z8oYs0wbIYY8"
BOT_USERNAME = "PingsterBot"

# ID форум-группы
FORUM_GROUP_ID = -1003753772298
RULES_TOPIC_ID = 5  # ID темы с правилами

# Секретный ключ для подписи ссылок (НИКОМУ НЕ ПОКАЗЫВАТЬ!)
SECRET_KEY = os.getenv("SECRET_KEY", "pingster_super_secret_key_2026_change_this")

# ============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================
def get_db():
    logger.debug("Подключение к базе данных...")
    try:
        return psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            connect_timeout=5
        )
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {e}")
        return None

def get_player_id(telegram_id):
    logger.debug(f"Поиск player_id по telegram_id: {telegram_id}")
    conn = get_db()
    if not conn:
        return None
    cursor = conn.cursor()
    cursor.execute("SELECT player_id FROM users WHERE telegram_id = %s", (telegram_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    if result:
        logger.debug(f"Найден player_id: {result[0]}")
        return result[0]
    logger.debug("Пользователь не найден")
    return None

def generate_player_id():
    return str(random.randint(10000000, 99999999))

def generate_random_nick():
    chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    return ''.join(random.choice(chars) for _ in range(6))

# Ранги
RANK_TO_VALUE = {
    'Silver 1': 1000, 'Silver 2': 1100, 'Silver 3': 1200, 'Silver 4': 1300,
    'Silver Elite': 1400, 'Gold Nova 1': 1500, 'Gold Nova 2': 1600,
    'Gold Nova 3': 1700, 'Gold Nova Master': 1800, 'Master Guardian 1': 1900,
    'Master Guardian 2': 2000, 'Master Guardian Elite': 2100,
    'Distinguished Master Guardian': 2200, 'Legendary Eagle': 2300,
    'Legendary Eagle Master': 2400, 'Supreme Master First Class': 2500,
    'Global Elite': 2600
}

RANK_LIST = [
    'Silver 1', 'Silver 2', 'Silver 3', 'Silver 4', 'Silver Elite',
    'Gold Nova 1', 'Gold Nova 2', 'Gold Nova 3', 'Gold Nova Master',
    'Master Guardian 1', 'Master Guardian 2', 'Master Guardian Elite',
    'Distinguished Master Guardian', 'Legendary Eagle', 'Legendary Eagle Master',
    'Supreme Master First Class', 'Global Elite'
]

def get_rank_index(rank):
    if rank in RANK_LIST:
        return RANK_LIST.index(rank)
    return 0

def calculate_score(player, candidate, mode):
    player_rating = int(player['rank']) if player['rank'] and player['rank'].isdigit() else 0
    cand_rating = int(candidate['rank']) if candidate['rank'] and candidate['rank'].isdigit() else 0
    
    if mode in ['faceit', 'premier']:
        rating_diff = abs(player_rating - cand_rating)
    else:
        player_idx = get_rank_index(player['rank'])
        cand_idx = get_rank_index(candidate['rank'])
        rating_diff = abs(player_idx - cand_idx) * 100
    
    age_diff = abs(player['age'] - candidate['age']) if candidate['age'] else 0
    return rating_diff + age_diff * 100

def get_range_buckets(mode, style, bucket):
    if mode == 'faceit':
        return bucket - 4, bucket + 4
    elif mode == 'premier':
        if style == 'tryhard':
            return bucket - 5, bucket + 5
        return None, None
    elif mode in ['mm prime', 'mm public']:
        if style == 'tryhard':
            return None, None
        return None, None
    return None, None

def filter_candidates_by_rank(candidates, player_rank, mode, style):
    if mode not in ['mm prime', 'mm public'] or style != 'tryhard':
        return candidates
    player_idx = get_rank_index(player_rank)
    filtered = []
    for cand in candidates:
        cand_idx = get_rank_index(cand['rank'])
        if abs(cand_idx - player_idx) <= 3:
            filtered.append(cand)
    return filtered

# ============================================
# ФУНКЦИИ ДЛЯ ЗАЩИЩЕННЫХ ССЫЛОК
# ============================================
def generate_match_token(match_id, user_id, expires_at):
    """
    Создаёт защищённый токен для доступа к матчу
    match_id - ID матча
    user_id - Telegram ID пользователя
    expires_at - время окончания матча (timestamp)
    """
    data = f"{match_id}:{user_id}:{expires_at}"
    
    # Создаём HMAC-SHA256 подпись
    signature = hmac.new(
        SECRET_KEY.encode(),
        data.encode(),
        hashlib.sha256
    ).hexdigest()[:16]  # Берём первые 16 символов для компактности
    
    token = f"{match_id}:{user_id}:{expires_at}:{signature}"
    
    # Кодируем в base64 для безопасной передачи в URL
    return base64.urlsafe_b64encode(token.encode()).decode().rstrip('=')

def verify_match_token(token):
    """
    Проверяет валидность токена
    Возвращает (match_id, user_id) если токен валиден, иначе (None, None)
    """
    try:
        # Декодируем из base64
        padded_token = token + '=' * (4 - len(token) % 4) if len(token) % 4 else token
        decoded = base64.urlsafe_b64decode(padded_token.encode()).decode()
        parts = decoded.split(':')
        
        if len(parts) != 4:
            logger.warning(f"Неверный формат токена: {decoded}")
            return None, None
            
        match_id, user_id, expires_at, signature = parts
        expires_at = int(expires_at)
        
        # Проверяем, не истёк ли матч
        current_time = int(time.time())
        if current_time > expires_at:
            logger.warning(f"Токен истёк: {current_time} > {expires_at}")
            return None, None
        
        # Проверяем подпись
        expected_data = f"{match_id}:{user_id}:{expires_at}"
        expected_signature = hmac.new(
            SECRET_KEY.encode(),
            expected_data.encode(),
            hashlib.sha256
        ).hexdigest()[:16]
        
        if hmac.compare_digest(signature, expected_signature):
            logger.info(f"✅ Токен валиден: match={match_id}, user={user_id}")
            return match_id, user_id
        else:
            logger.warning(f"❌ Неверная подпись токена: {signature} != {expected_signature}")
            return None, None
            
    except Exception as e:
        logger.error(f"Ошибка проверки токена: {e}")
        return None, None

def check_user_in_forum(user_id):
    """
    Проверяет, состоит ли пользователь в форуме
    """
    try:
        # Пытаемся получить информацию о пользователе в чате
        get_member_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember"
        params = {
            "chat_id": FORUM_GROUP_ID,
            "user_id": int(user_id)
        }
        response = requests.get(get_member_url, params=params, timeout=5)
        data = response.json()
        
        if data.get('ok'):
            status = data['result']['status']
            # Если пользователь состоит в чате (участник или администратор)
            return status in ['member', 'administrator', 'creator']
        return False
    except Exception as e:
        logger.error(f"Ошибка проверки участия в форуме: {e}")
        return False

# ============================================
# ЗАЩИТА ОТ ЧУЖИХ
# ============================================
def eject_intruder(user_id, topic_id):
    try:
        ban_url = f"https://api.telegram.org/bot{BOT_TOKEN}/banChatMember"
        ban_data = {
            "chat_id": FORUM_GROUP_ID,
            "user_id": int(user_id),
            "until_date": int(time.time()) + 30
        }
        requests.post(ban_url, json=ban_data)
        
        unban_url = f"https://api.telegram.org/bot{BOT_TOKEN}/unbanChatMember"
        unban_data = {
            "chat_id": FORUM_GROUP_ID,
            "user_id": int(user_id),
            "only_if_banned": True
        }
        requests.post(unban_url, json=unban_data)
        
        rules_link = f"https://t.me/c/{str(FORUM_GROUP_ID).replace('-100', '')}/{RULES_TOPIC_ID}"
        msg_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        msg_data = {
            "chat_id": int(user_id),
            "text": f"⛔ **Вы перешли в чужой матч!**\n\n"
                    f"Каждая тема в Pingster — личное пространство игроков.\n"
                    f"Пожалуйста, ознакомьтесь с [правилами сообщества]({rules_link}).\n\n"
                    f"*Ваше посещение автоматически зафиксировано.*",
            "parse_mode": "Markdown"
        }
        requests.post(msg_url, json=msg_data)
        
        log_intrusion(user_id, topic_id)
        return True
    except Exception as e:
        logger.error(f"Ошибка при выкидывании чужака {user_id}: {e}")
        return False

def log_intrusion(user_id, topic_id):
    try:
        conn = get_db()
        if not conn:
            return
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO intrusions (user_id, topic_id, created_at)
            VALUES (%s, %s, (NOW() AT TIME ZONE 'UTC'))
        """, (str(user_id), str(topic_id)))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"🚨 Зафиксировано нарушение: user={user_id}, topic={topic_id}")
    except Exception as e:
        logger.error(f"Ошибка логирования нарушения: {e}")

# ============================================
# УПРАВЛЕНИЕ ТЕМАМИ
# ============================================
def close_topic(topic_id):
    try:
        close_url = f"https://api.telegram.org/bot{BOT_TOKEN}/closeForumTopic"
        close_data = {
            "chat_id": FORUM_GROUP_ID,
            "message_thread_id": topic_id
        }
        response = requests.post(close_url, json=close_data)
        if response.json().get('ok'):
            logger.info(f"✅ Тема {topic_id} закрыта")
            conn = get_db()
            if not conn:
                return False
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE games SET status = 'closed' 
                WHERE telegram_chat_id = %s
            """, (str(topic_id),))
            conn.commit()
            cursor.close()
            conn.close()
            return True
        else:
            logger.error(f"❌ Ошибка закрытия темы {topic_id}: {response.json()}")
            return False
    except Exception as e:
        logger.error(f"Ошибка при закрытии темы: {e}")
        return False

def check_and_close_topics():
    try:
        conn = get_db()
        if not conn:
            return
        cursor = conn.cursor()
        cursor.execute("""
            SELECT telegram_chat_id FROM games 
            WHERE expires_at < (NOW() AT TIME ZONE 'UTC')
            AND status = 'active'
        """)
        topics_to_close = cursor.fetchall()
        cursor.close()
        conn.close()
        for topic in topics_to_close:
            close_topic(topic[0])
            time.sleep(0.5)
    except Exception as e:
        logger.error(f"Ошибка в фоновом процессе: {e}")

def background_worker():
    logger.info("🚀 Фоновый поток для закрытия тем запущен")
    while True:
        try:
            check_and_close_topics()
        except Exception as e:
            logger.error(f"Ошибка в цикле background_worker: {e}")
        time.sleep(60)

# ============================================
# НОВЫЙ ФОНОВЫЙ ПОТОК ДЛЯ ОЧИСТКИ ОЧЕРЕДИ
# ============================================
def clean_search_queue():
    """Удаляет истекшие записи из search_queue"""
    try:
        conn = get_db()
        if not conn:
            return
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM search_queue 
            WHERE expires_at < (NOW() AT TIME ZONE 'UTC')
            RETURNING id
        """)
        deleted = cursor.rowcount
        if deleted > 0:
            logger.info(f"🧹 Очищено {deleted} истекших записей из очереди поиска")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка при очистке очереди: {e}")

def queue_cleaner_worker():
    """Фоновый поток для очистки очереди каждые 5 секунд"""
    logger.info("🧹 Запущен фоновый поток для очистки очереди поиска")
    while True:
        try:
            clean_search_queue()
        except Exception as e:
            logger.error(f"Ошибка в цикле queue_cleaner_worker: {e}")
        time.sleep(5)

# ============================================
# ПОЛЬЗОВАТЕЛЬСКИЕ ЭНДПОИНТЫ
# ============================================
@app.route('/', methods=['GET'])
def home():
    return "Pingster backend is running!"

@app.route('/api', methods=['GET'])
def api_root():
    return jsonify({"message": "Pingster API is running!", "status": "ok"})

# ============================================
# ЭНДПОИНТЫ ДЛЯ ПРОФИЛЯ
# ============================================
@app.route('/api/profile/get', methods=['POST', 'OPTIONS'])
def get_profile():
    """Получить данные профиля пользователя"""
    if request.method == 'OPTIONS':
        return '', 200
        
    logger.info("POST /api/profile/get")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    telegram_id = request.json['telegram_id']
    
    conn = None
    cursor = None
    
    try:
        player_id = get_player_id(telegram_id)
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Получаем данные из таблицы profiles (ВКЛЮЧАЯ АВАТАРКУ)
        cursor.execute("""
            SELECT nick, age, steam_link, faceit_link, avatar, created_at
            FROM profiles 
            WHERE player_id = %s
        """, (player_id,))
        
        profile = cursor.fetchone()
        
        if not profile:
            return jsonify({"error": "Profile not found"}), 404
        
        logger.info(f"✅ Профиль загружен для player_id={player_id}")
        
        return jsonify({
            "status": "ok",
            "nick": profile['nick'],
            "age": profile['age'],
            "steam_link": profile['steam_link'],
            "faceit_link": profile['faceit_link'],
            "avatar": profile['avatar'],  # 👈 ДОБАВЛЕНА АВАТАРКА
            "created_at": profile['created_at'].isoformat() if profile['created_at'] else None
        })
        
    except Exception as e:
        logger.error(f"ОШИБКА в get_profile: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/profile/update', methods=['POST', 'OPTIONS'])
def update_profile():
    """Обновить данные профиля пользователя"""
    if request.method == 'OPTIONS':
        return '', 200
        
    logger.info("POST /api/profile/update")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    data = request.json
    telegram_id = data['telegram_id']
    
    conn = None
    cursor = None
    
    try:
        player_id = get_player_id(telegram_id)
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        
        cursor = conn.cursor()
        
        # Собираем поля для обновления
        update_fields = []
        update_values = []
        
        if 'nick' in data and data['nick']:
            update_fields.append("nick = %s")
            update_values.append(data['nick'])
        
        if 'age' in data:
            update_fields.append("age = %s")
            update_values.append(data['age'] if data['age'] else None)
        
        if 'steam_link' in data:
            update_fields.append("steam_link = %s")
            update_values.append(data['steam_link'] if data['steam_link'] else None)
        
        if 'faceit_link' in data:
            update_fields.append("faceit_link = %s")
            update_values.append(data['faceit_link'] if data['faceit_link'] else None)
        
        # 👇 ДОБАВЛЕНО ОБНОВЛЕНИЕ АВАТАРКИ
        if 'avatar' in data:
            update_fields.append("avatar = %s")
            update_values.append(data['avatar'] if data['avatar'] else None)
        
        if not update_fields:
            return jsonify({"error": "No fields to update"}), 400
        
        # Добавляем player_id в конец списка значений
        update_values.append(player_id)
        
        query = f"""
            UPDATE profiles 
            SET {', '.join(update_fields)}
            WHERE player_id = %s
            RETURNING nick, age, steam_link, faceit_link, avatar
        """
        
        cursor.execute(query, update_values)
        updated = cursor.fetchone()
        
        conn.commit()
        
        logger.info(f"✅ Профиль обновлен для player_id={player_id}")
        
        return jsonify({
            "status": "ok",
            "nick": updated[0] if updated else None,
            "age": updated[1] if updated else None,
            "steam_link": updated[2] if updated else None,
            "faceit_link": updated[3] if updated else None,
            "avatar": updated[4] if updated else None  # 👈 ВОЗВРАЩАЕМ АВАТАРКУ
        })
        
    except Exception as e:
        logger.error(f"ОШИБКА в update_profile: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ============================================
# ЭНДПОИНТЫ ДЛЯ АВАТАРКИ (НОВЫЕ)
# ============================================
@app.route('/api/profile/avatar/update', methods=['POST', 'OPTIONS'])
def update_avatar():
    """Обновить ссылку на аватарку пользователя"""
    if request.method == 'OPTIONS':
        return '', 200
        
    logger.info("POST /api/profile/avatar/update")
    
    if not request.json or 'telegram_id' not in request.json or 'avatar_url' not in request.json:
        return jsonify({"error": "Missing telegram_id or avatar_url"}), 400
    
    telegram_id = request.json['telegram_id']
    avatar_url = request.json['avatar_url']
    
    conn = None
    cursor = None
    
    try:
        player_id = get_player_id(telegram_id)
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        
        cursor = conn.cursor()
        
        # Обновляем аватарку в profiles
        cursor.execute("""
            UPDATE profiles 
            SET avatar = %s
            WHERE player_id = %s
            RETURNING avatar
        """, (avatar_url, player_id))
        
        updated = cursor.fetchone()
        conn.commit()
        
        logger.info(f"✅ Аватарка обновлена для player_id={player_id}")
        
        return jsonify({
            "status": "ok",
            "avatar_url": updated[0] if updated else None
        })
        
    except Exception as e:
        logger.error(f"ОШИБКА в update_avatar: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/profile/avatar/get', methods=['POST', 'OPTIONS'])
def get_avatar():
    """Получить ссылку на аватарку пользователя"""
    if request.method == 'OPTIONS':
        return '', 200
        
    logger.info("POST /api/profile/avatar/get")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    telegram_id = request.json['telegram_id']
    
    conn = None
    cursor = None
    
    try:
        player_id = get_player_id(telegram_id)
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT avatar FROM profiles WHERE player_id = %s
        """, (player_id,))
        
        result = cursor.fetchone()
        
        return jsonify({
            "status": "ok",
            "avatar_url": result[0] if result else None
        })
        
    except Exception as e:
        logger.error(f"ОШИБКА в get_avatar: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ============================================
# ЭНДПОИНТ ДЛЯ ИНИЦИАЛИЗАЦИИ ПОЛЬЗОВАТЕЛЯ
# ============================================
@app.route('/api/user/init', methods=['POST', 'OPTIONS'])
def user_init():
    if request.method == 'OPTIONS':
        return '', 200
        
    logger.info("POST /api/user/init")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    data = request.json
    telegram_id = data['telegram_id']
    username = data.get('username', '')
    
    conn = None
    cursor = None
    
    try:
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()
        
        # Проверяем, есть ли пользователь
        cursor.execute("SELECT player_id FROM users WHERE telegram_id = %s", (telegram_id,))
        existing = cursor.fetchone()
        
        if existing:
            player_id = existing[0]
            logger.info(f"Пользователь {telegram_id} уже существует, player_id={player_id}")
        else:
            # Создаём нового пользователя
            player_id = generate_player_id()
            nick = username if username else generate_random_nick()
            
            cursor.execute("""
                INSERT INTO users (telegram_id, player_id, created_at)
                VALUES (%s, %s, (NOW() AT TIME ZONE 'UTC'))
            """, (telegram_id, player_id))
            
            cursor.execute("""
                INSERT INTO profiles (player_id, nick, avatar, created_at)
                VALUES (%s, %s, %s, (NOW() AT TIME ZONE 'UTC'))
            """, (player_id, nick, None))  # 👇 АВАТАРКА ПО УМОЛЧАНИЮ NULL
            
            logger.info(f"Создан новый пользователь: {telegram_id} -> {player_id} (ник: {nick})")
        
        conn.commit()
        
        return jsonify({
            "status": "ok",
            "player_id": player_id
        })
        
    except Exception as e:
        logger.error(f"ОШИБКА в user_init: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ============================================
# ПОИСК И МЭТЧМЕЙКИНГ
# ============================================
@app.route('/api/search/start', methods=['POST', 'OPTIONS'])
def start_search():
    if request.method == 'OPTIONS':
        return '', 200
        
    logger.info("POST /api/search/start")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    data = request.json
    conn = None
    cursor = None
    
    try:
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()
        
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        logger.info(f"start_search для игрока {player_id}, режим {data.get('mode')}")
        
        if data.get('age') and (data['age'] < 16 or data['age'] > 100):
            return jsonify({"error": "Возраст должен быть от 16 до 100 лет"}), 400
        
        # Проверяем активный матч
        cursor.execute("""
            SELECT id FROM matches 
            WHERE (player1_id = %s OR player2_id = %s) 
            AND status IN ('pending', 'accepted')
            AND expires_at > (NOW() AT TIME ZONE 'UTC')
        """, (player_id, player_id))
        if cursor.fetchone():
            logger.warning(f"Игрок {player_id} уже в активном матче")
            return jsonify({"error": "Уже в матче"}), 400
        
        mode = data.get('mode', '').lower()
        rank_value = data.get('rating_value', '0')
        
        rating_number = 0
        if mode in ['faceit', 'premier']:
            try:
                rating_number = int(rank_value)
            except:
                rating_number = 0
        else:
            rating_number = RANK_TO_VALUE.get(rank_value, 1000)
        
        rating_bucket = rating_number // 100
        
        # Удаляем старые записи
        cursor.execute("DELETE FROM search_queue WHERE player_id = %s", (player_id,))
        
        # Создаем новую запись
        cursor.execute("""
            INSERT INTO search_queue 
            (player_id, mode, rank, rating_bucket, style, age, steam_link, faceit_link, comment, joined_at, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, (NOW() AT TIME ZONE 'UTC'), (NOW() AT TIME ZONE 'UTC' + INTERVAL '2 minutes'))
            RETURNING id
        """, (
            player_id, 
            mode, 
            str(rating_number), 
            rating_bucket,
            data.get('style'), 
            data.get('age'),
            data.get('steam_link'), 
            data.get('faceit_link'),
            data.get('comment')
        ))
        
        queue_id = cursor.fetchone()[0]
        logger.info(f"Игрок {player_id} добавлен в очередь (ID: {queue_id})")
        
        conn.commit()
        return jsonify({"status": "searching", "message": "В очереди"})
    
    except Exception as e:
        logger.error(f"ОШИБКА в start_search: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/match/check', methods=['POST', 'OPTIONS'])
def check_match():
    if request.method == 'OPTIONS':
        return '', 200
        
    logger.info("POST /api/match/check")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    data = request.json
    conn = None
    cursor = None
    
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        logger.info(f"check_match для игрока {player_id}")
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # === ШАГ 0: Проверяем существующий матч ===
        cursor.execute("""
            SELECT id, player1_id, player2_id, expires_at, status
            FROM matches
            WHERE player1_id = %s OR player2_id = %s
            ORDER BY id DESC
            LIMIT 1
        """, (player_id, player_id))
        
        existing_match = cursor.fetchone()
        
        if existing_match:
            if existing_match['status'] in ['pending', 'accepted'] and existing_match['expires_at'] > datetime.utcnow():
                logger.info(f"Найден активный матч ID={existing_match['id']}")
                
                other_id = existing_match['player2_id'] if existing_match['player1_id'] == player_id else existing_match['player1_id']
                
                cursor.execute("""
                    SELECT nick, age, steam_link, faceit_link, avatar
                    FROM profiles WHERE player_id = %s
                """, (other_id,))
                
                profile = cursor.fetchone()
                
                if profile:
                    opponent = {
                        "player_id": other_id,
                        "nick": profile['nick'],
                        "age": profile['age'],
                        "style": "fan",
                        "rating": "0",
                        "steam_link": profile['steam_link'] or "Не указана",
                        "faceit_link": profile['faceit_link'] or "Не указана",
                        "avatar": profile['avatar'],  # 👈 ДОБАВЛЕНА АВАТАРКА
                        "comment": "Нет комментария"
                    }
                    
                    return jsonify({
                        "match_found": True,
                        "match_id": existing_match['id'],
                        "opponent": opponent,
                        "expires_at": existing_match['expires_at'].isoformat() + "Z",
                        "server_time": datetime.utcnow().isoformat() + "Z"
                    })
        
        # === ШАГ 1: Получаем данные текущего игрока из очереди ===
        cursor.execute("""
            SELECT * FROM search_queue 
            WHERE player_id = %s AND expires_at > (NOW() AT TIME ZONE 'UTC')
            ORDER BY joined_at DESC
            LIMIT 1
            FOR UPDATE
        """, (player_id,))
        
        current = cursor.fetchone()
        
        if not current:
            logger.info(f"Игрок {player_id} не в очереди")
            return jsonify({"match_found": False})
        
        current_mode = current['mode']
        current_style = current['style']
        current_rank = current['rank']
        current_bucket = current['rating_bucket']
        
        min_bucket, max_bucket = get_range_buckets(current_mode, current_style, current_bucket)
        
        query = """
            SELECT 
                sq.*,
                p.nick,
                p.steam_link,
                p.faceit_link,
                p.avatar
            FROM search_queue sq
            JOIN profiles p ON sq.player_id = p.player_id
            WHERE sq.mode = %s 
            AND sq.player_id != %s
            AND sq.expires_at > (NOW() AT TIME ZONE 'UTC')
        """
        params = [current_mode, player_id]
        
        if min_bucket is not None and max_bucket is not None:
            query += " AND sq.rating_bucket BETWEEN %s AND %s"
            params.extend([min_bucket, max_bucket])
        
        query += " FOR UPDATE SKIP LOCKED"
        
        cursor.execute(query, params)
        candidates = cursor.fetchall()
        
        if not candidates:
            return jsonify({"match_found": False})
        
        same_style = []
        other_style = []
        
        for cand in candidates:
            if cand['style'] == current_style:
                same_style.append(cand)
            else:
                other_style.append(cand)
        
        same_style.sort(key=lambda x: calculate_score(current, x, current_mode))
        other_style.sort(key=lambda x: calculate_score(current, x, current_mode))
        
        best_candidate = (same_style + other_style)[0]
        
        cursor.execute("""
            SELECT id FROM search_queue 
            WHERE player_id = %s AND expires_at > (NOW() AT TIME ZONE 'UTC')
            FOR UPDATE
        """, (best_candidate['player_id'],))
        
        if not cursor.fetchone():
            return jsonify({"match_found": False})
        
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=30)
        
        cursor.execute("""
            INSERT INTO matches 
            (player1_id, player2_id, mode, created_at, expires_at, status)
            VALUES (%s, %s, %s, (NOW() AT TIME ZONE 'UTC'), %s, 'pending')
            RETURNING id
        """, (player_id, best_candidate['player_id'], current_mode, expires_at))
        
        match_id = cursor.fetchone()['id']
        
        cursor.execute("""
            DELETE FROM search_queue 
            WHERE player_id IN (%s, %s)
        """, (player_id, best_candidate['player_id']))
        
        conn.commit()
        
        opponent = {
            "player_id": best_candidate['player_id'],
            "nick": best_candidate['nick'],
            "age": best_candidate['age'],
            "style": best_candidate['style'],
            "rating": best_candidate['rank'],
            "steam_link": best_candidate['steam_link'] or "Не указана",
            "faceit_link": best_candidate['faceit_link'] or "Не указана",
            "avatar": best_candidate['avatar'],  # 👈 ДОБАВЛЕНА АВАТАРКА
            "comment": best_candidate['comment'] or "Нет комментария"
        }
        
        return jsonify({
            "match_found": True,
            "match_id": match_id,
            "opponent": opponent,
            "expires_at": expires_at.isoformat() + "Z",
            "server_time": now.isoformat() + "Z"
        })
    
    except Exception as e:
        logger.error(f"ОШИБКА в check_match: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/match/status/<int:match_id>', methods=['GET', 'OPTIONS'])
def match_status(match_id):
    if request.method == 'OPTIONS':
        return '', 200
        
    logger.info(f"GET /api/match/status/{match_id}")
    
    conn = None
    cursor = None
    
    try:
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cursor.execute("""
            SELECT status, player1_response, player2_response, expires_at
            FROM matches
            WHERE id = %s
        """, (match_id,))
        
        match = cursor.fetchone()
        
        if not match:
            return jsonify({"status": "not_found"})
        
        if match['player1_response'] == 'accept' and match['player2_response'] == 'accept':
            return jsonify({
                "status": "both_accepted",
                "expires_at": match['expires_at'].isoformat() + "Z" if match['expires_at'] else None
            })
        
        return jsonify({
            "status": match['status'],
            "expires_at": match['expires_at'].isoformat() + "Z" if match['expires_at'] else None
        })
    
    except Exception as e:
        logger.error(f"ОШИБКА: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/match/respond', methods=['POST', 'OPTIONS'])
def respond_match():
    if request.method == 'OPTIONS':
        return '', 200
        
    logger.info("POST /api/match/respond")
    
    if not request.json or 'telegram_id' not in request.json or 'match_id' not in request.json or 'response' not in request.json:
        return jsonify({"error": "Missing required fields"}), 400
    
    data = request.json
    conn = None
    cursor = None
    
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        logger.info(f"respond_match: игрок {player_id}, матч {data['match_id']}, ответ {data['response']}")
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cursor.execute("""
            SELECT player1_id, player2_id, player1_response, player2_response, expires_at, status
            FROM matches WHERE id = %s FOR UPDATE
        """, (data['match_id'],))
        
        match = cursor.fetchone()
        
        if not match:
            return jsonify({"error": "Match not found"}), 404
        
        current_time = datetime.utcnow()
        if match['expires_at'] and current_time > match['expires_at']:
            cursor.execute("UPDATE matches SET status = 'expired' WHERE id = %s", (data['match_id'],))
            conn.commit()
            return jsonify({"status": "expired", "message": "Время истекло"})
        
        if str(match['player1_id']) == str(player_id):
            if match['player1_response'] is not None:
                return jsonify({"status": "already_responded"})
            cursor.execute("UPDATE matches SET player1_response = %s WHERE id = %s",
                          (data['response'], data['match_id']))
        elif str(match['player2_id']) == str(player_id):
            if match['player2_response'] is not None:
                return jsonify({"status": "already_responded"})
            cursor.execute("UPDATE matches SET player2_response = %s WHERE id = %s",
                          (data['response'], data['match_id']))
        else:
            return jsonify({"error": "User not in this match"}), 403
        
        cursor.execute("SELECT player1_response, player2_response, expires_at FROM matches WHERE id = %s", (data['match_id'],))
        responses = cursor.fetchone()
        
        if responses['player1_response'] == 'accept' and responses['player2_response'] == 'accept':
            logger.info("Оба приняли матч")
            cursor.execute("UPDATE matches SET status = 'accepted' WHERE id = %s", (data['match_id'],))
            conn.commit()
            return jsonify({"status": "accepted", "both_accepted": True})
        
        elif responses['player1_response'] == 'reject' or responses['player2_response'] == 'reject':
            logger.info("Матч отклонен")
            cursor.execute("DELETE FROM matches WHERE id = %s", (data['match_id'],))
            
            now = datetime.utcnow()
            expires_at = now + timedelta(minutes=2)
            
            cursor.execute("""
                INSERT INTO search_queue 
                (player_id, mode, rank, rating_bucket, style, age, steam_link, faceit_link, comment, joined_at, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (match['player1_id'], "faceit", "1500", 15, "fan", 21, "", "", "", now, expires_at))
            
            cursor.execute("""
                INSERT INTO search_queue 
                (player_id, mode, rank, rating_bucket, style, age, steam_link, faceit_link, comment, joined_at, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (match['player2_id'], "faceit", "1500", 15, "fan", 21, "", "", "", now, expires_at))
            
            conn.commit()
            return jsonify({"status": "rejected", "both_accepted": False})
        
        else:
            conn.commit()
            time_left = 0
            if responses['expires_at']:
                time_left = max(0, int((responses['expires_at'] - datetime.utcnow()).total_seconds()))
            return jsonify({"status": "waiting", "both_accepted": False, "time_left": time_left})
    
    except Exception as e:
        logger.error(f"ОШИБКА в respond_match: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/search/stop', methods=['POST', 'OPTIONS'])
def stop_search():
    if request.method == 'OPTIONS':
        return '', 200
        
    logger.info("POST /api/search/stop")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    data = request.json
    conn = None
    cursor = None
    
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        logger.info(f"stop_search для игрока {player_id}")
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM search_queue WHERE player_id = %s", (player_id,))
        deleted = cursor.rowcount
        conn.commit()
        
        return jsonify({"status": "stopped", "deleted": deleted})
    
    except Exception as e:
        logger.error(f"ОШИБКА: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ============================================
# ЭНДПОИНТЫ ДЛЯ ИСТОРИИ МАТЧЕЙ
# ============================================
@app.route('/api/my-matches', methods=['POST', 'OPTIONS'])
def my_matches():
    if request.method == 'OPTIONS':
        return '', 200
        
    logger.info("POST /api/my-matches")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    telegram_id = request.json['telegram_id']
    player_id = get_player_id(telegram_id)
    
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    conn = None
    cursor = None
    
    try:
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
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
        logger.error(f"ОШИБКА в my_matches: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/active-match', methods=['POST', 'OPTIONS'])
def active_match():
    if request.method == 'OPTIONS':
        return '', 200
        
    logger.info("POST /api/active-match")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    telegram_id = request.json['telegram_id']
    player_id = get_player_id(telegram_id)
    
    if not player_id:
        return jsonify({"error": "User not found"}), 404
    
    conn = None
    cursor = None
    
    try:
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cursor.execute("""
            SELECT m.id, p1.nick as player1, p2.nick as player2,
                   g.telegram_chat_link
            FROM matches m
            JOIN games g ON m.id = g.match_id
            JOIN profiles p1 ON m.player1_id = p1.player_id
            JOIN profiles p2 ON m.player2_id = p2.player_id
            WHERE (m.player1_id = %s OR m.player2_id = %s)
            AND g.status = 'active'
            AND g.expires_at > (NOW() AT TIME ZONE 'UTC')
            ORDER BY m.id DESC
            LIMIT 1
        """, (player_id, player_id))
        
        match = cursor.fetchone()
        
        if match:
            return jsonify({
                "active": True,
                "match_id": match['id'],
                "chat_link": match['telegram_chat_link']
            })
        else:
            return jsonify({"active": False})
        
    except Exception as e:
        logger.error(f"ОШИБКА в active_match: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ============================================
# ЭНДПОИНТ СОЗДАНИЯ ИГРЫ (С ЗАЩИЩЕННЫМИ ССЫЛКАМИ)
# ============================================
@app.route('/api/game/create', methods=['POST', 'OPTIONS'])
def create_game():
    if request.method == 'OPTIONS':
        return '', 200
        
    logger.info("POST /api/game/create")
    
    if not request.json or 'match_id' not in request.json:
        return jsonify({"error": "Missing match_id"}), 400
    
    data = request.json
    match_id = data['match_id']
    conn = None
    cursor = None
    
    try:
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()
        
        logger.info(f"create_game для match_id={match_id}")
        
        # Проверяем существующую игру
        cursor.execute("""
            SELECT id, telegram_chat_link FROM games WHERE match_id = %s
        """, (match_id,))
        
        existing_game = cursor.fetchone()
        if existing_game:
            logger.info(f"Игра для match_id={match_id} уже существует")
            return jsonify({
                "status": "ok",
                "game_id": existing_game[0],
                "chat_link": existing_game[1],
                "already_exists": True
            })
        
        # Получаем матч
        for i in range(5):
            cursor.execute("""
                SELECT player1_id, player2_id, mode
                FROM matches
                WHERE id = %s AND status = 'accepted'
            """, (match_id,))
            
            match = cursor.fetchone()
            if match:
                logger.info(f"Матч найден после {i+1} попытки")
                break
                
            logger.info(f"Матч еще не в статусе accepted, ждем... попытка {i+1}")
            time.sleep(0.3)
        
        if not match:
            logger.error(f"Match not found or not accepted after retries: {match_id}")
            return jsonify({"error": "Match not found or not accepted"}), 404
        
        player1_id, player2_id, mode = match
        
        # Получаем telegram_id игроков
        cursor.execute("SELECT u.telegram_id, p.nick FROM users u JOIN profiles p ON u.player_id = p.player_id WHERE u.player_id = %s", (player1_id,))
        user1 = cursor.fetchone()
        
        cursor.execute("SELECT u.telegram_id, p.nick FROM users u JOIN profiles p ON u.player_id = p.player_id WHERE u.player_id = %s", (player2_id,))
        user2 = cursor.fetchone()
        
        if not user1 or not user2:
            logger.error("Users not found")
            return jsonify({"error": "Users not found"}), 404
        
        telegram_id1, nick1 = user1
        telegram_id2, nick2 = user2
        
        # Создаем тему в Telegram
        FORUM_ID = FORUM_GROUP_ID
        
        create_topic_url = f"https://api.telegram.org/bot{BOT_TOKEN}/createForumTopic"
        topic_data = {
            "chat_id": FORUM_ID,
            "name": f"#{match_id} | {nick1} & {nick2}",
            "icon_color": 0x6FB9F0
        }
        
        try:
            topic_response = requests.post(create_topic_url, json=topic_data, timeout=10)
            
            if topic_response.status_code != 200:
                logger.error(f"Telegram API HTTP error: {topic_response.status_code}")
                return jsonify({"error": "telegram_api_error"}), 500
                
            topic_result = topic_response.json()
            
            if not topic_result.get('ok'):
                logger.error(f"Failed to create forum topic: {topic_result}")
                return jsonify({"error": "topic_create_failed"}), 500
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Telegram API request failed: {e}")
            return jsonify({"error": "telegram_api_error"}), 500
        
        topic_id = topic_result['result']['message_thread_id']
        
        clean_chat_id = str(FORUM_ID).replace('-100', '')
        
        # Время окончания матча (30 минут)
        expires_at_dt = datetime.utcnow() + timedelta(minutes=30)
        expires_at_ts = int(expires_at_dt.timestamp())
        
        # Создаём защищённые ссылки для каждого игрока
        token1 = generate_match_token(match_id, telegram_id1, expires_at_ts)
        token2 = generate_match_token(match_id, telegram_id2, expires_at_ts)
        
        # Ссылки с токенами
        secure_link1 = f"https://t.me/c/{clean_chat_id}/{topic_id}?start={token1}"
        secure_link2 = f"https://t.me/c/{clean_chat_id}/{topic_id}?start={token2}"
        
        # Обычная ссылка для всех остальных
        public_link = f"https://t.me/c/{clean_chat_id}/{topic_id}"
        
        time.sleep(0.1)
        
        try:
            # Приветственное сообщение в теме
            welcome_text = f"""🎯 **МАТЧ #{match_id} СОЗДАН!**

Привет, {nick1} и {nick2}!
Это ваш временный чат для игры.

⏳ Чат будет активен 30 минут, затем закроется.
🔒 Только вы двое имеете доступ к чату."""
            
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": FORUM_ID,
                    "message_thread_id": topic_id,
                    "text": welcome_text,
                    "parse_mode": "Markdown"
                },
                timeout=5
            )
        except Exception as e:
            logger.error(f"Ошибка отправки приветствия: {e}")
        
        expires_at = expires_at_dt
        
        cursor.execute("""
            INSERT INTO games (match_id, player1_id, player2_id, telegram_chat_id, telegram_chat_link, status, created_at, expires_at)
            VALUES (%s, %s, %s, %s, %s, 'active', (NOW() AT TIME ZONE 'UTC'), %s)
            ON CONFLICT (match_id) DO NOTHING
            RETURNING id
        """, (match_id, player1_id, player2_id, topic_id, public_link, expires_at))
        
        result = cursor.fetchone()
        
        if not result:
            cursor.execute("""
                SELECT id, telegram_chat_link FROM games WHERE match_id = %s
            """, (match_id,))
            existing = cursor.fetchone()
            
            if existing:
                conn.commit()
                return jsonify({
                    "status": "ok",
                    "game_id": existing[0],
                    "chat_link": existing[1],
                    "already_exists": True
                })
            else:
                return jsonify({"error": "Game creation failed"}), 500
        
        game_id = result[0]
        conn.commit()
        
        # Отправляем каждому игроку ЕГО защищённую ссылку
        try:
            # Первому игроку
            msg_data1 = {
                "chat_id": int(telegram_id1),
                "text": f"✅ **Матч #{match_id} создан!**\n\n"
                        f"Соперник: {nick2}\n\n"
                        f"🔗 [Перейти в чат]({secure_link1})\n\n"
                        f"⚠️ Ссылка работает только для вас и только пока активен матч.",
                "parse_mode": "Markdown"
            }
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json=msg_data1,
                timeout=5
            )
            
            time.sleep(0.1)
            
            # Второму игроку
            msg_data2 = {
                "chat_id": int(telegram_id2),
                "text": f"✅ **Матч #{match_id} создан!**\n\n"
                        f"Соперник: {nick1}\n\n"
                        f"🔗 [Перейти в чат]({secure_link2})\n\n"
                        f"⚠️ Ссылка работает только для вас и только пока активен матч.",
                "parse_mode": "Markdown"
            }
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json=msg_data2,
                timeout=5
            )
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления в ЛС: {e}")
        
        return jsonify({
            "status": "ok",
            "game_id": game_id,
            "chat_link": public_link,
            "has_secure_links": True
        })
    
    except Exception as e:
        logger.error(f"ОШИБКА в create_game: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ============================================
# ЭНДПОИНТ ДЛЯ ПРОВЕРКИ ТОКЕНА
# ============================================
@app.route('/api/verify-token', methods=['POST', 'OPTIONS'])
def verify_token():
    """Проверяет валидность токена (может вызывать бот)"""
    if request.method == 'OPTIONS':
        return '', 200
        
    logger.info("POST /api/verify-token")
    
    if not request.json or 'token' not in request.json or 'user_id' not in request.json:
        return jsonify({"error": "Missing token or user_id"}), 400
    
    data = request.json
    token = data['token']
    user_id = str(data['user_id'])
    
    match_id, token_user_id = verify_match_token(token)
    
    if match_id and token_user_id == user_id:
        return jsonify({
            "valid": True,
            "match_id": match_id
        })
    else:
        return jsonify({
            "valid": False
        })

# ============================================
# ЗАПУСК
# ============================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    
    print("🔥 PINGSTER BACKEND - ПУБЛИЧНЫЙ ФОРУМ + ЗАЩИЩЕННЫЕ ССЫЛКИ")
    print("✅ Форум теперь публичный — любой может зайти и смотреть")
    print("✅ У игроков персональные защищённые ссылки (подделать нельзя)")
    print("✅ Проверка по HMAC-SHA256 + время жизни")
    print("✅ Эндпоинты для профиля (/api/profile/get и /api/profile/update)")
    print("✅ Эндпоинты для аватарок (/api/profile/avatar/*)")
    print(f"\n🚀 Сервер будет запущен на порту {port}")
    
    # Запускаем фоновые потоки
    try:
        thread = threading.Thread(target=background_worker, daemon=True)
        thread.start()
        print("✅ Фоновый процесс для закрытия тем запущен")
    except Exception as e:
        print(f"❌ Ошибка запуска фонового процесса для тем: {e}")
    
    try:
        queue_thread = threading.Thread(target=queue_cleaner_worker, daemon=True)
        queue_thread.start()
        print("✅ Фоновый процесс для очистки очереди поиска запущен")
    except Exception as e:
        print(f"❌ Ошибка запуска фонового процесса для очереди: {e}")
    
    app.run(host='0.0.0.0', port=port, debug=False)
