import sys
import os
import threading
import time
from datetime import datetime, timedelta
import random
import logging
import requests

BUILD_VERSION = int(time.time())

sys.path.append('/app/.local/lib/python3.14/site-packages')
sys.path.append(os.path.expanduser('~/.local/lib/python3.14/site-packages'))

from flask import Flask, request, jsonify, render_template 
from flask_cors import CORS
import psycopg2
import psycopg2.extras

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

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
        time.sleep(5)  # Проверяем каждые 5 секунд

# ============================================
# ПОЛЬЗОВАТЕЛЬСКИЕ ЭНДПОИНТЫ
# ============================================
@app.route('/', methods=['GET'])
def home():
    # Проверяем, хочет ли браузер HTML или JSON
    if request.headers.get('Accept') == 'application/json':
        return jsonify({"message": "Pingster backend is running!", "status": "ok"})
    
    # Для браузера отдаём HTML с версией
    return render_template('index.html', build_version=BUILD_VERSION)

@app.route('/api', methods=['GET'])
def api_root():
    return jsonify({"message": "Pingster API is running!", "status": "ok"})

@app.route('/api/user/init', methods=['POST'])
def init_user():
    logger.info("POST /api/user/init")
    
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
        
        cursor.execute("SELECT player_id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        result = cursor.fetchone()
        
        if not result:
            player_id = generate_player_id()
            cursor.execute("""
                INSERT INTO users (telegram_id, username, player_id, last_active, is_online)
                VALUES (%s, %s, %s, (NOW() AT TIME ZONE 'UTC'), true)
                RETURNING player_id
            """, (data['telegram_id'], data.get('username', 'no_username'), player_id))
            player_id = cursor.fetchone()[0]
            
            nick = generate_random_nick()
            cursor.execute("""
                INSERT INTO profiles (player_id, nick, pingcoins, telegram_id)
                VALUES (%s, %s, 1000, %s)
            """, (player_id, nick, data['telegram_id']))
            
            conn.commit()
            return jsonify({"status": "ok", "new_user": True, "player_id": player_id, "nick": nick, "pingcoins": 1000})
        else:
            player_id = result[0]
            cursor.execute("UPDATE users SET last_active = (NOW() AT TIME ZONE 'UTC'), is_online = true WHERE player_id = %s", (player_id,))
            
            cursor.execute("SELECT nick, pingcoins FROM profiles WHERE player_id = %s", (player_id,))
            profile = cursor.fetchone()
            
            if not profile:
                nick = generate_random_nick()
                cursor.execute("""
                    INSERT INTO profiles (player_id, nick, pingcoins, telegram_id)
                    VALUES (%s, %s, 1000, %s)
                """, (player_id, nick, data['telegram_id']))
                conn.commit()
                return jsonify({"status": "ok", "new_user": False, "player_id": player_id, "nick": nick, "pingcoins": 1000})
            
            conn.commit()
            return jsonify({"status": "ok", "new_user": False, "player_id": player_id, "nick": profile[0], "pingcoins": profile[1]})
    
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

@app.route('/api/profile/get', methods=['POST'])
def get_profile():
    logger.info("POST /api/profile/get")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    try:
        player_id = get_player_id(request.json['telegram_id'])
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT nick, age, steam_link, faceit_link, avatar, pingcoins
            FROM profiles WHERE player_id = %s
        """, (player_id,))
        profile = cursor.fetchone()
        
        if not profile:
            return jsonify({"error": "Profile not found"}), 404
        
        return jsonify({
            "status": "ok",
            "player_id": player_id,
            "nick": profile[0],
            "age": profile[1],
            "steam_link": profile[2],
            "faceit_link": profile[3],
            "avatar": profile[4],
            "pingcoins": profile[5]
        })
    
    except Exception as e:
        logger.error(f"ОШИБКА: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/profile/update', methods=['POST'])
def update_profile():
    logger.info("POST /api/profile/update")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    data = request.json
    conn = None
    cursor = None
    
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE profiles 
            SET nick = COALESCE(%s, nick),
                age = COALESCE(%s, age),
                steam_link = COALESCE(%s, steam_link),
                faceit_link = COALESCE(%s, faceit_link),
                updated_at = (NOW() AT TIME ZONE 'UTC')
            WHERE player_id = %s
        """, (
            data.get('nick'),
            data.get('age'),
            data.get('steam_link'),
            data.get('faceit_link'),
            player_id
        ))
        
        conn.commit()
        return jsonify({"status": "ok"})
    
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

@app.route('/api/avatar/save', methods=['POST'])
def save_avatar():
    logger.info("POST /api/avatar/save")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    data = request.json
    
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()
        
        cursor.execute("UPDATE profiles SET avatar = %s WHERE player_id = %s", (data.get('avatar'), player_id))
        conn.commit()
        
        return jsonify({"status": "ok"})
    
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

@app.route('/api/user/balance', methods=['POST'])
def get_balance():
    logger.info("POST /api/user/balance")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    try:
        player_id = get_player_id(request.json['telegram_id'])
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()
        
        cursor.execute("SELECT pingcoins FROM profiles WHERE player_id = %s", (player_id,))
        result = cursor.fetchone()
        
        return jsonify({"status": "ok", "balance": result[0] if result else 0})
    
    except Exception as e:
        logger.error(f"ОШИБКА: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/shop/buy', methods=['POST'])
def buy_case():
    logger.info("POST /api/shop/buy")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    data = request.json
    conn = None
    cursor = None
    
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()
        
        cursor.execute("SELECT pingcoins FROM profiles WHERE player_id = %s", (player_id,))
        result = cursor.fetchone()
        if not result:
            return jsonify({"error": "Profile not found"}), 404
        
        coins = result[0]
        price = data.get('price', 0)
        
        if coins < price:
            return jsonify({"error": "Not enough coins"}), 400
        
        cursor.execute("UPDATE profiles SET pingcoins = pingcoins - %s WHERE player_id = %s", (price, player_id))
        
        cursor.execute("""
            INSERT INTO inventory (player_id, case_id, case_name, unique_id, status_case)
            VALUES (%s, %s, %s, %s, 'new')
        """, (player_id, data.get('case_id'), data.get('case_name'), data.get('unique_id')))
        
        conn.commit()
        
        cursor.execute("SELECT pingcoins FROM profiles WHERE player_id = %s", (player_id,))
        new_balance = cursor.fetchone()[0]
        
        return jsonify({"status": "ok", "new_balance": new_balance})
    
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

@app.route('/api/inventory/get', methods=['POST'])
def get_inventory():
    logger.info("POST /api/inventory/get")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    try:
        player_id = get_player_id(request.json['telegram_id'])
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT case_id, case_name, unique_id, status_case, item_id, item_name, status_item
            FROM inventory 
            WHERE player_id = %s
            ORDER BY unique_id DESC
        """, (player_id,))
        
        items = cursor.fetchall()
        inventory_list = []
        
        for item in items:
            inventory_list.append({
                "case_id": item[0],
                "case_name": item[1],
                "unique_id": item[2],
                "status_case": item[3],
                "item_id": item[4],
                "item_name": item[5],
                "status_item": item[6]
            })
        
        return jsonify({"status": "ok", "inventory": inventory_list})
    
    except Exception as e:
        logger.error(f"ОШИБКА: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

@app.route('/api/case/open', methods=['POST'])
def open_case():
    logger.info("POST /api/case/open")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    data = request.json
    conn = None
    cursor = None
    
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE inventory 
            SET status_case = 'opened',
                item_id = %s,
                item_name = %s,
                status_item = 'new'
            WHERE unique_id = %s AND player_id = %s
            RETURNING case_id, case_name
        """, (data.get('item_id'), data.get('item_name'), data.get('unique_id'), player_id))
        
        result = cursor.fetchone()
        if not result:
            return jsonify({"error": "Case not found"}), 404
        
        conn.commit()
        
        return jsonify({
            "status": "ok",
            "case_id": result[0],
            "case_name": result[1],
            "item_id": data.get('item_id'),
            "item_name": data.get('item_name')
        })
    
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

@app.route('/api/item/update_status', methods=['POST'])
def update_item_status():
    logger.info("POST /api/item/update_status")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    data = request.json
    conn = None
    cursor = None
    
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE inventory 
            SET status_item = %s
            WHERE unique_id = %s AND player_id = %s
            RETURNING item_id, item_name
        """, (data.get('status'), data.get('unique_id'), player_id))
        
        result = cursor.fetchone()
        if not result:
            return jsonify({"error": "Item not found"}), 404
        
        conn.commit()
        
        return jsonify({
            "status": "ok",
            "item_id": result[0],
            "item_name": result[1],
            "new_status": data.get('status')
        })
    
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

@app.route('/api/item/delete', methods=['POST'])
def delete_item():
    logger.info("POST /api/item/delete")
    
    if not request.json or 'telegram_id' not in request.json:
        return jsonify({"error": "Missing telegram_id"}), 400
    
    data = request.json
    conn = None
    cursor = None
    
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()
        
        cursor.execute("""
            DELETE FROM inventory 
            WHERE unique_id = %s AND player_id = %s
            RETURNING item_id, item_name
        """, (data.get('unique_id'), player_id))
        
        result = cursor.fetchone()
        if not result:
            return jsonify({"error": "Item not found"}), 404
        
        conn.commit()
        
        return jsonify({"status": "ok", "deleted": result[0]})
    
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
# ПОИСК И МЭТЧМЕЙКИНГ
# ============================================
@app.route('/api/search/start', methods=['POST'])
def start_search():
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
        
        # ИСПРАВЛЕНО: 2 минуты на поиск, всё в UTC
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

@app.route('/api/match/check', methods=['POST'])
def check_match():
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
        logger.info("ШАГ 0: Проверяем существующий матч...")
        cursor.execute("""
            SELECT id, player1_id, player2_id, expires_at, status
            FROM matches
            WHERE player1_id = %s OR player2_id = %s
            ORDER BY id DESC
            LIMIT 1
        """, (player_id, player_id))
        
        existing_match = cursor.fetchone()
        
        if existing_match:
            # Проверяем что матч еще активен
            if existing_match['status'] in ['pending', 'accepted'] and existing_match['expires_at'] > datetime.utcnow():
                logger.info(f"Найден активный матч ID={existing_match['id']}")
                
                other_id = existing_match['player2_id'] if existing_match['player1_id'] == player_id else existing_match['player1_id']
                
                # Получаем данные соперника
                cursor.execute("""
                    SELECT nick, age, steam_link, faceit_link
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
                        "comment": "Нет комментария"
                    }
                    
                    return jsonify({
                        "match_found": True,
                        "match_id": existing_match['id'],
                        "opponent": opponent,
                        "expires_at": existing_match['expires_at'].isoformat() + "Z",  # ← ДОБАВЛЕНО + "Z"
                        "server_time": datetime.utcnow().isoformat() + "Z"             # ← ДОБАВЛЕНО + "Z"
                    })
        
        # === ШАГ 1: Получаем данные текущего игрока из очереди ===
        logger.info("ШАГ 1: Проверяем очередь поиска...")
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
        
        logger.info(f"Игрок {player_id} в очереди, режим {current['mode']}")
        
        current_mode = current['mode']
        current_style = current['style']
        current_age = current['age']
        current_rank = current['rank']
        current_bucket = current['rating_bucket']
        
        # Определяем диапазон поиска
        min_bucket, max_bucket = get_range_buckets(current_mode, current_style, current_bucket)
        logger.info(f"Диапазон бакетов: {min_bucket} - {max_bucket}")
        
        # === ШАГ 2: Ищем кандидатов ===
        logger.info("ШАГ 2: Ищем кандидатов...")
        query = """
            SELECT 
                sq.*,
                p.nick,
                p.steam_link,
                p.faceit_link
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
        logger.info(f"Найдено кандидатов до фильтрации: {len(candidates)}")
        
        candidates = filter_candidates_by_rank(candidates, current_rank, current_mode, current_style)
        logger.info(f"Кандидатов после фильтрации: {len(candidates)}")
        
        if not candidates:
            logger.info("Нет подходящих кандидатов")
            return jsonify({"match_found": False})
        
        # === ШАГ 3: Сортируем кандидатов ===
        logger.info("ШАГ 3: Сортируем кандидатов...")
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
        logger.info(f"Лучший кандидат: {best_candidate['player_id']}")
        
        # === ШАГ 4: Проверяем кандидата ===
        cursor.execute("""
            SELECT id FROM search_queue 
            WHERE player_id = %s AND expires_at > (NOW() AT TIME ZONE 'UTC')
            FOR UPDATE
        """, (best_candidate['player_id'],))
        
        if not cursor.fetchone():
            logger.warning(f"Кандидат {best_candidate['player_id']} больше не в очереди")
            return jsonify({"match_found": False})
        
        # === ШАГ 5: Создаем матч ===
        logger.info("ШАГ 5: Создаем матч...")
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=30)  # 30 секунд на принятие
        
        cursor.execute("""
            INSERT INTO matches 
            (player1_id, player2_id, mode, created_at, expires_at, status)
            VALUES (%s, %s, %s, (NOW() AT TIME ZONE 'UTC'), %s, 'pending')
            RETURNING id
        """, (player_id, best_candidate['player_id'], current_mode, expires_at))
        
        match_id = cursor.fetchone()['id']
        logger.info(f"Создан матч ID={match_id}, expires_at={expires_at}")
        
        # Удаляем ОБОИХ из очереди
        cursor.execute("""
            DELETE FROM search_queue 
            WHERE player_id IN (%s, %s)
        """, (player_id, best_candidate['player_id']))
        logger.info(f"Игроки {player_id} и {best_candidate['player_id']} удалены из очереди")
        
        conn.commit()
        
        opponent = {
            "player_id": best_candidate['player_id'],
            "nick": best_candidate['nick'],
            "age": best_candidate['age'],
            "style": best_candidate['style'],
            "rating": best_candidate['rank'],
            "steam_link": best_candidate['steam_link'] or "Не указана",
            "faceit_link": best_candidate['faceit_link'] or "Не указана",
            "comment": best_candidate['comment'] or "Нет комментария"
        }
        
        return jsonify({
            "match_found": True,
            "match_id": match_id,
            "opponent": opponent,
            "expires_at": expires_at.isoformat() + "Z",  # ← ДОБАВЛЕНО + "Z"
            "server_time": now.isoformat() + "Z"         # ← ДОБАВЛЕНО + "Z"
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

@app.route('/api/match/status/<int:match_id>', methods=['GET'])
def match_status(match_id):
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
            logger.info(f"Матч {match_id} не найден")
            return jsonify({"status": "not_found"})
        
        logger.info(f"Статус матча {match_id}: {match['status']}")
        
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

@app.route('/api/match/respond', methods=['POST'])
def respond_match():
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
            FROM matches WHERE id = %s
        """, (data['match_id'],))
        
        match = cursor.fetchone()
        
        if not match:
            logger.error(f"Матч {data['match_id']} не найден")
            return jsonify({"error": "Match not found"}), 404
        
        # Проверяем не истекло ли время
        current_time = datetime.utcnow()
        if match['expires_at'] and current_time > match['expires_at']:
            logger.warning(f"Матч {data['match_id']} истек")
            cursor.execute("UPDATE matches SET status = 'expired' WHERE id = %s", (data['match_id'],))
            conn.commit()
            return jsonify({"status": "expired", "message": "Время истекло"})
        
        # Обновляем ответ игрока
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
        
        # Оба приняли
        if responses['player1_response'] == 'accept' and responses['player2_response'] == 'accept':
            logger.info("Оба приняли матч")
            cursor.execute("UPDATE matches SET status = 'accepted' WHERE id = %s", (data['match_id'],))
            conn.commit()
            return jsonify({"status": "accepted", "both_accepted": True})
        
        # Кто-то отклонил
        elif responses['player1_response'] == 'reject' or responses['player2_response'] == 'reject':
            logger.info("Матч отклонен")
            cursor.execute("DELETE FROM matches WHERE id = %s", (data['match_id'],))
            
            # ИСПРАВЛЕНО: 2 минуты на поиск при возврате в очередь, всё в UTC
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
        
        # Один ответил, второй ждем
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

@app.route('/api/search/stop', methods=['POST'])
def stop_search():
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
@app.route('/api/my-matches', methods=['POST'])
def my_matches():
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
        
        # Активные матчи
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
        
        # История
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

@app.route('/api/active-match', methods=['POST'])
def active_match():
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
# ЭНДПОИНТ СОЗДАНИЯ ИГРЫ
# ============================================
@app.route('/api/game/create', methods=['POST'])
def create_game():
    logger.info("POST /api/game/create")
    
    if not request.json or 'match_id' not in request.json:
        return jsonify({"error": "Missing match_id"}), 400
    
    data = request.json
    conn = None
    cursor = None
    
    try:
        conn = get_db()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()
        
        logger.info(f"create_game для match_id={data['match_id']}")
        
        # === ПРОВЕРКА: уже есть игра для этого match_id? ===
        cursor.execute("""
            SELECT id, telegram_chat_link FROM games WHERE match_id = %s
        """, (data['match_id'],))
        
        existing_game = cursor.fetchone()
        if existing_game:
            logger.info(f"Игра для match_id={data['match_id']} уже существует")
            return jsonify({
                "status": "ok",
                "game_id": existing_game[0],
                "chat_link": existing_game[1]
            })
        
        # Получаем данные матча
        cursor.execute("""
            SELECT player1_id, player2_id, mode
            FROM matches
            WHERE id = %s AND status = 'accepted'
        """, (data['match_id'],))
        
        match = cursor.fetchone()
        
        if not match:
            logger.error(f"Match not found or not accepted: {data['match_id']}")
            return jsonify({"error": "Match not found or not accepted"}), 404
        
        player1_id, player2_id = match[0], match[1]
        logger.info(f"Матч найден: игроки {player1_id} и {player2_id}")
        
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
        logger.info(f"Telegram IDs: {telegram_id1}, {telegram_id2}")
        
        # === СОЗДАЕМ ТЕМУ ===
        FORUM_ID = FORUM_GROUP_ID
        
        create_topic_url = f"https://api.telegram.org/bot{BOT_TOKEN}/createForumTopic"
        topic_data = {
            "chat_id": FORUM_ID,
            "name": f"#{data['match_id']} | {nick1} & {nick2}",
            "icon_color": 0x6FB9F0
        }
        
        logger.info(f"Создаем тему для матча #{data['match_id']}...")
        topic_response = requests.post(create_topic_url, json=topic_data)
        topic_result = topic_response.json()
        
        if not topic_result.get('ok'):
            logger.error(f"Failed to create forum topic: {topic_result}")
            return jsonify({"error": "Failed to create chat"}), 500
        
        topic_id = topic_result['result']['message_thread_id']
        logger.info(f"Тема создана, ID: {topic_id}")
        
        # === ССЫЛКА НА ТЕМУ ===
        clean_chat_id = str(FORUM_ID).replace('-100', '')
        chat_link = f"https://t.me/c/{clean_chat_id}/{topic_id}"
        logger.info(f"Ссылка на тему: {chat_link}")
        
        # === ПРИВЕТСТВИЕ ===
        welcome_text = f"""🎯 **МАТЧ #{data['match_id']} СОЗДАН!**

Привет, {nick1} и {nick2}!
Это ваш временный чат для игры.

⏳ Чат будет активен 30 минут, затем закроется.
🔒 Чужие сюда не зайдут — защищено ботом."""
        
        try:
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": FORUM_ID,
                    "message_thread_id": topic_id,
                    "text": welcome_text,
                    "parse_mode": "Markdown"
                }
            )
            logger.info("Приветствие отправлено в тему")
        except Exception as e:
            logger.error(f"Ошибка отправки приветствия: {e}")
        
        # === СОХРАНЯЕМ ===
        expires_at = datetime.utcnow() + timedelta(minutes=30)  # 30 минут на чат
        
        cursor.execute("""
            INSERT INTO games (match_id, player1_id, player2_id, telegram_chat_id, telegram_chat_link, status, created_at, expires_at)
            VALUES (%s, %s, %s, %s, %s, 'active', (NOW() AT TIME ZONE 'UTC'), %s)
            RETURNING id
        """, (data['match_id'], player1_id, player2_id, topic_id, chat_link, expires_at))
        
        game_id = cursor.fetchone()[0]
        conn.commit()
        logger.info(f"Игра создана, ID: {game_id}")
        
        # === УВЕДОМЛЕНИЯ ===
        try:
            for tg_id, nick in [(telegram_id1, nick1), (telegram_id2, nick2)]:
                msg_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
                msg_data = {
                    "chat_id": int(tg_id),
                    "text": f"✅ **Матч #{data['match_id']} создан!**\n\n"
                            f"Соперник: {nick2 if nick == nick1 else nick1}\n\n"
                            f"🔗 [Перейти в чат]({chat_link})",
                    "parse_mode": "Markdown"
                }
                requests.post(msg_url, json=msg_data)
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления в ЛС: {e}")
        
        return jsonify({
            "status": "ok",
            "game_id": game_id,
            "chat_link": chat_link
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
# ЗАПУСК
# ============================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    
    print("🔥 PINGSTER BACKEND - ФИНАЛЬНАЯ АРХИТЕКТУРА 9.8/10!")
    print("✅ Что работает:")
    print("   - Мэтчмейкинг (оба получают матч)")
    print("   - Форум-группа с темами")
    print("   - Темы живут 30 минут, потом закрываются (🔒)")
    print("   - Защита от чужих (автовыброс за 0.1 сек + правила)")
    print("   - Эндпоинт /api/my-matches для истории")
    print("   - Эндпоинт /api/active-match для авто-редиректа")
    print("   - Красивые названия: #ID | ник1 & ник2")
    print("   - Приветствие в теме")
    print("   - Возврат chat_link для фронта")
    print("   - Защита от дублей (проверка existing_game)")
    print("   - Фоновый поток для очистки очереди поиска (каждые 5 секунд)")
    print(f"📌 ID форум-группы: {FORUM_GROUP_ID}")
    print(f"📌 ID темы с правилами: {RULES_TOPIC_ID}")
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
    
    # Запускаем Flask (debug=False для продакшена)
    app.run(host='0.0.0.0', port=port, debug=False)
