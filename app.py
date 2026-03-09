import sys
import os

sys.path.append('/app/.local/lib/python3.14/site-packages')
sys.path.append(os.path.expanduser('~/.local/lib/python3.14/site-packages'))

from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import random
from datetime import datetime, timedelta
import logging
import requests

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
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

def get_db():
    logger.debug("Подключение к базе данных...")
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

def get_player_id(telegram_id):
    logger.debug(f"Поиск player_id по telegram_id: {telegram_id}")
    conn = get_db()
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

# ============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================
def get_rank_index(rank):
    if rank in RANK_LIST:
        return RANK_LIST.index(rank)
    return 0

def calculate_score(player, candidate, mode):
    """Вычисляет score совместимости (чем меньше, тем лучше)"""
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
    """Возвращает минимальный и максимальный бакет для поиска"""
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
    """Фильтрует кандидатов по рангу для MM режимов"""
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
# ЭНДПОИНТЫ
# ============================================
@app.route('/', methods=['GET'])
def home():
    return jsonify({"message": "Pingster backend is running!", "status": "ok"})

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
        cursor = conn.cursor()
        
        cursor.execute("SELECT player_id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        result = cursor.fetchone()
        
        if not result:
            player_id = generate_player_id()
            cursor.execute("""
                INSERT INTO users (telegram_id, username, player_id, last_active, is_online)
                VALUES (%s, %s, %s, NOW(), true)
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
            cursor.execute("UPDATE users SET last_active = NOW(), is_online = true WHERE player_id = %s", (player_id,))
            
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
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE profiles 
            SET nick = COALESCE(%s, nick),
                age = COALESCE(%s, age),
                steam_link = COALESCE(%s, steam_link),
                faceit_link = COALESCE(%s, faceit_link),
                updated_at = NOW()
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
            AND expires_at > NOW()
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW() + INTERVAL '1 minute')
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
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # === ШАГ 0: Проверяем существующий матч ===
        logger.info("ШАГ 0: Проверяем существующий матч...")
        cursor.execute("""
            SELECT id, player1_id, player2_id, expires_at, status
            FROM matches
            WHERE (player1_id = %s OR player2_id = %s)
            AND status IN ('pending', 'accepted')
            AND expires_at > NOW()
            LIMIT 1
        """, (player_id, player_id))
        
        existing_match = cursor.fetchone()
        
        if existing_match:
            logger.info(f"Найден существующий матч ID={existing_match['id']}, статус={existing_match['status']}")
            
            other_id = existing_match['player2_id'] if existing_match['player1_id'] == player_id else existing_match['player1_id']
            logger.info(f"Соперник ID: {other_id}")
            
            # Получаем данные соперника из profiles
            cursor.execute("""
                SELECT nick, age, steam_link, faceit_link
                FROM profiles WHERE player_id = %s
            """, (other_id,))
            
            profile = cursor.fetchone()
            
            if profile:
                # Пытаемся получить стиль и ранг (из search_queue, если есть)
                cursor.execute("""
                    SELECT style, rank, comment
                    FROM search_queue 
                    WHERE player_id = %s
                    ORDER BY joined_at DESC
                    LIMIT 1
                """, (other_id,))
                
                queue_data = cursor.fetchone()
                
                opponent = {
                    "player_id": other_id,
                    "nick": profile['nick'],
                    "age": profile['age'],
                    "style": queue_data['style'] if queue_data and queue_data['style'] else "fan",
                    "rating": queue_data['rank'] if queue_data and queue_data['rank'] else "0",
                    "steam_link": profile['steam_link'] if profile['steam_link'] else "Не указана",
                    "faceit_link": profile['faceit_link'] if profile['faceit_link'] else "Не указана",
                    "comment": queue_data['comment'] if queue_data and queue_data['comment'] else "Нет комментария"
                }
                
                logger.info(f"Возвращаем существующий матч для игрока {player_id}")
                return jsonify({
                    "match_found": True,
                    "match_id": existing_match['id'],
                    "opponent": opponent,
                    "expires_at": existing_match['expires_at'].isoformat(),
                    "server_time": datetime.utcnow().isoformat()
                })
            else:
                logger.error(f"Профиль соперника {other_id} не найден")
        else:
            logger.info("Существующий матч не найден")
            
            # Отладочный запрос - покажем все матчи игрока
            cursor.execute("""
                SELECT id, player1_id, player2_id, status, expires_at
                FROM matches
                WHERE player1_id = %s OR player2_id = %s
                ORDER BY id DESC
                LIMIT 5
            """, (player_id, player_id))
            debug_matches = cursor.fetchall()
            logger.info(f"Все последние матчи игрока: {debug_matches}")
        
        # === ШАГ 1: Получаем данные текущего игрока из очереди ===
        logger.info("ШАГ 1: Проверяем очередь поиска...")
        cursor.execute("""
            SELECT * FROM search_queue 
            WHERE player_id = %s AND expires_at > NOW()
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
            AND sq.expires_at > NOW()
        """
        params = [current_mode, player_id]
        
        if min_bucket is not None and max_bucket is not None:
            query += " AND sq.rating_bucket BETWEEN %s AND %s"
            params.extend([min_bucket, max_bucket])
        
        cursor.execute(query, params)
        candidates = cursor.fetchall()
        logger.info(f"Найдено кандидатов до фильтрации: {len(candidates)}")
        
        # Фильтруем по рангам для MM Tryhard
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
        
        logger.info(f"Кандидатов с таким же стилем: {len(same_style)}")
        logger.info(f"Кандидатов с другим стилем: {len(other_style)}")
        
        same_style.sort(key=lambda x: calculate_score(current, x, current_mode))
        other_style.sort(key=lambda x: calculate_score(current, x, current_mode))
        
        best_candidate = (same_style + other_style)[0]
        best_score = calculate_score(current, best_candidate, current_mode)
        logger.info(f"Лучший кандидат: {best_candidate['player_id']}, score={best_score}")
        
        # === ШАГ 4: Проверяем, что кандидат еще в очереди ===
        logger.info("ШАГ 4: Проверяем кандидата...")
        cursor.execute("""
            SELECT id FROM search_queue 
            WHERE player_id = %s AND expires_at > NOW()
            FOR UPDATE
        """, (best_candidate['player_id'],))
        
        if not cursor.fetchone():
            logger.warning(f"Кандидат {best_candidate['player_id']} больше не в очереди")
            return jsonify({"match_found": False})
        
        # === ШАГ 5: Создаем матч ===
        logger.info("ШАГ 5: Создаем матч...")
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=30)
        
        cursor.execute("""
            INSERT INTO matches 
            (player1_id, player2_id, mode, created_at, expires_at, status)
            VALUES (%s, %s, %s, NOW(), %s, 'pending')
            RETURNING id
        """, (player_id, best_candidate['player_id'], current_mode, expires_at))
        
        match_id = cursor.fetchone()['id']
        logger.info(f"Создан матч ID={match_id}")
        
        # Удаляем только себя из очереди
        cursor.execute("""
            DELETE FROM search_queue 
            WHERE player_id = %s
        """, (player_id,))
        logger.info(f"Игрок {player_id} удален из очереди")
        
        conn.commit()
        
        opponent = {
            "player_id": best_candidate['player_id'],
            "nick": best_candidate['nick'],
            "age": best_candidate['age'],
            "style": best_candidate['style'],
            "rating": best_candidate['rank'],
            "steam_link": best_candidate['steam_link'] if best_candidate['steam_link'] else "Не указана",
            "faceit_link": best_candidate['faceit_link'] if best_candidate['faceit_link'] else "Не указана",
            "comment": best_candidate['comment'] if best_candidate['comment'] else "Нет комментария"
        }
        
        return jsonify({
            "match_found": True,
            "match_id": match_id,
            "opponent": opponent,
            "expires_at": expires_at.isoformat(),
            "server_time": now.isoformat()
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
                "expires_at": match['expires_at'].isoformat() if match['expires_at'] else None
            })
        
        return jsonify({
            "status": match['status'],
            "expires_at": match['expires_at'].isoformat() if match['expires_at'] else None
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
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        cursor.execute("""
            SELECT player1_id, player2_id, player1_response, player2_response, expires_at, status
            FROM matches WHERE id = %s
        """, (data['match_id'],))
        
        match = cursor.fetchone()
        
        if not match:
            logger.error(f"Матч {data['match_id']} не найден")
            return jsonify({"error": "Match not found"}), 404
        
        logger.info(f"Матч найден: статус {match['status']}, истекает {match['expires_at']}")
        
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
                logger.warning(f"Игрок {player_id} уже ответил")
                return jsonify({"status": "already_responded"})
            cursor.execute("UPDATE matches SET player1_response = %s WHERE id = %s",
                          (data['response'], data['match_id']))
            logger.info(f"Обновлен ответ player1: {data['response']}")
        elif str(match['player2_id']) == str(player_id):
            if match['player2_response'] is not None:
                logger.warning(f"Игрок {player_id} уже ответил")
                return jsonify({"status": "already_responded"})
            cursor.execute("UPDATE matches SET player2_response = %s WHERE id = %s",
                          (data['response'], data['match_id']))
            logger.info(f"Обновлен ответ player2: {data['response']}")
        else:
            logger.error(f"Игрок {player_id} не участвует в матче {data['match_id']}")
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
            # Удаляем матч
            cursor.execute("DELETE FROM matches WHERE id = %s", (data['match_id'],))
            
            # Возвращаем обоих в очередь
            now = datetime.utcnow()
            expires_at = now + timedelta(minutes=1)
            
            # Сохраняем данные игроков (упрощенная версия)
            cursor.execute("""
                INSERT INTO search_queue 
                (player_id, mode, rank, rating_bucket, style, age, steam_link, faceit_link, comment, joined_at, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                match['player1_id'], "faceit", "1500", 15, "fan", 21, "", "", "", now, expires_at
            ))
            logger.info(f"Игрок {match['player1_id']} возвращен в очередь")
            
            cursor.execute("""
                INSERT INTO search_queue 
                (player_id, mode, rank, rating_bucket, style, age, steam_link, faceit_link, comment, joined_at, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                match['player2_id'], "faceit", "1500", 15, "fan", 21, "", "", "", now, expires_at
            ))
            logger.info(f"Игрок {match['player2_id']} возвращен в очередь")
            
            conn.commit()
            return jsonify({"status": "rejected", "both_accepted": False})
        
        # Один ответил, второй ждем
        else:
            logger.info("Ожидаем ответа второго игрока")
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
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM search_queue WHERE player_id = %s", (player_id,))
        deleted = cursor.rowcount
        conn.commit()
        
        logger.info(f"Удалено {deleted} записей из очереди")
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
        cursor = conn.cursor()
        
        logger.info(f"create_game для match_id={data['match_id']}")
        
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
        
        # Создаем чат в Telegram
        create_chat_url = f"https://api.telegram.org/bot{BOT_TOKEN}/createGroupChat"
        chat_data = {
            "title": f"Pingster Match #{data['match_id']}",
            "user_ids": [telegram_id1, telegram_id2]
        }
        
        logger.info("Создаем чат в Telegram...")
        chat_response = requests.post(create_chat_url, json=chat_data)
        chat_result = chat_response.json()
        
        if not chat_result.get('ok'):
            logger.error(f"Failed to create Telegram chat: {chat_result}")
            return jsonify({"error": "Failed to create chat"}), 500
        
        chat_id = chat_result['result']['id']
        logger.info(f"Чат создан, ID: {chat_id}")
        
        # Отправляем приветствие
        welcome_text = f"""** МАТЧ создан ! **

Привет, {nick1} и {nick2}!
Это ваш временный чат для игры.

Здесь вы можете договориться и кинуть приглашение в игру.
Чат самоуничтожится через час."""

        send_msg_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        msg_data = {
            "chat_id": chat_id,
            "text": welcome_text,
            "parse_mode": "Markdown"
        }
        
        requests.post(send_msg_url, json=msg_data)
        logger.info("Приветствие отправлено")
        
        # Создаем пригласительную ссылку
        invite_url = f"https://api.telegram.org/bot{BOT_TOKEN}/createChatInviteLink"
        invite_data = {
            "chat_id": chat_id,
            "member_limit": 2,
            "expire_date": int((datetime.utcnow() + timedelta(hours=1)).timestamp())
        }
        
        invite_response = requests.post(invite_url, json=invite_data)
        invite_result = invite_response.json()
        
        chat_link = invite_result['result']['invite_link'] if invite_result.get('ok') else f"https://t.me/c/{str(chat_id)[4:]}"
        logger.info(f"Ссылка-приглашение: {chat_link}")
        
        # Сохраняем в БД
        cursor.execute("""
            INSERT INTO games (match_id, player1_id, player2_id, telegram_chat_id, telegram_chat_link, status, created_at)
            VALUES (%s, %s, %s, %s, %s, 'active', NOW())
            RETURNING id
        """, (data['match_id'], player1_id, player2_id, chat_id, chat_link))
        
        game_id = cursor.fetchone()[0]
        
        conn.commit()
        logger.info(f"Игра создана, ID: {game_id}")
        
        return jsonify({
            "status": "ok",
            "game_id": game_id,
            "chat_id": chat_id,
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
    print("Pingster backend с РОТАЦИОННЫМ МЭТЧМЕЙКИНГОМ и ЧАТАМИ запускается...")
    print("НОВАЯ АРХИТЕКТУРА:")
    print("   - Ротационный подбор (каждый новый проверяет всех)")
    print("   - Приоритет стиля (сначала свой стиль, потом другой)")
    print("   - Умная формула: |rating_diff| + |age_diff|*100")
    print("   - FACEIT: ±400 ELO (всегда)")
    print("   - PREMIER Fan: без ограничений")
    print("   - PREMIER Tryhard: ±5000")
    print("   - MM Fan: без ограничений")
    print("   - MM Tryhard: ±3 ранга")
    print("   - Telegram чаты создаются автоматически!")
    print("   - search_queue: только waiting (без status/match_id)")
    print("   - matches: pending → accepted → не удаляются сразу")
    print("   - При отказе: удаляем матч и возвращаем в очередь")
    print("   - Добавлена проверка существующего матча в начале check_match")
    print("   - УБРАНО ПРИНУДИТЕЛЬНОЕ ПРОСТАВЛЕНИЕ EXPIRED")
    print("   - Добавлена проверка кандидата перед созданием матча")
    print("\nЭндпоинты:")
    print("   - /api/user/init")
    print("   - /api/profile/get")
    print("   - /api/profile/update")
    print("   - /api/avatar/save")
    print("   - /api/user/balance")
    print("   - /api/shop/buy")
    print("   - /api/inventory/get")
    print("   - /api/case/open")
    print("   - /api/item/update_status")
    print("   - /api/item/delete")
    print("   - /api/search/start")
    print("   - /api/search/stop")
    print("   - /api/match/check")
    print("   - /api/match/status/<match_id>")
    print("   - /api/match/respond")
    print("   - /api/game/create")
    print("\nСервер запущен на порту 5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
