import sys
import os

sys.path.append('/app/.local/lib/python3.14/site-packages')
sys.path.append(os.path.expanduser('~/.local/lib/python3.14/site-packages'))

from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import random
from datetime import datetime, timedelta
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

def get_db():
    logger.debug("Подключение к базе данных...")
    return psycopg2.connect(
        host="85.239.33.182",
        database="pingster_db",
        user="gen_user",
        password="{,@~:5my>jvOAj",
        port=5432
    )

def get_player_id(telegram_id):
    """Получает player_id по telegram_id"""
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
    player_id = str(random.randint(10000000, 99999999))
    logger.debug(f"Сгенерирован player_id: {player_id}")
    return player_id

def generate_random_nick():
    chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    nick = ''.join(random.choice(chars) for _ in range(6))
    logger.debug(f"Сгенерирован nick: {nick}")
    return nick

# Конвертация рангов в числа
RANK_TO_VALUE = {
    'Silver 1': 1000, 'Silver 2': 1100, 'Silver 3': 1200, 'Silver 4': 1300,
    'Silver Elite': 1400, 'Gold Nova 1': 1500, 'Gold Nova 2': 1600,
    'Gold Nova 3': 1700, 'Gold Nova Master': 1800, 'Master Guardian 1': 1900,
    'Master Guardian 2': 2000, 'Master Guardian Elite': 2100,
    'Distinguished Master Guardian': 2200, 'Legendary Eagle': 2300,
    'Legendary Eagle Master': 2400, 'Supreme Master First Class': 2500,
    'Global Elite': 2600
}

# Штрафы за стиль игры
STYLE_PENALTY = {
    'faceit': 100,
    'premier': 500,
    'prime': 300,
    'public': 100
}

# Веса возраста
AGE_WEIGHT = {
    'faceit': 100,
    'premier': 750,
    'prime': 250,
    'public': 250
}

# Лимиты рейтинга по времени ожидания
RATING_LIMITS = {
    5: 200,    # 0-5 сек
    10: 400,   # 5-10 сек
    15: 800,   # 10-15 сек
    999: 2000  # 15+ сек
}

# ============================================
# ГЛАВНАЯ
# ============================================
@app.route('/', methods=['GET'])
def home():
    logger.info("GET /")
    return jsonify({"message": "Pingster backend is running!", "status": "ok"})

@app.route('/api', methods=['GET'])
def api_root():
    logger.info("GET /api")
    return jsonify({"message": "Pingster API is running!", "status": "ok"})

# ============================================
# ИНИЦИАЛИЗАЦИЯ ПОЛЬЗОВАТЕЛЯ
# ============================================
@app.route('/api/user/init', methods=['POST'])
def init_user():
    logger.info("POST /api/user/init")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Проверяем пользователя
        logger.debug(f"Поиск пользователя с telegram_id: {data['telegram_id']}")
        cursor.execute("SELECT player_id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        result = cursor.fetchone()
        logger.debug(f"Результат поиска: {result}")
        
        if not result:
            logger.info("Создание нового пользователя")
            # Создаём пользователя
            player_id = generate_player_id()
            cursor.execute("""
                INSERT INTO users (telegram_id, username, player_id, last_active, is_online)
                VALUES (%s, %s, %s, NOW(), true)
                RETURNING player_id
            """, (data['telegram_id'], data.get('username', 'no_username'), player_id))
            player_id = cursor.fetchone()[0]
            logger.info(f"Создан пользователь с player_id: {player_id}")
            
            # Создаём профиль
            nick = generate_random_nick()
            logger.debug(f"Создание профиля для player_id: {player_id}, nick: {nick}")
            cursor.execute("""
                INSERT INTO profiles (player_id, nick, pingcoins, telegram_id)
                VALUES (%s, %s, 1000, %s)
            """, (player_id, nick, data['telegram_id']))
            logger.info("Профиль создан")
            
            conn.commit()
            logger.info("Данные сохранены в БД")
            
            return jsonify({
                "status": "ok", 
                "new_user": True, 
                "player_id": player_id,
                "nick": nick,
                "pingcoins": 1000
            })
        else:
            player_id = result[0]
            logger.info(f"Существующий пользователь player_id: {player_id}")
            
            # Обновляем last_active
            cursor.execute("""
                UPDATE users SET last_active = NOW(), is_online = true
                WHERE player_id = %s
            """, (player_id,))
            logger.debug(f"Обновлен last_active для player_id: {player_id}")
            
            # Проверяем, есть ли профиль
            logger.debug(f"Поиск профиля для player_id: {player_id}")
            cursor.execute("SELECT nick, pingcoins FROM profiles WHERE player_id = %s", (player_id,))
            profile = cursor.fetchone()
            logger.debug(f"Профиль найден: {profile}")
            
            if not profile:
                logger.warning(f"Профиль не найден для player_id: {player_id}, создаем новый")
                nick = generate_random_nick()
                cursor.execute("""
                    INSERT INTO profiles (player_id, nick, pingcoins, telegram_id)
                    VALUES (%s, %s, 1000, %s)
                """, (player_id, nick, data['telegram_id']))
                conn.commit()
                logger.info(f"Создан недостающий профиль для player_id={player_id}")
                
                return jsonify({
                    "status": "ok", 
                    "new_user": False, 
                    "player_id": player_id,
                    "nick": nick,
                    "pingcoins": 1000
                })
            
            conn.commit()
            logger.info("Данные обновлены")
            
            return jsonify({
                "status": "ok", 
                "new_user": False, 
                "player_id": player_id,
                "nick": profile[0],
                "pingcoins": profile[1]
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
        logger.debug("Завершение запроса")

# ============================================
# ПОЛУЧИТЬ ПРОФИЛЬ
# ============================================
@app.route('/api/profile/get', methods=['POST'])
def get_profile():
    logger.info("POST /api/profile/get")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        cursor = conn.cursor()
        
        logger.debug(f"Получение профиля для player_id: {player_id}")
        cursor.execute("""
            SELECT nick, age, steam_link, faceit_link, avatar, pingcoins
            FROM profiles WHERE player_id = %s
        """, (player_id,))
        profile = cursor.fetchone()
        logger.debug(f"Профиль: {profile}")
        
        if not profile:
            logger.error(f"Profile not found for player_id: {player_id}")
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

# ============================================
# ОБНОВИТЬ ПРОФИЛЬ
# ============================================
@app.route('/api/profile/update', methods=['POST'])
def update_profile():
    logger.info("POST /api/profile/update")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        cursor = conn.cursor()
        
        logger.debug(f"Обновление профиля для player_id: {player_id}")
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
        logger.info("Профиль обновлен")
        
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

# ============================================
# СОХРАНИТЬ АВАТАРКУ
# ============================================
@app.route('/api/avatar/save', methods=['POST'])
def save_avatar():
    logger.info("POST /api/avatar/save")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        cursor = conn.cursor()
        
        logger.debug(f"Сохранение аватарки для player_id: {player_id}")
        cursor.execute("""
            UPDATE profiles SET avatar = %s WHERE player_id = %s
        """, (data.get('avatar'), player_id))
        
        conn.commit()
        logger.info("Аватарка сохранена")
        
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

# ============================================
# ПОЛУЧИТЬ БАЛАНС
# ============================================
@app.route('/api/user/balance', methods=['POST'])
def get_balance():
    logger.info("POST /api/user/balance")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        cursor = conn.cursor()
        
        logger.debug(f"Получение баланса для player_id: {player_id}")
        cursor.execute("SELECT pingcoins FROM profiles WHERE player_id = %s", (player_id,))
        result = cursor.fetchone()
        balance = result[0] if result else 0
        logger.debug(f"Баланс: {balance}")
        
        return jsonify({"status": "ok", "balance": balance})
    
    except Exception as e:
        logger.error(f"ОШИБКА: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ============================================
# КУПИТЬ КЕЙС
# ============================================
@app.route('/api/shop/buy', methods=['POST'])
def buy_case():
    logger.info("POST /api/shop/buy")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        cursor = conn.cursor()
        
        # Проверяем баланс
        logger.debug(f"Проверка баланса для player_id: {player_id}")
        cursor.execute("SELECT pingcoins FROM profiles WHERE player_id = %s", (player_id,))
        result = cursor.fetchone()
        if not result:
            logger.error(f"Profile not found for player_id: {player_id}")
            return jsonify({"error": "Profile not found"}), 404
        
        coins = result[0]
        price = data.get('price', 0)
        logger.debug(f"Баланс: {coins}, цена: {price}")
        
        if coins < price:
            logger.warning(f"Недостаточно монет: {coins} < {price}")
            return jsonify({"error": "Not enough coins"}), 400
        
        # Списываем монеты
        logger.debug(f"Списываем {price} монет")
        cursor.execute("UPDATE profiles SET pingcoins = pingcoins - %s WHERE player_id = %s", 
                      (price, player_id))
        
        logger.debug(f"Добавление кейса в инвентарь для player_id: {player_id}, case: {data.get('case_id')}")
        cursor.execute("""
            INSERT INTO inventory (player_id, case_id, case_name, unique_id, status_case)
            VALUES (%s, %s, %s, %s, 'new')
        """, (player_id, data.get('case_id'), data.get('case_name'), data.get('unique_id')))
        
        conn.commit()
        logger.info("Покупка совершена")
        
        # Получаем новый баланс
        cursor.execute("SELECT pingcoins FROM profiles WHERE player_id = %s", (player_id,))
        new_balance = cursor.fetchone()[0]
        logger.debug(f"Новый баланс: {new_balance}")
        
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

# ============================================
# ПОЛУЧИТЬ ИНВЕНТАРЬ
# ============================================
@app.route('/api/inventory/get', methods=['POST'])
def get_inventory():
    logger.info("POST /api/inventory/get")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        cursor = conn.cursor()
        
        logger.debug(f"Получение инвентаря для player_id: {player_id}")
        cursor.execute("""
            SELECT case_id, case_name, unique_id, status_case, 
                   item_id, item_name, status_item
            FROM inventory 
            WHERE player_id = %s
            ORDER BY 
                CASE WHEN status_case = 'new' THEN 0 ELSE 1 END,
                CASE WHEN status_item = 'new' THEN 0 ELSE 1 END,
                unique_id DESC
        """, (player_id,))
        
        items = cursor.fetchall()
        logger.debug(f"Найдено предметов: {len(items)}")
        
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

# ============================================
# ОТКРЫТЬ КЕЙС
# ============================================
@app.route('/api/case/open', methods=['POST'])
def open_case():
    logger.info("POST /api/case/open")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        cursor = conn.cursor()
        
        logger.debug(f"Открытие кейса для player_id: {player_id}, unique_id: {data.get('unique_id')}")
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
            logger.error(f"Case not found: {data.get('unique_id')}")
            return jsonify({"error": "Case not found"}), 404
        
        conn.commit()
        logger.info("Кейс открыт")
        
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

# ============================================
# ОБНОВИТЬ СТАТУС ПРЕДМЕТА
# ============================================
@app.route('/api/item/update_status', methods=['POST'])
def update_item_status():
    logger.info("POST /api/item/update_status")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        cursor = conn.cursor()
        
        logger.debug(f"Обновление статуса предмета: {data.get('unique_id')} -> {data.get('status')}")
        cursor.execute("""
            UPDATE inventory 
            SET status_item = %s
            WHERE unique_id = %s AND player_id = %s
            RETURNING item_id, item_name
        """, (data.get('status'), data.get('unique_id'), player_id))
        
        result = cursor.fetchone()
        if not result:
            logger.error(f"Item not found: {data.get('unique_id')}")
            return jsonify({"error": "Item not found"}), 404
        
        conn.commit()
        logger.info("Статус обновлен")
        
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

# ============================================
# УДАЛИТЬ ПРЕДМЕТ
# ============================================
@app.route('/api/item/delete', methods=['POST'])
def delete_item():
    logger.info("POST /api/item/delete")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        cursor = conn.cursor()
        
        logger.debug(f"Удаление предмета: {data.get('unique_id')}")
        cursor.execute("""
            DELETE FROM inventory 
            WHERE unique_id = %s AND player_id = %s
            RETURNING item_id, item_name
        """, (data.get('unique_id'), player_id))
        
        result = cursor.fetchone()
        if not result:
            logger.error(f"Item not found: {data.get('unique_id')}")
            return jsonify({"error": "Item not found"}), 404
        
        conn.commit()
        logger.info("Предмет удален")
        
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
# НАЧАТЬ ПОИСК (С АЛГОРИТМОМ) - ТОЛЬКО player_id
# ============================================
@app.route('/api/search/start', methods=['POST'])
def start_search():
    logger.info("POST /api/search/start")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Получаем player_id
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        logger.debug(f"Найден player_id: {player_id}")
        
        # Удаляем старые записи в очереди
        cursor.execute("DELETE FROM search_queue WHERE player_id = %s", (player_id,))
        logger.debug("Старые записи удалены")
        
        # Определяем режим
        mode = data.get('mode', '').lower()
        
        # Получаем значение ранга
        rank_value = data.get('rating_value', '0')
        
        # Преобразуем в число для faceit/premier
        rating_number = 0
        
        if mode in ['faceit', 'premier']:
            try:
                rating_number = int(rank_value)
            except:
                rating_number = 0
        else:
            rating_number = RANK_TO_VALUE.get(rank_value, 1000)
        
        # Вставляем только player_id
        base_query = """
            INSERT INTO search_queue 
            (player_id, mode, rank, style, age, steam_link, faceit_link, comment, joined_at, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW() + INTERVAL '5 minutes')
            RETURNING id
        """
        
        values = (
            player_id, mode, str(rating_number), 
            data.get('style'), data.get('age'),
            data.get('steam_link'), data.get('faceit_link'),
            data.get('comment')
        )
        
        logger.debug(f"Вставляем значения: {values}")
        cursor.execute(base_query, values)
        queue_id = cursor.fetchone()[0]
        conn.commit()
        logger.info(f"Добавлен в очередь с ID: {queue_id}, player_id: {player_id}")
        
        # Проверяем сколько кандидатов в очереди ДО нас
        cursor.execute("""
            SELECT COUNT(*) FROM search_queue 
            WHERE mode = %s AND player_id != %s AND id != %s
        """, (mode, player_id, queue_id))
        count_before = cursor.fetchone()[0]
        logger.info(f"В очереди до нас: {count_before} игроков")
        
        # Поиск кандидатов - берем всех в том же режиме, кроме себя
        cursor.execute("""
            SELECT * FROM search_queue 
            WHERE mode = %s 
            AND player_id != %s
            AND id != %s
            ORDER BY joined_at ASC
        """, (mode, player_id, queue_id))
        
        candidates = cursor.fetchall()
        logger.debug(f"Найдено кандидатов: {len(candidates)}")
        
        if not candidates:
            logger.info("Нет кандидатов, ждем...")
            return jsonify({"status": "searching", "message": "В очереди"})
        
        # Параметры текущего игрока
        current_time = datetime.now()
        current = {
            'id': queue_id,
            'player_id': player_id,
            'mode': mode,
            'rank': str(rating_number),
            'style': data.get('style'),
            'age': data.get('age'),
            'joined_at': current_time
        }
        
        logger.info(f"Текущий игрок: player_id={player_id}, rank={rating_number}, style={current['style']}, age={current['age']}")
        
        best_match = None
        best_score = float('inf')
        best_candidate_raw = None
        
        for idx, candidate in enumerate(candidates):
            # candidate: id, player_id, mode, rank, style, age, steam_link, faceit_link, comment, joined_at, expires_at
            candidate_data = {
                'id': candidate[0],
                'player_id': candidate[1],
                'mode': candidate[2],
                'rank': candidate[3],
                'style': candidate[4],
                'age': candidate[5],
                'joined_at': candidate[9]
            }
            
            logger.info(f"Кандидат {idx+1}: player_id={candidate_data['player_id']}, rank={candidate_data['rank']}, style={candidate_data['style']}, age={candidate_data['age']}")
            
            # Проверяем joined_at
            if candidate_data['joined_at'] is None:
                logger.debug(f"У кандидата {candidate_data['player_id']} нет joined_at, пропускаем")
                continue
            
            # Считаем время ожидания кандидата
            wait_time = (current_time - candidate_data['joined_at']).total_seconds()
            logger.debug(f"Кандидат ждет {wait_time:.1f} секунд")
            
            # Лимит рейтинга по времени
            if wait_time < 5:
                max_rating_diff = RATING_LIMITS[5]
            elif wait_time < 10:
                max_rating_diff = RATING_LIMITS[10]
            elif wait_time < 15:
                max_rating_diff = RATING_LIMITS[15]
            else:
                max_rating_diff = RATING_LIMITS[999]
            
            logger.debug(f"Максимальная разница рейтинга: {max_rating_diff}")
            
            # Преобразуем rank в число для сравнения
            try:
                current_rank_val = int(current['rank'])
                candidate_rank_val = int(candidate_data['rank'])
            except:
                current_rank_val = 0
                candidate_rank_val = 0
                logger.debug(f"Ошибка преобразования rank, используем 0")
            
            # Разница рейтинга
            rating_diff = abs(current_rank_val - candidate_rank_val)
            logger.debug(f"Разница рейтинга: {rating_diff}")
            
            if rating_diff > max_rating_diff:
                logger.debug(f"Кандидат не подходит по рейтингу: разница {rating_diff} > {max_rating_diff}")
                continue
            
            # Разница возраста
            age_diff = abs(current['age'] - candidate_data['age'])
            logger.debug(f"Разница возраста: {age_diff}")
            
            # Штраф за стиль
            style_penalty = 0
            if current['style'] != candidate_data['style']:
                style_penalty = STYLE_PENALTY.get(mode, 100)
                logger.debug(f"Штраф за стиль: {style_penalty}")
            
            # Считаем score
            age_weight = AGE_WEIGHT.get(mode, 250)
            score = (age_weight * age_diff) + rating_diff + style_penalty
            
            logger.info(f"Кандидат {candidate_data['player_id']}: score={score}, "
                        f"rating_diff={rating_diff}, age_diff={age_diff}, style_penalty={style_penalty}")
            
            if score < best_score:
                logger.debug(f"Новый лучший кандидат! Предыдущий score={best_score}, новый={score}")
                best_score = score
                best_match = candidate_data
                best_candidate_raw = candidate
        
        if best_match:
            logger.info(f"✅ Найден лучший кандидат с score={best_score}")
            
            # Создаем match с player_id
            cursor.execute("""
                INSERT INTO matches 
                (player1_id, player2_id, mode, compatibility_score, created_at, expires_at)
                VALUES (%s, %s, %s, %s, NOW(), NOW() + INTERVAL '30 seconds')
                RETURNING id
            """, (player_id, best_match['player_id'], mode, best_score))
            
            match_id = cursor.fetchone()[0]
            logger.info(f"Создан match ID: {match_id}")
            
            # Удаляем обоих из очереди по player_id
            cursor.execute("DELETE FROM search_queue WHERE player_id IN (%s, %s)", 
                         (player_id, best_match['player_id']))
            logger.info(f"Удалены из очереди player_id: {player_id} и {best_match['player_id']}")
            
            conn.commit()
            
            # Данные оппонента
            opponent_data = {
                "player_id": best_match['player_id'],
                "age": best_match['age'],
                "style": best_match['style'],
                "rating": best_match['rank']
            }
            
            return jsonify({
                "status": "match_found",
                "match_id": match_id,
                "score": best_score,
                "opponent": opponent_data
            })
        
        conn.commit()
        logger.info("Кандидатов нет, ждем...")
        return jsonify({"status": "searching", "message": "В очереди"})
    
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
# ОСТАНОВИТЬ ПОИСК
# ============================================
@app.route('/api/search/stop', methods=['POST'])
def stop_search():
    logger.info("POST /api/search/stop")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        cursor = conn.cursor()
        
        logger.debug(f"Найден player_id: {player_id}")
        
        cursor.execute("DELETE FROM search_queue WHERE player_id = %s", (player_id,))
        conn.commit()
        logger.info("Поиск остановлен")
        
        return jsonify({"status": "stopped"})
    
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
# ПРОВЕРИТЬ МЭТЧ
# ============================================
@app.route('/api/match/check', methods=['POST'])
def check_match():
    logger.info("POST /api/match/check")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        cursor = conn.cursor()
        
        logger.debug(f"Найден player_id: {player_id}")
        
        # Ищем активный мэтч
        cursor.execute("""
            SELECT * FROM matches 
            WHERE (player1_id = %s OR player2_id = %s) 
            AND status = 'pending'
            ORDER BY id DESC LIMIT 1
        """, (player_id, player_id))
        
        match = cursor.fetchone()
        logger.info(f"Результат поиска мэтча для {player_id}: {match}")
        
        if match:
            logger.info(f"✅ Найден мэтч ID={match[0]}")
            
            # Определяем оппонента
            other_id = match[1] if match[1] != player_id else match[2]
            logger.debug(f"ID оппонента: {other_id}")
            
            # Получаем данные оппонента
            cursor.execute("""
                SELECT p.nick, p.age
                FROM profiles p
                WHERE p.player_id = %s
            """, (other_id,))
            opponent = cursor.fetchone()
            
            if opponent:
                logger.debug(f"Данные оппонента: nick={opponent[0]}, age={opponent[1]}")
                
                # Проверяем, ответил ли уже этот игрок
                player_response = None
                if match[1] == player_id:
                    player_response = match[5]  # player1_response
                else:
                    player_response = match[6]  # player2_response
                
                # Формируем ответ
                response_data = {
                    "match_found": True,
                    "match_id": match[0],
                    "opponent": {
                        "player_id": other_id,
                        "nick": opponent[0],
                        "age": opponent[1],
                        "style": "fan",  # можно добавить стиль из search_queue если нужно
                        "rating": match[4]  # compatibility_score
                    },
                    "your_response": player_response,
                    "expires_at": match[8].isoformat() if match[8] else None
                }
                logger.info(f"Отправляем данные: {response_data}")
                return jsonify(response_data)
            else:
                logger.error(f"Профиль оппонента {other_id} не найден")
                return jsonify({"match_found": False})
        else:
            logger.debug("Мэтч не найден")
            return jsonify({"match_found": False})
    
    except Exception as e:
        logger.error(f"ОШИБКА в check_match: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ============================================
# ОТВЕТИТЬ НА МЭТЧ
# ============================================
@app.route('/api/match/respond', methods=['POST'])
def respond_match():
    logger.info("POST /api/match/respond")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'telegram_id' not in data or 'match_id' not in data or 'response' not in data:
        logger.error("Missing required fields")
        return jsonify({"error": "Missing telegram_id, match_id or response"}), 400
    
    conn = None
    cursor = None
    try:
        player_id = get_player_id(data['telegram_id'])
        if not player_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        cursor = conn.cursor()
        
        logger.debug(f"Найден player_id: {player_id}")
        
        # Получаем матч
        cursor.execute("SELECT * FROM matches WHERE id = %s", (data['match_id'],))
        match = cursor.fetchone()
        
        if not match:
            logger.error(f"Match not found: {data['match_id']}")
            return jsonify({"error": "Match not found"}), 404
        
        # Проверяем не истекло ли время
        if match[8] and datetime.now() > match[8]:
            logger.warning(f"Match {data['match_id']} expired")
            cursor.execute("UPDATE matches SET status = 'expired' WHERE id = %s", (data['match_id'],))
            conn.commit()
            return jsonify({"status": "expired", "message": "Время истекло"})
        
        logger.debug(f"Найден мэтч: {match}")
        
        # Обновляем ответ игрока
        if match[1] == player_id:
            if match[5] is not None:  # уже ответил
                logger.warning(f"Player {player_id} already responded with {match[5]}")
                return jsonify({"status": "already_responded", "your_response": match[5]})
            
            cursor.execute("UPDATE matches SET player1_response = %s WHERE id = %s", 
                         (data['response'], data['match_id']))
            logger.debug("Обновлен ответ player1")
        elif match[2] == player_id:
            if match[6] is not None:
                logger.warning(f"Player {player_id} already responded with {match[6]}")
                return jsonify({"status": "already_responded", "your_response": match[6]})
            
            cursor.execute("UPDATE matches SET player2_response = %s WHERE id = %s", 
                         (data['response'], data['match_id']))
            logger.debug("Обновлен ответ player2")
        else:
            logger.error("User not in this match")
            return jsonify({"error": "User not in this match"}), 403
        
        # Проверяем ответы
        cursor.execute("SELECT player1_response, player2_response, expires_at FROM matches WHERE id = %s", 
                      (data['match_id'],))
        responses = cursor.fetchone()
        logger.debug(f"Ответы: {responses}")
        
        # Если оба ответили
        if responses[0] is not None and responses[1] is not None:
            if responses[0] == 'accept' and responses[1] == 'accept':
                # Оба приняли
                cursor.execute("UPDATE matches SET status = 'accepted' WHERE id = %s", 
                             (data['match_id'],))
                conn.commit()
                logger.info("✅ Мэтч принят обоими")
                
                return jsonify({
                    "status": "accepted", 
                    "both_accepted": True,
                    "match_id": data['match_id']
                })
            else:
                # Кто-то отклонил
                cursor.execute("UPDATE matches SET status = 'rejected' WHERE id = %s", 
                             (data['match_id'],))
                conn.commit()
                logger.info("❌ Мэтч отклонен")
                return jsonify({"status": "rejected", "both_accepted": False})
        else:
            # Ждем ответа второго
            conn.commit()
            
            # Проверяем сколько времени осталось
            time_left = 30
            if responses[2]:
                time_left = max(0, int((responses[2] - datetime.now()).total_seconds()))
            
            logger.info(f"⏳ Ожидание ответа, осталось {time_left} сек")
            return jsonify({
                "status": "waiting", 
                "both_accepted": False,
                "time_left": time_left
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


# ============================================
# ПРОВЕРИТЬ ПРОСРОЧЕННЫЕ МЭТЧИ
# ============================================
@app.route('/api/match/cleanup', methods=['POST'])
def cleanup_matches():
    logger.info("POST /api/match/cleanup")
    
    conn = None
    cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Находим просроченные pending матчи
        cursor.execute("""
            UPDATE matches 
            SET status = 'expired' 
            WHERE status = 'pending' 
            AND expires_at < NOW()
            RETURNING id, player1_id, player2_id
        """)
        
        expired = cursor.fetchall()
        conn.commit()
        
        logger.info(f"Очищено просроченных мэтчей: {len(expired)}")
        
        return jsonify({
            "status": "ok", 
            "cleaned": len(expired)
        })
    
    except Exception as e:
        logger.error(f"ОШИБКА: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ============================================
# СОЗДАТЬ ИГРУ
# ============================================
@app.route('/api/game/create', methods=['POST'])
def create_game():
    logger.info("POST /api/game/create")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'match_id' not in data:
        logger.error("Missing match_id")
        return jsonify({"error": "Missing match_id"}), 400
    
    conn = None
    cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        cursor.execute("SELECT player1_id, player2_id FROM matches WHERE id = %s AND status = 'accepted'", 
                      (data['match_id'],))
        match = cursor.fetchone()
        
        if not match:
            logger.error(f"Match not found or not accepted: {data['match_id']}")
            return jsonify({"error": "Match not found or not accepted"}), 404
        
        chat_id = random.randint(1000000, 9999999)
        chat_link = f"https://t.me/+{random.randint(1000000, 9999999)}"
        
        cursor.execute("""
            INSERT INTO games (match_id, player1_id, player2_id, telegram_chat_id, telegram_chat_link, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            RETURNING id
        """, (data['match_id'], match[0], match[1], chat_id, chat_link))
        
        game_id = cursor.fetchone()[0]
        conn.commit()
        logger.info(f"Создана игра ID: {game_id}, чат: {chat_link}")
        
        return jsonify({
            "status": "ok",
            "game_id": game_id,
            "chat_id": chat_id,
            "chat_link": chat_link
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

# ============================================
# ОЦЕНИТЬ ИГРОКА
# ============================================
@app.route('/api/game/vote', methods=['POST'])
def vote_player():
    logger.info("POST /api/game/vote")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"Получены данные: {data}")
    
    if 'game_id' not in data or 'player_id' not in data or 'vote' not in data:
        logger.error("Missing required fields")
        return jsonify({"error": "Missing game_id, player_id or vote"}), 400
    
    conn = None
    cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        if data['player_id'] == data['voter_id']:
            logger.error("User cannot vote for themselves")
            return jsonify({"error": "Cannot vote for yourself"}), 400
        
        cursor.execute("""
            UPDATE games 
            SET player1_vote = CASE WHEN player1_id = %s THEN %s ELSE player1_vote END,
                player2_vote = CASE WHEN player2_id = %s THEN %s ELSE player2_vote END,
                completed_at = CASE 
                    WHEN player1_vote IS NOT NULL AND player2_vote IS NOT NULL 
                    THEN NOW() ELSE completed_at 
                END
            WHERE id = %s
        """, (data['voter_id'], data['vote'], data['voter_id'], data['vote'], data['game_id']))
        
        conn.commit()
        logger.info("Голос записан")
        
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

# ============================================
# ЗАПУСК
# ============================================
if __name__ == '__main__':
    print("Pingster backend запускается...")
    print("Эндпоинты:")
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
    print("   - /api/match/respond")
    print("   - /api/match/cleanup")
    print("   - /api/game/create")
    print("   - /api/game/vote")
    print("\nАлгоритм поиска активен!")
    print("   - Веса возраста: faceit=100, premier=750, prime/public=250")
    print("   - Штрафы за стиль: faceit=100, premier=500, prime=300, public=100")
    print("   - Лимиты рейтинга: 5с=200, 10с=400, 15с=800, 15+с=2000")
    print("\nСервер запущен на порту 5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
