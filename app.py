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

def get_user_id(telegram_id):
    """Получает user_id по telegram_id"""
    logger.debug(f"Поиск user_id по telegram_id: {telegram_id}")
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (telegram_id,))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    if result:
        logger.debug(f"Найден user_id: {result[0]}")
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
        cursor.execute("SELECT id, player_id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        user = cursor.fetchone()
        logger.debug(f"Результат поиска: {user}")
        
        if not user:
            logger.info("Создание нового пользователя")
            # Создаём пользователя
            player_id = generate_player_id()
            cursor.execute("""
                INSERT INTO users (telegram_id, username, player_id, last_active, is_online)
                VALUES (%s, %s, %s, NOW(), true)
                RETURNING id, player_id
            """, (data['telegram_id'], data.get('username', 'no_username'), player_id))
            new_id, player_id = cursor.fetchone()
            logger.info(f"Создан пользователь с ID: {new_id}, player_id: {player_id}")
            
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
                "user_id": new_id, 
                "player_id": player_id,
                "nick": nick,
                "pingcoins": 1000
            })
        else:
            user_id, player_id = user
            logger.info(f"Существующий пользователь ID: {user_id}, player_id: {player_id}")
            
            # Обновляем last_active
            cursor.execute("""
                UPDATE users SET last_active = NOW(), is_online = true
                WHERE id = %s
            """, (user_id,))
            logger.debug(f"Обновлен last_active для user_id: {user_id}")
            
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
                    "user_id": user_id, 
                    "player_id": player_id,
                    "nick": nick,
                    "pingcoins": 1000
                })
            
            conn.commit()
            logger.info("Данные обновлены")
            
            return jsonify({
                "status": "ok", 
                "new_user": False, 
                "user_id": user_id, 
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
        
        # Получаем user_id
        cursor.execute("SELECT id FROM users WHERE player_id = %s", (player_id,))
        user_id = cursor.fetchone()[0]
        
        logger.debug(f"Добавление кейса в инвентарь для user_id: {user_id}, case: {data.get('case_id')}")
        cursor.execute("""
            INSERT INTO inventory (user_id, case_id, case_name, unique_id, status_case)
            VALUES (%s, %s, %s, %s, 'new')
        """, (user_id, data.get('case_id'), data.get('case_name'), data.get('unique_id')))
        
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
        
        # Получаем user_id
        cursor.execute("SELECT id FROM users WHERE player_id = %s", (player_id,))
        user_id = cursor.fetchone()[0]
        
        logger.debug(f"Получение инвентаря для user_id: {user_id}")
        cursor.execute("""
            SELECT case_id, case_name, unique_id, status_case, 
                   item_id, item_name, status_item
            FROM inventory 
            WHERE user_id = %s
            ORDER BY 
                CASE WHEN status_case = 'new' THEN 0 ELSE 1 END,
                CASE WHEN status_item = 'new' THEN 0 ELSE 1 END,
                unique_id DESC
        """, (user_id,))
        
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
        
        # Получаем user_id
        cursor.execute("SELECT id FROM users WHERE player_id = %s", (player_id,))
        user_id = cursor.fetchone()[0]
        
        logger.debug(f"Открытие кейса для user_id: {user_id}, unique_id: {data.get('unique_id')}")
        cursor.execute("""
            UPDATE inventory 
            SET status_case = 'opened',
                item_id = %s,
                item_name = %s,
                status_item = 'new'
            WHERE unique_id = %s AND user_id = %s
            RETURNING case_id, case_name
        """, (data.get('item_id'), data.get('item_name'), data.get('unique_id'), user_id))
        
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
        
        # Получаем user_id
        cursor.execute("SELECT id FROM users WHERE player_id = %s", (player_id,))
        user_id = cursor.fetchone()[0]
        
        logger.debug(f"Обновление статуса предмета: {data.get('unique_id')} -> {data.get('status')}")
        cursor.execute("""
            UPDATE inventory 
            SET status_item = %s
            WHERE unique_id = %s AND user_id = %s
            RETURNING item_id, item_name
        """, (data.get('status'), data.get('unique_id'), user_id))
        
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
        
        # Получаем user_id
        cursor.execute("SELECT id FROM users WHERE player_id = %s", (player_id,))
        user_id = cursor.fetchone()[0]
        
        logger.debug(f"Удаление предмета: {data.get('unique_id')}")
        cursor.execute("""
            DELETE FROM inventory 
            WHERE unique_id = %s AND user_id = %s
            RETURNING item_id, item_name
        """, (data.get('unique_id'), user_id))
        
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
# НАЧАТЬ ПОИСК (С АЛГОРИТМОМ)
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
        
        # Получаем user_id и player_id
        cursor.execute("SELECT id, player_id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        user = cursor.fetchone()
        if not user:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        user_id = user[0]
        player_id = user[1]
        logger.debug(f"Найден user_id: {user_id}, player_id: {player_id}")
        
        # Удаляем старые записи в очереди
        cursor.execute("DELETE FROM search_queue WHERE user_id = %s", (user_id,))
        logger.debug("Старые записи удалены")
        
        # Определяем режим
        mode = data.get('mode', '').lower()
        
        # Базовые поля для всех режимов
        base_query = """
            INSERT INTO search_queue 
            (user_id, player_id, mode, rating_value, style, age, steam_link, faceit_link,
             faceit_elo, premier_rating, prime_rank, public_rank, joined_at, expires_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW() + INTERVAL '5 minutes')
            RETURNING id
        """
        
        # Подготавливаем значения
        if mode == 'faceit':
            rating_value = data.get('rating_value', 0)
            values = (
                user_id, player_id, mode, rating_value, data.get('style'), data.get('age'),
                data.get('steam_link'), data.get('faceit_link'),
                rating_value, 0, None, None
            )
        elif mode == 'premier':
            rating_value = data.get('rating_value', 0)
            values = (
                user_id, player_id, mode, 0, data.get('style'), data.get('age'),
                data.get('steam_link'), data.get('faceit_link'),
                0, rating_value, None, None
            )
        elif mode == 'prime':
            rank_value = data.get('rating_value', 'Silver 1')
            rating_value = RANK_TO_VALUE.get(rank_value, 1000)
            values = (
                user_id, player_id, mode, rating_value, data.get('style'), data.get('age'),
                data.get('steam_link'), data.get('faceit_link'),
                0, 0, rank_value, None
            )
        else:
            rank_value = data.get('rating_value', 'Silver 1')
            rating_value = RANK_TO_VALUE.get(rank_value, 1000)
            values = (
                user_id, player_id, mode, rating_value, data.get('style'), data.get('age'),
                data.get('steam_link'), data.get('faceit_link'),
                0, 0, None, rank_value
            )
        
        logger.debug(f"Вставляем значения: {values}")
        cursor.execute(base_query, values)
        queue_id = cursor.fetchone()[0]
        conn.commit()
        logger.info(f"Добавлен в очередь с ID: {queue_id}")
        
        # Поиск кандидатов
        cursor.execute("""
            SELECT * FROM search_queue 
            WHERE mode = %s 
            AND user_id != %s
            AND id != %s
            ORDER BY joined_at ASC
        """, (mode, user_id, queue_id))
        
        candidates = cursor.fetchall()
        logger.debug(f"Найдено кандидатов: {len(candidates)}")
        
        if not candidates:
            logger.info("Нет кандидатов, ждем...")
            return jsonify({"status": "searching", "message": "В очереди"})
        
        # Параметры текущего игрока
        current = {
            'id': queue_id,
            'user_id': user_id,
            'player_id': player_id,
            'mode': mode,
            'rating_value': values[3],
            'style': values[4],
            'age': values[5],
            'joined_at': datetime.now()
        }
        
        best_match = None
        best_score = float('inf')
        best_candidate_data = None
        
        for candidate in candidates:
            candidate_data = {
                'id': candidate[0],
                'user_id': candidate[1],
                'player_id': candidate[2],
                'mode': candidate[3],
                'rating_value': candidate[4],
                'style': candidate[5],
                'age': candidate[6],
                'joined_at': candidate[13]
            }
            
            # Время ожидания кандидата
            wait_time = (datetime.now() - candidate_data['joined_at']).total_seconds()
            
            # Лимит рейтинга по времени
            if wait_time < 5:
                max_rating_diff = RATING_LIMITS[5]
            elif wait_time < 10:
                max_rating_diff = RATING_LIMITS[10]
            elif wait_time < 15:
                max_rating_diff = RATING_LIMITS[15]
            else:
                max_rating_diff = RATING_LIMITS[999]
            
            # Разница рейтинга
            rating_diff = abs(current['rating_value'] - candidate_data['rating_value'])
            if rating_diff > max_rating_diff:
                continue
            
            # Разница возраста
            age_diff = abs(current['age'] - candidate_data['age'])
            
            # Штраф за стиль
            style_penalty = 0
            if current['style'] != candidate_data['style']:
                style_penalty = STYLE_PENALTY.get(mode, 100)
            
            # Считаем score
            age_weight = AGE_WEIGHT.get(mode, 250)
            score = (age_weight * age_diff) + rating_diff + style_penalty
            
            logger.debug(f"Кандидат {candidate_data['user_id']}: score={score}, "
                        f"rating_diff={rating_diff}, age_diff={age_diff}, style_penalty={style_penalty}")
            
            if score < best_score:
                best_score = score
                best_match = candidate_data
                best_candidate_data = candidate
        
        if best_match:
            logger.info(f"Найден лучший кандидат с score={best_score}")
            
            # Создаем match
            cursor.execute("""
                INSERT INTO matches 
                (user1_id, user2_id, mode, compatibility_score, created_at, expires_at)
                VALUES (%s, %s, %s, %s, NOW(), NOW() + INTERVAL '30 seconds')
                RETURNING id
            """, (user_id, best_match['user_id'], mode, best_score))
            
            match_id = cursor.fetchone()[0]
            
            # Удаляем обоих из очереди
            cursor.execute("DELETE FROM search_queue WHERE user_id IN (%s, %s)", 
                         (user_id, best_match['user_id']))
            
            conn.commit()
            logger.info(f"Создан match ID: {match_id}")
            
            # Данные оппонента
            opponent_data = {
                "user_id": best_match['user_id'],
                "player_id": best_match['player_id'],
                "age": best_match['age'],
                "style": best_match['style'],
                "rating": best_match['rating_value']
            }
            
            if mode in ['prime', 'public']:
                rank_field = 'prime_rank' if mode == 'prime' else 'public_rank'
                opponent_data['rank'] = best_candidate_data[10] if mode == 'prime' else best_candidate_data[11]
            
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
        user_id = get_user_id(data['telegram_id'])
        if not user_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        cursor = conn.cursor()
        
        logger.debug(f"Найден user_id: {user_id}")
        
        cursor.execute("DELETE FROM search_queue WHERE user_id = %s", (user_id,))
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
        user_id = get_user_id(data['telegram_id'])
        if not user_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        cursor = conn.cursor()
        
        logger.debug(f"Найден user_id: {user_id}")
        
        cursor.execute("""
            SELECT * FROM matches 
            WHERE (user1_id = %s OR user2_id = %s) 
            AND status = 'pending'
            ORDER BY id DESC LIMIT 1
        """, (user_id, user_id))
        
        match = cursor.fetchone()
        
        if match:
            logger.debug(f"Найден мэтч: {match}")
            other_id = match[1] if match[1] != user_id else match[2]
            
            cursor.execute("""
                SELECT u.telegram_id, p.nick, p.age
                FROM users u
                JOIN profiles p ON u.player_id = p.player_id
                WHERE u.id = %s
            """, (other_id,))
            opponent = cursor.fetchone()
            
            if opponent:
                logger.debug(f"Данные оппонента: {opponent}")
                return jsonify({
                    "match_found": True,
                    "match_id": match[0],
                    "opponent": {
                        "telegram_id": opponent[0],
                        "nick": opponent[1],
                        "age": opponent[2]
                    }
                })
            else:
                logger.debug("Данные оппонента не найдены")
                return jsonify({"match_found": False})
        else:
            logger.debug("Мэтч не найден")
            return jsonify({"match_found": False})
    
    except Exception as e:
        logger.error(f"ОШИБКА: {e}", exc_info=True)
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
        user_id = get_user_id(data['telegram_id'])
        if not user_id:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        conn = get_db()
        cursor = conn.cursor()
        
        logger.debug(f"Найден user_id: {user_id}")
        
        cursor.execute("SELECT * FROM matches WHERE id = %s", (data['match_id'],))
        match = cursor.fetchone()
        
        if not match:
            logger.error(f"Match not found: {data['match_id']}")
            return jsonify({"error": "Match not found"}), 404
        
        logger.debug(f"Найден мэтч: {match}")
        
        if match[1] == user_id:
            cursor.execute("UPDATE matches SET user1_response = %s WHERE id = %s", 
                         (data['response'], data['match_id']))
            logger.debug("Обновлен ответ user1")
        elif match[2] == user_id:
            cursor.execute("UPDATE matches SET user2_response = %s WHERE id = %s", 
                         (data['response'], data['match_id']))
            logger.debug("Обновлен ответ user2")
        else:
            logger.error("User not in this match")
            return jsonify({"error": "User not in this match"}), 403
        
        cursor.execute("SELECT user1_response, user2_response FROM matches WHERE id = %s", 
                      (data['match_id'],))
        responses = cursor.fetchone()
        logger.debug(f"Ответы: {responses}")
        
        if responses[0] == 'accept' and responses[1] == 'accept':
            cursor.execute("UPDATE matches SET status = 'accepted' WHERE id = %s", 
                         (data['match_id'],))
            conn.commit()
            logger.info("Мэтч принят обоими")
            return jsonify({"status": "accepted", "both_accepted": True})
        
        elif responses[0] == 'reject' or responses[1] == 'reject':
            cursor.execute("UPDATE matches SET status = 'rejected' WHERE id = %s", 
                         (data['match_id'],))
            conn.commit()
            logger.info("Мэтч отклонен")
            return jsonify({"status": "rejected", "both_accepted": False})
        else:
            conn.commit()
            logger.info("Ожидание ответа")
            return jsonify({"status": "waiting", "both_accepted": False})
    
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
        
        cursor.execute("SELECT user1_id, user2_id FROM matches WHERE id = %s AND status = 'accepted'", 
                      (data['match_id'],))
        match = cursor.fetchone()
        
        if not match:
            logger.error(f"Match not found or not accepted: {data['match_id']}")
            return jsonify({"error": "Match not found or not accepted"}), 404
        
        chat_id = random.randint(1000000, 9999999)
        chat_link = f"https://t.me/+{random.randint(1000000, 9999999)}"
        
        cursor.execute("""
            INSERT INTO games (match_id, user1_id, user2_id, telegram_chat_id, telegram_chat_link, created_at)
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
    
    if 'game_id' not in data or 'user_id' not in data or 'vote' not in data:
        logger.error("Missing required fields")
        return jsonify({"error": "Missing game_id, user_id or vote"}), 400
    
    conn = None
    cursor = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        if data['user_id'] == data['voter_id']:
            logger.error("User cannot vote for themselves")
            return jsonify({"error": "Cannot vote for yourself"}), 400
        
        cursor.execute("""
            UPDATE games 
            SET user1_vote = CASE WHEN user1_id = %s THEN %s ELSE user1_vote END,
                user2_vote = CASE WHEN user2_id = %s THEN %s ELSE user2_vote END,
                completed_at = CASE 
                    WHEN user1_vote IS NOT NULL AND user2_vote IS NOT NULL 
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
    print("   - /api/game/create")
    print("   - /api/game/vote")
    print("\nАлгоритм поиска активен!")
    print("   - Веса возраста: faceit=100, premier=750, prime/public=250")
    print("   - Штрафы за стиль: faceit=100, premier=500, prime=300, public=100")
    print("   - Лимиты рейтинга: 5с=200, 10с=400, 15с=800, 15+с=2000")
    print("\nСервер запущен на порту 5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
