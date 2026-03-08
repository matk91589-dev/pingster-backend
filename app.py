import sys
import os

sys.path.append('/app/.local/lib/python3.14/site-packages')
sys.path.append(os.path.expanduser('~/.local/lib/python3.14/site-packages'))

from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import random
from datetime import datetime, timedelta, timezone
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
# НАЧАТЬ ПОИСК - С БАКЕТАМИ
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
        
        # Проверка возраста
        if data.get('age') and (data['age'] < 16 or data['age'] > 100):
            return jsonify({"error": "Возраст должен быть от 16 до 100 лет"}), 400
        
        # Определяем режим
        mode = data.get('mode', '').lower()
        
        # Получаем значение ранга
        rank_value = data.get('rating_value', '0')
        
        # Преобразуем в число
        rating_number = 0
        if mode in ['faceit', 'premier']:
            try:
                rating_number = int(rank_value)
            except:
                rating_number = 0
        else:
            rating_number = RANK_TO_VALUE.get(rank_value, 1000)
        
        # Вычисляем бакет рейтинга (группировка по 100 единиц)
        rating_bucket = rating_number // 100
        
        logger.debug(f"Рейтинг: {rating_number}, бакет: {rating_bucket}")
        
        # Удаляем старые записи этого игрока (только если они не в матче)
        cursor.execute("""
            DELETE FROM search_queue 
            WHERE player_id = %s AND status = 'waiting'
        """, (player_id,))
        deleted_old = cursor.rowcount
        logger.debug(f"Удалено старых записей: {deleted_old}")
        
        # Создаем новую запись в очереди
        cursor.execute("""
            INSERT INTO search_queue 
            (player_id, mode, rank, rating_bucket, style, age, steam_link, faceit_link, comment, joined_at, expires_at, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW() + INTERVAL '5 minutes', 'waiting')
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
        conn.commit()
        logger.info(f"Добавлен в очередь с ID: {queue_id}, player_id: {player_id}, бакет: {rating_bucket}")
        
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
# ПРОВЕРИТЬ МЭТЧ - ИСПРАВЛЕНО С FOR UPDATE SKIP LOCKED
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
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        logger.debug(f"Найден player_id: {player_id}")
        
        # Проверяем, есть ли активный матч
        cursor.execute("""
            SELECT id, player1_id, player2_id, mode, compatibility_score, 
                   expires_at, player1_response, player2_response, status
            FROM matches 
            WHERE (player1_id = %s OR player2_id = %s) 
            AND status = 'pending'
            ORDER BY id DESC LIMIT 1
        """, (player_id, player_id))
        
        match = cursor.fetchone()
        
        if match:
            logger.info(f"Найден активный матч ID={match['id']}")
            
            # Определяем оппонента
            other_id = match['player2_id'] if match['player1_id'] == player_id else match['player1_id']
            player_response = match['player1_response'] if match['player1_id'] == player_id else match['player2_response']
            
            # Получаем данные оппонента из очереди
            cursor.execute("""
                SELECT style, rank, comment
                FROM search_queue 
                WHERE player_id = %s 
                ORDER BY joined_at DESC LIMIT 1
            """, (other_id,))
            
            queue_data = cursor.fetchone()
            
            # Получаем данные из профиля
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
                    "style": queue_data['style'] if queue_data else "fan",
                    "rating": queue_data['rank'] if queue_data else "0",
                    "steam_link": profile['steam_link'] if profile['steam_link'] else "Не указана",
                    "faceit_link": profile['faceit_link'] if profile['faceit_link'] else "Не указана",
                    "comment": queue_data['comment'] if queue_data and queue_data['comment'] else "Нет комментария"
                }
                
                server_time = datetime.now(timezone.utc)
                
                return jsonify({
                    "match_found": True,
                    "match_id": match['id'],
                    "opponent": opponent,
                    "your_response": player_response,
                    "expires_at": match['expires_at'].isoformat() if match['expires_at'] else None,
                    "server_time": server_time.isoformat()
                })
        
        # Если нет активного матча, ищем кандидатов с блокировкой
        logger.debug("Поиск кандидатов в очереди")
        
        # Получаем данные текущего игрока из очереди
        cursor.execute("""
            SELECT rating_bucket, mode FROM search_queue 
            WHERE player_id = %s AND status = 'waiting' AND expires_at > NOW()
            ORDER BY joined_at DESC LIMIT 1
        """, (player_id,))
        
        current = cursor.fetchone()
        
        if not current:
            logger.debug("Игрок не в очереди")
            return jsonify({"match_found": False})
        
        bucket, mode = current['rating_bucket'], current['mode']
        min_bucket = bucket - 1
        max_bucket = bucket + 1
        
        logger.debug(f"Поиск кандидатов: режим={mode}, бакет от {min_bucket} до {max_bucket}")
        
        # Ищем подходящих кандидатов с блокировкой строки
        cursor.execute("""
            SELECT player_id, style, age, rank, comment
            FROM search_queue 
            WHERE mode = %s 
            AND status = 'waiting'
            AND rating_bucket BETWEEN %s AND %s
            AND player_id != %s
            AND expires_at > NOW()
            ORDER BY joined_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """, (mode, min_bucket, max_bucket, player_id))
        
        candidate = cursor.fetchone()
        
        if candidate:
            logger.info(f"Найден кандидат: player_id={candidate['player_id']}")
            
            # Создаем матч
            cursor.execute("""
                INSERT INTO matches 
                (player1_id, player2_id, mode, compatibility_score, created_at, expires_at, status)
                VALUES (%s, %s, %s, 0, NOW(), NOW() + INTERVAL '30 seconds', 'pending')
                RETURNING id, expires_at
            """, (player_id, candidate['player_id'], mode))
            
            match_result = cursor.fetchone()
            match_id = match_result['id']
            expires_at = match_result['expires_at']
            
            logger.info(f"Создан матч ID: {match_id}")
            
            # Обновляем статус в очереди
            cursor.execute("""
                UPDATE search_queue 
                SET status = 'matched', match_id = %s
                WHERE player_id IN (%s, %s)
            """, (match_id, player_id, candidate['player_id']))
            
            conn.commit()
            
            # Получаем данные кандидата из профиля
            cursor.execute("""
                SELECT nick, age, steam_link, faceit_link
                FROM profiles WHERE player_id = %s
            """, (candidate['player_id'],))
            
            profile = cursor.fetchone()
            
            if profile:
                opponent = {
                    "player_id": candidate['player_id'],
                    "nick": profile['nick'],
                    "age": profile['age'],
                    "style": candidate['style'] if candidate['style'] else "fan",
                    "rating": candidate['rank'] if candidate['rank'] else "0",
                    "steam_link": profile['steam_link'] if profile['steam_link'] else "Не указана",
                    "faceit_link": profile['faceit_link'] if profile['faceit_link'] else "Не указана",
                    "comment": candidate['comment'] if candidate['comment'] else "Нет комментария"
                }
                
                server_time = datetime.now(timezone.utc)
                
                return jsonify({
                    "match_found": True,
                    "match_id": match_id,
                    "opponent": opponent,
                    "your_response": None,
                    "expires_at": expires_at.isoformat(),
                    "server_time": server_time.isoformat()
                })
        
        logger.debug("Кандидатов не найдено")
        return jsonify({"match_found": False})
    
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

# ============================================
# ОТВЕТИТЬ НА МЭТЧ - ИСПРАВЛЕНО С TIMEZONE
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
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        logger.debug(f"Найден player_id: {player_id}")
        
        # Получаем информацию о матче
        cursor.execute("""
            SELECT player1_id, player2_id, player1_response, player2_response, expires_at, status
            FROM matches WHERE id = %s
        """, (data['match_id'],))
        
        match = cursor.fetchone()
        
        if not match:
            logger.error(f"Match not found: {data['match_id']}")
            return jsonify({"error": "Match not found"}), 404
        
        # Проверяем не истекло ли время (используем timezone.utcnow)
        current_time = datetime.now(timezone.utc)
        if match['expires_at'] and current_time > match['expires_at']:
            logger.warning(f"Match {data['match_id']} expired")
            
            # Сбрасываем ответы и статус
            cursor.execute("""
                UPDATE matches 
                SET status = 'expired', player1_response = NULL, player2_response = NULL 
                WHERE id = %s
            """, (data['match_id'],))
            
            # Возвращаем игроков в очередь
            cursor.execute("""
                UPDATE search_queue 
                SET status = 'waiting', match_id = NULL
                WHERE player_id IN (%s, %s)
            """, (match['player1_id'], match['player2_id']))
            
            conn.commit()
            return jsonify({"status": "expired", "message": "Время истекло"})
        
        if data['response'] == 'timeout':
            cursor.execute("""
                UPDATE matches 
                SET status = 'expired', player1_response = NULL, player2_response = NULL 
                WHERE id = %s
            """, (data['match_id'],))
            
            # Возвращаем игроков в очередь
            cursor.execute("""
                UPDATE search_queue 
                SET status = 'waiting', match_id = NULL
                WHERE player_id IN (%s, %s)
            """, (match['player1_id'], match['player2_id']))
            
            conn.commit()
            logger.info(f"Мэтч {data['match_id']} закрыт по таймауту")
            return jsonify({"status": "timeout", "message": "Время вышло"})
        
        # Обновляем ответ игрока
        if str(match['player1_id']) == str(player_id):
            if match['player1_response'] is not None:
                logger.warning(f"Player {player_id} already responded")
                return jsonify({"status": "already_responded"})
            cursor.execute("UPDATE matches SET player1_response = %s WHERE id = %s",
                          (data['response'], data['match_id']))
            logger.debug("Обновлен ответ player1")
        elif str(match['player2_id']) == str(player_id):
            if match['player2_response'] is not None:
                logger.warning(f"Player {player_id} already responded")
                return jsonify({"status": "already_responded"})
            cursor.execute("UPDATE matches SET player2_response = %s WHERE id = %s",
                          (data['response'], data['match_id']))
            logger.debug("Обновлен ответ player2")
        else:
            logger.error("User not in this match")
            return jsonify({"error": "User not in this match"}), 403
        
        # Проверяем ответы обоих
        cursor.execute("SELECT player1_response, player2_response, expires_at FROM matches WHERE id = %s", 
                      (data['match_id'],))
        responses = cursor.fetchone()
        
        if responses['player1_response'] == 'accept' and responses['player2_response'] == 'accept':
            # Оба приняли
            cursor.execute("UPDATE matches SET status = 'accepted' WHERE id = %s", (data['match_id'],))
            conn.commit()
            logger.info("Мэтч принят обоими")
            return jsonify({"status": "accepted", "both_accepted": True})
        
        elif responses['player1_response'] == 'reject' or responses['player2_response'] == 'reject':
            # Кто-то отклонил
            cursor.execute("""
                UPDATE matches 
                SET status = 'rejected', player1_response = NULL, player2_response = NULL 
                WHERE id = %s
            """, (data['match_id'],))
            
            # Возвращаем игроков в очередь
            cursor.execute("""
                UPDATE search_queue 
                SET status = 'waiting', match_id = NULL
                WHERE player_id IN (%s, %s)
            """, (match['player1_id'], match['player2_id']))
            
            conn.commit()
            logger.info("Мэтч отклонен")
            return jsonify({"status": "rejected", "both_accepted": False})
        
        else:
            # Ждем второго
            conn.commit()
            time_left = 0
            if responses['expires_at']:
                time_left = max(0, int((responses['expires_at'] - datetime.now(timezone.utc)).total_seconds()))
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
        
        cursor.execute("DELETE FROM search_queue WHERE player_id = %s AND status = 'waiting'", (player_id,))
        deleted = cursor.rowcount
        conn.commit()
        logger.info(f"Поиск остановлен, удалено записей: {deleted}")
        
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
            INSERT INTO games (match_id, player1_id, player2_id, telegram_chat_id, telegram_chat_link, status, created_at)
            VALUES (%s, %s, %s, %s, %s, 'active', NOW())
            RETURNING id
        """, (data['match_id'], match[0], match[1], chat_id, chat_link))
        
        game_id = cursor.fetchone()[0]
        
        # Удаляем игроков из очереди
        cursor.execute("DELETE FROM search_queue WHERE player_id IN (%s, %s)", (match[0], match[1]))
        
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
    print("🔥 Pingster backend с бакетированием запускается...")
    print("🚀 Новая архитектура:")
    print("   - search_queue с rating_bucket")
    print("   - FOR UPDATE SKIP LOCKED для защиты от race condition")
    print("   - Синхронизация времени через server_time")
    print("   - Сброс ответов при reject/expire")
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
    print("   - /api/match/respond")
    print("   - /api/game/create")
    print("   - /api/game/vote")
    print("\nСервер запущен на порту 5000")
    app.run(host='0.0.0.0', port=5000, debug=True)
