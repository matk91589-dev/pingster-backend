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

def get_user_id(telegram_id):
    logger.debug(f"Поиск user_id по telegram_id: {telegram_id}")
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (telegram_id,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    if user:
        logger.debug(f"Найден user_id: {user[0]}")
        return user[0]
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
    logger.info("🔥 POST /api/user/init")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"📦 Получены данные: {data}")
    
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
        cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        user = cursor.fetchone()
        logger.debug(f"Результат поиска: {user}")
        
        if not user:
            logger.info("Создание нового пользователя")
            # Создаём пользователя
            player_id = generate_player_id()
            cursor.execute("""
                INSERT INTO users (telegram_id, username, player_id, last_active, is_online)
                VALUES (%s, %s, %s, NOW(), true)
                RETURNING id
            """, (data['telegram_id'], data.get('username', 'no_username'), player_id))
            new_id = cursor.fetchone()[0]
            logger.info(f"✅ Создан пользователь с ID: {new_id}")
            
            # Создаём профиль с ником и 1000 монет
            nick = generate_random_nick()
            logger.debug(f"Создание профиля для user_id: {new_id}, nick: {nick}")
            cursor.execute("""
                INSERT INTO profiles (user_id, nick, pingcoins)
                VALUES (%s, %s, 1000)
            """, (new_id, nick))
            logger.info("✅ Профиль создан")
            
            conn.commit()
            logger.info("💾 Данные сохранены в БД")
            
            return jsonify({
                "status": "ok", 
                "new_user": True, 
                "user_id": new_id, 
                "player_id": player_id,
                "nick": nick,
                "pingcoins": 1000
            })
        else:
            user_id = user[0]
            logger.info(f"👤 Существующий пользователь ID: {user_id}")
            
            # Обновляем last_active
            cursor.execute("""
                UPDATE users SET last_active = NOW(), is_online = true
                WHERE id = %s
                RETURNING player_id
            """, (user_id,))
            player_id = cursor.fetchone()[0]
            logger.debug(f"Обновлен last_active, player_id: {player_id}")
            
            # Проверяем, есть ли профиль
            logger.debug(f"Поиск профиля для user_id: {user_id}")
            cursor.execute("SELECT nick, pingcoins FROM profiles WHERE user_id = %s", (user_id,))
            profile = cursor.fetchone()
            logger.debug(f"Профиль найден: {profile}")
            
            if not profile:
                logger.warning(f"Профиль не найден для user_id: {user_id}, создаем новый")
                # Если профиля нет — создаём
                nick = generate_random_nick()
                cursor.execute("""
                    INSERT INTO profiles (user_id, nick, pingcoins)
                    VALUES (%s, %s, 1000)
                """, (user_id, nick))
                conn.commit()
                logger.info(f"✅ Создан недостающий профиль для user_id={user_id}")
                
                return jsonify({
                    "status": "ok", 
                    "new_user": False, 
                    "user_id": user_id, 
                    "player_id": player_id,
                    "nick": nick,
                    "pingcoins": 1000
                })
            
            conn.commit()
            logger.info("💾 Данные обновлены")
            
            return jsonify({
                "status": "ok", 
                "new_user": False, 
                "user_id": user_id, 
                "player_id": player_id,
                "nick": profile[0],
                "pingcoins": profile[1]
            })
    
    except Exception as e:
        logger.error(f"❌ ОШИБКА: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.debug("🔚 Завершение запроса")

# ============================================
# ПОЛУЧИТЬ ПРОФИЛЬ
# ============================================
@app.route('/api/profile/get', methods=['POST'])
def get_profile():
    logger.info("🔥 POST /api/profile/get")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"📦 Получены данные: {data}")
    
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
        
        logger.debug(f"Получение профиля для user_id: {user_id}")
        cursor.execute("""
            SELECT nick, age, steam_link, faceit_link, avatar_base64, pingcoins
            FROM profiles WHERE user_id = %s
        """, (user_id,))
        profile = cursor.fetchone()
        logger.debug(f"Профиль: {profile}")
        
        if not profile:
            logger.error(f"Profile not found for user_id: {user_id}")
            return jsonify({"error": "Profile not found"}), 404
        
        return jsonify({
            "status": "ok",
            "nick": profile[0],
            "age": profile[1],
            "steam_link": profile[2],
            "faceit_link": profile[3],
            "avatar": profile[4],
            "pingcoins": profile[5]
        })
    
    except Exception as e:
        logger.error(f"❌ ОШИБКА: {e}", exc_info=True)
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
    logger.info("🔥 POST /api/profile/update")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"📦 Получены данные: {data}")
    
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
        
        logger.debug(f"Обновление профиля для user_id: {user_id}")
        cursor.execute("""
            UPDATE profiles 
            SET nick = COALESCE(%s, nick),
                age = COALESCE(%s, age),
                steam_link = COALESCE(%s, steam_link),
                faceit_link = COALESCE(%s, faceit_link),
                updated_at = NOW()
            WHERE user_id = %s
        """, (
            data.get('nick'),
            data.get('age'),
            data.get('steam_link'),
            data.get('faceit_link'),
            user_id
        ))
        
        conn.commit()
        logger.info("✅ Профиль обновлен")
        
        return jsonify({"status": "ok"})
    
    except Exception as e:
        logger.error(f"❌ ОШИБКА: {e}", exc_info=True)
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
    logger.info("🔥 POST /api/avatar/save")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"📦 Получены данные: {data}")
    
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
        
        logger.debug(f"Сохранение аватарки для user_id: {user_id}")
        cursor.execute("""
            UPDATE profiles SET avatar_base64 = %s WHERE user_id = %s
        """, (data.get('avatar_base64'), user_id))
        
        conn.commit()
        logger.info("✅ Аватарка сохранена")
        
        return jsonify({"status": "ok"})
    
    except Exception as e:
        logger.error(f"❌ ОШИБКА: {e}", exc_info=True)
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
    logger.info("🔥 POST /api/user/balance")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"📦 Получены данные: {data}")
    
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
        
        logger.debug(f"Получение баланса для user_id: {user_id}")
        cursor.execute("SELECT pingcoins FROM profiles WHERE user_id = %s", (user_id,))
        result = cursor.fetchone()
        balance = result[0] if result else 0
        logger.debug(f"Баланс: {balance}")
        
        return jsonify({"status": "ok", "balance": balance})
    
    except Exception as e:
        logger.error(f"❌ ОШИБКА: {e}", exc_info=True)
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
    logger.info("🔥 POST /api/shop/buy")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"📦 Получены данные: {data}")
    
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
        
        # Проверяем баланс
        logger.debug(f"Проверка баланса для user_id: {user_id}")
        cursor.execute("SELECT pingcoins FROM profiles WHERE user_id = %s", (user_id,))
        result = cursor.fetchone()
        if not result:
            logger.error(f"Profile not found for user_id: {user_id}")
            return jsonify({"error": "Profile not found"}), 404
        
        coins = result[0]
        price = data.get('price', 0)
        logger.debug(f"Баланс: {coins}, цена: {price}")
        
        if coins < price:
            logger.warning(f"Недостаточно монет: {coins} < {price}")
            return jsonify({"error": "Not enough coins"}), 400
        
        # Списываем монеты
        logger.debug(f"Списываем {price} монет")
        cursor.execute("UPDATE profiles SET pingcoins = pingcoins - %s WHERE user_id = %s", 
                      (price, user_id))
        
        # Добавляем кейс в инвентарь
        logger.debug(f"Добавление кейса в инвентарь: {data.get('case_id')}")
        cursor.execute("""
            INSERT INTO inventory (user_id, case_id, case_name, unique_id, status_case)
            VALUES (%s, %s, %s, %s, 'new')
        """, (user_id, data.get('case_id'), data.get('case_name'), data.get('unique_id')))
        
        conn.commit()
        logger.info("✅ Покупка совершена")
        
        # Получаем новый баланс
        cursor.execute("SELECT pingcoins FROM profiles WHERE user_id = %s", (user_id,))
        new_balance = cursor.fetchone()[0]
        logger.debug(f"Новый баланс: {new_balance}")
        
        return jsonify({"status": "ok", "new_balance": new_balance})
    
    except Exception as e:
        logger.error(f"❌ ОШИБКА: {e}", exc_info=True)
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
    logger.info("🔥 POST /api/inventory/get")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"📦 Получены данные: {data}")
    
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
        logger.error(f"❌ ОШИБКА: {e}", exc_info=True)
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
    logger.info("🔥 POST /api/case/open")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"📦 Получены данные: {data}")
    
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
        logger.info("✅ Кейс открыт")
        
        return jsonify({
            "status": "ok", 
            "case_id": result[0],
            "case_name": result[1],
            "item_id": data.get('item_id'),
            "item_name": data.get('item_name')
        })
    
    except Exception as e:
        logger.error(f"❌ ОШИБКА: {e}", exc_info=True)
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
    logger.info("🔥 POST /api/item/update_status")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"📦 Получены данные: {data}")
    
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
        logger.info("✅ Статус обновлен")
        
        return jsonify({
            "status": "ok",
            "item_id": result[0],
            "item_name": result[1],
            "new_status": data.get('status')
        })
    
    except Exception as e:
        logger.error(f"❌ ОШИБКА: {e}", exc_info=True)
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
    logger.info("🔥 POST /api/item/delete")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"📦 Получены данные: {data}")
    
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
        logger.info("✅ Предмет удален")
        
        return jsonify({"status": "ok", "deleted": result[0]})
    
    except Exception as e:
        logger.error(f"❌ ОШИБКА: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# ============================================
# НАЧАТЬ ПОИСК
# ============================================
@app.route('/api/search/start', methods=['POST'])
def start_search():
    logger.info("🔥 POST /api/search/start")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"📦 Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        user = cursor.fetchone()
        if not user:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        user_id = user[0]
        logger.debug(f"Найден user_id: {user_id}")
        
        cursor.execute("DELETE FROM search_queue WHERE user_id = %s", (user_id,))
        logger.debug("Старые записи удалены")
        
        cursor.execute("""
            INSERT INTO search_queue (user_id, mode, rank_value, age, steam_link, faceit_link)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (user_id, data.get('mode'), data.get('rank'), data.get('age'), data.get('steam_link'), data.get('faceit_link')))
        
        conn.commit()
        logger.info("✅ Поиск начат")
        
        return jsonify({"status": "searching"})
    
    except Exception as e:
        logger.error(f"❌ ОШИБКА: {e}", exc_info=True)
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
    logger.info("🔥 POST /api/search/stop")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"📦 Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        user = cursor.fetchone()
        if not user:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        user_id = user[0]
        logger.debug(f"Найден user_id: {user_id}")
        
        cursor.execute("DELETE FROM search_queue WHERE user_id = %s", (user_id,))
        conn.commit()
        logger.info("✅ Поиск остановлен")
        
        return jsonify({"status": "stopped"})
    
    except Exception as e:
        logger.error(f"❌ ОШИБКА: {e}", exc_info=True)
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
    logger.info("🔥 POST /api/match/check")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"📦 Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        user = cursor.fetchone()
        if not user:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        user_id = user[0]
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
                SELECT age, mode, rank_value, steam_link, faceit_link 
                FROM search_queue WHERE user_id = %s
            """, (other_id,))
            other_data = cursor.fetchone()
            
            if other_data:
                logger.debug(f"Данные оппонента: {other_data}")
                return jsonify({
                    "match_found": True,
                    "match_id": match[0],
                    "opponent": {
                        "age": other_data[0],
                        "mode": other_data[1],
                        "rank": other_data[2],
                        "steam_link": other_data[3],
                        "faceit_link": other_data[4]
                    }
                })
            else:
                logger.debug("Данные оппонента не найдены")
                return jsonify({"match_found": False})
        else:
            logger.debug("Мэтч не найден")
            return jsonify({"match_found": False})
    
    except Exception as e:
        logger.error(f"❌ ОШИБКА: {e}", exc_info=True)
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
    logger.info("🔥 POST /api/match/respond")
    
    if not request.json:
        logger.error("No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    data = request.json
    logger.info(f"📦 Получены данные: {data}")
    
    if 'telegram_id' not in data:
        logger.error("Missing telegram_id")
        return jsonify({"error": "Missing telegram_id"}), 400
    
    conn = None
    cursor = None
    try:
        cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        user = cursor.fetchone()
        if not user:
            logger.error(f"User not found for telegram_id: {data['telegram_id']}")
            return jsonify({"error": "User not found"}), 404
        
        user_id = user[0]
        logger.debug(f"Найден user_id: {user_id}")
        
        cursor.execute("SELECT * FROM matches WHERE id = %s", (data['match_id'],))
        match = cursor.fetchone()
        
        if not match:
            logger.error(f"Match not found: {data['match_id']}")
            return jsonify({"error": "Match not found"}), 404
        
        logger.debug(f"Найден мэтч: {match}")
        
        if match[1] == user_id:
            cursor.execute("UPDATE matches SET user1_response = %s WHERE id = %s", (data['response'], data['match_id']))
            logger.debug("Обновлен ответ user1")
        elif match[2] == user_id:
            cursor.execute("UPDATE matches SET user2_response = %s WHERE id = %s", (data['response'], data['match_id']))
            logger.debug("Обновлен ответ user2")
        else:
            logger.error("User not in this match")
            return jsonify({"error": "User not in this match"}), 403
        
        cursor.execute("SELECT user1_response, user2_response FROM matches WHERE id = %s", (data['match_id'],))
        responses = cursor.fetchone()
        logger.debug(f"Ответы: {responses}")
        
        if responses[0] == 'accept' and responses[1] == 'accept':
            cursor.execute("UPDATE matches SET status = 'accepted' WHERE id = %s", (data['match_id'],))
            cursor.execute("DELETE FROM search_queue WHERE user_id IN (%s, %s)", (match[1], match[2]))
            conn.commit()
            logger.info("✅ Мэтч принят обоими")
            return jsonify({"status": "accepted", "both_accepted": True})
        
        elif responses[0] == 'reject' or responses[1] == 'reject':
            cursor.execute("UPDATE matches SET status = 'rejected' WHERE id = %s", (data['match_id'],))
            conn.commit()
            logger.info("❌ Мэтч отклонен")
            return jsonify({"status": "rejected", "both_accepted": False})
        else:
            conn.commit()
            logger.info("⏳ Ожидание ответа")
            return jsonify({"status": "waiting", "both_accepted": False})
    
    except Exception as e:
        logger.error(f"❌ ОШИБКА: {e}", exc_info=True)
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
    print("🚀 Pingster backend запускается...")
    print("✅ Эндпоинты:")
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
