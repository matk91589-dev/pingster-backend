import sys
import os

sys.path.append('/app/.local/lib/python3.14/site-packages')
sys.path.append(os.path.expanduser('~/.local/lib/python3.14/site-packages'))

from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
import random
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)  # Разрешаем запросы с других доменов

# Подключение к базе
def get_db():
    return psycopg2.connect(
        host="85.239.33.182",
        database="pingster_db",
        user="gen_user",
        password="{,@~:5my>jvOAj",
        port=5432
    )

# Вспомогательная функция для получения user_id по telegram_id
def get_user_id(telegram_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (telegram_id,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    return user[0] if user else None

# Генерация player_id
def generate_player_id():
    return str(random.randint(10000000, 99999999))

# ============================================
# ЭНДПОИНТ 1: Главная (проверка сервера)
# ============================================
@app.route('/', methods=['GET'])
def home():
    return jsonify({"message": "Pingster backend is running!", "status": "ok"})

@app.route('/api', methods=['GET'])
def api_root():
    return jsonify({"message": "Pingster API is running!", "status": "ok"})

# ============================================
# ЭНДПОИНТ 2: Инициализация пользователя
# ============================================
@app.route('/api/user/init', methods=['POST'])
def init_user():
    data = request.json
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        user = cursor.fetchone()
        
        if not user:
            player_id = generate_player_id()
            cursor.execute("""
                INSERT INTO users (telegram_id, username, player_id, last_active, is_online, pingcoins)
                VALUES (%s, %s, %s, NOW(), true, 1000)
                RETURNING id
            """, (data['telegram_id'], data['username'], player_id))
            new_id = cursor.fetchone()[0]
            conn.commit()
            return jsonify({"status": "ok", "new_user": True, "user_id": new_id, "player_id": player_id, "pingcoins": 1000})
        else:
            cursor.execute("""
                UPDATE users SET last_active = NOW(), is_online = true
                WHERE telegram_id = %s
                RETURNING id, player_id, pingcoins
            """, (data['telegram_id'],))
            user_data = cursor.fetchone()
            conn.commit()
            return jsonify({
                "status": "ok", 
                "new_user": False, 
                "user_id": user_data[0], 
                "player_id": user_data[1],
                "pingcoins": user_data[2]
            })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================
# ЭНДПОИНТ 3: Начать поиск
# ============================================
@app.route('/api/search/start', methods=['POST'])
def start_search():
    data = request.json
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        user = cursor.fetchone()
        if not user:
            return jsonify({"error": "User not found"}), 404
        
        user_id = user[0]
        
        cursor.execute("DELETE FROM search_queue WHERE user_id = %s", (user_id,))
        
        cursor.execute("""
            INSERT INTO search_queue (user_id, mode, rank_value, age, steam_link, faceit_link)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (user_id, data['mode'], data['rank'], data['age'], data['steam_link'], data.get('faceit_link')))
        
        conn.commit()
        return jsonify({"status": "searching"})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================
# ЭНДПОИНТ 4: Остановить поиск
# ============================================
@app.route('/api/search/stop', methods=['POST'])
def stop_search():
    data = request.json
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        user = cursor.fetchone()
        if not user:
            return jsonify({"error": "User not found"}), 404
        
        cursor.execute("DELETE FROM search_queue WHERE user_id = %s", (user[0],))
        conn.commit()
        return jsonify({"status": "stopped"})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================
# ЭНДПОИНТ 5: Проверить мэтч
# ============================================
@app.route('/api/match/check', methods=['POST'])
def check_match():
    data = request.json
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        user = cursor.fetchone()
        if not user:
            return jsonify({"error": "User not found"}), 404
        
        user_id = user[0]
        
        cursor.execute("""
            SELECT * FROM matches 
            WHERE (user1_id = %s OR user2_id = %s) 
            AND status = 'pending'
            ORDER BY id DESC LIMIT 1
        """, (user_id, user_id))
        
        match = cursor.fetchone()
        
        if match:
            other_id = match[1] if match[1] != user_id else match[2]
            
            cursor.execute("""
                SELECT age, mode, rank_value, steam_link, faceit_link 
                FROM search_queue WHERE user_id = %s
            """, (other_id,))
            other_data = cursor.fetchone()
            
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
            return jsonify({"match_found": False})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================
# ЭНДПОИНТ 6: Ответить на мэтч
# ============================================
@app.route('/api/match/respond', methods=['POST'])
def respond_match():
    data = request.json
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        user = cursor.fetchone()
        if not user:
            return jsonify({"error": "User not found"}), 404
        
        user_id = user[0]
        
        cursor.execute("SELECT * FROM matches WHERE id = %s", (data['match_id'],))
        match = cursor.fetchone()
        
        if not match:
            return jsonify({"error": "Match not found"}), 404
        
        if match[1] == user_id:
            cursor.execute("UPDATE matches SET user1_response = %s WHERE id = %s", (data['response'], data['match_id']))
        elif match[2] == user_id:
            cursor.execute("UPDATE matches SET user2_response = %s WHERE id = %s", (data['response'], data['match_id']))
        else:
            return jsonify({"error": "User not in this match"}), 403
        
        cursor.execute("SELECT user1_response, user2_response FROM matches WHERE id = %s", (data['match_id'],))
        responses = cursor.fetchone()
        
        if responses[0] == 'accept' and responses[1] == 'accept':
            cursor.execute("UPDATE matches SET status = 'accepted' WHERE id = %s", (data['match_id'],))
            cursor.execute("DELETE FROM search_queue WHERE user_id IN (%s, %s)", (match[1], match[2]))
            conn.commit()
            return jsonify({"status": "accepted", "both_accepted": True})
        
        elif responses[0] == 'reject' or responses[1] == 'reject':
            cursor.execute("UPDATE matches SET status = 'rejected' WHERE id = %s", (data['match_id'],))
            conn.commit()
            return jsonify({"status": "rejected", "both_accepted": False})
        else:
            conn.commit()
            return jsonify({"status": "waiting", "both_accepted": False})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================
# ЭНДПОИНТ 7: Получить инвентарь пользователя
# ============================================
@app.route('/api/inventory/get', methods=['POST'])
def get_inventory():
    data = request.json
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        user_id = get_user_id(data['telegram_id'])
        if not user_id:
            return jsonify({"error": "User not found"}), 404
        
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
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================
# ЭНДПОИНТ 8: Купить кейс
# ============================================
@app.route('/api/shop/buy', methods=['POST'])
def buy_case():
    data = request.json
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        user_id = get_user_id(data['telegram_id'])
        if not user_id:
            return jsonify({"error": "User not found"}), 404
        
        # Проверяем баланс
        cursor.execute("SELECT pingcoins FROM users WHERE id = %s", (user_id,))
        coins = cursor.fetchone()[0]
        
        if coins < data['price']:
            return jsonify({"error": "Not enough coins"}), 400
        
        # Списываем монеты
        cursor.execute("UPDATE users SET pingcoins = pingcoins - %s WHERE id = %s", 
                      (data['price'], user_id))
        
        # Добавляем кейс в инвентарь
        cursor.execute("""
            INSERT INTO inventory (user_id, case_id, case_name, unique_id, status_case)
            VALUES (%s, %s, %s, %s, 'new')
        """, (user_id, data['case_id'], data['case_name'], data['unique_id']))
        
        conn.commit()
        
        # Получаем новый баланс
        cursor.execute("SELECT pingcoins FROM users WHERE id = %s", (user_id,))
        new_balance = cursor.fetchone()[0]
        
        return jsonify({"status": "ok", "new_balance": new_balance})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================
# ЭНДПОИНТ 9: Открыть кейс
# ============================================
@app.route('/api/case/open', methods=['POST'])
def open_case():
    data = request.json
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        user_id = get_user_id(data['telegram_id'])
        if not user_id:
            return jsonify({"error": "User not found"}), 404
        
        # Обновляем кейс
        cursor.execute("""
            UPDATE inventory 
            SET status_case = 'opened',
                item_id = %s,
                item_name = %s,
                status_item = 'new'
            WHERE unique_id = %s AND user_id = %s
            RETURNING case_id, case_name
        """, (data['item_id'], data['item_name'], data['unique_id'], user_id))
        
        result = cursor.fetchone()
        if not result:
            return jsonify({"error": "Case not found"}), 404
        
        conn.commit()
        
        return jsonify({
            "status": "ok", 
            "case_id": result[0],
            "case_name": result[1],
            "item_id": data['item_id'],
            "item_name": data['item_name']
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================
# ЭНДПОИНТ 10: Обновить статус предмета (new -> old)
# ============================================
@app.route('/api/item/update_status', methods=['POST'])
def update_item_status():
    data = request.json
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        user_id = get_user_id(data['telegram_id'])
        if not user_id:
            return jsonify({"error": "User not found"}), 404
        
        cursor.execute("""
            UPDATE inventory 
            SET status_item = %s
            WHERE unique_id = %s AND user_id = %s
            RETURNING item_id, item_name
        """, (data['status'], data['unique_id'], user_id))
        
        result = cursor.fetchone()
        if not result:
            return jsonify({"error": "Item not found"}), 404
        
        conn.commit()
        
        return jsonify({
            "status": "ok",
            "item_id": result[0],
            "item_name": result[1],
            "new_status": data['status']
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================
# ЭНДПОИНТ 11: Удалить предмет (при продаже)
# ============================================
@app.route('/api/item/delete', methods=['POST'])
def delete_item():
    data = request.json
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        user_id = get_user_id(data['telegram_id'])
        if not user_id:
            return jsonify({"error": "User not found"}), 404
        
        cursor.execute("""
            DELETE FROM inventory 
            WHERE unique_id = %s AND user_id = %s
            RETURNING item_id, item_name
        """, (data['unique_id'], user_id))
        
        result = cursor.fetchone()
        if not result:
            return jsonify({"error": "Item not found"}), 404
        
        conn.commit()
        
        return jsonify({"status": "ok", "deleted": result[0]})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================
# ЭНДПОИНТ 12: Получить аватарку
# ============================================
@app.route('/api/avatar/get', methods=['POST'])
def get_avatar():
    data = request.json
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        user_id = get_user_id(data['telegram_id'])
        if not user_id:
            return jsonify({"error": "User not found"}), 404
        
        cursor.execute("SELECT avatar_base64 FROM avatars WHERE user_id = %s", (user_id,))
        avatar = cursor.fetchone()
        
        return jsonify({
            "status": "ok", 
            "avatar": avatar[0] if avatar else None
        })
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================
# ЭНДПОИНТ 13: Сохранить аватарку
# ============================================
@app.route('/api/avatar/save', methods=['POST'])
def save_avatar():
    data = request.json
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        user_id = get_user_id(data['telegram_id'])
        if not user_id:
            return jsonify({"error": "User not found"}), 404
        
        cursor.execute("""
            INSERT INTO avatars (user_id, avatar_base64) 
            VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE 
            SET avatar_base64 = EXCLUDED.avatar_base64
        """, (user_id, data['avatar_base64']))
        
        conn.commit()
        return jsonify({"status": "ok"})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================
# ЭНДПОИНТ 14: Получить баланс Pingcoins
# ============================================
@app.route('/api/user/balance', methods=['POST'])
def get_balance():
    data = request.json
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        user_id = get_user_id(data['telegram_id'])
        if not user_id:
            return jsonify({"error": "User not found"}), 404
        
        cursor.execute("SELECT pingcoins FROM users WHERE id = %s", (user_id,))
        balance = cursor.fetchone()[0]
        
        return jsonify({"status": "ok", "balance": balance})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

# ============================================
# ЗАПУСК СЕРВЕРА
# ============================================
if __name__ == '__main__':
    print("🚀 Pingster backend запускается...")
    print("✅ Эндпоинты:")
    print("   - /api/user/init")
    print("   - /api/search/start")
    print("   - /api/search/stop")
    print("   - /api/match/check")
    print("   - /api/match/respond")
    print("   - /api/inventory/get")
    print("   - /api/shop/buy")
    print("   - /api/case/open")
    print("   - /api/item/update_status")
    print("   - /api/item/delete")
    print("   - /api/avatar/get")
    print("   - /api/avatar/save")
    print("   - /api/user/balance")
