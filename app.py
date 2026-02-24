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
                INSERT INTO users (telegram_id, username, player_id, last_active, is_online)
                VALUES (%s, %s, %s, NOW(), true)
                RETURNING id
            """, (data['telegram_id'], data['username'], player_id))
            new_id = cursor.fetchone()[0]
            conn.commit()
            return jsonify({"status": "ok", "new_user": True, "user_id": new_id, "player_id": player_id})
        else:
            cursor.execute("""
                UPDATE users SET last_active = NOW(), is_online = true
                WHERE telegram_id = %s
                RETURNING id, player_id
            """, (data['telegram_id'],))
            user_data = cursor.fetchone()
            conn.commit()
            return jsonify({"status": "ok", "new_user": False, "user_id": user_data[0], "player_id": user_data[1]})
    
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
# ЗАПУСК СЕРВЕРА (для Timeweb Cloud не нужен)
# ============================================
if __name__ == '__main__':
    print("🚀 Pingster backend запускается...")
    print(f"📡 Сервер будет доступен по адресу: https://matk91589-dev-pinster-0b38.twc1.net")
    # Timeweb Cloud использует Gunicorn, поэтому этот код не выполнится
    # app.run(host='0.0.0.0', port=5000, debug=True) - ЗАКОММЕНТИРОВАНО
