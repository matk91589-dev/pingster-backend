from flask import Flask, request, jsonify
import psycopg2
import random
from datetime import datetime, timedelta

app = Flask(__name__)

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
    return jsonify({"message": "Pingster backend is running!"})

# ============================================
# ЭНДПОИНТ 2: Инициализация пользователя
# ============================================
@app.route('/api/user/init', methods=['POST'])
def init_user():
    data = request.json
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        # Проверяем есть ли пользователь
        cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        user = cursor.fetchone()
        
        if not user:
            # Создаем нового
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
            # Обновляем активность
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
        # Получаем user_id по telegram_id
        cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
        user = cursor.fetchone()
        if not user:
            return jsonify({"error": "User not found"}), 404
        
        user_id = user[0]
        
        # Удаляем старые записи этого пользователя из очереди
        cursor.execute("DELETE FROM search_queue WHERE user_id = %s", (user_id,))
        
        # Добавляем в очередь
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
        
        # Ищем мэтч где этот пользователь участвует
        cursor.execute("""
            SELECT * FROM matches 
            WHERE (user1_id = %s OR user2_id = %s) 
            AND status = 'pending'
            ORDER BY id DESC LIMIT 1
        """, (user_id, user_id))
        
        match = cursor.fetchone()
        
        if match:
            # Определяем кто второй игрок
            other_id = match[1] if match[1] != user_id else match[2]
            
            # Получаем данные второго игрока из очереди поиска
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
        
        # Получаем мэтч
        cursor.execute("SELECT * FROM matches WHERE id = %s", (data['match_id'],))
        match = cursor.fetchone()
        
        if not match:
            return jsonify({"error": "Match not found"}), 404
        
        # Определяем, кто из игроков отвечает
        if match[1] == user_id:
            cursor.execute("UPDATE matches SET user1_response = %s WHERE id = %s", (data['response'], data['match_id']))
        elif match[2] == user_id:
            cursor.execute("UPDATE matches SET user2_response = %s WHERE id = %s", (data['response'], data['match_id']))
        else:
            return jsonify({"error": "User not in this match"}), 403
        
        # Проверяем, ответили ли оба
        cursor.execute("SELECT user1_response, user2_response FROM matches WHERE id = %s", (data['match_id'],))
        responses = cursor.fetchone()
        
        if responses[0] == 'accept' and responses[1] == 'accept':
            # Оба приняли
            cursor.execute("UPDATE matches SET status = 'accepted' WHERE id = %s", (data['match_id'],))
            
            # Удаляем обоих из очереди
            cursor.execute("DELETE FROM search_queue WHERE user_id IN (%s, %s)", (match[1], match[2]))
            
            conn.commit()
            return jsonify({"status": "accepted", "both_accepted": True})
        
        elif responses[0] == 'reject' or responses[1] == 'reject':
            # Кто-то отклонил
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
# ЗАПУСК СЕРВЕРА
# ============================================
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
