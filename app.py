from flask import Flask, request, jsonify
import psycopg2
import random

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

# Функция генерации player_id
def generate_player_id():
    return str(random.randint(10000000, 99999999))

# Эндпоинт входа
@app.route('/api/user/init', methods=['POST'])
def init_user():
    data = request.json
    conn = get_db()
    cursor = conn.cursor()
    
    # Проверяем есть ли пользователь
    cursor.execute("SELECT id FROM users WHERE telegram_id = %s", (data['telegram_id'],))
    user = cursor.fetchone()
    
    if not user:
        # Создаем нового
        player_id = generate_player_id()
        cursor.execute("""
            INSERT INTO users (telegram_id, username, player_id, last_active, is_online)
            VALUES (%s, %s, %s, NOW(), true)
        """, (data['telegram_id'], data['username'], player_id))
        conn.commit()
        print(f"✅ Новый пользователь: {data['username']} (ID: {player_id})")
    else:
        # Обновляем активность
        cursor.execute("""
            UPDATE users SET last_active = NOW(), is_online = true
            WHERE telegram_id = %s
        """, (data['telegram_id'],))
        conn.commit()
        print(f"🔄 Пользователь {data['username']} зашел снова")
    
    cursor.close()
    conn.close()
    return jsonify({"status": "ok"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
