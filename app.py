from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
CORS(app)

# ============================================
# ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ (TimeWeb)
# ============================================
DB_CONFIG = {
    "host": "192.168.0.5",
    "port": "5432",
    "database": "default_db",
    "user": "gen_user",
    "password": "{,@~:5my>jvOAj" 
}

def get_db():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

# ============================================
# СОЗДАНИЕ ТАБЛИЦЫ (вызвать один раз)
# ============================================
@app.route('/setup_db', methods=['GET'])
def setup_db():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT,
                avatar TEXT,
                age INTEGER,
                steam_link TEXT,
                faceit_link TEXT,
                coins INTEGER DEFAULT 1000,
                owned_nicks TEXT[] DEFAULT '{}',
                owned_frames TEXT[] DEFAULT '{}',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({"status": "ok", "message": "Table created"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

# ============================================
# ТЕСТОВЫЙ МАРШРУТ
# ============================================
@app.route('/')
def home():
    return jsonify({"message": "Pingster backend is running!"})

# ============================================
# ЗАГЛУШКА ПОИСКА
# ============================================
@app.route('/find_match', methods=['POST'])
def find_match():
    data = request.json
    print("Получен запрос на поиск:", data)

    # Здесь будет алгоритм подбора
    # Пока просто заглушка
    return jsonify({
        "status": "searching",
        "message": "Поиск запущен (тест)"
    })

# ============================================
# ЗАГЛУШКА СОХРАНЕНИЯ ПРОФИЛЯ
# ============================================
@app.route('/save_profile', methods=['POST'])
def save_profile():
    data = request.json
    print("Сохраняем профиль:", data)

    # Здесь будет сохранение в БД
    # Пока просто заглушка
    return jsonify({
        "status": "ok",
        "message": "Profile saved (test)"
    })

if __name__ == '__main__':
    app.run(debug=True)
