from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # разрешаем запросы с твоего фронтенда

# Простой тестовый маршрут
@app.route('/')
def home():
    return jsonify({"message": "Pingster backend is running!"})

# Маршрут для поиска (заглушка, позже добавим логику)
@app.route('/find_match', methods=['POST'])
def find_match():
    data = request.json
    print("Получен запрос на поиск:", data)

    # Пока просто возвращаем тестовый ответ
    return jsonify({
        "status": "searching",
        "message": "Поиск запущен (тестовая заглушка)"
    })

if __name__ == '__main__':
    app.run(debug=True)
