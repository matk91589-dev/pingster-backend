import os
import sys
import time
import random
import hashlib
import hmac
import base64
import threading
import logging
import requests
from datetime import datetime, timedelta
from contextlib import contextmanager
from flask import Flask, request, jsonify
from flask_cors import CORS

# ============================================
# КОНФИГУРАЦИЯ
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Токен бота
BOT_TOKEN = "8484054850:AAGwAcn1URrcKtikJKclqP8Z8oYs0wbIYY8"
FORUM_GROUP_ID = -1003753772298
RULES_TOPIC_ID = 5
SECRET_KEY = os.getenv("SECRET_KEY", "pingster_super_secret_key_2026_change_this")

# ============================================
# МОК-ДАННЫЕ (ВМЕСТО БД)
# ============================================
# Простое хранилище в памяти
mock_users = {}  # telegram_id -> player_id
mock_profiles = {}  # player_id -> profile

def get_player_id_mock(telegram_id):
    return mock_users.get(str(telegram_id))

def set_player_id_mock(telegram_id, player_id):
    mock_users[str(telegram_id)] = player_id

def get_profile_mock(player_id):
    return mock_profiles.get(str(player_id))

def set_profile_mock(player_id, profile):
    mock_profiles[str(player_id)] = profile

def generate_player_id():
    return str(random.randint(10000000, 99999999))

def generate_random_nick():
    chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    return ''.join(random.choice(chars) for _ in range(6))

# Ранги (оставляем для логики)
RANK_TO_VALUE = {
    'Silver 1': 1000, 'Silver 2': 1100, 'Silver 3': 1200, 'Silver 4': 1300,
    'Silver Elite': 1400, 'Gold Nova 1': 1500, 'Gold Nova 2': 1600,
    'Gold Nova 3': 1700, 'Gold Nova Master': 1800, 'Master Guardian 1': 1900,
    'Master Guardian 2': 2000, 'Master Guardian Elite': 2100,
    'Distinguished Master Guardian': 2200, 'Legendary Eagle': 2300,
    'Legendary Eagle Master': 2400, 'Supreme Master First Class': 2500,
    'Global Elite': 2600
}

RANK_LIST = list(RANK_TO_VALUE.keys())

def get_rank_index(rank):
    return RANK_LIST.index(rank) if rank in RANK_LIST else 0

def calculate_score(player, candidate, mode):
    player_rating = int(player['rank']) if player.get('rank', '').isdigit() else 0
    cand_rating = int(candidate.get('rank', '')) if candidate.get('rank', '').isdigit() else 0
    
    if mode in ['faceit', 'premier']:
        rating_diff = abs(player_rating - cand_rating)
    else:
        rating_diff = abs(get_rank_index(player.get('rank', '')) - get_rank_index(candidate.get('rank', ''))) * 100
    
    age_diff = abs((player.get('age') or 0) - (candidate.get('age') or 0))
    return rating_diff + age_diff * 100

def get_range_buckets(mode, style, bucket):
    if mode == 'faceit':
        return bucket - 4, bucket + 4
    elif mode == 'premier' and style == 'tryhard':
        return bucket - 5, bucket + 5
    return None, None

def generate_match_token(match_id, user_id, expires_at):
    data = f"{match_id}:{user_id}:{expires_at}"
    signature = hmac.new(SECRET_KEY.encode(), data.encode(), hashlib.sha256).hexdigest()[:16]
    token = f"{match_id}:{user_id}:{expires_at}:{signature}"
    return base64.urlsafe_b64encode(token.encode()).decode().rstrip('=')

def verify_match_token(token):
    try:
        padded_token = token + '=' * (4 - len(token) % 4) if len(token) % 4 else token
        decoded = base64.urlsafe_b64decode(padded_token.encode()).decode()
        parts = decoded.split(':')
        if len(parts) != 4:
            return None, None
        match_id, user_id, expires_at, signature = parts
        expires_at = int(expires_at)
        if int(time.time()) > expires_at:
            return None, None
        expected_data = f"{match_id}:{user_id}:{expires_at}"
        expected_signature = hmac.new(SECRET_KEY.encode(), expected_data.encode(), hashlib.sha256).hexdigest()[:16]
        if hmac.compare_digest(signature, expected_signature):
            return match_id, user_id
        return None, None
    except Exception as e:
        logger.error(f"Ошибка проверки токена: {e}")
        return None, None

def check_user_in_forum_cached(user_id):
    try:
        response = requests.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember",
            params={"chat_id": FORUM_GROUP_ID, "user_id": int(user_id)},
            timeout=3
        )
        data = response.json()
        status = data.get('result', {}).get('status') if data.get('ok') else None
        return status in ['member', 'administrator', 'creator']
    except Exception as e:
        logger.error(f"Ошибка проверки форума: {e}")
        return False

# ============================================
# ЭНДПОИНТЫ (ВСЕ БЕЗ БД)
# ============================================
@app.route('/', methods=['GET'])
def home():
    return "Pingster backend is running! (DEMO MODE - NO DB)"

@app.route('/api', methods=['GET'])
def api_root():
    return jsonify({"message": "Pingster API is running! (DEMO MODE)", "status": "ok", "demo_mode": True})

# ---------- ПРОФИЛЬ ----------
@app.route('/api/profile/get', methods=['POST'])
def get_profile():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            return jsonify({"error": "Missing telegram_id"}), 400
        
        telegram_id = data['telegram_id']
        player_id = get_player_id_mock(telegram_id)
        
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        profile = get_profile_mock(player_id)
        if not profile:
            return jsonify({"error": "Profile not found"}), 404
        
        return jsonify({
            "status": "ok",
            "nick": profile.get('nick'),
            "age": profile.get('age'),
            "steam_link": profile.get('steam_link'),
            "faceit_link": profile.get('faceit_link'),
            "avatar": profile.get('avatar'),
            "created_at": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Ошибка get_profile: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/profile/update', methods=['POST'])
def update_profile():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            return jsonify({"error": "Missing telegram_id"}), 400
        
        telegram_id = data['telegram_id']
        player_id = get_player_id_mock(telegram_id)
        
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        profile = get_profile_mock(player_id) or {}
        
        for field in ['nick', 'age', 'steam_link', 'faceit_link', 'avatar']:
            if field in data and data[field] is not None:
                profile[field] = data[field]
        
        set_profile_mock(player_id, profile)
        
        return jsonify({
            "status": "ok",
            "nick": profile.get('nick'),
            "age": profile.get('age'),
            "steam_link": profile.get('steam_link'),
            "faceit_link": profile.get('faceit_link'),
            "avatar": profile.get('avatar')
        })
    except Exception as e:
        logger.error(f"Ошибка update_profile: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/profile/avatar/update', methods=['POST'])
def update_avatar():
    try:
        data = request.json
        if not data or 'telegram_id' not in data or 'avatar_url' not in data:
            return jsonify({"error": "Missing fields"}), 400
        
        telegram_id = data['telegram_id']
        player_id = get_player_id_mock(telegram_id)
        
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        profile = get_profile_mock(player_id) or {}
        profile['avatar'] = data['avatar_url']
        set_profile_mock(player_id, profile)
        
        return jsonify({"status": "ok", "avatar_url": data['avatar_url']})
    except Exception as e:
        logger.error(f"Ошибка update_avatar: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/profile/avatar/get', methods=['POST'])
def get_avatar():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            return jsonify({"error": "Missing telegram_id"}), 400
        
        telegram_id = data['telegram_id']
        player_id = get_player_id_mock(telegram_id)
        
        if not player_id:
            return jsonify({"error": "User not found"}), 404
        
        profile = get_profile_mock(player_id) or {}
        return jsonify({"status": "ok", "avatar_url": profile.get('avatar')})
    except Exception as e:
        logger.error(f"Ошибка get_avatar: {e}")
        return jsonify({"error": str(e)}), 500

# ---------- ПОЛЬЗОВАТЕЛЬ ----------
@app.route('/api/user/init', methods=['POST'])
def user_init():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            return jsonify({"error": "Missing telegram_id"}), 400
        
        telegram_id = data['telegram_id']
        username = data.get('username', '')
        
        player_id = get_player_id_mock(telegram_id)
        
        if player_id:
            return jsonify({"status": "ok", "player_id": player_id, "demo_mode": True})
        
        player_id = generate_player_id()
        nick = username if username else generate_random_nick()
        
        set_player_id_mock(telegram_id, player_id)
        set_profile_mock(player_id, {
            "nick": nick,
            "age": None,
            "steam_link": None,
            "faceit_link": None,
            "avatar": None
        })
        
        logger.info(f"✅ [DEMO] Создан пользователь: {telegram_id} -> {player_id}")
        
        return jsonify({"status": "ok", "player_id": player_id, "demo_mode": True})
    except Exception as e:
        logger.error(f"Ошибка user_init: {e}")
        return jsonify({"error": str(e)}), 500

# ---------- ПОИСК (упрощенный, без БД) ----------
@app.route('/api/search/start', methods=['POST'])
def start_search():
    try:
        data = request.json
        if not data or 'telegram_id' not in data:
            return jsonify({"error": "Missing telegram_id"}), 400
        
        logger.info(f"[DEMO] start_search для {data['telegram_id']}")
        
        return jsonify({"status": "searching", "message": "DEMO MODE - поиск не работает без БД"})
    except Exception as e:
        logger.error(f"Ошибка start_search: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/match/check', methods=['POST'])
def check_match():
    return jsonify({"match_found": False, "demo_mode": True})

@app.route('/api/match/status/<int:match_id>', methods=['GET'])
def match_status(match_id):
    return jsonify({"status": "not_found", "demo_mode": True})

@app.route('/api/match/respond', methods=['POST'])
def respond_match():
    return jsonify({"status": "rejected", "demo_mode": True})

@app.route('/api/search/stop', methods=['POST'])
def stop_search():
    return jsonify({"status": "stopped", "deleted": 0, "demo_mode": True})

# ---------- ДРУЗЬЯ ----------
@app.route('/api/friends/list', methods=['POST'])
def friends_list():
    return jsonify({"status": "ok", "friends": [], "demo_mode": True})

@app.route('/api/friends/add', methods=['POST'])
def add_friend():
    return jsonify({"status": "ok", "demo_mode": True})

@app.route('/api/friends/remove', methods=['POST'])
def remove_friend():
    return jsonify({"status": "ok", "demo_mode": True})

# ---------- ИСТОРИЯ МАТЧЕЙ ----------
@app.route('/api/my-matches', methods=['POST'])
def my_matches():
    return jsonify({"status": "ok", "active": [], "history": [], "demo_mode": True})

@app.route('/api/active-match', methods=['POST'])
def active_match():
    return jsonify({"active": False, "demo_mode": True})

# ---------- ИГРЫ ----------
@app.route('/api/game/create', methods=['POST'])
def create_game():
    return jsonify({"error": "DEMO MODE - создание игры отключено"}, 400)

# ---------- ПОИСК ИГРОКОВ ----------
@app.route('/api/users/all', methods=['POST'])
def get_all_users():
    return jsonify({"status": "ok", "users": [], "demo_mode": True})

@app.route('/api/users/search', methods=['POST'])
def search_users():
    return jsonify({"status": "ok", "users": [], "demo_mode": True})

@app.route('/api/user/profile/<player_id>', methods=['POST'])
def get_user_profile(player_id):
    profile = get_profile_mock(player_id)
    if not profile:
        return jsonify({"error": "Profile not found"}), 404
    return jsonify({
        "status": "ok",
        "player_id": player_id,
        "nick": profile.get('nick'),
        "age": profile.get('age'),
        "steam_link": profile.get('steam_link'),
        "faceit_link": profile.get('faceit_link'),
        "avatar": profile.get('avatar')
    })

# ---------- ВЕРИФИКАЦИЯ ТОКЕНА ----------
@app.route('/api/verify-token', methods=['POST'])
def verify_token():
    try:
        data = request.json
        if not data or 'token' not in data or 'user_id' not in data:
            return jsonify({"error": "Missing fields"}), 400
        
        match_id, token_user_id = verify_match_token(data['token'])
        
        if match_id and token_user_id == str(data['user_id']):
            return jsonify({"valid": True, "match_id": match_id})
        return jsonify({"valid": False})
    except Exception as e:
        logger.error(f"Ошибка verify_token: {e}")
        return jsonify({"error": str(e)}), 500

# ---------- ФОРУМ ----------
@app.route('/api/check-forum', methods=['POST'])
def check_forum():
    try:
        data = request.json
        if not data or 'user_id' not in data:
            return jsonify({"error": "Missing user_id"}), 400
        
        in_forum = check_user_in_forum_cached(data['user_id'])
        return jsonify({"in_forum": in_forum})
    except Exception as e:
        logger.error(f"Ошибка check_forum: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================
# ЗАПУСК
# ============================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("🔥 PINGSTER BACKEND - DEMO MODE (БЕЗ БД)")
    print(f"🚀 Запуск на порту {port}")
    print("⚠️ Все данные хранятся в памяти и исчезнут при перезапуске!")
    print("✅ Режим: только тестирование скорости загрузки")
    
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
