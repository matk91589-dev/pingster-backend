import telebot
import requests
import time
import os
import threading
import traceback
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from dotenv import load_dotenv

load_dotenv()

# ============================================
# КОНФИГУРАЦИЯ
# ============================================
TOKEN = os.getenv('BOT_TOKEN')
API_URL = os.getenv('API_URL', 'https://matk91589-dev-pingster-backend-cee8.twc1.net/api')
FRONTEND_URL = os.getenv('FRONTEND_URL', 'https://matk91589-dev-pinster-0530.twc1.net')
FORUM_USERNAME = os.getenv('FORUM_USERNAME', 'pingster_team')
FORUM_LINK = os.getenv('FORUM_LINK', 'https://t.me/pingster_team')
SUPPORT_USERNAME = os.getenv('SUPPORT_USERNAME', 'pingster_support')

# 🔥 БЕЛЫЙ СПИСОК
ALLOWED_USERS = [5015478106, 8541469401]

if not TOKEN:
    raise ValueError("❌ BOT_TOKEN не найден в .env файле!")

bot = telebot.TeleBot(TOKEN)
user_messages = {}

# ============================================
# ФУНКЦИИ
# ============================================

def check_server_awake():
    try:
        r = requests.get(f'{API_URL.replace("/api", "")}/health', timeout=5)
        print(f"🔍 Проверка сервера: {r.status_code}")
        return r.status_code == 200
    except Exception as e:
        print(f"❌ Сервер не отвечает: {e}")
        return False

def wake_up_server():
    try:
        requests.get(f'{API_URL.replace("/api", "")}/health', timeout=3)
        print("🔔 Пинг сервера отправлен")
    except:
        pass

def delete_old_command(user_id, msg_type):
    if user_id in user_messages and msg_type in user_messages[user_id]:
        old = user_messages[user_id][msg_type]
        if 'user' in old:
            try:
                bot.delete_message(user_id, old['user'])
            except:
                pass
        if 'bot' in old:
            try:
                bot.delete_message(user_id, old['bot'])
            except:
                pass

def save_command_message(user_id, msg_type, user_msg_id, bot_msg_id):
    if user_id not in user_messages:
        user_messages[user_id] = {}
    user_messages[user_id][msg_type] = {'user': user_msg_id, 'bot': bot_msg_id}

def register_user(telegram_id, username):
    """Регистрирует пользователя в API и возвращает player_id"""
    print(f"📝 Регистрация пользователя: tg_id={telegram_id}, username={username}")
    try:
        payload = {
            'telegram_id': telegram_id,
            'username': username
        }
        print(f"📤 POST {API_URL}/user/init | payload: {payload}")
        
        response = requests.post(
            f'{API_URL}/user/init',
            json=payload,
            timeout=10
        )
        
        print(f"📥 Ответ API: status={response.status_code}, body={response.text[:200]}")
        
        if response.status_code == 200:
            data = response.json()
            player_id = data.get('player_id')
            print(f"✅ player_id получен: {player_id}")
            return player_id
        else:
            print(f"❌ API вернул ошибку: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Ошибка регистрации: {e}")
        traceback.print_exc()
        return None

# ============================================
# ОБРАБОТЧИК /START
# ============================================

@bot.message_handler(commands=['start'])
def start(message):
    telegram_id = message.from_user.id
    username = message.from_user.username or f'user_{telegram_id}'
    user_msg_id = message.message_id
    
    print(f"\n{'='*50}")
    print(f"🚀 /start от @{username} (id: {telegram_id})")
    print(f"{'='*50}")
    
    # Удаляем старые сообщения
    delete_old_command(telegram_id, 'start')
    
    # Проверяем сервер
    if not check_server_awake():
        wake_up_server()
        bot_msg = bot.send_message(
            telegram_id,
            "♻️ **загрузка сервера**\n\n"
            "Пожалуйста, подождите 5 секунд и нажмите /start снова.",
            parse_mode='Markdown'
        )
        save_command_message(telegram_id, 'start', user_msg_id, bot_msg.message_id)
        print("⏳ Сервер спит, ждём")
        return
    
    # 🔥 РЕГИСТРИРУЕМ ВСЕХ (и тестеров и обычных)
    player_id = register_user(telegram_id, username)
    print(f"🎮 Игровой ID: {player_id}")
    
    # Проверяем доступ
    is_allowed = telegram_id in ALLOWED_USERS
    print(f"🔐 Доступ: {'РАЗРЕШЁН' if is_allowed else 'ЗАКРЫТ (тестер)'}")
    
    if not is_allowed:
        # Обычный пользователь — только канал
        text = (
            f"***@{username}***\n"
            f"Добро пожаловать в Pingster!\n\n"
            f"👤 твой игровой id: `{player_id or '—'}`\n\n"
            f"🚧 Приложение пока в разработке.\n"
            f"👇 Перейти в канал"
        )
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton(
            text="📢 Telegram канал",
            url="https://t.me/pingster_team_channel"
        ))
        
        bot_msg = bot.send_message(
            telegram_id, 
            text, 
            parse_mode='Markdown', 
            reply_markup=markup
        )
        save_command_message(telegram_id, 'start', user_msg_id, bot_msg.message_id)
        print("✅ Сообщение для обычного юзера отправлено")
        return
    
    # Тестер — полный доступ
    text = (
        f"***@{username}***\n"
        f"Добро пожаловать в Pingster!\n\n"
        f"👤 твой игровой id: `{player_id or '—'}`\n\n"
        f"👇 Нажми кнопку ниже, чтобы начать:"
    )
    
    cache_buster = int(time.time())
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(
        text="🚀 ЗАПУСТИТЬ",
        web_app=WebAppInfo(url=f'{FRONTEND_URL}/?v={cache_buster}&tg_id={telegram_id}')
    ))
    
    bot_msg = bot.send_message(
        telegram_id, 
        text, 
        parse_mode='Markdown', 
        reply_markup=markup
    )
    save_command_message(telegram_id, 'start', user_msg_id, bot_msg.message_id)
    print("✅ Сообщение для тестера отправлено")
    print(f"{'='*50}\n")

# ============================================
# ГОЛОСОВАНИЕ ЗА РЕПУТАЦИЮ
# ============================================

@bot.callback_query_handler(func=lambda call: call.data.startswith('vote:'))
def handle_reputation_vote(call):
    message = call.message
    chat_id = message.chat.id
    message_id = message.message_id
    callback_data = call.data

    vote_type = "👍" if ":up:" in callback_data else "👎"
    print(f"🗳 Голос: {callback_data}")

    # API в фоне
    def send_api():
        try:
            r = requests.post(
                f"{API_URL}/reputation/vote",
                json={"callback_data": callback_data},
                timeout=5
            )
            print(f"📡 API vote: {r.status_code}")
        except Exception as e:
            print(f"❌ API vote error: {e}")
    threading.Thread(target=send_api, daemon=True).start()

    # Ответ на callback
    bot.answer_callback_query(call.id, "✅ Спасибо за оценку!")

    # Ищем ссылку на чат
    chat_link = None
    if message.reply_markup:
        for row in message.reply_markup.keyboard:
            for btn in row:
                if hasattr(btn, 'url') and btn.url:
                    chat_link = btn.url
                    break
            if chat_link:
                break

    # Формируем новый текст
    original_text = message.text or message.caption or ""
    if "Оцените тиммейта:" in original_text:
        base_text = original_text.split("Оцените тиммейта:")[0].strip()
    else:
        base_text = original_text

    new_text = f"{base_text}\n\n✅ Вы оценили тиммейта: {vote_type}"

    # Редактируем сообщение
    try:
        if chat_link:
            link_markup = InlineKeyboardMarkup()
            link_markup.add(InlineKeyboardButton("👉 Перейти в чат", url=chat_link))
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=new_text,
                reply_markup=link_markup
            )
        else:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=new_text,
                reply_markup=None
            )
        print("✅ Сообщение отредактировано")
    except Exception as e:
        print(f"❌ Ошибка редактирования: {e}")

# ============================================
# УДАЛЕНИЕ НЕПОНЯТНЫХ СООБЩЕНИЙ (ТОЛЬКО ТЕКСТ ЮЗЕРОВ)
# ============================================

@bot.message_handler(func=lambda message: True)
def delete_unknown_messages(message):
    """Удаляет ВСЕ текстовые сообщения от юзеров, кроме команд"""
    # Пропускаем команды
    if message.text and message.text.startswith('/'):
        return
    
    # Пропускаем сообщения от ботов
    if message.from_user.is_bot:
        return
    
    # Удаляем только текст (не фото/стикеры/войсы)
    if message.content_type != 'text':
        return
    
    # Удаляем только в личке
    if message.chat.type == 'private':
        try:
            bot.delete_message(message.chat.id, message.message_id)
            print(f"🗑 Удалено сообщение от {message.from_user.id}: {message.text[:50] if message.text else '—'}")
        except Exception as e:
            print(f"⚠️ Ошибка удаления: {e}")

# ============================================
# ЗАПУСК
# ============================================
if __name__ == '__main__':
    print("🤖 Pingster бот запущен!")
    print(f"📡 API: {API_URL}")
    print(f"👥 Тестеры: {ALLOWED_USERS}")
    print(f"📊 Все пользователи регистрируются в БД")
    print()
    
    bot.remove_webhook()
    
    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=20)
        except Exception as e:
            print(f"❌ Ошибка polling: {e}")
            traceback.print_exc()
            time.sleep(5)
