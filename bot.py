import telebot
import requests
import time
import os
import threading
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

# 🔥 БЕЛЫЙ СПИСОК ДЛЯ ТЕСТИРОВАНИЯ
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
        response = requests.get(f'{API_URL.replace("/api", "")}/health', timeout=5)
        return response.status_code == 200
    except:
        return False

def wake_up_server():
    try:
        requests.get(f'{API_URL.replace("/api", "")}/health', timeout=3)
    except:
        pass

def delete_old_command(user_id, msg_type):
    if user_id in user_messages and msg_type in user_messages[user_id]:
        old = user_messages[user_id][msg_type]
        if 'user' in old:
            try: bot.delete_message(user_id, old['user'])
            except: pass
        if 'bot' in old:
            try: bot.delete_message(user_id, old['bot'])
            except: pass

def save_command_message(user_id, msg_type, user_msg_id, bot_msg_id):
    if user_id not in user_messages:
        user_messages[user_id] = {}
    user_messages[user_id][msg_type] = {'user': user_msg_id, 'bot': bot_msg_id}

def register_user(telegram_id, username):
    try:
        response = requests.post(
            f'{API_URL}/user/init',
            json={'telegram_id': telegram_id, 'username': username},
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            return data.get('player_id')
    except: pass
    return None

# ============================================
# ОБРАБОТЧИКИ КОМАНД
# ============================================

@bot.message_handler(commands=['start'])
def start(message):
    telegram_id = message.from_user.id
    username = message.from_user.username or 'юзер'
    user_msg_id = message.message_id
    
    delete_old_command(telegram_id, 'start')
    
    # 🔥 ПРОВЕРКА ДОСТУПА
    if telegram_id not in ALLOWED_USERS:
        bot.send_message(
            telegram_id,
            "🚧 Приложение пока в разработке.\n\n"
            "Скоро запустимся для всех!\n"
            "Подпишись на канал чтобы не пропустить: @pingster_team_channel"
        )
        return
    
    if not check_server_awake():
        wake_up_server()
        bot_msg = bot.send_message(
            telegram_id,
            "♻️ **загрузка сервера**\n\n"
            "Пожалуйста, подождите 5 секунд и нажмите /start снова.",
            parse_mode='Markdown'
        )
        save_command_message(telegram_id, 'start', user_msg_id, bot_msg.message_id)
        return
    
    player_id = register_user(telegram_id, username)
    
    text = f"***@{username}***\n"
    text += f"Добро пожаловать в Pingster!\n\n"
    text += f"👤 твой игровой id: {player_id or '—'}\n\n"
    text += f"👇 Нажми кнопку ниже, чтобы начать:"
    
    cache_buster = int(time.time())
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(
        text="🚀 ЗАПУСТИТЬ",
        web_app=WebAppInfo(url=f'{FRONTEND_URL}/?v={cache_buster}&tg_id={telegram_id}')
    ))
    
    bot_msg = bot.send_message(telegram_id, text, parse_mode='Markdown', reply_markup=markup)
    save_command_message(telegram_id, 'start', user_msg_id, bot_msg.message_id)

@bot.message_handler(commands=['support'])
def support_command(message):
    telegram_id = message.from_user.id
    user_msg_id = message.message_id
    
    delete_old_command(telegram_id, 'support')
    
    text = f"📞 **Поддержка Pingster**\n\n👇 написать в поддержку"
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(
        text="💬 ПОДДЕРЖКА",
        url=f"https://t.me/{SUPPORT_USERNAME}"
    ))
    
    bot_msg = bot.send_message(telegram_id, text, parse_mode='Markdown', reply_markup=markup)
    save_command_message(telegram_id, 'support', user_msg_id, bot_msg.message_id)

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

    def send_api():
        try:
            requests.post(f"{API_URL}/reputation/vote", json={"callback_data": callback_data}, timeout=3)
        except Exception as e:
            print(f"API error: {e}")
    threading.Thread(target=send_api, daemon=True).start()

    bot.answer_callback_query(call.id, "✅ Спасибо за оценку!")

    chat_link = None
    if message.entities:
        for entity in message.entities:
            if entity.type == "text_link":
                chat_link = entity.url
                break

    if not chat_link and message.reply_markup:
        for row in message.reply_markup.keyboard:
            for btn in row:
                if hasattr(btn, 'url') and btn.url:
                    chat_link = btn.url
                    break
            if chat_link: break

    original_text = message.text or message.caption or ""
    if "Оцените тиммейта:" in original_text:
        base_text = original_text.split("Оцените тиммейта:")[0].strip()
    else:
        base_text = original_text

    new_text = f"{base_text}\n\n✅ Вы оценили тиммейта: {vote_type}"

    try:
        if chat_link:
            link_position = base_text.find("👉 Перейти в чат")
            if link_position != -1:
                entity = {"type": "text_link", "offset": link_position, "length": len("👉 Перейти в чат"), "url": chat_link}
                bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=new_text, reply_markup=None, entities=[entity], disable_web_page_preview=True)
            else:
                bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=new_text, reply_markup=None)
        else:
            bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=new_text, reply_markup=None)
    except Exception as e:
        print(f"❌ Ошибка редактирования: {e}")
        try:
            bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=new_text, reply_markup=None)
        except: pass

# ============================================
# УДАЛЕНИЕ НЕПОНЯТНЫХ СООБЩЕНИЙ
# ============================================
@bot.message_handler(func=lambda message: True)
def delete_unknown_messages(message):
    if message.text and message.text.startswith('/'): return
    if message.from_user.is_bot: return
    if message.chat.type == 'private':
        try: bot.delete_message(message.from_user.id, message.message_id)
        except: pass

# ============================================
# ЗАПУСК
# ============================================
if __name__ == '__main__':
    print("🤖 Pingster бот запущен!")
    print("🔒 Режим тестирования — доступ только у избранных")
    bot.remove_webhook()
    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=20)
        except Exception as e:
            print(f"❌ Ошибка polling: {e}")
            time.sleep(5)
