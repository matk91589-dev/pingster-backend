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

if not TOKEN:
    raise ValueError("❌ BOT_TOKEN не найден в .env файле!")

bot = telebot.TeleBot(TOKEN)

# Хранилище сообщений
user_messages = {}
temp_messages = {}

# ============================================
# ФУНКЦИИ
# ============================================

def delete_message_later(chat_id, message_id, delay=30):
    """Удаляет сообщение через указанную задержку"""
    time.sleep(delay)
    try:
        bot.delete_message(chat_id, message_id)
    except:
        pass

def replace_message(user_id, msg_type, new_msg):
    """Заменяет старое сообщение того же типа"""
    if user_id in user_messages and msg_type in user_messages[user_id]:
        try:
            bot.delete_message(user_id, user_messages[user_id][msg_type])
        except:
            pass
    
    if user_id not in user_messages:
        user_messages[user_id] = {}
    user_messages[user_id][msg_type] = new_msg.message_id
    
    # Авто-удаление для support
    if msg_type == 'support':
        threading.Thread(target=delete_message_later, args=(user_id, new_msg.message_id, 30), daemon=True).start()

def is_user_in_forum(user_id):
    """Проверяет, состоит ли пользователь в форуме"""
    try:
        response = requests.get(
            f"https://api.telegram.org/bot{TOKEN}/getChatMember",
            params={"chat_id": f"@{FORUM_USERNAME}", "user_id": user_id},
            timeout=10
        )
        data = response.json()
        if data.get('ok'):
            status = data['result']['status']
            return status in ['member', 'creator', 'administrator']
        return False
    except:
        return False

def save_temp_message(user_id, message_id):
    temp_messages[user_id] = message_id

def delete_temp_message(user_id):
    if user_id in temp_messages:
        try:
            bot.delete_message(user_id, temp_messages[user_id])
        except:
            pass
        del temp_messages[user_id]

def send_forum_invite(user_id):
    """Отправляет приглашение вступить в форум"""
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("📢 Вступить в форум Pingster", url=FORUM_LINK))
    markup.add(InlineKeyboardButton("✅ Я вступил, продолжить", callback_data="check_forum"))
    
    msg = bot.send_message(
        user_id,
        "🎮 **Добро пожаловать в Pingster!**\n\n"
        "Для начала нужно вступить в наш форум:\n\n"
        "1️⃣ Нажми **«Вступить в форум»**\n"
        "2️⃣ Нажми **«Я вступил, продолжить»**\n\n"
        "После этого откроется главное меню! 🔥",
        parse_mode='Markdown',
        reply_markup=markup
    )
    save_temp_message(user_id, msg.message_id)

# ============================================
# ОБРАБОТЧИКИ КОМАНД
# ============================================

@bot.message_handler(commands=['start'])
def start(message):
    telegram_id = message.from_user.id
    username = message.from_user.username or 'юзер'
    
    # Удаляем сообщение с командой
    try:
        bot.delete_message(telegram_id, message.message_id)
    except:
        pass
    
    # Проверяем форум
    if not is_user_in_forum(telegram_id):
        send_forum_invite(telegram_id)
        return
    
    # Регистрируем
    player_id = None
    try:
        response = requests.post(
            f'{API_URL}/user/init',
            json={'telegram_id': telegram_id, 'username': username},
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            player_id = data.get('player_id')
    except:
        pass
    
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
    
    new_msg = bot.send_message(telegram_id, text, parse_mode='Markdown', reply_markup=markup)
    replace_message(telegram_id, 'start', new_msg)

@bot.message_handler(commands=['support'])
def support_command(message):
    telegram_id = message.from_user.id
    
    try:
        bot.delete_message(telegram_id, message.message_id)
    except:
        pass
    
    text = f"📞 **Поддержка Pingster**\n\n👇 написать в поддержку"
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(
        text="💬 ПОДДЕРЖКА",
        url=f"https://t.me/{SUPPORT_USERNAME}"
    ))
    
    new_msg = bot.send_message(telegram_id, text, parse_mode='Markdown', reply_markup=markup)
    replace_message(telegram_id, 'support', new_msg)

# ============================================
# УДАЛЕНИЕ ЛЮБЫХ ДРУГИХ СООБЩЕНИЙ
# ============================================

@bot.message_handler(func=lambda message: True)
def delete_other_messages(message):
    telegram_id = message.from_user.id
    
    if message.text and message.text.startswith('/'):
        return
    
    try:
        bot.delete_message(telegram_id, message.message_id)
    except:
        pass

# ============================================
# ОБРАБОТЧИК КНОПОК
# ============================================

@bot.callback_query_handler(func=lambda call: call.data == "check_forum")
def check_forum_callback(call):
    user_id = call.from_user.id
    username = call.from_user.username or 'юзер'
    
    if is_user_in_forum(user_id):
        delete_temp_message(user_id)
        
        bot.edit_message_text(
            "✅ Отлично! Ты в форуме!\n\nЗагружаем главное меню...",
            user_id,
            call.message.message_id
        )
        
        player_id = None
        try:
            response = requests.post(
                f'{API_URL}/user/init',
                json={'telegram_id': user_id, 'username': username},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                player_id = data.get('player_id')
        except:
            pass
        
        text = f"***@{username}***\n"
        text += f"Добро пожаловать в Pingster!\n\n"
        text += f"👤 твой игровой id: {player_id or '—'}\n\n"
        text += f"👇 Нажми кнопку ниже, чтобы начать:"
        
        cache_buster = int(time.time())
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton(
            text="🚀 ЗАПУСТИТЬ",
            web_app=WebAppInfo(url=f'{FRONTEND_URL}/?v={cache_buster}&tg_id={user_id}')
        ))
        
        new_msg = bot.send_message(user_id, text, parse_mode='Markdown', reply_markup=markup)
        replace_message(user_id, 'start', new_msg)
    else:
        bot.answer_callback_query(
            call.id,
            "❌ Ты ещё не в форуме! Сначала нажми «Вступить в форум»",
            show_alert=True
        )

@bot.callback_query_handler(func=lambda call: call.data.startswith('vote_'))
def handle_reputation_vote(call):
    user_id = call.from_user.id
    callback_data = call.data
    message = call.message
    
    try:
        response = requests.post(
            f'{API_URL}/reputation/vote',
            json={
                'callback_data': callback_data,
                'message': {
                    'message_id': message.message_id,
                    'chat': {'id': user_id},
                    'text': message.text,
                    'reply_markup': message.reply_markup.to_dict() if message.reply_markup else None
                }
            },
            timeout=10
        )
        
        if response.status_code == 200:
            bot.answer_callback_query(call.id, "✅ Спасибо за оценку!")
        else:
            bot.answer_callback_query(call.id, "❌ Ошибка, попробуй позже")
    except:
        bot.answer_callback_query(call.id, "❌ Ошибка соединения")

# ============================================
# ЗАПУСК
# ============================================
if __name__ == '__main__':
    print("🤖 Pingster бот запущен!")
    print(f"📡 API: {API_URL}")
    print(f"🌐 FRONTEND: {FRONTEND_URL}")
    print(f"📞 Поддержка: @{SUPPORT_USERNAME}")
    print("🗑 /support удаляется через 30с")
    
    bot.remove_webhook()
    
    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=20)
        except Exception as e:
            print(f"❌ Ошибка: {e}")
            time.sleep(5)
