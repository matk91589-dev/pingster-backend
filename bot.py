import telebot
import requests
import random
import time
import os
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# ============================================
# КОНФИГУРАЦИЯ ИЗ .env
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

# Хранилище временных сообщений
temp_messages = {}

# ============================================
# ФУНКЦИИ
# ============================================

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

def send_welcome_with_webapp(user_id, player_id=None):
    """Отправляет приветствие с кнопкой ЗАПУСТИТЬ"""
    cache_buster = int(time.time())
    
    markup = InlineKeyboardMarkup()
    
    # 🔥 ГЛАВНАЯ КНОПКА ЗАПУСКА
    webapp_btn = InlineKeyboardButton(
        text="🚀 ЗАПУСТИТЬ PINGSTER",
        web_app=WebAppInfo(url=f'{FRONTEND_URL}/?v={cache_buster}&tg_id={user_id}')
    )
    markup.add(webapp_btn)
    
    # Дополнительные кнопки
    support_btn = InlineKeyboardButton(
        text="📞 Поддержка",
        url=f"https://t.me/{SUPPORT_USERNAME}"
    )
    markup.add(support_btn)
    
    text = f"🎮 **Добро пожаловать в Pingster!**\n\n"
    text += f"🔥 **Tinder для поиска тиммейтов в CS2**\n\n"
    if player_id:
        text += f"👤 Твой ID: `{player_id}`\n"
    text += f"\n👇 Нажми кнопку ниже, чтобы начать:"
    
    bot.send_message(
        user_id,
        text,
        parse_mode='Markdown',
        reply_markup=markup
    )

def send_forum_invite(user_id):
    """Отправляет приглашение вступить в форум"""
    markup = InlineKeyboardMarkup(row_width=1)
    
    forum_btn = InlineKeyboardButton(
        text="📢 Вступить в форум Pingster",
        url=FORUM_LINK
    )
    
    check_btn = InlineKeyboardButton(
        text="✅ Я вступил, продолжить",
        callback_data="check_forum"
    )
    
    markup.add(forum_btn, check_btn)
    
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
    username = message.from_user.username or ''
    
    print(f"👉 /start от {username} (ID: {telegram_id})")
    
    # Проверяем форум
    if not is_user_in_forum(telegram_id):
        send_forum_invite(telegram_id)
        return
    
    # Регистрируем юзера
    try:
        response = requests.post(
            f'{API_URL}/user/init',
            json={'telegram_id': telegram_id, 'username': username},
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            send_welcome_with_webapp(telegram_id, data.get('player_id'))
        else:
            send_welcome_with_webapp(telegram_id)
    except:
        send_welcome_with_webapp(telegram_id)

@bot.message_handler(commands=['support'])
def support(message):
    """Команда /support"""
    bot.send_message(
        message.chat.id,
        f" **Поддержка Pingster** \n\n"
        f"По всем вопросам писать:\n"
        f"👉 @{SUPPORT_USERNAME}",
        parse_mode='Markdown'
    )

# ============================================
# ОБРАБОТЧИК КНОПОК
# ============================================

@bot.callback_query_handler(func=lambda call: call.data == "check_forum")
def check_forum_callback(call):
    user_id = call.from_user.id
    
    if is_user_in_forum(user_id):
        delete_temp_message(user_id)
        
        bot.edit_message_text(
            "✅ Отлично! Ты в форуме!\n\nЗагружаем главное меню...",
            user_id,
            call.message.message_id
        )
        
        # Регистрируем
        try:
            response = requests.post(
                f'{API_URL}/user/init',
                json={'telegram_id': user_id, 'username': 'from_forum'},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                send_welcome_with_webapp(user_id, data.get('player_id'))
            else:
                send_welcome_with_webapp(user_id)
        except:
            send_welcome_with_webapp(user_id)
    else:
        bot.answer_callback_query(
            call.id,
            "❌ Ты ещё не в форуме! Сначала нажми «Вступить в форум»",
            show_alert=True
        )

@bot.callback_query_handler(func=lambda call: call.data.startswith('vote_'))
def handle_reputation_vote(call):
    """Обрабатывает голоса за репутацию"""
    user_id = call.from_user.id
    callback_data = call.data
    message = call.message
    
    print(f"🗳 Голос: {callback_data} от {user_id}")
    
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
    
    bot.remove_webhook()
    
    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=20)
        except Exception as e:
            print(f"❌ Ошибка: {e}")
            time.sleep(5)
