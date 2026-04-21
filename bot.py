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

def check_server_awake():
    """Проверяет, проснулся ли сервер"""
    try:
        response = requests.get(f'{API_URL.replace("/api", "")}/health', timeout=5)
        return response.status_code == 200
    except:
        return False

def wake_up_server():
    """Отправляет пинг для пробуждения сервера"""
    try:
        requests.get(f'{API_URL.replace("/api", "")}/health', timeout=3)
    except:
        pass

def delete_message_later(chat_id, message_id, delay=30):
    """Удаляет сообщение через указанную задержку"""
    time.sleep(delay)
    try:
        bot.delete_message(chat_id, message_id)
    except:
        pass

def delete_old_command(user_id, msg_type):
    """Удаляет старые сообщения команды (и юзера, и бота)"""
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
    """Сохраняет ID сообщений команды"""
    if user_id not in user_messages:
        user_messages[user_id] = {}
    user_messages[user_id][msg_type] = {
        'user': user_msg_id,
        'bot': bot_msg_id
    }

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
        "1. Нажми **«Вступить в форум»**\n"
        "2. Нажми **«Я вступил, продолжить»**\n\n"
        "После этого откроется главное меню!",
        parse_mode='Markdown',
        reply_markup=markup
    )
    save_temp_message(user_id, msg.message_id)

def register_user(telegram_id, username):
    """Регистрирует пользователя в API"""
    try:
        response = requests.post(
            f'{API_URL}/user/init',
            json={'telegram_id': telegram_id, 'username': username},
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            return data.get('player_id')
    except:
        pass
    return None

# ============================================
# ОБРАБОТЧИКИ КОМАНД
# ============================================

@bot.message_handler(commands=['start'])
def start(message):
    telegram_id = message.from_user.id
    username = message.from_user.username or 'юзер'
    user_msg_id = message.message_id
    
    # Удаляем старые сообщения start
    delete_old_command(telegram_id, 'start')
    
    # 🔥 ПРОВЕРЯЕМ СЕРВЕР
    if not check_server_awake():
        # Будим сервер
        wake_up_server()
        bot_msg = bot.send_message(
            telegram_id,
            " ♻️ **загрузка сервера**\n\n"
            "Пожалуйста, подождите 5 секунд и нажмите /start снова.",
            parse_mode='Markdown'
        )
        save_command_message(telegram_id, 'start', user_msg_id, bot_msg.message_id)
        return
    
    # Проверяем форум
    if not is_user_in_forum(telegram_id):
        send_forum_invite(telegram_id)
        return
    
    # Регистрируем пользователя
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
    
    threading.Thread(target=delete_message_later, args=(telegram_id, user_msg_id, 30), daemon=True).start()
    threading.Thread(target=delete_message_later, args=(telegram_id, bot_msg.message_id, 30), daemon=True).start()

# ============================================
# УДАЛЕНИЕ НЕПОНЯТНЫХ СООБЩЕНИЙ
# ============================================

@bot.message_handler(func=lambda message: True)
def delete_unknown_messages(message):
    telegram_id = message.from_user.id
    
    if message.text and message.text.startswith('/'):
        return
    
    try:
        bot.delete_message(telegram_id, message.message_id)
        print(f"🗑 Удалено сообщение от {telegram_id}")
    except:
        pass

# ============================================
# ОБРАБОТЧИК КНОПОК
# ============================================

@bot.callback_query_handler(func=lambda call: call.data == "check_forum")
def check_forum_callback(call):
    user_id = call.from_user.id
    username = call.from_user.username or 'юзер'
    
    print(f"🔍 Проверка форума для {user_id} (@{username})")
    
    if is_user_in_forum(user_id):
        print(f"✅ {user_id} в форуме!")
        delete_temp_message(user_id)
        
        try:
            bot.delete_message(user_id, call.message.message_id)
        except:
            pass
        
        player_id = register_user(user_id, username)
        
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
        
        try:
            new_msg = bot.send_message(user_id, text, parse_mode='Markdown', reply_markup=markup)
            print(f"✅ Меню отправлено, msg_id: {new_msg.message_id}")
            
            if user_id not in user_messages:
                user_messages[user_id] = {}
            user_messages[user_id]['start'] = {'user': 0, 'bot': new_msg.message_id}
        except:
            plain_text = f"@{username}\nДобро пожаловать в Pingster!\n\n👤 твой игровой id: {player_id or '—'}\n\n👇 Нажми кнопку ниже, чтобы начать:"
            new_msg = bot.send_message(user_id, plain_text, reply_markup=markup)
            
            if user_id not in user_messages:
                user_messages[user_id] = {}
            user_messages[user_id]['start'] = {'user': 0, 'bot': new_msg.message_id}
    else:
        bot.answer_callback_query(
            call.id,
            "❌ Ты ещё не в форуме! Сначала нажми «Вступить в форум»",
            show_alert=True
        )

@bot.callback_query_handler(func=lambda call: call.data.startswith('vote:'))
def handle_reputation_vote(call):
    callback_data = call.data
    message = call.message
    user_id = call.from_user.id

    vote_type = "👍" if ":up:" in callback_data else "👎"

    print(f"🗳 Получен голос: {callback_data}")

    # 1. Ответ
    bot.answer_callback_query(call.id, "✅ Спасибо за оценку!")

    # 2. API
    try:
        requests.post(
            f'{API_URL}/reputation/vote',
            json={'callback_data': callback_data},
            timeout=5
        )
    except Exception as e:
        print(f"❌ Ошибка API: {e}")

    # 3. чат ссылка
    chat_link = None
    if message.reply_markup:
        for row in message.reply_markup.inline_keyboard:
            for btn in row:
                if btn.url:
                    chat_link = btn.url

    # 4. удаление
    try:
        bot.delete_message(
            chat_id=message.chat.id,
            message_id=message.message_id
        )
    except Exception as e:
        print(f"⚠️ Не удалось удалить: {e}")

    # 5. новое сообщение
    new_markup = None
    if chat_link:
        new_markup = InlineKeyboardMarkup()
        new_markup.add(InlineKeyboardButton("👉 Перейти в чат", url=chat_link))

    try:
        bot.send_message(
            chat_id=user_id,
            text=f"🎮 У вас создан мэтч!\n\n✅ Вы оценили тиммейта: {vote_type}",
            reply_markup=new_markup
        )
    except Exception as e:
        print(f"❌ Ошибка отправки: {e}")

# ============================================
# ЗАПУСК
# ============================================
if __name__ == '__main__':
    print("🤖 Pingster бот запущен!")
    print(f"📡 API: {API_URL}")
    print(f"🌐 FRONTEND: {FRONTEND_URL}")
    print(f"📞 Поддержка: @{SUPPORT_USERNAME}")
    print("⏳ Проверка сервера при /start")
    print("👍 Репутация: упрощённая отправка")
    
    bot.remove_webhook()
    
    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=20)
        except Exception as e:
            print(f"❌ Ошибка polling: {e}")
            time.sleep(5)
