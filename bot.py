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
user_messages = {}  # {user_id: {'start': {'user': msg_id, 'bot': msg_id}, 'support': {...}}}
temp_messages = {}  # для форума

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
        "После этого откроется главное меню! ",
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
    user_msg_id = message.message_id
    
    # Удаляем старые сообщения start
    delete_old_command(telegram_id, 'start')
    
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
    
    bot_msg = bot.send_message(telegram_id, text, parse_mode='Markdown', reply_markup=markup)
    save_command_message(telegram_id, 'start', user_msg_id, bot_msg.message_id)

@bot.message_handler(commands=['support'])
def support_command(message):
    telegram_id = message.from_user.id
    user_msg_id = message.message_id
    
    # Удаляем старые сообщения support
    delete_old_command(telegram_id, 'support')
    
    text = f" **Поддержка Pingster**\n\n👇 написать в поддержку"
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton(
        text="поддержка",
        url=f"https://t.me/{SUPPORT_USERNAME}"
    ))
    
    bot_msg = bot.send_message(telegram_id, text, parse_mode='Markdown', reply_markup=markup)
    save_command_message(telegram_id, 'support', user_msg_id, bot_msg.message_id)
    
    # Запускаем таймер на удаление новых сообщений через 30 секунд
    threading.Thread(target=delete_message_later, args=(telegram_id, user_msg_id, 30), daemon=True).start()
    threading.Thread(target=delete_message_later, args=(telegram_id, bot_msg.message_id, 30), daemon=True).start()

# ============================================
# УДАЛЕНИЕ НЕПОНЯТНЫХ СООБЩЕНИЙ ОТ ПОЛЬЗОВАТЕЛЯ
# ============================================

@bot.message_handler(func=lambda message: True)
def delete_unknown_messages(message):
    """Удаляет все сообщения от пользователя, которые не являются командами"""
    telegram_id = message.from_user.id
    
    # Пропускаем команды (они уже обработаны)
    if message.text and message.text.startswith('/'):
        return
    
    # Удаляем любое другое сообщение от пользователя
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
        
        # Удаляем сообщение с кнопками
        try:
            bot.delete_message(user_id, call.message.message_id)
            print(f"🗑 Сообщение с кнопками удалено")
        except Exception as e:
            print(f"⚠️ Не удалось удалить сообщение: {e}")
        
        # Регистрируем пользователя
        player_id = None
        try:
            print(f"📡 Регистрация пользователя в API...")
            response = requests.post(
                f'{API_URL}/user/init',
                json={'telegram_id': user_id, 'username': username},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                player_id = data.get('player_id')
                print(f"✅ Пользователь зарегистрирован, player_id: {player_id}")
            else:
                print(f"⚠️ Ошибка API: {response.status_code}")
        except Exception as e:
            print(f"❌ Ошибка регистрации: {e}")
        
        # Формируем приветственное сообщение
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
        
        # Отправляем меню
        try:
            new_msg = bot.send_message(user_id, text, parse_mode='Markdown', reply_markup=markup)
            print(f"✅ Меню отправлено, msg_id: {new_msg.message_id}")
            
            # Сохраняем в user_messages
            if user_id not in user_messages:
                user_messages[user_id] = {}
            user_messages[user_id]['start'] = {
                'user': 0,  # Нет сообщения пользователя
                'bot': new_msg.message_id
            }
        except Exception as e:
            print(f"❌ Ошибка отправки меню с Markdown: {e}")
            # Пробуем без Markdown
            try:
                plain_text = f"@{username}\nДобро пожаловать в Pingster!\n\n👤 твой игровой id: {player_id or '—'}\n\n👇 Нажми кнопку ниже, чтобы начать:"
                new_msg = bot.send_message(user_id, plain_text, reply_markup=markup)
                print(f"✅ Меню отправлено (plain text)")
                
                if user_id not in user_messages:
                    user_messages[user_id] = {}
                user_messages[user_id]['start'] = {
                    'user': 0,
                    'bot': new_msg.message_id
                }
            except Exception as e2:
                print(f"❌ Полная ошибка отправки: {e2}")
    else:
        print(f"❌ {user_id} НЕ в форуме!")
        bot.answer_callback_query(
            call.id,
            "❌ Ты ещё не в форуме! Сначала нажми «Вступить в форум»",
            show_alert=True
        )

@bot.callback_query_handler(func=lambda call: call.data.startswith('vote_'))
def handle_reputation_vote(call):
    """Обрабатывает голоса за репутацию — эти сообщения НЕ УДАЛЯЕМ!"""
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
    print("🗑 Непонятные сообщения удаляются сразу")
    print("🔄 Новый /start заменяет старый")
    print("⏱ /support удаляется через 30с")
    print("👍 Сообщения с оценкой тиммейта НЕ удаляются")
    
    bot.remove_webhook()
    
    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=20)
        except Exception as e:
            print(f"❌ Ошибка: {e}")
            time.sleep(5)
