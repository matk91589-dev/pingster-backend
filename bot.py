import telebot
import requests
import random
import time
import os
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# ============================================
# КОНФИГУРАЦИЯ ИЗ .env
# ============================================
# Токен бота (теперь в .env)
TOKEN = os.getenv('BOT_TOKEN')

# URL API (БЭКЕНД)
API_URL = os.getenv('API_URL', 'https://matk91589-dev-pingster-backend-cee8.twc1.net/api')

# URL фронтенда (ФРОНТЕНД)
FRONTEND_URL = os.getenv('FRONTEND_URL', 'https://matk91589-dev-pinster-0530.twc1.net')

# ID форума
FORUM_USERNAME = os.getenv('FORUM_USERNAME', 'pingster_team')
FORUM_LINK = os.getenv('FORUM_LINK', 'https://t.me/pingster_team')

# Проверка наличия токена
if not TOKEN:
    raise ValueError("❌ BOT_TOKEN не найден в .env файле! Создай .env с BOT_TOKEN=твой_токен")

bot = telebot.TeleBot(TOKEN)

# Хранилище временных сообщений
temp_messages = {}

# ============================================
# ФУНКЦИИ С ПОВТОРАМИ
# ============================================

def api_request_with_retry(url, json_data, max_retries=3, delay=2):
    """Выполняет запрос к API с повторными попытками при ошибке"""
    for attempt in range(max_retries):
        try:
            response = requests.post(url, json=json_data, timeout=15)
            if response.status_code == 200:
                return response
            print(f"⚠️ Попытка {attempt + 1}: статус {response.status_code}")
        except requests.exceptions.Timeout:
            print(f"⚠️ Попытка {attempt + 1}: таймаут")
        except requests.exceptions.ConnectionError:
            print(f"⚠️ Попытка {attempt + 1}: ошибка соединения")
        except Exception as e:
            print(f"⚠️ Попытка {attempt + 1}: {e}")
        
        if attempt < max_retries - 1:
            time.sleep(delay)
    
    return None

def is_user_in_forum(user_id):
    """Проверяет, состоит ли пользователь в форуме (с повторами)"""
    for attempt in range(3):
        try:
            response = requests.get(
                f"https://api.telegram.org/bot{TOKEN}/getChatMember",
                params={
                    "chat_id": f"@{FORUM_USERNAME}",
                    "user_id": user_id
                },
                timeout=10
            )
            data = response.json()
            print(f"📡 Проверка форума для {user_id}: {data}")
            
            if data.get('ok'):
                status = data['result']['status']
                return status in ['member', 'creator', 'administrator']
            return False
        except Exception as e:
            print(f"❌ Попытка {attempt + 1} ошибки проверки форума: {e}")
            if attempt < 2:
                time.sleep(1)
    
    return False

def save_temp_message(user_id, message_id):
    """Сохраняет ID временного сообщения"""
    temp_messages[user_id] = message_id
    print(f"💾 Сохранено временное сообщение для {user_id}: {message_id}")

def delete_temp_message(user_id):
    """Удаляет временное сообщение"""
    if user_id in temp_messages:
        try:
            bot.delete_message(user_id, temp_messages[user_id])
            print(f"🗑 Удалено сообщение для {user_id}")
        except Exception as e:
            print(f"❌ Ошибка удаления: {e}")
        del temp_messages[user_id]

def send_main_menu(user_id, player_id=None):
    """Отправляет главное меню с кнопкой Mini App"""
    markup = InlineKeyboardMarkup()
    
    cache_buster = int(time.time())
    
    web_app_button = InlineKeyboardButton(
        text="🚀 Открыть Pingster",
        web_app=telebot.types.WebAppInfo(
            url=f'{FRONTEND_URL}/?v={cache_buster}&tg_id={user_id}'
        )
    )
    markup.add(web_app_button)
    
    text = f"🎮 **Добро пожаловать в Pingster!**\n\n"
    if player_id:
        text += f"👤 Твой игровой ID: `{player_id}`\n"
    text += f"⭐ Твоя репутация: **0**\n\n"
    text += f"👇 Нажми кнопку ниже, чтобы открыть Mini App:"
    
    bot.send_message(
        user_id,
        text,
        parse_mode='Markdown',
        reply_markup=markup
    )

def send_forum_invite(user_id):
    """Отправляет приглашение вступить в форум с кнопкой подтверждения"""
    markup = InlineKeyboardMarkup(row_width=1)
    
    forum_btn = InlineKeyboardButton(
        text="📢 1. Вступить в форум Pingster",
        url=FORUM_LINK
    )
    
    check_btn = InlineKeyboardButton(
        text="✅ 2. Я вступил, продолжить",
        callback_data="check_forum"
    )
    
    markup.add(forum_btn, check_btn)
    
    msg = bot.send_message(
        user_id,
        "🎮 **Добро пожаловать в Pingster!**\n\n"
        "Для начала пользования нужно вступить в наш форум.\n\n"
        "**1️⃣ Нажми кнопку «Вступить в форум»**\n"
        "**2️⃣ Нажми «Я вступил, продолжить»**\n\n"
        "После этого откроется главное меню 🔥",
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
    username = message.from_user.username or 'no_username'
    
    print(f"👉 Получен /start от {username} (ID: {telegram_id})")
    print(f"📡 API URL: {API_URL}")
    print(f"🌐 FRONTEND URL: {FRONTEND_URL}")
    
    # ШАГ 1: Проверяем, есть ли юзер в форуме
    if not is_user_in_forum(telegram_id):
        print(f"🟡 Новичок {telegram_id} — отправляем приглашение в форум")
        send_forum_invite(telegram_id)
        return
    
    # 🟢 СТАРЫЙ ЮЗЕР — продолжаем как обычно
    print(f"🟢 Старый юзер {telegram_id} — отправляем главное меню")
    
    # Используем запрос с повторами
    response = api_request_with_retry(
        f'{API_URL}/user/init',
        {'telegram_id': telegram_id, 'username': username},
        max_retries=3,
        delay=2
    )
    
    if response and response.status_code == 200:
        data = response.json()
        if data.get('status') == 'ok':
            send_main_menu(telegram_id, data.get('player_id'))
        else:
            bot.reply_to(message, "❌ Ошибка при регистрации")
    else:
        # Если API не ответил после всех попыток
        bot.reply_to(
            message, 
            "⚠️ Сервер временно недоступен. Пожалуйста, попробуй через 10 секунд.\n\nЕсли проблема повторяется, напиши @pingster_support"
        )

@bot.message_handler(commands=['help'])
def help(message):
    markup = InlineKeyboardMarkup()
    
    cache_buster = int(time.time())
    
    web_app_button = InlineKeyboardButton(
        text="🚀 Открыть Pingster",
        web_app=telebot.types.WebAppInfo(url=f'{FRONTEND_URL}/?v={cache_buster}')
    )
    markup.add(web_app_button)
    
    bot.send_message(
        message.chat.id,
        "🎮 **Pingster — поиск тиммейтов для CS2**\n\n"
        "**Команды:**\n"
        "/start - Начать\n"
        "/help - Помощь\n"
        "/check - Проверить сервер\n\n"
        "**Как это работает:**\n"
        "1. Открой Mini App по кнопке ниже\n"
        "2. Заполни профиль\n"
        "3. Нажми 'Найти тиммейта'\n"
        "4. Прими мэтч и играй!",
        parse_mode='Markdown',
        reply_markup=markup
    )

@bot.message_handler(commands=['check'])
def check(message):
    try:
        response = requests.get(f'{API_URL}', timeout=5)
        if response.status_code == 200:
            bot.reply_to(message, f"✅ Сервер доступен!\nОтвет: {response.text}")
        else:
            bot.reply_to(message, f"❌ Сервер вернул ошибку: {response.status_code}")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка подключения: {str(e)}")

# ============================================
# ОБРАБОТЧИК НАЖАТИЯ КНОПОК
# ============================================

@bot.callback_query_handler(func=lambda call: call.data == "check_forum")
def check_forum_callback(call):
    """Обрабатывает нажатие кнопки 'Я вступил, продолжить'"""
    user_id = call.from_user.id
    
    if is_user_in_forum(user_id):
        delete_temp_message(user_id)
        
        try:
            bot.edit_message_text(
                "✅ Отлично! Ты в форуме. Загружаем главное меню...",
                user_id,
                call.message.message_id
            )
        except:
            pass
        
        response = api_request_with_retry(
            f'{API_URL}/user/init',
            {'telegram_id': user_id, 'username': 'from_forum'},
            max_retries=2,
            delay=1
        )
        
        if response and response.status_code == 200:
            data = response.json()
            send_main_menu(user_id, data.get('player_id'))
        else:
            send_main_menu(user_id)
    else:
        bot.answer_callback_query(
            call.id,
            "❌ Ты ещё не в форуме! Сначала нажми кнопку «Вступить» ☝️",
            show_alert=True
        )

# ============================================
# ЗАПУСК БОТА
# ============================================
if __name__ == '__main__':
    print("🤖 Pingster бот запущен...")
    print(f"📡 API URL: {API_URL}")
    print(f"🌐 FRONTEND URL: {FRONTEND_URL}")
    print(f"📢 Форум: @{FORUM_USERNAME}")
    print("✅ Режим: с проверкой форума и кнопкой подтверждения")
    print("✅ Cache buster активен")
    print("✅ Повторные попытки при ошибках (3 раза)")
    
    bot.remove_webhook()
    
    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=20)
        except Exception as e:
            print(f"❌ Ошибка polling: {e}")
            time.sleep(5)
