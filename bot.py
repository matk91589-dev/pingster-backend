import telebot
import requests
import random
import time
import os
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, MenuButtonWebApp, WebAppInfo
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

def delete_temp_message(user_id):
    """Удаляет временное сообщение"""
    if user_id in temp_messages:
        try:
            bot.delete_message(user_id, temp_messages[user_id])
        except:
            pass
        del temp_messages[user_id]

def send_main_menu(user_id, player_id=None):
    """Отправляет главное меню с инструкцией"""
    text = f"🎮 **Добро пожаловать в Pingster!**\n\n"
    if player_id:
        text += f"👤 Твой игровой ID: `{player_id}`\n"
    text += f"\n👇 **Нажми кнопку «Запустить» в меню слева от ввода сообщения**\n\n"
    text += f"Или используй команды:\n"
    text += f"/start — главное меню\n"
    text += f"/support — связаться с поддержкой"
    
    bot.send_message(
        user_id,
        text,
        parse_mode='Markdown'
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
        "После этого появится кнопка запуска в меню 🔥",
        parse_mode='Markdown',
        reply_markup=markup
    )
    
    save_temp_message(user_id, msg.message_id)

def setup_bot_menu():
    """Настраивает кнопку запуска в меню бота (слева от ввода)"""
    try:
        # Устанавливаем кнопку WebApp в меню
        cache_buster = int(time.time())
        web_app_url = f'{FRONTEND_URL}/?v={cache_buster}'
        
        bot.set_chat_menu_button(
            menu_button=MenuButtonWebApp(
                text="🚀 Запустить",
                web_app=WebAppInfo(url=web_app_url)
            )
        )
        print(f"✅ Кнопка «Запустить» установлена в меню")
        
        # Устанавливаем команды
        commands = [
            BotCommand("start", "Главное меню"),
            BotCommand("support", "Поддержка")
        ]
        bot.set_my_commands(commands)
        print(f"✅ Команды установлены: /start, /support")
        
    except Exception as e:
        print(f"❌ Ошибка настройки меню: {e}")

# ============================================
# ОБРАБОТЧИКИ КОМАНД
# ============================================

@bot.message_handler(commands=['start'])
def start(message):
    telegram_id = message.from_user.id
    username = message.from_user.username or 'no_username'
    
    print(f"👉 /start от {username} (ID: {telegram_id})")
    
    # Проверяем, есть ли юзер в форуме
    if not is_user_in_forum(telegram_id):
        print(f"🟡 Новичок {telegram_id} — отправляем приглашение в форум")
        send_forum_invite(telegram_id)
        return
    
    # Старый юзер — отправляем главное меню
    print(f"🟢 Старый юзер {telegram_id} — отправляем главное меню")
    
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
        bot.reply_to(
            message, 
            f"⚠️ Сервер временно недоступен.\n\nЕсли проблема повторяется, напиши @{SUPPORT_USERNAME}"
        )

@bot.message_handler(commands=['support'])
def support(message):
    """Команда /support — контакты поддержки"""
    bot.send_message(
        message.chat.id,
        f" **Поддержка Pingster**\n\n"
        f"По всем вопросам писать:\n"
        f"👉 @{SUPPORT_USERNAME}\n\n",
        parse_mode='Markdown'
    )

@bot.message_handler(commands=['help'])
def help_command(message):
    """Команда /help — справка"""
    bot.send_message(
        message.chat.id,
        "🎮 **Pingster — поиск тиммейтов для CS2**\n\n"
        "**Команды:**\n"
        "/start — запустить приложение\n"
        "/support — связаться с поддержкой\n\n"
        "**Как это работает:**\n"
        "1. Нажми «Запустить» в меню слева\n"
        "2. Выбери режим и начни поиск\n"
        "3. Свайпай и находи тиммейтов!",
        parse_mode='Markdown'
    )

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
                "✅ Отлично! Ты в форуме.\n\n"
                "Теперь нажми кнопку **«Запустить»** в меню слева от ввода сообщения! 🚀",
                user_id,
                call.message.message_id,
                parse_mode='Markdown'
            )
        except:
            pass
        
        # Регистрируем пользователя
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

@bot.callback_query_handler(func=lambda call: call.data.startswith('vote_'))
def handle_reputation_vote(call):
    """Обрабатывает нажатие на кнопки 👍 или 👎"""
    user_id = call.from_user.id
    callback_data = call.data
    message = call.message
    
    print(f"🗳 Получен голос: {callback_data} от пользователя {user_id}")
    
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
            print(f"✅ Голос успешно обработан через API")
            bot.answer_callback_query(call.id, "✅ Спасибо за оценку!")
        else:
            print(f"❌ Ошибка API: {response.status_code}")
            bot.answer_callback_query(call.id, "❌ Ошибка, попробуй позже")
            
    except Exception as e:
        print(f"❌ Ошибка отправки голоса: {e}")
        bot.answer_callback_query(call.id, "❌ Ошибка соединения")

# ============================================
# ЗАПУСК БОТА
# ============================================
if __name__ == '__main__':
    print("🤖 Pingster бот запущен...")
    print(f"📡 API URL: {API_URL}")
    print(f"🌐 FRONTEND URL: {FRONTEND_URL}")
    print(f"📢 Форум: @{FORUM_USERNAME}")
    print(f"📞 Поддержка: @{SUPPORT_USERNAME}")
    
    bot.remove_webhook()
    
    # 🔥 НАСТРАИВАЕМ КНОПКУ В МЕНЮ
    setup_bot_menu()
    
    print("🚀 Бот готов к работе!")
    
    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=20)
        except Exception as e:
            print(f"❌ Ошибка polling: {e}")
            time.sleep(5)
