import telebot
import requests
import random
import time
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# Твой токен
TOKEN = '8484054850:AAGwAcn1URrcKtikJKclqP8Z8oYs0wbIYY8'

# URL твоего API (ВНЕШНИЙ адрес сервера)
API_URL = 'https://matk91589-dev-pingster-backend-e306.twc1.net/api'

# ID форума (из ссылки https://t.me/pingster_team)
FORUM_USERNAME = 'pingster_team'  # username форума
FORUM_LINK = 'https://t.me/pingster_team'  # ссылка на форум

bot = telebot.TeleBot(TOKEN)

# Хранилище временных сообщений (можно заменить на БД)
temp_messages = {}

# ============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================

def is_user_in_forum(user_id):
    """Проверяет, состоит ли пользователь в форуме"""
    try:
        response = requests.get(
            f"https://api.telegram.org/bot{TOKEN}/getChatMember",
            params={
                "chat_id": f"@{FORUM_USERNAME}",
                "user_id": user_id
            },
            timeout=5
        )
        data = response.json()
        print(f"📡 Проверка форума для {user_id}: {data}")
        
        if data.get('ok'):
            status = data['result']['status']
            return status in ['member', 'creator', 'administrator']
        return False
    except Exception as e:
        print(f"❌ Ошибка проверки форума: {e}")
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
    web_app_button = InlineKeyboardButton(
        text="🚀 Открыть Pingster",
        web_app=telebot.types.WebAppInfo(url=f'https://matk91589-dev-pinster-0b38.twc1.net?tg_id={user_id}')
    )
    markup.add(web_app_button)
    
    text = f"🎮 **Добро пожаловать в Pingster!**\n\n"
    if player_id:
        text += f"👤 Твой игровой ID: `{player_id}`\n"
    text += f"⭐ Твой рейтинг: **0**\n\n"
    text += f"👇 Нажми кнопку ниже, чтобы открыть Mini App:"
    
    bot.send_message(
        user_id,
        text,
        parse_mode='Markdown',
        reply_markup=markup
    )

def send_forum_invite(user_id):
    """Отправляет приглашение вступить в форум"""
    markup = InlineKeyboardMarkup()
    forum_button = InlineKeyboardButton(
        text="📢 Вступить в форум Pingster",
        url=FORUM_LINK
    )
    markup.add(forum_button)
    
    msg = bot.send_message(
        user_id,
        "🎮 **Добро пожаловать в Pingster!**\n\n"
        "Для начала пользования нужно вступить в наш форум.\n"
        "Это займёт 1 секунду 🔥\n\n"
        "После вступления я сразу покажу главное меню!",
        parse_mode='Markdown',
        reply_markup=markup
    )
    
    # Сохраняем сообщение, чтобы потом удалить
    save_temp_message(user_id, msg.message_id)

# Генерация ссылки на Mini App (через Telegram)
def get_mini_app_url(telegram_id):
    bot_username = 'pingster_team_bot'
    return f'https://t.me/{bot_username}/app?startapp={telegram_id}'

# ============================================
# ОБРАБОТЧИКИ КОМАНД
# ============================================

@bot.message_handler(commands=['start'])
def start(message):
    telegram_id = message.from_user.id
    username = message.from_user.username or 'no_username'
    
    print(f"👉 Получен /start от {username} (ID: {telegram_id})")
    
    # ШАГ 1: Проверяем, есть ли юзер в форуме
    if not is_user_in_forum(telegram_id):
        # 🟡 НОВИЧОК — отправляем приглашение вступить
        print(f"🟡 Новичок {telegram_id} — отправляем приглашение в форум")
        send_forum_invite(telegram_id)
        return
    
    # 🟢 СТАРЫЙ ЮЗЕР — продолжаем как обычно
    print(f"🟢 Старый юзер {telegram_id} — отправляем главное меню")
    
    try:
        print(f"📡 Отправка запроса на {API_URL}/user/init")
        
        response = requests.post(f'{API_URL}/user/init', json={
            'telegram_id': telegram_id,
            'username': username
        }, timeout=10)
        
        print(f"✅ Ответ от API: {response.status_code}")
        print(f"📦 Текст ответа: {response.text}")
        
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'ok':
                send_main_menu(telegram_id, data.get('player_id'))
            else:
                bot.reply_to(message, "❌ Ошибка при регистрации")
        else:
            bot.reply_to(message, f"❌ Ошибка сервера: {response.status_code}")
            
    except requests.exceptions.ConnectionError as e:
        bot.reply_to(message, "❌ Не могу подключиться к серверу. Проверь, запущен ли сервер.")
        print(f"❌ ConnectionError: {e}")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")
        print(f"❌ Ошибка: {str(e)}")

@bot.message_handler(commands=['help'])
def help(message):
    markup = InlineKeyboardMarkup()
    web_app_button = InlineKeyboardButton(
        text="🚀 Открыть Pingster",
        web_app=telebot.types.WebAppInfo(url='https://matk91589-dev-pinster-0b38.twc1.net')
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
# ОБРАБОТЧИК ВСТУПЛЕНИЯ В ФОРУМ
# ============================================
@bot.chat_member_handler()
def handle_chat_member_update(update):
    """Срабатывает при изменении статуса участника в чате"""
    try:
        new_status = update.new_chat_member.status
        user_id = update.from_user.id
        chat_id = update.chat.id
        
        print(f"📢 Обновление статуса в чате {chat_id}: юзер {user_id} -> {new_status}")
        
        # Проверяем, что это наш форум
        if str(chat_id) != f"@{FORUM_USERNAME}" and str(chat_id) != str(FORUM_USERNAME):
            return
        
        # Если пользователь стал участником
        if new_status in ['member', 'administrator', 'creator']:
            print(f"✅ Юзер {user_id} вступил в форум!")
            
            # Удаляем временное сообщение-приглашение
            delete_temp_message(user_id)
            
            # Отправляем главное меню
            # Получаем player_id через API
            try:
                response = requests.post(f'{API_URL}/user/init', json={
                    'telegram_id': user_id,
                    'username': 'from_forum'
                }, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    send_main_menu(user_id, data.get('player_id'))
                else:
                    send_main_menu(user_id)
            except:
                send_main_menu(user_id)
                
    except Exception as e:
        print(f"❌ Ошибка в handle_chat_member_update: {e}")

# Альтернативный обработчик (если chat_member_handler не работает)
@bot.chat_join_request_handler()
def handle_join_request(request):
    """Срабатывает при запросе на вступление"""
    try:
        user_id = request.from_user.id
        chat_id = request.chat.id
        
        print(f"📢 Запрос на вступление от {user_id} в чат {chat_id}")
        
        # Принимаем заявку
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/approveChatJoinRequest",
            json={
                "chat_id": chat_id,
                "user_id": user_id
            }
        )
        
        print(f"✅ Заявка {user_id} принята!")
        
        # Удаляем временное сообщение
        delete_temp_message(user_id)
        
        # Отправляем главное меню
        try:
            response = requests.post(f'{API_URL}/user/init', json={
                'telegram_id': user_id,
                'username': 'from_forum'
            }, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                send_main_menu(user_id, data.get('player_id'))
            else:
                send_main_menu(user_id)
        except:
            send_main_menu(user_id)
            
    except Exception as e:
        print(f"❌ Ошибка в handle_join_request: {e}")

# ============================================
# ЗАПУСК БОТА
# ============================================
if __name__ == '__main__':
    print("🤖 Pingster бот запущен...")
    print(f"📡 API URL: {API_URL}")
    print(f"📢 Форум: @{FORUM_USERNAME}")
    print("✅ Режим: с проверкой форума")
    
    # Удаляем вебхук (на всякий случай)
    bot.remove_webhook()
    
    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=20)
        except Exception as e:
            print(f"❌ Ошибка polling: {e}")
            time.sleep(5)
