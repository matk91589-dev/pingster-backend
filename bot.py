import telebot
import requests
import random
import time

# Твой токен
TOKEN = '8484054850:AAGwAcn1URrcKtikJKclqP8Z8oYs0wbIYY8'

# URL твоего API (ВНУТРЕННИЙ адрес контейнера)
API_URL = 'http://127.0.0.1:5000/api'

bot = telebot.TeleBot(TOKEN)

# Генерация ссылки на Mini App
def get_mini_app_url(telegram_id):
    return f'https://matk91589-dev-pinster-0b38.twc1.net?tg_id={telegram_id}'

# Команда /start
@bot.message_handler(commands=['start'])
def start(message):
    telegram_id = message.from_user.id
    username = message.from_user.username or 'no_username'
    
    try:
        print(f"👉 Получен /start от {username} (ID: {telegram_id})")
        
        # Отправляем данные в API
        response = requests.post(f'{API_URL}/user/init', json={
            'telegram_id': telegram_id,
            'username': username
        }, timeout=10)
        
        print(f"✅ Ответ от API: {response.status_code}")
        data = response.json()
        print(f"📦 Данные: {data}")
        
        if data.get('status') == 'ok':
            mini_app_url = get_mini_app_url(telegram_id)
            bot.reply_to(message, 
                f"🎮 Добро пожаловать в Pingster!\n\n"
                f"👤 Твой игровой ID: {data.get('player_id')}\n"
                f"⭐ Твой рейтинг: 0\n\n"
                f"👇 Открывай Mini App и ищи тиммейтов:\n"
                f"{mini_app_url}"
            )
        else:
            bot.reply_to(message, "❌ Ошибка при регистрации")
    except requests.exceptions.ConnectionError:
        bot.reply_to(message, "❌ Не могу подключиться к серверу. Попробуй позже.")
        print("❌ ConnectionError: API не доступен")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")
        print(f"❌ Ошибка: {str(e)}")

# Команда /help
@bot.message_handler(commands=['help'])
def help(message):
    bot.reply_to(message, 
        "🎮 Pingster — поиск тиммейтов для CS2\n\n"
        "Команды:\n"
        "/start - Начать\n"
        "/help - Помощь\n\n"
        "Как это работает:\n"
        "1. Открой Mini App\n"
        "2. Заполни профиль\n"
        "3. Нажми 'Найти тиммейта'\n"
        "4. Прими мэтч и играй!"
    )

# Запуск бота с автоматическим переподключением
if __name__ == '__main__':
    print("🤖 Pingster бот запущен...")
    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=20)
        except Exception as e:
            print(f"❌ Ошибка polling: {e}")
            time.sleep(5)
