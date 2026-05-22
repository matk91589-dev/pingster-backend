from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
import requests
import time
import os
import threading
import traceback
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

# Хранилище сообщений для удаления старых
user_messages = {}
user_messages_lock = threading.Lock()

# ============================================
# ФУНКЦИИ
# ============================================

def check_server_awake():
    """Проверка что сервер жив"""
    try:
        r = requests.get(f'{API_URL.replace("/api", "")}/health', timeout=5)
        print(f"🔍 Проверка сервера: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            features = data.get('features', {})
            print(f"📱 Фичи сервера: fullscreen={features.get('fullscreen_mode')}, native_chat={features.get('native_chat')}")
        return r.status_code == 200
    except Exception as e:
        print(f"❌ Сервер не отвечает: {e}")
        return False

def wake_up_server():
    """Пробуждение сервера"""
    try:
        requests.get(f'{API_URL.replace("/api", "")}/health', timeout=3)
        print("🔔 Пинг сервера отправлен")
    except:
        pass

def delete_old_command(user_id: int, msg_type: str):
    """Удаление старых сообщений команды"""
    with user_messages_lock:
        if user_id in user_messages and msg_type in user_messages[user_id]:
            old = user_messages[user_id][msg_type]
            if 'bot' in old:
                try:
                    # В python-telegram-bot v20+ нужно использовать bot.delete_message
                    pass  # Будет удалено через контекст
                except:
                    pass

def save_command_message(user_id: int, msg_type: str, user_msg_id: int, bot_msg_id: int):
    """Сохранение сообщений для последующего удаления"""
    with user_messages_lock:
        if user_id not in user_messages:
            user_messages[user_id] = {}
        user_messages[user_id][msg_type] = {
            'user': user_msg_id, 
            'bot': bot_msg_id,
            'timestamp': time.time()
        }

def register_user(telegram_id: int, username: str) -> str:
    """Регистрирует пользователя в API и возвращает player_id"""
    print(f"📝 Регистрация: tg_id={telegram_id}, username={username}")
    try:
        # Используем новый эндпоинт с поддержкой initData
        response = requests.post(
            f'{API_URL}/user/init',
            json={
                'telegram_id': str(telegram_id),
                'username': username
            },
            headers={
                'Content-Type': 'application/json'
            },
            timeout=10
        )
        print(f"📥 Ответ: status={response.status_code}, body={response.text[:200]}")
        
        if response.status_code == 200:
            data = response.json()
            player_id = data.get('player_id')
            nick = data.get('nick')
            auth_method = data.get('auth_method', 'telegram_id')
            print(f"✅ player_id={player_id}, nick={nick}, auth={auth_method}")
            return player_id
        
        return None
    except Exception as e:
        print(f"❌ Ошибка регистрации: {e}")
        return None

def get_user_profile(telegram_id: int) -> dict:
    """Получение профиля пользователя"""
    try:
        response = requests.post(
            f'{API_URL}/profile/get',
            json={'telegram_id': str(telegram_id)},
            timeout=5
        )
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return {}

# ============================================
# ОБРАБОТЧИК /START
# ============================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    message = update.message
    telegram_id = update.effective_user.id
    username = update.effective_user.username or f'user_{telegram_id}'
    user_msg_id = message.message_id
    
    print(f"\n{'='*50}")
    print(f"🚀 /start от @{username} (id: {telegram_id})")
    
    # Удаляем старое сообщение бота если есть
    await delete_old_bot_message(update, context, telegram_id, 'start')
    
    # 🔥 ВСЕГДА РЕГИСТРИРУЕМ — даже если сервер спит, пробуем
    player_id = None
    if check_server_awake():
        player_id = register_user(telegram_id, username)
    else:
        wake_up_server()
        time.sleep(1)
        if check_server_awake():
            player_id = register_user(telegram_id, username)
    
    print(f"🎮 Игровой ID: {player_id}")
    
    is_allowed = telegram_id in ALLOWED_USERS
    print(f"🔐 Доступ: {'РАЗРЕШЁН' if is_allowed else 'ОБЫЧНЫЙ'}")
    
    if not is_allowed:
        # 🔥 ОБЫЧНЫЙ ПОЛЬЗОВАТЕЛЬ
        text = (
            f"*@{username}*\n"
            f"Добро пожаловать в Pingster\\!\n\n"
            f"👤 Твой игровой ID: `{player_id or '—'}`\n\n"
            f"🚧 Приложение пока в разработке\\.\n"
            f"👇 Перейти в канал"
        )
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                text="📢 Telegram канал",
                url="https://t.me/pingster_team_channel"
            )]
        ])
        
        bot_msg = await message.reply_text(
            text,
            parse_mode='MarkdownV2',
            reply_markup=markup
        )
        save_command_message(telegram_id, 'start', user_msg_id, bot_msg.message_id)
        print(f"✅ Обычный юзер, player_id={player_id}")
        return
    
    # 🔥 ТЕСТЕР — показываем WebApp кнопку
    text = (
        f"*@{username}*\n"
        f"Добро пожаловать в Pingster\\!\n\n"
        f"👤 Твой игровой ID: `{player_id or '—'}`\n\n"
        f"🎮 Новые фичи:\n"
        f"• Полноэкранный режим\n"
        f"• Нативный чат при мэтче\n"
        f"• Быстрое кеширование\n\n"
        f"👇 Нажми кнопку ниже, чтобы начать:"
    )
    
    cache_buster = int(time.time())
    markup = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            text="🚀 ЗАПУСТИТЬ",
            web_app=WebAppInfo(url=f'{FRONTEND_URL}/?v={cache_buster}&tg_id={telegram_id}')
        )],
        [InlineKeyboardButton(
            text="📢 Канал сообщества",
            url=FORUM_LINK
        )]
    ])
    
    bot_msg = await message.reply_text(
        text,
        parse_mode='MarkdownV2',
        reply_markup=markup
    )
    save_command_message(telegram_id, 'start', user_msg_id, bot_msg.message_id)
    print(f"✅ Тестер, player_id={player_id}")
    print(f"{'='*50}\n")

async def delete_old_bot_message(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, msg_type: str):
    """Удаление старого сообщения бота"""
    with user_messages_lock:
        if user_id in user_messages and msg_type in user_messages[user_id]:
            old_data = user_messages[user_id][msg_type]
            if 'bot' in old_data:
                try:
                    await context.bot.delete_message(
                        chat_id=user_id,
                        message_id=old_data['bot']
                    )
                except:
                    pass

# ============================================
# ГОЛОСОВАНИЕ ЗА РЕПУТАЦИЮ
# ============================================

async def handle_reputation_vote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик голосования за репутацию"""
    query = update.callback_query
    await query.answer()
    
    callback_data = query.data
    message = query.message
    chat_id = message.chat.id
    message_id = message.message_id
    
    vote_type = "👍" if ":up:" in callback_data else "👎"
    print(f"🗳 Голос: {callback_data} -> {vote_type}")
    
    # Отправляем голос в API асинхронно
    def send_vote():
        try:
            r = requests.post(
                f"{API_URL}/reputation/vote",
                json={"callback_data": callback_data},
                timeout=5
            )
            print(f"📡 API vote: {r.status_code}")
        except Exception as e:
            print(f"❌ API vote error: {e}")
    
    threading.Thread(target=send_vote, daemon=True).start()
    
    # Обновляем сообщение
    chat_link = None
    if message.reply_markup:
        for row in message.reply_markup.inline_keyboard:
            for btn in row:
                if btn.url:
                    chat_link = btn.url
                    break
            if chat_link:
                break
    
    original_text = message.text or message.caption or ""
    base_text = original_text.split("Оцените тиммейта:")[0].strip() if "Оцените тиммейта:" in original_text else original_text
    new_text = f"{base_text}\n\n✅ Вы оценили тиммейта: {vote_type}"
    
    try:
        if chat_link:
            link_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("👉 Перейти в чат", url=chat_link)]
            ])
            await query.edit_message_text(
                text=new_text,
                reply_markup=link_markup
            )
        else:
            await query.edit_message_text(
                text=new_text,
                reply_markup=None
            )
        print("✅ Сообщение отредактировано")
    except Exception as e:
        print(f"❌ Ошибка редактирования: {e}")

# ============================================
# КОМАНДА /profile — ПРОСМОТР ПРОФИЛЯ
# ============================================

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает профиль пользователя"""
    telegram_id = update.effective_user.id
    username = update.effective_user.username or f'user_{telegram_id}'
    
    profile_data = get_user_profile(telegram_id)
    
    if profile_data.get('status') == 'ok':
        nick = profile_data.get('nick', username)
        player_id = profile_data.get('player_id')
        age = profile_data.get('age', 'не указан')
        steam = profile_data.get('steam_link', 'не указан')
        faceit = profile_data.get('faceit_link', 'не указан')
        
        text = (
            f"*Профиль игрока*\n\n"
            f"👤 Ник: {nick}\n"
            f"🆔 ID: `{player_id}`\n"
            f"🎂 Возраст: {age}\n"
            f"🎮 Steam: {steam}\n"
            f"🏆 Faceit: {faceit}"
        )
        
        await update.message.reply_text(
            text,
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text("❌ Профиль не найден. Используйте /start")

# ============================================
# УДАЛЕНИЕ НЕПОНЯТНЫХ СООБЩЕНИЙ
# ============================================

async def delete_unknown_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаление всех непонятных сообщений в личке"""
    message = update.message
    
    # Пропускаем команды
    if message.text and message.text.startswith('/'):
        return
    
    # Пропускаем ботов
    if update.effective_user.is_bot:
        return
    
    # Удаляем только в личных сообщениях
    if message.chat.type == 'private':
        try:
            await message.delete()
            print(f"🗑 Удалено сообщение от {update.effective_user.id}")
        except Exception as e:
            print(f"⚠️ Ошибка удаления: {e}")

# ============================================
# ОБРАБОТКА ОШИБОК
# ============================================

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Глобальный обработчик ошибок"""
    print(f"❌ Ошибка: {context.error}")
    traceback.print_exception(type(context.error), context.error, context.error.__traceback__)

# ============================================
# ЗАПУСК БОТА
# ============================================

def main():
    """Главная функция запуска бота"""
    print("🤖 Pingster бот запускается...")
    print(f"📡 API: {API_URL}")
    print(f"🎨 Frontend: {FRONTEND_URL}")
    print(f"👥 Тестеры: {ALLOWED_USERS}")
    print(f"📱 Новые фичи: Fullscreen Mode | Native Chat | Device Storage")
    print()
    
    # Создаем приложение
    application = Application.builder().token(TOKEN).build()
    
    # Регистрируем обработчики
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('profile', profile))
    application.add_handler(CallbackQueryHandler(
        handle_reputation_vote,
        pattern='^vote:'
    ))
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND,
        delete_unknown_messages
    ))
    
    # Обработчик ошибок
    application.add_error_handler(error_handler)
    
    # Запускаем бота
    print("✅ Бот готов к работе!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
