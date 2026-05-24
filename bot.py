from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telegram.constants import ParseMode
import requests
import time
import os
import asyncio
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

ALLOWED_USERS = [5015478106, 8541469401]

if not TOKEN:
    raise ValueError("BOT_TOKEN not found!")

user_messages = {}
user_messages_lock = threading.Lock()

# ============================================
# ФУНКЦИИ
# ============================================

def check_server_awake():
    try:
        r = requests.get(f'{API_URL.replace("/api", "")}/health', timeout=5)
        print(f"Server check: {r.status_code}")
        return r.status_code == 200
    except:
        return False

def wake_up_server():
    try:
        requests.get(f'{API_URL.replace("/api", "")}/health', timeout=3)
    except:
        pass

def save_command_message(user_id: int, msg_type: str, user_msg_id: int, bot_msg_id: int):
    with user_messages_lock:
        if user_id not in user_messages:
            user_messages[user_id] = {}
        user_messages[user_id][msg_type] = {
            'user': user_msg_id, 
            'bot': bot_msg_id,
            'timestamp': time.time()
        }

def register_user(telegram_id: int, username: str) -> str:
    print(f"Register: tg_id={telegram_id}, username={username}")
    try:
        response = requests.post(
            f'{API_URL}/user/init',
            json={'telegram_id': str(telegram_id), 'username': username},
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            return data.get('player_id')
        return None
    except Exception as e:
        print(f"Register error: {e}")
        return None

def get_user_profile(telegram_id: int) -> dict:
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
    message = update.message
    telegram_id = update.effective_user.id
    username = update.effective_user.username or f'user_{telegram_id}'
    user_msg_id = message.message_id
    
    print(f"\n{'='*50}")
    print(f"/start from @{username} (id: {telegram_id})")
    
    await delete_old_bot_message(update, context, telegram_id, 'start')
    
    player_id = None
    if check_server_awake():
        player_id = register_user(telegram_id, username)
    else:
        wake_up_server()
        time.sleep(1)
        if check_server_awake():
            player_id = register_user(telegram_id, username)
    
    print(f"Player ID: {player_id}")
    is_allowed = telegram_id in ALLOWED_USERS
    
    if not is_allowed:
        # 🔥 HTML разметка
        text = (
            f"<b>@{username}</b>\n"
            f"Добро пожаловать в Pingster!\n\n"
            f"<b>Твой игровой ID:</b> <code>{player_id or '—'}</code>\n\n"
            f"🚧 Приложение пока в разработке.\n"
            f"👇 Перейти в канал"
        )
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton(text="📢 Telegram канал", url="https://t.me/pingster_team_channel")]
        ])
        
        bot_msg = await message.reply_text(text, parse_mode='HTML', reply_markup=markup)
        save_command_message(telegram_id, 'start', user_msg_id, bot_msg.message_id)
        print(f"Regular user, player_id={player_id}")
        return
    
    # 🔥 ТЕСТЕР
    text = (
        f"<b>@{username}</b>\n"
        f"Добро пожаловать в Pingster!\n\n"
        f"<b>Твой игровой ID:</b> <code>{player_id or '—'}</code>\n\n"
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
        [InlineKeyboardButton(text="📢 Канал сообщества", url=FORUM_LINK)]
    ])
    
    bot_msg = await message.reply_text(text, parse_mode='HTML', reply_markup=markup)
    save_command_message(telegram_id, 'start', user_msg_id, bot_msg.message_id)
    print(f"Tester, player_id={player_id}")

async def delete_old_bot_message(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, msg_type: str):
    with user_messages_lock:
        if user_id in user_messages and msg_type in user_messages[user_id]:
            old_data = user_messages[user_id][msg_type]
            if 'bot' in old_data:
                try:
                    await context.bot.delete_message(chat_id=user_id, message_id=old_data['bot'])
                except:
                    pass

# ============================================
# ГОЛОСОВАНИЕ
# ============================================

async def handle_reputation_vote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    callback_data = query.data
    message = query.message
    vote_type = "👍" if ":up:" in callback_data else "👎"
    
    def send_vote():
        try:
            requests.post(f"{API_URL}/reputation/vote", json={"callback_data": callback_data}, timeout=5)
        except:
            pass
    
    threading.Thread(target=send_vote, daemon=True).start()
    
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
            await query.edit_message_text(text=new_text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👉 Перейти в чат", url=chat_link)]]))
        else:
            await query.edit_message_text(text=new_text)
    except:
        pass

# ============================================
# /profile
# ============================================

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    telegram_id = update.effective_user.id
    profile_data = get_user_profile(telegram_id)
    
    if profile_data.get('status') == 'ok':
        nick = profile_data.get('nick', '—')
        player_id = profile_data.get('player_id', '—')
        age = profile_data.get('age', '—')
        steam = profile_data.get('steam_link', '—')
        faceit = profile_data.get('faceit_link', '—')
        
        text = (
            f"<b>Профиль игрока</b>\n\n"
            f"<b>Ник:</b> {nick}\n"
            f"<b>ID:</b> <code>{player_id}</code>\n"
            f"<b>Возраст:</b> {age}\n"
            f"<b>Steam:</b> {steam}\n"
            f"<b>Faceit:</b> {faceit}"
        )
        await update.message.reply_text(text, parse_mode='HTML')
    else:
        await update.message.reply_text("Профиль не найден. Используйте /start")

# ============================================
# УДАЛЕНИЕ СООБЩЕНИЙ
# ============================================

async def delete_unknown_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if message.text and message.text.startswith('/'):
        return
    if update.effective_user.is_bot:
        return
    if message.chat.type == 'private':
        try:
            await message.delete()
        except:
            pass

# ============================================
# ОБРАБОТКА ОШИБОК
# ============================================

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"Error: {context.error}")

# ============================================
# ЗАПУСК
# ============================================

def main():
    print("Bot starting...")
    
    # 🔥 FIX PYTHON 3.14
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    
    application = Application.builder().token(TOKEN).build()
    
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('profile', profile))
    application.add_handler(CallbackQueryHandler(handle_reputation_vote, pattern='^vote:'))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, delete_unknown_messages))
    application.add_error_handler(error_handler)
    
    print("Bot ready!")
    application.run_polling(allowed_updates=Update.ALL_TYPES, close_loop=False)

if __name__ == '__main__':
    main()
