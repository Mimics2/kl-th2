import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List

from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

# ========== CONFIG ==========
API_TOKEN = "YOUR_BOT_TOKEN"  # Замени на свой токен бота
DATABASE_URL = "YOUR_RAILWAY_POSTGRESQL_URL"  # Замени на свою URL от Railway

ADMIN_ID = 123456789  # Замени на свой Telegram ID для статистики и рассылки

# ========== SETUP ==========
logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

scheduler = AsyncIOScheduler(timezone="UTC")

# ========== DB CONNECTION ==========
async def get_db_connection():
    return await asyncpg.connect(DATABASE_URL)

async def init_db():
    """Инициализация таблиц в PostgreSQL"""
    conn = await get_db_connection()
    try:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                registered_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT,
                channel_id BIGINT,
                channel_name TEXT,
                added_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_posts (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT,
                channel_id BIGINT,
                message_text TEXT,
                scheduled_time TIMESTAMP,
                sent BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        logging.info("База данных инициализирована")
    finally:
        await conn.close()

# ========== STATES ==========
class PostStates(StatesGroup):
    waiting_for_channel = State()
    waiting_for_text = State()
    waiting_for_time = State()

# ========== KEYBOARDS ==========
def get_main_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Запланировать пост", callback_data="schedule_post")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="broadcast")]
    ])
    return keyboard

def get_cancel_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])
    return keyboard

# ========== HANDLERS ==========
@router.message(Command("start"))
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name

    conn = await get_db_connection()
    try:
        await conn.execute('''
            INSERT INTO users (id, username, first_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO NOTHING
        ''', user_id, username, first_name)
    finally:
        await conn.close()

    await message.answer(
        f"Привет, {first_name}! 👋\n"
        "Я бот для планирования постов в Telegram-каналах.\n\n"
        "Что я умею:\n"
        "📅 - Планировать посты на будущее\n"
        "📊 - Показывать статистику\n"
        "📢 - Делать рассылку (только для админа)\n\n"
        "Выбери действие:",
        reply_markup=get_main_keyboard()
    )

@router.callback_query(lambda c: c.data == "schedule_post")
async def process_schedule(callback: types.CallbackQuery, state: FSMContext):
    """Начало планирования поста"""
    await state.set_state(PostStates.waiting_for_channel)
    await callback.message.edit_text(
        "Пришли мне ID канала (в формате -1001234567890) или перешли любое сообщение из канала.\n"
        "Убедись, что бот добавлен в канал с правами на отправку сообщений!",
        reply_markup=get_cancel_keyboard()
    )

@router.message(PostStates.waiting_for_channel)
async def process_channel(message: Message, state: FSMContext):
    """Обработка ID канала"""
    channel_id = None
    
    if message.forward_from_chat:
        channel_id = message.forward_from_chat.id
    elif message.text and message.text.startswith('-100'):
        try:
            channel_id = int(message.text)
        except ValueError:
            await message.answer("Некорректный ID. Попробуй еще раз.", reply_markup=get_cancel_keyboard())
            return
    
    if not channel_id:
        await message.answer("Пожалуйста, отправь корректный ID канала или перешли сообщение из канала.", reply_markup=get_cancel_keyboard())
        return
    
    # Сохраняем ID канала в состояние
    await state.update_data(channel_id=channel_id)
    
    # Сохраняем канал в БД
    conn = await get_db_connection()
    try:
        await conn.execute('''
            INSERT INTO channels (user_id, channel_id, channel_name)
            VALUES ($1, $2, $3)
            ON CONFLICT (channel_id) DO NOTHING
        ''', message.from_user.id, channel_id, "Канал пользователя")
    finally:
        await conn.close()
    
    await state.set_state(PostStates.waiting_for_text)
    await message.answer(
        "Отлично! Теперь пришли текст поста.\n"
        "Поддерживается HTML-разметка: <b>жирный</b>, <i>курсив</i>, <code>код</code>",
        reply_markup=get_cancel_keyboard()
    )

@router.message(PostStates.waiting_for_text)
async def process_text(message: Message, state: FSMContext):
    """Обработка текста поста"""
    await state.update_data(text=message.html_text)
    await state.set_state(PostStates.waiting_for_time)
    
    await message.answer(
        "Теперь отправь дату и время публикации в формате:\n"
        "<b>ГГГГ-ММ-ДД ЧЧ:ММ</b>\n\n"
        "Пример: 2024-12-31 20:00\n"
        "Время указывается по UTC.",
        reply_markup=get_cancel_keyboard()
    )

@router.message(PostStates.waiting_for_time)
async def process_time(message: Message, state: FSMContext):
    """Обработка времени публикации"""
    try:
        scheduled_time = datetime.strptime(message.text, "%Y-%m-%d %H:%M")
        
        # Проверяем, что время в будущем
        if scheduled_time <= datetime.now():
            await message.answer("Время должно быть в будущем! Попробуй еще раз.", reply_markup=get_cancel_keyboard())
            return
        
    except ValueError:
        await message.answer("Некорректный формат даты. Используй: ГГГГ-ММ-ДД ЧЧ:ММ", reply_markup=get_cancel_keyboard())
        return
    
    # Получаем данные из состояния
    data = await state.get_data()
    channel_id = data['channel_id']
    text = data['text']
    user_id = message.from_user.id
    
    # Сохраняем пост в БД
    conn = await get_db_connection()
    try:
        post_id = await conn.fetchval('''
            INSERT INTO scheduled_posts (user_id, channel_id, message_text, scheduled_time)
            VALUES ($1, $2, $3, $4)
            RETURNING id
        ''', user_id, channel_id, text, scheduled_time)
        
        # Планируем отправку
        scheduler.add_job(
            send_scheduled_post,
            trigger=DateTrigger(run_date=scheduled_time),
            args=(channel_id, text, post_id),
            id=f"post_{post_id}"
        )
        
    finally:
        await conn.close()
    
    await state.clear()
    
    await message.answer(
        f"✅ Пост запланирован на {scheduled_time.strftime('%Y-%m-%d %H:%M')} UTC!\n"
        f"ID поста: {post_id}\n\n"
        f"Что дальше?",
        reply_markup=get_main_keyboard()
    )

async def send_scheduled_post(channel_id: int, text: str, post_id: int):
    """Отправка запланированного поста"""
    try:
        await bot.send_message(
            chat_id=channel_id,
            text=text,
            parse_mode="HTML"
        )
        
        # Обновляем статус в БД
        conn = await get_db_connection()
        try:
            await conn.execute('''
                UPDATE scheduled_posts 
                SET sent = TRUE 
                WHERE id = $1
            ''', post_id)
        finally:
            await conn.close()
            
        logging.info(f"Пост {post_id} отправлен в канал {channel_id}")
        
    except Exception as e:
        logging.error(f"Ошибка отправки поста {post_id}: {e}")

@router.callback_query(lambda c: c.data == "stats")
async def show_stats(callback: types.CallbackQuery):
    """Показ статистики"""
    conn = await get_db_connection()
    try:
        # Общее количество пользователей
        total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
        
        # Общее количество запланированных постов
        total_posts = await conn.fetchval("SELECT COUNT(*) FROM scheduled_posts")
        
        # Количество активных постов (не отправленных)
        active_posts = await conn.fetchval("SELECT COUNT(*) FROM scheduled_posts WHERE sent = FALSE")
        
        # Количество каналов
        total_channels = await conn.fetchval("SELECT COUNT(*) FROM channels")
        
        stats_text = (
            f"📊 <b>Статистика бота</b>\n\n"
            f"👤 Пользователей: <code>{total_users}</code>\n"
            f"📅 Всего постов запланировано: <code>{total_posts}</code>\n"
            f"⏳ Ожидают отправки: <code>{active_posts}</code>\n"
            f"📢 Каналов добавлено: <code>{total_channels}</code>"
        )
        
        await callback.message.edit_text(stats_text, parse_mode="HTML", reply_markup=get_main_keyboard())
        
    finally:
        await conn.close()

@router.callback_query(lambda c: c.data == "broadcast")
async def process_broadcast(callback: types.CallbackQuery):
    """Рассылка сообщений (только для админа)"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Эта функция только для администратора!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📢 <b>Режим рассылки</b>\n\n"
        "Пришлите сообщение, которое нужно разослать всем пользователям.\n"
        "Поддерживается HTML-разметка.\n\n"
        "Для отмены используйте /cancel",
        parse_mode="HTML"
    )
    
    # Здесь можно добавить FSM для рассылки

@router.callback_query(lambda c: c.data == "cancel")
async def cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    """Отмена текущего действия"""
    await state.clear()
    await callback.message.edit_text(
        "Действие отменено.\n\nЧто будем делать дальше?",
        reply_markup=get_main_keyboard()
    )

# ========== ADMIN COMMANDS ==========
@router.message(Command("broadcast"))
async def admin_broadcast(message: Message):
    """Команда рассылки для админа"""
    if message.from_user.id != ADMIN_ID:
        return
    
    # Простая рассылка всем пользователям
    conn = await get_db_connection()
    try:
        users = await conn.fetch("SELECT id FROM users")
        
        for user in users:
            try:
                await bot.send_message(
                    user['id'],
                    "📢 <b>Важное сообщение от администратора!</b>\n\n"
                    "Это тестовая рассылка всем пользователям бота.",
                    parse_mode="HTML"
                )
                await asyncio.sleep(0.1)  # Задержка против ограничений Telegram
            except Exception as e:
                logging.error(f"Не удалось отправить сообщение пользователю {user['id']}: {e}")
                
    finally:
        await conn.close()
    
    await message.answer("✅ Рассылка завершена!")

# ========== MAIN ==========
async def main():
    """Основная функция запуска бота"""
    # Инициализация БД
    await init_db()
    
    # Запуск планировщика
    scheduler.start()
    
    # Восстановление запланированных постов из БД при запуске
    await restore_scheduled_jobs()
    
    # Запуск бота
    await dp.start_polling(bot)

async def restore_scheduled_jobs():
    """Восстановление запланированных постов из БД при перезапуске бота"""
    conn = await get_db_connection()
    try:
        posts = await conn.fetch('''
            SELECT id, channel_id, message_text, scheduled_time 
            FROM scheduled_posts 
            WHERE sent = FALSE AND scheduled_time > NOW()
        ''')
        
        for post in posts:
            scheduler.add_job(
                send_scheduled_post,
                trigger=DateTrigger(run_date=post['scheduled_time']),
                args=(post['channel_id'], post['message_text'], post['id']),
                id=f"post_{post['id']}"
            )
        
        logging.info(f"Восстановлено {len(posts)} запланированных постов")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
