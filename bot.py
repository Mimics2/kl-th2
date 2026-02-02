import os
import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from enum import Enum

import pytz
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ContentType, FSInputFile
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.fsm.storage.memory import MemoryStorage

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

# ========== CONFIG ==========
API_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

# Настройки
MOSCOW_TZ = pytz.timezone('Europe/Moscow')
MAX_POSTS_PER_USER = 100  # Максимум постов на пользователя
POST_CHARACTER_LIMIT = 4000  # Лимит символов на пост

# ========== SETUP ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN, parse_mode="HTML")

# Используем Redis для продакшена, но можем fallback на MemoryStorage
try:
    if REDIS_URL and "redis://" in REDIS_URL:
        storage = RedisStorage.from_url(REDIS_URL)
        logger.info("✅ Используется Redis хранилище")
    else:
        storage = MemoryStorage()
        logger.info("⚠️ Используется Memory хранилище (Redis не настроен)")
except Exception as e:
    logger.warning(f"Redis недоступен: {e}, использую MemoryStorage")
    storage = MemoryStorage()

dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

scheduler = AsyncIOScheduler(timezone=MOSCOW_TZ)

# ========== DATABASE MODELS ==========
class UserStatus(Enum):
    ACTIVE = "active"
    BANNED = "banned"
    ADMIN = "admin"

class PostType(Enum):
    TEXT = "text"
    PHOTO = "photo"
    VIDEO = "video"
    DOCUMENT = "document"

# ========== DATABASE FUNCTIONS ==========
async def get_db_connection():
    """Создает соединение с базой данных"""
    try:
        if DATABASE_URL:
            # Для Railway PostgreSQL добавляем sslmode
            if "postgresql://" in DATABASE_URL and "sslmode" not in DATABASE_URL:
                conn_string = DATABASE_URL + "?sslmode=require"
            else:
                conn_string = DATABASE_URL
            return await asyncpg.connect(conn_string)
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {e}")
        raise

async def init_db():
    """Инициализация таблиц в PostgreSQL"""
    try:
        conn = await get_db_connection()
        
        # Таблица пользователей
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                status TEXT DEFAULT 'active' CHECK (status IN ('active', 'banned', 'admin')),
                daily_post_limit INTEGER DEFAULT 50,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Таблица каналов
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
                channel_id BIGINT UNIQUE NOT NULL,
                channel_name TEXT NOT NULL,
                channel_link TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(user_id, channel_id)
            )
        ''')
        
        # Таблица запланированных постов
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_posts (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
                channel_id BIGINT NOT NULL,
                message_type TEXT NOT NULL,
                message_text TEXT,
                media_file_id TEXT,
                media_caption TEXT,
                scheduled_time TIMESTAMP NOT NULL,
                sent BOOLEAN DEFAULT FALSE,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                sent_at TIMESTAMP
            )
        ''')
        
        # Индексы для производительности
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_scheduled_posts_time 
            ON scheduled_posts(scheduled_time) WHERE sent = FALSE
        ''')
        
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_scheduled_posts_user 
            ON scheduled_posts(user_id, sent)
        ''')
        
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_channels_user 
            ON channels(user_id)
        ''')
        
        # Добавляем админа если его нет
        await conn.execute('''
            INSERT INTO users (id, username, first_name, status, daily_post_limit)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id) DO UPDATE 
            SET status = EXCLUDED.status
        ''', ADMIN_ID, 'admin', 'Администратор', 'admin', 9999)
        
        logger.info("✅ База данных инициализирована")
        await conn.close()
        
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        raise

# ========== HELPER FUNCTIONS ==========
async def check_user_access(user_id: int) -> bool:
    """Проверяет доступ пользователя к боту"""
    try:
        conn = await get_db_connection()
        user = await conn.fetchrow(
            "SELECT status FROM users WHERE id = $1", 
            user_id
        )
        await conn.close()
        
        if not user:
            # Нового пользователя создаем со статусом active
            conn = await get_db_connection()
            await conn.execute('''
                INSERT INTO users (id, status) VALUES ($1, 'active')
            ''', user_id)
            await conn.close()
            return True
            
        return user['status'] != 'banned'
    except Exception as e:
        logger.error(f"Ошибка проверки доступа: {e}")
        return False

async def get_user_channels(user_id: int) -> List[Dict]:
    """Получает каналы пользователя"""
    try:
        conn = await get_db_connection()
        channels = await conn.fetch(
            "SELECT channel_id, channel_name FROM channels WHERE user_id = $1 AND is_active = TRUE",
            user_id
        )
        await conn.close()
        return [dict(channel) for channel in channels]
    except Exception as e:
        logger.error(f"Ошибка получения каналов: {e}")
        return []

async def add_user_channel(user_id: int, channel_id: int, channel_name: str) -> bool:
    """Добавляет канал пользователя"""
    try:
        conn = await get_db_connection()
        
        # Проверяем, есть ли уже такой канал
        existing = await conn.fetchrow(
            "SELECT id FROM channels WHERE channel_id = $1",
            channel_id
        )
        
        if existing:
            # Обновляем, если канал уже есть
            await conn.execute('''
                UPDATE channels 
                SET user_id = $1, channel_name = $2, is_active = TRUE 
                WHERE channel_id = $3
            ''', user_id, channel_name, channel_id)
        else:
            # Добавляем новый канал
            await conn.execute('''
                INSERT INTO channels (user_id, channel_id, channel_name, is_active)
                VALUES ($1, $2, $3, TRUE)
            ''', user_id, channel_id, channel_name)
        
        await conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка добавления канала: {e}")
        return False

async def get_user_stats(user_id: int) -> Dict:
    """Получает статистику пользователя"""
    try:
        conn = await get_db_connection()
        
        total_posts = await conn.fetchval(
            "SELECT COUNT(*) FROM scheduled_posts WHERE user_id = $1",
            user_id
        )
        
        active_posts = await conn.fetchval(
            "SELECT COUNT(*) FROM scheduled_posts WHERE user_id = $1 AND sent = FALSE",
            user_id
        )
        
        sent_posts = await conn.fetchval(
            "SELECT COUNT(*) FROM scheduled_posts WHERE user_id = $1 AND sent = TRUE",
            user_id
        )
        
        channels_count = await conn.fetchval(
            "SELECT COUNT(*) FROM channels WHERE user_id = $1 AND is_active = TRUE",
            user_id
        )
        
        await conn.close()
        
        return {
            'total_posts': total_posts or 0,
            'active_posts': active_posts or 0,
            'sent_posts': sent_posts or 0,
            'channels': channels_count or 0
        }
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        return {}

def format_datetime(dt: datetime) -> str:
    """Форматирует дату-время в читаемый вид"""
    moscow_time = dt.astimezone(MOSCOW_TZ)
    return moscow_time.strftime("%d.%m.%Y в %H:%М")

def parse_datetime(date_str: str, time_str: str) -> Optional[datetime]:
    """Парсит дату и время из строк"""
    try:
        # Пробуем разные форматы дат
        date_formats = ["%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]
        
        for date_format in date_formats:
            try:
                date_obj = datetime.strptime(date_str.strip(), date_format)
                break
            except ValueError:
                continue
        else:
            return None
        
        # Парсим время
        time_formats = ["%H:%M", "%H.%M"]
        
        for time_format in time_formats:
            try:
                time_obj = datetime.strptime(time_str.strip(), time_format)
                break
            except ValueError:
                continue
        else:
            return None
        
        # Комбинируем дату и время
        combined = datetime.combine(
            date_obj.date(), 
            time_obj.time()
        ).replace(tzinfo=MOSCOW_TZ)
        
        return combined
    except Exception:
        return None

# ========== KEYBOARDS ==========
def get_main_menu(user_id: int) -> InlineKeyboardMarkup:
    """Главное меню"""
    buttons = [
        [InlineKeyboardButton(text="📅 Запланировать пост", callback_data="schedule_post")],
        [InlineKeyboardButton(text="📊 Моя статистика", callback_data="my_stats")],
        [InlineKeyboardButton(text="📢 Мои каналы", callback_data="my_channels")],
    ]
    
    if user_id == ADMIN_ID:
        buttons.append([InlineKeyboardButton(text="👑 Админ-панель", callback_data="admin_panel")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_cancel_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура отмены"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel")]
    ])

def get_channels_keyboard(channels: List[Dict]) -> InlineKeyboardMarkup:
    """Клавиатура с каналами"""
    buttons = []
    for channel in channels:
        name = channel['channel_name'][:20] + "..." if len(channel['channel_name']) > 20 else channel['channel_name']
        buttons.append([InlineKeyboardButton(
            text=f"📢 {name}", 
            callback_data=f"channel_{channel['channel_id']}"
        )])
    
    buttons.append([InlineKeyboardButton(text="➕ Добавить канал", callback_data="add_channel")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_keyboard() -> InlineKeyboardMarkup:
    """Админ-панель"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Общая статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="👥 Управление пользователями", callback_data="admin_users")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])

def get_confirmation_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура подтверждения"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, все верно", callback_data="confirm_yes"),
            InlineKeyboardButton(text="❌ Нет, исправить", callback_data="confirm_no")
        ]
    ])

# ========== STATES ==========
class PostStates(StatesGroup):
    waiting_for_channel = State()
    waiting_for_content = State()
    waiting_for_date = State()
    waiting_for_time = State()
    waiting_for_confirmation = State()

class BroadcastStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_confirmation = State()

# ========== MIDDLEWARE ==========
async def access_middleware(handler, event: types.Message | CallbackQuery, data: Dict):
    """Проверка доступа пользователя"""
    user_id = event.from_user.id if isinstance(event, Message) else event.from_user.id
    
    # Проверяем команды, которые доступны всегда
    allowed_commands = ['/start', '/help', '/cancel']
    
    if isinstance(event, Message) and event.text and any(event.text.startswith(cmd) for cmd in allowed_commands):
        return await handler(event, data)
    
    # Проверяем доступ
    if not await check_user_access(user_id):
        if isinstance(event, Message):
            await event.answer(
                "⛔ Доступ запрещен!\n"
                "Ваш аккаунт заблокирован администратором."
            )
        else:
            await event.answer(
                "⛔ Доступ запрещен!",
                show_alert=True
            )
        return
    
    return await handler(event, data)

# Регистрируем middleware
router.message.middleware(access_middleware)
router.callback_query.middleware(access_middleware)

# ========== HANDLERS ==========
@router.message(CommandStart())
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    user_id = message.from_user.id
    username = message.from_user.username or ""
    first_name = message.from_user.first_name or "Пользователь"
    
    # Регистрируем/обновляем пользователя
    try:
        conn = await get_db_connection()
        await conn.execute('''
            INSERT INTO users (id, username, first_name, status)
            VALUES ($1, $2, $3, 'active')
            ON CONFLICT (id) DO UPDATE 
            SET username = EXCLUDED.username, first_name = EXCLUDED.first_name
        ''', user_id, username, first_name)
        await conn.close()
    except Exception as e:
        logger.error(f"Ошибка регистрации пользователя: {e}")
    
    welcome_text = (
        f"👋 <b>Привет, {first_name}!</b>\n\n"
        "🤖 <b>Я — бот для планирования постов в Telegram</b>\n\n"
        "✨ <b>Что я умею:</b>\n"
        "• 📅 Запланировать пост с текстом и медиа\n"
        "• 📊 Показать вашу статистику\n"
        "• 📢 Управлять каналами\n"
        "• ⏰ Автоматически публиковать в нужное время\n\n"
        "📍 <i>Время указывается по Москве</i>\n\n"
        "👇 <b>Выберите действие:</b>"
    )
    
    await message.answer(
        welcome_text,
        reply_markup=get_main_menu(user_id)
    )

@router.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await state.clear()
    await callback.message.edit_text(
        "🤖 <b>Главное меню</b>\n\n"
        "👇 Выберите действие:",
        reply_markup=get_main_menu(callback.from_user.id)
    )

@router.callback_query(F.data == "cancel")
async def cancel_action(callback: CallbackQuery, state: FSMContext):
    """Отмена текущего действия"""
    await state.clear()
    await callback.message.edit_text(
        "❌ Действие отменено.\n\n"
        "👇 Выберите действие:",
        reply_markup=get_main_menu(callback.from_user.id)
    )

# ========== POST SCHEDULING ==========
@router.callback_query(F.data == "schedule_post")
async def start_scheduling(callback: CallbackQuery, state: FSMContext):
    """Начало планирования поста"""
    user_id = callback.from_user.id
    
    # Проверяем каналы пользователя
    channels = await get_user_channels(user_id)
    
    if not channels:
        # Предлагаем добавить канал
        await callback.message.edit_text(
            "📢 <b>Сначала нужно добавить канал!</b>\n\n"
            "Чтобы запланировать пост, мне нужен доступ к вашему каналу.\n\n"
            "1. Добавьте меня в канал как администратора\n"
            "2. Перешлите любое сообщение из канала\n"
            "3. Или отправьте ID канала в формате <code>-1001234567890</code>\n\n"
            "👇 Нажмите кнопку ниже, чтобы добавить канал:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить канал", callback_data="add_channel")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
            ])
        )
        return
    
    await state.set_state(PostStates.waiting_for_channel)
    await callback.message.edit_text(
        "📢 <b>Выберите канал для поста:</b>\n\n"
        "👇 Выберите из списка:",
        reply_markup=get_channels_keyboard(channels)
    )

@router.callback_query(F.data.startswith("channel_"))
async def select_channel(callback: CallbackQuery, state: FSMContext):
    """Выбор канала из списка"""
    channel_id = int(callback.data.split("_")[1])
    
    # Получаем имя канала
    channels = await get_user_channels(callback.from_user.id)
    channel_name = next((ch['channel_name'] for ch in channels if ch['channel_id'] == channel_id), "Неизвестный канал")
    
    await state.update_data(channel_id=channel_id, channel_name=channel_name)
    await state.set_state(PostStates.waiting_for_content)
    
    await callback.message.edit_text(
        f"📢 <b>Канал выбран:</b> {channel_name}\n\n"
        "📝 <b>Теперь отправьте контент для поста:</b>\n\n"
        "• Текст сообщения\n"
        "• Фотографию с подписью\n"
        "• Видео с подписью\n"
        "• Документ с подписью\n\n"
        "⚠️ <i>Можно отправить только один файл в одном посте</i>",
        reply_markup=get_cancel_keyboard()
    )

@router.callback_query(F.data == "add_channel")
async def add_channel_start(callback: CallbackQuery, state: FSMContext):
    """Добавление нового канала"""
    await state.set_state(PostStates.waiting_for_channel)
    await callback.message.edit_text(
        "📢 <b>Добавление канала</b>\n\n"
        "Чтобы я мог публиковать посты в вашем канале:\n\n"
        "1. Добавьте меня в канал как <b>администратора</b>\n"
        "2. Дайте права на <b>отправку сообщений</b>\n"
        "3. Пришлите мне ID канала в формате <code>-1001234567890</code>\n"
        "4. Или просто перешлите любое сообщение из канала\n\n"
        "📍 ID канала можно получить через бота @username_to_id_bot\n\n"
        "👇 Отправьте ID или перешлите сообщение:",
        reply_markup=get_cancel_keyboard()
    )

@router.message(PostStates.waiting_for_channel)
async def process_channel_input(message: Message, state: FSMContext):
    """Обработка ввода канала"""
    channel_id = None
    channel_name = "Неизвестный канал"
    
    if message.forward_from_chat:
        channel_id = message.forward_from_chat.id
        channel_name = message.forward_from_chat.title
    elif message.text and message.text.startswith('-100'):
        try:
            channel_id = int(message.text.strip())
            channel_name = f"Канал {channel_id}"
        except ValueError:
            await message.answer(
                "❌ Неверный формат ID!\n"
                "ID канала должен начинаться с -100 и содержать только цифры.\n"
                "Пример: <code>-1001234567890</code>",
                reply_markup=get_cancel_keyboard()
            )
            return
    else:
        await message.answer(
            "❌ Пожалуйста, отправьте ID канала или перешлите сообщение из канала.",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    # Проверяем, есть ли бот в канале
    try:
        chat_member = await bot.get_chat_member(channel_id, bot.id)
        if chat_member.status not in ['administrator', 'creator']:
            await message.answer(
                "❌ Я не являюсь администратором в этом канале!\n"
                "Пожалуйста, добавьте меня как администратора с правами на отправку сообщений.",
                reply_markup=get_cancel_keyboard()
            )
            return
    except Exception as e:
        await message.answer(
            f"❌ Не могу получить доступ к каналу!\n"
            f"Ошибка: {str(e)[:100]}\n\n"
            f"Убедитесь, что:\n"
            f"1. Бот добавлен в канал\n"
            f"2. Бот имеет права администратора\n"
            f"3. ID канала правильный",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    # Сохраняем канал
    success = await add_user_channel(message.from_user.id, channel_id, channel_name)
    
    if not success:
        await message.answer(
            "❌ Ошибка при добавлении канала. Попробуйте позже.",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    await state.update_data(channel_id=channel_id, channel_name=channel_name)
    await state.set_state(PostStates.waiting_for_content)
    
    await message.answer(
        f"✅ <b>Канал добавлен:</b> {channel_name}\n\n"
        "📝 <b>Теперь отправьте контент для поста:</b>\n\n"
        "• Текст сообщения\n"
        "• Фотографию с подписью\n"
        "• Видео с подписью\n"
        "• Документ с подписью\n\n"
        "⚠️ <i>Можно отправить только один файл в одном посте</i>",
        reply_markup=get_cancel_keyboard()
    )

@router.message(PostStates.waiting_for_content)
async def process_content(message: Message, state: FSMContext):
    """Обработка контента поста"""
    post_data = {}
    
    # Определяем тип контента
    if message.text:
        if len(message.text) > POST_CHARACTER_LIMIT:
            await message.answer(
                f"❌ Слишком длинный текст!\n"
                f"Максимум {POST_CHARACTER_LIMIT} символов.\n"
                f"У вас: {len(message.text)} символов.",
                reply_markup=get_cancel_keyboard()
            )
            return
        post_data = {
            'message_type': 'text',
            'message_text': message.text,
            'media_file_id': None,
            'media_caption': None
        }
    
    elif message.photo:
        if message.caption and len(message.caption) > 1000:
            await message.answer(
                "❌ Слишком длинная подпись к фото!\n"
                "Максимум 1000 символов.",
                reply_markup=get_cancel_keyboard()
            )
            return
        post_data = {
            'message_type': 'photo',
            'message_text': None,
            'media_file_id': message.photo[-1].file_id,
            'media_caption': message.caption or ''
        }
    
    elif message.video:
        if message.caption and len(message.caption) > 1000:
            await message.answer(
                "❌ Слишком длинная подпись к видео!\n"
                "Максимум 1000 символов.",
                reply_markup=get_cancel_keyboard()
            )
            return
        post_data = {
            'message_type': 'video',
            'message_text': None,
            'media_file_id': message.video.file_id,
            'media_caption': message.caption or ''
        }
    
    elif message.document:
        if message.caption and len(message.caption) > 1000:
            await message.answer(
                "❌ Слишком длинная подпись к документу!\n"
                "Максимум 1000 символов.",
                reply_markup=get_cancel_keyboard()
            )
            return
        post_data = {
            'message_type': 'document',
            'message_text': None,
            'media_file_id': message.document.file_id,
            'media_caption': message.caption or ''
        }
    
    else:
        await message.answer(
            "❌ Неподдерживаемый тип контента!\n"
            "Отправьте текст, фото, видео или документ.",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    await state.update_data(**post_data)
    await state.set_state(PostStates.waiting_for_date)
    
    now_moscow = datetime.now(MOSCOW_TZ)
    tomorrow = (now_moscow + timedelta(days=1)).strftime("%d.%m.%Y")
    
    await message.answer(
        "📅 <b>Теперь укажите дату публикации:</b>\n\n"
        f"📅 <b>Формат:</b> ДД.ММ.ГГГГ\n"
        f"📅 <b>Пример:</b> {tomorrow}\n\n"
        f"📍 <i>Сегодня: {now_moscow.strftime('%d.%m.%Y')}</i>",
        reply_markup=get_cancel_keyboard()
    )

@router.message(PostStates.waiting_for_date)
async def process_date(message: Message, state: FSMContext):
    """Обработка даты публикации"""
    date_str = message.text.strip()
    
    # Пробуем распарсить дату
    now_moscow = datetime.now(MOSCOW_TZ)
    date_obj = parse_datetime(date_str, "00:00")
    
    if not date_obj:
        await message.answer(
            "❌ Неверный формат даты!\n"
            "Используйте: <b>ДД.ММ.ГГГГ</b>\n"
            f"Пример: <code>{now_moscow.strftime('%d.%m.%Y')}</code>",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    # Проверяем, что дата не в прошлом
    if date_obj.date() < now_moscow.date():
        await message.answer(
            "❌ Дата не может быть в прошлом!\n"
            f"Сегодня: {now_moscow.strftime('%d.%m.%Y')}\n"
            "Укажите сегодняшнюю или будущую дату.",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    # Проверяем, что не слишком далеко (максимум 1 год)
    max_date = now_moscow + timedelta(days=365)
    if date_obj > max_date:
        await message.answer(
            "❌ Слишком далекая дата!\n"
            "Максимум можно запланировать на год вперед.",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    await state.update_data(date_str=date_str, date_obj=date_obj)
    await state.set_state(PostStates.waiting_for_time)
    
    await message.answer(
        "⏰ <b>Теперь укажите время публикации:</b>\n\n"
        "⏰ <b>Формат:</b> ЧЧ:ММ\n"
        "⏰ <b>Пример:</b> 14:30\n\n"
        f"📍 <i>Сейчас: {now_moscow.strftime('%H:%M')}</i>",
        reply_markup=get_cancel_keyboard()
    )

@router.message(PostStates.waiting_for_time)
async def process_time(message: Message, state: FSMContext):
    """Обработка времени публикации"""
    time_str = message.text.strip()
    
    data = await state.get_data()
    date_str = data.get('date_str')
    
    # Пробуем распарсить время
    scheduled_time = parse_datetime(date_str, time_str)
    
    if not scheduled_time:
        await message.answer(
            "❌ Неверный формат времени!\n"
            "Используйте: <b>ЧЧ:ММ</b>\n"
            "Пример: <code>14:30</code>",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    # Проверяем, что время не в прошлом
    now_moscow = datetime.now(MOSCOW_TZ)
    if scheduled_time < now_moscow:
        await message.answer(
            "❌ Время не может быть в прошлом!\n"
            f"Сейчас: {now_moscow.strftime('%H:%M')}\n"
            "Укажите будущее время.",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    await state.update_data(time_str=time_str, scheduled_time=scheduled_time)
    
    # Показываем превью и запрашиваем подтверждение
    data = await state.get_data()
    await show_post_preview(message, data)
    
    await state.set_state(PostStates.waiting_for_confirmation)

async def show_post_preview(message: Message, data: Dict):
    """Показывает превью поста"""
    channel_name = data.get('channel_name', 'Неизвестный канал')
    scheduled_time = data.get('scheduled_time')
    message_type = data.get('message_type')
    message_text = data.get('message_text')
    media_caption = data.get('media_caption', '')
    
    preview_text = (
        f"📋 <b>Превью поста</b>\n\n"
        f"📢 <b>Канал:</b> {channel_name}\n"
        f"⏰ <b>Время публикации:</b> {format_datetime(scheduled_time)}\n\n"
    )
    
    if message_type == 'text':
        preview_text += f"📝 <b>Текст:</b>\n{message_text[:300]}..."
        if len(message_text) > 300:
            preview_text += "\n<i>(показаны первые 300 символов)</i>"
    
    elif message_type in ['photo', 'video', 'document']:
        media_type = {
            'photo': '🖼 Фото',
            'video': '🎬 Видео',
            'document': '📄 Документ'
        }.get(message_type, 'Медиа')
        
        preview_text += f"{media_type} с подписью:\n"
        if media_caption:
            preview_text += f"{media_caption[:300]}..."
            if len(media_caption) > 300:
                preview_text += "\n<i>(показаны первые 300 символов)</i>"
        else:
            preview_text += "<i>Без подписи</i>"
    
    preview_text += "\n\n✅ <b>Все верно?</b>"
    
    # Отправляем превью
    if message_type == 'text':
        await message.answer(
            preview_text,
            reply_markup=get_confirmation_keyboard()
        )
    else:
        # Для медиа пытаемся отправить превью
        media_file_id = data.get('media_file_id')
        try:
            if message_type == 'photo':
                await bot.send_photo(
                    chat_id=message.chat.id,
                    photo=media_file_id,
                    caption=preview_text,
                    reply_markup=get_confirmation_keyboard()
                )
            elif message_type == 'video':
                await bot.send_video(
                    chat_id=message.chat.id,
                    video=media_file_id,
                    caption=preview_text,
                    reply_markup=get_confirmation_keyboard()
                )
            elif message_type == 'document':
                await bot.send_document(
                    chat_id=message.chat.id,
                    document=media_file_id,
                    caption=preview_text,
                    reply_markup=get_confirmation_keyboard()
                )
        except Exception as e:
            logger.error(f"Ошибка отправки превью: {e}")
            await message.answer(
                preview_text,
                reply_markup=get_confirmation_keyboard()
            )

@router.callback_query(F.data == "confirm_yes")
async def confirm_post(callback: CallbackQuery, state: FSMContext):
    """Подтверждение поста"""
    data = await state.get_data()
    user_id = callback.from_user.id
    
    # Проверяем лимиты пользователя
    stats = await get_user_stats(user_id)
    if stats['active_posts'] >= MAX_POSTS_PER_USER:
        await callback.message.edit_text(
            f"❌ <b>Достигнут лимит постов!</b>\n\n"
            f"У вас уже {stats['active_posts']} активных постов.\n"
            f"Максимум: {MAX_POSTS_PER_USER} постов одновременно.\n\n"
            "Дождитесь публикации или удалите старые посты.",
            reply_markup=get_main_menu(user_id)
        )
        await state.clear()
        return
    
    # Сохраняем пост в БД
    try:
        conn = await get_db_connection()
        post_id = await conn.fetchval('''
            INSERT INTO scheduled_posts 
            (user_id, channel_id, message_type, message_text, media_file_id, media_caption, scheduled_time)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
        ''', 
        user_id,
        data['channel_id'],
        data['message_type'],
        data.get('message_text'),
        data.get('media_file_id'),
        data.get('media_caption'),
        data['scheduled_time']
        )
        
        # Планируем отправку
        scheduler.add_job(
            send_scheduled_post,
            trigger=DateTrigger(run_date=data['scheduled_time']),
            args=(data['channel_id'], data, post_id),
            id=f"post_{post_id}",
            replace_existing=True
        )
        
        await conn.close()
        
        # Отправляем подтверждение
        await callback.message.edit_text(
            f"✅ <b>Пост успешно запланирован!</b>\n\n"
            f"📢 <b>Канал:</b> {data['channel_name']}\n"
            f"⏰ <b>Время:</b> {format_datetime(data['scheduled_time'])}\n"
            f"🆔 <b>ID поста:</b> <code>{post_id}</code>\n\n"
            f"📍 Пост будет автоматически опубликован в указанное время.\n\n"
            f"👇 Что дальше?",
            reply_markup=get_main_menu(user_id)
        )
        
        logger.info(f"Пост {post_id} запланирован пользователем {user_id}")
        
    except Exception as e:
        logger.error(f"Ошибка сохранения поста: {e}")
        await callback.message.edit_text(
            f"❌ <b>Ошибка при сохранении поста!</b>\n\n"
            f"Ошибка: {str(e)[:200]}\n\n"
            f"Попробуйте еще раз или обратитесь в поддержку.",
            reply_markup=get_main_menu(user_id)
        )
    
    await state.clear()

@router.callback_query(F.data == "confirm_no")
async def reject_post(callback: CallbackQuery, state: FSMContext):
    """Отказ от поста"""
    await state.clear()
    await callback.message.edit_text(
        "❌ <b>Планирование отменено</b>\n\n"
        "Вы можете начать заново.\n\n"
        "👇 Выберите действие:",
        reply_markup=get_main_menu(callback.from_user.id)
    )

async def send_scheduled_post(channel_id: int, post_data: Dict, post_id: int):
    """Отправка запланированного поста"""
    try:
        message_type = post_data.get('message_type')
        
        if message_type == 'text':
            await bot.send_message(
                chat_id=channel_id,
                text=post_data.get('message_text'),
                parse_mode="HTML"
            )
            
        elif message_type == 'photo':
            await bot.send_photo(
                chat_id=channel_id,
                photo=post_data.get('media_file_id'),
                caption=post_data.get('media_caption'),
                parse_mode="HTML"
            )
            
        elif message_type == 'video':
            await bot.send_video(
                chat_id=channel_id,
                video=post_data.get('media_file_id'),
                caption=post_data.get('media_caption'),
                parse_mode="HTML"
            )
            
        elif message_type == 'document':
            await bot.send_document(
                chat_id=channel_id,
                document=post_data.get('media_file_id'),
                caption=post_data.get('media_caption'),
                parse_mode="HTML"
            )
        
        # Обновляем статус в БД
        conn = await get_db_connection()
        await conn.execute('''
            UPDATE scheduled_posts 
            SET sent = TRUE, sent_at = NOW() 
            WHERE id = $1
        ''', post_id)
        await conn.close()
        
        logger.info(f"✅ Пост {post_id} отправлен в канал {channel_id}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки поста {post_id}: {e}")
        
        # Сохраняем ошибку в БД
        try:
            conn = await get_db_connection()
            await conn.execute('''
                UPDATE scheduled_posts 
                SET error_message = $1 
                WHERE id = $2
            ''', str(e)[:500], post_id)
            await conn.close()
        except Exception as db_error:
            logger.error(f"Ошибка сохранения ошибки поста: {db_error}")

# ========== STATISTICS ==========
@router.callback_query(F.data == "my_stats")
async def show_my_stats(callback: CallbackQuery):
    """Показ статистики пользователя"""
    user_id = callback.from_user.id
    stats = await get_user_stats(user_id)
    
    stats_text = (
        f"📊 <b>Ваша статистика</b>\n\n"
        f"👤 <b>Пользователь:</b> {callback.from_user.first_name}\n"
        f"📅 <b>Всего постов:</b> {stats['total_posts']}\n"
        f"✅ <b>Опубликовано:</b> {stats['sent_posts']}\n"
        f"⏳ <b>Ожидает публикации:</b> {stats['active_posts']}\n"
        f"📢 <b>Каналов:</b> {stats['channels']}\n\n"
        f"📍 <i>Обновлено: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}</i>"
    )
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=get_main_menu(user_id)
    )

@router.callback_query(F.data == "my_channels")
async def show_my_channels(callback: CallbackQuery):
    """Показ каналов пользователя"""
    user_id = callback.from_user.id
    channels = await get_user_channels(user_id)
    
    if not channels:
        await callback.message.edit_text(
            "📢 <b>У вас нет добавленных каналов</b>\n\n"
            "Добавьте канал, чтобы начать планировать посты.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить канал", callback_data="add_channel")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
            ])
        )
        return
    
    channels_text = "📢 <b>Ваши каналы:</b>\n\n"
    for i, channel in enumerate(channels, 1):
        channels_text += f"{i}. {channel['channel_name']}\n"
    
    channels_text += f"\n📍 Всего: {len(channels)} каналов"
    
    await callback.message.edit_text(
        channels_text,
        reply_markup=get_main_menu(user_id)
    )

# ========== ADMIN PANEL ==========
@router.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    """Админ-панель"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Только для администратора!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "👑 <b>Админ-панель</b>\n\n"
        "👇 Выберите действие:",
        reply_markup=get_admin_keyboard()
    )

@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    """Общая статистика для админа"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Только для администратора!", show_alert=True)
        return
    
    try:
        conn = await get_db_connection()
        
        # Общая статистика
        total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
        active_users = await conn.fetchval("SELECT COUNT(*) FROM users WHERE status = 'active'")
        banned_users = await conn.fetchval("SELECT COUNT(*) FROM users WHERE status = 'banned'")
        
        total_posts = await conn.fetchval("SELECT COUNT(*) FROM scheduled_posts")
        active_posts = await conn.fetchval("SELECT COUNT(*) FROM scheduled_posts WHERE sent = FALSE")
        sent_posts = await conn.fetchval("SELECT COUNT(*) FROM scheduled_posts WHERE sent = TRUE")
        
        total_channels = await conn.fetchval("SELECT COUNT(*) FROM channels WHERE is_active = TRUE")
        
        await conn.close()
        
        stats_text = (
            f"📊 <b>Общая статистика бота</b>\n\n"
            f"👥 <b>Пользователи:</b>\n"
            f"   • Всего: {total_users}\n"
            f"   • Активных: {active_users}\n"
            f"   • Заблокированных: {banned_users}\n\n"
            f"📅 <b>Посты:</b>\n"
            f"   • Всего запланировано: {total_posts}\n"
            f"   • Ожидает публикации: {active_posts}\n"
            f"   • Опубликовано: {sent_posts}\n\n"
            f"📢 <b>Каналы:</b> {total_channels}\n\n"
            f"📍 <i>Обновлено: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}</i>"
        )
        
        await callback.message.edit_text(
            stats_text,
            reply_markup=get_admin_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        await callback.message.edit_text(
            f"❌ Ошибка получения статистики: {e}",
            reply_markup=get_admin_keyboard()
        )

@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    """Начало рассылки"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Только для администратора!", show_alert=True)
        return
    
    await state.set_state(BroadcastStates.waiting_for_message)
    await callback.message.edit_text(
        "📢 <b>Рассылка сообщений</b>\n\n"
        "Отправьте сообщение, которое нужно разослать всем активным пользователям.\n\n"
        "⚠️ <i>Поддерживается любой тип контента: текст, фото, видео и т.д.</i>",
        reply_markup=get_cancel_keyboard()
    )

@router.message(BroadcastStates.waiting_for_message)
async def process_broadcast_message(message: Message, state: FSMContext):
    """Обработка сообщения для рассылки"""
    # Сохраняем сообщение
    await state.update_data(broadcast_message=message)
    await state.set_state(BroadcastStates.waiting_for_confirmation)
    
    # Получаем количество пользователей
    try:
        conn = await get_db_connection()
        user_count = await conn.fetchval("SELECT COUNT(*) FROM users WHERE status = 'active'")
        await conn.close()
    except Exception as e:
        user_count = 0
        logger.error(f"Ошибка получения количества пользователей: {e}")
    
    await message.answer(
        f"📢 <b>Подтверждение рассылки</b>\n\n"
        f"📍 <b>Получателей:</b> {user_count} активных пользователей\n\n"
        f"✅ <b>Начать рассылку?</b>",
        reply_markup=get_confirmation_keyboard()
    )

@router.callback_query(BroadcastStates.waiting_for_confirmation, F.data == "confirm_yes")
async def confirm_broadcast(callback: CallbackQuery, state: FSMContext):
    """Подтверждение рассылки"""
    data = await state.get_data()
    broadcast_message = data.get('broadcast_message')
    
    if not broadcast_message:
        await callback.answer("❌ Сообщение не найдено!", show_alert=True)
        return
    
    await callback.message.edit_text("📢 Начинаю рассылку...")
    
    # Получаем пользователей
    try:
        conn = await get_db_connection()
        users = await conn.fetch("SELECT id FROM users WHERE status = 'active'")
        await conn.close()
    except Exception as e:
        logger.error(f"Ошибка получения пользователей: {e}")
        await callback.message.edit_text("❌ Ошибка получения списка пользователей")
        await state.clear()
        return
    
    total = len(users)
    success = 0
    failed = 0
    
    # Отправляем сообщения
    for i, user in enumerate(users):
        try:
            # Копируем сообщение
            if broadcast_message.text:
                await bot.send_message(user['id'], broadcast_message.text)
            elif broadcast_message.photo:
                await bot.send_photo(user['id'], broadcast_message.photo[-1].file_id, 
                                   caption=broadcast_message.caption)
            elif broadcast_message.video:
                await bot.send_video(user['id'], broadcast_message.video.file_id,
                                   caption=broadcast_message.caption)
            elif broadcast_message.document:
                await bot.send_document(user['id'], broadcast_message.document.file_id,
                                      caption=broadcast_message.caption)
            else:
                await bot.copy_message(user['id'], broadcast_message.chat.id, broadcast_message.message_id)
            
            success += 1
            
            # Обновляем статус каждые 10 сообщений
            if (i + 1) % 10 == 0:
                await callback.message.edit_text(f"📢 Рассылка: {i + 1}/{total} отправлено...")
            
            # Задержка против лимитов Telegram
            await asyncio.sleep(0.1)
            
        except Exception as e:
            failed += 1
            logger.error(f"Ошибка отправки пользователю {user['id']}: {e}")
    
    # Итоги
    await callback.message.edit_text(
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"📊 <b>Итоги:</b>\n"
        f"• Всего получателей: {total}\n"
        f"• Успешно отправлено: {success}\n"
        f"• Не удалось отправить: {failed}\n\n"
        f"📍 <i>Время: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}</i>",
        reply_markup=get_admin_keyboard()
    )
    
    await state.clear()

# ========== RESTORE JOBS ==========
async def restore_scheduled_jobs():
    """Восстановление запланированных постов при перезапуске"""
    try:
        conn = await get_db_connection()
        posts = await conn.fetch('''
            SELECT sp.id, sp.channel_id, sp.message_type, sp.message_text, 
                   sp.media_file_id, sp.media_caption, sp.scheduled_time
            FROM scheduled_posts sp
            WHERE sp.sent = FALSE AND sp.scheduled_time > NOW()
            ORDER BY sp.scheduled_time
        ''')
        await conn.close()
        
        restored = 0
        for post in posts:
            try:
                post_data = {
                    'message_type': post['message_type'],
                    'message_text': post['message_text'],
                    'media_file_id': post['media_file_id'],
                    'media_caption': post['media_caption']
                }
                
                scheduler.add_job(
                    send_scheduled_post,
                    trigger=DateTrigger(run_date=post['scheduled_time']),
                    args=(post['channel_id'], post_data, post['id']),
                    id=f"post_{post['id']}",
                    replace_existing=True
                )
                restored += 1
            except Exception as e:
                logger.error(f"Ошибка восстановления поста {post['id']}: {e}")
        
        logger.info(f"✅ Восстановлено {restored} запланированных постов")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при восстановлении постов: {e}")

# ========== STARTUP/SHUTDOWN ==========
async def on_startup():
    """Действия при запуске бота"""
    logger.info("🚀 Запуск бота...")
    
    try:
        # Инициализация БД
        await init_db()
        logger.info("✅ База данных инициализирована")
        
        # Восстановление задач
        await restore_scheduled_jobs()
        
        # Запуск планировщика
        scheduler.start()
        logger.info("✅ Планировщик запущен")
        
        # Проверяем, что бот работает
        me = await bot.get_me()
        logger.info(f"✅ Бот @{me.username} запущен")
        
        # Уведомление админу
        if ADMIN_ID:
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"🤖 Бот @{me.username} успешно запущен!\n"
                    f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M:%S')}\n"
                    f"📍 Время московское"
                )
                logger.info(f"✅ Уведомление отправлено админу {ADMIN_ID}")
            except Exception as e:
                logger.warning(f"Не удалось отправить уведомление админу: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске: {e}")
        return False

async def on_shutdown():
    """Действия при выключении бота"""
    logger.info("🛑 Выключение бота...")
    
    # Останавливаем планировщик
    if scheduler.running:
        scheduler.shutdown()
        logger.info("✅ Планировщик остановлен")
    
    logger.info("👋 Бот выключен")

# ========== MAIN ==========
async def main():
    """Основная функция"""
    # Настройка логирования
    logger.info("=" * 50)
    logger.info(f"🤖 Запуск бота...")
    logger.info(f"👑 Admin ID: {ADMIN_ID}")
    logger.info(f"🌐 Database: {'Настроена' if DATABASE_URL else 'Нет'}")
    logger.info(f"📅 Timezone: {MOSCOW_TZ}")
    logger.info("=" * 50)
    
    # Запуск startup процедур
    if not await on_startup():
        logger.error("❌ Не удалось запустить бота")
        return
    
    try:
        # Запуск polling
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
    finally:
        # Выполняем shutdown процедуры
        await on_shutdown()

if __name__ == "__main__":
    asyncio.run(main())
