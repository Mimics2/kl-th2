import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List

import pytz
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ContentType
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

# ========== CONFIG ==========
API_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

# Настройки
MOSCOW_TZ = pytz.timezone('Europe/Moscow')
MAX_POSTS_PER_USER = 100
POST_CHARACTER_LIMIT = 4000

# ========== SETUP ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

scheduler = AsyncIOScheduler(timezone=MOSCOW_TZ)

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
        
        # Удаляем старые таблицы если есть проблемы (только для разработки!)
        # await conn.execute('DROP TABLE IF EXISTS scheduled_posts CASCADE')
        # await conn.execute('DROP TABLE IF EXISTS channels CASCADE')
        # await conn.execute('DROP TABLE IF EXISTS users CASCADE')
        
        # Таблица пользователей
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                is_admin BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Таблица каналов
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT,
                channel_id BIGINT UNIQUE NOT NULL,
                channel_name TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Таблица запланированных постов
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_posts (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT,
                channel_id BIGINT,
                message_type TEXT NOT NULL,
                message_text TEXT,
                media_file_id TEXT,
                media_caption TEXT,
                scheduled_time TIMESTAMP NOT NULL,
                is_sent BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Добавляем админа
        if ADMIN_ID > 0:
            await conn.execute('''
                INSERT INTO users (id, username, first_name, is_active, is_admin)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (id) DO UPDATE SET
                is_admin = EXCLUDED.is_admin
            ''', ADMIN_ID, 'admin', 'Администратор', True, True)
        
        logger.info("✅ База данных инициализирована")
        await conn.close()
        
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        raise

async def check_user_access(user_id: int) -> bool:
    """Проверяет доступ пользователя к боту"""
    try:
        conn = await get_db_connection()
        user = await conn.fetchrow(
            "SELECT is_active FROM users WHERE id = $1", 
            user_id
        )
        await conn.close()
        
        if not user:
            # Создаем нового пользователя
            conn = await get_db_connection()
            await conn.execute('''
                INSERT INTO users (id, is_active) VALUES ($1, TRUE)
            ''', user_id)
            await conn.close()
            return True
            
        return user['is_active']
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
        
        await conn.execute('''
            INSERT INTO channels (user_id, channel_id, channel_name, is_active)
            VALUES ($1, $2, $3, TRUE)
            ON CONFLICT (channel_id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            channel_name = EXCLUDED.channel_name,
            is_active = TRUE
        ''', user_id, channel_id, channel_name)
        
        await conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка добавления канала: {e}")
        return False

async def save_scheduled_post(user_id: int, channel_id: int, post_data: Dict, scheduled_time: datetime) -> Optional[int]:
    """Сохраняет запланированный пост в БД"""
    try:
        conn = await get_db_connection()
        
        post_id = await conn.fetchval('''
            INSERT INTO scheduled_posts 
            (user_id, channel_id, message_type, message_text, media_file_id, media_caption, scheduled_time)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
        ''', 
        user_id,
        channel_id,
        post_data.get('message_type'),
        post_data.get('message_text'),
        post_data.get('media_file_id'),
        post_data.get('media_caption'),
        scheduled_time
        )
        
        await conn.close()
        return post_id
    except Exception as e:
        logger.error(f"Ошибка сохранения поста: {e}")
        return None

async def get_user_stats(user_id: int) -> Dict:
    """Получает статистику пользователя"""
    try:
        conn = await get_db_connection()
        
        total_posts = await conn.fetchval(
            "SELECT COUNT(*) FROM scheduled_posts WHERE user_id = $1",
            user_id
        ) or 0
        
        active_posts = await conn.fetchval(
            "SELECT COUNT(*) FROM scheduled_posts WHERE user_id = $1 AND is_sent = FALSE",
            user_id
        ) or 0
        
        sent_posts = await conn.fetchval(
            "SELECT COUNT(*) FROM scheduled_posts WHERE user_id = $1 AND is_sent = TRUE",
            user_id
        ) or 0
        
        channels_count = await conn.fetchval(
            "SELECT COUNT(*) FROM channels WHERE user_id = $1 AND is_active = TRUE",
            user_id
        ) or 0
        
        await conn.close()
        
        return {
            'total_posts': total_posts,
            'active_posts': active_posts,
            'sent_posts': sent_posts,
            'channels': channels_count
        }
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}")
        return {'total_posts': 0, 'active_posts': 0, 'sent_posts': 0, 'channels': 0}

async def get_total_stats() -> Dict:
    """Получает общую статистику для админа"""
    try:
        conn = await get_db_connection()
        
        total_users = await conn.fetchval("SELECT COUNT(*) FROM users") or 0
        active_users = await conn.fetchval("SELECT COUNT(*) FROM users WHERE is_active = TRUE") or 0
        
        total_posts = await conn.fetchval("SELECT COUNT(*) FROM scheduled_posts") or 0
        active_posts = await conn.fetchval("SELECT COUNT(*) FROM scheduled_posts WHERE is_sent = FALSE") or 0
        sent_posts = await conn.fetchval("SELECT COUNT(*) FROM scheduled_posts WHERE is_sent = TRUE") or 0
        
        total_channels = await conn.fetchval("SELECT COUNT(*) FROM channels WHERE is_active = TRUE") or 0
        
        await conn.close()
        
        return {
            'total_users': total_users,
            'active_users': active_users,
            'total_posts': total_posts,
            'active_posts': active_posts,
            'sent_posts': sent_posts,
            'total_channels': total_channels
        }
    except Exception as e:
        logger.error(f"Ошибка получения общей статистики: {e}")
        return {}

def format_datetime(dt: datetime) -> str:
    """Форматирует дату-время в читаемый вид"""
    moscow_time = dt.astimezone(MOSCOW_TZ)
    return moscow_time.strftime("%d.%m.%Y в %H:%M")

def parse_datetime(date_str: str, time_str: str) -> Optional[datetime]:
    """Парсит дату и время из строк"""
    try:
        date_str = date_str.strip()
        time_str = time_str.strip()
        
        # Парсим дату
        date_formats = ["%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]
        date_obj = None
        
        for fmt in date_formats:
            try:
                date_obj = datetime.strptime(date_str, fmt)
                break
            except ValueError:
                continue
        
        if not date_obj:
            return None
        
        # Парсим время
        time_formats = ["%H:%M", "%H.%M"]
        time_obj = None
        
        for fmt in time_formats:
            try:
                time_obj = datetime.strptime(time_str, fmt)
                break
            except ValueError:
                continue
        
        if not time_obj:
            return None
        
        # Комбинируем
        combined = datetime.combine(
            date_obj.date(), 
            time_obj.time()
        )
        
        return MOSCOW_TZ.localize(combined)
    except Exception:
        return None

# ========== KEYBOARDS ==========
def get_main_menu(user_id: int, is_admin: bool = False) -> InlineKeyboardMarkup:
    """Главное меню - ТОЛЬКО обычные функции"""
    buttons = [
        [InlineKeyboardButton(text="📅 Запланировать пост", callback_data="schedule_post")],
        [InlineKeyboardButton(text="📊 Моя статистика", callback_data="my_stats")],
        [InlineKeyboardButton(text="📢 Мои каналы", callback_data="my_channels")],
    ]
    
    # ТОЛЬКО админ видит админ-панель
    if is_admin:
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
        name = channel['channel_name']
        if len(name) > 20:
            name = name[:20] + "..."
        buttons.append([InlineKeyboardButton(
            text=f"📢 {name}", 
            callback_data=f"channel_{channel['channel_id']}"
        )])
    
    buttons.append([InlineKeyboardButton(text="➕ Добавить новый канал", callback_data="add_channel")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_confirmation_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура подтверждения"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, все верно", callback_data="confirm_yes"),
            InlineKeyboardButton(text="🔄 Нет, начать заново", callback_data="confirm_no")
        ]
    ])

def get_admin_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура админ-панели (ТОЛЬКО ДЛЯ АДМИНА)"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Общая статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="📢 Сделать рассылку", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ])

# ========== STATES ==========
class PostStates(StatesGroup):
    waiting_for_channel = State()
    waiting_for_content = State()
    waiting_for_date = State()
    waiting_for_time = State()
    waiting_for_confirmation = State()

# ========== HANDLERS ==========
@router.message(CommandStart())
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    user_id = message.from_user.id
    username = message.from_user.username or ""
    first_name = message.from_user.first_name or "Пользователь"
    
    # Проверяем, является ли пользователь админом
    is_admin = user_id == ADMIN_ID
    
    # Регистрируем пользователя
    try:
        conn = await get_db_connection()
        await conn.execute('''
            INSERT INTO users (id, username, first_name, is_admin)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE 
            SET username = EXCLUDED.username, first_name = EXCLUDED.first_name
        ''', user_id, username, first_name, is_admin)
        await conn.close()
    except Exception as e:
        logger.error(f"Ошибка регистрации пользователя: {e}")
    
    # Простое приветствие без HTML разметки
    welcome_text = (
        "👋 Привет, {name}!\n\n"
        "🤖 Я — бот для планирования постов в Telegram каналах.\n\n"
        "✨ Что я умею:\n"
        "• 📅 Запланировать пост с текстом, фото, видео или документом\n"
        "• 📊 Показать вашу статистику\n"
        "• 📢 Управлять вашими каналами\n"
        "• ⏰ Автоматически публиковать в нужное время\n\n"
        "📍 Время указывается по Москве\n\n"
        "👇 Выберите действие:"
    ).format(name=first_name)
    
    await message.answer(
        welcome_text,
        reply_markup=get_main_menu(user_id, is_admin)
    )

@router.message(Command("help"))
async def cmd_help(message: Message):
    """Команда помощи"""
    help_text = (
        "📚 Помощь по использованию бота:\n\n"
        
        "1. Добавьте меня в канал как администратора\n"
        "2. Дайте права на отправку сообщений\n"
        "3. Перешлите мне любое сообщение из канала\n\n"
        
        "📅 Планирование поста:\n"
        "1. Выберите 'Запланировать пост'\n"
        "2. Выберите канал\n"
        "3. Отправьте контент (текст, фото, видео или документ)\n"
        "4. Укажите дату в формате ДД.ММ.ГГГГ\n"
        "5. Укажите время в формате ЧЧ:ММ\n"
        "6. Подтвердите публикацию\n\n"
        
        "💡 Советы:\n"
        "• Вы можете запланировать пост на несколько месяцев вперед\n"
        "• Посты публикуются автоматически\n"
        "• Вы можете добавить несколько каналов\n\n"
        
        "📞 Поддержка: @ваш_username_для_поддержки"
    )
    
    await message.answer(help_text)

@router.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await state.clear()
    is_admin = callback.from_user.id == ADMIN_ID
    
    await callback.message.edit_text(
        "🤖 Главное меню\n\n"
        "👇 Выберите действие:",
        reply_markup=get_main_menu(callback.from_user.id, is_admin)
    )

@router.callback_query(F.data == "cancel")
async def cancel_action(callback: CallbackQuery, state: FSMContext):
    """Отмена текущего действия"""
    await state.clear()
    is_admin = callback.from_user.id == ADMIN_ID
    
    await callback.message.edit_text(
        "❌ Действие отменено.\n\n"
        "👇 Выберите действие:",
        reply_markup=get_main_menu(callback.from_user.id, is_admin)
    )

# ========== POST SCHEDULING ==========
@router.callback_query(F.data == "schedule_post")
async def start_scheduling(callback: CallbackQuery, state: FSMContext):
    """Начало планирования поста"""
    user_id = callback.from_user.id
    
    if not await check_user_access(user_id):
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    channels = await get_user_channels(user_id)
    
    if not channels:
        await callback.message.edit_text(
            "📢 Сначала нужно добавить канал!\n\n"
            "Чтобы запланировать пост, добавьте меня в канал как администратора "
            "и перешлите любое сообщение из канала.\n\n"
            "👇 Нажмите кнопку ниже, чтобы добавить канал:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить канал", callback_data="add_channel")],
                [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
            ])
        )
        return
    
    await state.set_state(PostStates.waiting_for_channel)
    await callback.message.edit_text(
        "📢 Выберите канал для поста:\n\n"
        "👇 Выберите из списка ваших каналов:",
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
        f"✅ Канал выбран: {channel_name}\n\n"
        "📝 Теперь отправьте контент для поста:\n\n"
        "• Текст сообщения\n"
        "• Фотографию с подписью\n"
        "• Видео с подписью\n"
        "• Документ с подписью\n\n"
        "❓ Если хотите добавить фото с текстом, просто отправьте фото и в подписи напишите текст.",
        reply_markup=get_cancel_keyboard()
    )

@router.callback_query(F.data == "add_channel")
async def add_channel_start(callback: CallbackQuery, state: FSMContext):
    """Добавление нового канала"""
    await state.set_state(PostStates.waiting_for_channel)
    await callback.message.edit_text(
        "📢 Добавление канала\n\n"
        "Чтобы я мог публиковать посты в вашем канале:\n\n"
        "1. Добавьте меня в канал как администратора\n"
        "2. Дайте права на отправку сообщений\n"
        "3. Пришлите мне ID канала в формате -1001234567890\n"
        "4. Или просто перешлите любое сообщение из канала\n\n"
        "🔧 ID канала можно получить через бота @username_to_id_bot\n\n"
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
                "❌ Неверный формат ID!\n\n"
                "ID канала должен начинаться с -100 и содержать только цифры.\n"
                "Пример: -1001234567890\n\n"
                "Попробуйте еще раз:",
                reply_markup=get_cancel_keyboard()
            )
            return
    else:
        await message.answer(
            "❌ Пожалуйста, отправьте ID канала или перешлите сообщение из канала.",
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
        f"✅ Канал добавлен: {channel_name}\n\n"
        "📝 Теперь отправьте контент для поста:\n\n"
        "• Текст сообщения\n"
        "• Фотографию с подписью\n"
        "• Видео с подписью\n"
        "• Документ с подписью\n\n"
        "❓ Если хотите добавить фото с текстом, просто отправьте фото и в подписи напишите текст.",
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
                f"Максимум {POST_CHARACTER_LIMIT} символов.\n\n"
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
        post_data = {
            'message_type': 'photo',
            'message_text': None,
            'media_file_id': message.photo[-1].file_id,
            'media_caption': message.caption or ''
        }
    
    elif message.video:
        post_data = {
            'message_type': 'video',
            'message_text': None,
            'media_file_id': message.video.file_id,
            'media_caption': message.caption or ''
        }
    
    elif message.document:
        post_data = {
            'message_type': 'document',
            'message_text': None,
            'media_file_id': message.document.file_id,
            'media_caption': message.caption or ''
        }
    
    else:
        await message.answer(
            "❌ Неподдерживаемый тип контента!\n\n"
            "Отправьте текст, фото, видео или документ.",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    await state.update_data(**post_data)
    await state.set_state(PostStates.waiting_for_date)
    
    now_moscow = datetime.now(MOSCOW_TZ)
    tomorrow = (now_moscow + timedelta(days=1)).strftime("%d.%m.%Y")
    
    await message.answer(
        "📅 Теперь укажите дату публикации:\n\n"
        f"Формат: ДД.ММ.ГГГГ\n"
        f"Пример: {tomorrow}\n\n"
        f"Сегодня: {now_moscow.strftime('%d.%m.%Y')}",
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
            "❌ Неверный формат даты!\n\n"
            "Используйте: ДД.ММ.ГГГГ\n"
            f"Пример: {now_moscow.strftime('%d.%m.%Y')}\n\n"
            "Попробуйте еще раз:",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    # Проверяем, что дата не в прошлом
    if date_obj.date() < now_moscow.date():
        await message.answer(
            "❌ Дата не может быть в прошлом!\n\n"
            f"Сегодня: {now_moscow.strftime('%d.%m.%Y')}\n"
            "Укажите сегодняшнюю или будущую дату.",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    await state.update_data(date_str=date_str, date_obj=date_obj)
    await state.set_state(PostStates.waiting_for_time)
    
    await message.answer(
        "⏰ Теперь укажите время публикации:\n\n"
        "Формат: ЧЧ:ММ\n"
        "Пример: 14:30\n\n"
        f"Сейчас: {now_moscow.strftime('%H:%M')}",
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
            "❌ Неверный формат времени!\n\n"
            "Используйте: ЧЧ:ММ\n"
            "Пример: 14:30\n\n"
            "Попробуйте еще раз:",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    # Проверяем, что время не в прошлом
    now_moscow = datetime.now(MOSCOW_TZ)
    if scheduled_time < now_moscow:
        await message.answer(
            "❌ Время не может быть в прошлом!\n\n"
            f"Сейчас: {now_moscow.strftime('%H:%M')}\n"
            "Укажите будущее время.",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    await state.update_data(time_str=time_str, scheduled_time=scheduled_time)
    
    # Показываем превью
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
        "📋 Превью поста\n\n"
        f"Канал: {channel_name}\n"
        f"Время публикации: {format_datetime(scheduled_time)}\n\n"
    )
    
    if message_type == 'text':
        text_preview = message_text[:200]
        if len(message_text) > 200:
            text_preview += "..."
        preview_text += f"Текст:\n{text_preview}"
    
    elif message_type in ['photo', 'video', 'document']:
        media_type = {
            'photo': 'Фото',
            'video': 'Видео',
            'document': 'Документ'
        }.get(message_type, 'Медиа')
        
        preview_text += f"{media_type}"
        if media_caption:
            caption_preview = media_caption[:200]
            if len(media_caption) > 200:
                caption_preview += "..."
            preview_text += f" с подписью:\n{caption_preview}"
        else:
            preview_text += " без подписи"
    
    preview_text += "\n\n✅ Все верно?"
    
    # Отправляем превью
    await message.answer(
        preview_text,
        reply_markup=get_confirmation_keyboard()
    )

@router.callback_query(F.data == "confirm_yes")
async def confirm_post(callback: CallbackQuery, state: FSMContext):
    """Подтверждение поста"""
    data = await state.get_data()
    user_id = callback.from_user.id
    
    # Проверяем лимиты
    stats = await get_user_stats(user_id)
    if stats['active_posts'] >= MAX_POSTS_PER_USER:
        await callback.message.edit_text(
            f"❌ Достигнут лимит постов!\n\n"
            f"У вас уже {stats['active_posts']} активных постов.\n"
            f"Максимум: {MAX_POSTS_PER_USER} постов одновременно.\n\n"
            "Дождитесь публикации некоторых постов.",
            reply_markup=get_main_menu(user_id, user_id == ADMIN_ID)
        )
        await state.clear()
        return
    
    # Сохраняем пост
    post_id = await save_scheduled_post(
        user_id,
        data['channel_id'],
        data,
        data['scheduled_time']
    )
    
    if not post_id:
        await callback.message.edit_text(
            "❌ Ошибка при сохранении поста!\n\n"
            "Попробуйте еще раз или обратитесь в поддержку.",
            reply_markup=get_main_menu(user_id, user_id == ADMIN_ID)
        )
        await state.clear()
        return
    
    # Планируем отправку
    scheduler.add_job(
        send_scheduled_post,
        trigger=DateTrigger(run_date=data['scheduled_time']),
        args=(data['channel_id'], data, post_id),
        id=f"post_{post_id}",
        replace_existing=True
    )
    
    # Отправляем подтверждение
    await callback.message.edit_text(
        f"✅ Пост успешно запланирован!\n\n"
        f"Канал: {data['channel_name']}\n"
        f"Время: {format_datetime(data['scheduled_time'])}\n"
        f"ID поста: {post_id}\n\n"
        f"📍 Пост будет автоматически опубликован в указанное время.\n\n"
        "👇 Что дальше?",
        reply_markup=get_main_menu(user_id, user_id == ADMIN_ID)
    )
    
    logger.info(f"Пост {post_id} запланирован пользователем {user_id}")
    await state.clear()

@router.callback_query(F.data == "confirm_no")
async def reject_post(callback: CallbackQuery, state: FSMContext):
    """Отказ от поста"""
    await state.clear()
    is_admin = callback.from_user.id == ADMIN_ID
    
    await callback.message.edit_text(
        "❌ Планирование отменено\n\n"
        "Вы можете начать заново.",
        reply_markup=get_main_menu(callback.from_user.id, is_admin)
    )

async def send_scheduled_post(channel_id: int, post_data: Dict, post_id: int):
    """Отправка запланированного поста"""
    try:
        message_type = post_data.get('message_type')
        
        if message_type == 'text':
            await bot.send_message(
                chat_id=channel_id,
                text=post_data.get('message_text')
            )
            
        elif message_type == 'photo':
            await bot.send_photo(
                chat_id=channel_id,
                photo=post_data.get('media_file_id'),
                caption=post_data.get('media_caption')
            )
            
        elif message_type == 'video':
            await bot.send_video(
                chat_id=channel_id,
                video=post_data.get('media_file_id'),
                caption=post_data.get('media_caption')
            )
            
        elif message_type == 'document':
            await bot.send_document(
                chat_id=channel_id,
                document=post_data.get('media_file_id'),
                caption=post_data.get('media_caption')
            )
        
        # Обновляем статус в БД
        conn = await get_db_connection()
        await conn.execute('''
            UPDATE scheduled_posts 
            SET is_sent = TRUE 
            WHERE id = $1
        ''', post_id)
        await conn.close()
        
        logger.info(f"✅ Пост {post_id} отправлен в канал {channel_id}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки поста {post_id}: {e}")

# ========== USER STATISTICS ==========
@router.callback_query(F.data == "my_stats")
async def show_my_stats(callback: CallbackQuery):
    """Показ статистики пользователя"""
    user_id = callback.from_user.id
    stats = await get_user_stats(user_id)
    
    stats_text = (
        f"📊 Ваша статистика\n\n"
        f"Пользователь: {callback.from_user.first_name}\n"
        f"Всего постов: {stats['total_posts']}\n"
        f"Опубликовано: {stats['sent_posts']}\n"
        f"Ожидает публикации: {stats['active_posts']}\n"
        f"Каналов: {stats['channels']}\n\n"
        f"Обновлено: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}"
    )
    
    is_admin = user_id == ADMIN_ID
    await callback.message.edit_text(
        stats_text,
        reply_markup=get_main_menu(user_id, is_admin)
    )

@router.callback_query(F.data == "my_channels")
async def show_my_channels(callback: CallbackQuery):
    """Показ каналов пользователя"""
    user_id = callback.from_user.id
    channels = await get_user_channels(user_id)
    
    if not channels:
        await callback.message.edit_text(
            "📢 У вас нет добавленных каналов\n\n"
            "Добавьте канал, чтобы начать планировать посты.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить канал", callback_data="add_channel")],
                [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
            ])
        )
        return
    
    channels_text = "📢 Ваши каналы:\n\n"
    for i, channel in enumerate(channels, 1):
        channels_text += f"{i}. {channel['channel_name']}\n"
    
    channels_text += f"\nВсего: {len(channels)} каналов"
    
    is_admin = user_id == ADMIN_ID
    await callback.message.edit_text(
        channels_text,
        reply_markup=get_main_menu(user_id, is_admin)
    )

# ========== ADMIN FUNCTIONS ==========
@router.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    """Админ-панель - ТОЛЬКО ДЛЯ АДМИНА"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Только для администратора!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "👑 Админ-панель\n\n"
        "👇 Выберите действие:",
        reply_markup=get_admin_keyboard()
    )

@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    """Общая статистика для админа"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Только для администратора!", show_alert=True)
        return
    
    stats = await get_total_stats()
    
    if not stats:
        await callback.message.edit_text(
            "❌ Ошибка получения статистики",
            reply_markup=get_admin_keyboard()
        )
        return
    
    stats_text = (
        f"📊 Общая статистика бота\n\n"
        f"👥 Пользователи:\n"
        f"   • Всего: {stats['total_users']}\n"
        f"   • Активных: {stats['active_users']}\n\n"
        f"📅 Посты:\n"
        f"   • Всего: {stats['total_posts']}\n"
        f"   • Ожидает: {stats['active_posts']}\n"
        f"   • Опубликовано: {stats['sent_posts']}\n\n"
        f"📢 Каналы: {stats['total_channels']}\n\n"
        f"Обновлено: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}"
    )
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=get_admin_keyboard()
    )

@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: CallbackQuery):
    """Начало рассылки - ТОЛЬКО ДЛЯ АДМИНА"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Только для администратора!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📢 Рассылка сообщений\n\n"
        "Используйте команду:\n"
        "/broadcast Ваше сообщение здесь\n\n"
        "Чтобы отправить рассылку всем пользователям.",
        reply_markup=get_admin_keyboard()
    )

@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    """Команда рассылки - ТОЛЬКО ДЛЯ АДМИНА"""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ У вас нет прав для этой команды.")
        return
    
    if len(message.text) < 12:  # /broadcast + пробел
        await message.answer(
            "❌ Не указано сообщение!\n"
            "Используйте: /broadcast Ваше сообщение"
        )
        return
    
    broadcast_text = message.text[11:]  # Убираем "/broadcast "
    
    try:
        conn = await get_db_connection()
        users = await conn.fetch("SELECT id FROM users WHERE is_active = TRUE")
        await conn.close()
    except Exception as e:
        logger.error(f"Ошибка получения пользователей: {e}")
        await message.answer(f"❌ Ошибка: {e}")
        return
    
    total = len(users)
    success = 0
    failed = 0
    
    status_msg = await message.answer(f"📢 Начинаю рассылку для {total} пользователей...")
    
    for i, user in enumerate(users):
        try:
            await bot.send_message(
                user['id'],
                f"📢 Сообщение от администратора\n\n{broadcast_text}"
            )
            success += 1
            
            # Обновляем статус каждые 10 сообщений
            if (i + 1) % 10 == 0:
                await status_msg.edit_text(f"📢 Рассылка: {i + 1}/{total} отправлено...")
            
            await asyncio.sleep(0.1)  # Задержка против ограничений Telegram
            
        except Exception as e:
            failed += 1
    
    await status_msg.edit_text(
        f"✅ Рассылка завершена!\n\n"
        f"📊 Итоги:\n"
        f"• Всего получателей: {total}\n"
        f"• Успешно отправлено: {success}\n"
        f"• Не удалось отправить: {failed}"
    )

# ========== RESTORE JOBS ==========
async def restore_scheduled_jobs():
    """Восстановление запланированных постов при перезапуске"""
    try:
        conn = await get_db_connection()
        posts = await conn.fetch('''
            SELECT id, channel_id, message_type, message_text, 
                   media_file_id, media_caption, scheduled_time
            FROM scheduled_posts
            WHERE is_sent = FALSE AND scheduled_time > NOW()
            ORDER BY scheduled_time
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
                    f"📍 Готов к работе!"
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
    
    if scheduler.running:
        scheduler.shutdown()
        logger.info("✅ Планировщик остановлен")
    
    logger.info("👋 Бот выключен")

# ========== MAIN ==========
async def main():
    """Основная функция"""
    logger.info("=" * 50)
    logger.info(f"🤖 Запуск бота...")
    logger.info(f"👑 Admin ID: {ADMIN_ID}")
    logger.info(f"🌐 Database: {'Настроена' if DATABASE_URL else 'Нет'}")
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
