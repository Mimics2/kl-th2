import os
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from enum import Enum

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
if not API_TOKEN:
    print("❌ ОШИБКА: Не указан BOT_TOKEN в переменных окружения")
    sys.exit(1)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ ОШИБКА: Не указан DATABASE_URL в переменных окружения")
    sys.exit(1)

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
SUPPORT_BOT_USERNAME = os.getenv("SUPPORT_BOT_USERNAME", "support_bot")
ADMIN_CONTACT = os.getenv("ADMIN_CONTACT", "@admin")

MOSCOW_TZ = pytz.timezone('Europe/Moscow')
POST_CHARACTER_LIMIT = 4000

# ========== TARIFF SYSTEM ==========
class Tariff(Enum):
    MINI = "mini"
    STANDARD = "standard"
    VIP = "vip"
    ADMIN = "admin"

TARIFFS = {
    Tariff.MINI.value: {
        "name": "🚀 Mini",
        "price": 0,
        "currency": "USD",
        "channels_limit": 1,
        "daily_posts_limit": 2,
        "description": "Бесплатный тариф для начала работы"
    },
    Tariff.STANDARD.value: {
        "name": "⭐ Standard",
        "price": 4,
        "currency": "USD",
        "channels_limit": 2,
        "daily_posts_limit": 6,
        "description": "Для активных пользователей"
    },
    Tariff.VIP.value: {
        "name": "👑 VIP",
        "price": 7,
        "currency": "USD",
        "channels_limit": 3,
        "daily_posts_limit": 12,
        "description": "Максимальные возможности"
    },
    Tariff.ADMIN.value: {
        "name": "⚡ Admin",
        "price": 0,
        "currency": "USD",
        "channels_limit": 999,
        "daily_posts_limit": 999,
        "description": "Безлимитный доступ"
    }
}

# ========== SETUP ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
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
        if DATABASE_URL.startswith("postgres://"):
            conn_string = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        else:
            conn_string = DATABASE_URL
        
        if "sslmode" not in conn_string:
            if "?" in conn_string:
                conn_string += "&sslmode=require"
            else:
                conn_string += "?sslmode=require"
        
        return await asyncpg.connect(conn_string, timeout=30)
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
                tariff TEXT DEFAULT 'mini',
                posts_today INTEGER DEFAULT 0,
                posts_reset_date DATE DEFAULT CURRENT_DATE,
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
                scheduled_time TIMESTAMPTZ NOT NULL,
                is_sent BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # Таблица заказов тарифов
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS tariff_orders (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                tariff TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                order_date TIMESTAMP DEFAULT NOW(),
                processed_date TIMESTAMP,
                admin_notes TEXT
            )
        ''')
        
        await conn.close()
        logger.info("✅ Таблицы БД созданы/проверены")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        raise

async def migrate_db():
    """Миграция существующей базы данных"""
    try:
        conn = await get_db_connection()
        
        migrations = [
            ('users', 'tariff', 'TEXT DEFAULT \'mini\''),
            ('users', 'is_admin', 'BOOLEAN DEFAULT FALSE'),
            ('users', 'posts_today', 'INTEGER DEFAULT 0'),
            ('users', 'posts_reset_date', 'DATE DEFAULT CURRENT_DATE'),
        ]
        
        for table, column, definition in migrations:
            try:
                exists = await conn.fetchval(f'''
                    SELECT EXISTS (
                        SELECT FROM information_schema.columns 
                        WHERE table_name = $1 AND column_name = $2
                    )
                ''', table, column)
                
                if not exists:
                    await conn.execute(f'ALTER TABLE {table} ADD COLUMN {column} {definition}')
            except Exception as e:
                logger.error(f"Ошибка при добавлении колонки {column}: {e}")
        
        # Обновляем админа
        if ADMIN_ID > 0:
            await conn.execute('''
                UPDATE users 
                SET is_admin = TRUE, tariff = 'admin' 
                WHERE id = $1
            ''', ADMIN_ID)
        
        await conn.close()
        logger.info("✅ Миграции БД завершены")
    except Exception as e:
        logger.error(f"❌ Ошибка миграции БД: {e}")

async def get_user_tariff(user_id: int) -> str:
    """Получает тариф пользователя"""
    try:
        conn = await get_db_connection()
        user = await conn.fetchrow(
            "SELECT tariff, is_admin FROM users WHERE id = $1", 
            user_id
        )
        await conn.close()
        
        if not user:
            conn = await get_db_connection()
            await conn.execute('''
                INSERT INTO users (id, tariff) VALUES ($1, 'mini')
            ''', user_id)
            await conn.close()
            return 'mini'
        
        if user.get('is_admin'):
            return 'admin'
            
        return user.get('tariff', 'mini')
    except Exception as e:
        logger.error(f"Ошибка получения тарифа: {e}")
        return 'mini'

async def update_user_tariff(user_id: int, tariff: str) -> bool:
    """Обновляет тариф пользователя"""
    try:
        conn = await get_db_connection()
        await conn.execute('''
            UPDATE users SET tariff = $1 WHERE id = $2
        ''', tariff, user_id)
        await conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления тарифа: {e}")
        return False

async def create_tariff_order(user_id: int, tariff_id: str) -> bool:
    """Создает заказ на тариф"""
    try:
        conn = await get_db_connection()
        await conn.execute('''
            INSERT INTO tariff_orders (user_id, tariff, status)
            VALUES ($1, $2, 'pending')
        ''', user_id, tariff_id)
        await conn.close()
        
        # Уведомляем админа
        if ADMIN_ID:
            tariff_info = TARIFFS.get(tariff_id, {})
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"🛒 НОВЫЙ ЗАКАЗ ТАРИФА!\n\n"
                    f"👤 Пользователь: {user_id}\n"
                    f"💎 Тариф: {tariff_info.get('name', tariff_id)}\n"
                    f"💰 Стоимость: {tariff_info.get('price', 0)} {tariff_info.get('currency', 'USD')}\n"
                    f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}"
                )
            except Exception:
                pass
        
        return True
    except Exception as e:
        logger.error(f"Ошибка создания заказа тарифа: {e}")
        return False

async def get_tariff_limits(user_id: int) -> Tuple[int, int]:
    """Получает лимиты пользователя"""
    tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    return tariff_info['channels_limit'], tariff_info['daily_posts_limit']

async def get_user_channels_count(user_id: int) -> int:
    """Получает количество каналов пользователя"""
    try:
        conn = await get_db_connection()
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM channels WHERE user_id = $1 AND is_active = TRUE",
            user_id
        )
        await conn.close()
        return count or 0
    except Exception as e:
        logger.error(f"Ошибка получения количества каналов: {e}")
        return 0

async def reset_daily_posts():
    """Сбрасывает счетчик постов за день"""
    try:
        conn = await get_db_connection()
        await conn.execute('''
            UPDATE users 
            SET posts_today = 0, posts_reset_date = CURRENT_DATE 
            WHERE posts_reset_date < CURRENT_DATE
        ''')
        await conn.close()
    except Exception as e:
        logger.error(f"Ошибка сброса счетчиков: {e}")

async def increment_user_posts(user_id: int) -> bool:
    """Увеличивает счетчик постов пользователя"""
    try:
        conn = await get_db_connection()
        
        user = await conn.fetchrow(
            "SELECT posts_reset_date FROM users WHERE id = $1",
            user_id
        )
        
        if user and user['posts_reset_date'] < datetime.now(MOSCOW_TZ).date():
            await conn.execute('''
                UPDATE users 
                SET posts_today = 1, posts_reset_date = CURRENT_DATE 
                WHERE id = $1
            ''', user_id)
        else:
            await conn.execute('''
                UPDATE users 
                SET posts_today = posts_today + 1 
                WHERE id = $1
            ''', user_id)
        
        await conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка увеличения счетчика постов: {e}")
        return False

async def get_user_posts_today(user_id: int) -> int:
    """Получает количество постов пользователя за сегодня"""
    try:
        conn = await get_db_connection()
        user = await conn.fetchrow(
            "SELECT posts_today, posts_reset_date FROM users WHERE id = $1",
            user_id
        )
        await conn.close()
        
        if not user:
            return 0
            
        if user['posts_reset_date'] < datetime.now(MOSCOW_TZ).date():
            return 0
            
        return user['posts_today'] or 0
    except Exception as e:
        logger.error(f"Ошибка получения счетчика постов: {e}")
        return 0

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
        # Конвертируем время в UTC
        if scheduled_time.tzinfo is None:
            scheduled_time = MOSCOW_TZ.localize(scheduled_time)
        scheduled_time_utc = scheduled_time.astimezone(pytz.UTC)
        
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
        scheduled_time_utc  # Передаем datetime объект, а не строку!
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
        combined = datetime.combine(date_obj.date(), time_obj.time())
        return MOSCOW_TZ.localize(combined)
    except Exception:
        return None

# ========== KEYBOARDS ==========
def get_main_menu(user_id: int, is_admin: bool = False) -> InlineKeyboardMarkup:
    """Главное меню"""
    buttons = [
        [InlineKeyboardButton(text="📅 Запланировать пост", callback_data="schedule_post")],
        [InlineKeyboardButton(text="📊 Моя статистика", callback_data="my_stats")],
        [InlineKeyboardButton(text="📢 Мои каналы", callback_data="my_channels")],
        [InlineKeyboardButton(text="💎 Тарифы", callback_data="tariffs")],
        [InlineKeyboardButton(text="🆘 Техподдержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME}")],
    ]
    
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

def get_tariffs_keyboard(user_tariff: str = 'mini') -> InlineKeyboardMarkup:
    """Клавиатура с тарифами"""
    buttons = []
    
    for tariff_id, tariff_info in TARIFFS.items():
        if tariff_id == 'admin':
            continue
            
        name = tariff_info['name']
        price = tariff_info['price']
        
        if tariff_id == user_tariff:
            button_text = f"✅ {name} (текущий)"
        else:
            if price == 0:
                button_text = f"{name} - Бесплатно"
            else:
                button_text = f"{name} - {price} USD/месяц"
        
        buttons.append([InlineKeyboardButton(
            text=button_text,
            callback_data=f"tariff_info_{tariff_id}"
        )])
    
    buttons.append([InlineKeyboardButton(text="⏰ Проверить время", callback_data="check_time")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_tariff_order_keyboard(tariff_id: str) -> InlineKeyboardMarkup:
    """Клавиатура для заказа тарифа"""
    tariff_info = TARIFFS.get(tariff_id)
    
    if tariff_info and tariff_info['price'] == 0:
        buttons = [
            [InlineKeyboardButton(text="🆓 Активировать бесплатный тариф", callback_data=f"activate_{tariff_id}")],
            [InlineKeyboardButton(text="⬅️ Назад к тарифам", callback_data="tariffs")]
        ]
    else:
        buttons = [
            [InlineKeyboardButton(text="💳 Заказать тариф", callback_data=f"order_{tariff_id}")],
            [InlineKeyboardButton(text="💬 Связаться с менеджером", url=f"https://t.me/{ADMIN_CONTACT.replace('@', '')}")],
            [InlineKeyboardButton(text="⬅️ Назад к тарифам", callback_data="tariffs")]
        ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура админ-панели"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Общая статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="👥 Управление пользователями", callback_data="admin_users")],
        [InlineKeyboardButton(text="📢 Сделать рассылку", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="🛒 Управление заказами", callback_data="admin_orders")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ])

# ========== STATES ==========
class PostStates(StatesGroup):
    waiting_for_channel = State()
    waiting_for_content = State()
    waiting_for_date = State()
    waiting_for_time = State()
    waiting_for_confirmation = State()

class AdminStates(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_tariff = State()
    waiting_for_broadcast = State()
    waiting_for_order_action = State()

# ========== BASIC HANDLERS ==========
@router.message(CommandStart())
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    user_id = message.from_user.id
    username = message.from_user.username or ""
    first_name = message.from_user.first_name or "Пользователь"
    is_admin = user_id == ADMIN_ID
    
    # Регистрируем пользователя
    try:
        conn = await get_db_connection()
        await conn.execute('''
            INSERT INTO users (id, username, first_name, is_admin, tariff)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id) DO UPDATE 
            SET username = EXCLUDED.username, first_name = EXCLUDED.first_name,
                is_admin = EXCLUDED.is_admin
        ''', user_id, username, first_name, is_admin, 'mini' if not is_admin else 'admin')
        await conn.close()
    except Exception as e:
        logger.error(f"Ошибка регистрации пользователя {user_id}: {e}")
    
    current_tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(current_tariff, TARIFFS['mini'])
    
    welcome_text = (
        f"👋 Привет, {first_name}!\n\n"
        f"🤖 Я — бот для планирования постов в Telegram каналах.\n\n"
        f"💎 Ваш текущий тариф: {tariff_info['name']}\n\n"
        f"✨ Возможности:\n"
        f"• 📅 Запланировать пост с текстом, фото, видео или документом\n"
        f"• 📊 Показать вашу статистику\n"
        f"• 📢 Управлять вашими каналами\n"
        f"• ⏰ Автоматически публиковать в нужное время\n\n"
        f"📍 Время указывается по Москве\n\n"
        f"👇 Выберите действие:"
    )
    
    await message.answer(welcome_text, reply_markup=get_main_menu(user_id, is_admin))

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
        
        "💎 Тарифы:\n"
        "• Mini (бесплатно) - 1 канал, 2 поста в день\n"
        "• Standard ($4/месяц) - 2 канала, 6 постов в день\n"
        "• VIP ($7/месяц) - 3 канала, 12 постов в день\n\n"
        
        f"🆘 Поддержка: @{SUPPORT_BOT_USERNAME}\n"
        f"💬 Вопросы по оплате: @{ADMIN_CONTACT.replace('@', '')}"
    )
    
    await message.answer(help_text)

# ========== NAVIGATION HANDLERS ==========
@router.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await state.clear()
    user_id = callback.from_user.id
    is_admin = user_id == ADMIN_ID
    
    await callback.message.edit_text(
        "🤖 Главное меню\n\n👇 Выберите действие:",
        reply_markup=get_main_menu(user_id, is_admin)
    )

@router.callback_query(F.data == "cancel")
async def cancel_action(callback: CallbackQuery, state: FSMContext):
    """Отмена текущего действия"""
    await state.clear()
    user_id = callback.from_user.id
    is_admin = user_id == ADMIN_ID
    
    await callback.message.edit_text(
        "❌ Действие отменено.\n\n👇 Выберите действие:",
        reply_markup=get_main_menu(user_id, is_admin)
    )

@router.callback_query(F.data == "check_time")
async def check_time(callback: CallbackQuery):
    """Проверка текущего времени"""
    now_moscow = datetime.now(MOSCOW_TZ)
    time_text = (
        f"🕐 Текущее время по Москве:\n\n"
        f"📅 Дата: {now_moscow.strftime('%d.%m.%Y')}\n"
        f"⏰ Время: {now_moscow.strftime('%H:%M:%S')}\n\n"
        f"📍 Используйте это время для планирования постов."
    )
    
    await callback.message.edit_text(
        time_text,
        reply_markup=get_tariffs_keyboard()
    )

# ========== STATISTICS HANDLERS ==========
@router.callback_query(F.data == "my_stats")
async def show_my_stats(callback: CallbackQuery):
    """Показать статистику пользователя"""
    user_id = callback.from_user.id
    stats = await get_user_stats(user_id)
    current_tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(current_tariff, TARIFFS['mini'])
    posts_today = await get_user_posts_today(user_id)
    
    stats_text = (
        f"📊 Ваша статистика:\n\n"
        f"💎 Текущий тариф: {tariff_info['name']}\n"
        f"📈 Всего запланировано постов: {stats['total_posts']}\n"
        f"⏳ Активных постов: {stats['active_posts']}\n"
        f"✅ Отправлено постов: {stats['sent_posts']}\n"
        f"📢 Подключено каналов: {stats['channels']}/{tariff_info['channels_limit']}\n"
        f"📅 Постов сегодня: {posts_today}/{tariff_info['daily_posts_limit']}\n\n"
        f"📍 Время по Москве: {datetime.now(MOSCOW_TZ).strftime('%H:%M')}"
    )
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=get_main_menu(user_id, user_id == ADMIN_ID)
    )

# ========== CHANNELS HANDLERS ==========
@router.callback_query(F.data == "my_channels")
async def show_my_channels(callback: CallbackQuery):
    """Показать каналы пользователя"""
    user_id = callback.from_user.id
    channels = await get_user_channels(user_id)
    
    if not channels:
        await callback.message.edit_text(
            "📢 У вас еще нет подключенных каналов.\n\n"
            "👇 Нажмите кнопку ниже, чтобы добавить канал:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить канал", callback_data="add_channel")],
                [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
            ])
        )
        return
    
    channels_text = "📢 Ваши каналы:\n\n"
    for i, channel in enumerate(channels, 1):
        channels_text += f"{i}. {channel['channel_name']}\n"
    
    channels_text += f"\n📊 Всего: {len(channels)} каналов"
    
    await callback.message.edit_text(
        channels_text,
        reply_markup=get_channels_keyboard(channels)
    )

@router.callback_query(F.data == "add_channel")
async def add_channel_start(callback: CallbackQuery, state: FSMContext):
    """Начало добавления канала"""
    user_id = callback.from_user.id
    
    # Проверяем лимит каналов
    channels_count = await get_user_channels_count(user_id)
    channels_limit, _ = await get_tariff_limits(user_id)
    
    if channels_count >= channels_limit:
        await callback.message.edit_text(
            f"❌ Достигнут лимит каналов!\n\n"
            f"У вас подключено: {channels_count} каналов\n"
            f"Ваш лимит: {channels_limit} каналов\n\n"
            "💎 Чтобы увеличить лимит, выберите другой тариф.",
            reply_markup=get_main_menu(user_id, user_id == ADMIN_ID)
        )
        return
    
    await state.set_state(PostStates.waiting_for_channel)
    await callback.message.edit_text(
        "📢 Добавление канала\n\n"
        "Чтобы я мог публиковать посты в вашем канале:\n\n"
        "1. Добавьте меня в канал как администратора\n"
        "2. Дайте права на отправку сообщений\n"
        "3. Пришлите мне ID канала в формате -1001234567890\n"
        "4. Или просто перешлите любое сообщение из канала\n\n"
        "👇 Отправьте ID или перешлите сообщение:",
        reply_markup=get_cancel_keyboard()
    )

@router.message(PostStates.waiting_for_channel)
async def process_channel_input(message: Message, state: FSMContext):
    """Обработка ввода канала"""
    user_id = message.from_user.id
    
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
    success = await add_user_channel(user_id, channel_id, channel_name)
    
    if not success:
        await message.answer(
            "❌ Ошибка при добавлении канала. Попробуйте позже.",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    await state.clear()
    await message.answer(
        f"✅ Канал успешно добавлен: {channel_name}\n\n"
        "Теперь вы можете запланировать пост в этом канале.",
        reply_markup=get_main_menu(user_id, user_id == ADMIN_ID)
    )

# ========== TARIFFS HANDLERS ==========
@router.callback_query(F.data == "tariffs")
async def show_tariffs(callback: CallbackQuery):
    """Показ тарифов"""
    user_id = callback.from_user.id
    current_tariff = await get_user_tariff(user_id)
    
    tariffs_text = (
        "💎 Доступные тарифы:\n\n"
        "🚀 Mini (Бесплатно):\n"
        "• 1 канал\n"
        "• 2 поста в день\n"
        "• Базовые функции\n\n"
        "⭐ Standard ($4/месяц):\n"
        "• 2 канала\n"
        "• 6 постов в день\n"
        "• Все функции Mini\n\n"
        "👑 VIP ($7/месяц):\n"
        "• 3 канала\n"
        "• 12 постов в день\n"
        "• Приоритетная поддержка\n"
        "• Все функции Standard\n\n"
        f"💎 Ваш текущий тариф: {TARIFFS.get(current_tariff, TARIFFS['mini'])['name']}\n\n"
        "👇 Выберите тариф для подробной информации:"
    )
    
    await callback.message.edit_text(
        tariffs_text,
        reply_markup=get_tariffs_keyboard(current_tariff)
    )

@router.callback_query(F.data.startswith("tariff_info_"))
async def tariff_info(callback: CallbackQuery):
    """Информация о тарифе"""
    tariff_id = callback.data.split("_")[2]
    tariff_info = TARIFFS.get(tariff_id)
    
    if not tariff_info:
        await callback.answer("Тариф не найден!", show_alert=True)
        return
    
    user_id = callback.from_user.id
    current_tariff = await get_user_tariff(user_id)
    
    info_text = (
        f"{tariff_info['name']}\n\n"
        f"📊 Лимиты:\n"
        f"• Каналов: {tariff_info['channels_limit']}\n"
        f"• Постов в день: {tariff_info['daily_posts_limit']}\n\n"
        f"💵 Стоимость: "
    )
    
    if tariff_info['price'] == 0:
        info_text += "Бесплатно\n\n"
    else:
        info_text += f"{tariff_info['price']} {tariff_info['currency']} в месяц\n\n"
    
    info_text += f"📝 {tariff_info['description']}\n\n"
    
    if tariff_id == 'mini':
        info_text += "🆓 Это бесплатный тариф, вы можете активировать его сразу"
    elif tariff_id == current_tariff:
        info_text += "✅ Это ваш текущий тариф"
    else:
        info_text += (
            f"💳 Для заказа тарифа нажмите кнопку ниже\n\n"
            f"📋 Ваш ID для заказа: {user_id}"
        )
    
    await callback.message.edit_text(
        info_text,
        reply_markup=get_tariff_order_keyboard(tariff_id)
    )

@router.callback_query(F.data.startswith("activate_"))
async def activate_free_tariff(callback: CallbackQuery):
    """Активация бесплатного тарифа"""
    user_id = callback.from_user.id
    tariff_id = callback.data.split("_")[1]
    
    if tariff_id != 'mini':
        await callback.answer("❌ Этот тариф не бесплатный!", show_alert=True)
        return
    
    success = await update_user_tariff(user_id, tariff_id)
    
    if success:
        await callback.message.edit_text(
            "🎉 Бесплатный тариф Mini успешно активирован!\n\n"
            "Теперь вы можете:\n"
            "• Добавить 1 канал\n"
            "• Публиковать до 2 постов в день",
            reply_markup=get_main_menu(user_id, user_id == ADMIN_ID)
        )
    else:
        await callback.answer("❌ Ошибка при активации тарифа", show_alert=True)

@router.callback_query(F.data.startswith("order_"))
async def order_tariff(callback: CallbackQuery):
    """Заказ платного тарифа"""
    user_id = callback.from_user.id
    tariff_id = callback.data.split("_")[1]
    tariff_info = TARIFFS.get(tariff_id)
    
    if not tariff_info or tariff_info['price'] == 0:
        await callback.answer("❌ Неверный тариф!", show_alert=True)
        return
    
    success = await create_tariff_order(user_id, tariff_id)
    
    if success:
        order_text = (
            f"🛒 Заказ тарифа {tariff_info['name']} создан!\n\n"
            f"💰 Стоимость: {tariff_info['price']} {tariff_info['currency']}\n"
            f"⏱ Срок действия: 30 дней\n\n"
            f"📋 Для завершения заказа:\n"
            f"1. Напишите менеджеру: @{ADMIN_CONTACT.replace('@', '')}\n"
            f"2. Укажите ваш Telegram ID: {user_id}\n"
            f"3. Оплатите через CryptoBot (чек)\n"
            f"4. Пришлите скриншот оплаты\n\n"
            f"⏳ Тариф будет активирован в течение 24 часов после оплаты."
        )
        
        await callback.message.edit_text(
            order_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💬 Написать менеджеру", url=f"https://t.me/{ADMIN_CONTACT.replace('@', '')}")],
                [InlineKeyboardButton(text="⬅️ Назад к тарифам", callback_data="tariffs")]
            ])
        )
    else:
        await callback.answer("❌ Ошибка при создании заказа", show_alert=True)

# ========== POST SCHEDULING HANDLERS ==========
@router.callback_query(F.data == "schedule_post")
async def start_scheduling(callback: CallbackQuery, state: FSMContext):
    """Начало планирования поста"""
    user_id = callback.from_user.id
    
    # Проверяем лимит постов за сегодня
    posts_today = await get_user_posts_today(user_id)
    _, daily_limit = await get_tariff_limits(user_id)
    
    if posts_today >= daily_limit:
        await callback.message.edit_text(
            f"❌ Достигнут дневной лимит постов!\n\n"
            f"Сегодня вы запланировали: {posts_today} постов\n"
            f"Ваш лимит: {daily_limit} постов в день\n\n"
            "💎 Чтобы увеличить лимит, выберите другой тариф.",
            reply_markup=get_main_menu(user_id, user_id == ADMIN_ID)
        )
        return
    
    channels = await get_user_channels(user_id)
    
    if not channels:
        await callback.message.edit_text(
            "📢 Сначала нужно добавить канал!\n\n"
            "Чтобы запланировать пост, добавьте меня в канал как администратора "
            "и перешлите любое сообщение из канала.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить канал", callback_data="add_channel")],
                [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
            ])
        )
        return
    
    await state.set_state(PostStates.waiting_for_channel)
    await callback.message.edit_text(
        "📢 Выберите канал для поста:\n\n👇 Выберите из списка:",
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
        "• Документ с подписью",
        reply_markup=get_cancel_keyboard()
    )

@router.message(PostStates.waiting_for_content)
async def process_content(message: Message, state: FSMContext):
    """Обработка контента поста"""
    post_data = {}
    
    if message.text:
        if len(message.text) > POST_CHARACTER_LIMIT:
            await message.answer(
                f"❌ Слишком длинный текст!\n"
                f"Максимум {POST_CHARACTER_LIMIT} символов.",
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
    now_moscow = datetime.now(MOSCOW_TZ)
    date_obj = parse_datetime(date_str, "00:00")
    
    if not date_obj:
        await message.answer(
            "❌ Неверный формат даты!\n\n"
            "Используйте: ДД.ММ.ГГГГ\n"
            f"Пример: {now_moscow.strftime('%d.%m.%Y')}",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    if date_obj.date() < now_moscow.date():
        await message.answer(
            "❌ Дата не может быть в прошлом!\n\n"
            f"Сегодня: {now_moscow.strftime('%d.%m.%Y')}",
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
    
    scheduled_time = parse_datetime(date_str, time_str)
    
    if not scheduled_time:
        await message.answer(
            "❌ Неверный формат времени!\n\n"
            "Используйте: ЧЧ:ММ\n"
            "Пример: 14:30",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    now_moscow = datetime.now(MOSCOW_TZ)
    if scheduled_time < now_moscow:
        await message.answer(
            "❌ Время не может быть в прошлом!\n\n"
            f"Сейчас: {now_moscow.strftime('%H:%M')}",
            reply_markup=get_cancel_keyboard()
        )
        return
    
    await state.update_data(time_str=time_str, scheduled_time=scheduled_time)
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
        text_preview = message_text[:200] + ("..." if len(message_text) > 200 else "")
        preview_text += f"Текст:\n{text_preview}"
    elif message_type in ['photo', 'video', 'document']:
        media_type = {
            'photo': '📷 Фото',
            'video': '🎥 Видео',
            'document': '📎 Документ'
        }.get(message_type, '📁 Медиа')
        
        preview_text += f"{media_type}"
        if media_caption:
            caption_preview = media_caption[:200] + ("..." if len(media_caption) > 200 else "")
            preview_text += f" с подписью:\n{caption_preview}"
        else:
            preview_text += " без подписи"
    
    preview_text += "\n\n✅ Все верно?"
    
    await message.answer(preview_text, reply_markup=get_confirmation_keyboard())

@router.callback_query(F.data == "confirm_yes")
async def confirm_post(callback: CallbackQuery, state: FSMContext):
    """Подтверждение поста"""
    data = await state.get_data()
    user_id = callback.from_user.id
    
    await increment_user_posts(user_id)
    
    post_id = await save_scheduled_post(
        user_id,
        data['channel_id'],
        data,
        data['scheduled_time']
    )
    
    if not post_id:
        await callback.message.edit_text(
            "❌ Ошибка при сохранении поста!",
            reply_markup=get_main_menu(user_id, user_id == ADMIN_ID)
        )
        await state.clear()
        return
    
    # Планируем отправку
    scheduled_time_utc = data['scheduled_time'].astimezone(pytz.UTC)
    scheduler.add_job(
        send_scheduled_post,
        trigger=DateTrigger(run_date=scheduled_time_utc),
        args=(data['channel_id'], data, post_id),
        id=f"post_{post_id}"
    )
    
    posts_today = await get_user_posts_today(user_id)
    _, daily_limit = await get_tariff_limits(user_id)
    
    await callback.message.edit_text(
        f"✅ Пост успешно запланирован!\n\n"
        f"📢 Канал: {data['channel_name']}\n"
        f"⏰ Время: {format_datetime(data['scheduled_time'])}\n"
        f"📝 ID поста: {post_id}\n\n"
        f"📊 Сегодня: {posts_today}/{daily_limit} постов",
        reply_markup=get_main_menu(user_id, user_id == ADMIN_ID)
    )
    
    await state.clear()

@router.callback_query(F.data == "confirm_no")
async def reject_post(callback: CallbackQuery, state: FSMContext):
    """Отказ от поста"""
    await state.clear()
    user_id = callback.from_user.id
    is_admin = user_id == ADMIN_ID
    
    await callback.message.edit_text(
        "❌ Планирование отменено",
        reply_markup=get_main_menu(user_id, is_admin)
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
        await conn.execute('UPDATE scheduled_posts SET is_sent = TRUE WHERE id = $1', post_id)
        await conn.close()
        
        logger.info(f"✅ Пост {post_id} отправлен в канал {channel_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки поста {post_id}: {e}")

# ========== ADMIN HANDLERS ==========
@router.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    """Админ панель"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "👑 Админ-панель\n\n👇 Выберите действие:",
        reply_markup=get_admin_keyboard()
    )

# ========== RESTORE JOBS ==========
async def restore_scheduled_jobs():
    """Восстановление запланированных постов"""
    try:
        conn = await get_db_connection()
        posts = await conn.fetch('''
            SELECT id, channel_id, message_type, message_text, 
                   media_file_id, media_caption, scheduled_time
            FROM scheduled_posts
            WHERE is_sent = FALSE AND scheduled_time > NOW()
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
                
                scheduled_time = post['scheduled_time']
                if scheduled_time.tzinfo is None:
                    scheduled_time = pytz.UTC.localize(scheduled_time)
                
                scheduler.add_job(
                    send_scheduled_post,
                    trigger=DateTrigger(run_date=scheduled_time),
                    args=(post['channel_id'], post_data, post['id']),
                    id=f"post_{post['id']}"
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
    logger.info("=" * 60)
    logger.info(f"🚀 ЗАПУСК БОТА")
    logger.info(f"👑 Admin ID: {ADMIN_ID}")
    logger.info("=" * 60)
    
    try:
        await init_db()
        await migrate_db()
        await restore_scheduled_jobs()
        
        scheduler.start()
        scheduler.add_job(
            scheduled_reset_posts,
            trigger='cron',
            hour=0,
            minute=1,
            timezone=MOSCOW_TZ,
            id='reset_posts'
        )
        
        me = await bot.get_me()
        logger.info(f"✅ Бот @{me.username} запущен (ID: {me.id})")
        
        if ADMIN_ID:
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"🤖 Бот @{me.username} успешно запущен!\n"
                    f"🆔 ID: {me.id}\n"
                    f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M:%S')}"
                )
            except:
                pass
        
        logger.info("=" * 60)
        logger.info("🎉 БОТ УСПЕШНО ЗАПУЩЕН!")
        logger.info("=" * 60)
        return True
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА ПРИ ЗАПУСКЕ: {e}")
        return False

async def on_shutdown():
    """Действия при выключении бота"""
    logger.info("🛑 Выключение бота...")
    if scheduler.running:
        scheduler.shutdown()
    logger.info("👋 Бот выключен")

async def scheduled_reset_posts():
    """Ежедневный сброс счетчиков постов"""
    await reset_daily_posts()

# ========== MAIN ==========
async def main():
    """Основная функция"""
    if not API_TOKEN or not DATABASE_URL:
        logger.error("❌ Отсутствуют обязательные переменные")
        return
    
    if not await on_startup():
        logger.error("❌ Не удалось запустить бота")
        return
    
    try:
        await dp.start_polling(bot, skip_updates=True)
    except KeyboardInterrupt:
        logger.info("⚠️ Получен сигнал прерывания")
    except Exception as e:
        logger.error(f"💥 КРИТИЧЕСКАЯ ОШИБКА: {e}")
    finally:
        await on_shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Бот остановлен")
    except Exception as e:
        print(f"💥 Фатальная ошибка: {e}")
        sys.exit(1)
