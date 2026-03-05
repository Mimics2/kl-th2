import os
import asyncio
import logging
import sys
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any

import pytz
from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ContentType
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

from apscheduler.schedulers.asyncio import AsyncIOScheduler
import signal
import traceback

# Импортируем модули
from database import (
    DatabasePool, execute_query, init_database, migrate_database,
    update_user_activity, get_user_tariff, update_user_subscription,
    get_user_subscription_info, update_ai_usage_log, check_ai_limits,
    get_user_channels, add_user_channel, get_user_channels_count,
    get_tariff_limits, get_user_posts_today, increment_user_posts,
    save_scheduled_post, get_user_stats, create_tariff_order,
    get_user_by_id, update_user_tariff, force_update_user_tariff,
    get_all_users, get_tariff_orders, update_order_status
)

from ai_service import (
    AdvancedAISessionManager, generate_with_gemini_advanced,
    COPYWRITER_PROMPT, IDEAS_PROMPT, TARIFFS, init_ai_manager, get_ai_manager
)

from publisher import (
    schedule_post_in_scheduler, send_scheduled_post,
    restore_scheduled_posts, PostStates
)

# ========== CONFIGURATION ==========
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
SUPPORT_URL = os.getenv("SUPPORT_URL", "https://t.me/koles_tech_support")

# Railway порт
PORT = int(os.getenv("PORT", 8080))

# ========== AI CONFIGURATION ==========
GEMINI_API_KEYS = os.getenv("GEMINI_API_KEYS", "")
if GEMINI_API_KEYS:
    try:
        GEMINI_API_KEYS = json.loads(GEMINI_API_KEYS)
        if not isinstance(GEMINI_API_KEYS, list):
            GEMINI_API_KEYS = GEMINI_API_KEYS.split(",")
    except:
        GEMINI_API_KEYS = []
else:
    GEMINI_API_KEYS = []

# Если ключи не указаны в переменных окружения
if not GEMINI_API_KEYS:
    GEMINI_API_KEYS = [
        "AIzaSyAGVcbSi0-EEBTJWJU8oc989AadsWzeilY",
        "AIzaSyBihajPnKjaJELQpQ7WnMFZm9SUPMe8Msw",
        "AIzaSyB7jaCPHmQvqVaZV9g3jEJYKFc6y4_NFXk",
        "AIzaSyC_Wq7F3qQyITUsN9THk5zZyMKXEN1YgA4",
        "AIzaSyChO_8oLyW_2e4iN7PIOIDW_WuC_xiIwJs",
        "AIzaSyAMf3qefoEXwYE7b_wN7IkfIpKl_TsZ_R4",
        "AIzaSyBeQIegKdG-XpTkUYcBOMIuXAjjGYU6aB4",
        "AIzaSyDeCtWXLkg0HIBrNq8Q2ctkTELS27PEHl8",
        "AIzaSyDbv5Y2Hu19lzdMr-uMqt8Si65v9RKYpJ4",
        "AIzaSyBl75uONFu_GAF62AEalb2KAYkgOTFYzvo",
        "AIzaSyA4gmL7YCOYgpPFN0dhF0mgnPKtrdNiYUE",
        "AIzaSyCOY7hZcYBDv7QIuY5AmgDyNH5U4fs_iWo",
        "AIzaSyBLwd-hf6P8hZB5X5rVMccWxj4S3QrM1xM",
        "AIzaSyABkNUcsMT1BjWmh-6zC9-JczrTfz1oBqw",
        "AIzaSyBlTxfNMdmgj4WsU-l8DPAwKb5KW8eF-JI"
    ]

# Убираем пустые ключи
GEMINI_API_KEYS = [key.strip() for key in GEMINI_API_KEYS if key and key.strip()]

if not GEMINI_API_KEYS:
    print("❌ ОШИБКА: Не указаны Gemini API ключи")
    sys.exit(1)

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
ALTERNATIVE_MODELS = ["gemini-2.5-flash", "gemini-1.5-flash", "gemini-1.5-pro"]

MOSCOW_TZ = pytz.timezone('Europe/Moscow')
POST_CHARACTER_LIMIT = 4000

# ========== SETUP ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)
scheduler = AsyncIOScheduler(timezone=MOSCOW_TZ)

# ========== ИНИЦИАЛИЗАЦИЯ AI МЕНЕДЖЕРА ==========
# Создаем и инициализируем глобальный AI менеджер
ai_manager = init_ai_manager(GEMINI_API_KEYS, GEMINI_MODEL, ALTERNATIVE_MODELS, MOSCOW_TZ)

# ========== STATES ==========
class AIStates(StatesGroup):
    waiting_for_topic = State()
    waiting_for_examples = State()
    waiting_for_style = State()
    waiting_for_word_count = State()
    waiting_for_idea_topic = State()

class AdminStates(StatesGroup):
    waiting_for_broadcast = State()
    waiting_for_order_note = State()
    waiting_for_user_id = State()
    waiting_for_tariff_selection = State()
    waiting_for_days_selection = State()
    waiting_for_confirm_grant = State()
    waiting_for_confirm_extend = State()
    waiting_for_force_user_id = State()  # Для принудительного обновления тарифа
    waiting_for_force_tariff = State()    # Для выбора тарифа при принудительном обновлении
    waiting_for_order_id = State()        # Для обработки заказов

# ========== KEYBOARDS ==========
def get_main_menu(user_id: int, is_admin: bool = False) -> InlineKeyboardMarkup:
    """Главное меню"""
    buttons = [
        [InlineKeyboardButton(text="🤖 ИИ-сервисы", callback_data="ai_services")],
        [InlineKeyboardButton(text="📅 Запланировать пост", callback_data="schedule_post")],
        [InlineKeyboardButton(text="📊 Моя статистика", callback_data="my_stats")],
        [InlineKeyboardButton(text="📢 Мои каналы", callback_data="my_channels")],
        [InlineKeyboardButton(text="💎 Тарифы", callback_data="tariffs")]
    ]
    
    if SUPPORT_BOT_USERNAME and SUPPORT_BOT_USERNAME != "support_bot":
        buttons.append([InlineKeyboardButton(text="🆘 Техподдержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME.replace('@', '')}")])
    else:
        buttons.append([InlineKeyboardButton(text="🆘 Поддержка", url=SUPPORT_URL)])
    
    if is_admin:
        buttons.append([InlineKeyboardButton(text="👑 Админ-панель", callback_data="admin_panel")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_channels_keyboard(channels: List[Dict]) -> InlineKeyboardMarkup:
    """Клавиатура для выбора каналов"""
    buttons = []
    for channel in channels:
        buttons.append([
            InlineKeyboardButton(
                text=f"📢 {channel['channel_name']}",
                callback_data=f"channel_{channel['channel_id']}"
            )
        ])
    buttons.append([InlineKeyboardButton(text="❌ Отменить", callback_data="cancel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_yes_no_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура Да/Нет"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да", callback_data="confirm_yes"),
            InlineKeyboardButton(text="❌ Нет", callback_data="confirm_no")
        ],
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel")]
    ])

def get_tariffs_keyboard(user_tariff: str = 'mini') -> InlineKeyboardMarkup:
    """Клавиатура для выбора тарифов"""
    buttons = []
    for tariff_id, tariff_info in TARIFFS.items():
        if tariff_id == 'admin':
            continue
            
        is_current = tariff_id == user_tariff
        price_text = f" - ${tariff_info['price']}" if tariff_info['price'] > 0 else " - Бесплатно"
        
        buttons.append([
            InlineKeyboardButton(
                text=f"{tariff_info['name']}{price_text}{' ✅' if is_current else ''}",
                callback_data=f"tariff_{tariff_id}"
            )
        ])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_panel_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура админ-панели"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика пользователей", callback_data="admin_users")],
        [InlineKeyboardButton(text="🛒 Заказы тарифов", callback_data="admin_orders")],
        [InlineKeyboardButton(text="🤖 Статистика AI", callback_data="admin_ai_stats")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="💎 Управление подписками", callback_data="admin_subscriptions")],
        [InlineKeyboardButton(text="🔄 Принудительное обновление тарифа", callback_data="admin_force_tariff")],
        [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
    ])

def get_admin_subscription_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для управления подписками в админке"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Выдать подписку", callback_data="admin_grant_subscription"),
            InlineKeyboardButton(text="🔄 Продлить подписку", callback_data="admin_extend_subscription")
        ],
        [
            InlineKeyboardButton(text="📋 Список подписок", callback_data="admin_list_subscriptions"),
            InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin_panel")
        ]
    ])

def get_admin_orders_keyboard(orders: List[Dict]) -> InlineKeyboardMarkup:
    """Клавиатура для заказов тарифов"""
    buttons = []
    for order in orders[:5]:  # Показываем только первые 5 заказов
        if order.get('status') == 'pending':
            tariff_name = TARIFFS.get(order.get('tariff'), {}).get('name', order.get('tariff'))
            buttons.append([
                InlineKeyboardButton(
                    text=f"✅ Заказ #{order['id']} - {tariff_name}",
                    callback_data=f"admin_process_order_{order['id']}"
                )
            ])
    
    buttons.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_orders")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_users_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для статистики пользователей"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Все пользователи", callback_data="admin_all_users")],
        [InlineKeyboardButton(text="📈 Активные пользователи", callback_data="admin_active_users")],
        [InlineKeyboardButton(text="💰 С подписками", callback_data="admin_subscribed_users")],
        [InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin_panel")]
    ])

def get_force_tariff_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для выбора тарифа при принудительном обновлении"""
    buttons = []
    for tariff_id, tariff_info in TARIFFS.items():
        if tariff_id != 'admin':
            buttons.append([
                InlineKeyboardButton(
                    text=f"{tariff_info['name']}",
                    callback_data=f"force_tariff_{tariff_id}"
                )
            ])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ========== UTILITY FUNCTIONS ==========
def split_message(text: str, max_length: int = 4000) -> List[str]:
    """Разбивает сообщение на части"""
    parts = []
    current_part = ""
    
    for line in text.split('\n'):
        if len(current_part + line + '\n') > max_length:
            parts.append(current_part)
            current_part = line + '\n'
        else:
            current_part += line + '\n'
    
    if current_part:
        parts.append(current_part)
    
    return parts

# ========== BASIC HANDLERS ==========
@router.message(CommandStart())
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    user_id = message.from_user.id
    username = message.from_user.username or ""
    first_name = message.from_user.first_name or "Пользователь"
    is_admin = user_id == ADMIN_ID
    
    try:
        await execute_query('''
            INSERT INTO users (id, username, first_name, is_admin, tariff, last_seen)
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (id) DO UPDATE 
            SET username = EXCLUDED.username, 
                first_name = EXCLUDED.first_name,
                is_admin = EXCLUDED.is_admin,
                last_seen = NOW()
        ''', user_id, username, first_name, is_admin, 'admin' if is_admin else 'mini', database_url=DATABASE_URL)
    except Exception as e:
        logger.error(f"Ошибка регистрации пользователя {user_id}: {e}")
    
    current_tariff = await get_user_tariff(user_id, DATABASE_URL)
    tariff_info = TARIFFS.get(current_tariff, TARIFFS['mini'])
    
    welcome_text = (
        f"👋 Привет, {first_name}!\n\n"
        f"🤖 Я — бот KOLES-TECH для планирования постов и AI-контента.\n\n"
        f"🌸С 8 марта! скидки 20% для каждого!\n\n"
        f"💎 Ваш текущий тариф: {tariff_info['name']}\n\n"
        f"✨ Возможности:\n"
        f"• 🤖 AI-копирайтер и генератор идей\n"
        f"• 📅 Запланировать пост с любым контентом\n"
        f"• 📊 Детальная статистика\n"
        f"• 📢 Управление каналов\n"
        f"• ⏰ Автопубликация в нужное время\n"
        f"• 🆘 Техподдержка всегда на связи\n\n"
        f"📍 Время указывается по Москве\n\n"
        f"👇 Выберите действия:"
    )
    
    await message.answer(welcome_text, reply_markup=get_main_menu(user_id, is_admin), parse_mode="HTML")

@router.message(Command("help"))
async def cmd_help(message: Message):
    """Обработчик команды /help"""
    help_text = (
        "📚 Помощь по использованию бота:\n\n"
        
        "🤖 AI-сервисы:\n"
        "• Копирайтер - создает продающий текст\n"
        "• Генератор идей - предлагает темы постов\n"
        "• Лимиты обновляются каждый день\n\n"
        
        "📅 Планирование поста:\n"
        "1. Выберите 'Запланировать пост'\n"
        "2. Выберите канал\n"
        "3. Отправьте контент\n"
        "4. Укажите дату и время\n"
        "5. Подтвердите публикацию\n\n"
        
        "💎 Тарифы:\n"
        "• Mini - 1 копирайт, 10 идей, 1 канал, 2 постов\n"
        "• Standard ($4) - 3 копирайта, 30 идей, 2 канала, 6 постов\n"
        "• VIP ($7) - 7 копирайтов, 50 идей, 3 канал, 12 постов\n\n"
        
        f"🆘 Поддержка: {SUPPORT_URL}\n"
        f"💬 Вопросы по оплате: @{ADMIN_CONTACT.replace('@', '')}"
    )
    
    await message.answer(help_text)

# ========== AI HANDLERS ==========
@router.callback_query(F.data == "ai_services")
async def ai_services_menu(callback: CallbackQuery):
    """Меню AI сервисов"""
    user_id = callback.from_user.id
    tariff = await get_user_tariff(user_id, DATABASE_URL)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    
    welcome_text = (
        "🤖 ИИ-Сервисы KOLES-TECH\n\n"
        "✨ Доступные возможности:\n\n"
        "📝 ИИ-копирайтер:\n"
        "• Создаст продающий текст для поста\n"
        "• Учитывает тему, стиль и примеры\n"
        "• Настройка количества слов\n"
        "• Готовый текст для публикации\n\n"
        "💡 Генератор идей:\n"
        "• {ideas_limit} идей в день\n"
        "• Разнообразные темы\n"
        "• Готовые концепты постов\n\n"
        "👇 Выберите сервис:"
    ).format(
        ideas_limit=tariff_info['ai_ideas_limit']
    )
    
    await callback.message.edit_text(
        welcome_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 ИИ-копирайтер", callback_data="ai_copywriter")],
            [InlineKeyboardButton(text="💡 Генератор идей", callback_data="ai_ideas")],
            [InlineKeyboardButton(text="📊 Мои AI-лимиты", callback_data="ai_limits")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
        ])
    )

@router.callback_query(F.data == "ai_copywriter")
async def start_copywriter(callback: CallbackQuery, state: FSMContext):
    """Запуск AI копирайтера"""
    user_id = callback.from_user.id
    
    # Получаем AI менеджер
    ai_manager = get_ai_manager()
    
    # Проверка лимитов
    can_use, message_text, tariff_info = await check_ai_limits(user_id, 'copy', DATABASE_URL, ai_manager)
    if not can_use:
        await callback.message.edit_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад в AI", callback_data="ai_services")]
            ])
        )
        return
    
    # Проверка времени между запросами
    can_request, wait_message = ai_manager.can_user_request(user_id)
    if not can_request:
        await callback.answer(wait_message, show_alert=True)
        return
    
    await state.set_state(AIStates.waiting_for_topic)
    
    session = ai_manager.get_session(user_id)
    remaining = tariff_info['ai_copies_limit'] - session['copies_used']
    
    await callback.message.edit_text(
        f"📝 ИИ-копирайтер\n\n"
        f"✅ Доступно: {remaining}/{tariff_info['ai_copies_limit']} текстов сегодня\n\n"
        f"📌 Шаг 1/4\n"
        f"Введите тему для поста:\n\n"
        f"Примеры:\n"
        f"• Запуск нового курса по маркетингу\n"
        f"• Анонс вебинара по трейдингу\n"
        f"• Продажа SEO-услуг\n"
        f"• Реклама онлайн-школы\n\n"
        f"📍 Пишите конкретно и ясно (минимум 5 символов):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
        ])
    )

@router.message(AIStates.waiting_for_topic)
async def process_topic(message: Message, state: FSMContext):
    """Обработка темы"""
    if len(message.text) < 5:
        await message.answer(
            "❌ Тема слишком короткая! Минимум 5 символов.\n\nВведите тему еще раз:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
            ])
        )
        return
    
    await state.update_data(topic=message.text)
    await state.set_state(AIStates.waiting_for_examples)
    
    await message.answer(
        "📌 Шаг 2/4\n"
        "Пришлите примеры работ или ссылки (по желанию):\n\n"
        "Можно:\n"
        "• Прислать тексты постов\n"
        "• Ссылки на каналы\n"
        "• Ключевые фразы\n"
        "• Стилистические предпочтения\n\n"
        "Или напишите 'пропустить', если примеров нет:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
        ])
    )

@router.message(AIStates.waiting_for_examples)
async def process_examples(message: Message, state: FSMContext):
    """Обработка примеров"""
    examples = message.text if message.text.lower() != 'пропустить' else "Примеры не предоставлены"
    
    await state.update_data(examples=examples)
    await state.set_state(AIStates.waiting_for_style)
    
    await message.answer(
        "📌 Шаг 3/4\n"
        "Выберите стиль текста:\n\n"
        "📱 Продающий - для продаж и конверсии\n"
        "📝 Информационный - полезный контент\n"
        "🎭 Креативный - нестандартный подход\n"
        "🎯 Целевой - для конкретной аудитории\n"
        "🚀 Для соцсетей - виральный контент\n"
        "📰 Новостной - анонсы и новости",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📱 Продающий", callback_data="style_selling"),
                InlineKeyboardButton(text="📝 Информационный", callback_data="style_info")
            ],
            [
                InlineKeyboardButton(text="🎭 Креативный", callback_data="style_creative"),
                InlineKeyboardButton(text="🎯 Целевой", callback_data="style_targeted")
            ],
            [
                InlineKeyboardButton(text="🚀 Для соцсетей", callback_data="style_social"),
                InlineKeyboardButton(text="📰 Новостной", callback_data="style_news")
            ],
            [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
        ])
    )

@router.callback_query(F.data.startswith("style_"))
async def process_style(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора стиля"""
    style_map = {
        "style_selling": "продающий",
        "style_info": "информационный",
        "style_creative": "креативный",
        "style_targeted": "целевой",
        "style_social": "для соцсетей",
        "style_news": "новостной"
    }
    
    style_key = callback.data
    style_name = style_map.get(style_key, "продающий")
    
    await state.update_data(style=style_name)
    await state.set_state(AIStates.waiting_for_word_count)
    
    ai_manager = get_ai_manager()
    current_word_count = ai_manager.get_word_count(callback.from_user.id)
    
    await callback.message.edit_text(
        f"📌 Шаг 4/4\n"
        f"Выберите количество слов для текста:\n\n"
        f"📊 Рекомендуем:\n"
        f"• 50-100 слов - короткие анонсы\n"
        f"• 150-200 слов - стандартные посты\n"
        f"• 250-300 слов - подробные статьи\n\n"
        f"📍 Текущая настройка: {current_word_count} слов",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="50 слов", callback_data="words_50"),
                InlineKeyboardButton(text="100 слов", callback_data="words_100")
            ],
            [
                InlineKeyboardButton(text="150 слов", callback_data="words_150"),
                InlineKeyboardButton(text="200 слов", callback_data="words_200")
            ],
            [
                InlineKeyboardButton(text="250 слов", callback_data="words_250"),
                InlineKeyboardButton(text="300 слов", callback_data="words_300")
            ],
            [
                InlineKeyboardButton(text="📝 Свое значение", callback_data="words_custom"),
                InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")
            ]
        ])
    )

@router.callback_query(F.data.startswith("words_"))
async def process_word_count(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора количества слов"""
    if callback.data == "words_custom":
        await callback.message.edit_text(
            "📝 Введите нужное количество слов (от 50 до 1000):\n\n"
            "Примеры:\n"
            "• 80 - короткий анонс\n"
            "• 150 - стандартный пост\n"
            "• 400 - подробная статья\n"
            "• 600 - длинный обзор",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
            ])
        )
        return
    
    try:
        word_count = int(callback.data.split("_")[1])
        user_id = callback.from_user.id
        
        ai_manager = get_ai_manager()
        
        # Устанавливаем количество слов
        ai_manager.set_word_count(user_id, word_count)
        
        # Получаем данные из состояния
        data = await state.get_data()
        
        # Показываем превью запроса
        preview_text = (
            f"📋 Ваш запрос:\n\n"
            f"📌 Тема: {data['topic']}\n"
            f"🎨 Стиль: {data['style']}\n"
            f"📝 Слов: {word_count}\n"
            f"📚 Примеры: {data['examples'][:100]}...\n\n"
            f"⏳ Генерирую текст... Пробую разные ключи (макс. 8 попыток)"
        )
        
        await callback.message.edit_text(preview_text)
        
        # Создаем промпт
        current_date = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y")
        prompt = COPYWRITER_PROMPT.format(
            topic=data['topic'],
            style=data['style'],
            examples=data['examples'],
            word_count=word_count,
            current_date=current_date
        )
        
        # Индикатор прогресса
        progress_msg = await callback.message.answer("🔄 Пробую ключ #1...")
        
        # Генерируем текст
        generated_text = await generate_with_gemini_advanced(prompt, user_id, ai_manager, max_retries=8)
        
        await progress_msg.delete()
        
        # Обработка результата
        if not generated_text:
            system_stats = ai_manager.get_system_stats()
            available_keys = system_stats['available_keys']
            total_keys = system_stats['total_keys']
            
            await callback.message.edit_text(
                f"❌ Не удалось сгенерировать текст после 8 попыток!\n\n"
                f"📊 Статистика системы:\n"
                f"• Доступных ключей: {available_keys} из {total_keys}\n"
                f"• Все ключи могут быть временно недоступны\n\n"
                f"📌 Что можно сделать:\n"
                f"1. Попробовать позже (через 5-10 минут)\n"
                f"2. Проверить доступность новых ключей API\n"
                f"3. Обратиться в поддержку: {SUPPORT_URL}\n\n"
                f"⚠️ Система автоматически попробует другие ключи при следующем запросе.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Попробовать еще раз", callback_data="ai_copywriter")],
                    [InlineKeyboardButton(text="⬅️ В меню AI", callback_data="ai_services")]
                ])
            )
            await state.clear()
            return
        
        # Обновляем статистику
        session = ai_manager.get_session(user_id)
        session['copies_used'] += 1
        
        # Логируем успешный запрос
        await update_ai_usage_log(
            user_id=user_id,
            service_type='copy',
            success=True,
            api_key_index=session.get('current_key_index', 0),
            model_name=ai_manager.get_current_model(),
            prompt_length=len(prompt),
            response_length=len(generated_text),
            database_url=DATABASE_URL
        )
        
        # Форматируем результат
        actual_word_count = len(generated_text.split())
        attempts = session['current_attempts'] or 1
        
        result_text = (
            f"✅ Текст готов! (Попытка #{attempts})\n\n"
            f"📊 Детали:\n"
            f"• Запрошено слов: {word_count}\n"
            f"• Получено слов: {actual_word_count}\n"
            f"• Символов: {len(generated_text)}\n"
            f"• Время генерации: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}\n\n"
            f"📝 Результат:\n\n"
            f"{generated_text}\n\n"
            f"📈 Статистика:\n"
            f"• Использовано сегодня: {session['copies_used']}/{TARIFFS.get(await get_user_tariff(user_id, DATABASE_URL), TARIFFS['mini'])['ai_copies_limit']}"
        )
        
        # Отправляем результат (разбиваем если нужно)
        if len(result_text) > 4000:
            parts = split_message(result_text)
            for i, part in enumerate(parts):
                if i == 0:
                    await callback.message.edit_text(part)
                else:
                    await callback.message.answer(part)
        else:
            await callback.message.edit_text(result_text)
        
        # Клавиатура действий с текстом
        action_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📱 Отправить в чат", callback_data="send_to_chat"),
                InlineKeyboardButton(text="✏️ Редактировать", callback_data="edit_text")
            ],
            [
                InlineKeyboardButton(text="🔄 Новый текст", callback_data="ai_copywriter"),
                InlineKeyboardButton(text="📋 Сохранить", callback_data="save_text")
            ],
            [
                InlineKeyboardButton(text="⬅️ В меню AI", callback_data="ai_services")
            ]
        ])
        
        # Сохраняем сгенерированный текст в состоянии
        await state.update_data(generated_text=generated_text)
        
        await callback.message.answer(
            "👇 Что сделать с текстом?",
            reply_markup=action_keyboard
        )
        
    except ValueError:
        await callback.answer("❌ Ошибка в количестве слов", show_alert=True)

@router.message(AIStates.waiting_for_word_count)
async def process_custom_word_count(message: Message, state: FSMContext):
    """Обработка пользовательского количества слов"""
    try:
        word_count = int(message.text.strip())
        if word_count < 50 or word_count > 1000:
            await message.answer(
                "❌ Количество слов должно быть от 50 до 1000!\n\n"
                "Попробуйте еще раз:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
                ])
            )
            return
        
        user_id = message.from_user.id
        ai_manager = get_ai_manager()
        ai_manager.set_word_count(user_id, word_count)
        
        data = await state.get_data()
        
        # Показываем превью запроса
        preview_text = (
            f"📋 Ваш запрос:\n\n"
            f"📌 Тема: {data['topic']}\n"
            f"🎨 Стиль: {data['style']}\n"
            f"📝 Слов: {word_count}\n"
            f"📚 Примеры: {data['examples'][:100]}...\n\n"
            f"⏳ Генерирую текст... Пробую разные ключи (макс. 8 попыток)"
        )
        
        await message.answer(preview_text)
        
        # Создаем промпт
        current_date = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y")
        prompt = COPYWRITER_PROMPT.format(
            topic=data['topic'],
            style=data['style'],
            examples=data['examples'],
            word_count=word_count,
            current_date=current_date
        )
        
        # Индикатор прогресса
        progress_msg = await message.answer("🔄 Пробую ключ #1...")
        
        # Генерируем текст
        generated_text = await generate_with_gemini_advanced(prompt, user_id, ai_manager, max_retries=8)
        
        await progress_msg.delete()
        
        # Обработка результата
        if not generated_text:
            system_stats = ai_manager.get_system_stats()
            available_keys = system_stats['available_keys']
            total_keys = system_stats['total_keys']
            
            await message.answer(
                f"❌ Не удалось сгенерировать текст после 8 попыток!\n\n"
                f"📊 Статистика системы:\n"
                f"• Доступных ключей: {available_keys} из {total_keys}\n"
                f"• Все ключи могут быть временно недоступны\n\n"
                f"Попробуйте позже или обратитесь в поддержку.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Попробовать еще раз", callback_data="ai_copywriter")],
                    [InlineKeyboardButton(text="⬅️ В меню AI", callback_data="ai_services")]
                ])
            )
            return
        
        # Обновляем статистику
        session = ai_manager.get_session(user_id)
        session['copies_used'] += 1
        
        # Логируем успешный запрос
        await update_ai_usage_log(
            user_id=user_id,
            service_type='copy',
            success=True,
            api_key_index=session.get('current_key_index', 0),
            model_name=ai_manager.get_current_model(),
            prompt_length=len(prompt),
            response_length=len(generated_text),
            database_url=DATABASE_URL
        )
        
        # Форматируем результат
        actual_word_count = len(generated_text.split())
        attempts = session['current_attempts'] or 1
        
        result_text = (
            f"✅ Текст готов! (Попытка #{attempts})\n\n"
            f"📊 Детали:\n"
            f"• Запрошено слов: {word_count}\n"
            f"• Получено слов: {actual_word_count}\n"
            f"• Символов: {len(generated_text)}\n"
            f"• Время генерации: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}\n\n"
            f"📝 Результат:\n\n"
            f"{generated_text}\n\n"
            f"📈 Статистика:\n"
            f"• Использовано сегодня: {session['copies_used']}/{TARIFFS.get(await get_user_tariff(user_id, DATABASE_URL), TARIFFS['mini'])['ai_copies_limit']}"
        )
        
        # Отправляем результат (разбиваем если нужно)
        if len(result_text) > 4000:
            parts = split_message(result_text)
            for i, part in enumerate(parts):
                await message.answer(part)
        else:
            await message.answer(result_text)
        
        # Клавиатура действий с текстом
        action_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📱 Отправить в чат", callback_data="send_to_chat"),
                InlineKeyboardButton(text="✏️ Редактировать", callback_data="edit_text")
            ],
            [
                InlineKeyboardButton(text="🔄 Новый текст", callback_data="ai_copywriter"),
                InlineKeyboardButton(text="📋 Сохранить", callback_data="save_text")
            ],
            [
                InlineKeyboardButton(text="⬅️ В меню AI", callback_data="ai_services")
            ]
        ])
        
        await state.update_data(generated_text=generated_text)
        
        await message.answer(
            "👇 Что сделать с текстом?",
            reply_markup=action_keyboard
        )
        
    except ValueError:
        await message.answer(
            "❌ Введите число!\n\nПример: 150, 200, 300",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
            ])
        )

@router.callback_query(F.data == "ai_ideas")
async def start_ideas_generator(callback: CallbackQuery, state: FSMContext):
    """Запуск генератора идей"""
    user_id = callback.from_user.id
    
    ai_manager = get_ai_manager()
    
    # Проверка лимитов
    can_use, message_text, tariff_info = await check_ai_limits(user_id, 'ideas', DATABASE_URL, ai_manager)
    if not can_use:
        await callback.message.edit_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад в AI", callback_data="ai_services")]
            ])
        )
        return
    
    # Проверка времени между запросами
    can_request, wait_message = ai_manager.can_user_request(user_id)
    if not can_request:
        await callback.answer(wait_message, show_alert=True)
        return
    
    await state.set_state(AIStates.waiting_for_idea_topic)
    
    session = ai_manager.get_session(user_id)
    remaining = tariff_info['ai_ideas_limit'] - session['ideas_used']
    
    await callback.message.edit_text(
        f"💡 Генератор идей\n\n"
        f"✅ Доступно: {remaining}/{tariff_info['ai_ideas_limit']} идей сегодня\n\n"
        f"Введите тему для генерации идей:\n\n"
        f"Примеры:\n"
        f"• Маркетинг в Telegram\n"
        f"• Образовательный контент\n"
        f"• Новости IT-сферы\n"
        f"• Здоровый образ жизни\n\n"
        f"📍 Чем конкретнее тема, тем лучше идеи (минимум 3 символа):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
        ])
    )

@router.message(AIStates.waiting_for_idea_topic)
async def process_idea_topic(message: Message, state: FSMContext):
    """Обработка темы для идей"""
    if len(message.text) < 3:
        await message.answer(
            "❌ Тема слишком короткая! Минимум 3 символа.\n\nВведите тему еще раз:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
            ])
        )
        return
    
    await state.update_data(topic=message.text)
    
    await message.answer(
        "Выберите количество идей (от 5 до 20):\n\n"
        "📊 Рекомендуем:\n"
        "• 5 идей - быстрый просмотр\n"
        "• 10 идей - оптимальный выбор\n"
        "• 15-20 идей - полный охват темы",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="5 идей", callback_data="ideas_5"),
                InlineKeyboardButton(text="10 идей", callback_data="ideas_10")
            ],
            [
                InlineKeyboardButton(text="15 идей", callback_data="ideas_15"),
                InlineKeyboardButton(text="20 идей", callback_data="ideas_20")
            ],
            [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
        ])
    )

@router.callback_query(F.data.startswith("ideas_"))
async def generate_ideas(callback: CallbackQuery, state: FSMContext):
    """Генерация идей"""
    count = int(callback.data.split("_")[1])
    data = await state.get_data()
    
    if count > 20:
        count = 20
    
    ai_manager = get_ai_manager()
    
    # Показываем индикатор
    await callback.message.edit_text(
        f"💡 Генерация {count} идей по теме:\n"
        f"📌 '{data['topic']}'\n\n"
        f"⏳ Это займет 10-30 секунд..."
    )
    
    # Генерация идей
    current_date = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y")
    prompt = IDEAS_PROMPT.format(
        count=count,
        topic=data['topic'],
        current_date=current_date
    )
    
    loading_msg = await callback.message.answer("🔄 ИИ генерирует идеи...")
    
    generated_ideas = await generate_with_gemini_advanced(prompt, callback.from_user.id, ai_manager, max_retries=8)
    
    await loading_msg.delete()
    
    # Обработка результата
    if not generated_ideas:
        system_stats = ai_manager.get_system_stats()
        available_keys = system_stats['available_keys']
        total_keys = system_stats['total_keys']
        
        await callback.message.edit_text(
            f"❌ Не удалось сгенерировать идеи после 8 попыток!\n\n"
            f"📊 Статистика системы:\n"
            f"• Доступных ключей: {available_keys} из {total_keys}\n"
            f"• Все ключи могут быть временно недоступны\n\n"
            f"Попробуйте позже или обратитесь в поддержку.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Попробовать еще раз", callback_data="ai_ideas")],
                [InlineKeyboardButton(text="⬅️ В меню AI", callback_data="ai_services")]
            ])
        )
        await state.clear()
        return
    
    # Форматируем результат
    ideas_list = generated_ideas.split('\n')
    formatted_ideas = []
    
    for i, idea in enumerate(ideas_list[:count], 1):
        if idea.strip():
            formatted_ideas.append(f"{i}. {idea.strip()}")
    
    # Обновляем статистику
    session = ai_manager.get_session(callback.from_user.id)
    session['ideas_used'] += 1
    
    # Логируем успешный запрос
    await update_ai_usage_log(
        user_id=callback.from_user.id,
        service_type='ideas',
        success=True,
        api_key_index=session.get('current_key_index', 0),
        model_name=ai_manager.get_current_model(),
        prompt_length=len(prompt),
        response_length=len(generated_ideas),
        database_url=DATABASE_URL
    )
    
    result_text = (
        f"✅ Сгенерировано {len(formatted_ideas)} идей! (Попытка #{session['current_attempts'] or 1})\n\n"
        f"📌 Тема: {data['topic']}\n\n"
        f"💡 Идеи:\n\n" +
        "\n".join(formatted_ideas) +
        f"\n\n📊 Статистика:\n"
        f"• Использовано сегодня: {session['ideas_used']}/{TARIFFS.get(await get_user_tariff(callback.from_user.id, DATABASE_URL), TARIFFS['mini'])['ai_ideas_limit']}"
    )
    
    # Разбиваем длинные сообщения
    if len(result_text) > 4000:
        parts = split_message(result_text)
        for i, part in enumerate(parts):
            if i == 0:
                await callback.message.edit_text(part)
            else:
                await callback.message.answer(part)
    else:
        await callback.message.edit_text(result_text)
    
    await callback.message.answer(
        "👇 Выберите действие:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💡 Новые идеи", callback_data="ai_ideas")],
            [InlineKeyboardButton(text="📝 Копирайтер", callback_data="ai_copywriter")],
            [InlineKeyboardButton(text="⬅️ В меню AI", callback_data="ai_services")]
        ])
    )
    
    await state.clear()

@router.callback_query(F.data == "ai_limits")
async def show_ai_limits(callback: CallbackQuery):
    """Показывает лимиты AI"""
    user_id = callback.from_user.id
    tariff = await get_user_tariff(user_id, DATABASE_URL)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    
    ai_manager = get_ai_manager()
    session = ai_manager.get_session(user_id)
    
    # Рассчитываем оставшееся время до сброса
    today = datetime.now(MOSCOW_TZ).date()
    reset_time = datetime.combine(today + timedelta(days=1), datetime.min.time())
    reset_time = MOSCOW_TZ.localize(reset_time)
    time_left = reset_time - datetime.now(MOSCOW_TZ)
    hours = int(time_left.total_seconds() // 3600)
    minutes = int((time_left.total_seconds() % 3600) // 60)
    
    system_stats = ai_manager.get_system_stats()
    available_keys = system_stats['available_keys']
    total_keys = system_stats['total_keys']
    
    limits_text = (
        f"📊 Ваши AI-лимиты\n\n"
        f"💎 Тариф: {tariff_info['name']}\n\n"
        f"📝 Копирайтер:\n"
        f"• Использовано: {session['copies_used']}/{tariff_info['ai_copies_limit']}\n"
        f"• Осталось: {tariff_info['ai_copies_limit'] - session['copies_used']}\n\n"
        f"💡 Генератор идей:\n"
        f"• Использовано: {session['ideas_used']}/{tariff_info['ai_ideas_limit']}\n"
        f"• Осталось: {tariff_info['ai_ideas_limit'] - session['ideas_used']}\n\n"
        f"🔄 Обновление через: {hours}ч {minutes}м\n\n"
        f"📈 Всего AI запросов: {session['total_requests']}\n\n"
        f"🔑 Система ключей:\n"
        f"• Доступных ключей: {available_keys} из {total_keys}\n"
        f"• Ошибок подряд: {session['consecutive_errors']}"
    )
    
    await callback.message.edit_text(
        limits_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ В меню AI", callback_data="ai_services")]
        ])
    )

@router.callback_query(F.data == "send_to_chat")
async def send_to_chat_handler(callback: CallbackQuery, state: FSMContext):
    """Отправляет текст в чат"""
    data = await state.get_data()
    generated_text = data.get('generated_text')
    
    if not generated_text:
        await callback.answer("❌ Текст не найден!", show_alert=True)
        return
    
    await callback.message.answer(
        f"📝 Ваш текст:\n\n{generated_text}"
    )
    
    await callback.answer("✅ Текст отправлен в чат!")

@router.callback_query(F.data == "edit_text")
async def edit_text_handler(callback: CallbackQuery, state: FSMContext):
    """Редактирование текста"""
    data = await state.get_data()
    generated_text = data.get('generated_text')
    
    if not generated_text:
        await callback.answer("❌ Текст не найден!", show_alert=True)
        return
    
    await callback.message.answer(
        f"✏️ Чтобы отредактировать текст, просто отправьте новую версию:\n\n"
        f"Текущий текст:\n{generated_text[:500]}..."
    )

@router.callback_query(F.data == "save_text")
async def save_text_handler(callback: CallbackQuery):
    """Сохранение текста"""
    await callback.answer("✅ Текст сохранен (функция в разработке)", show_alert=True)

@router.callback_query(F.data == "cancel_ai")
async def cancel_ai(callback: CallbackQuery, state: FSMContext):
    """Отмена AI операций"""
    await state.clear()
    user_id = callback.from_user.id
    is_admin = user_id == ADMIN_ID
    
    await callback.message.edit_text(
        "❌ Операция отменена",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ В меню AI", callback_data="ai_services")]
        ])
    )

# ========== ИСПРАВЛЕННЫЕ HANDLERS ПЛАНИРОВАНИЯ ПОСТОВ ==========
@router.callback_query(F.data == "schedule_post")
async def schedule_post_start(callback: CallbackQuery, state: FSMContext):
    """Начало планирования поста"""
    user_id = callback.from_user.id
    
    # Проверяем лимиты постов
    posts_today = await get_user_posts_today(user_id, DATABASE_URL)
    channels_limit, posts_limit, _, _ = await get_tariff_limits(user_id, DATABASE_URL)
    
    if posts_today >= posts_limit:
        now = datetime.now(MOSCOW_TZ)
        reset_time = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
        reset_time = MOSCOW_TZ.localize(reset_time)
        time_left = reset_time - now
        hours = int(time_left.total_seconds() // 3600)
        minutes = int((time_left.total_seconds() % 3600) // 60)
        
        await callback.message.edit_text(
            f"❌ Достигнут дневной лимит постов!\n\n"
            f"📊 Статистика:\n"
            f"• Отправлено сегодня: {posts_today}/{posts_limit}\n"
            f"• Доступно каналов: {await get_user_channels_count(user_id, DATABASE_URL)}/{channels_limit}\n\n"
            f"⏳ Обновление через: {hours}ч {minutes}м",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
            ])
        )
        return
    
    # Получаем каналы пользователя
    channels = await get_user_channels(user_id, DATABASE_URL)
    
    if not channels:
        await callback.message.edit_text(
            "📢 У вас нет подключенных каналов!\n\n"
            "Чтобы добавить канал:\n"
            "1. Добавьте бота в администраторы вашего канала\n"
            "2. Отправьте любое сообщение из канала боту\n"
            "3. Бот автоматически добавит канал\n\n"
            "📍 Бот должен иметь права администратора в канале",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
            ])
        )
        return
    
    await state.set_state(PostStates.waiting_for_channel)
    
    await callback.message.edit_text(
        f"📅 Планирование поста\n\n"
        f"📊 Статистика:\n"
        f"• Отправлено сегодня: {posts_today}/{posts_limit}\n"
        f"• Каналов подключено: {len(channels)}/{channels_limit}\n\n"
        f"📍 Шаг 1/5\n"
        f"Выберите канал для публикации:",
        reply_markup=get_channels_keyboard(channels)
    )

@router.callback_query(F.data.startswith("channel_"))
async def select_channel(callback: CallbackQuery, state: FSMContext):
    """Выбор канала"""
    channel_id = int(callback.data.split("_")[1])
    
    # Получаем название канала
    channels = await get_user_channels(callback.from_user.id, DATABASE_URL)
    channel_name = "Неизвестный канал"
    for channel in channels:
        if channel['channel_id'] == channel_id:
            channel_name = channel['channel_name']
            break
    
    await state.update_data(channel_id=channel_id, channel_name=channel_name)
    await state.set_state(PostStates.waiting_for_content)
    
    await callback.message.edit_text(
        f"✅ Выбран канал: {channel_name}\n\n"
        f"📍 Шаг 2/5\n"
        f"Отправьте контент для поста:\n\n"
        f"📌 Можно отправить:\n"
        f"• Текст (до {POST_CHARACTER_LIMIT} символов)\n"
        f"• Фото с подписью\n"
        f"• Видео с подписью\n"
        f"• Документ с подписью\n\n"
        f"⚠️ Ограничения:\n"
        f"• Подпись к медиа: до 1000 символов\n"
        f"• Текст поста: до {POST_CHARACTER_LIMIT} символов",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
        ])
    )

@router.message(PostStates.waiting_for_content)
async def process_content(message: Message, state: FSMContext):
    """Обработка контента"""
    post_data = {}
    
    if message.content_type == ContentType.TEXT:
        if len(message.text) > POST_CHARACTER_LIMIT:
            await message.answer(
                f"❌ Текст слишком длинный! Максимум {POST_CHARACTER_LIMIT} символов.\n\n"
                f"Ваш текст: {len(message.text)} символов\n"
                f"Сократите текст и отправьте снова:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
                ])
            )
            return
        
        post_data = {
            'message_type': 'text',
            'message_text': message.text
        }
    
    elif message.content_type == ContentType.PHOTO:
        if message.caption and len(message.caption) > 1000:
            await message.answer(
                "❌ Подпись к фото слишком длинная! Максимум 1000 символов.\n\n"
                "Сократите подпись и отправьте фото снова:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
                ])
            )
            return
        
        post_data = {
            'message_type': 'photo',
            'media_file_id': message.photo[-1].file_id,
            'media_caption': message.caption or ''
        }
    
    elif message.content_type == ContentType.VIDEO:
        if message.caption and len(message.caption) > 1000:
            await message.answer(
                "❌ Подпись к видео слишком длинная! Максимум 1000 символов.\n\n"
                "Сократите подпись и отправьте видео снова:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
                ])
            )
            return
        
        post_data = {
            'message_type': 'video',
            'media_file_id': message.video.file_id,
            'media_caption': message.caption or ''
        }
    
    elif message.content_type == ContentType.DOCUMENT:
        if message.caption and len(message.caption) > 1000:
            await message.answer(
                "❌ Подпись к документу слишком длинная! Максимум 1000 символов.\n\n"
                "Сократите подпись и отправьте документ снова:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
                ])
            )
            return
        
        post_data = {
            'message_type': 'document',
            'media_file_id': message.document.file_id,
            'media_caption': message.caption or ''
        }
    
    else:
        await message.answer(
            "❌ Неподдерживаемый тип контента!\n\n"
            "Отправьте текст, фото, видео или документ:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
            ])
        )
        return
    
    await state.update_data(post_data=post_data)
    await state.set_state(PostStates.waiting_for_date)
    
    current_date = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y")
    tomorrow_date = (datetime.now(MOSCOW_TZ) + timedelta(days=1)).strftime("%d.%m.%Y")
    
    await message.answer(
        f"✅ Контент принят!\n\n"
        f"📍 Шаг 3/5\n"
        f"Введите дату публикации (ДД.ММ.ГГГГ):\n\n"
        f"📅 Примеры:\n"
        f"• Сегодня: {current_date}\n"
        f"• Завтра: {tomorrow_date}\n"
        f"• 25.12.2024\n"
        f"• 01.01.2025\n\n"
        f"⚠️ Дата должна быть в будущем",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
        ])
    )

@router.message(PostStates.waiting_for_date)
async def process_date(message: Message, state: FSMContext):
    """Обработка даты"""
    try:
        # Пробуем разные форматы дат
        date_formats = ["%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]
        date_obj = None
        
        for fmt in date_formats:
            try:
                date_obj = datetime.strptime(message.text.strip(), fmt)
                break
            except ValueError:
                continue
        
        if not date_obj:
            await message.answer(
                "❌ Неверный формат даты!\n\n"
                "Правильные форматы:\n"
                "• ДД.ММ.ГГГГ (15.12.2024)\n"
                "• ДД/ММ/ГГГГ (15/12/2024)\n"
                "• ДД-ММ-ГГГГ (15-12-2024)\n\n"
                "Введите дату еще раз:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
                ])
            )
            return
        
        # Проверяем что дата в будущем
        now = datetime.now(MOSCOW_TZ)
        input_date = MOSCOW_TZ.localize(datetime.combine(date_obj.date(), datetime.min.time()))
        
        if input_date.date() < now.date():
            await message.answer(
                "❌ Дата должна быть сегодня или в будущем!\n\n"
                f"Вы ввели: {date_obj.strftime('%d.%m.%Y')}\n"
                f"Сегодня: {now.strftime('%d.%m.%Y')}\n\n"
                "Введите дату еще раз:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
                ])
            )
            return
        
        await state.update_data(date_str=message.text)
        await state.set_state(PostStates.waiting_for_time)
        
        current_time = datetime.now(MOSCOW_TZ).strftime("%H:%M")
        
        await message.answer(
            f"✅ Дата принята: {date_obj.strftime('%d.%m.%Y')}\n\n"
            f"📍 Шаг 4/5\n"
            f"Введите время публикации (ЧЧ:ММ):\n\n"
            f"🕐 Примеры:\n"
            f"• Сейчас: {current_time}\n"
            f"• 09:00\n"
            f"• 14:30\n"
            f"• 18:45\n\n"
            f"📍 Время указывается по Москве",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
            ])
        )
        
    except Exception as e:
        logger.error(f"Ошибка обработки даты: {e}")
        await message.answer(
            "❌ Ошибка обработки даты!\n\n"
            "Попробуйте еще раз:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
            ])
        )

@router.message(PostStates.waiting_for_time)
async def process_time(message: Message, state: FSMContext):
    """Обработка времени"""
    try:
        from publisher import parse_datetime
        
        data = await state.get_data()
        date_str = data.get('date_str')
        
        # Парсим полную дату и время
        scheduled_datetime = parse_datetime(date_str, message.text.strip(), MOSCOW_TZ)
        
        if not scheduled_datetime:
            await message.answer(
                "❌ Неверный формат времени!\n\n"
                "Правильные форматы:\n"
                "• ЧЧ:ММ (14:30)\n"
                "• ЧЧ.ММ (14.30)\n\n"
                "Введите время еще раз:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
                ])
            )
            return
        
        # Проверяем что время в будущем
        now = datetime.now(MOSCOW_TZ)
        if scheduled_datetime <= now:
            await message.answer(
                "❌ Время должно быть в будущем!\n\n"
                f"Вы указали: {scheduled_datetime.strftime('%d.%m.%Y %H:%M')}\n"
                f"Сейчас: {now.strftime('%d.%m.%Y %H:%M')}\n\n"
                "Введите время еще раз:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
                ])
            )
            return
        
        await state.update_data(scheduled_datetime=scheduled_datetime)
        await state.set_state(PostStates.waiting_for_confirmation)
        
        # Форматируем информацию для подтверждения
        channel_name = data.get('channel_name', 'Неизвестный канал')
        post_data = data.get('post_data', {})
        
        if post_data.get('message_type') == 'text':
            content_preview = post_data.get('message_text', '')[:200] + "..." if len(post_data.get('message_text', '')) > 200 else post_data.get('message_text', '')
            content_info = f"📝 Текст: {content_preview}"
        else:
            media_type = {
                'photo': '📷 Фото',
                'video': '🎥 Видео',
                'document': '📄 Документ'
            }.get(post_data.get('message_type'), 'Медиа')
            caption = post_data.get('media_caption', '')[:100] + "..." if post_data.get('media_caption') and len(post_data.get('media_caption', '')) > 100 else post_data.get('media_caption', '')
            content_info = f"{media_type}\n📝 Подпись: {caption if caption else 'Нет подписи'}"
        
        await message.answer(
            f"✅ Время принято!\n\n"
            f"📍 Шаг 5/5\n"
            f"📋 ПОДТВЕРЖДЕНИЕ ЗАПЛАНИРОВАННОГО ПОСТА\n\n"
            f"📢 Канал: {channel_name}\n"
            f"📅 Дата и время: {scheduled_datetime.strftime('%d.%m.%Y %H:%M')} (МСК)\n"
            f"{content_info}\n\n"
            f"📍 После публикации:\n"
            f"• Пост будет автоматически отправлен в указанное время\n"
            f"• Счетчик ваших постов увеличится\n"
            f"• Вы получите уведомление об отправке\n\n"
            f"Подтвердить публикацию?",
            reply_markup=get_yes_no_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка обработки времени: {e}")
        await message.answer(
            "❌ Ошибка обработки времени!\n\n"
            "Попробуйте еще раз:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
            ])
        )

@router.callback_query(F.data == "confirm_yes", PostStates.waiting_for_confirmation)
async def confirm_post(callback: CallbackQuery, state: FSMContext):
    """Подтверждение публикации"""
    from publisher import schedule_post_in_scheduler
    
    user_id = callback.from_user.id
    data = await state.get_data()
    
    channel_id = data.get('channel_id')
    channel_name = data.get('channel_name')
    post_data = data.get('post_data', {})
    scheduled_datetime = data.get('scheduled_datetime')
    
    # Сохраняем пост в базу данных
    post_id = await save_scheduled_post(user_id, channel_id, post_data, scheduled_datetime, MOSCOW_TZ, DATABASE_URL)
    
    if not post_id:
        await callback.message.edit_text(
            "❌ Ошибка при сохранении поста!\n\n"
            "Попробуйте позже или обратитесь в поддержку.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
            ])
        )
        await state.clear()
        return
    
    # Добавляем задачу в планировщик
    scheduled_success = await schedule_post_in_scheduler(post_id, scheduled_datetime, scheduler, bot, send_scheduled_post, MOSCOW_TZ, DATABASE_URL)
    
    if not scheduled_success:
        await callback.message.edit_text(
            "❌ Ошибка при планировании поста!\n\n"
            "Пост сохранен в базе данных, но не добавлен в планировщик.\n"
            "Попробуйте позже или обратитесь в поддержку.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
            ])
        )
        await state.clear()
        return
    
    # Увеличиваем счетчик постов пользователя
    await increment_user_posts(user_id, DATABASE_URL)
    
    await callback.message.edit_text(
        f"✅ Пост успешно запланирован!\n\n"
        f"📋 Детали:\n"
        f"• Канал: {channel_name}\n"
        f"• Время: {scheduled_datetime.strftime('%d.%m.%Y %H:%M')} (МСК)\n"
        f"• ID поста: {post_id}\n\n"
        f"📍 Система автоматически отправит пост в указанное время.\n"
        f"Вы получите уведомление об отправке.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📅 Новый пост", callback_data="schedule_post")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
        ])
    )
    
    await state.clear()

@router.callback_query(F.data == "confirm_no", PostStates.waiting_for_confirmation)
async def cancel_post(callback: CallbackQuery, state: FSMContext):
    """Отмена публикации"""
    await state.clear()
    user_id = callback.from_user.id
    is_admin = user_id == ADMIN_ID
    
    await callback.message.edit_text(
        "❌ Публикация отменена.\n\n👇 Выберите действие:",
        reply_markup=get_main_menu(user_id, is_admin)
    )

# ========== STATISTICS HANDLERS ==========
@router.callback_query(F.data == "my_stats")
async def show_my_stats(callback: CallbackQuery):
    """Показывает статистику пользователя"""
    user_id = callback.from_user.id
    ai_manager = get_ai_manager()
    stats = await get_user_stats(user_id, DATABASE_URL, ai_manager)
    
    system_stats = ai_manager.get_system_stats()
    available_keys = system_stats['available_keys']
    total_keys = system_stats['total_keys']
    
    # Форматируем дату окончания подписки
    expires_info = ""
    if stats.get('subscription_expires'):
        expires_date = stats['subscription_expires']
        if stats.get('subscription_expired'):
            expires_info = f"❌ Подписка истекла: {expires_date.strftime('%d.%m.%Y')}"
        else:
            expires_info = f"✅ Подписка активна до: {expires_date.strftime('%d.%m.%Y')} (осталось {stats['subscription_days_left']} дней)"
    else:
        expires_info = "ℹ️ Подписка отсутствует"
    
    stats_text = (
        f"📊 Ваша статистика:\n\n"
        f"💎 Тариф: {stats.get('tariff', 'Mini')}\n"
        f"{expires_info}\n\n"
        f"📅 Посты сегодня:\n"
        f"• Отправлено: {stats.get('posts_today', 0)}/{stats.get('posts_limit', 2)}\n"
        f"• Запланировано: {stats.get('scheduled_posts', 0)}\n\n"
        f"📢 Каналы:\n"
        f"• Подключено: {stats.get('channels_count', 0)}/{stats.get('channels_limit', 1)}\n\n"
        f"🤖 AI-сервисы:\n"
        f"• Копирайтинг: {stats.get('ai_copies_used', 0)}/{stats.get('ai_copies_limit', 1)}\n"
        f"• Идеи: {stats.get('ai_ideas_used', 0)}/{stats.get('ai_ideas_limit', 10)}\n"
        f"• Всего AI запросов: {stats.get('total_ai_requests', 0)}\n\n"
        f"🔑 Система ключей:\n"
        f"• Доступных ключей: {available_keys} из {total_keys}\n\n"
        f"📍 Время по Москве: {datetime.now(MOSCOW_TZ).strftime('%H:%M')}"
    )
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=get_main_menu(user_id, user_id == ADMIN_ID)
    )

# ========== CHANNELS HANDLERS ==========
@router.callback_query(F.data == "my_channels")
async def show_my_channels(callback: CallbackQuery):
    """Показывает каналы пользователя"""
    user_id = callback.from_user.id
    channels = await get_user_channels(user_id, DATABASE_URL)
    channels_count = len(channels)
    channels_limit, _, _, _ = await get_tariff_limits(user_id, DATABASE_URL)
    
    if not channels:
        channels_text = "📭 У вас нет подключенных каналов"
    else:
        channels_list = []
        for i, channel in enumerate(channels, 1):
            channels_list.append(f"{i}. {channel['channel_name']} (ID: {channel['channel_id']})")
        
        channels_text = "📢 Ваши каналы:\n\n" + "\n".join(channels_list)
    
    text = (
        f"{channels_text}\n\n"
        f"📊 Статистика:\n"
        f"• Подключено: {channels_count}/{channels_limit}\n\n"
        f"📍 Чтобы добавить канал:\n"
        f"1. Добавьте бота в администраторы канала\n"
        f"2. Отправьте любое сообщение из канала боту\n"
        f"3. Канал будет автоматически добавлен"
    )
    
    try:
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить список", callback_data="my_channels")],
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
            ])
        )
    except Exception as e:
        # Если сообщение не изменилось, игнорируем ошибку
        if "message is not modified" in str(e):
            await callback.answer("Список каналов не изменился")
        else:
            logger.error(f"Ошибка при показе каналов: {e}")
            await callback.answer("❌ Произошла ошибка", show_alert=True)

@router.message(F.forward_from_chat)
async def handle_forwarded_channel_message(message: Message):
    """Обработка пересланных сообщений из канала для добавления канала"""
    user_id = message.from_user.id
    
    # Проверяем, является ли сообщение из канала
    if message.forward_from_chat:
        channel = message.forward_from_chat
        
        # Проверяем лимит каналов
        channels_count = await get_user_channels_count(user_id, DATABASE_URL)
        channels_limit, _, _, _ = await get_tariff_limits(user_id, DATABASE_URL)
        
        if channels_count >= channels_limit:
            await message.answer(
                f"❌ Достигнут лимит каналов!\n\n"
                f"📊 Статистика:\n"
                f"• Подключено: {channels_count}/{channels_limit}\n\n"
                f"📍 Чтобы увеличить лимит, перейдите на тариф Standard или VIP:\n"
                f"Меню → Тарифы",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="💎 Тарифы", callback_data="tariffs")],
                    [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
                ])
            )
            return
        
        # Добавляем канал
        success = await add_user_channel(user_id, channel.id, channel.title or f"Канал {channel.id}", DATABASE_URL)
        
        if success:
            # Получаем обновленное количество каналов
            new_count = await get_user_channels_count(user_id, DATABASE_URL)
            
            await message.answer(
                f"✅ Канал успешно добавлен!\n\n"
                f"📢 {channel.title}\n"
                f"🆔 ID: {channel.id}\n\n"
                f"📊 Каналов подключено: {new_count}/{channels_limit}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📅 Запланировать пост", callback_data="schedule_post")],
                    [InlineKeyboardButton(text="📢 Мои каналы", callback_data="my_channels")],
                    [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
                ])
            )
        else:
            await message.answer(
                "❌ Ошибка при добавлении канала!\n\n"
                "Убедитесь, что бот добавлен в администраторы канала и попробуйте еще раз.\n\n"
                "Возможные причины:\n"
                "• Бот не является администратором канала\n"
                "• Канал уже добавлен другим пользователем\n"
                "• Техническая ошибка",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data="my_channels")],
                    [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
                ])
            )
    else:
        await message.answer(
            "📍 Чтобы добавить канал:\n\n"
            "1. Добавьте бота в администраторы вашего канала\n"
            "2. Перешлите любое сообщение из канала в этот чат\n"
            "3. Бот автоматически добавит канал"
        )

# ========== TARIFFS HANDLERS ==========
@router.callback_query(F.data == "tariffs")
async def show_tariffs(callback: CallbackQuery):
    """Показывает тарифы"""
    user_id = callback.from_user.id
    user_tariff = await get_user_tariff(user_id, DATABASE_URL)
    
    tariffs_text = "💎 ТАРИФЫ KOLES-TECH\n\n"
    
    for tariff_id, tariff_info in TARIFFS.items():
        if tariff_id == 'admin':
            continue
            
        is_current = tariff_id == user_tariff
        current_marker = " ✅ ТЕКУЩИЙ" if is_current else ""
        
        price_text = f"${tariff_info['price']}/месяц" if tariff_info['price'] > 0 else "Бесплатно"
        
        tariffs_text += (
            f"{tariff_info['name']} - {price_text}{current_marker}\n"
            f"• Каналов: {tariff_info['channels_limit']}\n"
            f"• Постов в день: {tariff_info['daily_posts_limit']}\n"
            f"• AI-текстов: {tariff_info['ai_copies_limit']}/день\n"
            f"• AI-идей: {tariff_info['ai_ideas_limit']}/день\n"
            f"• {tariff_info['description']}\n\n"
        )
    
    tariffs_text += (
        f"📍 Чтобы оформить подписку:\n"
        f"1. Выберите нужный тариф ниже\n"
        f"2. Свяжитесь с администратором для оплаты\n"
        f"3. После оплаты подписка будет активирована\n\n"
        f"💬 Контакт администратора: @{ADMIN_CONTACT.replace('@', '')}"
    )
    
    await callback.message.edit_text(
        tariffs_text,
        reply_markup=get_tariffs_keyboard(user_tariff)
    )

@router.callback_query(F.data.startswith("tariff_"))
async def select_tariff(callback: CallbackQuery):
    """Выбор тарифа"""
    tariff_id = callback.data.split("_")[1]
    user_id = callback.from_user.id
    user_tariff = await get_user_tariff(user_id, DATABASE_URL)
    
    if tariff_id == user_tariff:
        await callback.answer("❌ Это ваш текущий тариф!", show_alert=True)
        return
    
    tariff_info = TARIFFS.get(tariff_id)
    if not tariff_info:
        await callback.answer("❌ Тариф не найден!", show_alert=True)
        return
    
    # Создаем заказ
    success = await create_tariff_order(user_id, tariff_id, DATABASE_URL)
    
    if success:
        await callback.message.edit_text(
            f"🛒 Запрос на смену тарифа отправлен!\n\n"
            f"📋 Детали:\n"
            f"• Новый тариф: {tariff_info['name']}\n"
            f"• Стоимость: {tariff_info['price']} {tariff_info['currency']}/месяц\n"
            f"• Ваш ID: {user_id}\n\n"
            f"📍 Дальнейшие действия:\n"
            f"1. Свяжитесь с администратором для оплаты\n"
            f"2. После оплаты подписка будет активирована\n"
            f"3. Вы получите уведомление\n\n"
            f"💬 Контакт: @{ADMIN_CONTACT.replace('@', '')}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ К тарифам", callback_data="tariffs")],
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
            ])
        )
    else:
        await callback.message.edit_text(
            "❌ Ошибка при создании заказа!\n\n"
            "Попробуйте позже или обратитесь в поддержку.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ К тарифам", callback_data="tariffs")],
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
            ])
        )

# ========== BACK HANDLERS ==========
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
    """Отмена действия"""
    await state.clear()
    user_id = callback.from_user.id
    is_admin = user_id == ADMIN_ID
    
    await callback.message.edit_text(
        "❌ Действие отменено.\n\n👇 Выберите действие:",
        reply_markup=get_main_menu(user_id, is_admin)
    )

# ========== ИСПРАВЛЕННЫЕ ADMIN HANDLERS ==========
@router.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    """Админ панель"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    # Получаем статистику
    total_users = await execute_query("SELECT COUNT(*) as count FROM users", database_url=DATABASE_URL)
    total_users = total_users[0]['count'] if total_users else 0
    
    active_users = await execute_query("SELECT COUNT(*) as count FROM users WHERE last_seen > NOW() - INTERVAL '7 days'", database_url=DATABASE_URL)
    active_users = active_users[0]['count'] if active_users else 0
    
    pending_orders = await execute_query("SELECT COUNT(*) as count FROM tariff_orders WHERE status = 'pending'", database_url=DATABASE_URL)
    pending_orders = pending_orders[0]['count'] if pending_orders else 0
    
    active_subscriptions = await execute_query("SELECT COUNT(*) as count FROM users WHERE tariff_expires >= CURRENT_DATE", database_url=DATABASE_URL)
    active_subscriptions = active_subscriptions[0]['count'] if active_subscriptions else 0
    
    ai_manager = get_ai_manager()
    system_stats = ai_manager.get_system_stats() if ai_manager else {'total_keys': 0, 'available_keys': 0, 'total_requests': 0}
    
    stats_text = (
        f"👑 АДМИН ПАНЕЛЬ\n\n"
        f"📊 Статистика бота:\n"
        f"• Всего пользователей: {total_users}\n"
        f"• Активных (7 дней): {active_users}\n"
        f"• Активных подписок: {active_subscriptions}\n"
        f"• Ожидающих заказов: {pending_orders}\n\n"
        f"🤖 AI система:\n"
        f"• Всего ключей: {system_stats['total_keys']}\n"
        f"• Доступных ключей: {system_stats['available_keys']}\n"
        f"• Всего запросов: {system_stats['total_requests']}\n\n"
        f"📍 Время сервера: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M:%S')}"
    )
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=get_admin_panel_keyboard()
    )

# ========== ADMIN USERS HANDLERS ==========
@router.callback_query(F.data == "admin_users")
async def admin_users_menu(callback: CallbackQuery):
    """Меню статистики пользователей"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📊 Статистика пользователей\n\n"
        "Выберите тип отчета:",
        reply_markup=get_admin_users_keyboard()
    )

@router.callback_query(F.data == "admin_all_users")
async def admin_all_users(callback: CallbackQuery):
    """Все пользователи"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    users = await get_all_users(DATABASE_URL)
    
    if not users:
        await callback.message.edit_text(
            "📭 Нет пользователей",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")]
            ])
        )
        return
    
    users_text = "📋 ВСЕ ПОЛЬЗОВАТЕЛИ\n\n"
    
    for i, user in enumerate(users[:20], 1):  # Показываем первые 20
        tariff_name = TARIFFS.get(user.get('tariff', 'mini'), {}).get('name', user.get('tariff'))
        created_date = user.get('created_at').strftime('%d.%m.%Y') if user.get('created_at') else 'N/A'
        
        users_text += (
            f"{i}. {user.get('first_name', 'N/A')} (@{user.get('username', 'нет')})\n"
            f"   🆔 ID: {user['id']}\n"
            f"   💎 Тариф: {tariff_name}\n"
            f"   📅 Зарегистрирован: {created_date}\n"
            f"   {'👑 АДМИН' if user.get('is_admin') else ''}\n\n"
        )
    
    users_text += f"📊 Всего пользователей: {len(users)}"
    
    if len(users_text) > 4000:
        parts = split_message(users_text)
        for i, part in enumerate(parts):
            if i == 0:
                await callback.message.edit_text(part)
            else:
                await callback.message.answer(part)
    else:
        await callback.message.edit_text(users_text)
    
    await callback.message.answer(
        "👇 Выберите действие:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")]
        ])
    )

@router.callback_query(F.data == "admin_active_users")
async def admin_active_users(callback: CallbackQuery):
    """Активные пользователи"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    active_users = await execute_query(
        "SELECT id, username, first_name, tariff, last_seen FROM users WHERE last_seen > NOW() - INTERVAL '7 days' ORDER BY last_seen DESC",
        database_url=DATABASE_URL
    )
    
    if not active_users:
        await callback.message.edit_text(
            "📭 Нет активных пользователей за последние 7 дней",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")]
            ])
        )
        return
    
    users_text = "📈 АКТИВНЫЕ ПОЛЬЗОВАТЕЛИ (7 дней)\n\n"
    
    for i, user in enumerate(active_users[:20], 1):
        tariff_name = TARIFFS.get(user.get('tariff', 'mini'), {}).get('name', user.get('tariff'))
        last_seen = user.get('last_seen').astimezone(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M') if user.get('last_seen') else 'N/A'
        
        users_text += (
            f"{i}. {user.get('first_name', 'N/A')} (@{user.get('username', 'нет')})\n"
            f"   🆔 ID: {user['id']}\n"
            f"   💎 Тариф: {tariff_name}\n"
            f"   🕐 Последний визит: {last_seen}\n\n"
        )
    
    users_text += f"📊 Всего активных: {len(active_users)}"
    
    if len(users_text) > 4000:
        parts = split_message(users_text)
        for i, part in enumerate(parts):
            if i == 0:
                await callback.message.edit_text(part)
            else:
                await callback.message.answer(part)
    else:
        await callback.message.edit_text(users_text)
    
    await callback.message.answer(
        "👇 Выберите действие:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")]
        ])
    )

@router.callback_query(F.data == "admin_subscribed_users")
async def admin_subscribed_users(callback: CallbackQuery):
    """Пользователи с подписками"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    subscribed_users = await execute_query(
        "SELECT id, username, first_name, tariff, tariff_expires FROM users WHERE tariff_expires >= CURRENT_DATE ORDER BY tariff_expires ASC",
        database_url=DATABASE_URL
    )
    
    if not subscribed_users:
        await callback.message.edit_text(
            "💰 Нет пользователей с активными подписками",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")]
            ])
        )
        return
    
    users_text = "💰 ПОЛЬЗОВАТЕЛИ С ПОДПИСКАМИ\n\n"
    
    for i, user in enumerate(subscribed_users[:20], 1):
        tariff_name = TARIFFS.get(user.get('tariff', 'mini'), {}).get('name', user.get('tariff'))
        expires_date = user.get('tariff_expires').strftime('%d.%m.%Y') if user.get('tariff_expires') else 'N/A'
        days_left = (user.get('tariff_expires') - datetime.now(MOSCOW_TZ).date()).days if user.get('tariff_expires') else 0
        
        users_text += (
            f"{i}. {user.get('first_name', 'N/A')} (@{user.get('username', 'нет')})\n"
            f"   🆔 ID: {user['id']}\n"
            f"   💎 Тариф: {tariff_name}\n"
            f"   📅 Действует до: {expires_date}\n"
            f"   ⏳ Осталось дней: {days_left}\n\n"
        )
    
    users_text += f"📊 Всего с подписками: {len(subscribed_users)}"
    
    if len(users_text) > 4000:
        parts = split_message(users_text)
        for i, part in enumerate(parts):
            if i == 0:
                await callback.message.edit_text(part)
            else:
                await callback.message.answer(part)
    else:
        await callback.message.edit_text(users_text)
    
    await callback.message.answer(
        "👇 Выберите действие:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_users")]
        ])
    )

# ========== ADMIN ORDERS HANDLERS ==========
@router.callback_query(F.data == "admin_orders")
async def admin_orders(callback: CallbackQuery):
    """Заказы тарифов"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    orders = await get_tariff_orders(status='pending', database_url=DATABASE_URL)
    
    if not orders:
        # Показываем последние завершенные заказы
        orders = await get_tariff_orders(database_url=DATABASE_URL)
        orders = orders[:10] if orders else []
        
        if not orders:
            await callback.message.edit_text(
                "🛒 Нет заказов",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin_panel")]
                ])
            )
            return
        
        orders_text = "🛒 ПОСЛЕДНИЕ ЗАКАЗЫ\n\n"
        
        for i, order in enumerate(orders[:10], 1):
            tariff_name = TARIFFS.get(order.get('tariff'), {}).get('name', order.get('tariff'))
            status_emoji = {
                'pending': '⏳',
                'completed': '✅',
                'cancelled': '❌',
                'granted_by_admin': '👑',
                'extended_by_admin': '🔄',
                'force_completed': '⚡'
            }.get(order.get('status'), '📋')
            
            orders_text += (
                f"{i}. {status_emoji} Заказ #{order['id']}\n"
                f"   👤 Пользователь: {order['user_id']}\n"
                f"   💎 Тариф: {tariff_name}\n"
                f"   📅 Дата: {order['order_date'].astimezone(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M')}\n"
                f"   📊 Статус: {order['status']}\n\n"
            )
        
        await callback.message.edit_text(
            orders_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_orders")],
                [InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin_panel")]
            ])
        )
        return
    
    await callback.message.edit_text(
        f"🛒 ОЖИДАЮЩИЕ ЗАКАЗЫ ({len(orders)})\n\n"
        f"Выберите заказ для обработки:",
        reply_markup=get_admin_orders_keyboard(orders)
    )

@router.callback_query(F.data.startswith("admin_process_order_"))
async def admin_process_order(callback: CallbackQuery, state: FSMContext):
    """Обработка конкретного заказа"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    order_id = int(callback.data.split("_")[3])
    
    # Получаем информацию о заказе
    orders = await execute_query(
        "SELECT * FROM tariff_orders WHERE id = $1",
        order_id,
        database_url=DATABASE_URL
    )
    
    if not orders:
        await callback.answer("❌ Заказ не найден!", show_alert=True)
        return
    
    order = orders[0]
    await state.update_data(order_id=order_id, target_user_id=order['user_id'], tariff_id=order['tariff'])
    
    # Получаем информацию о пользователе
    user = await get_user_by_id(order['user_id'], DATABASE_URL)
    tariff_name = TARIFFS.get(order['tariff'], {}).get('name', order['tariff'])
    
    await state.set_state(AdminStates.waiting_for_days_selection)
    
    await callback.message.edit_text(
        f"🛒 ОБРАБОТКА ЗАКАЗА #{order_id}\n\n"
        f"👤 Пользователь: {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
        f"🆔 ID: {order['user_id']}\n"
        f"💎 Запрошенный тариф: {tariff_name}\n"
        f"📅 Дата заказа: {order['order_date'].astimezone(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M')}\n\n"
        f"Выберите количество дней для подписки:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="7 дней", callback_data="admin_days_7"),
                InlineKeyboardButton(text="30 дней", callback_data="admin_days_30")
            ],
            [
                InlineKeyboardButton(text="90 дней", callback_data="admin_days_90"),
                InlineKeyboardButton(text="180 дней", callback_data="admin_days_180")
            ],
            [
                InlineKeyboardButton(text="365 дней", callback_data="admin_days_365"),
                InlineKeyboardButton(text="📝 Другое", callback_data="admin_days_custom")
            ],
            [
                InlineKeyboardButton(text="❌ Отменить заказ", callback_data="admin_cancel_order"),
                InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_orders")
            ]
        ])
    )

@router.callback_query(F.data == "admin_cancel_order")
async def admin_cancel_order(callback: CallbackQuery, state: FSMContext):
    """Отмена заказа"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    data = await state.get_data()
    order_id = data.get('order_id')
    
    if order_id:
        await update_order_status(order_id, 'cancelled', f"Отменен админом {user_id}", DATABASE_URL)
    
    await state.clear()
    await callback.message.edit_text(
        "✅ Заказ отменен",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛒 К заказам", callback_data="admin_orders")],
            [InlineKeyboardButton(text="⬅️ Админ панель", callback_data="admin_panel")]
        ])
    )

# ========== ADMIN AI STATS HANDLERS ==========
@router.callback_query(F.data == "admin_ai_stats")
async def admin_ai_stats(callback: CallbackQuery):
    """Статистика AI"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    ai_manager = get_ai_manager()
    
    if not ai_manager:
        await callback.message.edit_text(
            "❌ AI менеджер не инициализирован",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin_panel")]
            ])
        )
        return
    
    system_stats = ai_manager.get_system_stats()
    
    # Получаем статистику из логов
    ai_logs = await execute_query('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN success = TRUE THEN 1 ELSE 0 END) as successful,
            service_type,
            DATE(created_at AT TIME ZONE 'Europe/Moscow') as date
        FROM ai_request_logs 
        WHERE created_at > NOW() - INTERVAL '7 days'
        GROUP BY service_type, DATE(created_at AT TIME ZONE 'Europe/Moscow')
        ORDER BY date DESC
        LIMIT 20
    ''', database_url=DATABASE_URL)
    
    stats_text = (
        f"🤖 СТАТИСТИКА AI СИСТЕМЫ\n\n"
        f"📊 Система ключей:\n"
        f"• Всего ключей: {system_stats['total_keys']}\n"
        f"• Доступных ключей: {system_stats['available_keys']}\n"
        f"• Неудачных подряд: {system_stats['consecutive_failures']}\n"
        f"• Текущий индекс: {system_stats['current_key_index']}\n"
        f"• Всего запросов: {system_stats['total_requests']}\n\n"
        f"📈 Использование моделей:\n"
    )
    
    # Статистика по моделям
    model_stats = {}
    for log in ai_logs:
        model_name = log.get('model_name', 'unknown')
        if model_name not in model_stats:
            model_stats[model_name] = 0
        model_stats[model_name] += 1
    
    for model, count in list(model_stats.items())[:5]:
        stats_text += f"• {model}: {count} запросов\n"
    
    stats_text += f"\n🔄 Действия:\n"
    stats_text += f"• /rotate_keys - принудительная ротация ключей\n"
    stats_text += f"• /reset_failures - сброс счетчика ошибок\n"
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🔄 Ротация ключей", callback_data="admin_rotate_keys"),
                InlineKeyboardButton(text="📊 Детальная статистика", callback_data="admin_ai_detailed")
            ],
            [InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin_panel")]
        ])
    )

@router.callback_query(F.data == "admin_rotate_keys")
async def admin_rotate_keys(callback: CallbackQuery):
    """Принудительная ротация ключей"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    ai_manager = get_ai_manager()
    
    if ai_manager:
        ai_manager.force_rotate_key()
        await callback.answer("✅ Ключи принудительно ротированы!", show_alert=True)
    else:
        await callback.answer("❌ AI менеджер не инициализирован!", show_alert=True)
    
    await admin_ai_stats(callback)

@router.callback_query(F.data == "admin_ai_detailed")
async def admin_ai_detailed(callback: CallbackQuery):
    """Детальная статистика AI"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    # Получаем топ пользователей по AI запросам
    top_users = await execute_query('''
        SELECT 
            user_id,
            COUNT(*) as request_count,
            SUM(CASE WHEN success = TRUE THEN 1 ELSE 0 END) as successful,
            SUM(prompt_length) as total_prompt,
            SUM(response_length) as total_response
        FROM ai_request_logs 
        WHERE created_at > NOW() - INTERVAL '30 days'
        GROUP BY user_id
        ORDER BY request_count DESC
        LIMIT 10
    ''', database_url=DATABASE_URL)
    
    stats_text = "📊 ДЕТАЛЬНАЯ СТАТИСТИКА AI\n\n"
    stats_text += "🏆 Топ пользователей (30 дней):\n\n"
    
    for i, user in enumerate(top_users, 1):
        success_rate = (user['successful'] / user['request_count'] * 100) if user['request_count'] > 0 else 0
        stats_text += (
            f"{i}. Пользователь: {user['user_id']}\n"
            f"   • Запросов: {user['request_count']}\n"
            f"   • Успешно: {user['successful']} ({success_rate:.1f}%)\n"
            f"   • Токенов: {user['total_prompt'] + user['total_response']}\n\n"
        )
    
    # Получаем статистику по дням
    daily_stats = await execute_query('''
        SELECT 
            DATE(created_at AT TIME ZONE 'Europe/Moscow') as date,
            COUNT(*) as requests,
            COUNT(DISTINCT user_id) as users
        FROM ai_request_logs 
        WHERE created_at > NOW() - INTERVAL '7 days'
        GROUP BY DATE(created_at AT TIME ZONE 'Europe/Moscow')
        ORDER BY date DESC
    ''', database_url=DATABASE_URL)
    
    stats_text += "📅 Статистика по дням:\n\n"
    
    for day in daily_stats[:7]:
        stats_text += f"• {day['date'].strftime('%d.%m.%Y')}: {day['requests']} запросов, {day['users']} пользователей\n"
    
    if len(stats_text) > 4000:
        parts = split_message(stats_text)
        for i, part in enumerate(parts):
            if i == 0:
                await callback.message.edit_text(part)
            else:
                await callback.message.answer(part)
    else:
        await callback.message.edit_text(stats_text)
    
    await callback.message.answer(
        "👇 Выберите действие:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад к AI статистике", callback_data="admin_ai_stats")],
            [InlineKeyboardButton(text="⬅️ Админ панель", callback_data="admin_panel")]
        ])
    )

# ========== ADMIN BROADCAST HANDLERS ==========
@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    """Начало рассылки"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    await state.set_state(AdminStates.waiting_for_broadcast)
    
    await callback.message.edit_text(
        "📢 РАССЫЛКА\n\n"
        "Введите текст сообщения для рассылки:\n\n"
        "⚠️ Сообщение будет отправлено ВСЕМ пользователям бота.\n"
        "Рекомендуется использовать форматирование HTML.\n\n"
        "📍 Чтобы отменить рассылку, нажмите кнопку ниже:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel")]
        ])
    )

@router.message(AdminStates.waiting_for_broadcast)
async def admin_broadcast_process(message: Message, state: FSMContext):
    """Обработка и отправка рассылки"""
    user_id = message.from_user.id
    
    if user_id != ADMIN_ID:
        await message.answer("❌ У вас нет прав администратора!")
        await state.clear()
        return
    
    broadcast_text = message.text
    
    # Получаем всех пользователей
    users = await get_all_users(DATABASE_URL)
    
    if not users:
        await message.answer("📭 Нет пользователей для рассылки")
        await state.clear()
        return
    
    status_msg = await message.answer(f"📤 Начинаю рассылку {len(users)} пользователям...")
    
    success_count = 0
    fail_count = 0
    
    for i, user in enumerate(users):
        try:
            await bot.send_message(
                user['id'],
                f"📢 РАССЫЛКА ОТ АДМИНИСТРАТОРА\n\n{broadcast_text}",
                parse_mode="HTML"
            )
            success_count += 1
        except Exception as e:
            fail_count += 1
            logger.error(f"Ошибка отправки рассылки пользователю {user['id']}: {e}")
        
        # Обновляем статус каждые 10 пользователей
        if (i + 1) % 10 == 0:
            await status_msg.edit_text(f"📤 Рассылка: {i + 1}/{len(users)}...")
    
    await status_msg.edit_text(
        f"✅ Рассылка завершена!\n\n"
        f"📊 Результаты:\n"
        f"• Успешно: {success_count}\n"
        f"• Ошибок: {fail_count}\n"
        f"• Всего: {len(users)}"
    )
    
    await state.clear()

# ========== ADMIN FORCE TARIFF HANDLERS ==========
@router.callback_query(F.data == "admin_force_tariff")
async def admin_force_tariff_start(callback: CallbackQuery, state: FSMContext):
    """Принудительное обновление тарифа"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    await state.set_state(AdminStates.waiting_for_force_user_id)
    
    await callback.message.edit_text(
        "🔄 ПРИНУДИТЕЛЬНОЕ ОБНОВЛЕНИЕ ТАРИФА\n\n"
        "Введите ID пользователя:\n\n"
        "📍 Эта операция изменит тариф пользователя без привязки к подписке.\n"
        "Рекомендуется использовать для тестирования или особых случаев.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel")]
        ])
    )

@router.message(AdminStates.waiting_for_force_user_id)
async def admin_force_tariff_user_id(message: Message, state: FSMContext):
    """Обработка ID пользователя для принудительного обновления"""
    try:
        target_user_id = int(message.text.strip())
        
        # Проверяем существование пользователя
        user = await get_user_by_id(target_user_id, DATABASE_URL)
        if not user:
            await message.answer(
                "❌ Пользователь с таким ID не найден!\n\n"
                "Проверьте ID и попробуйте снова:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel")]
                ])
            )
            return
        
        await state.update_data(target_user_id=target_user_id)
        await state.set_state(AdminStates.waiting_for_force_tariff)
        
        await message.answer(
            f"✅ Пользователь найден:\n"
            f"👤 ID: {target_user_id}\n"
            f"📛 Имя: {user.get('first_name', 'N/A')}\n"
            f"👤 Username: @{user.get('username', 'N/A')}\n"
            f"💎 Текущий тариф: {user.get('tariff', 'mini')}\n\n"
            f"Выберите новый тариф:",
            reply_markup=get_force_tariff_keyboard()
        )
    except ValueError:
        await message.answer(
            "❌ ID пользователя должен быть числом!\n\n"
            "Введите ID пользователя:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel")]
            ])
        )

@router.callback_query(F.data.startswith("force_tariff_"))
async def admin_force_tariff_select(callback: CallbackQuery, state: FSMContext):
    """Выбор тарифа для принудительного обновления"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    tariff_id = callback.data.split("_")[2]
    data = await state.get_data()
    target_user_id = data.get('target_user_id')
    
    # Принудительно обновляем тариф
    success, message_text = await force_update_user_tariff(target_user_id, tariff_id, user_id, DATABASE_URL)
    
    if success:
        # Отправляем уведомление пользователю
        try:
            tariff_name = TARIFFS.get(tariff_id, {}).get('name', tariff_id)
            await bot.send_message(
                target_user_id,
                f"⚡ ВАШ ТАРИФ ИЗМЕНЕН АДМИНИСТРАТОРОМ!\n\n"
                f"💎 Новый тариф: {tariff_name}\n"
                f"🆔 Ваш ID: {target_user_id}\n\n"
                f"📍 Изменение выполнено администратором.\n"
                f"Подробности можно узнать в поддержке."
            )
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление пользователю {target_user_id}: {e}")
    
    await callback.message.edit_text(message_text)
    await state.clear()

# ========== ADMIN SUBSCRIPTION HANDLERS ==========
@router.callback_query(F.data == "admin_subscriptions")
async def admin_subscriptions_menu(callback: CallbackQuery):
    """Меню управления подписками"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "💎 Управление подписками\n\n"
        "Выберите действие:",
        reply_markup=get_admin_subscription_keyboard()
    )

@router.callback_query(F.data == "admin_grant_subscription")
async def admin_grant_subscription_start(callback: CallbackQuery, state: FSMContext):
    """Начало выдачи подписки"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    await state.set_state(AdminStates.waiting_for_user_id)
    await state.update_data(action="grant")
    
    await callback.message.edit_text(
        "👤 Введите ID пользователя для выдачи подписки:\n\n"
        "📍 ID можно получить:\n"
        "• Из статистики пользователей\n"
        "• Из заказов тарифов\n"
        "• Попросить пользователя отправить /start\n\n"
        "Введите ID пользователя:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_subscriptions")]
        ])
    )

@router.callback_query(F.data == "admin_extend_subscription")
async def admin_extend_subscription_start(callback: CallbackQuery, state: FSMContext):
    """Начало продления подписки"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    await state.set_state(AdminStates.waiting_for_user_id)
    await state.update_data(action="extend")
    
    await callback.message.edit_text(
        "👤 Введите ID пользователя для продления подписки:\n\n"
        "📍 ID можно получить:\n"
        "• Из статистики пользователей\n"
        "• Из заказов тарифов\n"
        "• Попросить пользователя отправить /start\n\n"
        "Введите ID пользователя:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_subscriptions")]
        ])
    )

@router.message(AdminStates.waiting_for_user_id)
async def admin_process_user_id(message: Message, state: FSMContext):
    """Обработка ID пользователя"""
    try:
        target_user_id = int(message.text.strip())
        data = await state.get_data()
        action = data.get('action')
        
        # Проверяем существование пользователя
        user = await get_user_by_id(target_user_id, DATABASE_URL)
        if not user:
            await message.answer(
                "❌ Пользователь с таким ID не найден!\n\n"
                "Проверьте ID и попробуйте снова:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_subscriptions")]
                ])
            )
            return
        
        await state.update_data(target_user_id=target_user_id)
        
        if action == "grant":
            await state.set_state(AdminStates.waiting_for_tariff_selection)
            await message.answer(
                f"✅ Пользователь найден:\n"
                f"👤 ID: {target_user_id}\n"
                f"📛 Имя: {user.get('first_name', 'N/A')}\n"
                f"👤 Username: @{user.get('username', 'N/A')}\n"
                f"💎 Текущий тариф: {user.get('tariff', 'mini')}\n\n"
                f"Выберите тариф для выдачи:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="⭐ Standard", callback_data="admin_tariff_standard"),
                        InlineKeyboardButton(text="👑 VIP", callback_data="admin_tariff_vip")
                    ],
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_subscriptions")]
                ])
            )
        elif action == "extend":
            # Получаем информацию о текущей подписке
            subscription_info = await get_user_subscription_info(target_user_id, DATABASE_URL)
            
            if subscription_info.get('expired') and not subscription_info.get('expires'):
                await message.answer(
                    f"❌ У пользователя нет активной подписки!\n\n"
                    f"👤 Пользователь: {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
                    f"💎 Текущий тариф: {user.get('tariff', 'mini')}\n\n"
                    f"Используйте функцию 'Выдать подписку' для нового пользователя.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_subscriptions")]
                    ])
                )
                await state.clear()
                return
            
            await state.set_state(AdminStates.waiting_for_days_selection)
            
            expires_text = "Нет подписки"
            if subscription_info.get('expires'):
                expires_date = subscription_info['expires']
                expires_text = expires_date.strftime('%d.%m.%Y')
                if subscription_info.get('expired'):
                    expires_text += " (истекла)"
                else:
                    expires_text += f" (осталось {subscription_info.get('days_left', 0)} дней)"
            
            await message.answer(
                f"✅ Пользователь найден:\n"
                f"👤 ID: {target_user_id}\n"
                f"📛 Имя: {user.get('first_name', 'N/A')}\n"
                f"👤 Username: @{user.get('username', 'N/A')}\n"
                f"💎 Текущий тариф: {user.get('tariff', 'mini')}\n"
                f"📅 Подписка до: {expires_text}\n\n"
                f"Выберите количество дней для продления:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="7 дней", callback_data="admin_days_7"),
                        InlineKeyboardButton(text="30 дней", callback_data="admin_days_30")
                    ],
                    [
                        InlineKeyboardButton(text="90 дней", callback_data="admin_days_90"),
                        InlineKeyboardButton(text="180 дней", callback_data="admin_days_180")
                    ],
                    [
                        InlineKeyboardButton(text="365 дней", callback_data="admin_days_365"),
                        InlineKeyboardButton(text="📝 Другое", callback_data="admin_days_custom")
                    ],
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_subscriptions")]
                ])
            )
        
    except ValueError:
        await message.answer(
            "❌ ID пользователя должен быть числом!\n\n"
            "Введите ID пользователя:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_subscriptions")]
            ])
        )

@router.callback_query(F.data.startswith("admin_tariff_"))
async def admin_process_tariff_selection(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора тарифа"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    tariff_id = callback.data.split("_")[2]
    data = await state.get_data()
    target_user_id = data.get('target_user_id')
    
    # Преобразуем ID тарифа
    tariff_map = {
        'standard': 'standard',
        'vip': 'vip'
    }
    tariff_id = tariff_map.get(tariff_id, tariff_id)
    
    await state.update_data(tariff_id=tariff_id)
    
    # Получаем информацию о пользователе
    user = await get_user_by_id(target_user_id, DATABASE_URL)
    
    await state.set_state(AdminStates.waiting_for_days_selection)
    
    await callback.message.edit_text(
        f"✅ Выбран тариф: {TARIFFS.get(tariff_id, {}).get('name', tariff_id)}\n\n"
        f"👤 Пользователь: {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
        f"💎 Текущий тариф: {user.get('tariff', 'mini')}\n\n"
        f"Выберите количество дней для подписки:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="7 дней", callback_data="admin_days_7"),
                InlineKeyboardButton(text="30 дней", callback_data="admin_days_30")
            ],
            [
                InlineKeyboardButton(text="90 дней", callback_data="admin_days_90"),
                InlineKeyboardButton(text="180 дней", callback_data="admin_days_180")
            ],
            [
                InlineKeyboardButton(text="365 дней", callback_data="admin_days_365"),
                InlineKeyboardButton(text="📝 Другое", callback_data="admin_days_custom")
            ],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_subscriptions")]
        ])
    )

@router.callback_query(F.data.startswith("admin_days_"))
async def admin_process_days_selection(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора количества дней"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    if callback.data == "admin_days_custom":
        await callback.message.edit_text(
            "📝 Введите количество дней для подписки (от 1 до 365):\n\n"
            "Примеры:\n"
            "• 30 - 1 месяц\n"
            "• 90 - 3 месяца\n"
            "• 180 - 6 месяцев\n"
            "• 365 - 1 год",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_subscriptions")]
            ])
        )
        return
    
    try:
        days = int(callback.data.split("_")[2])
        await state.update_data(days=days)
        
        data = await state.get_data()
        action = data.get('action')
        target_user_id = data.get('target_user_id')
        tariff_id = data.get('tariff_id')
        
        user = await get_user_by_id(target_user_id, DATABASE_URL)
        
        if action == "grant":
            tariff_name = TARIFFS.get(tariff_id, {}).get('name', tariff_id)
            await state.set_state(AdminStates.waiting_for_confirm_grant)
            
            await callback.message.edit_text(
                f"📋 ПОДТВЕРЖДЕНИЕ ВЫДАЧИ ПОДПИСКИ\n\n"
                f"👤 Пользователь: {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
                f"🆔 ID: {target_user_id}\n"
                f"💎 Тариф: {tariff_name}\n"
                f"📅 Срок: {days} дней\n"
                f"💰 Стоимость: ${TARIFFS.get(tariff_id, {}).get('price', 0)}/месяц\n\n"
                f"📍 После подтверждения:\n"
                f"• Пользователь получит уведомление\n"
                f"• Подписка будет активирована\n"
                f"• Тариф будет обновлен\n\n"
                f"Выдать подписку?",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✅ Да, выдать", callback_data="admin_confirm_grant"),
                        InlineKeyboardButton(text="❌ Нет, отмена", callback_data="admin_subscriptions")
                    ]
                ])
            )
        elif action == "extend":
            subscription_info = await get_user_subscription_info(target_user_id, DATABASE_URL)
            current_tariff = user.get('tariff', 'mini')
            tariff_name = TARIFFS.get(current_tariff, {}).get('name', current_tariff)
            
            await state.set_state(AdminStates.waiting_for_confirm_extend)
            
            expires_text = "Нет подписки"
            new_expires = None
            
            if subscription_info.get('expires'):
                expires_date = subscription_info['expires']
                if expires_date >= datetime.now(MOSCOW_TZ).date():
                    new_expires = expires_date + timedelta(days=days)
                else:
                    new_expires = datetime.now(MOSCOW_TZ).date() + timedelta(days=days)
                expires_text = expires_date.strftime('%d.%m.%Y')
            else:
                new_expires = datetime.now(MOSCOW_TZ).date() + timedelta(days=days)
            
            await callback.message.edit_text(
                f"📋 ПОДТВЕРЖДЕНИЕ ПРОДЛЕНИЯ ПОДПИСКИ\n\n"
                f"👤 Пользователь: {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
                f"🆔 ID: {target_user_id}\n"
                f"💎 Тариф: {tariff_name}\n"
                f"📅 Текущая подписка до: {expires_text}\n"
                f"📅 Новый срок: {days} дней\n"
                f"📅 Новая дата окончания: {new_expires.strftime('%d.%m.%Y') if new_expires else 'N/A'}\n\n"
                f"📍 После подтверждения:\n"
                f"• Пользователь получит уведомление\n"
                f"• Подписка будет продлена\n"
                f"• Счетчик дней увеличится\n\n"
                f"Продлить подписку?",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✅ Да, продлить", callback_data="admin_confirm_extend"),
                        InlineKeyboardButton(text="❌ Нет, отмена", callback_data="admin_subscriptions")
                    ]
                ])
            )
        
    except ValueError:
        await callback.answer("❌ Ошибка в количестве дней", show_alert=True)

@router.message(AdminStates.waiting_for_days_selection)
async def admin_process_custom_days(message: Message, state: FSMContext):
    """Обработка пользовательского количества дней"""
    try:
        days = int(message.text.strip())
        if days < 1 or days > 365:
            await message.answer(
                "❌ Количество дней должно быть от 1 до 365!\n\n"
                "Введите количество дней еще раз:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_subscriptions")]
                ])
            )
            return
        
        await state.update_data(days=days)
        
        data = await state.get_data()
        action = data.get('action')
        target_user_id = data.get('target_user_id')
        tariff_id = data.get('tariff_id')
        
        user = await get_user_by_id(target_user_id, DATABASE_URL)
        
        if action == "grant":
            tariff_name = TARIFFS.get(tariff_id, {}).get('name', tariff_id)
            await state.set_state(AdminStates.waiting_for_confirm_grant)
            
            await message.answer(
                f"📋 ПОДТВЕРЖДЕНИЕ ВЫДАЧИ ПОДПИСКИ\n\n"
                f"👤 Пользователь: {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
                f"🆔 ID: {target_user_id}\n"
                f"💎 Тариф: {tariff_name}\n"
                f"📅 Срок: {days} дней\n"
                f"💰 Стоимость: ${TARIFFS.get(tariff_id, {}).get('price', 0)}/месяц\n\n"
                f"📍 После подтверждения:\n"
                f"• Пользователь получит уведомление\n"
                f"• Подписка будет активирована\n"
                f"• Тариф будет обновлен\n\n"
                f"Выдать подписку?",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✅ Да, выдать", callback_data="admin_confirm_grant"),
                        InlineKeyboardButton(text="❌ Нет, отмена", callback_data="admin_subscriptions")
                    ]
                ])
            )
        elif action == "extend":
            subscription_info = await get_user_subscription_info(target_user_id, DATABASE_URL)
            current_tariff = user.get('tariff', 'mini')
            tariff_name = TARIFFS.get(current_tariff, {}).get('name', current_tariff)
            
            await state.set_state(AdminStates.waiting_for_confirm_extend)
            
            expires_text = "Нет подписки"
            new_expires = None
            
            if subscription_info.get('expires'):
                expires_date = subscription_info['expires']
                if expires_date >= datetime.now(MOSCOW_TZ).date():
                    new_expires = expires_date + timedelta(days=days)
                else:
                    new_expires = datetime.now(MOSCOW_TZ).date() + timedelta(days=days)
                expires_text = expires_date.strftime('%d.%m.%Y')
            else:
                new_expires = datetime.now(MOSCOW_TZ).date() + timedelta(days=days)
            
            await message.answer(
                f"📋 ПОДТВЕРЖДЕНИЕ ПРОДЛЕНИЯ ПОДПИСКИ\n\n"
                f"👤 Пользователь: {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
                f"🆔 ID: {target_user_id}\n"
                f"💎 Тариф: {tariff_name}\n"
                f"📅 Текущая подписка до: {expires_text}\n"
                f"📅 Новый срок: {days} дней\n"
                f"📅 Новая дата окончания: {new_expires.strftime('%d.%m.%Y') if new_expires else 'N/A'}\n\n"
                f"📍 После подтверждения:\n"
                f"• Пользователь получит уведомление\n"
                f"• Подписка будет продлена\n"
                f"• Счетчик дней увеличится\n\n"
                f"Продлить подписку?",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✅ Да, продлить", callback_data="admin_confirm_extend"),
                        InlineKeyboardButton(text="❌ Нет, отмена", callback_data="admin_subscriptions")
                    ]
                ])
            )
        
    except ValueError:
        await message.answer(
            "❌ Введите число!\n\nПример: 30, 90, 180",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_subscriptions")]
            ])
        )

@router.callback_query(F.data == "admin_confirm_grant")
async def admin_confirm_grant(callback: CallbackQuery, state: FSMContext):
    """Подтверждение выдачи подписки"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    data = await state.get_data()
    target_user_id = data.get('target_user_id')
    tariff_id = data.get('tariff_id')
    days = data.get('days')
    
    # Обновляем подписку пользователя
    success = await update_user_subscription(target_user_id, tariff_id, days, DATABASE_URL)
    
    if success:
        # Получаем информацию о пользователе
        user = await get_user_by_id(target_user_id, DATABASE_URL)
        tariff_name = TARIFFS.get(tariff_id, {}).get('name', tariff_id)
        
        # Отправляем уведомление пользователю
        try:
            await bot.send_message(
                target_user_id,
                f"🎉 ВАМ ВЫДАНА ПОДПИСКА!\n\n"
                f"💎 Тариф: {tariff_name}\n"
                f"📅 Срок: {days} дней\n"
                f"🆔 Ваш ID: {target_user_id}\n\n"
                f"📍 Подписка активна с сегодняшнего дня.\n"
                f"Вы можете проверить статус в разделе 'Моя статистика'.\n\n"
                f"Спасибо за использование KOLES-TECH! 🤖"
            )
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление пользователю {target_user_id}: {e}")
        
        # Обновляем статус заказа, если он был
        order_id = data.get('order_id')
        if order_id:
            await update_order_status(order_id, 'completed', f"Выдано админом {user_id} на {days} дней", DATABASE_URL)
        else:
            # Создаем запись о заказе
            await execute_query('''
                INSERT INTO tariff_orders (user_id, tariff, status, admin_notes)
                VALUES ($1, $2, 'granted_by_admin', $3)
            ''', target_user_id, tariff_id, f"Выдано админом {user_id} на {days} дней", database_url=DATABASE_URL)
        
        await callback.message.edit_text(
            f"✅ Подписка успешно выдана!\n\n"
            f"📋 Детали:\n"
            f"👤 Пользователь: {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
            f"🆔 ID: {target_user_id}\n"
            f"💎 Тариф: {tariff_name}\n"
            f"📅 Срок: {days} дней\n"
            f"👑 Выдал: админ {user_id}\n"
            f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Управление подписками", callback_data="admin_subscriptions")],
                [InlineKeyboardButton(text="⬅️ Админ панель", callback_data="admin_panel")]
            ])
        )
    else:
        await callback.message.edit_text(
            "❌ Ошибка при выдаче подписки!\n\n"
            "Попробуйте позже или обратитесь к разработчику.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Управление подписками", callback_data="admin_subscriptions")],
                [InlineKeyboardButton(text="⬅️ Админ панель", callback_data="admin_panel")]
            ])
        )
    
    await state.clear()

@router.callback_query(F.data == "admin_confirm_extend")
async def admin_confirm_extend(callback: CallbackQuery, state: FSMContext):
    """Подтверждение продления подписки"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    data = await state.get_data()
    target_user_id = data.get('target_user_id')
    days = data.get('days')
    
    # Получаем текущий тариф пользователя
    user = await get_user_by_id(target_user_id, DATABASE_URL)
    current_tariff = user.get('tariff', 'mini')
    
    # Обновляем подписку пользователя
    success = await update_user_subscription(target_user_id, current_tariff, days, DATABASE_URL)
    
    if success:
        tariff_name = TARIFFS.get(current_tariff, {}).get('name', current_tariff)
        
        # Получаем обновленную информацию о подписке
        subscription_info = await get_user_subscription_info(target_user_id, DATABASE_URL)
        
        # Отправляем уведомление пользователю
        try:
            await bot.send_message(
                target_user_id,
                f"🎉 ВАША ПОДПИСКА ПРОДЛЕНА!\n\n"
                f"💎 Тариф: {tariff_name}\n"
                f"📅 Добавлено дней: {days}\n"
                f"📅 Новая дата окончания: {subscription_info['expires'].strftime('%d.%m.%Y') if subscription_info.get('expires') else 'N/A'}\n"
                f"🆔 Ваш ID: {target_user_id}\n\n"
                f"📍 Подписка успешно продлена.\n"
                f"Вы можете проверить статус в разделе 'Моя статистика'.\n\n"
                f"Спасибо за использование KOLES-TECH! 🤖"
            )
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление пользователю {target_user_id}: {e}")
        
        # Обновляем статус заказа, если он был
        order_id = data.get('order_id')
        if order_id:
            await update_order_status(order_id, 'completed', f"Продлено админом {user_id} на {days} дней", DATABASE_URL)
        else:
            # Создаем запись о заказе
            await execute_query('''
                INSERT INTO tariff_orders (user_id, tariff, status, admin_notes)
                VALUES ($1, $2, 'extended_by_admin', $3)
            ''', target_user_id, current_tariff, f"Продлено админом {user_id} на {days} дней", database_url=DATABASE_URL)
        
        expires_text = subscription_info['expires'].strftime('%d.%m.%Y') if subscription_info.get('expires') else 'N/A'
        
        await callback.message.edit_text(
            f"✅ Подписка успешно продлена!\n\n"
            f"📋 Детали:\n"
            f"👤 Пользователь: {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
            f"🆔 ID: {target_user_id}\n"
            f"💎 Тариф: {tariff_name}\n"
            f"📅 Добавлено дней: {days}\n"
            f"📅 Новая дата окончания: {expires_text}\n"
            f"👑 Продлил: админ {user_id}\n"
            f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Управление подписками", callback_data="admin_subscriptions")],
                [InlineKeyboardButton(text="⬅️ Админ панель", callback_data="admin_panel")]
            ])
        )
    else:
        await callback.message.edit_text(
            "❌ Ошибка при продлении подписки!\n\n"
            "Попробуйте позже или обратитесь к разработчику.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Управление подписками", callback_data="admin_subscriptions")],
                [InlineKeyboardButton(text="⬅️ Админ панель", callback_data="admin_panel")]
            ])
        )
    
    await state.clear()

@router.callback_query(F.data == "admin_list_subscriptions")
async def admin_list_subscriptions(callback: CallbackQuery):
    """Список активных подписок"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    # Получаем активные подписки
    subscriptions = await execute_query('''
        SELECT id, username, first_name, tariff, tariff_expires, subscription_days
        FROM users 
        WHERE tariff_expires >= CURRENT_DATE
        ORDER BY tariff_expires ASC
    ''', database_url=DATABASE_URL)
    
    if not subscriptions:
        await callback.message.edit_text(
            "📭 Нет активных подписок",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_subscriptions")]
            ])
        )
        return
    
    subscriptions_text = "📋 АКТИВНЫЕ ПОДПИСКИ\n\n"
    
    for i, sub in enumerate(subscriptions, 1):
        expires_date = sub['tariff_expires']
        days_left = (expires_date - datetime.now(MOSCOW_TZ).date()).days
        tariff_name = TARIFFS.get(sub['tariff'], {}).get('name', sub['tariff'])
        
        subscriptions_text += (
            f"{i}. {sub['first_name']} (@{sub['username'] or 'нет'})\n"
            f"   🆔 ID: {sub['id']}\n"
            f"   💎 Тариф: {tariff_name}\n"
            f"   📅 До: {expires_date.strftime('%d.%m.%Y')}\n"
            f"   ⏳ Осталось: {days_left} дней\n"
            f"   📊 Всего дней: {sub['subscription_days']}\n\n"
        )
    
    total_active = len(subscriptions)
    total_days = sum(sub['subscription_days'] for sub in subscriptions)
    
    subscriptions_text += f"📊 Итого: {total_active} активных подписок, {total_days} дней всего"
    
    # Разбиваем длинное сообщение
    if len(subscriptions_text) > 4000:
        parts = split_message(subscriptions_text)
        for i, part in enumerate(parts):
            if i == 0:
                await callback.message.edit_text(part)
            else:
                await callback.message.answer(part)
    else:
        await callback.message.edit_text(subscriptions_text)
    
    await callback.message.answer(
        "👇 Выберите действие:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить список", callback_data="admin_list_subscriptions")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_subscriptions")]
        ])
    )

# ========== STARTUP/SHUTDOWN ==========
async def start_web_server():
    """Запуск веб-сервера для Railway"""
    try:
        from aiohttp import web
        
        async def health_check(request):
            return web.Response(text="OK", status=200)
        
        app = web.Application()
        app.router.add_get('/', health_check)
        app.router.add_get('/health', health_check)
        
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        await site.start()
        
        logger.info(f"🌐 Веб-сервер запущен на порту {PORT}")
        return runner
    except Exception as e:
        logger.error(f"❌ Ошибка запуска веб-сервера: {e}")
        return None

async def on_startup():
    """Запуск бота"""
    logger.info("=" * 60)
    logger.info(f"🚀 ЗАПУСК БОТА KOLES-TECH v3.0 (МОДУЛЬНАЯ ВЕРСИЯ)")
    logger.info(f"🤖 AI сервисы: ВКЛЮЧЕНЫ")
    logger.info(f"🔑 Gemini ключей: {len(GEMINI_API_KEYS)}")
    logger.info(f"👑 Admin ID: {ADMIN_ID}")
    logger.info(f"🆘 Поддержка: {SUPPORT_BOT_USERNAME or SUPPORT_URL}")
    logger.info(f"🌐 Порт Railway: {PORT}")
    logger.info("=" * 60)
    
    try:
        # Инициализация базы данных
        await init_database(DATABASE_URL)
        await migrate_database(DATABASE_URL)
        
        # Получаем AI менеджер
        ai_manager = get_ai_manager()
        if ai_manager:
            ai_manager.init_keys(GEMINI_API_KEYS)
            logger.info("✅ AI менеджер инициализирован с ключами")
        
        # Запуск планировщика
        scheduler.start()
        
        # Ежедневные задачи
        scheduler.add_job(
            reset_daily_limits_task,
            trigger='cron',
            hour=0,
            minute=1,
            timezone=MOSCOW_TZ,
            id='reset_daily_limits'
        )
        
        # Очистка сессий раз в день
        scheduler.add_job(
            cleanup_old_sessions_task,
            trigger='cron',
            hour=3,
            minute=0,
            timezone=MOSCOW_TZ,
            id='cleanup_sessions'
        )
        
        # Автоматическая ротация ключей каждые 15 минут
        scheduler.add_job(
            auto_rotate_keys_task,
            trigger='interval',
            minutes=15,
            id='auto_rotate_keys'
        )
        
        # Восстановление запланированных постов
        await restore_scheduled_posts(scheduler, send_scheduled_post, bot, logger, MOSCOW_TZ, DATABASE_URL)
        
        # Проверка планировщика каждые 30 минут
        scheduler.add_job(
            check_scheduler_status,
            trigger='interval',
            minutes=30,
            id='check_scheduler'
        )
        
        # Получаем информацию о боте
        me = await bot.get_me()
        logger.info(f"✅ Бот @{me.username} запущен (ID: {me.id})")
        
        # Уведомление админа
        if ADMIN_ID:
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"🤖 Бот @{me.username} успешно запущен!\n\n"
                    f"🆔 ID: {me.id}\n"
                    f"🤖 AI сервисы: ВКЛЮЧЕНЫ\n"
                    f"🔑 Gemini ключей: {len(GEMINI_API_KEYS)}\n"
                    f"🔄 Система ротации ключей: АКТИВНА\n"
                    f"💎 Система подписок: АКТИВНА\n"
                    f"📅 Система планирования постов: АКТИВНА\n"
                    f"🌐 Порт Railway: {PORT}\n"
                    f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M:%S')}"
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить админа: {e}")
        
        logger.info("=" * 60)
        logger.info("🎉 БОТ УСПЕШНО ЗАПУЩЕН С МОДУЛЬНОЙ АРХИТЕКТУРОЙ!")
        logger.info("=" * 60)
        return True
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА ПРИ ЗАПУСКЕ: {e}")
        traceback.print_exc()
        return False

async def on_shutdown():
    """Выключение бота"""
    logger.info("🛑 Выключение бота...")
    
    # Останавливаем планировщик
    if scheduler.running:
        scheduler.shutdown()
    
    # Закрываем пул соединений
    await DatabasePool.close_pool()
    
    logger.info("👋 Бот выключен")

async def reset_daily_limits_task():
    """Ежедневный сброс лимитов"""
    from database import execute_query
    
    try:
        # Сбрасываем счетчики постов
        await execute_query('''
            UPDATE users 
            SET posts_today = 0, posts_reset_date = CURRENT_DATE 
            WHERE posts_reset_date < CURRENT_DATE
        ''', database_url=DATABASE_URL)
        
        # Сбрасываем AI лимиты
        ai_manager = get_ai_manager()
        if ai_manager:
            ai_manager.reset_daily_limits()
        
        # Проверяем истекшие подписки
        expired_subscriptions = await execute_query('''
            SELECT id, first_name, username 
            FROM users 
            WHERE tariff_expires < CURRENT_DATE AND tariff != 'mini'
        ''', database_url=DATABASE_URL)
        
        for user in expired_subscriptions:
            # Понижаем тариф до минимума
            await execute_query('''
                UPDATE users 
                SET tariff = 'mini' 
                WHERE id = $1 AND tariff != 'admin'
            ''', user['id'], database_url=DATABASE_URL)
            
            # Отправляем уведомление пользователю
            try:
                await bot.send_message(
                    user['id'],
                    f"⚠️ ВАША ПОДПИСКА ИСТЕКЛА\n\n"
                    f"📅 Дата окончания подписки наступила.\n"
                    f"💎 Ваш тариф изменен на Mini.\n\n"
                    f"📍 Для продления подписки:\n"
                    f"1. Перейдите в раздел 'Тарифы'\n"
                    f"2. Выберите нужный тариф\n"
                    f"3. Свяжитесь с администратором\n\n"
                    f"💬 Контакт: @{ADMIN_CONTACT.replace('@', '')}"
                )
            except Exception:
                pass
        
        logger.info("✅ Ежедневные лимиты сброшены")
    except Exception as e:
        logger.error(f"Ошибка сброса лимитов: {e}")

async def cleanup_old_sessions_task():
    """Очистка старых сессий"""
    from ai_service import get_ai_manager
    
    try:
        ai_manager = get_ai_manager()
        if not ai_manager:
            return
            
        week_ago = datetime.now(MOSCOW_TZ) - timedelta(days=7)
        users_to_remove = []
        
        for user_id, session in list(ai_manager.sessions.items()):
            if session['total_requests'] == 0:
                last_activity = await execute_query(
                    "SELECT last_seen FROM users WHERE id = $1",
                    user_id,
                    database_url=DATABASE_URL
                )
                if last_activity:
                    last_seen = last_activity[0].get('last_seen')
                    if last_seen and last_seen.replace(tzinfo=pytz.UTC).astimezone(MOSCOW_TZ) < week_ago:
                        users_to_remove.append(user_id)
        
        for user_id in users_to_remove:
            if user_id in ai_manager.sessions:
                del ai_manager.sessions[user_id]
        
        if users_to_remove:
            logger.info(f"✅ Очищено {len(users_to_remove)} неактивных сессий")
    except Exception as e:
        logger.error(f"Ошибка очистки сессий: {e}")

async def auto_rotate_keys_task():
    """Автоматическая ротация ключей"""
    from ai_service import get_ai_manager
    
    try:
        ai_manager = get_ai_manager()
        if ai_manager:
            ai_manager.check_and_rotate_keys()
            logger.info("✅ Автоматическая ротация ключей выполнена")
    except Exception as e:
        logger.error(f"Ошибка автоматической ротации ключей: {e}")

async def check_scheduler_status():
    """Проверка статуса планировщика"""
    try:
        jobs = scheduler.get_jobs()
        scheduled_posts = await execute_query(
            "SELECT COUNT(*) as count FROM scheduled_posts WHERE is_sent = FALSE",
            database_url=DATABASE_URL
        )
        scheduled_count = scheduled_posts[0]['count'] if scheduled_posts else 0
        
        logger.info(f"📊 Статус планировщика: {len(jobs)} задач, {scheduled_count} постов в очереди")
    except Exception as e:
        logger.error(f"Ошибка проверки статуса планировщика: {e}")

# ========== MAIN ==========
async def main():
    """Основная функция"""
    if not API_TOKEN or not DATABASE_URL:
        logger.error("❌ Отсутствуют обязательные переменные окружения")
        return
    
    if not GEMINI_API_KEYS:
        logger.error("❌ Отсутствуют Gemini API ключи")
        return
    
    if not await on_startup():
        logger.error("❌ Не удалось запустить бота")
        return
    
    # Запускаем веб-сервер для Railway
    web_runner = await start_web_server()
    
    try:
        # Запускаем поллинг бота
        await dp.start_polling(bot, skip_updates=True)
    except KeyboardInterrupt:
        logger.info("⚠️ Получен сигнал прерывания")
    except Exception as e:
        logger.error(f"💥 КРИТИЧЕСКАЯ ОШИБКА: {e}")
        logger.error(traceback.format_exc())
    finally:
        # Останавливаем веб-сервер
        if web_runner:
            await web_runner.cleanup()
        await on_shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Бот остановлен")
    except Exception as e:
        print(f"💥 Фатальная ошибка: {e}")
        traceback.print_exc()
        sys.exit(1)
