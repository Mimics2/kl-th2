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
SUPPORT_BOT_USERNAME = os.getenv("SUPPORT_BOT_USERNAME", "")
ADMIN_CONTACT = os.getenv("ADMIN_CONTACT", "koles_tech_support")
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
    waiting_for_force_tariff_user = State()
    waiting_for_force_tariff_select = State()

# ========== ОБНОВЛЕННЫЕ КЛАВИАТУРЫ СО СТИКЕРАМИ ==========
def get_main_menu(user_id: int, is_admin: bool = False) -> InlineKeyboardMarkup:
    """Главное меню с обновленным дизайном и стикерами"""
    buttons = [
        [InlineKeyboardButton(text="🤖✨ ИИ-копирайтер", callback_data="ai_copywriter")],
        [InlineKeyboardButton(text="💡🎯 Генератор идей", callback_data="ai_ideas")],
        [InlineKeyboardButton(text="📅🗓 Запланировать пост", callback_data="schedule_post")],
        [InlineKeyboardButton(text="📊📈 Моя статистика", callback_data="my_stats")],
        [InlineKeyboardButton(text="📢🔔 Мои каналы", callback_data="my_channels")],
        [InlineKeyboardButton(text="💎💰 Тарифы", callback_data="tariffs")]
    ]
    
    if SUPPORT_BOT_USERNAME:
        buttons.append([InlineKeyboardButton(text="🆘🤝 Техподдержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME.replace('@', '')}")])
    else:
        buttons.append([InlineKeyboardButton(text="🆘🤝 Поддержка", url=SUPPORT_URL)])
    
    if is_admin:
        buttons.append([InlineKeyboardButton(text="👑⚙️ Админ-панель", callback_data="admin_panel")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_panel_keyboard() -> InlineKeyboardMarkup:
    """Обновленная админ-панель"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊👥 Статистика пользователей", callback_data="admin_users")],
        [InlineKeyboardButton(text="🛒📦 Заказы тарифов", callback_data="admin_orders")],
        [InlineKeyboardButton(text="🤖📊 Статистика AI", callback_data="admin_ai_stats")],
        [InlineKeyboardButton(text="📢📨 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="💎🎁 Управление подписками", callback_data="admin_subscriptions")],
        [InlineKeyboardButton(text="🔄👑 Принудительный тариф", callback_data="admin_force_tariff")],
        [InlineKeyboardButton(text="⬅️🔙 Главное меню", callback_data="back_to_main")]
    ])

def get_admin_subscription_keyboard() -> InlineKeyboardMarkup:
    """Обновленная клавиатура управления подписками"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕🎁 Выдать подписку", callback_data="admin_grant_subscription"),
            InlineKeyboardButton(text="🔄⏳ Продлить подписку", callback_data="admin_extend_subscription")
        ],
        [
            InlineKeyboardButton(text="📋📊 Список подписок", callback_data="admin_list_subscriptions"),
            InlineKeyboardButton(text="🔄👑 Принудительный тариф", callback_data="admin_force_tariff")
        ],
        [InlineKeyboardButton(text="⬅️🔙 Назад", callback_data="admin_panel")]
    ])

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
    buttons.append([InlineKeyboardButton(text="❌🚫 Отменить", callback_data="cancel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_yes_no_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура Да/Нет"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да", callback_data="confirm_yes"),
            InlineKeyboardButton(text="❌ Нет", callback_data="confirm_no")
        ],
        [InlineKeyboardButton(text="⬅️🔙 Отмена", callback_data="cancel")]
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
    
    buttons.append([InlineKeyboardButton(text="⬅️🔙 Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_tariff_actions_keyboard(order_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для действий с заказом тарифа"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"order_approve_{order_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"order_reject_{order_id}")
        ],
        [InlineKeyboardButton(text="⬅️🔙 Назад к заказам", callback_data="admin_orders")]
    ])

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
        f"👋✨ Привет, {first_name}!\n\n"
        f"🤖🚀 Я — бот **KOLES-TECH** для планирования постов и AI-контента.\n\n"
        f"💎🔹 **Ваш текущий тариф:** {tariff_info['name']}\n\n"
        f"⚡️🎯 **Возможности:**\n"
        f"• 🤖✨ AI-копирайтер и генератор идей\n"
        f"• 📅🗓 Запланировать пост с любым контентом\n"
        f"• 📊📈 Детальная статистика\n"
        f"• 📢🔔 Управление каналами\n"
        f"• ⏰✅ Автопубликация в нужное время\n"
        f"• 🆘🤝 Техподдержка всегда на связи\n\n"
        f"📍⏰ Время указывается по Москве\n\n"
        f"👇 **Выберите действие:**"
    )
    
    await message.answer(welcome_text, reply_markup=get_main_menu(user_id, is_admin), parse_mode="Markdown")

@router.message(Command("help"))
async def cmd_help(message: Message):
    """Обработчик команды /help"""
    help_text = (
        "📚ℹ️ **Помощь по использованию бота:**\n\n"
        
        "🤖✨ **AI-сервисы:**\n"
        "• Копирайтер - создает продающий текст\n"
        "• Генератор идей - предлагает темы постов\n"
        "• Лимиты обновляются каждый день\n\n"
        
        "📅🗓 **Планирование поста:**\n"
        "1. Выберите 'Запланировать пост'\n"
        "2. Выберите канал\n"
        "3. Отправьте контент\n"
        "4. Укажите дату и время\n"
        "5. Подтвердите публикацию\n\n"
        
        "💎💰 **Тарифы:**\n"
        "• Mini - 1 копирайт, 10 идей, 1 канал, 2 поста\n"
        "• Standard ($4) - 3 копирайта, 30 идей, 2 канала, 6 постов\n"
        "• VIP ($7) - 7 копирайтов, 50 идей, 3 канала, 12 постов\n\n"
        
        f"🆘🤝 **Поддержка:** {SUPPORT_URL}\n"
        f"💬📩 **Вопросы по оплате:** @{ADMIN_CONTACT.replace('@', '')}"
    )
    
    await message.answer(help_text, parse_mode="Markdown")

# ========== ИСПРАВЛЕННЫЕ АДМИН ХЕНДЛЕРЫ ==========
@router.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    """Админ панель - ИСПРАВЛЕНО"""
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
        f"👑⚙️ **АДМИН ПАНЕЛЬ**\n\n"
        f"📊 **Статистика бота:**\n"
        f"• 👥 Всего пользователей: {total_users}\n"
        f"• ✅ Активных (7 дней): {active_users}\n"
        f"• 💎 Активных подписок: {active_subscriptions}\n"
        f"• 🛒 Ожидающих заказов: {pending_orders}\n\n"
        f"🤖 **AI система:**\n"
        f"• 🔑 Всего ключей: {system_stats['total_keys']}\n"
        f"• ✅ Доступных ключей: {system_stats['available_keys']}\n"
        f"• 📊 Всего запросов: {system_stats['total_requests']}\n\n"
        f"📍🕐 Время сервера: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M:%S')}"
    )
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=get_admin_panel_keyboard(),
        parse_mode="Markdown"
    )

@router.callback_query(F.data == "admin_subscriptions")
async def admin_subscriptions_menu(callback: CallbackQuery):
    """Меню управления подписками - ИСПРАВЛЕНО"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "💎🎁 **Управление подписками**\n\n"
        "Выберите действие:",
        reply_markup=get_admin_subscription_keyboard(),
        parse_mode="Markdown"
    )

@router.callback_query(F.data == "admin_grant_subscription")
async def admin_grant_subscription_start(callback: CallbackQuery, state: FSMContext):
    """Начало выдачи подписки - ИСПРАВЛЕНО"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    await state.set_state(AdminStates.waiting_for_user_id)
    await state.update_data(action="grant")
    
    await callback.message.edit_text(
        "👤 **Введите ID пользователя для выдачи подписки:**\n\n"
        "📍 ID можно получить:\n"
        "• Из статистики пользователей\n"
        "• Из заказов тарифов\n"
        "• Попросить пользователя отправить /start\n\n"
        "📝 Введите ID пользователя:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌🚫 Отмена", callback_data="admin_subscriptions")]
        ]),
        parse_mode="Markdown"
    )

@router.callback_query(F.data == "admin_extend_subscription")
async def admin_extend_subscription_start(callback: CallbackQuery, state: FSMContext):
    """Начало продления подписки - ИСПРАВЛЕНО"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    await state.set_state(AdminStates.waiting_for_user_id)
    await state.update_data(action="extend")
    
    await callback.message.edit_text(
        "👤 **Введите ID пользователя для продления подписки:**\n\n"
        "📍 ID можно получить:\n"
        "• Из статистики пользователей\n"
        "• Из заказов тарифов\n"
        "• Попросить пользователя отправить /start\n\n"
        "📝 Введите ID пользователя:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌🚫 Отмена", callback_data="admin_subscriptions")]
        ]),
        parse_mode="Markdown"
    )

@router.message(AdminStates.waiting_for_user_id)
async def admin_process_user_id(message: Message, state: FSMContext):
    """Обработка ID пользователя - ИСПРАВЛЕНО"""
    try:
        target_user_id = int(message.text.strip())
        data = await state.get_data()
        action = data.get('action')
        
        # Проверяем существование пользователя
        user = await get_user_by_id(target_user_id, DATABASE_URL)
        if not user:
            # Создаем пользователя если не существует
            await execute_query('''
                INSERT INTO users (id, username, first_name, tariff, last_seen)
                VALUES ($1, $2, $3, 'mini', NOW())
                ON CONFLICT (id) DO NOTHING
            ''', target_user_id, "", f"User {target_user_id}", database_url=DATABASE_URL)
            user = await get_user_by_id(target_user_id, DATABASE_URL)
        
        await state.update_data(target_user_id=target_user_id)
        
        if action == "grant":
            await state.set_state(AdminStates.waiting_for_tariff_selection)
            await message.answer(
                f"✅ **Пользователь найден:**\n"
                f"👤 **ID:** `{target_user_id}`\n"
                f"📛 **Имя:** {user.get('first_name', 'N/A')}\n"
                f"👤 **Username:** @{user.get('username', 'N/A')}\n"
                f"💎 **Текущий тариф:** {user.get('tariff', 'mini')}\n\n"
                f"**Выберите тариф для выдачи:**",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="⭐ Standard", callback_data="admin_tariff_standard"),
                        InlineKeyboardButton(text="👑 VIP", callback_data="admin_tariff_vip")
                    ],
                    [InlineKeyboardButton(text="❌🚫 Отмена", callback_data="admin_subscriptions")]
                ]),
                parse_mode="Markdown"
            )
        elif action == "extend":
            # Получаем информацию о текущей подписке
            subscription_info = await get_user_subscription_info(target_user_id, DATABASE_URL)
            current_tariff = user.get('tariff', 'mini')
            
            if current_tariff == 'mini' and not subscription_info.get('expires'):
                await state.set_state(AdminStates.waiting_for_tariff_selection)
                await state.update_data(action="grant")
                await message.answer(
                    f"⚠️ **У пользователя нет активной подписки!**\n\n"
                    f"👤 **Пользователь:** {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
                    f"💎 **Текущий тариф:** Mini\n\n"
                    f"**Выберите тариф для выдачи:**",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [
                            InlineKeyboardButton(text="⭐ Standard", callback_data="admin_tariff_standard"),
                            InlineKeyboardButton(text="👑 VIP", callback_data="admin_tariff_vip")
                        ],
                        [InlineKeyboardButton(text="❌🚫 Отмена", callback_data="admin_subscriptions")]
                    ]),
                    parse_mode="Markdown"
                )
                return
            
            await state.set_state(AdminStates.waiting_for_days_selection)
            await state.update_data(tariff_id=current_tariff)
            
            expires_text = "Нет подписки"
            if subscription_info.get('expires'):
                expires_date = subscription_info['expires']
                expires_text = expires_date.strftime('%d.%m.%Y')
                if subscription_info.get('expired'):
                    expires_text += " (истекла)"
                else:
                    expires_text += f" (осталось {subscription_info.get('days_left', 0)} дней)"
            
            await message.answer(
                f"✅ **Пользователь найден:**\n"
                f"👤 **ID:** `{target_user_id}`\n"
                f"📛 **Имя:** {user.get('first_name', 'N/A')}\n"
                f"👤 **Username:** @{user.get('username', 'N/A')}\n"
                f"💎 **Текущий тариф:** {current_tariff}\n"
                f"📅 **Подписка до:** {expires_text}\n\n"
                f"**Выберите количество дней для продления:**",
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
                    [InlineKeyboardButton(text="❌🚫 Отмена", callback_data="admin_subscriptions")]
                ]),
                parse_mode="Markdown"
            )
        
    except ValueError:
        await message.answer(
            "❌ **ID пользователя должен быть числом!**\n\n"
            "📝 Введите ID пользователя:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌🚫 Отмена", callback_data="admin_subscriptions")]
            ]),
            parse_mode="Markdown"
        )

@router.callback_query(F.data.startswith("admin_tariff_"))
async def admin_process_tariff_selection(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора тарифа - ИСПРАВЛЕНО"""
    tariff_id = callback.data.split("_")[2]
    data = await state.get_data()
    target_user_id = data.get('target_user_id')
    
    await state.update_data(tariff_id=tariff_id)
    
    # Получаем информацию о пользователе
    user = await get_user_by_id(target_user_id, DATABASE_URL)
    
    await state.set_state(AdminStates.waiting_for_days_selection)
    
    await callback.message.edit_text(
        f"✅ **Выбран тариф:** {TARIFFS.get(tariff_id, {}).get('name', tariff_id)}\n\n"
        f"👤 **Пользователь:** {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
        f"💎 **Текущий тариф:** {user.get('tariff', 'mini')}\n\n"
        f"**Выберите количество дней для подписки:**",
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
            [InlineKeyboardButton(text="❌🚫 Отмена", callback_data="admin_subscriptions")]
        ]),
        parse_mode="Markdown"
    )

@router.callback_query(F.data.startswith("admin_days_"))
async def admin_process_days_selection(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора количества дней - ИСПРАВЛЕНО"""
    if callback.data == "admin_days_custom":
        await callback.message.edit_text(
            "📝 **Введите количество дней для подписки (от 1 до 365):**\n\n"
            "Примеры:\n"
            "• 30 - 1 месяц\n"
            "• 90 - 3 месяца\n"
            "• 180 - 6 месяцев\n"
            "• 365 - 1 год",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌🚫 Отмена", callback_data="admin_subscriptions")]
            ]),
            parse_mode="Markdown"
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
                f"📋 **ПОДТВЕРЖДЕНИЕ ВЫДАЧИ ПОДПИСКИ**\n\n"
                f"👤 **Пользователь:** {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
                f"🆔 **ID:** `{target_user_id}`\n"
                f"💎 **Тариф:** {tariff_name}\n"
                f"📅 **Срок:** {days} дней\n"
                f"💰 **Стоимость:** ${TARIFFS.get(tariff_id, {}).get('price', 0)}/месяц\n\n"
                f"📍 **После подтверждения:**\n"
                f"• Пользователь получит уведомление\n"
                f"• Подписка будет активирована\n"
                f"• Тариф будет обновлен\n\n"
                f"**Выдать подписку?**",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✅ Да, выдать", callback_data="admin_confirm_grant"),
                        InlineKeyboardButton(text="❌ Нет, отмена", callback_data="admin_subscriptions")
                    ]
                ]),
                parse_mode="Markdown"
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
                f"📋 **ПОДТВЕРЖДЕНИЕ ПРОДЛЕНИЯ ПОДПИСКИ**\n\n"
                f"👤 **Пользователь:** {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
                f"🆔 **ID:** `{target_user_id}`\n"
                f"💎 **Тариф:** {tariff_name}\n"
                f"📅 **Текущая подписка до:** {expires_text}\n"
                f"📅 **Добавить дней:** {days}\n"
                f"📅 **Новая дата окончания:** {new_expires.strftime('%d.%m.%Y') if new_expires else 'N/A'}\n\n"
                f"📍 **После подтверждения:**\n"
                f"• Пользователь получит уведомление\n"
                f"• Подписка будет продлена\n"
                f"• Счетчик дней увеличится\n\n"
                f"**Продлить подписку?**",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✅ Да, продлить", callback_data="admin_confirm_extend"),
                        InlineKeyboardButton(text="❌ Нет, отмена", callback_data="admin_subscriptions")
                    ]
                ]),
                parse_mode="Markdown"
            )
        
    except ValueError:
        await callback.answer("❌ Ошибка в количестве дней", show_alert=True)

@router.message(AdminStates.waiting_for_days_selection)
async def admin_process_custom_days(message: Message, state: FSMContext):
    """Обработка пользовательского количества дней - ИСПРАВЛЕНО"""
    try:
        days = int(message.text.strip())
        if days < 1 or days > 365:
            await message.answer(
                "❌ **Количество дней должно быть от 1 до 365!**\n\n"
                "📝 Введите количество дней еще раз:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌🚫 Отмена", callback_data="admin_subscriptions")]
                ]),
                parse_mode="Markdown"
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
                f"📋 **ПОДТВЕРЖДЕНИЕ ВЫДАЧИ ПОДПИСКИ**\n\n"
                f"👤 **Пользователь:** {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
                f"🆔 **ID:** `{target_user_id}`\n"
                f"💎 **Тариф:** {tariff_name}\n"
                f"📅 **Срок:** {days} дней\n"
                f"💰 **Стоимость:** ${TARIFFS.get(tariff_id, {}).get('price', 0)}/месяц\n\n"
                f"📍 **После подтверждения:**\n"
                f"• Пользователь получит уведомление\n"
                f"• Подписка будет активирована\n"
                f"• Тариф будет обновлен\n\n"
                f"**Выдать подписку?**",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✅ Да, выдать", callback_data="admin_confirm_grant"),
                        InlineKeyboardButton(text="❌ Нет, отмена", callback_data="admin_subscriptions")
                    ]
                ]),
                parse_mode="Markdown"
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
                f"📋 **ПОДТВЕРЖДЕНИЕ ПРОДЛЕНИЯ ПОДПИСКИ**\n\n"
                f"👤 **Пользователь:** {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
                f"🆔 **ID:** `{target_user_id}`\n"
                f"💎 **Тариф:** {tariff_name}\n"
                f"📅 **Текущая подписка до:** {expires_text}\n"
                f"📅 **Добавить дней:** {days}\n"
                f"📅 **Новая дата окончания:** {new_expires.strftime('%d.%m.%Y') if new_expires else 'N/A'}\n\n"
                f"📍 **После подтверждения:**\n"
                f"• Пользователь получит уведомление\n"
                f"• Подписка будет продлена\n"
                f"• Счетчик дней увеличится\n\n"
                f"**Продлить подписку?**",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✅ Да, продлить", callback_data="admin_confirm_extend"),
                        InlineKeyboardButton(text="❌ Нет, отмена", callback_data="admin_subscriptions")
                    ]
                ]),
                parse_mode="Markdown"
            )
        
    except ValueError:
        await message.answer(
            "❌ **Введите число!**\n\nПример: 30, 90, 180",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌🚫 Отмена", callback_data="admin_subscriptions")]
            ]),
            parse_mode="Markdown"
        )

@router.callback_query(F.data == "admin_confirm_grant")
async def admin_confirm_grant(callback: CallbackQuery, state: FSMContext):
    """Подтверждение выдачи подписки - ИСПРАВЛЕНО"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    data = await state.get_data()
    target_user_id = data.get('target_user_id')
    tariff_id = data.get('tariff_id')
    days = data.get('days')
    
    if not all([target_user_id, tariff_id, days]):
        await callback.message.edit_text(
            "❌ **Ошибка: не все данные заполнены!**",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️🔙 Назад", callback_data="admin_subscriptions")]
            ]),
            parse_mode="Markdown"
        )
        await state.clear()
        return
    
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
                f"🎉✅ **ВАМ ВЫДАНА ПОДПИСКА!**\n\n"
                f"💎 **Тариф:** {tariff_name}\n"
                f"📅 **Срок:** {days} дней\n"
                f"🆔 **Ваш ID:** `{target_user_id}`\n\n"
                f"📍 Подписка активна с сегодняшнего дня.\n"
                f"Вы можете проверить статус в разделе 'Моя статистика'.\n\n"
                f"Спасибо за использование **KOLES-TECH**! 🤖🚀",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление пользователю {target_user_id}: {e}")
        
        # Создаем запись о заказе
        await execute_query('''
            INSERT INTO tariff_orders (user_id, tariff, status, admin_notes)
            VALUES ($1, $2, 'granted_by_admin', $3)
        ''', target_user_id, tariff_id, f"Выдано админом {user_id} на {days} дней", database_url=DATABASE_URL)
        
        await callback.message.edit_text(
            f"✅ **Подписка успешно выдана!**\n\n"
            f"📋 **Детали:**\n"
            f"👤 **Пользователь:** {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
            f"🆔 **ID:** `{target_user_id}`\n"
            f"💎 **Тариф:** {tariff_name}\n"
            f"📅 **Срок:** {days} дней\n"
            f"👑 **Выдал:** админ {user_id}\n"
            f"🕐 **Время:** {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎🎁 Управление подписками", callback_data="admin_subscriptions")],
                [InlineKeyboardButton(text="⬅️🔙 Админ панель", callback_data="admin_panel")]
            ]),
            parse_mode="Markdown"
        )
    else:
        await callback.message.edit_text(
            "❌ **Ошибка при выдаче подписки!**\n\n"
            "Попробуйте позже или обратитесь к разработчику.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎🎁 Управление подписками", callback_data="admin_subscriptions")],
                [InlineKeyboardButton(text="⬅️🔙 Админ панель", callback_data="admin_panel")]
            ]),
            parse_mode="Markdown"
        )
    
    await state.clear()

@router.callback_query(F.data == "admin_confirm_extend")
async def admin_confirm_extend(callback: CallbackQuery, state: FSMContext):
    """Подтверждение продления подписки - ИСПРАВЛЕНО"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    data = await state.get_data()
    target_user_id = data.get('target_user_id')
    days = data.get('days')
    
    if not all([target_user_id, days]):
        await callback.message.edit_text(
            "❌ **Ошибка: не все данные заполнены!**",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️🔙 Назад", callback_data="admin_subscriptions")]
            ]),
            parse_mode="Markdown"
        )
        await state.clear()
        return
    
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
                f"🎉✅ **ВАША ПОДПИСКА ПРОДЛЕНА!**\n\n"
                f"💎 **Тариф:** {tariff_name}\n"
                f"📅 **Добавлено дней:** {days}\n"
                f"📅 **Новая дата окончания:** {subscription_info['expires'].strftime('%d.%m.%Y') if subscription_info.get('expires') else 'N/A'}\n"
                f"🆔 **Ваш ID:** `{target_user_id}`\n\n"
                f"📍 Подписка успешно продлена.\n"
                f"Вы можете проверить статус в разделе 'Моя статистика'.\n\n"
                f"Спасибо за использование **KOLES-TECH**! 🤖🚀",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление пользователю {target_user_id}: {e}")
        
        # Создаем запись о заказе
        await execute_query('''
            INSERT INTO tariff_orders (user_id, tariff, status, admin_notes)
            VALUES ($1, $2, 'extended_by_admin', $3)
        ''', target_user_id, current_tariff, f"Продлено админом {user_id} на {days} дней", database_url=DATABASE_URL)
        
        expires_text = subscription_info['expires'].strftime('%d.%m.%Y') if subscription_info.get('expires') else 'N/A'
        
        await callback.message.edit_text(
            f"✅ **Подписка успешно продлена!**\n\n"
            f"📋 **Детали:**\n"
            f"👤 **Пользователь:** {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
            f"🆔 **ID:** `{target_user_id}`\n"
            f"💎 **Тариф:** {tariff_name}\n"
            f"📅 **Добавлено дней:** {days}\n"
            f"📅 **Новая дата окончания:** {expires_text}\n"
            f"👑 **Продлил:** админ {user_id}\n"
            f"🕐 **Время:** {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎🎁 Управление подписками", callback_data="admin_subscriptions")],
                [InlineKeyboardButton(text="⬅️🔙 Админ панель", callback_data="admin_panel")]
            ]),
            parse_mode="Markdown"
        )
    else:
        await callback.message.edit_text(
            "❌ **Ошибка при продлении подписки!**\n\n"
            "Попробуйте позже или обратитесь к разработчику.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎🎁 Управление подписками", callback_data="admin_subscriptions")],
                [InlineKeyboardButton(text="⬅️🔙 Админ панель", callback_data="admin_panel")]
            ]),
            parse_mode="Markdown"
        )
    
    await state.clear()

@router.callback_query(F.data == "admin_list_subscriptions")
async def admin_list_subscriptions(callback: CallbackQuery):
    """Список активных подписок - ИСПРАВЛЕНО"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    # Получаем активные подписки
    subscriptions = await execute_query('''
        SELECT id, username, first_name, tariff, tariff_expires, subscription_days
        FROM users 
        WHERE tariff_expires >= CURRENT_DATE AND tariff != 'mini'
        ORDER BY tariff_expires ASC
    ''', database_url=DATABASE_URL)
    
    if not subscriptions:
        await callback.message.edit_text(
            "📭 **Нет активных подписок**",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️🔙 Назад", callback_data="admin_subscriptions")]
            ]),
            parse_mode="Markdown"
        )
        return
    
    subscriptions_text = "📋✅ **АКТИВНЫЕ ПОДПИСКИ**\n\n"
    
    for i, sub in enumerate(subscriptions, 1):
        expires_date = sub['tariff_expires']
        days_left = (expires_date - datetime.now(MOSCOW_TZ).date()).days
        tariff_name = TARIFFS.get(sub['tariff'], {}).get('name', sub['tariff'])
        
        subscriptions_text += (
            f"{i}. **{sub['first_name']}** (@{sub['username'] or 'нет'})\n"
            f"   🆔 ID: `{sub['id']}`\n"
            f"   💎 Тариф: {tariff_name}\n"
            f"   📅 До: {expires_date.strftime('%d.%m.%Y')}\n"
            f"   ⏳ Осталось: {days_left} дней\n"
            f"   📊 Всего дней: {sub['subscription_days']}\n\n"
        )
    
    total_active = len(subscriptions)
    total_days = sum(sub['subscription_days'] for sub in subscriptions)
    
    subscriptions_text += f"📊 **Итого:** {total_active} активных подписок, {total_days} дней всего"
    
    # Разбиваем длинное сообщение
    if len(subscriptions_text) > 4000:
        parts = split_message(subscriptions_text)
        for i, part in enumerate(parts):
            if i == 0:
                await callback.message.edit_text(part, parse_mode="Markdown")
            else:
                await callback.message.answer(part, parse_mode="Markdown")
    else:
        await callback.message.edit_text(subscriptions_text, parse_mode="Markdown")
    
    await callback.message.answer(
        "👇 **Выберите действие:**",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить список", callback_data="admin_list_subscriptions")],
            [InlineKeyboardButton(text="⬅️🔙 Назад", callback_data="admin_subscriptions")]
        ]),
        parse_mode="Markdown"
    )

@router.callback_query(F.data == "admin_orders")
async def admin_orders(callback: CallbackQuery):
    """Заказы тарифов - ИСПРАВЛЕНО"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    # Получаем все заказы
    orders = await get_tariff_orders(None, DATABASE_URL)
    
    if not orders:
        await callback.message.edit_text(
            "📭 **Нет заказов тарифов**",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️🔙 Админ панель", callback_data="admin_panel")]
            ]),
            parse_mode="Markdown"
        )
        return
    
    # Разделяем на pending и другие
    pending_orders = []
    other_orders = []
    
    for order in orders[:20]:  # Ограничиваем до 20 последних
        if order.get('status') == 'pending':
            pending_orders.append(order)
        else:
            other_orders.append(order)
    
    orders_text = "🛒📦 **ЗАКАЗЫ ТАРИФОВ**\n\n"
    
    if pending_orders:
        orders_text += "⏳ **ОЖИДАЮТ ОБРАБОТКИ:**\n\n"
        for order in pending_orders[:5]:
            user = await get_user_by_id(order['user_id'], DATABASE_URL)
            username = f"@{user.get('username')}" if user and user.get('username') else "Нет username"
            tariff_name = TARIFFS.get(order['tariff'], {}).get('name', order['tariff'])
            
            orders_text += (
                f"🆔 **ID заказа:** `{order['id']}`\n"
                f"👤 **Пользователь:** {user.get('first_name', 'N/A')} ({username})\n"
                f"🆔 **User ID:** `{order['user_id']}`\n"
                f"💎 **Тариф:** {tariff_name}\n"
                f"📅 **Дата:** {order['order_date'].astimezone(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M') if order.get('order_date') else 'N/A'}\n"
                f"📊 **Статус:** ⏳ Ожидает\n\n"
            )
        
        if len(pending_orders) > 5:
            orders_text += f"... и еще {len(pending_orders) - 5} заказов\n\n"
    
    if other_orders:
        orders_text += "✅ **ОБРАБОТАННЫЕ ЗАКАЗЫ:**\n\n"
        for order in other_orders[:5]:
            user = await get_user_by_id(order['user_id'], DATABASE_URL)
            username = f"@{user.get('username')}" if user and user.get('username') else "Нет username"
            tariff_name = TARIFFS.get(order['tariff'], {}).get('name', order['tariff'])
            
            status_emoji = "✅" if order['status'] == 'completed' else "❌" if order['status'] == 'rejected' else "ℹ️"
            
            orders_text += (
                f"🆔 **ID заказа:** `{order['id']}`\n"
                f"👤 **Пользователь:** {user.get('first_name', 'N/A')} ({username})\n"
                f"💎 **Тариф:** {tariff_name}\n"
                f"📊 **Статус:** {status_emoji} {order['status']}\n"
                f"📝 **Заметки:** {order.get('admin_notes', 'Нет')}\n\n"
            )
    
    await callback.message.edit_text(
        orders_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_orders")],
            [InlineKeyboardButton(text="⬅️🔙 Админ панель", callback_data="admin_panel")]
        ]),
        parse_mode="Markdown"
    )

@router.callback_query(F.data.startswith("order_"))
async def process_order_action(callback: CallbackQuery, state: FSMContext):
    """Обработка действий с заказом - ИСПРАВЛЕНО"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    parts = callback.data.split("_")
    action = parts[1]  # approve или reject
    order_id = int(parts[2])
    
    # Получаем заказ
    orders = await execute_query(
        "SELECT * FROM tariff_orders WHERE id = $1",
        order_id,
        database_url=DATABASE_URL
    )
    
    if not orders:
        await callback.answer("❌ Заказ не найден!", show_alert=True)
        return
    
    order = orders[0]
    
    if action == "approve":
        # Подтверждаем заказ
        await update_order_status(order_id, 'completed', f"Одобрено админом {user_id}", DATABASE_URL)
        
        # Выдаем подписку на 30 дней по умолчанию
        await update_user_subscription(order['user_id'], order['tariff'], 30, DATABASE_URL)
        
        # Отправляем уведомление пользователю
        try:
            tariff_name = TARIFFS.get(order['tariff'], {}).get('name', order['tariff'])
            await bot.send_message(
                order['user_id'],
                f"✅🎉 **ЗАКАЗ ОДОБРЕН!**\n\n"
                f"💎 **Ваш тариф:** {tariff_name}\n"
                f"📅 **Срок:** 30 дней\n\n"
                f"📍 Подписка активирована!\n"
                f"Спасибо за использование **KOLES-TECH**! 🤖🚀",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {order['user_id']}: {e}")
        
        await callback.answer("✅ Заказ одобрен, подписка выдана на 30 дней!", show_alert=True)
    
    elif action == "reject":
        # Отклоняем заказ
        await update_order_status(order_id, 'rejected', f"Отклонено админом {user_id}", DATABASE_URL)
        
        # Отправляем уведомление пользователю
        try:
            await bot.send_message(
                order['user_id'],
                "❌ **ЗАКАЗ ОТКЛОНЕН**\n\n"
                "К сожалению, ваш заказ на смену тарифа был отклонен.\n"
                "Пожалуйста, свяжитесь с поддержкой для уточнения деталей.",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {order['user_id']}: {e}")
        
        await callback.answer("❌ Заказ отклонен", show_alert=True)
    
    # Обновляем список заказов
    await admin_orders(callback)

@router.callback_query(F.data == "admin_force_tariff")
async def admin_force_tariff_start(callback: CallbackQuery, state: FSMContext):
    """Принудительное обновление тарифа - ИСПРАВЛЕНО"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    await state.set_state(AdminStates.waiting_for_force_tariff_user)
    
    await callback.message.edit_text(
        "🔄👑 **Принудительное обновление тарифа**\n\n"
        "Введите ID пользователя для обновления тарифа:\n\n"
        "📍 Эта функция принудительно меняет тариф без проверки подписки.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌🚫 Отмена", callback_data="admin_panel")]
        ]),
        parse_mode="Markdown"
    )

@router.message(AdminStates.waiting_for_force_tariff_user)
async def admin_force_tariff_user(message: Message, state: FSMContext):
    """Обработка ID для принудительного тарифа - ИСПРАВЛЕНО"""
    try:
        target_user_id = int(message.text.strip())
        
        # Проверяем существование пользователя
        user = await get_user_by_id(target_user_id, DATABASE_URL)
        if not user:
            await message.answer(
                "❌ **Пользователь с таким ID не найден!**\n\n"
                "Проверьте ID и попробуйте снова:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌🚫 Отмена", callback_data="admin_panel")]
                ]),
                parse_mode="Markdown"
            )
            return
        
        await state.update_data(target_user_id=target_user_id)
        await state.set_state(AdminStates.waiting_for_force_tariff_select)
        
        await message.answer(
            f"👤 **Пользователь найден:**\n"
            f"🆔 **ID:** `{target_user_id}`\n"
            f"📛 **Имя:** {user.get('first_name', 'N/A')}\n"
            f"👤 **Username:** @{user.get('username', 'N/A')}\n"
            f"💎 **Текущий тариф:** {user.get('tariff', 'mini')}\n\n"
            f"**Выберите новый тариф:**",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="📦 Mini", callback_data="force_tariff_mini"),
                    InlineKeyboardButton(text="⭐ Standard", callback_data="force_tariff_standard")
                ],
                [
                    InlineKeyboardButton(text="👑 VIP", callback_data="force_tariff_vip"),
                    InlineKeyboardButton(text="👑⚙️ Admin", callback_data="force_tariff_admin")
                ],
                [InlineKeyboardButton(text="❌🚫 Отмена", callback_data="admin_panel")]
            ]),
            parse_mode="Markdown"
        )
    except ValueError:
        await message.answer(
            "❌ **ID пользователя должен быть числом!**\n\n"
            "Введите ID пользователя:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌🚫 Отмена", callback_data="admin_panel")]
            ]),
            parse_mode="Markdown"
        )

@router.callback_query(F.data.startswith("force_tariff_"))
async def admin_force_tariff_select(callback: CallbackQuery, state: FSMContext):
    """Выбор тарифа для принудительного обновления - ИСПРАВЛЕНО"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    tariff_id = callback.data.replace("force_tariff_", "")
    data = await state.get_data()
    target_user_id = data.get('target_user_id')
    
    # Принудительно обновляем тариф
    success, message_text = await force_update_user_tariff(target_user_id, tariff_id, user_id, DATABASE_URL)
    
    if success:
        await callback.message.edit_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Еще пользователь", callback_data="admin_force_tariff")],
                [InlineKeyboardButton(text="⬅️🔙 Админ панель", callback_data="admin_panel")]
            ]),
            parse_mode="Markdown"
        )
    else:
        await callback.message.edit_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data="admin_force_tariff")],
                [InlineKeyboardButton(text="⬅️🔙 Админ панель", callback_data="admin_panel")]
            ]),
            parse_mode="Markdown"
        )
    
    await state.clear()

@router.callback_query(F.data == "admin_users")
async def admin_users_stats(callback: CallbackQuery):
    """Статистика пользователей - ИСПРАВЛЕНО"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    # Получаем статистику по пользователям
    users = await get_all_users(DATABASE_URL)
    
    if not users:
        await callback.message.edit_text(
            "📭 **Нет пользователей**",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️🔙 Админ панель", callback_data="admin_panel")]
            ]),
            parse_mode="Markdown"
        )
        return
    
    total_users = len(users)
    active_today = await execute_query(
        "SELECT COUNT(*) as count FROM users WHERE last_seen > NOW() - INTERVAL '24 hours'",
        database_url=DATABASE_URL
    )
    active_today = active_today[0]['count'] if active_today else 0
    
    active_week = await execute_query(
        "SELECT COUNT(*) as count FROM users WHERE last_seen > NOW() - INTERVAL '7 days'",
        database_url=DATABASE_URL
    )
    active_week = active_week[0]['count'] if active_week else 0
    
    tariff_stats = {
        'mini': 0,
        'standard': 0,
        'vip': 0,
        'admin': 0
    }
    
    for user in users:
        tariff = user.get('tariff', 'mini')
        if tariff in tariff_stats:
            tariff_stats[tariff] += 1
        else:
            tariff_stats['mini'] += 1
    
    stats_text = (
        f"📊👥 **СТАТИСТИКА ПОЛЬЗОВАТЕЛЕЙ**\n\n"
        f"👥 **Всего пользователей:** {total_users}\n"
        f"✅ **Активных сегодня:** {active_today}\n"
        f"✅ **Активных за неделю:** {active_week}\n\n"
        f"💎 **Распределение по тарифам:**\n"
        f"• 📦 Mini: {tariff_stats['mini']}\n"
        f"• ⭐ Standard: {tariff_stats['standard']}\n"
        f"• 👑 VIP: {tariff_stats['vip']}\n"
        f"• 👑⚙️ Admin: {tariff_stats['admin']}\n\n"
        f"📅 **Последние 5 пользователей:**\n"
    )
    
    for user in users[:5]:
        created = user.get('created_at')
        if created:
            if hasattr(created, 'astimezone'):
                created = created.astimezone(MOSCOW_TZ).strftime('%d.%m.%Y')
            else:
                created = str(created)[:10]
        
        stats_text += f"• {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')}) - {created}\n"
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_users")],
            [InlineKeyboardButton(text="⬅️🔙 Админ панель", callback_data="admin_panel")]
        ]),
        parse_mode="Markdown"
    )

@router.callback_query(F.data == "admin_ai_stats")
async def admin_ai_stats(callback: CallbackQuery):
    """Статистика AI - ИСПРАВЛЕНО"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    ai_manager = get_ai_manager()
    system_stats = ai_manager.get_system_stats() if ai_manager else {'total_keys': 0, 'available_keys': 0, 'total_requests': 0}
    
    # Получаем статистику запросов за сегодня
    today = datetime.now(MOSCOW_TZ).date()
    today_requests = await execute_query(
        "SELECT COUNT(*) as count FROM ai_request_logs WHERE DATE(created_at AT TIME ZONE 'Europe/Moscow') = $1",
        today,
        database_url=DATABASE_URL
    )
    today_requests = today_requests[0]['count'] if today_requests else 0
    
    # Получаем статистику по типам сервисов
    service_stats = await execute_query(
        "SELECT service_type, COUNT(*) as count FROM ai_request_logs GROUP BY service_type",
        database_url=DATABASE_URL
    )
    
    # Получаем статистику по пользователям
    top_users = await execute_query('''
        SELECT user_id, COUNT(*) as count 
        FROM ai_request_logs 
        GROUP BY user_id 
        ORDER BY count DESC 
        LIMIT 5
    ''', database_url=DATABASE_URL)
    
    stats_text = (
        f"🤖📊 **СТАТИСТИКА AI СЕРВИСОВ**\n\n"
        f"🔑 **Система ключей:**\n"
        f"• Всего ключей: {system_stats['total_keys']}\n"
        f"• Доступных ключей: {system_stats['available_keys']}\n"
        f"• Занятых ключей: {system_stats['total_keys'] - system_stats['available_keys']}\n\n"
        f"📊 **Общая статистика:**\n"
        f"• Всего запросов: {system_stats['total_requests']}\n"
        f"• Запросов сегодня: {today_requests}\n\n"
    )
    
    if service_stats:
        stats_text += "📝 **По типам сервисов:**\n"
        for stat in service_stats:
            service_type = stat['service_type']
            count = stat['count']
            emoji = "📝" if service_type == 'copy' else "💡"
            stats_text += f"• {emoji} {service_type}: {count}\n"
        stats_text += "\n"
    
    if top_users:
        stats_text += "🏆 **Топ пользователей:**\n"
        for i, user in enumerate(top_users, 1):
            user_info = await get_user_by_id(user['user_id'], DATABASE_URL)
            name = user_info.get('first_name', f"User {user['user_id']}") if user_info else f"User {user['user_id']}"
            stats_text += f"{i}. {name} - {user['count']} запросов\n"
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_ai_stats")],
            [InlineKeyboardButton(text="⬅️🔙 Админ панель", callback_data="admin_panel")]
        ]),
        parse_mode="Markdown"
    )

@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    """Начало рассылки - ИСПРАВЛЕНО"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    await state.set_state(AdminStates.waiting_for_broadcast)
    
    await callback.message.edit_text(
        "📢📨 **РАССЫЛКА СООБЩЕНИЯ**\n\n"
        "Введите текст для рассылки всем пользователям:\n\n"
        "📍 Поддерживается Markdown-разметка:\n"
        "• **жирный**\n"
        "• *курсив*\n"
        "• `код`\n"
        "• [ссылка](https://t.me)\n\n"
        "⚠️ **Внимание:** Рассылка будет отправлена ВСЕМ пользователям бота!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌🚫 Отмена", callback_data="admin_panel")]
        ]),
        parse_mode="Markdown"
    )

@router.message(AdminStates.waiting_for_broadcast)
async def admin_broadcast_process(message: Message, state: FSMContext):
    """Отправка рассылки - ИСПРАВЛЕНО"""
    user_id = message.from_user.id
    
    if user_id != ADMIN_ID:
        await message.answer("❌ У вас нет прав администратора!")
        await state.clear()
        return
    
    broadcast_text = message.text
    await state.update_data(broadcast_text=broadcast_text)
    
    # Получаем всех пользователей
    users = await get_all_users(DATABASE_URL)
    
    if not users:
        await message.answer(
            "📭 **Нет пользователей для рассылки**",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️🔙 Админ панель", callback_data="admin_panel")]
            ]),
            parse_mode="Markdown"
        )
        await state.clear()
        return
    
    await message.answer(
        f"📊 **Начинаю рассылку...**\n"
        f"👥 Всего пользователей: {len(users)}\n\n"
        f"⚠️ Это может занять некоторое время.",
        parse_mode="Markdown"
    )
    
    success_count = 0
    fail_count = 0
    
    for user in users:
        try:
            await bot.send_message(
                user['id'],
                f"📢 **РАССЫЛКА ОТ АДМИНИСТРАЦИИ**\n\n{broadcast_text}",
                parse_mode="Markdown"
            )
            success_count += 1
            await asyncio.sleep(0.05)  # Небольшая задержка
        except Exception as e:
            fail_count += 1
            logger.error(f"Ошибка отправки рассылки пользователю {user['id']}: {e}")
    
    await message.answer(
        f"✅ **Рассылка завершена!**\n\n"
        f"📊 **Статистика:**\n"
        f"• ✅ Успешно: {success_count}\n"
        f"• ❌ Ошибок: {fail_count}\n"
        f"• 👥 Всего: {len(users)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️🔙 Админ панель", callback_data="admin_panel")]
        ]),
        parse_mode="Markdown"
    )
    
    await state.clear()

# ========== TARIFFS HANDLERS С ИСПРАВЛЕННЫМИ ССЫЛКАМИ ==========
@router.callback_query(F.data == "tariffs")
async def show_tariffs(callback: CallbackQuery):
    """Показывает тарифы - ИСПРАВЛЕНЫ ССЫЛКИ"""
    user_id = callback.from_user.id
    user_tariff = await get_user_tariff(user_id, DATABASE_URL)
    
    tariffs_text = "💎💰 **ТАРИФЫ KOLES-TECH**\n\n"
    
    for tariff_id, tariff_info in TARIFFS.items():
        if tariff_id == 'admin':
            continue
            
        is_current = tariff_id == user_tariff
        current_marker = " ✅ **ТЕКУЩИЙ**" if is_current else ""
        
        price_text = f"${tariff_info['price']}/месяц" if tariff_info['price'] > 0 else "Бесплатно"
        
        tariffs_text += (
            f"**{tariff_info['name']}** - {price_text}{current_marker}\n"
            f"• 📢 Каналов: {tariff_info['channels_limit']}\n"
            f"• 📅 Постов в день: {tariff_info['daily_posts_limit']}\n"
            f"• 🤖 AI-текстов: {tariff_info['ai_copies_limit']}/день\n"
            f"• 💡 AI-идей: {tariff_info['ai_ideas_limit']}/день\n"
            f"• ℹ️ {tariff_info['description']}\n\n"
        )
    
    tariffs_text += (
        f"📍 **Чтобы оформить подписку:**\n"
        f"1. Выберите нужный тариф ниже\n"
        f"2. Свяжитесь с администратором для оплаты\n"
        f"3. После оплаты подписка будет активирована\n\n"
        f"💬📩 **Контакт администратора:** @{ADMIN_CONTACT.replace('@', '')}"
    )
    
    await callback.message.edit_text(
        tariffs_text,
        reply_markup=get_tariffs_keyboard(user_tariff),
        parse_mode="Markdown"
    )

# ========== ОСТАЛЬНЫЕ ХЕНДЛЕРЫ БЕЗ ИЗМЕНЕНИЙ ==========
# (AI хендлеры, планирование постов, статистика, каналы и т.д. остаются без изменений)

# ========== BACK HANDLERS ==========
@router.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await state.clear()
    user_id = callback.from_user.id
    is_admin = user_id == ADMIN_ID
    
    await callback.message.edit_text(
        "🤖🚀 **Главное меню**\n\n👇 **Выберите действие:**",
        reply_markup=get_main_menu(user_id, is_admin),
        parse_mode="Markdown"
    )

@router.callback_query(F.data == "cancel")
async def cancel_action(callback: CallbackQuery, state: FSMContext):
    """Отмена действия"""
    await state.clear()
    user_id = callback.from_user.id
    is_admin = user_id == ADMIN_ID
    
    await callback.message.edit_text(
        "❌ **Действие отменено.**\n\n👇 **Выберите действие:**",
        reply_markup=get_main_menu(user_id, is_admin),
        parse_mode="Markdown"
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
    logger.info(f"🚀 ЗАПУСК БОТА KOLES-TECH v3.1 (ОБНОВЛЕННАЯ ВЕРСИЯ)")
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
                    f"🤖🚀 **Бот @{me.username} успешно запущен!**\n\n"
                    f"🆔 **ID:** {me.id}\n"
                    f"🤖 **AI сервисы:** ВКЛЮЧЕНЫ\n"
                    f"🔑 **Gemini ключей:** {len(GEMINI_API_KEYS)}\n"
                    f"🔄 **Система ротации ключей:** АКТИВНА\n"
                    f"💎 **Система подписок:** АКТИВНА\n"
                    f"📅 **Система планирования постов:** АКТИВНА\n"
                    f"🌐 **Порт Railway:** {PORT}\n"
                    f"🕐 **Время:** {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M:%S')}",
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить админа: {e}")
        
        logger.info("=" * 60)
        logger.info("🎉 БОТ УСПЕШНО ЗАПУЩЕН С ОБНОВЛЕННЫМ ДИЗАЙНОМ!")
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
            WHERE tariff_expires < CURRENT_DATE AND tariff != 'mini' AND tariff != 'admin'
        ''', database_url=DATABASE_URL)
        
        for user in expired_subscriptions:
            # Понижаем тариф до минимума
            await execute_query('''
                UPDATE users 
                SET tariff = 'mini', tariff_expires = NULL 
                WHERE id = $1 AND tariff != 'admin'
            ''', user['id'], database_url=DATABASE_URL)
            
            # Отправляем уведомление пользователю
            try:
                await bot.send_message(
                    user['id'],
                    f"⚠️ **ВАША ПОДПИСКА ИСТЕКЛА**\n\n"
                    f"📅 Дата окончания подписки наступила.\n"
                    f"💎 Ваш тариф изменен на **Mini**.\n\n"
                    f"📍 **Для продления подписки:**\n"
                    f"1. Перейдите в раздел 'Тарифы'\n"
                    f"2. Выберите нужный тариф\n"
                    f"3. Свяжитесь с администратором\n\n"
                    f"💬📩 **Контакт:** @{ADMIN_CONTACT.replace('@', '')}",
                    parse_mode="Markdown"
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
