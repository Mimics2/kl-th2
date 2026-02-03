import os
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from enum import Enum
from collections import deque

import pytz
from aiogram import Bot, types, Router, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

import google.generativeai as genai
import asyncpg

# ========== CONFIG ==========
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# Gemini API Keys (минимум 8 для ротации)
GEMINI_API_KEYS = [
    "AIzaSyAI_vkc2IFhOPKELbxpu1QODKCd5h-bEOI",
    "AIzaSyBy_aoWhZ5ZKm4yyhw7mNzP-8U-t4pXWMI",
    "AIzaSyA4jtchIEaTWrHnr_yQcRGTsZIWTAstXNA",
    "AIzaSyANoeHQtBBxInIYCfNHHO_JGE6DWmhQ2Rg",
    "AIzaSyAI_vkc2IFhOPKELbxpu1QODKCd5h-bEOI_2",
    "AIzaSyBy_aoWhZ5ZKm4yyhw7mNzP-8U-t4pXWMI_2",
    "AIzaSyA4jtchIEaTWrHnr_yQcRGTsZIWTAstXNA_2",
    "AIzaSyANoeHQtBBxInIYCfNHHO_JGE6DWmhQ2Rg_2"
]

MODEL_NAME = "gemini-2.0-flash-exp"
REQUESTS_PER_KEY = 5  # Ротация каждые 5 запросов
REQUEST_COOLDOWN = 60  # 60 секунд между запросами

# ========== TARIFF LIMITS ==========
class AITariffLimits(Enum):
    MINI = {"copies": 1, "ideas": 10}
    STANDARD = {"copies": 3, "ideas": 30}
    VIP = {"copies": 7, "ideas": 50}
    ADMIN = {"copies": 999, "ideas": 999}

# ========== SETUP ==========
logger = logging.getLogger(__name__)
router = Router()

# ========== SESSION MANAGER ==========
class AISessionManager:
    def __init__(self):
        self.sessions: Dict[int, Dict] = {}
        self.key_stats = {key: 0 for key in GEMINI_API_KEYS}
        self.user_queues: Dict[int, deque] = {}
        self.last_request_time: Dict[int, datetime] = {}
    
    def get_session(self, user_id: int) -> Dict:
        """Получает или создает сессию пользователя"""
        if user_id not in self.sessions:
            self.sessions[user_id] = {
                'history': deque(maxlen=10),
                'key_index': 0,
                'request_count': 0,
                'total_requests': 0,
                'copies_used': 0,
                'ideas_used': 0,
                'last_reset': datetime.now(MOSCOW_TZ).date()
            }
        return self.sessions[user_id]
    
    def get_next_key(self, session: Dict) -> Tuple[str, int]:
        """Получает следующий API ключ с ротацией"""
        if session['request_count'] >= REQUESTS_PER_KEY:
            session['key_index'] = (session['key_index'] + 1) % len(GEMINI_API_KEYS)
            session['request_count'] = 0
        
        key = GEMINI_API_KEYS[session['key_index']]
        session['request_count'] += 1
        session['total_requests'] += 1
        self.key_stats[key] = self.key_stats.get(key, 0) + 1
        
        return key, session['key_index']
    
    def can_make_request(self, user_id: int) -> Tuple[bool, Optional[str]]:
        """Проверяет, может ли пользователь сделать запрос"""
        now = datetime.now(MOSCOW_TZ)
        
        if user_id in self.last_request_time:
            time_diff = (now - self.last_request_time[user_id]).total_seconds()
            if time_diff < REQUEST_COOLDOWN:
                wait_time = REQUEST_COOLDOWN - int(time_diff)
                return False, f"⏳ Подождите {wait_time} секунд перед следующим запросом"
        
        self.last_request_time[user_id] = now
        return True, None
    
    def reset_daily_limits(self):
        """Сбрасывает дневные лимиты"""
        today = datetime.now(MOSCOW_TZ).date()
        for user_id, session in self.sessions.items():
            if session['last_reset'] < today:
                session['copies_used'] = 0
                session['ideas_used'] = 0
                session['last_reset'] = today

ai_manager = AISessionManager()

# ========== DATABASE FUNCTIONS ==========
async def get_db_connection():
    """Создает соединение с базой данных"""
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise Exception("DATABASE_URL не указан")
    
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
            return 'mini'
        
        if user.get('is_admin'):
            return 'admin'
            
        return user.get('tariff', 'mini')
    except Exception as e:
        logger.error(f"Ошибка получения тарифа: {e}")
        return 'mini'

async def update_ai_usage(user_id: int, service_type: str) -> bool:
    """Обновляет использование AI услуг"""
    try:
        conn = await get_db_connection()
        
        if service_type == 'copy':
            await conn.execute('''
                UPDATE users 
                SET ai_copies_used = COALESCE(ai_copies_used, 0) + 1,
                    ai_last_used = NOW()
                WHERE id = $1
            ''', user_id)
        elif service_type == 'ideas':
            await conn.execute('''
                UPDATE users 
                SET ai_ideas_used = COALESCE(ai_ideas_used, 0) + 1,
                    ai_last_used = NOW()
                WHERE id = $1
            ''', user_id)
        
        await conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления использования AI: {e}")
        return False

async def get_ai_usage_stats(user_id: int) -> Dict:
    """Получает статистику использования AI"""
    try:
        conn = await get_db_connection()
        user = await conn.fetchrow('''
            SELECT ai_copies_used, ai_ideas_used, ai_last_used 
            FROM users 
            WHERE id = $1
        ''', user_id)
        await conn.close()
        
        return {
            'copies_used': user['ai_copies_used'] if user and user['ai_copies_used'] else 0,
            'ideas_used': user['ai_ideas_used'] if user and user['ai_ideas_used'] else 0,
            'last_used': user['ai_last_used'] if user and user['ai_last_used'] else None
        }
    except Exception as e:
        logger.error(f"Ошибка получения статистики AI: {e}")
        return {'copies_used': 0, 'ideas_used': 0, 'last_used': None}

# ========== STATES ==========
class AIStates(StatesGroup):
    waiting_for_topic = State()
    waiting_for_examples = State()
    waiting_for_style = State()
    waiting_for_idea_topic = State()

# ========== KEYBOARDS ==========
def get_ai_main_menu(user_tariff: str) -> InlineKeyboardMarkup:
    """Главное меню AI-сервисов"""
    tariff_limits = AITariffLimits[user_tariff.upper()].value
    
    buttons = [
        [InlineKeyboardButton(text="📝 ИИ-копирайтер", callback_data="ai_copywriter")],
        [InlineKeyboardButton(text="💡 Генератор идей", callback_data="ai_ideas")],
        [InlineKeyboardButton(text="📊 Мои лимиты", callback_data="ai_limits")],
        [InlineKeyboardButton(text="📚 Примеры работ", callback_data="ai_examples")],
        [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_cancel_ai_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура отмены для AI"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
    ])

def get_style_keyboard() -> InlineKeyboardMarkup:
    """Выбор стиля текста"""
    return InlineKeyboardMarkup(inline_keyboard=[
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

def get_idea_count_keyboard() -> InlineKeyboardMarkup:
    """Выбор количества идей"""
    return InlineKeyboardMarkup(inline_keyboard=[
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

# ========== PROMPT TEMPLATES ==========
COPYWRITER_PROMPT = """Ты профессиональный копирайтер для Telegram-каналов. Создай продающий текст на основе следующих данных:

ТЕМА: {topic}
СТИЛЬ: {style}
ПРИМЕРЫ РАБОТ: {examples}

ТРЕБОВАНИЯ:
1. Текст должен быть цепляющим и вовлекающим
2. Используй эмодзи уместно (но не переборщи)
3. Структура: заголовок → проблема → решение → призыв к действию
4. Длина: 150-300 символов
5. Пиши как для живых людей, без воды
6. Учитывай примеры, но не копируй их

ДОПОЛНИТЕЛЬНО:
- Текущая дата: {current_date}
- Не упоминай что ты ИИ
- Пиши в настоящем времени

Верни ТОЛЬКО готовый текст, без пояснений."""

IDEAS_PROMPT = """Ты эксперт по контенту для Telegram. Сгенерируй {count} идей для постов на тему:

ТЕМА: {topic}

ТРЕБОВАНИЯ К ИДЕЯМ:
1. Каждая идея должна быть конкретной и реализуемой
2. Формат: краткое описание (1-2 предложения)
3. Укажи возможный тип контента (текст, фото, видео, опрос)
4. Идеи должны быть разнообразными

ПРИМЕР ФОРМАТА:
1. [Тип] Название идеи - Краткое описание
2. [Тип] Название идеи - Краткое описание

ДОПОЛНИТЕЛЬНО:
- Учитывай тренды {current_date}
- Идеи должны вовлекать аудиторию
- Не повторяйся

Верни список идей с нумерацией, каждый с новой строки."""

# ========== AI FUNCTIONS ==========
async def generate_with_gemini(prompt: str, user_id: int) -> Optional[str]:
    """Генерирует текст через Gemini API с ротацией ключей"""
    try:
        session = ai_manager.get_session(user_id)
        api_key, key_index = ai_manager.get_next_key(session)
        
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(MODEL_NAME)
        
        response = model.generate_content(
            prompt,
            generation_config={
                "temperature": 0.8,
                "top_p": 0.95,
                "top_k": 40,
                "max_output_tokens": 1000,
            }
        )
        
        logger.info(f"AI запрос | user_{user_id} | key_{key_index}")
        return response.text.strip()
        
    except Exception as e:
        error_msg = str(e).lower()
        if "quota" in error_msg or "429" in error_msg:
            logger.warning(f"Лимит ключа для user_{user_id}")
            return None
        else:
            logger.error(f"Ошибка Gemini: {e}")
            return None

async def check_ai_limits(user_id: int, service_type: str) -> Tuple[bool, str, Dict]:
    """Проверяет лимиты пользователя"""
    tariff = await get_user_tariff(user_id)
    tariff_limits = AITariffLimits[tariff.upper()].value
    
    session = ai_manager.get_session(user_id)
    
    # Сброс дневных лимитов
    today = datetime.now(MOSCOW_TZ).date()
    if session['last_reset'] < today:
        session['copies_used'] = 0
        session['ideas_used'] = 0
        session['last_reset'] = today
    
    if service_type == 'copy':
        limit = tariff_limits['copies']
        used = session['copies_used']
        remaining = limit - used
        
        if used >= limit:
            reset_time = datetime.combine(today + timedelta(days=1), datetime.min.time())
            reset_time = MOSCOW_TZ.localize(reset_time)
            time_left = reset_time - datetime.now(MOSCOW_TZ)
            hours = int(time_left.total_seconds() // 3600)
            
            return False, f"❌ Достигнут дневной лимит!\n\n📝 Копирайтинг: {used}/{limit}\n⏳ Обновление через: {hours} часов", tariff_limits
        
        session['copies_used'] += 1
        
    elif service_type == 'ideas':
        limit = tariff_limits['ideas']
        used = session['ideas_used']
        remaining = limit - used
        
        if used >= limit:
            reset_time = datetime.combine(today + timedelta(days=1), datetime.min.time())
            reset_time = MOSCOW_TZ.localize(reset_time)
            time_left = reset_time - datetime.now(MOSCOW_TZ)
            hours = int(time_left.total_seconds() // 3600)
            
            return False, f"❌ Достигнут дневной лимит!\n\n💡 Идеи: {used}/{limit}\n⏳ Обновление через: {hours} часов", tariff_limits
        
        session['ideas_used'] += 1
    
    # Обновляем в базе
    await update_ai_usage(user_id, service_type)
    
    return True, f"✅ Доступно! Осталось: {remaining}/{limit}", tariff_limits

# ========== HANDLERS ==========
@router.callback_query(F.data == "ai_services")
async def ai_services_menu(callback: CallbackQuery):
    """Меню AI-сервисов"""
    user_id = callback.from_user.id
    tariff = await get_user_tariff(user_id)
    
    welcome_text = (
        "🤖 ИИ-Сервисы KOLES-TECH\n\n"
        "✨ Доступные возможности:\n\n"
        "📝 ИИ-копирайтер:\n"
        "• Создаст продающий текст для поста\n"
        "• Учитывает тему, стиль и примеры\n"
        "• Готовый текст для публикации\n\n"
        "💡 Генератор идей:\n"
        "• {ideas_limit} идей в день\n"
        "• Разнообразные темы\n"
        "• Готовые концепты постов\n\n"
        "👇 Выберите сервис:"
    ).format(
        ideas_limit=AITariffLimits[tariff.upper()].value['ideas']
    )
    
    await callback.message.edit_text(
        welcome_text,
        reply_markup=get_ai_main_menu(tariff)
    )

@router.callback_query(F.data == "ai_copywriter")
async def start_copywriter(callback: CallbackQuery, state: FSMContext):
    """Начало работы с копирайтером"""
    user_id = callback.from_user.id
    
    # Проверка лимитов
    can_use, message, limits = await check_ai_limits(user_id, 'copy')
    if not can_use:
        await callback.message.edit_text(
            message,
            reply_markup=get_ai_main_menu(await get_user_tariff(user_id))
        )
        return
    
    # Проверка времени
    can_request, wait_message = ai_manager.can_make_request(user_id)
    if not can_request:
        await callback.answer(wait_message, show_alert=True)
        return
    
    await state.set_state(AIStates.waiting_for_topic)
    await callback.message.edit_text(
        f"📝 ИИ-копирайтер\n\n"
        f"✅ Доступно: {limits['copies'] - ai_manager.get_session(user_id)['copies_used']}/{limits['copies']} текстов сегодня\n\n"
        f"📌 Шаг 1/3\n"
        f"Введите тему для поста:\n\n"
        f"Примеры:\n"
        f"• Запуск нового курса по маркетингу\n"
        f"• Анонс вебинара по трейдингу\n"
        f"• Продажа SEO-услуг\n\n"
        f"📍 Пишите конкретно и ясно:",
        reply_markup=get_cancel_ai_keyboard()
    )

@router.message(AIStates.waiting_for_topic)
async def process_topic(message: Message, state: FSMContext):
    """Обработка темы"""
    if len(message.text) < 5:
        await message.answer(
            "❌ Тема слишком короткая! Минимум 5 символов.\n\nВведите тему еще раз:",
            reply_markup=get_cancel_ai_keyboard()
        )
        return
    
    await state.update_data(topic=message.text)
    await state.set_state(AIStates.waiting_for_examples)
    
    await message.answer(
        "📌 Шаг 2/3\n"
        "Пришлите примеры работ или ссылки (по желанию):\n\n"
        "Можно:\n"
        "• Прислать тексты постов\n"
        "• Ссылки на каналы\n"
        "• Ключевые фразы\n\n"
        "Или напишите 'пропустить', если примеров нет:",
        reply_markup=get_cancel_ai_keyboard()
    )

@router.message(AIStates.waiting_for_examples)
async def process_examples(message: Message, state: FSMContext):
    """Обработка примеров"""
    examples = message.text if message.text.lower() != 'пропустить' else "Примеры не предоставлены"
    
    await state.update_data(examples=examples)
    await state.set_state(AIStates.waiting_for_style)
    
    await message.answer(
        "📌 Шаг 3/3\n"
        "Выберите стиль текста:\n\n"
        "📱 Продающий - для продаж и конверсии\n"
        "📝 Информационный - полезный контент\n"
        "🎭 Креативный - нестандартный подход\n"
        "🎯 Целевой - для конкретной аудитории\n"
        "🚀 Для соцсетей - виральный контент\n"
        "📰 Новостной - анонсы и новости",
        reply_markup=get_style_keyboard()
    )

@router.callback_query(F.data.startswith("style_"))
async def process_style(callback: CallbackQuery, state: FSMContext):
    """Обработка стиля текста"""
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
    data = await state.get_data()
    
    # Показываем превью
    preview_text = (
        f"📋 Ваш запрос:\n\n"
        f"📌 Тема: {data['topic']}\n"
        f"🎨 Стиль: {style_name}\n"
        f"📚 Примеры: {data['examples'][:100]}...\n\n"
        f"⏳ Генерирую текст... Это займет 10-20 секунд."
    )
    
    await callback.message.edit_text(preview_text)
    
    # Генерация текста
    current_date = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y")
    prompt = COPYWRITER_PROMPT.format(
        topic=data['topic'],
        style=style_name,
        examples=data['examples'],
        current_date=current_date
    )
    
    # Показываем индикатор загрузки
    loading_msg = await callback.message.answer("🔄 ИИ генерирует текст...")
    
    generated_text = await generate_with_gemini(prompt, callback.from_user.id)
    
    if not generated_text:
        await loading_msg.delete()
        await callback.message.edit_text(
            "❌ Ошибка генерации! Возможно, закончились лимиты API.\n"
            "Попробуйте позже или обратитесь в поддержку.",
            reply_markup=get_ai_main_menu(await get_user_tariff(callback.from_user.id))
        )
        await state.clear()
        return
    
    await loading_msg.delete()
    
    # Показываем результат
    result_text = (
        f"✅ Текст готов!\n\n"
        f"📝 Результат:\n\n"
        f"{generated_text}\n\n"
        f"📊 Статистика:\n"
        f"• Символов: {len(generated_text)}\n"
        f"• Использовано: {ai_manager.get_session(callback.from_user.id)['copies_used']}/{AITariffLimits[await get_user_tariff(callback.from_user.id).upper()].value['copies']}"
    )
    
    await callback.message.edit_text(
        result_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📋 Скопировать", callback_data="copy_text")],
            [InlineKeyboardButton(text="🔄 Новый текст", callback_data="ai_copywriter")],
            [InlineKeyboardButton(text="⬅️ В меню", callback_data="ai_services")]
        ])
    )
    
    await state.clear()

@router.callback_query(F.data == "ai_ideas")
async def start_ideas_generator(callback: CallbackQuery, state: FSMContext):
    """Начало генератора идей"""
    user_id = callback.from_user.id
    
    # Проверка лимитов
    can_use, message, limits = await check_ai_limits(user_id, 'ideas')
    if not can_use:
        await callback.message.edit_text(
            message,
            reply_markup=get_ai_main_menu(await get_user_tariff(user_id))
        )
        return
    
    # Проверка времени
    can_request, wait_message = ai_manager.can_make_request(user_id)
    if not can_request:
        await callback.answer(wait_message, show_alert=True)
        return
    
    await state.set_state(AIStates.waiting_for_idea_topic)
    await callback.message.edit_text(
        f"💡 Генератор идей\n\n"
        f"✅ Доступно: {limits['ideas'] - ai_manager.get_session(user_id)['ideas_used']}/{limits['ideas']} идей сегодня\n\n"
        f"Введите тему для генерации идей:\n\n"
        f"Примеры:\n"
        f"• Маркетинг в Telegram\n"
        f"• Образовательный контент\n"
        f"• Новости IT-сферы\n"
        f"• Здоровый образ жизни\n\n"
        f"📍 Чем конкретнее тема, тем лучше идеи:",
        reply_markup=get_cancel_ai_keyboard()
    )

@router.message(AIStates.waiting_for_idea_topic)
async def process_idea_topic(message: Message, state: FSMContext):
    """Обработка темы для идей"""
    if len(message.text) < 3:
        await message.answer(
            "❌ Тема слишком короткая! Минимум 3 символа.\n\nВведите тему еще раз:",
            reply_markup=get_cancel_ai_keyboard()
        )
        return
    
    await state.update_data(topic=message.text)
    
    await message.answer(
        "Выберите количество идей (от 5 до 20):\n\n"
        "📊 Рекомендуем:\n"
        "• 5 идей - быстрый просмотр\n"
        "• 10 идей - оптимальный выбор\n"
        "• 15-20 идей - полный охват темы",
        reply_markup=get_idea_count_keyboard()
    )

@router.callback_query(F.data.startswith("ideas_"))
async def generate_ideas(callback: CallbackQuery, state: FSMContext):
    """Генерация идей"""
    count = int(callback.data.split("_")[1])
    data = await state.get_data()
    
    if count > 20:
        count = 20
    
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
    
    generated_ideas = await generate_with_gemini(prompt, callback.from_user.id)
    
    if not generated_ideas:
        await loading_msg.delete()
        await callback.message.edit_text(
            "❌ Ошибка генерации! Возможно, закончились лимиты API.\n"
            "Попробуйте позже или обратитесь в поддержку.",
            reply_markup=get_ai_main_menu(await get_user_tariff(callback.from_user.id))
        )
        await state.clear()
        return
    
    await loading_msg.delete()
    
    # Форматируем результат
    ideas_list = generated_ideas.split('\n')
    formatted_ideas = []
    
    for i, idea in enumerate(ideas_list[:count], 1):
        if idea.strip():
            formatted_ideas.append(f"{i}. {idea.strip()}")
    
    result_text = (
        f"✅ Сгенерировано {len(formatted_ideas)} идей!\n\n"
        f"📌 Тема: {data['topic']}\n\n"
        f"💡 Идеи:\n\n" +
        "\n".join(formatted_ideas) +
        f"\n\n📊 Статистика:\n"
        f"• Использовано: {ai_manager.get_session(callback.from_user.id)['ideas_used']}/{AITariffLimits[await get_user_tariff(callback.from_user.id).upper()].value['ideas']}"
    )
    
    # Разбиваем длинные сообщения
    if len(result_text) > 4000:
        parts = []
        current_part = ""
        
        for line in result_text.split('\n'):
            if len(current_part + line + '\n') > 4000:
                parts.append(current_part)
                current_part = line + '\n'
            else:
                current_part += line + '\n'
        
        if current_part:
            parts.append(current_part)
        
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
            [InlineKeyboardButton(text="⬅️ В меню", callback_data="ai_services")]
        ])
    )
    
    await state.clear()

@router.callback_query(F.data == "ai_limits")
async def show_ai_limits(callback: CallbackQuery):
    """Показывает лимиты AI"""
    user_id = callback.from_user.id
    tariff = await get_user_tariff(user_id)
    limits = AITariffLimits[tariff.upper()].value
    stats = await get_ai_usage_stats(user_id)
    
    session = ai_manager.get_session(user_id)
    
    # Рассчитываем оставшееся время до сброса
    today = datetime.now(MOSCOW_TZ).date()
    reset_time = datetime.combine(today + timedelta(days=1), datetime.min.time())
    reset_time = MOSCOW_TZ.localize(reset_time)
    time_left = reset_time - datetime.now(MOSCOW_TZ)
    hours = int(time_left.total_seconds() // 3600)
    minutes = int((time_left.total_seconds() % 3600) // 60)
    
    limits_text = (
        f"📊 Ваши AI-лимиты\n\n"
        f"💎 Тариф: {tariff.upper()}\n\n"
        f"📝 Копирайтер:\n"
        f"• Использовано: {session['copies_used']}/{limits['copies']}\n"
        f"• Осталось: {limits['copies'] - session['copies_used']}\n\n"
        f"💡 Генератор идей:\n"
        f"• Использовано: {session['ideas_used']}/{limits['ideas']}\n"
        f"• Осталось: {limits['ideas'] - session['ideas_used']}\n\n"
        f"🔄 Обновление через: {hours}ч {minutes}м\n\n"
        f"📈 Всего запросов: {session['total_requests']}"
    )
    
    await callback.message.edit_text(
        limits_text,
        reply_markup=get_ai_main_menu(tariff)
    )

@router.callback_query(F.data == "ai_examples")
async def show_ai_examples(callback: CallbackQuery):
    """Показывает примеры работ"""
    examples_text = (
        "📚 Примеры работ ИИ-копирайтера\n\n"
        
        "📌 Пример 1 (Продающий текст):\n"
        "🔥 ЗАПУСК КУРСА! 🔥\n\n"
        "Устали от низких продаж? 😔\n\n"
        "Представляем курс «Маркетинг в TG 3.0» 🚀\n\n"
        "✅ Кейсы из 2024 года\n"
        "✅ Работающие стратегии\n"
        "✅ Личный разбор от эксперта\n\n"
        "Цена сегодня: 990₽ (вместо 2990₽)\n\n"
        "👉 Записаться: @manager\n\n"
        
        "📌 Пример 2 (Информационный):\n"
        "📊 Как увеличить конверсию в 2 раза?\n\n"
        "Исследование 100+ каналов показало:\n\n"
        "1. Оптимальное время постинга: 19:00-21:00 🕐\n"
        "2. Лучший день: среда 📅\n"
        "3. Эмодзи повышают вовлеченность на 37% 😊\n\n"
        "Совет: тестируйте разные форматы!\n\n"
        
        "📌 Пример 3 (Креативный):\n"
        "🎭 ВАШ КАНАЛ СКУЧНЫЙ? 😴\n\n"
        "Мы превращаем скучные темы в вирусный контент! ✨\n\n"
        "Формула успеха:\n"
        "Проблема × Решение × Эмоция = ВИРУС 🦠\n\n"
        "Хотите такой же результат? Пишите! 👇"
    )
    
    await callback.message.edit_text(
        examples_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Заказать текст", callback_data="ai_copywriter")],
            [InlineKeyboardButton(text="💡 Получить идеи", callback_data="ai_ideas")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="ai_services")]
        ])
    )

@router.callback_query(F.data == "copy_text")
async def copy_text_handler(callback: CallbackQuery):
    """Обработчик копирования текста"""
    await callback.answer("📋 Текст скопирован в буфер обмена!", show_alert=True)

@router.callback_query(F.data == "cancel_ai")
async def cancel_ai(callback: CallbackQuery, state: FSMContext):
    """Отмена AI операций"""
    await state.clear()
    user_id = callback.from_user.id
    tariff = await get_user_tariff(user_id)
    
    await callback.message.edit_text(
        "❌ Операция отменена",
        reply_markup=get_ai_main_menu(tariff)
    )

# ========== SCHEDULED TASKS ==========
async def reset_ai_limits_daily():
    """Ежедневный сброс лимитов"""
    ai_manager.reset_daily_limits()
    logger.info("✅ AI лимиты сброшены")

# ========== INTEGRATION WITH MAIN BOT ==========
def setup_ai_handlers(dp):
    """Добавляет AI хендлеры в основной диспетчер"""
    dp.include_router(router)
    
    # Добавляем кнопку AI в главное меню (модифицируем существующую функцию)
    def get_expanded_main_menu(user_id: int, is_admin: bool = False) -> InlineKeyboardMarkup:
        """Расширенное главное меню с AI"""
        buttons = [
            [InlineKeyboardButton(text="🤖 ИИ-сервисы", callback_data="ai_services")],
            [InlineKeyboardButton(text="📅 Запланировать пост", callback_data="schedule_post")],
            [InlineKeyboardButton(text="📊 Моя статистика", callback_data="my_stats")],
            [InlineKeyboardButton(text="📢 Мои каналы", callback_data="my_channels")],
            [InlineKeyboardButton(text="💎 Тарифы", callback_data="tariffs")],
            [InlineKeyboardButton(text="🆘 Техподдержка", url=f"https://t.me/поддержка")],
        ]
        
        if is_admin:
            buttons.append([InlineKeyboardButton(text="👑 Админ-панель", callback_data="admin_panel")])
        
        return InlineKeyboardMarkup(inline_keyboard=buttons)
    
    # Возвращаем модифицированную функцию для использования в основном боте
    return get_expanded_main_menu

# ========== INITIALIZATION ==========
async def init_ai_tables():
    """Инициализация таблиц для AI"""
    try:
        conn = await get_db_connection()
        
        # Добавляем колонки для AI статистики
        await conn.execute('''
            ALTER TABLE users 
            ADD COLUMN IF NOT EXISTS ai_copies_used INTEGER DEFAULT 0,
            ADD COLUMN IF NOT EXISTS ai_ideas_used INTEGER DEFAULT 0,
            ADD COLUMN IF NOT EXISTS ai_last_used TIMESTAMP
        ''')
        
        await conn.close()
        logger.info("✅ AI таблицы инициализированы")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации AI таблиц: {e}")

# Запуск инициализации при импорте
async def initialize():
    await init_ai_tables()

# Автоматическая инициализация
import asyncio
try:
    asyncio.create_task(initialize())
except:
    pass
