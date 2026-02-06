import os
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any
from enum import Enum
import json
import re

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
import google.generativeai as genai

# ========== КОНФИГУРАЦИЯ ==========
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
HELP_URL = os.getenv("HELP_URL", "https://telegra.ph/")
EXAMPLES_URL = os.getenv("EXAMPLES_URL", "https://telegra.ph/")
PRIVACY_URL = os.getenv("PRIVACY_URL", "https://telegra.ph/")

# ========== КОНФИГУРАЦИЯ AI ==========
# Загружаем API ключи из переменных окружения
GEMINI_API_KEYS_STR = os.getenv("GEMINI_API_KEYS", "")
if GEMINI_API_KEYS_STR:
    try:
        GEMINI_API_KEYS = json.loads(GEMINI_API_KEYS_STR)
    except json.JSONDecodeError:
        # Пробуем разбить по запятым
        keys = [k.strip() for k in GEMINI_API_KEYS_STR.split(',') if k.strip()]
        GEMINI_API_KEYS = keys if keys else []
else:
    GEMINI_API_KEYS = []

if not GEMINI_API_KEYS:
    print("⚠️  ВНИМАНИЕ: Нет API ключей Gemini в переменных окружения")
    print("🔄 Использую тестовые ключи (в продакшене добавьте реальные ключи)")
    GEMINI_API_KEYS = [
        "AIzaSyA2j48JnmiuQKf6uAfzHSg0vAW1gkN7ISc",
        "AIzaSyCsq2YBVbc0mxoaQcjnGnd3qasoVZaucQk",
        "AIzaSyCkvLqyIoX4M_dvyG4Tyy1ujpuK_ia-BtQ"
    ]

# Модель из переменных окружения
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
ALTERNATIVE_MODELS = [
    "gemini-2.5-flash",
    "gemini-1.5-pro",
    "gemini-1.0-pro"
]

# Настройки ротации
MAX_RETRIES_PER_REQUEST = 3  # Максимум 3 попытки на запрос пользователя
REQUESTS_PER_KEY = int(os.getenv("REQUESTS_PER_KEY", "3"))
REQUEST_COOLDOWN = int(os.getenv("REQUEST_COOLDOWN", "30"))
KEY_COOLDOWN = int(os.getenv("KEY_COOLDOWN", "300"))

MOSCOW_TZ = pytz.timezone('Europe/Moscow')
POST_CHARACTER_LIMIT = 4000

# ========== ТАРИФНАЯ СИСТЕМА ==========
class Tariff(Enum):
    MINI = "mini"
    STANDARD = "standard"
    VIP = "vip"
    ADMIN = "admin"

TARIFFS = {
    Tariff.MINI.value: {
        "name": "🚀 Mini",
        "icon": "🚀",
        "price": 0,
        "currency": "USD",
        "channels_limit": 1,
        "daily_posts_limit": 2,
        "ai_copies_limit": 1,
        "ai_ideas_limit": 10,
        "color": "#3498db",
        "description": "Бесплатный стартовый тариф",
        "features": [
            "1 подключенный канал",
            "2 поста в день",
            "1 AI-копирайтинг в день",
            "10 AI-идей в день",
            "Базовые функции"
        ]
    },
    Tariff.STANDARD.value: {
        "name": "⭐ Standard",
        "icon": "⭐",
        "price": 4,
        "currency": "USD",
        "channels_limit": 2,
        "daily_posts_limit": 6,
        "ai_copies_limit": 3,
        "ai_ideas_limit": 30,
        "color": "#9b59b6",
        "description": "Для активных пользователей",
        "features": [
            "2 подключенных канала",
            "6 постов в день",
            "3 AI-копирайтинга в день",
            "30 AI-идей в день",
            "Приоритетная очередь"
        ]
    },
    Tariff.VIP.value: {
        "name": "👑 VIP",
        "icon": "👑",
        "price": 7,
        "currency": "USD",
        "channels_limit": 3,
        "daily_posts_limit": 12,
        "ai_copies_limit": 7,
        "ai_ideas_limit": 50,
        "color": "#f39c12",
        "description": "Максимальные возможности",
        "features": [
            "3 подключенных канала",
            "12 постов в день",
            "7 AI-копирайтингов в день",
            "50 AI-идей в день",
            "Экспресс-поддержка",
            "Расширенная статистика"
        ]
    },
    Tariff.ADMIN.value: {
        "name": "⚡ Admin",
        "icon": "⚡",
        "price": 0,
        "currency": "USD",
        "channels_limit": 999,
        "daily_posts_limit": 999,
        "ai_copies_limit": 999,
        "ai_ideas_limit": 999,
        "color": "#e74c3c",
        "description": "Безлимитный доступ",
        "features": [
            "Неограниченно каналов",
            "Неограниченно постов",
            "Неограниченно AI-запросов",
            "Все функции VIP",
            "Админ-панель"
        ]
    }
}

# ========== НАСТРОЙКА ==========
logging.basicConfig(
    level=logging.INFO,
    format='\033[94m%(asctime)s\033[0m - \033[92m%(name)s\033[0m - \033[93m%(levelname)s\033[0m - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)
scheduler = AsyncIOScheduler(timezone=MOSCOW_TZ)

# ========== МЕНЕДЖЕР СЕССИЙ AI С УЛУЧШЕННОЙ РОТАЦИЕЙ ==========
class AISessionManager:
    def __init__(self):
        self.sessions: Dict[int, Dict] = {}
        self.key_stats = {key: {"requests": 0, "errors": 0, "blocked_until": None, "error_403": 0, "error_429": 0, "error_500": 0} for key in GEMINI_API_KEYS}
        self.last_request_time: Dict[int, datetime] = {}
        self.current_model_index = 0
        self.models = [GEMINI_MODEL] + [m for m in ALTERNATIVE_MODELS if m != GEMINI_MODEL]
        
    def get_session(self, user_id: int) -> Dict:
        """Получает или создает сессию пользователя"""
        if user_id not in self.sessions:
            self.sessions[user_id] = {
                'history': [],
                'current_key_index': 0,
                'request_count': 0,
                'total_requests': 0,
                'copies_used': 0,
                'ideas_used': 0,
                'last_reset': datetime.now(MOSCOW_TZ).date(),
                'current_request_retries': 0,
                'last_successful_key': None,
                'word_count': 200,
                'last_error': None
            }
        return self.sessions[user_id]
    
    def get_available_key(self, user_id: int) -> Tuple[Optional[str], int, str]:
        """Получает доступный API ключ с интеллектуальной ротацией"""
        session = self.get_session(user_id)
        
        # Если есть успешный ключ - пробуем его сначала
        if session['last_successful_key'] and session['last_successful_key'] in self.key_stats:
            key_info = self.key_stats[session['last_successful_key']]
            if not key_info['blocked_until'] or key_info['blocked_until'] < datetime.now(MOSCOW_TZ):
                if key_info['error_403'] < REQUESTS_PER_KEY:
                    return session['last_successful_key'], GEMINI_API_KEYS.index(session['last_successful_key']), self.get_current_model()
        
        # Ищем доступные ключи
        available_keys = []
        for key_index, key in enumerate(GEMINI_API_KEYS):
            key_info = self.key_stats[key]
            
            # Пропускаем заблокированные ключи
            if key_info['blocked_until'] and key_info['blocked_until'] > datetime.now(MOSCOW_TZ):
                continue
            
            # Пропускаем ключи с 3+ ошибками 403
            if key_info['error_403'] >= REQUESTS_PER_KEY:
                continue
            
            # Вычисляем приоритет (меньше ошибок - выше приоритет)
            priority = key_info['error_403'] * 100 + key_info['error_429'] * 10 + key_info['error_500']
            
            available_keys.append({
                'key': key,
                'index': key_index,
                'priority': priority,
                'errors': key_info['errors'],
                'requests': key_info['requests']
            })
        
        if not available_keys:
            # Если все ключи заблокированы, сбрасываем счетчики через 5 минут
            for key in self.key_stats:
                if self.key_stats[key]['blocked_until'] and self.key_stats[key]['blocked_until'] < datetime.now(MOSCOW_TZ) + timedelta(minutes=5):
                    self.key_stats[key]['error_403'] = 0
                    self.key_stats[key]['blocked_until'] = None
            
            # Пробуем первый ключ
            if GEMINI_API_KEYS:
                key = GEMINI_API_KEYS[0]
                return key, 0, self.get_current_model()
            return None, -1, self.get_current_model()
        
        # Выбираем ключ с наивысшим приоритетом (меньше ошибок)
        best_key = min(available_keys, key=lambda x: x['priority'])
        
        session['current_key_index'] = best_key['index']
        session['request_count'] += 1
        self.key_stats[best_key['key']]['requests'] += 1
        
        return best_key['key'], best_key['index'], self.get_current_model()
    
    def mark_key_error(self, key: str, error_type: str = "generic"):
        """Отмечает ошибку для ключа"""
        if key in self.key_stats:
            self.key_stats[key]['errors'] += 1
            
            if error_type == "403":
                self.key_stats[key]['error_403'] += 1
                logger.warning(f"🔑 Ключ {key[:15]}... получил 403 ошибку. Всего: {self.key_stats[key]['error_403']}/{REQUESTS_PER_KEY}")
                
                if self.key_stats[key]['error_403'] >= REQUESTS_PER_KEY:
                    self.key_stats[key]['blocked_until'] = datetime.now(MOSCOW_TZ) + timedelta(seconds=KEY_COOLDOWN)
                    logger.error(f"🔒 Ключ {key[:15]}... заблокирован на {KEY_COOLDOWN} сек (3 ошибки 403)")
                    
            elif error_type == "429":
                self.key_stats[key]['error_429'] += 1
                logger.warning(f"🔑 Ключ {key[:15]}... получил 429 ошибку (лимит)")
                
            elif error_type == "500":
                self.key_stats[key]['error_500'] += 1
                logger.warning(f"🔑 Ключ {key[:15]}... получил 500 ошибку")
    
    def mark_key_successful(self, key: str, user_id: int):
        """Отмечает ключ как успешный для пользователя"""
        if key in self.key_stats:
            # Частично сбрасываем счетчики при успехе
            self.key_stats[key]['error_403'] = max(0, self.key_stats[key]['error_403'] - 1)
            session = self.get_session(user_id)
            session['last_successful_key'] = key
            session['current_request_retries'] = 0
            session['last_error'] = None
    
    def increment_request_retry(self, user_id: int):
        """Увеличивает счетчик попыток для текущего запроса"""
        session = self.get_session(user_id)
        session['current_request_retries'] += 1
        return session['current_request_retries']
    
    def get_request_retries(self, user_id: int) -> int:
        """Получает количество попыток для текущего запроса"""
        return self.get_session(user_id)['current_request_retries']
    
    def set_last_error(self, user_id: int, error: str):
        """Сохраняет последнюю ошибку"""
        self.get_session(user_id)['last_error'] = error
    
    def get_last_error(self, user_id: int) -> Optional[str]:
        """Получает последнюю ошибку"""
        return self.get_session(user_id)['last_error']
    
    def get_current_model(self) -> str:
        """Возвращает текущую модель"""
        return self.models[self.current_model_index % len(self.models)]
    
    def rotate_model(self):
        """Переключает на следующую модель"""
        self.current_model_index += 1
        logger.info(f"🔄 Переключили модель на: {self.get_current_model()}")
    
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
                session['current_request_retries'] = 0
                session['last_error'] = None
    
    def set_word_count(self, user_id: int, word_count: int):
        """Устанавливает количество слов для генерации"""
        session = self.get_session(user_id)
        session['word_count'] = max(50, min(1000, word_count))
    
    def get_word_count(self, user_id: int) -> int:
        """Получает количество слов для генерации"""
        return self.get_session(user_id)['word_count']
    
    def get_stats_summary(self) -> Dict[str, Any]:
        """Возвращает статистику по ключам"""
        total_requests = sum(stat['requests'] for stat in self.key_stats.values())
        total_errors = sum(stat['errors'] for stat in self.key_stats.values())
        blocked_keys = sum(1 for stat in self.key_stats.values() if stat['blocked_until'] and stat['blocked_until'] > datetime.now(MOSCOW_TZ))
        
        return {
            'total_keys': len(GEMINI_API_KEYS),
            'total_requests': total_requests,
            'total_errors': total_errors,
            'blocked_keys': blocked_keys,
            'active_sessions': len(self.sessions),
            'current_model': self.get_current_model()
        }

ai_manager = AISessionManager()

# ========== ПРОМПТЫ ДЛЯ AI ==========
COPYWRITER_PROMPT = """Ты профессиональный копирайтер для Telegram-каналов. Создай продающий текст на основе следующих данных:

🎯 ТЕМА: {topic}
🎨 СТИЛЬ: {style}
📚 ПРИМЕРЫ: {examples}
📝 ОБЪЕМ: {word_count} слов

📌 ТРЕБОВАНИЯ:
1. Текст должен быть цепляющим и вовлекающим
2. Используй эмодзи уместно (2-3 на абзац)
3. Структура: Заголовок → Проблема → Решение → Призыв к действию
4. ТОЧНО {word_count} слов (±10%)
5. Пиши как для живых людей, без воды
6. Учитывай примеры, но не копируй их

📅 ДОПОЛНИТЕЛЬНО:
- Текущая дата: {current_date}
- Не упоминай что ты ИИ
- Пиши в настоящем времени
- Сделай текст готовым к публикации

🎪 Верни ТОЛЬКО готовый текст, без пояснений и обрамления."""

IDEAS_PROMPT = """Ты эксперт по контенту для Telegram. Сгенерируй {count} идей для постов на тему:

🎯 ТЕМА: {topic}

📌 ТРЕБОВАНИЯ К ИДЕЯМ:
1. Каждая идея должна быть конкретной и реализуемой
2. Формат: [Тип контента] Название идеи - Краткое описание (1-2 предложения)
3. Укажи тип контента: 📝 Текст, 📷 Фото, 🎥 Видео, 📊 Опрос, 🎭 Квиз
4. Идеи должны быть разнообразными и вовлекающими

✨ ПРИМЕР ФОРМАТА:
1. [📝 Текст] 5 ошибок новичков - Расскажи про частые ошибки с примерами
2. [📷 Фото] До/После - Покажи результат работы на фото
3. [🎥 Видео] Обзор инструмента - Сними короткий обзор полезного сервиса

📅 АКТУАЛЬНОСТЬ:
- Учитывай тренды {current_date}
- Идеи должны быть актуальными для Telegram
- Не повторяйся, будь креативным

📋 Верни список идей с нумерацией, каждый с новой строки. Только список, без вступлений и заключений."""

# ========== ФУНКЦИЯ ГЕНЕРАЦИИ С РОТАЦИЕЙ КЛЮЧЕЙ ==========
async def generate_with_gemini(prompt: str, user_id: int) -> Tuple[Optional[str], Dict[str, Any]]:
    """Генерирует текст через Gemini API с ротацией ключей"""
    
    session = ai_manager.get_session(user_id)
    session['current_request_retries'] = 0
    session['last_error'] = None
    
    metadata = {
        'attempts': 0,
        'keys_tried': [],
        'models_tried': [],
        'errors': [],
        'success': False
    }
    
    for attempt in range(MAX_RETRIES_PER_REQUEST):
        try:
            metadata['attempts'] += 1
            
            # Получаем доступный ключ
            key, key_index, model_name = ai_manager.get_available_key(user_id)
            
            if not key:
                error_msg = "❌ Нет доступных API ключей"
                ai_manager.set_last_error(user_id, error_msg)
                metadata['errors'].append(error_msg)
                return None, metadata
            
            # Проверяем, не использовали ли уже этот ключ в этой попытке
            if key in metadata['keys_tried']:
                # Все ключи уже перепробованы в этой попытке
                if len(metadata['keys_tried']) >= len(GEMINI_API_KEYS):
                    error_msg = "❌ Все ключи перепробованы"
                    ai_manager.set_last_error(user_id, error_msg)
                    metadata['errors'].append(error_msg)
                    return None, metadata
                # Пробуем другой ключ
                continue
            
            metadata['keys_tried'].append(key)
            metadata['models_tried'].append(model_name)
            
            # Настраиваем Gemini
            genai.configure(api_key=key)
            model = genai.GenerativeModel(
                model_name=model_name,
                generation_config={
                    "temperature": 0.8,
                    "top_p": 0.95,
                    "top_k": 40,
                    "max_output_tokens": 4000,
                }
            )
            
            logger.info(f"🔑 Попытка {attempt+1}: ключ {key_index}, модель {model_name}")
            
            # Отправляем запрос
            response = model.generate_content(prompt)
            
            # Если успешно
            ai_manager.mark_key_successful(key, user_id)
            metadata['success'] = True
            metadata['final_key'] = key
            metadata['final_model'] = model_name
            metadata['final_attempt'] = attempt + 1
            
            logger.info(f"✅ Успех после {attempt+1} попыток: user_{user_id}, ключ {key_index}")
            return response.text.strip(), metadata
            
        except Exception as e:
            error_str = str(e)
            metadata['errors'].append(f"Попытка {attempt+1}: {error_str}")
            
            # Определяем тип ошибки
            error_type = "generic"
            if "429" in error_str or "quota" in error_str.lower() or "resource exhausted" in error_str.lower():
                error_type = "429"
                logger.warning(f"🔄 Попытка {attempt+1}: Лимит ключа для user_{user_id}")
            elif "403" in error_str or "permission denied" in error_str.lower():
                error_type = "403"
                logger.warning(f"🔄 Попытка {attempt+1}: Ошибка 403 для user_{user_id}")
            elif "500" in error_str or "503" in error_str or "unavailable" in error_str.lower():
                error_type = "500"
                logger.warning(f"🔄 Попытка {attempt+1}: Ошибка сервера для user_{user_id}")
            else:
                logger.error(f"❌ Попытка {attempt+1}: Неизвестная ошибка для user_{user_id}: {e}")
            
            # Отмечаем ошибку для ключа
            if 'key' in locals():
                ai_manager.mark_key_error(key, error_type)
            
            # Сохраняем ошибку в сессии
            ai_manager.set_last_error(user_id, f"{error_type}: {error_str[:100]}")
            
            # Увеличиваем счетчик попыток
            ai_manager.increment_request_retry(user_id)
            
            # Если это не последняя попытка - ждем и продолжаем
            if attempt < MAX_RETRIES_PER_REQUEST - 1:
                wait_time = 1 * (attempt + 1)  # Увеличиваем задержку
                logger.info(f"⏳ Жду {wait_time} сек перед следующей попыткой...")
                await asyncio.sleep(wait_time)
                
                # Ротируем модель на следующей попытке
                if attempt % 2 == 0:
                    ai_manager.rotate_model()
            else:
                logger.error(f"❌ Все {MAX_RETRIES_PER_REQUEST} попыток исчерпаны для user_{user_id}")
    
    return None, metadata

# ========== ФУНКЦИИ БАЗЫ ДАННЫХ ==========
async def get_db_connection():
    """Создает подключение к базе данных"""
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
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        raise

async def init_db():
    """Инициализирует таблицы базы данных"""
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
                ai_copies_used INTEGER DEFAULT 0,
                ai_ideas_used INTEGER DEFAULT 0,
                ai_last_used TIMESTAMP,
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

# ... (остальные функции базы данных остаются такими же, как в исходном коде)
# Для экономии места я оставлю только основные изменения

# ========== УЛУЧШЕННЫЕ КЛАВИАТУРЫ С ЭМОДЗИ ==========
def format_tariff_card(tariff_id: str, is_current: bool = False) -> str:
    """Форматирует карточку тарифа"""
    tariff = TARIFFS.get(tariff_id)
    if not tariff:
        return ""
    
    card = ""
    if is_current:
        card += f"✅ {tariff['icon']} <b>{tariff['name']}</b> (Ваш тариф)\n\n"
    else:
        card += f"{tariff['icon']} <b>{tariff['name']}</b>\n"
    
    if tariff['price'] == 0:
        card += "💰 <b>Бесплатно</b>\n\n"
    else:
        card += f"💰 <b>{tariff['price']} {tariff['currency']}/месяц</b>\n\n"
    
    card += "📊 <b>Лимиты:</b>\n"
    card += f"• Каналов: {tariff['channels_limit']}\n"
    card += f"• Постов/день: {tariff['daily_posts_limit']}\n"
    card += f"• AI-копирайтингов: {tariff['ai_copies_limit']}\n"
    card += f"• AI-идей: {tariff['ai_ideas_limit']}\n\n"
    
    if tariff['description']:
        card += f"📝 {tariff['description']}\n"
    
    return card

def get_main_menu(user_id: int, is_admin: bool = False) -> InlineKeyboardMarkup:
    """Главное меню с улучшенным дизайном"""
    buttons = [
        [InlineKeyboardButton(text="🤖 ИИ-сервисы", callback_data="ai_services")],
        [InlineKeyboardButton(text="📅 Планировщик постов", callback_data="schedule_post")],
        [InlineKeyboardButton(text="📊 Моя статистика", callback_data="my_stats")],
        [InlineKeyboardButton(text="📢 Мои каналы", callback_data="my_channels")],
        [InlineKeyboardButton(text="💎 Тарифы и оплата", callback_data="tariffs")],
        [
            InlineKeyboardButton(text="🆘 Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME}"),
            InlineKeyboardButton(text="📚 Помощь", callback_data="help_command")
        ]
    ]
    
    if is_admin:
        buttons.append([InlineKeyboardButton(text="👑 Админ-панель", callback_data="admin_panel")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_tariffs_keyboard(user_tariff: str = 'mini') -> InlineKeyboardMarkup:
    """Клавиатура тарифов с визуальным оформлением"""
    buttons = []
    
    for tariff_id in ['mini', 'standard', 'vip']:
        tariff_info = TARIFFS.get(tariff_id)
        if not tariff_info:
            continue
            
        name = tariff_info['name']
        
        if tariff_id == user_tariff:
            button_text = f"✅ {name} (текущий)"
        else:
            if tariff_info['price'] == 0:
                button_text = f"{tariff_info['icon']} {name} - Бесплатно"
            else:
                button_text = f"{tariff_info['icon']} {name} - {tariff_info['price']} {tariff_info['currency']}"
        
        buttons.append([InlineKeyboardButton(
            text=button_text,
            callback_data=f"tariff_info_{tariff_id}"
        )])
    
    buttons.append([
        InlineKeyboardButton(text="🕐 Время по МСК", callback_data="check_time"),
        InlineKeyboardButton(text="💬 Консультация", url=f"https://t.me/{ADMIN_CONTACT.replace('@', '')}")
    ])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_ai_main_menu(user_tariff: str) -> InlineKeyboardMarkup:
    """Главное меню AI-сервисов"""
    buttons = [
        [InlineKeyboardButton(text="📝 ИИ-копирайтер", callback_data="ai_copywriter")],
        [InlineKeyboardButton(text="💡 Генератор идей", callback_data="ai_ideas")],
        [InlineKeyboardButton(text="📊 Мои AI-лимиты", callback_data="ai_limits")],
        [InlineKeyboardButton(text="📚 Примеры работ", callback_data="ai_examples")],
        [
            InlineKeyboardButton(text="🆘 Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME}"),
            InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")
        ]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_retry_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для повторной попытки"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Попробовать снова", callback_data="ai_copywriter")],
        [InlineKeyboardButton(text="🆘 Поддержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME}")],
        [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
    ])

# ========== STATES ==========
class PostStates(StatesGroup):
    waiting_for_channel = State()
    waiting_for_content = State()
    waiting_for_date = State()
    waiting_for_time = State()
    waiting_for_confirmation = State()

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
    waiting_for_confirm_assign = State()

# ========== ОСНОВНЫЕ ОБРАБОТЧИКИ ==========
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
            SET username = EXCLUDED.username, first_name = EXCLUDED.first_name
        ''', user_id, username, first_name, is_admin, 'mini' if not is_admin else 'admin')
        await conn.close()
    except Exception as e:
        logger.error(f"❌ Ошибка регистрации пользователя {user_id}: {e}")
    
    # Получаем тариф
    current_tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(current_tariff, TARIFFS['mini'])
    
    # Красивый текст приветствия
    welcome_text = (
        f"✨ <b>Добро пожаловать, {first_name}!</b>\n\n"
        f"🚀 <b>KOLES-TECH AI Bot</b> — ваш помощник в создании и планировании контента\n\n"
        
        f"🎯 <b>Что я умею:</b>\n"
        f"• 🤖 <b>AI-копирайтер</b> — пишу продающие тексты\n"
        f"• 💡 <b>Генератор идей</b> — создаю темы для постов\n"
        f"• 📅 <b>Планировщик</b> — публикую посты автоматически\n"
        f"• 📊 <b>Аналитика</b> — показываю статистику\n\n"
        
        f"💎 <b>Ваш тариф:</b> {tariff_info['icon']} {tariff_info['name']}\n\n"
        
        f"📍 <b>Время публикации:</b> Указывается по Москве\n\n"
        
        f"👇 <b>Выберите действие:</b>"
    )
    
    await message.answer(welcome_text, parse_mode='HTML', reply_markup=get_main_menu(user_id, is_admin))

@router.message(Command("help"))
@router.callback_query(F.data == "help_command")
async def show_help(message_or_callback: Message | CallbackQuery):
    """Показывает помощь"""
    help_text = (
        f"📚 <b>Помощь по использованию бота</b>\n\n"
        
        f"🤖 <b>AI-сервисы:</b>\n"
        f"• <b>Копирайтер</b> — создает текст по вашей теме\n"
        f"• <b>Генератор идей</b> — предлагает идеи для постов\n"
        f"• <b>Лимиты</b> обновляются ежедневно в 00:00\n\n"
        
        f"📅 <b>Планирование поста:</b>\n"
        f"1. Выберите «Планировщик постов»\n"
        f"2. Выберите канал\n"
        f"3. Отправьте контент\n"
        f"4. Укажите дату и время\n"
        f"5. Подтвердите публикацию\n\n"
        
        f"💎 <b>Тарифы:</b>\n"
        f"• 🚀 <b>Mini</b> — 1 копирайт, 10 идей, 1 канал, 2 поста\n"
        f"• ⭐ <b>Standard</b> — 3 копирайта, 30 идей, 2 канала, 6 постов\n"
        f"• 👑 <b>VIP</b> — 7 копирайтов, 50 идей, 3 канала, 12 постов\n\n"
        
        f"🔄 <b>Система ротации ключей:</b>\n"
        f"• При ошибке 403/429/500 ключ меняется автоматически\n"
        f"• Максимум 3 попытки на запрос\n"
        f"• Ключи восстанавливаются через 5 минут\n\n"
        
        f"🔗 <b>Ссылки:</b>\n"
        f"• 📚 <a href='{HELP_URL}'>Полная документация</a>\n"
        f"• 📝 <a href='{EXAMPLES_URL}'>Примеры работ</a>\n"
        f"• 🔒 <a href='{PRIVACY_URL}'>Политика конфиденциальности</a>\n\n"
        
        f"🆘 <b>Поддержка:</b> @{SUPPORT_BOT_USERNAME}\n"
        f"💬 <b>По оплате:</b> @{ADMIN_CONTACT.replace('@', '')}"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🆘 Техподдержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME}")],
        [InlineKeyboardButton(text="📚 Примеры работ", url=EXAMPLES_URL)],
        [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
    ])
    
    if isinstance(message_or_callback, Message):
        await message_or_callback.answer(help_text, parse_mode='HTML', reply_markup=keyboard, disable_web_page_preview=True)
    else:
        await message_or_callback.message.edit_text(help_text, parse_mode='HTML', reply_markup=keyboard, disable_web_page_preview=True)

# ========== AI ОБРАБОТЧИКИ С УЛУЧШЕННОЙ ОБРАБОТКОЙ ОШИБОК ==========
@router.callback_query(F.data == "ai_copywriter")
async def start_copywriter(callback: CallbackQuery, state: FSMContext):
    """Начинает процесс создания текста"""
    user_id = callback.from_user.id
    
    # Проверка лимитов
    tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    session = ai_manager.get_session(user_id)
    
    if session['copies_used'] >= tariff_info['ai_copies_limit']:
        await callback.message.edit_text(
            f"❌ <b>Достигнут дневной лимит!</b>\n\n"
            f"📝 Копирайтинг: {session['copies_used']}/{tariff_info['ai_copies_limit']}\n\n"
            f"🔄 Лимиты обновятся в 00:00 по Москве",
            parse_mode='HTML',
            reply_markup=get_ai_main_menu(tariff)
        )
        return
    
    # Проверка времени
    can_request, wait_message = ai_manager.can_make_request(user_id)
    if not can_request:
        await callback.answer(wait_message, show_alert=True)
        return
    
    await state.set_state(AIStates.waiting_for_topic)
    
    await callback.message.edit_text(
        f"🤖 <b>ИИ-копирайтер</b>\n\n"
        f"✅ <b>Доступно:</b> {tariff_info['ai_copies_limit'] - session['copies_used']}/{tariff_info['ai_copies_limit']} текстов сегодня\n\n"
        
        f"📌 <b>Шаг 1/4: Тема</b>\n"
        f"Введите тему для поста:\n\n"
        
        f"✨ <b>Примеры хороших тем:</b>\n"
        f"• Запуск нового курса по маркетингу\n"
        f"• Анонс вебинара по трейдингу\n"
        f"• Продажа SEO-услуг для малого бизнеса\n"
        f"• Обзор нового приложения для планирования\n\n"
        
        f"📍 <b>Чем конкретнее тема, тем лучше результат!</b>",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
        ])
    )

@router.message(AIStates.waiting_for_topic)
async def process_topic(message: Message, state: FSMContext):
    """Обрабатывает тему"""
    if len(message.text) < 5:
        await message.answer(
            "❌ <b>Тема слишком короткая!</b>\nМинимум 5 символов.\n\nВведите тему еще раз:",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
            ])
        )
        return
    
    await state.update_data(topic=message.text)
    await state.set_state(AIStates.waiting_for_examples)
    
    await message.answer(
        "📌 <b>Шаг 2/4: Примеры</b>\n\n"
        "Пришлите примеры работ или ссылки (по желанию):\n\n"
        "📋 <b>Можно:</b>\n"
        "• Прислать тексты постов\n"
        "• Ссылки на каналы\n"
        "• Ключевые фразы\n"
        "• Стилистические примеры\n\n"
        "📍 <b>Или напишите</b> «пропустить», если примеров нет:",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
        ])
    )

@router.message(AIStates.waiting_for_examples)
async def process_examples(message: Message, state: FSMContext):
    """Обрабатывает примеры"""
    examples = message.text if message.text.lower() != 'пропустить' else "Примеры не предоставлены"
    
    await state.update_data(examples=examples)
    await state.set_state(AIStates.waiting_for_style)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
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
    
    await message.answer(
        "📌 <b>Шаг 3/4: Стиль</b>\n\n"
        "Выберите стиль текста:\n\n"
        "📱 <b>Продающий</b> — для продаж и конверсии\n"
        "📝 <b>Информационный</b> — полезный контент\n"
        "🎭 <b>Креативный</b> — нестандартный подход\n"
        "🎯 <b>Целевой</b> — для конкретной аудитории\n"
        "🚀 <b>Для соцсетей</b> — виральный контент\n"
        "📰 <b>Новостной</b> — анонсы и новости",
        parse_mode='HTML',
        reply_markup=keyboard
    )

@router.callback_query(F.data.startswith("style_"))
async def process_style(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор стиля"""
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
    
    current_word_count = ai_manager.get_word_count(callback.from_user.id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="50 слов", callback_data="words_50"),
            InlineKeyboardButton(text="100 слов", callback_data="words_100"),
            InlineKeyboardButton(text="150 слов", callback_data="words_150")
        ],
        [
            InlineKeyboardButton(text="200 слов", callback_data="words_200"),
            InlineKeyboardButton(text="250 слов", callback_data="words_250"),
            InlineKeyboardButton(text="300 слов", callback_data="words_300")
        ],
        [
            InlineKeyboardButton(text="📝 Свое значение", callback_data="words_custom"),
            InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")
        ]
    ])
    
    await callback.message.edit_text(
        f"📌 <b>Шаг 4/4: Объем текста</b>\n\n"
        f"Выберите количество слов для текста:\n\n"
        f"📊 <b>Рекомендации:</b>\n"
        f"• 50-100 слов — короткие анонсы\n"
        f"• 150-200 слов — стандартные посты\n"
        f"• 250-300 слов — подробные статьи\n\n"
        f"📍 <b>Текущая настройка:</b> {current_word_count} слов",
        parse_mode='HTML',
        reply_markup=keyboard
    )

@router.callback_query(F.data.startswith("words_"))
async def process_word_count(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор количества слов"""
    if callback.data == "words_custom":
        await callback.message.edit_text(
            "📝 <b>Свое количество слов</b>\n\n"
            "Введите нужное количество слов (от 50 до 1000):\n\n"
            "✨ <b>Примеры:</b>\n"
            "• 80 — короткий анонс\n"
            "• 150 — стандартный пост\n"
            "• 400 — подробная статья\n"
            "• 600 — длинный обзор",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
            ])
        )
        return
    
    try:
        word_count = int(callback.data.split("_")[1])
        await generate_ai_text(callback, state, word_count)
    except ValueError:
        await callback.answer("❌ Ошибка в количестве слов", show_alert=True)

@router.message(AIStates.waiting_for_word_count)
async def process_custom_word_count(message: Message, state: FSMContext):
    """Обрабатывает кастомное количество слов"""
    try:
        word_count = int(message.text.strip())
        if word_count < 50 or word_count > 1000:
            await message.answer(
                "❌ <b>Количество слов должно быть от 50 до 1000!</b>\n\n"
                "Попробуйте еще раз:",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
                ])
            )
            return
        
        user_id = message.from_user.id
        ai_manager.set_word_count(user_id, word_count)
        
        data = await state.get_data()
        await generate_ai_text_from_message(message, data, word_count)
        
    except ValueError:
        await message.answer(
            "❌ <b>Введите число!</b>\n\nПример: 150, 200, 300",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
            ])
        )

async def generate_ai_text(callback: CallbackQuery, state: FSMContext, word_count: int):
    """Генерирует текст AI"""
    user_id = callback.from_user.id
    ai_manager.set_word_count(user_id, word_count)
    
    data = await state.get_data()
    
    # Показываем превью запроса
    preview_text = (
        f"🎯 <b>Ваш запрос:</b>\n\n"
        f"📌 <b>Тема:</b> {data['topic']}\n"
        f"🎨 <b>Стиль:</b> {data['style']}\n"
        f"📝 <b>Слов:</b> {word_count}\n"
        f"📚 <b>Примеры:</b> {data['examples'][:100]}...\n\n"
        f"🔄 <b>Генерирую текст...</b>\n"
        f"Пробую разные ключи (макс. 3 попытки)"
    )
    
    await callback.message.edit_text(preview_text, parse_mode='HTML')
    
    # Генерируем текст
    current_date = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y")
    prompt = COPYWRITER_PROMPT.format(
        topic=data['topic'],
        style=data['style'],
        examples=data['examples'],
        word_count=word_count,
        current_date=current_date
    )
    
    # Показываем процесс
    status_msg = await callback.message.answer("🔄 <b>Попытка 1/3:</b> Ищу доступный ключ...", parse_mode='HTML')
    
    generated_text, metadata = await generate_with_gemini(prompt, user_id)
    
    # Удаляем статус сообщение
    await status_msg.delete()
    
    if not generated_text:
        # Показываем ошибку с деталями
        error_details = ai_manager.get_last_error(user_id) or "Неизвестная ошибка"
        attempts = ai_manager.get_request_retries(user_id)
        
        error_text = (
            f"❌ <b>Не удалось сгенерировать текст!</b>\n\n"
            f"📊 <b>Детали:</b>\n"
            f"• Попыток: {attempts}/{MAX_RETRIES_PER_REQUEST}\n"
            f"• Ошибка: {error_details}\n\n"
            f"🔧 <b>Возможные причины:</b>\n"
            f"• Все ключи API временно недоступны\n"
            f"• Закончились лимиты на всех ключах\n"
            f"• Проблемы с сетью или сервером\n\n"
            f"💡 <b>Что делать:</b>\n"
            f"• Попробуйте позже (ключи восстанавливаются через 5 минут)\n"
            f"• Обратитесь в поддержку\n"
            f"• Проверьте ваш интернет"
        )
        
        await callback.message.answer(error_text, parse_mode='HTML', reply_markup=get_retry_keyboard())
        await state.clear()
        return
    
    # Обновляем статистику
    session = ai_manager.get_session(user_id)
    tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    
    session['copies_used'] += 1
    await update_ai_usage(user_id, 'copy')
    
    word_count_actual = len(generated_text.split())
    
    # Форматируем результат
    result_text = (
        f"✅ <b>Текст готов!</b>\n"
        f"📊 <b>Попытка:</b> {metadata.get('final_attempt', 1)}/3\n\n"
        
        f"📈 <b>Статистика:</b>\n"
        f"• Запрошено слов: {word_count}\n"
        f"• Получено слов: {word_count_actual}\n"
        f"• Символов: {len(generated_text)}\n"
        f"• Использовано: {session['copies_used']}/{tariff_info['ai_copies_limit']}\n\n"
        
        f"📝 <b>Результат:</b>\n\n"
        f"{generated_text}\n\n"
        
        f"📍 <b>Использованный ключ:</b> {metadata.get('final_key', 'Неизвестно')[:15]}..."
    )
    
    # Отправляем результат
    if len(result_text) > 4000:
        # Разбиваем на части
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
                await callback.message.edit_text(part, parse_mode='HTML')
            else:
                await callback.message.answer(part, parse_mode='HTML')
    else:
        await callback.message.edit_text(result_text, parse_mode='HTML')
    
    # Клавиатура действий
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📱 Отправить в чат", callback_data="send_to_chat"),
            InlineKeyboardButton(text="✏️ Редактировать", callback_data="edit_text")
        ],
        [
            InlineKeyboardButton(text="🔄 Новый текст", callback_data="ai_copywriter"),
            InlineKeyboardButton(text="📋 Сохранить", callback_data="save_text")
        ],
        [
            InlineKeyboardButton(text="📅 Запланировать", callback_data="schedule_post"),
            InlineKeyboardButton(text="⬅️ В меню AI", callback_data="ai_services")
        ]
    ])
    
    await state.update_data(generated_text=generated_text)
    await callback.message.answer(
        "👇 <b>Что сделать с текстом?</b>",
        parse_mode='HTML',
        reply_markup=keyboard
    )

async def generate_ai_text_from_message(message: Message, data: Dict, word_count: int):
    """Альтернативная функция генерации для сообщений"""
    user_id = message.from_user.id
    
    preview_text = (
        f"🎯 <b>Ваш запрос:</b>\n\n"
        f"📌 <b>Тема:</b> {data['topic']}\n"
        f"🎨 <b>Стиль:</b> {data['style']}\n"
        f"📝 <b>Слов:</b> {word_count}\n\n"
        f"🔄 <b>Генерирую текст...</b>"
    )
    
    await message.answer(preview_text, parse_mode='HTML')
    
    current_date = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y")
    prompt = COPYWRITER_PROMPT.format(
        topic=data['topic'],
        style=data['style'],
        examples=data['examples'],
        word_count=word_count,
        current_date=current_date
    )
    
    status_msg = await message.answer("🔄 <b>Попытка 1/3:</b> Ищу доступный ключ...", parse_mode='HTML')
    
    generated_text, metadata = await generate_with_gemini(prompt, user_id)
    
    await status_msg.delete()
    
    if not generated_text:
        error_details = ai_manager.get_last_error(user_id) or "Неизвестная ошибка"
        attempts = ai_manager.get_request_retries(user_id)
        
        error_text = (
            f"❌ <b>Не удалось сгенерировать текст!</b>\n\n"
            f"📊 <b>Детали:</b>\n"
            f"• Попыток: {attempts}/{MAX_RETRIES_PER_REQUEST}\n"
            f"• Ошибка: {error_details}\n\n"
            f"💡 <b>Что делать:</b>\n"
            f"• Попробуйте позже\n"
            f"• Обратитесь в поддержку: @{SUPPORT_BOT_USERNAME}"
        )
        
        await message.answer(error_text, parse_mode='HTML', reply_markup=get_retry_keyboard())
        return
    
    session = ai_manager.get_session(user_id)
    tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    
    session['copies_used'] += 1
    await update_ai_usage(user_id, 'copy')
    
    word_count_actual = len(generated_text.split())
    
    result_text = (
        f"✅ <b>Текст готов!</b> (Попытка {metadata.get('final_attempt', 1)}/3)\n\n"
        f"📊 <b>Статистика:</b>\n"
        f"• Слов: {word_count_actual} (запрошено {word_count})\n"
        f"• Использовано: {session['copies_used']}/{tariff_info['ai_copies_limit']}\n\n"
        f"📝 <b>Результат:</b>\n\n"
        f"{generated_text}"
    )
    
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
                await message.answer(part, parse_mode='HTML')
            else:
                await message.answer(part, parse_mode='HTML')
    else:
        await message.answer(result_text, parse_mode='HTML')
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📱 Отправить в чат", callback_data="send_to_chat"),
            InlineKeyboardButton(text="🔄 Новый текст", callback_data="ai_copywriter")
        ],
        [
            InlineKeyboardButton(text="📅 Запланировать", callback_data="schedule_post"),
            InlineKeyboardButton(text="⬅️ В меню", callback_data="back_to_main")
        ]
    ])
    
    await message.answer(
        "👇 <b>Что сделать с текстом?</b>",
        parse_mode='HTML',
        reply_markup=keyboard
    )

# ========== ОБРАБОТЧИК ОШИБОК И ПОВТОРНЫХ ПОПЫТОК ==========
@router.callback_query(F.data == "retry_ai")
async def retry_ai_request(callback: CallbackQuery):
    """Повторная попытка AI-запроса"""
    user_id = callback.from_user.id
    
    # Проверяем последнюю ошибку
    last_error = ai_manager.get_last_error(user_id)
    attempts = ai_manager.get_request_retries(user_id)
    
    if attempts >= MAX_RETRIES_PER_REQUEST:
        await callback.answer(
            f"❌ Достигнут лимит попыток ({MAX_RETRIES_PER_REQUEST}). Попробуйте позже.",
            show_alert=True
        )
        return
    
    # Показываем статистику ротации
    stats = ai_manager.get_stats_summary()
    
    status_text = (
        f"🔄 <b>Повторная попытка</b>\n\n"
        f"📊 <b>Статистика ротации:</b>\n"
        f"• Всего ключей: {stats['total_keys']}\n"
        f"• Заблокировано: {stats['blocked_keys']}\n"
        f"• Текущая модель: {stats['current_model']}\n"
        f"• Ваши попытки: {attempts}/{MAX_RETRIES_PER_REQUEST}\n\n"
        f"📝 <b>Последняя ошибка:</b>\n{last_error[:200] if last_error else 'Неизвестно'}"
    )
    
    await callback.message.edit_text(status_text, parse_mode='HTML')
    
    # Возвращаем в меню AI
    tariff = await get_user_tariff(user_id)
    await asyncio.sleep(2)
    await callback.message.edit_text(
        "🤖 <b>ИИ-сервисы</b>\n\nВыберите действие:",
        parse_mode='HTML',
        reply_markup=get_ai_main_menu(tariff)
    )

# ========== ОСТАЛЬНЫЕ ОБРАБОТЧИКИ ==========
# (Остальные обработчики — статистика, каналы, планирование постов, 
# тарифы, админ-панель — остаются аналогичными исходному коду,
# но с добавленным визуальным оформлением)

# Для экономии места я покажу только измененные части:

@router.callback_query(F.data == "my_stats")
async def show_my_stats(callback: CallbackQuery):
    """Показывает статистику пользователя"""
    user_id = callback.from_user.id
    stats = await get_user_stats(user_id)
    current_tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(current_tariff, TARIFFS['mini'])
    posts_today = await get_user_posts_today(user_id)
    
    session = ai_manager.get_session(user_id)
    ai_stats = await get_ai_usage_stats(user_id)
    
    today = datetime.now(MOSCOW_TZ).date()
    reset_time = datetime.combine(today + timedelta(days=1), datetime.min.time())
    reset_time = MOSCOW_TZ.localize(reset_time)
    time_left = reset_time - datetime.now(MOSCOW_TZ)
    hours = int(time_left.total_seconds() // 3600)
    minutes = int((time_left.total_seconds() % 3600) // 60)
    
    stats_text = (
        f"📊 <b>Ваша статистика</b>\n\n"
        
        f"💎 <b>Тариф:</b> {tariff_info['icon']} {tariff_info['name']}\n\n"
        
        f"📅 <b>Посты:</b>\n"
        f"• Всего запланировано: {stats['total_posts']}\n"
        f"• Активных постов: {stats['active_posts']}\n"
        f"• Отправлено постов: {stats['sent_posts']}\n"
        f"• Постов сегодня: {posts_today}/{tariff_info['daily_posts_limit']}\n\n"
        
        f"📢 <b>Каналы:</b>\n"
        f"• Подключено: {stats['channels']}/{tariff_info['channels_limit']}\n\n"
        
        f"🤖 <b>AI-сервисы:</b>\n"
        f"• Копирайтер: {session['copies_used']}/{tariff_info['ai_copies_limit']}\n"
        f"• Идеи: {session['ideas_used']}/{tariff_info['ai_ideas_limit']}\n"
        f"• Всего AI запросов: {session['total_requests']}\n\n"
        
        f"🔄 <b>Обновление лимитов через:</b> {hours}ч {minutes}м\n\n"
        
        f"📍 <b>Время по Москве:</b> {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}"
    )
    
    await callback.message.edit_text(stats_text, parse_mode='HTML', reply_markup=get_main_menu(user_id, user_id == ADMIN_ID))

# ========== ЗАПУСК ==========
async def on_startup():
    """Запуск бота"""
    logger.info("=" * 60)
    logger.info(f"🚀 ЗАПУСК БОТА KOLES-TECH")
    logger.info(f"🤖 AI сервисы: ВКЛЮЧЕНЫ")
    logger.info(f"🔑 Gemini ключей: {len(GEMINI_API_KEYS)}")
    logger.info(f"🎯 Максимум попыток: {MAX_RETRIES_PER_REQUEST}")
    logger.info(f"👑 Admin ID: {ADMIN_ID}")
    logger.info(f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}")
    logger.info("=" * 60)
    
    try:
        await init_db()
        await restore_scheduled_jobs()
        
        scheduler.start()
        
        # Ежедневный сброс лимитов
        scheduler.add_job(
            scheduled_reset_posts,
            trigger='cron',
            hour=0,
            minute=1,
            timezone=MOSCOW_TZ,
            id='reset_posts'
        )
        
        # Ежедневный сброс AI лимитов
        scheduler.add_job(
            reset_ai_limits_daily,
            trigger='cron',
            hour=0,
            minute=0,
            timezone=MOSCOW_TZ,
            id='reset_ai_limits'
        )
        
        # Ежечасная статистика
        scheduler.add_job(
            log_ai_stats,
            trigger='cron',
            hour='*',
            minute=0,
            timezone=MOSCOW_TZ,
            id='log_ai_stats'
        )
        
        me = await bot.get_me()
        logger.info(f"✅ Бот @{me.username} запущен (ID: {me.id})")
        
        if ADMIN_ID:
            try:
                stats = ai_manager.get_stats_summary()
                await bot.send_message(
                    ADMIN_ID,
                    f"🤖 <b>Бот запущен!</b>\n\n"
                    f"🔗 @{me.username}\n"
                    f"🆔 {me.id}\n\n"
                    f"📊 <b>AI статистика:</b>\n"
                    f"• Ключей: {stats['total_keys']}\n"
                    f"• Заблокировано: {stats['blocked_keys']}\n"
                    f"• Модель: {stats['current_model']}\n"
                    f"• Сессии: {stats['active_sessions']}\n\n"
                    f"🕐 {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M:%S')}",
                    parse_mode='HTML'
                )
            except Exception as e:
                logger.error(f"Не удалось отправить уведомление админу: {e}")
        
        logger.info("=" * 60)
        logger.info("🎉 БОТ УСПЕШНО ЗАПУЩЕН!")
        logger.info("=" * 60)
        return True
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА ПРИ ЗАПУСКЕ: {e}")
        return False

async def log_ai_stats():
    """Логирует статистику AI"""
    stats = ai_manager.get_stats_summary()
    logger.info(
        f"📊 AI статистика | "
        f"Ключей: {stats['total_keys']} | "
        f"Заблокировано: {stats['blocked_keys']} | "
        f"Запросов: {stats['total_requests']} | "
        f"Ошибок: {stats['total_errors']}"
    )

async def reset_ai_limits_daily():
    """Сбрасывает дневные лимиты AI"""
    ai_manager.reset_daily_limits()
    logger.info("✅ AI лимиты сброшены")

async def scheduled_reset_posts():
    """Сбрасывает дневные счетчики постов"""
    await reset_daily_posts()

async def restore_scheduled_jobs():
    """Восстанавливает запланированные посты"""
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
                    id=f"post_{post['id']}",
                    replace_existing=True
                )
                restored += 1
            except Exception as e:
                logger.error(f"❌ Ошибка восстановления поста {post['id']}: {e}")
        
        logger.info(f"✅ Восстановлено {restored} запланированных постов")
    except Exception as e:
        logger.error(f"❌ Ошибка при восстановлении постов: {e}")

async def on_shutdown():
    """Выключение бота"""
    logger.info("🛑 Выключение бота...")
    if scheduler.running:
        scheduler.shutdown()
    await bot.session.close()
    logger.info("👋 Бот выключен")

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
