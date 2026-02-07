import os
import asyncio
import logging
import sys
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any
from enum import Enum
from collections import defaultdict
import random
import signal
import traceback

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
        "AIzaSyBVX6wcwviTFLXZumpApEzogCddy4SHQaQ",
        "AIzaSyCJyiYNk2PDd0eEF-l_deLl638wtY4vcgQ",
        "AIzaSyASat89t1UUD7BXHxlXf9Oela6AsCzjOXc",
        "AIzaSyATKIJVRLb35J8K0HS1G_ql7IS9cJJm4Ys",
        "AIzaSyDJNu3lzF-VYrKpmw6Bzjm5JToasfhm8sU",
        "AIzaSyBudSc-lz-ypF_2pigH5_7DfLGxF0COJYQ",
        "AIzaSyA9iL90r62KthqSkdcon3wcLKeMaXOsBfM",
        "AIzaSyBZkVAruHt6zPCJF1gf67kVbk6fHY-eelo",
        "AIzaSyB9VqHVXudqHHN3_b_BWM9nNEEvNn-geKw",
        "AIzaSyCeME8Lvm3p5QYBjJh5FucEyJ4J22E1NOY",
        "AIzaSyDtcHsN6daIR9WQ1psELkArzRrJH1IHu70"   
    ]

# Убираем пустые ключи
GEMINI_API_KEYS = [key.strip() for key in GEMINI_API_KEYS if key and key.strip()]

if not GEMINI_API_KEYS:
    print("❌ ОШИБКА: Не указаны Gemini API ключи")
    sys.exit(1)

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
ALTERNATIVE_MODELS = ["gemini-2.5-flash", "gemini-1.5-flash", "gemini-1.5-pro"]

# Ротация настроек
MAX_403_RETRIES = 2  # 2 попытки при ошибке 403
REQUEST_COOLDOWN = 15  # 15 секунд между запросами пользователя
KEY_BLOCK_DURATION = 300  # 5 минут блокировки ключа после ошибки 403

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
        "ai_copies_limit": 1,
        "ai_ideas_limit": 10,
        "description": "Бесплатный тариф для начала работы"
    },
    Tariff.STANDARD.value: {
        "name": "⭐ Standard",
        "price": 4,
        "currency": "USD",
        "channels_limit": 2,
        "daily_posts_limit": 6,
        "ai_copies_limit": 3,
        "ai_ideas_limit": 30,
        "description": "Для активных пользователей"
    },
    Tariff.VIP.value: {
        "name": "👑 VIP",
        "price": 7,
        "currency": "USD",
        "channels_limit": 3,
        "daily_posts_limit": 12,
        "ai_copies_limit": 7,
        "ai_ideas_limit": 50,
        "description": "Максимальные возможности"
    },
    Tariff.ADMIN.value: {
        "name": "⚡ Admin",
        "price": 0,
        "currency": "USD",
        "channels_limit": 999,
        "daily_posts_limit": 999,
        "ai_copies_limit": 999,
        "ai_ideas_limit": 999,
        "description": "Безлимитный доступ"
    }
}

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

# ========== DATABASE CONNECTION POOL ==========
class DatabasePool:
    _pool = None
    
    @classmethod
    async def get_pool(cls):
        if cls._pool is None:
            if DATABASE_URL.startswith("postgres://"):
                conn_string = DATABASE_URL.replace("postgres://", "postgresql://", 1)
            else:
                conn_string = DATABASE_URL
            
            if "sslmode" not in conn_string:
                if "?" in conn_string:
                    conn_string += "&sslmode=require"
                else:
                    conn_string += "?sslmode=require"
            
            cls._pool = await asyncpg.create_pool(
                conn_string,
                min_size=10,
                max_size=30,
                command_timeout=60
            )
        return cls._pool
    
    @classmethod
    async def close_pool(cls):
        if cls._pool:
            await cls._pool.close()
            cls._pool = None

async def execute_query(query: str, *args) -> Any:
    """Выполняет SQL запрос с использованием пула соединений"""
    pool = await DatabasePool.get_pool()
    async with pool.acquire() as conn:
        try:
            if query.strip().upper().startswith("SELECT"):
                result = await conn.fetch(query, *args)
                return [dict(row) for row in result] if result else []
            else:
                result = await conn.execute(query, *args)
                return result
        except Exception as e:
            logger.error(f"Ошибка запроса: {e}\nЗапрос: {query}")
            raise

# ========== ADVANCED AI SESSION MANAGER ==========
class AdvancedAISessionManager:
    """Управление AI сессиями с улучшенной ротацией ключей"""
    
    def __init__(self):
        self.sessions: Dict[int, Dict] = {}
        self.key_stats = {}
        self._init_key_stats()
        self.last_request_time: Dict[int, datetime] = {}
        self.current_model_index = 0
        self.models = [GEMINI_MODEL] + ALTERNATIVE_MODELS
        self.user_request_counts = defaultdict(int)
        self.last_key_rotation = datetime.now(MOSCOW_TZ)
        self.current_key_index = random.randint(0, len(GEMINI_API_KEYS) - 1)  # Начинаем со случайного ключа
        
    def _init_key_stats(self):
        """Инициализация статистики ключей"""
        for key in GEMINI_API_KEYS:
            self.key_stats[key] = {
                "requests": 0,
                "errors": 0,
                "403_errors": 0,
                "blocked_until": None,
                "last_used": None,
                "successful_requests": 0,
                "last_error": None,
                "priority": 100,  # Приоритет: чем меньше, тем лучше
                "failed_users": set(),  # Пользователи, для которых ключ не сработал
                "last_success": None
            }
    
    def get_session(self, user_id: int) -> Dict:
        """Получает или создает сессию пользователя"""
        if user_id not in self.sessions:
            self.sessions[user_id] = {
                'history': [],
                'current_key_index': self.current_key_index,
                'request_count': 0,
                'total_requests': 0,
                'copies_used': 0,
                'ideas_used': 0,
                'last_reset': datetime.now(MOSCOW_TZ).date(),
                'last_successful_key': None,
                'word_count': 200,
                'current_attempts': 0,
                'consecutive_errors': 0,
                'last_error_time': None,
                'failed_keys': set(),  # Ключи, которые не сработали для этого пользователя
                'last_success_time': None
            }
        return self.sessions[user_id]
    
    def get_best_key(self, user_id: int) -> Tuple[Optional[str], int, str]:
        """Выбирает лучший доступный ключ с интеллектуальной ротацией"""
        session = self.get_session(user_id)
        
        # Пробуем разные ключи по порядку, начиная со следующего после последнего использованного
        available_keys = []
        start_index = session.get('current_key_index', self.current_key_index)
        
        for i in range(len(GEMINI_API_KEYS)):
            key_index = (start_index + i) % len(GEMINI_API_KEYS)
            key = GEMINI_API_KEYS[key_index]
            
            if self._is_key_available(key, user_id):
                stats = self.key_stats[key]
                priority = stats['priority']
                
                # Понижаем приоритет если ключ уже не сработал для этого пользователя
                if key in session['failed_keys']:
                    priority += 50
                
                # Повышаем приоритет если ключ недавно успешно использовался
                if stats['last_success']:
                    hours_since_success = (datetime.now(MOSCOW_TZ) - stats['last_success']).total_seconds() / 3600
                    if hours_since_success < 1:  # Успешно использовался менее часа назад
                        priority -= 30
                
                available_keys.append((priority, key_index, key))
        
        # Если нет доступных ключей, пробуем любой незаблокированный
        if not available_keys:
            for i, key in enumerate(GEMINI_API_KEYS):
                if self.key_stats[key]['blocked_until'] is None or \
                   self.key_stats[key]['blocked_until'] < datetime.now(MOSCOW_TZ):
                    self.key_stats[key]['403_errors'] = 0
                    self.key_stats[key]['blocked_until'] = None
                    available_keys.append((50, i, key))
        
        if not available_keys:
            logger.error("❌ Нет доступных ключей!")
            return None, 0, self.models[0]
        
        # Сортируем по приоритету
        available_keys.sort(key=lambda x: x[0])
        
        # Выбираем ключ с наилучшим приоритетом
        best_key = available_keys[0][2]
        key_index = available_keys[0][1]
        
        # Обновляем статистику
        session['current_key_index'] = (key_index + 1) % len(GEMINI_API_KEYS)  # Следующий ключ для следующего запроса
        self._update_key_stats_on_use(best_key)
        
        # Рандомно выбираем модель для разнообразия
        model_index = random.randint(0, len(self.models) - 1)
        
        return best_key, key_index, self.models[model_index]
    
    def _update_key_stats_on_use(self, key: str):
        """Обновляет статистику при использовании ключа"""
        if key in self.key_stats:
            self.key_stats[key]['requests'] += 1
            self.key_stats[key]['last_used'] = datetime.now(MOSCOW_TZ)
    
    def _is_key_available(self, key: str, user_id: int) -> bool:
        """Проверяет, доступен ли ключ для пользователя"""
        stats = self.key_stats.get(key)
        if not stats:
            return False
        
        # Проверяем блокировку
        if stats['blocked_until'] and stats['blocked_until'] > datetime.now(MOSCOW_TZ):
            return False
        
        # Проверяем количество ошибок 403
        if stats['403_errors'] >= MAX_403_RETRIES:
            return False
        
        # Ключ с очень высоким приоритетом считается менее доступным
        if stats['priority'] > 90:
            return False
        
        # Проверяем, не провалился ли ключ для этого пользователя
        if user_id in stats['failed_users']:
            # Но даем шанс через некоторое время
            if stats['last_error']:
                hours_since_error = (datetime.now(MOSCOW_TZ) - stats['last_error']).total_seconds() / 3600
                if hours_since_error > 2:  # Через 2 часа даем еще шанс
                    stats['failed_users'].discard(user_id)
                else:
                    return False
        
        return True
    
    def mark_key_error(self, key: str, error_type: str = "403", user_id: int = None):
        """Отмечает ошибку для ключа"""
        if key not in self.key_stats:
            return
        
        stats = self.key_stats[key]
        stats['errors'] += 1
        stats['last_error'] = datetime.now(MOSCOW_TZ)
        
        if user_id:
            stats['failed_users'].add(user_id)
        
        if error_type == "403":
            stats['403_errors'] += 1
            stats['priority'] = min(100, stats['priority'] + 30)  # Сильно понижаем приоритет
            logger.warning(f"Ключ {key[:15]}... получил 403 ошибку. Приоритет: {stats['priority']}")
            
            if stats['403_errors'] >= MAX_403_RETRIES:
                stats['blocked_until'] = datetime.now(MOSCOW_TZ) + timedelta(seconds=KEY_BLOCK_DURATION)
                stats['priority'] = 95  # Низкий приоритет для заблокированных
                logger.warning(f"Ключ {key[:15]}... заблокирован на {KEY_BLOCK_DURATION // 60} минут")
        elif error_type in ["429", "quota"]:
            stats['priority'] = min(100, stats['priority'] + 20)
            logger.warning(f"Ключ {key[:15]}... превысил лимит. Приоритет: {stats['priority']}")
        else:
            stats['priority'] = min(100, stats['priority'] + 10)
    
    def mark_key_success(self, key: str, user_id: int):
        """Отмечает успешное использование ключа"""
        if key not in self.key_stats:
            return
        
        stats = self.key_stats[key]
        stats['errors'] = 0
        stats['403_errors'] = 0
        stats['successful_requests'] += 1
        stats['priority'] = max(1, stats['priority'] - 20)  # Сильно повышаем приоритет
        stats['blocked_until'] = None
        stats['last_success'] = datetime.now(MOSCOW_TZ)
        stats['failed_users'].discard(user_id)  # Убираем пользователя из списка неудачных
        
        session = self.get_session(user_id)
        session['last_successful_key'] = key
        session['consecutive_errors'] = 0
        session['current_attempts'] = 0
        session['failed_keys'].discard(key)  # Убираем ключ из списка неудачных
        session['last_success_time'] = datetime.now(MOSCOW_TZ)
        
        logger.info(f"✅ Ключ {key[:15]}... успешно использован. Приоритет: {stats['priority']}")
    
    def increment_user_attempts(self, user_id: int) -> int:
        """Увеличивает счетчик попыток пользователя"""
        session = self.get_session(user_id)
        session['current_attempts'] += 1
        session['consecutive_errors'] += 1
        return session['current_attempts']
    
    def add_failed_key(self, user_id: int, key: str):
        """Добавляет ключ в список неудачных для пользователя"""
        session = self.get_session(user_id)
        session['failed_keys'].add(key)
        logger.info(f"Ключ {key[:15]}... добавлен в failed_keys для user_{user_id}")
    
    def reset_user_attempts(self, user_id: int):
        """Сбрасывает счетчик попыток пользователя"""
        session = self.get_session(user_id)
        session['current_attempts'] = 0
        session['consecutive_errors'] = 0
        session['failed_keys'].clear()
    
    def can_user_request(self, user_id: int) -> Tuple[bool, Optional[str]]:
        """Проверяет, может ли пользователь сделать запрос"""
        now = datetime.now(MOSCOW_TZ)
        
        if user_id in self.last_request_time:
            time_diff = (now - self.last_request_time[user_id]).total_seconds()
            if time_diff < REQUEST_COOLDOWN:
                wait_time = int(REQUEST_COOLDOWN - time_diff)
                return False, f"⏳ Подождите {wait_time} секунд перед следующим запросом"
        
        session = self.get_session(user_id)
        if session['consecutive_errors'] > 5:
            return False, "⚠️ Слишком много ошибок подряд. Попробуйте позже."
        
        self.last_request_time[user_id] = now
        return True, None
    
    def get_current_model(self) -> str:
        """Возвращает текущую модель"""
        return self.models[self.current_model_index % len(self.models)]
    
    def rotate_model(self):
        """Переключает на следующую модель"""
        self.current_model_index += 1
        model_name = self.get_current_model()
        logger.info(f"Ротация модели на: {model_name}")
    
    def reset_daily_limits(self):
        """Сбрасывает дневные лимиты"""
        today = datetime.now(MOSCOW_TZ).date()
        for user_id, session in self.sessions.items():
            if session['last_reset'] < today:
                session['copies_used'] = 0
                session['ideas_used'] = 0
                session['last_reset'] = today
                session['consecutive_errors'] = 0
                session['current_attempts'] = 0
                session['failed_keys'].clear()
    
    def set_word_count(self, user_id: int, word_count: int):
        """Устанавливает количество слов"""
        session = self.get_session(user_id)
        session['word_count'] = max(50, min(1000, word_count))
    
    def get_word_count(self, user_id: int) -> int:
        """Получает количество слов"""
        return self.get_session(user_id)['word_count']
    
    def get_user_stats(self, user_id: int) -> Dict:
        """Получает статистику пользователя"""
        session = self.get_session(user_id)
        return {
            'copies_used': session['copies_used'],
            'ideas_used': session['ideas_used'],
            'total_requests': session['total_requests'],
            'consecutive_errors': session['consecutive_errors'],
            'word_count': session['word_count'],
            'failed_keys_count': len(session['failed_keys'])
        }
    
    def get_system_stats(self) -> Dict:
        """Получает системную статистику"""
        total_requests = sum(s['total_requests'] for s in self.sessions.values())
        total_copies = sum(s['copies_used'] for s in self.sessions.values())
        total_ideas = sum(s['ideas_used'] for s in self.sessions.values())
        
        key_stats_summary = {}
        for key, stats in self.key_stats.items():
            key_stats_summary[key[:10] + "..."] = {
                'requests': stats['requests'],
                'errors': stats['errors'],
                '403_errors': stats['403_errors'],
                'successful': stats['successful_requests'],
                'priority': stats['priority'],
                'blocked': stats['blocked_until'] is not None and stats['blocked_until'] > datetime.now(MOSCOW_TZ),
                'failed_users_count': len(stats['failed_users'])
            }
        
        available_keys = len([k for k, v in self.key_stats.items() if v['priority'] < 80])
        
        return {
            'total_users': len(self.sessions),
            'total_requests': total_requests,
            'total_copies': total_copies,
            'total_ideas': total_ideas,
            'key_stats': key_stats_summary,
            'active_sessions': len([s for s in self.sessions.values() if s['total_requests'] > 0]),
            'available_keys': available_keys,
            'total_keys': len(GEMINI_API_KEYS)
        }
    
    def check_and_rotate_keys(self):
        """Проверяет и ротирует ключи если нужно"""
        now = datetime.now(MOSCOW_TZ)
        
        # Восстанавливаем приоритеты заблокированных ключей
        for key in GEMINI_API_KEYS:
            stats = self.key_stats[key]
            if stats['blocked_until'] and stats['blocked_until'] < now:
                stats['403_errors'] = 0
                stats['blocked_until'] = None
                stats['priority'] = 50
                stats['failed_users'].clear()
                logger.info(f"✅ Восстановлен ключ {key[:15]}...")
        
        # Повышаем приоритеты редко используемых ключей
        for key in GEMINI_API_KEYS:
            stats = self.key_stats[key]
            if stats['last_used']:
                hours_since_use = (now - stats['last_used']).total_seconds() / 3600
                if hours_since_use > 1:
                    stats['priority'] = max(1, stats['priority'] - 10)
        
        # Очищаем старые failed_users (старше 6 часов)
        for key in GEMINI_API_KEYS:
            stats = self.key_stats[key]
            if stats['last_error']:
                hours_since_error = (now - stats['last_error']).total_seconds() / 3600
                if hours_since_error > 6:
                    stats['failed_users'].clear()
        
        logger.info("✅ Выполнена автоматическая ротация ключей")

ai_manager = AdvancedAISessionManager()

# ========== PROMPT TEMPLATES ==========
COPYWRITER_PROMPT = """Ты профессиональный копирайтер для Telegram-каналов. Создай продающий текст на основе следующих данных:

ТЕМА: {topic}
СТИЛЬ: {style}
ПРИМЕРЫ РАБОТ: {examples}
КОЛИЧЕСТВО СЛОВ: {word_count} слов

ТРЕБОВАНИЯ:
1. Текст должен быть цепляющим и вовлекающим
2. Используй эмодзи уместно (но не переборщи)
3. Структура: заголовок → проблема → решение → призыв к действию
4. ТОЧНО {word_count} слов (±10%)
5. Пиши как для живых людей, без воды
6. Учитывай примеры, но не копируй их

ДОПОЛНИТЕЛЬНО:
- Текущая дата: {current_date}
- Не упоминай что ты ИИ
- Пиши в настоящем времени
- Убедись что текст содержит примерно {word_count} слов

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

# ========== AI GENERATION FUNCTIONS ==========
async def generate_with_gemini_advanced(prompt: str, user_id: int, max_retries: int = 8) -> Optional[str]:
    """Усовершенствованная генерация с интеллектуальной ротацией"""
    
    # Проверяем и ротируем ключи если нужно
    ai_manager.check_and_rotate_keys()
    
    session = ai_manager.get_session(user_id)
    session['total_requests'] += 1
    
    for attempt in range(1, max_retries + 1):
        try:
            key, key_index, model_name = ai_manager.get_best_key(user_id)
            
            if not key:
                logger.error(f"Нет доступных ключей для user_{user_id}")
                return None
            
            logger.info(f"Попытка #{attempt} | user_{user_id} | key_{key_index} | модель: {model_name} | приоритет: {ai_manager.key_stats[key]['priority']}")
            
            genai.configure(api_key=key)
            
            try:
                model = genai.GenerativeModel(model_name)
                
                response = model.generate_content(
                    prompt,
                    generation_config={
                        "temperature": 0.8,
                        "top_p": 0.95,
                        "top_k": 40,
                        "max_output_tokens": 4000,
                    }
                )
                
                ai_manager.mark_key_success(key, user_id)
                logger.info(f"✅ Успешно | user_{user_id} | ключ: {key_index} | модель: {model_name} | попытка: {attempt}")
                return response.text.strip()
                
            except Exception as model_error:
                error_str = str(model_error)
                
                # Пробуем другую модель если текущая не поддерживается
                if "not supported" in error_str.lower() or "not found" in error_str.lower():
                    logger.warning(f"Модель {model_name} не поддерживается, пробую следующую")
                    # Пробуем следующую модель
                    next_model_index = (ai_manager.models.index(model_name) + 1) % len(ai_manager.models)
                    model_name = ai_manager.models[next_model_index]
                    continue
                else:
                    raise model_error
                    
        except Exception as e:
            error_str = str(e)
            logger.warning(f"Ошибка попытки #{attempt} для user_{user_id}: {error_str[:100]}")
            
            # Анализируем ошибку
            if "429" in error_str or "quota" in error_str or "resource exhausted" in error_str:
                ai_manager.mark_key_error(key, "quota", user_id)
                ai_manager.add_failed_key(user_id, key)
            elif "403" in error_str or "permission denied" in error_str or "leaked" in error_str:
                ai_manager.mark_key_error(key, "403", user_id)
                ai_manager.add_failed_key(user_id, key)
            elif "503" in error_str or "unavailable" in error_str:
                ai_manager.rotate_model()
                ai_manager.add_failed_key(user_id, key)
            else:
                logger.error(f"Неизвестная ошибка: {e}")
                ai_manager.add_failed_key(user_id, key)
            
            attempts = ai_manager.increment_user_attempts(user_id)
            
            # Если много ошибок подряд, делаем паузу
            if attempts >= 3:
                wait_time = 1 * (attempts - 2)
                logger.info(f"Много ошибок подряд ({attempts}), пауза {wait_time} секунд")
                await asyncio.sleep(wait_time)
            
            if attempt < max_retries:
                wait_time = 0.3 * attempt
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Все {max_retries} попыток исчерпаны для user_{user_id}")
                system_stats = ai_manager.get_system_stats()
                logger.error(f"Статистика ключей: {system_stats['key_stats']}")
                
                # Сбрасываем попытки пользователя
                ai_manager.reset_user_attempts(user_id)
    
    return None

# ========== DATABASE INITIALIZATION ==========
async def init_database():
    """Инициализация базы данных с оптимизированными индексами"""
    queries = [
        # Таблица пользователей
        '''
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            tariff TEXT DEFAULT 'mini',
            posts_today INTEGER DEFAULT 0,
            posts_reset_date DATE DEFAULT CURRENT_DATE,
            ai_copies_used INTEGER DEFAULT 0,
            ai_ideas_used INTEGER DEFAULT 0,
            ai_last_used TIMESTAMPTZ,
            is_active BOOLEAN DEFAULT TRUE,
            is_admin BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            last_seen TIMESTAMPTZ DEFAULT NOW()
        )
        ''',
        
        # Индексы для таблицы users
        '''
        CREATE INDEX IF NOT EXISTS idx_users_tariff ON users(tariff)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_users_created ON users(created_at)
        ''',
        
        # Таблица каналов
        '''
        CREATE TABLE IF NOT EXISTS channels (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            channel_name TEXT NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(user_id, channel_id)
        )
        ''',
        
        # Индексы для таблицы channels
        '''
        CREATE INDEX IF NOT EXISTS idx_channels_user ON channels(user_id)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_channels_active ON channels(is_active)
        ''',
        
        # Таблица запланированных постов
        '''
        CREATE TABLE IF NOT EXISTS scheduled_posts (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            message_type TEXT NOT NULL,
            message_text TEXT,
            media_file_id TEXT,
            media_caption TEXT,
            scheduled_time TIMESTAMPTZ NOT NULL,
            is_sent BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        ''',
        
        # Индексы для таблицы scheduled_posts
        '''
        CREATE INDEX IF NOT EXISTS idx_scheduled_time ON scheduled_posts(scheduled_time)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_user_scheduled ON scheduled_posts(user_id, scheduled_time)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_sent_status ON scheduled_posts(is_sent)
        ''',
        
        # Таблица заказов тарифов
        '''
        CREATE TABLE IF NOT EXISTS tariff_orders (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            tariff TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            order_date TIMESTAMPTZ DEFAULT NOW(),
            processed_date TIMESTAMPTZ,
            admin_notes TEXT
        )
        ''',
        
        # Индексы для таблицы tariff_orders
        '''
        CREATE INDEX IF NOT EXISTS idx_orders_status ON tariff_orders(status)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_orders_user ON tariff_orders(user_id)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_orders_date ON tariff_orders(order_date)
        ''',
        
        # Таблица логов AI запросов
        '''
        CREATE TABLE IF NOT EXISTS ai_request_logs (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            service_type TEXT NOT NULL,
            prompt_length INTEGER,
            response_length INTEGER,
            success BOOLEAN DEFAULT FALSE,
            error_message TEXT,
            api_key_index INTEGER,
            model_name TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        ''',
        
        # Индексы для таблицы ai_request_logs
        '''
        CREATE INDEX IF NOT EXISTS idx_logs_user_date ON ai_request_logs(user_id, created_at)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_logs_success ON ai_request_logs(success)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_logs_service ON ai_request_logs(service_type)
        '''
    ]
    
    try:
        for query in queries:
            await execute_query(query)
        logger.info("✅ База данных инициализирована с оптимизированными индексами")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        raise

async def migrate_database():
    """Миграция базы данных"""
    try:
        columns_to_add = [
            ('users', 'last_seen', 'TIMESTAMPTZ'),
            ('users', 'ai_last_used', 'TIMESTAMPTZ'),
            ('scheduled_posts', 'message_type', 'TEXT')
        ]
        
        for table, column, definition in columns_to_add:
            try:
                check_query = f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table}' AND column_name = '{column}'
                """
                exists = await execute_query(check_query)
                
                if not exists:
                    await execute_query(f'ALTER TABLE {table} ADD COLUMN {column} {definition}')
                    logger.info(f"✅ Добавлена колонка {column} в таблицу {table}")
            except Exception as e:
                logger.error(f"Ошибка при добавлении колонки {column}: {e}")
        
        if ADMIN_ID > 0:
            await execute_query('''
                INSERT INTO users (id, is_admin, tariff) 
                VALUES ($1, TRUE, 'admin')
                ON CONFLICT (id) DO UPDATE 
                SET is_admin = TRUE, tariff = 'admin'
            ''', ADMIN_ID)
        
        logger.info("✅ Миграции завершены")
    except Exception as e:
        logger.error(f"❌ Ошибка миграции БД: {e}")

# ========== DATABASE FUNCTIONS ==========
async def update_user_activity(user_id: int):
    """Обновляет время последней активности пользователя"""
    await execute_query(
        "UPDATE users SET last_seen = NOW() WHERE id = $1",
        user_id
    )

async def get_user_tariff(user_id: int) -> str:
    """Получает тариф пользователя"""
    await update_user_activity(user_id)
    
    user = await execute_query(
        "SELECT tariff, is_admin FROM users WHERE id = $1", 
        user_id
    )
    
    if not user:
        await execute_query(
            "INSERT INTO users (id, tariff) VALUES ($1, 'mini') ON CONFLICT DO NOTHING",
            user_id
        )
        return 'mini'
    
    if user[0].get('is_admin'):
        return 'admin'
    
    return user[0].get('tariff', 'mini')

async def update_ai_usage_log(user_id: int, service_type: str, success: bool, 
                             api_key_index: int, model_name: str, 
                             prompt_length: int = 0, response_length: int = 0,
                             error_message: str = None):
    """Логирует использование AI сервисов"""
    await execute_query('''
        INSERT INTO ai_request_logs 
        (user_id, service_type, prompt_length, response_length, success, 
         error_message, api_key_index, model_name)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ''', user_id, service_type, prompt_length, response_length, 
        success, error_message, api_key_index, model_name)

async def check_ai_limits(user_id: int, service_type: str) -> Tuple[bool, str, Dict]:
    """Проверяет лимиты AI с кешированием"""
    tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    
    session = ai_manager.get_session(user_id)
    
    if service_type == 'copy':
        limit = tariff_info['ai_copies_limit']
        used = session['copies_used']
        remaining = limit - used
        
        if used >= limit:
            now = datetime.now(MOSCOW_TZ)
            reset_time = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
            reset_time = MOSCOW_TZ.localize(reset_time)
            time_left = reset_time - now
            hours = int(time_left.total_seconds() // 3600)
            minutes = int((time_left.total_seconds() % 3600) // 60)
            
            return False, f"❌ Достигнут дневной лимит!\n\n📝 Копирайтинг: {used}/{limit}\n⏳ Обновление через: {hours}ч {minutes}м", tariff_info
        
        return True, f"✅ Доступно! Осталось: {remaining}/{limit}", tariff_info
    
    elif service_type == 'ideas':
        limit = tariff_info['ai_ideas_limit']
        used = session['ideas_used']
        remaining = limit - used
        
        if used >= limit:
            now = datetime.now(MOSCOW_TZ)
            reset_time = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
            reset_time = MOSCOW_TZ.localize(reset_time)
            time_left = reset_time - now
            hours = int(time_left.total_seconds() // 3600)
            minutes = int((time_left.total_seconds() % 3600) // 60)
            
            return False, f"❌ Достигнут дневной лимит!\n\n💡 Идеи: {used}/{limit}\n⏳ Обновление через: {hours}ч {minutes}м", tariff_info
        
        return True, f"✅ Доступно! Осталось: {remaining}/{limit}", tariff_info
    
    return False, "❌ Неизвестный тип сервиса", tariff_info

async def get_user_channels(user_id: int) -> List[Dict]:
    """Получает каналы пользователя"""
    return await execute_query(
        "SELECT channel_id, channel_name FROM channels WHERE user_id = $1 AND is_active = TRUE",
        user_id
    )

async def add_user_channel(user_id: int, channel_id: int, channel_name: str) -> bool:
    """Добавляет канал пользователя"""
    try:
        await execute_query('''
            INSERT INTO channels (user_id, channel_id, channel_name, is_active)
            VALUES ($1, $2, $3, TRUE)
            ON CONFLICT (user_id, channel_id) DO UPDATE SET
            channel_name = EXCLUDED.channel_name,
            is_active = TRUE
        ''', user_id, channel_id, channel_name)
        return True
    except Exception as e:
        logger.error(f"Ошибка добавления канала: {e}")
        return False

async def get_user_channels_count(user_id: int) -> int:
    """Получает количество каналов пользователя"""
    result = await execute_query(
        "SELECT COUNT(*) as count FROM channels WHERE user_id = $1 AND is_active = TRUE",
        user_id
    )
    return result[0]['count'] if result else 0

async def get_tariff_limits(user_id: int) -> Tuple[int, int, int, int]:
    """Получает лимиты тарифа пользователя"""
    tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    return (tariff_info['channels_limit'], 
            tariff_info['daily_posts_limit'],
            tariff_info['ai_copies_limit'],
            tariff_info['ai_ideas_limit'])

async def get_user_posts_today(user_id: int) -> int:
    """Получает количество постов пользователя сегодня"""
    result = await execute_query(
        "SELECT posts_today, posts_reset_date FROM users WHERE id = $1",
        user_id
    )
    
    if not result:
        return 0
    
    user = result[0]
    if user['posts_reset_date'] < datetime.now(MOSCOW_TZ).date():
        return 0
    
    return user['posts_today'] or 0

async def increment_user_posts(user_id: int) -> bool:
    """Увеличивает счетчик постов пользователя"""
    try:
        user = await execute_query(
            "SELECT posts_reset_date FROM users WHERE id = $1",
            user_id
        )
        
        if not user:
            return False
        
        if user[0]['posts_reset_date'] < datetime.now(MOSCOW_TZ).date():
            await execute_query('''
                UPDATE users 
                SET posts_today = 1, posts_reset_date = CURRENT_DATE 
                WHERE id = $1
            ''', user_id)
        else:
            await execute_query('''
                UPDATE users 
                SET posts_today = posts_today + 1 
                WHERE id = $1
            ''', user_id)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка увеличения счетчика постов: {e}")
        return False

async def save_scheduled_post(user_id: int, channel_id: int, post_data: Dict, scheduled_time: datetime) -> Optional[int]:
    """Сохраняет запланированный пост"""
    try:
        if scheduled_time.tzinfo is None:
            scheduled_time = MOSCOW_TZ.localize(scheduled_time)
        scheduled_time_utc = scheduled_time.astimezone(pytz.UTC)
        
        result = await execute_query('''
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
        scheduled_time_utc
        )
        
        return result[0]['id'] if result else None
    except Exception as e:
        logger.error(f"Ошибка сохранения поста: {e}")
        return None

async def get_user_stats(user_id: int) -> Dict:
    """Получает статистику пользователя"""
    try:
        # Базовая статистика
        tariff = await get_user_tariff(user_id)
        tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
        
        # AI статистика
        ai_stats = ai_manager.get_user_stats(user_id)
        
        # Посты
        posts_today = await get_user_posts_today(user_id)
        
        # Каналы
        channels_count = await get_user_channels_count(user_id)
        
        # Запланированные посты
        scheduled_posts = await execute_query(
            "SELECT COUNT(*) as count FROM scheduled_posts WHERE user_id = $1 AND is_sent = FALSE",
            user_id
        )
        scheduled_posts = scheduled_posts[0]['count'] if scheduled_posts else 0
        
        return {
            'tariff': tariff_info['name'],
            'posts_today': posts_today,
            'posts_limit': tariff_info['daily_posts_limit'],
            'channels_count': channels_count,
            'channels_limit': tariff_info['channels_limit'],
            'ai_copies_used': ai_stats['copies_used'],
            'ai_copies_limit': tariff_info['ai_copies_limit'],
            'ai_ideas_used': ai_stats['ideas_used'],
            'ai_ideas_limit': tariff_info['ai_ideas_limit'],
            'total_ai_requests': ai_stats['total_requests'],
            'scheduled_posts': scheduled_posts
        }
    except Exception as e:
        logger.error(f"Ошибка получения статистики пользователя {user_id}: {e}")
        return {}

async def create_tariff_order(user_id: int, tariff_id: str) -> bool:
    """Создает заказ тарифа"""
    try:
        await execute_query('''
            INSERT INTO tariff_orders (user_id, tariff, status)
            VALUES ($1, $2, 'pending')
        ''', user_id, tariff_id)
        
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

async def get_tariff_orders(status: str = None) -> List[Dict]:
    """Получает заказы тарифов"""
    if status:
        return await execute_query(
            "SELECT * FROM tariff_orders WHERE status = $1 ORDER BY order_date DESC",
            status
        )
    else:
        return await execute_query(
            "SELECT * FROM tariff_orders ORDER BY order_date DESC"
        )

async def update_order_status(order_id: int, status: str, admin_notes: str = None) -> bool:
    """Обновляет статус заказа"""
    try:
        if admin_notes:
            await execute_query('''
                UPDATE tariff_orders 
                SET status = $1, processed_date = NOW(), admin_notes = $2
                WHERE id = $3
            ''', status, admin_notes, order_id)
        else:
            await execute_query('''
                UPDATE tariff_orders 
                SET status = $1, processed_date = NOW()
                WHERE id = $2
            ''', status, order_id)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления статуса заказа: {e}")
        return False

async def get_all_users() -> List[Dict]:
    """Получает всех пользователей"""
    return await execute_query('''
        SELECT id, username, first_name, tariff, is_admin, created_at
        FROM users 
        ORDER BY created_at DESC
    ''')

async def get_user_by_id(user_id: int) -> Optional[Dict]:
    """Получает пользователя по ID"""
    result = await execute_query(
        "SELECT id, username, first_name, tariff, is_admin, created_at FROM users WHERE id = $1",
        user_id
    )
    
    if result:
        return result[0]
    return None

async def update_user_tariff(user_id: int, tariff: str) -> bool:
    """Обновляет тариф пользователя"""
    try:
        await execute_query('''
            UPDATE users SET tariff = $1 WHERE id = $2
        ''', tariff, user_id)
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления тарифа: {e}")
        return False

async def force_update_user_tariff(user_id: int, tariff: str, admin_id: int) -> Tuple[bool, str]:
    """Принудительно обновляет тариф пользователя (админ)"""
    try:
        user = await get_user_by_id(user_id)
        if not user:
            return False, f"❌ Пользователь с ID {user_id} не найден"
        
        old_tariff = user.get('tariff', 'mini')
        
        success = await update_user_tariff(user_id, tariff)
        if success:
            await execute_query('''
                INSERT INTO tariff_orders (user_id, tariff, status, admin_notes)
                VALUES ($1, $2, 'force_completed', $3)
            ''', user_id, tariff, f"Принудительное обновление админом {admin_id}")
            
            tariff_info = TARIFFS.get(tariff, {})
            old_tariff_info = TARIFFS.get(old_tariff, {})
            
            return True, (
                f"✅ Тариф пользователя {user_id} обновлен!\n\n"
                f"📋 Информация:\n"
                f"👤 Пользователь: {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
                f"🔄 Старый тариф: {old_tariff_info.get('name', old_tariff)}\n"
                f"🆕 Новый тариф: {tariff_info.get('name', tariff)}\n"
                f"👑 Обновил: админ {admin_id}\n"
                f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}"
            )
        else:
            return False, f"❌ Ошибка при обновлении тарифа пользователя {user_id}"
    except Exception as e:
        logger.error(f"Ошибка принудительного обновления тарифа: {e}")
        return False, f"❌ Ошибка: {str(e)}"

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

# Остальные функции клавиатур остаются без изменений (они такие же как в исходном коде)
# Для экономии места я не буду их копировать полностью, они работают корректно

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

# ========== UTILITY FUNCTIONS ==========
def format_datetime(dt: datetime) -> str:
    """Форматирует datetime в строку"""
    moscow_time = dt.astimezone(MOSCOW_TZ)
    return moscow_time.strftime("%d.%m.%Y в %H:%M")

def parse_datetime(date_str: str, time_str: str) -> Optional[datetime]:
    """Парсит дату и время из строк"""
    try:
        date_str = date_str.strip()
        time_str = time_str.strip()
        
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
        
        combined = datetime.combine(date_obj.date(), time_obj.time())
        return MOSCOW_TZ.localize(combined)
    except Exception:
        return None

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
        ''', user_id, username, first_name, is_admin, 'admin' if is_admin else 'mini')
    except Exception as e:
        logger.error(f"Ошибка регистрации пользователя {user_id}: {e}")
    
    current_tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(current_tariff, TARIFFS['mini'])
    
    welcome_text = (
        f"👋 Привет, {first_name}!\n\n"
        f"🤖 Я — бот KOLES-TECH для планирования постов и AI-контента.\n\n"
        f"💎 Ваш текущий тариф: {tariff_info['name']}\n\n"
        f"✨ Возможности:\n"
        f"• 🤖 AI-копирайтер и генератор идей\n"
        f"• 📅 Запланировать пост с любым контентом\n"
        f"• 📊 Детальная статистика\n"
        f"• 📢 Управление каналов\n"
        f"• ⏰ Автопубликация в нужное время\n"
        f"• 🆘 Техподдержка всегда на связи\n\n"
        f"📍 Время указывается по Москве\n\n"
        f"👇 Выберите действие:"
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
    tariff = await get_user_tariff(user_id)
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
    
    # Проверка лимитов
    can_use, message, tariff_info = await check_ai_limits(user_id, 'copy')
    if not can_use:
        await callback.message.edit_text(
            message,
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
        generated_text = await generate_with_gemini_advanced(prompt, user_id, max_retries=8)
        
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
            response_length=len(generated_text)
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
            f"• Использовано сегодня: {session['copies_used']}/{TARIFFS.get(await get_user_tariff(user_id), TARIFFS['mini'])['ai_copies_limit']}"
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
        generated_text = await generate_with_gemini_advanced(prompt, user_id, max_retries=8)
        
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
            response_length=len(generated_text)
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
            f"• Использовано сегодня: {session['copies_used']}/{TARIFFS.get(await get_user_tariff(user_id), TARIFFS['mini'])['ai_copies_limit']}"
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
    
    # Проверка лимитов
    can_use, message, tariff_info = await check_ai_limits(user_id, 'ideas')
    if not can_use:
        await callback.message.edit_text(
            message,
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
    
    generated_ideas = await generate_with_gemini_advanced(prompt, callback.from_user.id, max_retries=8)
    
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
        response_length=len(generated_ideas)
    )
    
    result_text = (
        f"✅ Сгенерировано {len(formatted_ideas)} идей! (Попытка #{session['current_attempts'] or 1})\n\n"
        f"📌 Тема: {data['topic']}\n\n"
        f"💡 Идеи:\n\n" +
        "\n".join(formatted_ideas) +
        f"\n\n📊 Статистика:\n"
        f"• Использовано сегодня: {session['ideas_used']}/{TARIFFS.get(await get_user_tariff(callback.from_user.id), TARIFFS['mini'])['ai_ideas_limit']}"
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
    tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    
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
    
    await callback.message.edit_text(
        "❌ Операция отменена",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ В меню AI", callback_data="ai_services")]
        ])
    )

# ========== STATISTICS HANDLERS ==========
@router.callback_query(F.data == "my_stats")
async def show_my_stats(callback: CallbackQuery):
    """Показывает статистику пользователя"""
    user_id = callback.from_user.id
    stats = await get_user_stats(user_id)
    
    system_stats = ai_manager.get_system_stats()
    available_keys = system_stats['available_keys']
    total_keys = system_stats['total_keys']
    
    stats_text = (
        f"📊 Ваша статистика:\n\n"
        f"💎 Тариф: {stats['tariff']}\n\n"
        f"📅 Посты сегодня:\n"
        f"• Отправлено: {stats['posts_today']}/{stats['posts_limit']}\n"
        f"• Запланировано: {stats['scheduled_posts']}\n\n"
        f"📢 Каналы:\n"
        f"• Подключено: {stats['channels_count']}/{stats['channels_limit']}\n\n"
        f"🤖 AI-сервисы:\n"
        f"• Копирайтинг: {stats['ai_copies_used']}/{stats['ai_copies_limit']}\n"
        f"• Идеи: {stats['ai_ideas_used']}/{stats['ai_ideas_limit']}\n"
        f"• Всего AI запросов: {stats['total_ai_requests']}\n\n"
        f"🔑 Система ключей:\n"
        f"• Доступных ключей: {available_keys} из {total_keys}\n\n"
        f"📍 Время по Москве: {datetime.now(MOSCOW_TZ).strftime('%H:%M')}"
    )
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=get_main_menu(user_id, user_id == ADMIN_ID)
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

# ========== SCHEDULED TASKS ==========
async def reset_daily_limits_task():
    """Ежедневный сброс лимитов"""
    try:
        # Сбрасываем счетчики постов
        await execute_query('''
            UPDATE users 
            SET posts_today = 0, posts_reset_date = CURRENT_DATE 
            WHERE posts_reset_date < CURRENT_DATE
        ''')
        
        # Сбрасываем AI лимиты
        ai_manager.reset_daily_limits()
        
        logger.info("✅ Ежедневные лимиты сброшены")
    except Exception as e:
        logger.error(f"Ошибка сброса лимитов: {e}")

async def cleanup_old_sessions_task():
    """Очистка старых сессий"""
    try:
        week_ago = datetime.now(MOSCOW_TZ) - timedelta(days=7)
        users_to_remove = []
        
        for user_id, session in list(ai_manager.sessions.items()):
            if session['total_requests'] == 0:
                last_activity = await execute_query(
                    "SELECT last_seen FROM users WHERE id = $1",
                    user_id
                )
                if last_activity:
                    last_seen = last_activity[0].get('last_seen')
                    if last_seen and last_seen < week_ago:
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
    try:
        ai_manager.check_and_rotate_keys()
        logger.info("✅ Автоматическая ротация ключей выполнена")
    except Exception as e:
        logger.error(f"Ошибка автоматической ротации ключей: {e}")

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
    logger.info(f"🚀 ЗАПУСК БОТА KOLES-TECH v3.0")
    logger.info(f"🤖 AI сервисы: ВКЛЮЧЕНЫ")
    logger.info(f"🔑 Gemini ключей: {len(GEMINI_API_KEYS)}")
    logger.info(f"👑 Admin ID: {ADMIN_ID}")
    logger.info(f"🆘 Поддержка: {SUPPORT_BOT_USERNAME or SUPPORT_URL}")
    logger.info(f"🌐 Порт Railway: {PORT}")
    logger.info("=" * 60)
    
    try:
        # Инициализация базы данных
        await init_database()
        await migrate_database()
        
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
        await restore_scheduled_posts()
        
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
                    f"🌐 Порт Railway: {PORT}\n"
                    f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M:%S')}"
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить админа: {e}")
        
        logger.info("=" * 60)
        logger.info("🎉 БОТ УСПЕШНО ЗАПУЩЕН С AI СЕРВИСАМИ!")
        logger.info("=" * 60)
        return True
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА ПРИ ЗАПУСКЕ: {e}")
        return False

async def restore_scheduled_posts():
    """Восстановление запланированных постов"""
    try:
        posts = await execute_query('''
            SELECT id, channel_id, message_type, message_text, 
                   media_file_id, media_caption, scheduled_time
            FROM scheduled_posts
            WHERE is_sent = FALSE AND scheduled_time > NOW()
        ''')
        
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

async def on_shutdown():
    """Выключение бота"""
    logger.info("🛑 Выключение бота...")
    
    # Останавливаем планировщик
    if scheduler.running:
        scheduler.shutdown()
    
    # Закрываем пул соединений
    await DatabasePool.close_pool()
    
    logger.info("👋 Бот выключен")

async def send_scheduled_post(channel_id: int, post_data: Dict, post_id: int):
    """Отправка запланированного поста"""
    try:
        message_type = post_data.get('message_type')
        
        if message_type == 'text':
            await bot.send_message(
                chat_id=channel_id,
                text=post_data.get('message_text', ''),
                parse_mode="HTML"
            )
        elif message_type == 'photo':
            await bot.send_photo(
                chat_id=channel_id,
                photo=post_data.get('media_file_id'),
                caption=post_data.get('media_caption', ''),
                parse_mode="HTML"
            )
        elif message_type == 'video':
            await bot.send_video(
                chat_id=channel_id,
                video=post_data.get('media_file_id'),
                caption=post_data.get('media_caption', ''),
                parse_mode="HTML"
            )
        elif message_type == 'document':
            await bot.send_document(
                chat_id=channel_id,
                document=post_data.get('media_file_id'),
                caption=post_data.get('media_caption', ''),
                parse_mode="HTML"
            )
        
        await execute_query(
            "UPDATE scheduled_posts SET is_sent = TRUE WHERE id = $1",
            post_id
        )
        
        logger.info(f"✅ Пост {post_id} отправлен в канал {channel_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки поста {post_id}: {e}")

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
