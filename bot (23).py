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
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ContentType, InputFile
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
        "AIzaSyANdn61NlmIsibgohrNI8dNVbLNr-jv9_c",
        "AIzaSyAQWjtmj0czZ1NtU3tydH6d-ncz4hzy0zo",
        "AIzaSyCfdW8PSsY7bCzlBtuJuQXiVGuRQOPO7XA",
        "AIzaSyCf-95v3QI06jI6p40hffZ7XPBvqeDwZXE",
        "AIzaSyDHEGSl0soW0ei25bebXJnAV3JAsgM0xxQ",
        "AIzaSyBHbtCii0kasYAtonJjCb9BQ6t1tLGAFL4",
        "AIzaSyCHfiXQ3-6tGD-eNPKn9USEtNL7G92gPNk",
        "AIzaSyDESq3Lcey3jQAKVgkzLB0ilnrG9qL8t04",
        "AIzaSyC0lOzADIx-_lbUk1wtcxGBRi6UgIjPQM4"
    ]

# Убираем пустые ключи
GEMINI_API_KEYS = [key.strip() for key in GEMINI_API_KEYS if key and key.strip()]

if not GEMINI_API_KEYS:
    print("❌ ОШИБКА: Не указаны Gemini API ключи")
    sys.exit(1)

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
ALTERNATIVE_MODELS = ["gemini-2.5-flash", "gemini-1.5-flash", "gemini-1.5-pro"]

# Ротация настроек
MAX_403_RETRIES = 3  # 3 попытки при ошибке 403
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
        self.models = [GEMINI_MODEL] + [m for m in ALTERNATIVE_MODELS if m != GEMINI_MODEL]
        self.user_request_counts = defaultdict(int)
        self.last_key_rotation = datetime.now(MOSCOW_TZ)
        self.current_key_index = random.randint(0, len(GEMINI_API_KEYS) - 1)
        
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
                "priority": 50,
                "failed_users": set(),
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
                'failed_keys': set(),
                'last_success_time': None
            }
        return self.sessions[user_id]
    
    def get_best_key(self, user_id: int) -> Tuple[Optional[str], int, str]:
        """Выбирает лучший доступный ключ с интеллектуальной ротацией"""
        session = self.get_session(user_id)
        
        # Получаем список доступных ключей с приоритетами
        available_keys = []
        
        for i, key in enumerate(GEMINI_API_KEYS):
            if self._is_key_available(key, user_id):
                stats = self.key_stats[key]
                priority = stats['priority']
                
                # Понижаем приоритет если ключ уже не сработал для этого пользователя
                if key in session['failed_keys']:
                    priority += 50
                
                # Повышаем приоритет если ключ недавно успешно использовался
                if stats['last_success']:
                    hours_since_success = (datetime.now(MOSCOW_TZ) - stats['last_success']).total_seconds() / 3600
                    if hours_since_success < 1:
                        priority -= 30
                
                # Повышаем приоритет ключам, которые давно не использовались
                if stats['last_used']:
                    hours_since_use = (datetime.now(MOSCOW_TZ) - stats['last_used']).total_seconds() / 3600
                    if hours_since_use > 2:
                        priority -= 20
                
                available_keys.append((priority, i, key))
        
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
        
        # Сортируем по приоритету (ниже = лучше)
        available_keys.sort(key=lambda x: x[0])
        
        # Выбираем ключ с наилучшим приоритетом
        best_priority, key_index, best_key = available_keys[0]
        
        # Обновляем статистику
        session['current_key_index'] = (key_index + 1) % len(GEMINI_API_KEYS)
        self._update_key_stats_on_use(best_key)
        
        # Выбираем модель
        model_index = self.current_model_index % len(self.models)
        model_name = self.models[model_index]
        
        return best_key, key_index, model_name
    
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
        
        # Проверяем, не провалился ли ключ для этого пользователя
        if user_id in stats['failed_users']:
            # Даем шанс через 1 час
            if stats['last_error']:
                hours_since_error = (datetime.now(MOSCOW_TZ) - stats['last_error']).total_seconds() / 3600
                if hours_since_error > 1:
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
            stats['priority'] = min(100, stats['priority'] + 30)
            logger.warning(f"Ключ {key[:15]}... получил 403 ошибку. Приоритет: {stats['priority']}")
            
            if stats['403_errors'] >= MAX_403_RETRIES:
                stats['blocked_until'] = datetime.now(MOSCOW_TZ) + timedelta(seconds=KEY_BLOCK_DURATION)
                stats['priority'] = 95
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
        stats['priority'] = max(1, stats['priority'] - 25)
        stats['blocked_until'] = None
        stats['last_success'] = datetime.now(MOSCOW_TZ)
        stats['failed_users'].discard(user_id)
        
        session = self.get_session(user_id)
        session['last_successful_key'] = key
        session['consecutive_errors'] = 0
        session['current_attempts'] = 0
        session['failed_keys'].discard(key)
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
        
        available_keys = len([k for k, v in self.key_stats.items() if v['blocked_until'] is None or v['blocked_until'] < datetime.now(MOSCOW_TZ)])
        
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
        
        # Очищаем старые failed_users (старше 1 часа)
        for key in GEMINI_API_KEYS:
            stats = self.key_stats[key]
            if stats['last_error']:
                hours_since_error = (now - stats['last_error']).total_seconds() / 3600
                if hours_since_error > 1:
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
            
            logger.info(f"Попытка #{attempt} | user_{user_id} | key_{key_index} | модель: {model_name}")
            
            genai.configure(api_key=key)
            
            try:
                model = genai.GenerativeModel(model_name)
                
                response = await asyncio.to_thread(
                    model.generate_content,
                    prompt,
                    generation_config={
                        "temperature": 0.8,
                        "top_p": 0.95,
                        "top_k": 40,
                        "max_output_tokens": 4000,
                    }
                )
                
                if response.text:
                    ai_manager.mark_key_success(key, user_id)
                    logger.info(f"✅ Успешно | user_{user_id} | ключ: {key_index} | модель: {model_name} | попытка: {attempt}")
                    return response.text.strip()
                else:
                    raise Exception("Пустой ответ от модели")
                
            except Exception as model_error:
                error_str = str(model_error)
                
                # Пробуем другую модель если текущая не поддерживается
                if "not supported" in error_str.lower() or "not found" in error_str.lower():
                    logger.warning(f"Модель {model_name} не поддерживается, пробую следующую")
                    ai_manager.rotate_model()
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
                wait_time = 0.5 * attempt
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
            last_seen TIMESTAMPTZ DEFAULT NOW(),
            tariff_expires DATE DEFAULT NULL,
            subscription_days INTEGER DEFAULT 0
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
            sent_at TIMESTAMPTZ,
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
        # Добавляем колонку sent_at если её нет
        try:
            await execute_query('''
                ALTER TABLE scheduled_posts 
                ADD COLUMN IF NOT EXISTS sent_at TIMESTAMPTZ
            ''')
            logger.info("✅ Добавлена колонка sent_at в таблицу scheduled_posts")
        except Exception as e:
            logger.warning(f"Ошибка добавления колонки sent_at: {e}")
        
        columns_to_add = [
            ('users', 'tariff_expires', 'DATE'),
            ('users', 'subscription_days', 'INTEGER')
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
        "SELECT tariff, is_admin, tariff_expires FROM users WHERE id = $1", 
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
    
    # Проверяем срок действия тарифа
    tariff_expires = user[0].get('tariff_expires')
    if tariff_expires and tariff_expires < datetime.now(MOSCOW_TZ).date():
        # Тариф истек, возвращаем к минимуму
        await execute_query(
            "UPDATE users SET tariff = 'mini', tariff_expires = NULL, subscription_days = 0 WHERE id = $1",
            user_id
        )
        return 'mini'
    
    return user[0].get('tariff', 'mini')

async def update_user_subscription(user_id: int, tariff: str, days: int) -> bool:
    """Обновляет подписку пользователя"""
    try:
        today = datetime.now(MOSCOW_TZ).date()
        
        # Получаем текущую дату окончания
        user = await execute_query(
            "SELECT tariff_expires FROM users WHERE id = $1",
            user_id
        )
        
        if user and user[0]['tariff_expires']:
            # Продлеваем существующую подписку
            expires_date = user[0]['tariff_expires']
            if expires_date >= today:
                # Продлеваем с текущей даты окончания
                new_expires = expires_date + timedelta(days=days)
            else:
                # Начинаем с сегодня
                new_expires = today + timedelta(days=days)
        else:
            # Новая подписка
            new_expires = today + timedelta(days=days)
        
        await execute_query('''
            UPDATE users 
            SET tariff = $1, 
                tariff_expires = $2,
                subscription_days = subscription_days + $3
            WHERE id = $4
        ''', tariff, new_expires, days, user_id)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления подписки: {e}")
        return False

async def get_user_subscription_info(user_id: int) -> Dict:
    """Получает информацию о подписке пользователя"""
    user = await execute_query(
        "SELECT tariff, tariff_expires, subscription_days FROM users WHERE id = $1",
        user_id
    )
    
    if not user:
        return {'tariff': 'mini', 'expires': None, 'days': 0, 'expired': True}
    
    data = user[0]
    tariff_expires = data.get('tariff_expires')
    
    if tariff_expires:
        expired = tariff_expires < datetime.now(MOSCOW_TZ).date()
        days_left = (tariff_expires - datetime.now(MOSCOW_TZ).date()).days if not expired else 0
    else:
        expired = True
        days_left = 0
    
    return {
        'tariff': data.get('tariff', 'mini'),
        'expires': tariff_expires,
        'days': data.get('subscription_days', 0),
        'expired': expired,
        'days_left': days_left
    }

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
        
        # Правильная обработка post_data
        message_type = post_data.get('message_type', 'text')
        message_text = post_data.get('message_text')
        media_file_id = post_data.get('media_file_id')
        media_caption = post_data.get('media_caption')
        
        result = await execute_query('''
            INSERT INTO scheduled_posts 
            (user_id, channel_id, message_type, message_text, media_file_id, media_caption, scheduled_time)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
        ''', 
        user_id,
        channel_id,
        message_type,
        message_text,
        media_file_id,
        media_caption,
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
        
        # Информация о подписке
        subscription_info = await get_user_subscription_info(user_id)
        
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
            'scheduled_posts': scheduled_posts,
            'subscription_expires': subscription_info['expires'],
            'subscription_days_left': subscription_info['days_left'],
            'subscription_expired': subscription_info['expired']
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
        SELECT id, username, first_name, tariff, is_admin, created_at,
               tariff_expires, subscription_days
        FROM users 
        ORDER BY created_at DESC
    ''')

async def get_user_by_id(user_id: int) -> Optional[Dict]:
    """Получает пользователя по ID"""
    result = await execute_query(
        "SELECT id, username, first_name, tariff, is_admin, created_at, tariff_expires, subscription_days FROM users WHERE id = $1",
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

def get_admin_subscription_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для управления подписками в админке"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Выдать подписку", callback_data="admin_grant_subscription"),
            InlineKeyboardButton(text="🔄 Продлить подписку", callback_data="admin_extend_subscription")
        ],
        [
            InlineKeyboardButton(text="📋 Список подписок", callback_data="admin_list_subscriptions"),
            InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_panel")
        ]
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
    waiting_for_tariff_selection = State()
    waiting_for_days_selection = State()
    waiting_for_confirm_grant = State()
    waiting_for_confirm_extend = State()

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
            f"⏳ Генерирую текст... Пробю разные ключи (макс. 8 попыток)"
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

# ========== SCHEDULE POST HANDLERS (ИСПРАВЛЕННЫЕ) ==========
@router.callback_query(F.data == "schedule_post")
async def schedule_post_start(callback: CallbackQuery, state: FSMContext):
    """Начало планирования поста"""
    user_id = callback.from_user.id
    
    # Проверяем лимиты постов
    posts_today = await get_user_posts_today(user_id)
    channels_limit, posts_limit, _, _ = await get_tariff_limits(user_id)
    
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
            f"• Доступно каналов: {await get_user_channels_count(user_id)}/{channels_limit}\n\n"
            f"⏳ Обновление через: {hours}ч {minutes}м",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
            ])
        )
        return
    
    # Получаем каналы пользователя
    channels = await get_user_channels(user_id)
    
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
    channels = await get_user_channels(callback.from_user.id)
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
        data = await state.get_data()
        date_str = data.get('date_str')
        
        # Пробуем разные форматы времени
        time_formats = ["%H:%M", "%H.%M"]
        time_obj = None
        
        for fmt in time_formats:
            try:
                time_obj = datetime.strptime(message.text.strip(), fmt)
                break
            except ValueError:
                continue
        
        if not time_obj:
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
        
        # Парсим полную дату и время
        scheduled_datetime = parse_datetime(date_str, message.text.strip())
        
        if not scheduled_datetime:
            await message.answer(
                "❌ Ошибка при обработке даты и времени!\n\n"
                "Попробуйте еще раз:",
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
    user_id = callback.from_user.id
    data = await state.get_data()
    
    channel_id = data.get('channel_id')
    channel_name = data.get('channel_name')
    post_data = data.get('post_data', {})
    scheduled_datetime = data.get('scheduled_datetime')
    
    # Сохраняем пост в базу данных
    post_id = await save_scheduled_post(user_id, channel_id, post_data, scheduled_datetime)
    
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
    try:
        scheduler.add_job(
            send_scheduled_post_job,
            trigger='date',
            run_date=scheduled_datetime,
            args=[post_id],
            id=f"post_{post_id}",
            replace_existing=True,
            misfire_grace_time=300  # 5 минут на опоздание
        )
        
        # Увеличиваем счетчик постов пользователя
        await increment_user_posts(user_id)
        
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
        
    except Exception as e:
        logger.error(f"Ошибка добавления задачи в планировщик: {e}")
        
        # Удаляем пост из БД если не удалось добавить в планировщик
        await execute_query("DELETE FROM scheduled_posts WHERE id = $1", post_id)
        
        await callback.message.edit_text(
            "❌ Ошибка при планировании поста!\n\n"
            f"Техническая информация: {str(e)[:100]}\n\n"
            "Попробуйте позже или обратитесь в поддержку.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
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

# ========== ИСПРАВЛЕННАЯ ФУНКЦИЯ ОТПРАВКИ ПОСТА ==========
async def send_scheduled_post_job(post_id: int):
    """Функция для отправки запланированного поста (исправленная)"""
    try:
        # Получаем данные поста
        post_data = await execute_query(
            "SELECT user_id, channel_id, message_type, message_text, media_file_id, media_caption, is_sent FROM scheduled_posts WHERE id = $1",
            post_id
        )
        
        if not post_data:
            logger.error(f"❌ Пост {post_id} не найден в базе данных")
            return
        
        post = post_data[0]
        
        # Проверяем, не был ли уже отправлен
        if post['is_sent']:
            logger.info(f"⚠️ Пост {post_id} уже был отправлен ранее")
            return
        
        user_id = post['user_id']
        channel_id = post['channel_id']
        message_type = post['message_type']
        
        try:
            # Отправляем пост
            if message_type == 'text':
                await bot.send_message(
                    chat_id=channel_id,
                    text=post['message_text'],
                    parse_mode="HTML"
                )
            elif message_type == 'photo':
                await bot.send_photo(
                    chat_id=channel_id,
                    photo=post['media_file_id'],
                    caption=post['media_caption'] or '',
                    parse_mode="HTML"
                )
            elif message_type == 'video':
                await bot.send_video(
                    chat_id=channel_id,
                    video=post['media_file_id'],
                    caption=post['media_caption'] or '',
                    parse_mode="HTML"
                )
            elif message_type == 'document':
                await bot.send_document(
                    chat_id=channel_id,
                    document=post['media_file_id'],
                    caption=post['media_caption'] or '',
                    parse_mode="HTML"
                )
            
            # Обновляем статус поста
            await execute_query(
                "UPDATE scheduled_posts SET is_sent = TRUE, sent_at = NOW() WHERE id = $1",
                post_id
            )
            
            # Отправляем уведомление пользователю
            try:
                await bot.send_message(
                    user_id,
                    f"✅ Пост #{post_id} успешно опубликован!\n\n"
                    f"📢 Канал ID: {channel_id}\n"
                    f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}\n\n"
                    f"📍 Пост был автоматически отправлен в запланированное время."
                )
            except Exception as e:
                logger.warning(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
            
            logger.info(f"✅ Пост {post_id} успешно отправлен в канал {channel_id}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка отправки поста {post_id}: {e}")
            
            # Пытаемся отправить уведомление об ошибке пользователю
            try:
                error_msg = str(e)[:200]
                await bot.send_message(
                    user_id,
                    f"❌ Ошибка отправки запланированного поста #{post_id}!\n\n"
                    f"Техническая информация: {error_msg}\n\n"
                    f"📍 Возможные причины:\n"
                    f"• Бот удален из канала\n"
                    f"• Нет прав на публикацию\n"
                    f"• Канал удален или заблокирован\n\n"
                    f"Проверьте права бота в канале и попробуйте запланировать пост снова."
                )
            except Exception as notify_error:
                logger.error(f"Не удалось отправить уведомление об ошибке: {notify_error}")
    
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в send_scheduled_post_job для поста {post_id}: {e}")
        logger.error(traceback.format_exc())

# ========== STATISTICS HANDLERS ==========
@router.callback_query(F.data == "my_stats")
async def show_my_stats(callback: CallbackQuery):
    """Показывает статистику пользователя"""
    user_id = callback.from_user.id
    stats = await get_user_stats(user_id)
    
    system_stats = ai_manager.get_system_stats()
    available_keys = system_stats['available_keys']
    total_keys = system_stats['total_keys']
    
    # Форматируем дату окончания подписки
    expires_info = ""
    if stats['subscription_expires']:
        expires_date = stats['subscription_expires']
        if stats['subscription_expired']:
            expires_info = f"❌ Подписка истекла: {expires_date.strftime('%d.%m.%Y')}"
        else:
            expires_info = f"✅ Подписка активна до: {expires_date.strftime('%d.%m.%Y')} (осталось {stats['subscription_days_left']} дней)"
    else:
        expires_info = "ℹ️ Подписка отсутствует"
    
    stats_text = (
        f"📊 Ваша статистика:\n\n"
        f"💎 Тариф: {stats['tariff']}\n"
        f"{expires_info}\n\n"
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

# ========== CHANNELS HANDLERS ==========
@router.callback_query(F.data == "my_channels")
async def show_my_channels(callback: CallbackQuery):
    """Показывает каналы пользователя"""
    user_id = callback.from_user.id
    channels = await get_user_channels(user_id)
    channels_count = len(channels)
    channels_limit, _, _, _ = await get_tariff_limits(user_id)
    
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
    
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить список", callback_data="my_channels")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
        ])
    )

# ========== TARIFFS HANDLERS ==========
@router.callback_query(F.data == "tariffs")
async def show_tariffs(callback: CallbackQuery):
    """Показывает тарифы"""
    user_id = callback.from_user.id
    user_tariff = await get_user_tariff(user_id)
    
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
    user_tariff = await get_user_tariff(user_id)
    
    if tariff_id == user_tariff:
        await callback.answer("❌ Это ваш текущий тариф!", show_alert=True)
        return
    
    tariff_info = TARIFFS.get(tariff_id)
    if not tariff_info:
        await callback.answer("❌ Тариф не найден!", show_alert=True)
        return
    
    # Создаем заказ
    success = await create_tariff_order(user_id, tariff_id)
    
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

# ========== ADMIN HANDLERS ==========
@router.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    """Админ панель"""
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет прав администратора!", show_alert=True)
        return
    
    # Получаем статистику
    total_users = await execute_query("SELECT COUNT(*) as count FROM users")
    total_users = total_users[0]['count'] if total_users else 0
    
    active_users = await execute_query("SELECT COUNT(*) as count FROM users WHERE last_seen > NOW() - INTERVAL '7 days'")
    active_users = active_users[0]['count'] if active_users else 0
    
    pending_orders = await execute_query("SELECT COUNT(*) as count FROM tariff_orders WHERE status = 'pending'")
    pending_orders = pending_orders[0]['count'] if pending_orders else 0
    
    active_subscriptions = await execute_query("SELECT COUNT(*) as count FROM users WHERE tariff_expires >= CURRENT_DATE")
    active_subscriptions = active_subscriptions[0]['count'] if active_subscriptions else 0
    
    system_stats = ai_manager.get_system_stats()
    
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
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика пользователей", callback_data="admin_users")],
            [InlineKeyboardButton(text="🛒 Заказы тарифов", callback_data="admin_orders")],
            [InlineKeyboardButton(text="🤖 Статистика AI", callback_data="admin_ai_stats")],
            [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
            [InlineKeyboardButton(text="💎 Управление подписками", callback_data="admin_subscriptions")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
        ])
    )

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
        user = await get_user_by_id(target_user_id)
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
            subscription_info = await get_user_subscription_info(target_user_id)
            
            if subscription_info['expired'] and not subscription_info['expires']:
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
            if subscription_info['expires']:
                expires_text = subscription_info['expires'].strftime('%d.%m.%Y')
                if subscription_info['expired']:
                    expires_text += " (истекла)"
                else:
                    expires_text += f" (осталось {subscription_info['days_left']} дней)"
            
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
    tariff_id = callback.data.split("_")[2]
    data = await state.get_data()
    target_user_id = data.get('target_user_id')
    
    await state.update_data(tariff_id=tariff_id)
    
    # Получаем информацию о пользователе
    user = await get_user_by_id(target_user_id)
    
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
        
        user = await get_user_by_id(target_user_id)
        
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
            subscription_info = await get_user_subscription_info(target_user_id)
            current_tariff = user.get('tariff', 'mini')
            tariff_name = TARIFFS.get(current_tariff, {}).get('name', current_tariff)
            
            await state.set_state(AdminStates.waiting_for_confirm_extend)
            
            expires_text = "Нет подписки"
            new_expires = None
            
            if subscription_info['expires']:
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
        
        user = await get_user_by_id(target_user_id)
        
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
            subscription_info = await get_user_subscription_info(target_user_id)
            current_tariff = user.get('tariff', 'mini')
            tariff_name = TARIFFS.get(current_tariff, {}).get('name', current_tariff)
            
            await state.set_state(AdminStates.waiting_for_confirm_extend)
            
            expires_text = "Нет подписки"
            new_expires = None
            
            if subscription_info['expires']:
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
    success = await update_user_subscription(target_user_id, tariff_id, days)
    
    if success:
        # Получаем информацию о пользователе
        user = await get_user_by_id(target_user_id)
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
        
        # Создаем запись о заказе
        await execute_query('''
            INSERT INTO tariff_orders (user_id, tariff, status, admin_notes)
            VALUES ($1, $2, 'granted_by_admin', $3)
        ''', target_user_id, tariff_id, f"Выдано админом {user_id} на {days} дней")
        
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
    user = await get_user_by_id(target_user_id)
    current_tariff = user.get('tariff', 'mini')
    
    # Обновляем подписку пользователя
    success = await update_user_subscription(target_user_id, current_tariff, days)
    
    if success:
        tariff_name = TARIFFS.get(current_tariff, {}).get('name', current_tariff)
        
        # Получаем обновленную информацию о подписке
        subscription_info = await get_user_subscription_info(target_user_id)
        
        # Отправляем уведомление пользователю
        try:
            await bot.send_message(
                target_user_id,
                f"🎉 ВАША ПОДПИСКА ПРОДЛЕНА!\n\n"
                f"💎 Тариф: {tariff_name}\n"
                f"📅 Добавлено дней: {days}\n"
                f"📅 Новая дата окончания: {subscription_info['expires'].strftime('%d.%m.%Y') if subscription_info['expires'] else 'N/A'}\n"
                f"🆔 Ваш ID: {target_user_id}\n\n"
                f"📍 Подписка успешно продлена.\n"
                f"Вы можете проверить статус в разделе 'Моя статистика'.\n\n"
                f"Спасибо за использование KOLES-TECH! 🤖"
            )
        except Exception as e:
            logger.error(f"Не удалось отправить уведомление пользователю {target_user_id}: {e}")
        
        # Создаем запись о заказе
        await execute_query('''
            INSERT INTO tariff_orders (user_id, tariff, status, admin_notes)
            VALUES ($1, $2, 'extended_by_admin', $3)
        ''', target_user_id, current_tariff, f"Продлено админом {user_id} на {days} дней")
        
        expires_text = subscription_info['expires'].strftime('%d.%m.%Y') if subscription_info['expires'] else 'N/A'
        
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
    ''')
    
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
        
        # Проверяем истекшие подписки
        expired_subscriptions = await execute_query('''
            SELECT id, first_name, username 
            FROM users 
            WHERE tariff_expires < CURRENT_DATE AND tariff != 'mini'
        ''')
        
        for user in expired_subscriptions:
            # Понижаем тариф до минимума
            await execute_query('''
                UPDATE users 
                SET tariff = 'mini' 
                WHERE id = $1 AND tariff != 'admin'
            ''', user['id'])
            
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

# ========== ВОССТАНОВЛЕНИЕ ЗАПЛАНИРОВАННЫХ ПОСТОВ ==========
async def restore_scheduled_posts():
    """Восстановление запланированных постов при запуске"""
    try:
        posts = await execute_query('''
            SELECT id, channel_id, message_type, message_text, 
                   media_file_id, media_caption, scheduled_time
            FROM scheduled_posts
            WHERE is_sent = FALSE AND scheduled_time > NOW()
            ORDER BY scheduled_time ASC
        ''')
        
        restored = 0
        for post in posts:
            try:
                scheduled_time = post['scheduled_time']
                if scheduled_time.tzinfo is None:
                    scheduled_time = pytz.UTC.localize(scheduled_time)
                
                # Преобразуем в московское время для логирования
                scheduled_moscow = scheduled_time.astimezone(MOSCOW_TZ)
                time_until = scheduled_time - datetime.now(pytz.UTC)
                hours_until = time_until.total_seconds() / 3600
                
                scheduler.add_job(
                    send_scheduled_post_job,
                    trigger='date',
                    run_date=scheduled_time,
                    args=[post['id']],
                    id=f"post_{post['id']}",
                    replace_existing=True,
                    misfire_grace_time=300
                )
                
                restored += 1
                logger.info(f"✅ Восстановлен пост {post['id']} на {scheduled_moscow.strftime('%d.%m.%Y %H:%M')} МСК (через {hours_until:.1f} часов)")
                
            except Exception as e:
                logger.error(f"Ошибка восстановления поста {post['id']}: {e}")
        
        logger.info(f"✅ Восстановлено {restored} запланированных постов")
        
        # Также восстанавливаем просроченные посты (не старше 24 часов)
        missed_posts = await execute_query('''
            SELECT id, channel_id, scheduled_time
            FROM scheduled_posts
            WHERE is_sent = FALSE AND scheduled_time <= NOW() AND scheduled_time > NOW() - INTERVAL '24 hours'
        ''')
        
        for post in missed_posts:
            try:
                logger.warning(f"⚠️ Пропущенный пост {post['id']}, отправляю немедленно")
                await send_scheduled_post_job(post['id'])
            except Exception as e:
                logger.error(f"Ошибка отправки пропущенного поста {post['id']}: {e}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при восстановлении постов: {e}")

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
    logger.info(f"🚀 ЗАПУСК БОТА KOLES-TECH v3.0 (ИСПРАВЛЕННЫЙ)")
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
                    f"📅 Система планирования постов: ИСПРАВЛЕНА\n"
                    f"🌐 Порт Railway: {PORT}\n"
                    f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M:%S')}"
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить админа: {e}")
        
        logger.info("=" * 60)
        logger.info("🎉 БОТ УСПЕШНО ЗАПУЩЕН С ИСПРАВЛЕННОЙ СИСТЕМОЙ ПЛАНИРОВАНИЯ!")
        logger.info("=" * 60)
        return True
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА ПРИ ЗАПУСКЕ: {e}")
        traceback.print_exc()
        return False

async def check_scheduler_status():
    """Проверка статуса планировщика"""
    try:
        jobs = scheduler.get_jobs()
        scheduled_posts = await execute_query(
            "SELECT COUNT(*) as count FROM scheduled_posts WHERE is_sent = FALSE"
        )
        scheduled_count = scheduled_posts[0]['count'] if scheduled_posts else 0
        
        logger.info(f"📊 Статус планировщика: {len(jobs)} задач, {scheduled_count} постов в очереди")
    except Exception as e:
        logger.error(f"Ошибка проверки статуса планировщика: {e}")

async def on_shutdown():
    """Выключение бота"""
    logger.info("🛑 Выключение бота...")
    
    # Останавливаем планировщик
    if scheduler.running:
        scheduler.shutdown()
    
    # Закрываем пул соединений
    await DatabasePool.close_pool()
    
    logger.info("👋 Бот выключен")

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
