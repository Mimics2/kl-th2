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
        "description": "Бесплатный тариф для начала работы",
        "duration_days": 30  # Добавлено: длительность тарифа
    },
    Tariff.STANDARD.value: {
        "name": "⭐ Standard",
        "price": 5,  # Исправлено: было 4, стало 5
        "currency": "USD",
        "channels_limit": 2,
        "daily_posts_limit": 6,
        "ai_copies_limit": 3,
        "ai_ideas_limit": 30,
        "description": "Для активных пользователей",
        "duration_days": 30
    },
    Tariff.VIP.value: {
        "name": "👑 VIP",
        "price": 10,  # Исправлено: было 7, стало 10
        "currency": "USD",
        "channels_limit": 3,
        "daily_posts_limit": 12,
        "ai_copies_limit": 7,
        "ai_ideas_limit": 50,
        "description": "Максимальные возможности",
        "duration_days": 30
    },
    Tariff.ADMIN.value: {
        "name": "⚡ Admin",
        "price": 0,
        "currency": "USD",
        "channels_limit": 999,
        "daily_posts_limit": 999,
        "ai_copies_limit": 999,
        "ai_ideas_limit": 999,
        "description": "Безлимитный доступ",
        "duration_days": 9999
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
            tariff_expires_at TIMESTAMPTZ,  # Добавлено: срок действия тарифа
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
        CREATE INDEX IF NOT EXISTS idx_users_tariff_expires ON users(tariff_expires_at)
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
            ('users', 'tariff_expires_at', 'TIMESTAMPTZ'),
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
            # Устанавливаем админу вечный тариф
            await execute_query('''
                INSERT INTO users (id, is_admin, tariff, tariff_expires_at) 
                VALUES ($1, TRUE, 'admin', $2)
                ON CONFLICT (id) DO UPDATE 
                SET is_admin = TRUE, tariff = 'admin', tariff_expires_at = $2
            ''', ADMIN_ID, datetime.now(MOSCOW_TZ) + timedelta(days=9999))
        
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
        "SELECT tariff, is_admin, tariff_expires_at FROM users WHERE id = $1", 
        user_id
    )
    
    if not user:
        # Создаем нового пользователя с бесплатным тарифом на 30 дней
        expires_at = datetime.now(MOSCOW_TZ) + timedelta(days=30)
        await execute_query(
            "INSERT INTO users (id, tariff, tariff_expires_at) VALUES ($1, 'mini', $2) ON CONFLICT DO NOTHING",
            user_id, expires_at
        )
        return 'mini'
    
    # Проверяем срок действия тарифа
    tariff_expires = user[0].get('tariff_expires_at')
    if tariff_expires and tariff_expires < datetime.now(MOSCOW_TZ):
        # Тариф истек, ставим mini
        await execute_query(
            "UPDATE users SET tariff = 'mini', tariff_expires_at = $1 WHERE id = $2",
            datetime.now(MOSCOW_TZ) + timedelta(days=30), user_id
        )
        return 'mini'
    
    if user[0].get('is_admin'):
        return 'admin'
    
    return user[0].get('tariff', 'mini')

async def get_tariff_expires_date(user_id: int) -> Optional[datetime]:
    """Получает дату окончания тарифа"""
    result = await execute_query(
        "SELECT tariff_expires_at FROM users WHERE id = $1",
        user_id
    )
    
    if result and result[0]['tariff_expires_at']:
        return result[0]['tariff_expires_at']
    return None

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
        
        # Срок действия тарифа
        expires_at = await get_tariff_expires_date(user_id)
        days_left = 0
        if expires_at:
            days_left = max(0, (expires_at - datetime.now(MOSCOW_TZ)).days)
        
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
            'tariff_expires_days': days_left,
            'tariff_expires_date': expires_at
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
                    f"📅 На 30 дней\n"
                    f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}\n\n"
                    f"ℹ️ Для выдачи тарифа используйте команду /admin"
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
        SELECT id, username, first_name, tariff, tariff_expires_at, is_admin, created_at
        FROM users 
        ORDER BY created_at DESC
    ''')

async def get_user_by_id(user_id: int) -> Optional[Dict]:
    """Получает пользователя по ID"""
    result = await execute_query(
        "SELECT id, username, first_name, tariff, tariff_expires_at, is_admin, created_at FROM users WHERE id = $1",
        user_id
    )
    
    if result:
        return result[0]
    return None

async def update_user_tariff(user_id: int, tariff: str, extend_days: int = 30) -> bool:
    """Обновляет тариф пользователя на определенный срок"""
    try:
        # Получаем текущую дату окончания
        current_expires = await get_tariff_expires_date(user_id)
        if not current_expires or current_expires < datetime.now(MOSCOW_TZ):
            # Если тарифа нет или он истек, начинаем с сегодня
            new_expires = datetime.now(MOSCOW_TZ) + timedelta(days=extend_days)
        else:
            # Если тариф активен, продлеваем
            new_expires = current_expires + timedelta(days=extend_days)
        
        await execute_query('''
            UPDATE users SET tariff = $1, tariff_expires_at = $2 WHERE id = $3
        ''', tariff, new_expires, user_id)
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления тарифа: {e}")
        return False

async def force_update_user_tariff(user_id: int, tariff: str, admin_id: int, extend_days: int = 30) -> Tuple[bool, str]:
    """Принудительно обновляет тариф пользователя (админ)"""
    try:
        user = await get_user_by_id(user_id)
        if not user:
            return False, f"❌ Пользователь с ID {user_id} не найден"
        
        old_tariff = user.get('tariff', 'mini')
        
        success = await update_user_tariff(user_id, tariff, extend_days)
        if success:
            await execute_query('''
                INSERT INTO tariff_orders (user_id, tariff, status, admin_notes)
                VALUES ($1, $2, 'force_completed', $3)
            ''', user_id, tariff, f"Принудительное обновление админом {admin_id} на {extend_days} дней")
            
            tariff_info = TARIFFS.get(tariff, {})
            old_tariff_info = TARIFFS.get(old_tariff, {})
            
            new_expires = await get_tariff_expires_date(user_id)
            expires_str = new_expires.strftime("%d.%m.%Y %H:%M") if new_expires else "Не указано"
            
            return True, (
                f"✅ Тариф пользователя {user_id} обновлен!\n\n"
                f"📋 Информация:\n"
                f"👤 Пользователь: {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
                f"🔄 Старый тариф: {old_tariff_info.get('name', old_tariff)}\n"
                f"🆕 Новый тариф: {tariff_info.get('name', tariff)}\n"
                f"📅 Срок: {extend_days} дней\n"
                f"📆 Действует до: {expires_str}\n"
                f"👑 Обновил: админ {admin_id}\n"
                f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}"
            )
        else:
            return False, f"❌ Ошибка при обновлении тарифа пользователя {user_id}"
    except Exception as e:
        logger.error(f"Ошибка принудительного обновления тарифа: {e}")
        return False, f"❌ Ошибка: {str(e)}"

async def extend_user_tariff(user_id: int, extend_days: int = 30) -> Tuple[bool, str]:
    """Продлевает текущий тариф пользователя"""
    try:
        user = await get_user_by_id(user_id)
        if not user:
            return False, f"❌ Пользователь с ID {user_id} не найден"
        
        current_tariff = user.get('tariff', 'mini')
        current_expires = await get_tariff_expires_date(user_id)
        
        if not current_expires or current_expires < datetime.now(MOSCOW_TZ):
            new_expires = datetime.now(MOSCOW_TZ) + timedelta(days=extend_days)
        else:
            new_expires = current_expires + timedelta(days=extend_days)
        
        await execute_query('''
            UPDATE users SET tariff_expires_at = $1 WHERE id = $2
        ''', new_expires, user_id)
        
        tariff_info = TARIFFS.get(current_tariff, {})
        expires_str = new_expires.strftime("%d.%m.%Y %H:%M")
        
        return True, (
            f"✅ Тариф успешно продлен!\n\n"
            f"📋 Информация:\n"
            f"👤 Пользователь: {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
            f"💎 Тариф: {tariff_info.get('name', current_tariff)}\n"
            f"📅 Продлено на: {extend_days} дней\n"
            f"📆 Действует до: {expires_str}\n"
            f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}"
        )
    except Exception as e:
        logger.error(f"Ошибка продления тарифа: {e}")
        return False, f"❌ Ошибка: {str(e)}"

# ========== KEYBOARDS ==========
def get_main_menu(user_id: int, is_admin: bool = False) -> InlineKeyboardMarkup:
    """Главное меню"""
    buttons = [
        [InlineKeyboardButton(text="🤖 ИИ-сервисы", callback_data="ai_services")],
        [InlineKeyboardButton(text="📅 Запланировать пост", callback_data="schedule_post")],
        [InlineKeyboardButton(text="📊 Моя статистика", callback_data="my_stats")],
        [InlineKeyboardButton(text="📢 Мои каналы", callback_data="my_channels")],
        [InlineKeyboardButton(text="💎 Тарифы", callback_data="tariffs")],
        [InlineKeyboardButton(text="🔄 Продлить тариф", callback_data="extend_tariff")]
    ]
    
    if SUPPORT_BOT_USERNAME and SUPPORT_BOT_USERNAME != "support_bot":
        buttons.append([InlineKeyboardButton(text="🆘 Техподдержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME.replace('@', '')}")])
    else:
        buttons.append([InlineKeyboardButton(text="🆘 Поддержка", url=SUPPORT_URL)])
    
    if is_admin:
        buttons.append([InlineKeyboardButton(text="👑 Админ-панель", callback_data="admin_panel")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_tariffs_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Клавиатура с тарифами"""
    keyboard = []
    
    for tariff_id, tariff_info in TARIFFS.items():
        if tariff_id == "admin":
            continue
            
        price_text = "Бесплатно" if tariff_info['price'] == 0 else f"{tariff_info['price']} {tariff_info['currency']}"
        button_text = f"{tariff_info['name']} - {price_text}"
        keyboard.append([InlineKeyboardButton(text=button_text, callback_data=f"tariff_{tariff_id}")])
    
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_admin_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура админ-панели"""
    buttons = [
        [InlineKeyboardButton(text="📊 Статистика системы", callback_data="admin_stats")],
        [InlineKeyboardButton(text="👥 Список пользователей", callback_data="admin_users")],
        [InlineKeyboardButton(text="🛒 Заказы тарифов", callback_data="admin_orders")],
        [InlineKeyboardButton(text="💎 Выдать тариф", callback_data="admin_assign_tariff")],
        [InlineKeyboardButton(text="🔄 Продлить тариф", callback_data="admin_extend_tariff")],
        [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_orders_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для работы с заказами"""
    buttons = [
        [InlineKeyboardButton(text="⏳ Ожидающие", callback_data="orders_pending")],
        [InlineKeyboardButton(text="✅ Завершенные", callback_data="orders_completed")],
        [InlineKeyboardButton(text="❌ Отмененные", callback_data="orders_cancelled")],
        [InlineKeyboardButton(text="📋 Все заказы", callback_data="orders_all")],
        [InlineKeyboardButton(text="⬅️ В админ-панель", callback_data="admin_panel")]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_users_list_keyboard(users: List[Dict], page: int = 0, page_size: int = 10) -> InlineKeyboardMarkup:
    """Клавиатура списка пользователей"""
    buttons = []
    
    start_idx = page * page_size
    end_idx = min(start_idx + page_size, len(users))
    
    for user in users[start_idx:end_idx]:
        user_id = user['id']
        username = user.get('username', 'N/A')
        first_name = user.get('first_name', 'N/A')
        tariff = user.get('tariff', 'mini')
        
        button_text = f"{first_name} (@{username}) - {tariff}"
        buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"user_detail_{user_id}")])
    
    # Кнопки навигации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"users_page_{page-1}"))
    if end_idx < len(users):
        nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"users_page_{page+1}"))
    
    if nav_buttons:
        buttons.append(nav_buttons)
    
    buttons.append([InlineKeyboardButton(text="⬅️ В админ-панель", callback_data="admin_panel")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_tariff_selection_keyboard(action: str = "assign") -> InlineKeyboardMarkup:
    """Клавиатура выбора тарифа"""
    buttons = []
    
    for tariff_id, tariff_info in TARIFFS.items():
        if tariff_id == "admin":
            continue
            
        button_text = f"{tariff_info['name']} - {tariff_info['price']} {tariff_info['currency']}"
        buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"{action}_tariff_{tariff_id}")])
    
    buttons.append([InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_extend_period_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора периода продления"""
    buttons = [
        [InlineKeyboardButton(text="7 дней", callback_data="extend_7")],
        [InlineKeyboardButton(text="30 дней", callback_data="extend_30")],
        [InlineKeyboardButton(text="90 дней", callback_data="extend_90")],
        [InlineKeyboardButton(text="180 дней", callback_data="extend_180")],
        [InlineKeyboardButton(text="365 дней", callback_data="extend_365")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel")]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

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
    waiting_for_extend_user_id = State()
    waiting_for_extend_period = State()
    waiting_for_assign_tariff = State()

class TariffStates(StatesGroup):
    waiting_for_confirmation = State()

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
        # Устанавливаем бесплатный тариф на 30 дней для новых пользователей
        expires_at = datetime.now(MOSCOW_TZ) + timedelta(days=30)
        await execute_query('''
            INSERT INTO users (id, username, first_name, is_admin, tariff, tariff_expires_at, last_seen)
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
            ON CONFLICT (id) DO UPDATE 
            SET username = EXCLUDED.username, 
                first_name = EXCLUDED.first_name,
                is_admin = EXCLUDED.is_admin,
                last_seen = NOW()
        ''', user_id, username, first_name, is_admin, 'mini' if not is_admin else 'admin', expires_at)
    except Exception as e:
        logger.error(f"Ошибка регистрации пользователя {user_id}: {e}")
    
    current_tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(current_tariff, TARIFFS['mini'])
    
    # Получаем оставшееся время тарифа
    expires_at = await get_tariff_expires_date(user_id)
    days_left = 0
    if expires_at:
        days_left = max(0, (expires_at - datetime.now(MOSCOW_TZ)).days)
    
    welcome_text = (
        f"👋 Привет, {first_name}!\n\n"
        f"🤖 Я — бот KOLES-TECH для планирования постов и AI-контента.\n\n"
        f"💎 Ваш текущий тариф: {tariff_info['name']}\n"
        f"📅 Тариф действует: {days_left} дней\n\n"
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
        
        "💎 Тарифы (на 30 дней):\n"
        "• Mini - бесплатно (1 копирайт, 10 идей, 1 канал, 2 постов)\n"
        "• Standard ($5) - 3 копирайта, 30 идей, 2 канала, 6 постов\n"
        "• VIP ($10) - 7 копирайтов, 50 идей, 3 канал, 12 постов\n\n"
        
        f"🆘 Поддержка: {SUPPORT_URL}\n"
        f"💬 Вопросы по оплате: @{ADMIN_CONTACT.replace('@', '')}\n\n"
        "📍 Все тарифы действуют 30 дней с момента активации"
    )
    
    await message.answer(help_text)

@router.message(Command("admin"))
async def cmd_admin(message: Message):
    """Админ-панель"""
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await message.answer("❌ У вас нет доступа к админ-панели")
        return
    
    await message.answer(
        "👑 Админ-панель KOLES-TECH\n\n"
        "📊 Управление ботом и пользователями\n\n"
        "👇 Выберите действие:",
        reply_markup=get_admin_keyboard()
    )

# ========== TARIFF HANDLERS ==========
@router.callback_query(F.data == "tariffs")
async def show_tariffs(callback: CallbackQuery):
    """Показывает тарифы"""
    tariffs_text = (
        "💎 Тарифы KOLES-TECH\n\n"
        "📍 Все тарифы активируются на 30 дней\n\n"
        "🚀 Mini:\n"
        "• Цена: Бесплатно\n"
        "• AI-копирайтер: 1 текст/день\n"
        "• Идеи для постов: 10/день\n"
        "• Каналы: 1\n"
        "• Посты: 2/день\n"
        "• Идеально для знакомства\n\n"
        "⭐ Standard ($5):\n"
        "• AI-копирайтер: 3 текста/день\n"
        "• Идеи для постов: 30/день\n"
        "• Каналы: 2\n"
        "• Посты: 6/день\n"
        "• Для активных пользователей\n\n"
        "👑 VIP ($10):\n"
        "• AI-копирайтер: 7 текстов/день\n"
        "• Идеи для постов: 50/день\n"
        "• Каналы: 3\n"
        "• Посты: 12/день\n"
        "• Максимальные возможности\n\n"
        "💳 Оплата:\n"
        "• По вопросам оплаты пишите: @{admin}\n"
        "• После оплаты тариф активируется вручную админом\n"
        "• Тариф действует 30 дней с момента активации\n\n"
        "👇 Выберите тариф для заказа:"
    ).format(admin=ADMIN_CONTACT.replace('@', ''))
    
    await callback.message.edit_text(
        tariffs_text,
        reply_markup=get_tariffs_keyboard(callback.from_user.id)
    )

@router.callback_query(F.data.startswith("tariff_"))
async def select_tariff(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора тарифа"""
    tariff_id = callback.data.replace("tariff_", "")
    
    if tariff_id not in TARIFFS:
        await callback.answer("❌ Неизвестный тариф", show_alert=True)
        return
    
    tariff_info = TARIFFS[tariff_id]
    user_id = callback.from_user.id
    
    if tariff_id == "mini":
        # Бесплатный тариф - активируем сразу
        success = await update_user_tariff(user_id, "mini", 30)
        if success:
            await callback.message.edit_text(
                f"✅ Бесплатный тариф Mini активирован!\n\n"
                f"💎 Тариф: {tariff_info['name']}\n"
                f"📅 Действует: 30 дней\n"
                f"✨ Теперь вам доступны все функции тарифа\n\n"
                f"📍 Чтобы использовать больше возможностей, выберите платный тариф",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="💎 Посмотреть тарифы", callback_data="tariffs")],
                    [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
                ])
            )
        else:
            await callback.message.edit_text(
                "❌ Ошибка активации тарифа. Попробуйте позже.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад к тарифам", callback_data="tariffs")]
                ])
            )
        return
    
    # Платный тариф - создаем заказ
    await state.update_data(selected_tariff=tariff_id)
    await state.set_state(TariffStates.waiting_for_confirmation)
    
    confirmation_text = (
        f"🛒 Подтверждение заказа тарифа\n\n"
        f"💎 Тариф: {tariff_info['name']}\n"
        f"💰 Стоимость: {tariff_info['price']} {tariff_info['currency']}\n"
        f"📅 Срок действия: 30 дней\n\n"
        f"📋 Что включено:\n"
        f"• AI-копирайтер: {tariff_info['ai_copies_limit']} текстов/день\n"
        f"• Идеи для постов: {tariff_info['ai_ideas_limit']}/день\n"
        f"• Каналы: {tariff_info['channels_limit']}\n"
        f"• Посты: {tariff_info['daily_posts_limit']}/день\n\n"
        f"💳 Оплата:\n"
        f"1. Нажмите '✅ Подтвердить заказ'\n"
        f"2. Напишите админу для оплаты: @{ADMIN_CONTACT.replace('@', '')}\n"
        f"3. После оплаты админ активирует тариф\n"
        f"4. Тариф будет активен 30 дней с момента активации\n\n"
        f"📍 Вопросы? Пишите: @{ADMIN_CONTACT.replace('@', '')}"
    )
    
    await callback.message.edit_text(
        confirmation_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Подтвердить заказ", callback_data="confirm_order"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="tariffs")
            ]
        ])
    )

@router.callback_query(F.data == "confirm_order", TariffStates.waiting_for_confirmation)
async def confirm_tariff_order(callback: CallbackQuery, state: FSMContext):
    """Подтверждение заказа тарифа"""
    data = await state.get_data()
    tariff_id = data.get('selected_tariff')
    
    if not tariff_id:
        await callback.answer("❌ Ошибка: тариф не выбран", show_alert=True)
        return
    
    tariff_info = TARIFFS.get(tariff_id)
    if not tariff_info:
        await callback.answer("❌ Ошибка: тариф не найден", show_alert=True)
        return
    
    user_id = callback.from_user.id
    
    # Создаем заказ
    success = await create_tariff_order(user_id, tariff_id)
    
    if success:
        await callback.message.edit_text(
            f"✅ Заказ тарифа создан!\n\n"
            f"💎 Тариф: {tariff_info['name']}\n"
            f"💰 Стоимость: {tariff_info['price']} {tariff_info['currency']}\n"
            f"📅 Срок: 30 дней\n\n"
            f"📋 Дальнейшие действия:\n"
            f"1. Напишите админу для оплаты: @{ADMIN_CONTACT.replace('@', '')}\n"
            f"2. Укажите ваш ID: {user_id}\n"
            f"3. После оплаты админ активирует тариф\n"
            f"4. Вы получите уведомление о активации\n\n"
            f"📍 Вопросы? Пишите: @{ADMIN_CONTACT.replace('@', '')}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Посмотреть тарифы", callback_data="tariffs")],
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
            ])
        )
    else:
        await callback.message.edit_text(
            "❌ Ошибка создания заказа. Попробуйте позже.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад к тарифам", callback_data="tariffs")]
            ])
        )
    
    await state.clear()

@router.callback_query(F.data == "extend_tariff")
async def extend_tariff_handler(callback: CallbackQuery):
    """Продление тарифа"""
    user_id = callback.from_user.id
    
    # Получаем информацию о текущем тарифе
    current_tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(current_tariff, TARIFFS['mini'])
    expires_at = await get_tariff_expires_date(user_id)
    
    days_left = 0
    if expires_at:
        days_left = max(0, (expires_at - datetime.now(MOSCOW_TZ)).days)
    
    extend_text = (
        f"🔄 Продление тарифа\n\n"
        f"💎 Текущий тариф: {tariff_info['name']}\n"
        f"📅 Осталось дней: {days_left}\n\n"
        f"💰 Стоимость продления на 30 дней:\n"
        f"• Mini: Бесплатно\n"
        f"• Standard: $5\n"
        f"• VIP: $10\n\n"
        f"💳 Как продлить:\n"
        f"1. Напишите админу: @{ADMIN_CONTACT.replace('@', '')}\n"
        f"2. Укажите ваш ID: {user_id}\n"
        f"3. Укажите сколько дней хотите продлить\n"
        f"4. После оплаты админ продлит тариф\n\n"
        f"📍 При продлении дни добавляются к текущему сроку"
    )
    
    await callback.message.edit_text(
        extend_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💎 Посмотреть тарифы", callback_data="tariffs")],
            [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
        ])
    )

# ========== ADMIN HANDLERS ==========
@router.callback_query(F.data == "admin_panel")
async def admin_panel_handler(callback: CallbackQuery):
    """Обработчик админ-панели"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет доступа к админ-панели", show_alert=True)
        return
    
    await callback.message.edit_text(
        "👑 Админ-панель KOLES-TECH\n\n"
        "📊 Управление ботом и пользователями\n\n"
        "👇 Выберите действие:",
        reply_markup=get_admin_keyboard()
    )

@router.callback_query(F.data == "admin_stats")
async def admin_stats_handler(callback: CallbackQuery):
    """Статистика системы"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет доступа", show_alert=True)
        return
    
    # Получаем статистику
    all_users = await get_all_users()
    total_users = len(all_users)
    
    # Статистика по тарифам
    tariffs_count = {}
    for user in all_users:
        tariff = user.get('tariff', 'mini')
        tariffs_count[tariff] = tariffs_count.get(tariff, 0) + 1
    
    # AI статистика
    system_stats = ai_manager.get_system_stats()
    
    # Заказы
    pending_orders = await get_tariff_orders('pending')
    
    stats_text = (
        f"📊 Статистика системы\n\n"
        f"👥 Пользователи:\n"
        f"• Всего: {total_users}\n"
        f"• Mini: {tariffs_count.get('mini', 0)}\n"
        f"• Standard: {tariffs_count.get('standard', 0)}\n"
        f"• VIP: {tariffs_count.get('vip', 0)}\n"
        f"• Admin: {tariffs_count.get('admin', 0)}\n\n"
        f"🤖 AI сервисы:\n"
        f"• Всего запросов: {system_stats['total_requests']}\n"
        f"• Копирайтинг: {system_stats['total_copies']}\n"
        f"• Идеи: {system_stats['total_ideas']}\n"
        f"• Активных сессий: {system_stats['active_sessions']}\n\n"
        f"🔑 Ключи Gemini:\n"
        f"• Всего ключей: {system_stats['total_keys']}\n"
        f"• Доступных: {system_stats['available_keys']}\n\n"
        f"🛒 Заказы:\n"
        f"• Ожидающих: {len(pending_orders)}\n\n"
        f"🕐 Время сервера: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M:%S')}"
    )
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_stats")],
            [InlineKeyboardButton(text="⬅️ В админ-панель", callback_data="admin_panel")]
        ])
    )

@router.callback_query(F.data == "admin_users")
async def admin_users_handler(callback: CallbackQuery):
    """Список пользователей"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет доступа", show_alert=True)
        return
    
    all_users = await get_all_users()
    
    if not all_users:
        await callback.message.edit_text(
            "📭 Пользователей нет",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ В админ-панель", callback_data="admin_panel")]
            ])
        )
        return
    
    await callback.message.edit_text(
        f"👥 Список пользователей ({len(all_users)}):\n\n"
        f"👇 Выберите пользователя для просмотра деталей:",
        reply_markup=get_users_list_keyboard(all_users)
    )

@router.callback_query(F.data.startswith("users_page_"))
async def admin_users_page_handler(callback: CallbackQuery):
    """Навигация по страницам пользователей"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет доступа", show_alert=True)
        return
    
    page = int(callback.data.replace("users_page_", ""))
    all_users = await get_all_users()
    
    await callback.message.edit_text(
        f"👥 Список пользователей ({len(all_users)}):\n\n"
        f"Страница {page + 1}\n\n"
        f"👇 Выберите пользователя для просмотра деталей:",
        reply_markup=get_users_list_keyboard(all_users, page)
    )

@router.callback_query(F.data.startswith("user_detail_"))
async def user_detail_handler(callback: CallbackQuery):
    """Детали пользователя"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет доступа", show_alert=True)
        return
    
    target_user_id = int(callback.data.replace("user_detail_", ""))
    user = await get_user_by_id(target_user_id)
    
    if not user:
        await callback.answer("❌ Пользователь не найден", show_alert=True)
        return
    
    # Получаем статистику пользователя
    stats = await get_user_stats(target_user_id)
    expires_at = await get_tariff_expires_date(target_user_id)
    
    expires_str = "Не указано"
    if expires_at:
        expires_str = expires_at.strftime("%d.%m.%Y %H:%M")
        days_left = max(0, (expires_at - datetime.now(MOSCOW_TZ)).days)
        expires_str += f" ({days_left} дней)"
    
    user_text = (
        f"👤 Детали пользователя\n\n"
        f"🆔 ID: {user['id']}\n"
        f"👤 Имя: {user.get('first_name', 'N/A')}\n"
        f"📱 Username: @{user.get('username', 'N/A')}\n"
        f"💎 Тариф: {user.get('tariff', 'mini')}\n"
        f"📅 Тариф до: {expires_str}\n"
        f"👑 Админ: {'✅ Да' if user.get('is_admin') else '❌ Нет'}\n"
        f"📅 Регистрация: {user.get('created_at', 'N/A')}\n\n"
        f"📊 Статистика:\n"
        f"• Постов сегодня: {stats.get('posts_today', 0)}/{stats.get('posts_limit', 0)}\n"
        f"• Каналов: {stats.get('channels_count', 0)}/{stats.get('channels_limit', 0)}\n"
        f"• AI-копирайтинг: {stats.get('ai_copies_used', 0)}/{stats.get('ai_copies_limit', 0)}\n"
        f"• AI-идеи: {stats.get('ai_ideas_used', 0)}/{stats.get('ai_ideas_limit', 0)}\n"
        f"• Запланировано: {stats.get('scheduled_posts', 0)}"
    )
    
    await callback.message.edit_text(
        user_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="💎 Выдать тариф", callback_data=f"admin_assign_specific_{target_user_id}"),
                InlineKeyboardButton(text="🔄 Продлить", callback_data=f"admin_extend_specific_{target_user_id}")
            ],
            [InlineKeyboardButton(text="⬅️ Назад к списку", callback_data="admin_users")]
        ])
    )

@router.callback_query(F.data == "admin_orders")
async def admin_orders_handler(callback: CallbackQuery):
    """Заказы тарифов"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет доступа", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🛒 Управление заказами тарифов\n\n"
        "👇 Выберите тип заказов:",
        reply_markup=get_orders_keyboard()
    )

@router.callback_query(F.data.startswith("orders_"))
async def show_orders_handler(callback: CallbackQuery):
    """Показать заказы по статусу"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет доступа", show_alert=True)
        return
    
    status_filter = callback.data.replace("orders_", "")
    
    if status_filter == "all":
        orders = await get_tariff_orders()
        status_text = "все"
    else:
        orders = await get_tariff_orders(status_filter)
        status_text = status_filter
    
    if not orders:
        await callback.message.edit_text(
            f"📭 Заказов со статусом '{status_text}' нет",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад к заказам", callback_data="admin_orders")]
            ])
        )
        return
    
    orders_text = f"🛒 Заказы ({status_text}):\n\n"
    
    for i, order in enumerate(orders[:20], 1):  # Показываем первые 20 заказов
        user_info = await get_user_by_id(order['user_id'])
        username = user_info.get('username', 'N/A') if user_info else 'N/A'
        first_name = user_info.get('first_name', 'N/A') if user_info else 'N/A'
        
        tariff_info = TARIFFS.get(order['tariff'], {})
        tariff_name = tariff_info.get('name', order['tariff'])
        
        order_date = order['order_date']
        if order_date:
            order_date = order_date.strftime("%d.%m.%Y %H:%M")
        
        orders_text += (
            f"{i}. 👤 {first_name} (@{username})\n"
            f"   💎 Тариф: {tariff_name}\n"
            f"   📅 Дата: {order_date}\n"
            f"   📊 Статус: {order['status']}\n"
            f"   🆔 ID заказа: {order['id']}\n\n"
        )
    
    if len(orders) > 20:
        orders_text += f"📋 ... и еще {len(orders) - 20} заказов\n\n"
    
    orders_text += "ℹ️ Для обработки заказа используйте /admin"
    
    await callback.message.edit_text(
        orders_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад к заказам", callback_data="admin_orders")]
        ])
    )

@router.callback_query(F.data == "admin_assign_tariff")
async def admin_assign_tariff_handler(callback: CallbackQuery, state: FSMContext):
    """Выдача тарифа - запрос ID пользователя"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет доступа", show_alert=True)
        return
    
    await state.set_state(AdminStates.waiting_for_user_id)
    
    await callback.message.edit_text(
        "💎 Выдача тарифа пользователю\n\n"
        "Введите ID пользователя, которому нужно выдать тариф:\n\n"
        "📍 ID можно получить:\n"
        "• Из списка пользователей в админ-панели\n"
        "• Попросить пользователя отправить команду /id\n"
        "• Через детали заказа\n\n"
        "❌ Для отмены нажмите /cancel",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel")]
        ])
    )

@router.message(AdminStates.waiting_for_user_id)
async def process_user_id_for_assign(message: Message, state: FSMContext):
    """Обработка ID пользователя для выдачи тарифа"""
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        target_user_id = int(message.text.strip())
        
        # Проверяем существует ли пользователь
        user = await get_user_by_id(target_user_id)
        if not user:
            await message.answer(
                "❌ Пользователь с таким ID не найден.\n\n"
                "Проверьте ID и попробуйте еще раз:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel")]
                ])
            )
            return
        
        await state.update_data(target_user_id=target_user_id)
        await state.set_state(AdminStates.waiting_for_assign_tariff)
        
        current_tariff = user.get('tariff', 'mini')
        current_tariff_info = TARIFFS.get(current_tariff, {})
        
        await message.answer(
            f"👤 Пользователь найден:\n\n"
            f"🆔 ID: {target_user_id}\n"
            f"👤 Имя: {user.get('first_name', 'N/A')}\n"
            f"📱 Username: @{user.get('username', 'N/A')}\n"
            f"💎 Текущий тариф: {current_tariff_info.get('name', current_tariff)}\n\n"
            f"👇 Выберите тариф для выдачи:",
            reply_markup=get_tariff_selection_keyboard("assign")
        )
        
    except ValueError:
        await message.answer(
            "❌ Неверный формат ID! ID должен быть числом.\n\n"
            "Попробуйте еще раз:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel")]
            ])
        )

@router.callback_query(F.data.startswith("assign_tariff_"), AdminStates.waiting_for_assign_tariff)
async def process_tariff_selection_for_assign(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора тарифа для выдачи"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ У вас нет доступа", show_alert=True)
        return
    
    tariff_id = callback.data.replace("assign_tariff_", "")
    
    if tariff_id not in TARIFFS:
        await callback.answer("❌ Неизвестный тариф", show_alert=True)
        return
    
    data = await state.get_data()
    target_user_id = data.get('target_user_id')
    
    if not target_user_id:
        await callback.answer("❌ Ошибка: пользователь не выбран", show_alert=True)
        return
    
    tariff_info = TARIFFS[tariff_id]
    
    await state.update_data(selected_tariff=tariff_id)
    await state.set_state(AdminStates.waiting_for_confirm_assign)
    
    # Получаем информацию о пользователе
    user = await get_user_by_id(target_user_id)
    current_tariff = user.get('tariff', 'mini') if user else 'mini'
    current_tariff_info = TARIFFS.get(current_tariff, {})
    
    confirmation_text = (
        f"✅ Подтверждение выдачи тарифа\n\n"
        f"👤 Пользователь:\n"
        f"• ID: {target_user_id}\n"
        f"• Имя: {user.get('first_name', 'N/A') if user else 'N/A'}\n"
        f"• Username: @{user.get('username', 'N/A') if user else 'N/A'}\n\n"
        f"🔄 Текущий тариф: {current_tariff_info.get('name', current_tariff)}\n"
        f"🆕 Новый тариф: {tariff_info['name']}\n"
        f"💰 Цена: {tariff_info['price']} {tariff_info['currency']}\n"
        f"📅 Срок: 30 дней\n\n"
        f"📋 Что изменится:\n"
        f"• AI-копирайтер: {current_tariff_info.get('ai_copies_limit', 1)} → {tariff_info['ai_copies_limit']}/день\n"
        f"• Идеи: {current_tariff_info.get('ai_ideas_limit', 10)} → {tariff_info['ai_ideas_limit']}/день\n"
        f"• Каналы: {current_tariff_info.get('channels_limit', 1)} → {tariff_info['channels_limit']}\n"
        f"• Посты: {current_tariff_info.get('daily_posts_limit', 2)} → {tariff_info['daily_posts_limit']}/день\n\n"
        f"📍 Тариф будет действовать 30 дней с момента активации"
    )
    
    await callback.message.edit_text(
        confirmation_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_assign"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel")
            ]
        ])
    )

@router.callback_query(F.data == "confirm_assign", AdminStates.waiting_for_confirm_assign)
async def confirm_tariff_assign(callback: CallbackQuery, state: FSMContext):
    """Подтверждение выдачи тарифа"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ У вас нет доступа", show_alert=True)
        return
    
    data = await state.get_data()
    target_user_id = data.get('target_user_id')
    tariff_id = data.get('selected_tariff')
    
    if not target_user_id or not tariff_id:
        await callback.answer("❌ Ошибка: данные не найдены", show_alert=True)
        return
    
    # Выдаем тариф
    success, message = await force_update_user_tariff(target_user_id, tariff_id, callback.from_user.id, 30)
    
    if success:
        # Уведомляем пользователя
        try:
            await bot.send_message(
                target_user_id,
                f"🎉 Поздравляем! Вам выдан тариф!\n\n"
                f"💎 Тариф: {TARIFFS[tariff_id]['name']}\n"
                f"📅 Действует: 30 дней\n"
                f"✨ Теперь вам доступны все возможности тарифа\n\n"
                f"📍 Для просмотра статистики нажмите /start"
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {target_user_id}: {e}")
    
    await callback.message.edit_text(
        message,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ В админ-панель", callback_data="admin_panel")]
        ])
    )
    
    await state.clear()

@router.callback_query(F.data.startswith("admin_assign_specific_"))
async def admin_assign_specific_handler(callback: CallbackQuery, state: FSMContext):
    """Выдача тарифа конкретному пользователю"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет доступа", show_alert=True)
        return
    
    target_user_id = int(callback.data.replace("admin_assign_specific_", ""))
    
    await state.update_data(target_user_id=target_user_id)
    await state.set_state(AdminStates.waiting_for_assign_tariff)
    
    user = await get_user_by_id(target_user_id)
    current_tariff = user.get('tariff', 'mini') if user else 'mini'
    current_tariff_info = TARIFFS.get(current_tariff, {})
    
    await callback.message.edit_text(
        f"💎 Выдача тарифа пользователю\n\n"
        f"👤 Пользователь:\n"
        f"• ID: {target_user_id}\n"
        f"• Имя: {user.get('first_name', 'N/A') if user else 'N/A'}\n"
        f"• Username: @{user.get('username', 'N/A') if user else 'N/A'}\n"
        f"• Текущий тариф: {current_tariff_info.get('name', current_tariff)}\n\n"
        f"👇 Выберите тариф для выдачи:",
        reply_markup=get_tariff_selection_keyboard("assign")
    )

@router.callback_query(F.data == "admin_extend_tariff")
async def admin_extend_tariff_handler(callback: CallbackQuery, state: FSMContext):
    """Продление тарифа - запрос ID пользователя"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет доступа", show_alert=True)
        return
    
    await state.set_state(AdminStates.waiting_for_extend_user_id)
    
    await callback.message.edit_text(
        "🔄 Продление тарифа пользователю\n\n"
        "Введите ID пользователя, которому нужно продлить тариф:\n\n"
        "📍 ID можно получить:\n"
        "• Из списка пользователей в админ-панели\n"
        "• Попросить пользователя отправить команду /id\n"
        "• Через детали заказа\n\n"
        "❌ Для отмены нажмите /cancel",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel")]
        ])
    )

@router.message(AdminStates.waiting_for_extend_user_id)
async def process_user_id_for_extend(message: Message, state: FSMContext):
    """Обработка ID пользователя для продления тарифа"""
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        target_user_id = int(message.text.strip())
        
        # Проверяем существует ли пользователь
        user = await get_user_by_id(target_user_id)
        if not user:
            await message.answer(
                "❌ Пользователь с таким ID не найден.\n\n"
                "Проверьте ID и попробуйте еще раз:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel")]
                ])
            )
            return
        
        await state.update_data(target_user_id=target_user_id)
        await state.set_state(AdminStates.waiting_for_extend_period)
        
        current_tariff = user.get('tariff', 'mini')
        current_tariff_info = TARIFFS.get(current_tariff, {})
        expires_at = await get_tariff_expires_date(target_user_id)
        
        days_left = 0
        if expires_at:
            days_left = max(0, (expires_at - datetime.now(MOSCOW_TZ)).days)
        
        await message.answer(
            f"👤 Пользователь найден:\n\n"
            f"🆔 ID: {target_user_id}\n"
            f"👤 Имя: {user.get('first_name', 'N/A')}\n"
            f"📱 Username: @{user.get('username', 'N/A')}\n"
            f"💎 Текущий тариф: {current_tariff_info.get('name', current_tariff)}\n"
            f"📅 Осталось дней: {days_left}\n\n"
            f"👇 Выберите период продления:",
            reply_markup=get_extend_period_keyboard()
        )
        
    except ValueError:
        await message.answer(
            "❌ Неверный формат ID! ID должен быть числом.\n\n"
            "Попробуйте еще раз:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel")]
            ])
        )

@router.callback_query(F.data.startswith("extend_"), AdminStates.waiting_for_extend_period)
async def process_extend_period(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора периода продления"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ У вас нет доступа", show_alert=True)
        return
    
    period_str = callback.data.replace("extend_", "")
    try:
        period_days = int(period_str)
    except ValueError:
        await callback.answer("❌ Неверный период", show_alert=True)
        return
    
    data = await state.get_data()
    target_user_id = data.get('target_user_id')
    
    if not target_user_id:
        await callback.answer("❌ Ошибка: пользователь не выбран", show_alert=True)
        return
    
    # Продлеваем тариф
    success, message = await extend_user_tariff(target_user_id, period_days)
    
    if success:
        # Уведомляем пользователя
        try:
            user = await get_user_by_id(target_user_id)
            current_tariff = user.get('tariff', 'mini') if user else 'mini'
            tariff_info = TARIFFS.get(current_tariff, {})
            new_expires = await get_tariff_expires_date(target_user_id)
            
            expires_str = new_expires.strftime("%d.%m.%Y") if new_expires else "Не указано"
            
            await bot.send_message(
                target_user_id,
                f"✅ Ваш тариф продлен!\n\n"
                f"💎 Тариф: {tariff_info.get('name', current_tariff)}\n"
                f"📅 Продлено на: {period_days} дней\n"
                f"📆 Действует до: {expires_str}\n\n"
                f"📍 Для просмотра статистики нажмите /start"
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить пользователя {target_user_id}: {e}")
    
    await callback.message.edit_text(
        message,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ В админ-панель", callback_data="admin_panel")]
        ])
    )
    
    await state.clear()

@router.callback_query(F.data.startswith("admin_extend_specific_"))
async def admin_extend_specific_handler(callback: CallbackQuery, state: FSMContext):
    """Продление тарифа конкретному пользователю"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await callback.answer("❌ У вас нет доступа", show_alert=True)
        return
    
    target_user_id = int(callback.data.replace("admin_extend_specific_", ""))
    
    await state.update_data(target_user_id=target_user_id)
    await state.set_state(AdminStates.waiting_for_extend_period)
    
    user = await get_user_by_id(target_user_id)
    current_tariff = user.get('tariff', 'mini') if user else 'mini'
    current_tariff_info = TARIFFS.get(current_tariff, {})
    expires_at = await get_tariff_expires_date(target_user_id)
    
    days_left = 0
    if expires_at:
        days_left = max(0, (expires_at - datetime.now(MOSCOW_TZ)).days)
    
    await callback.message.edit_text(
        f"🔄 Продление тарифа пользователю\n\n"
        f"👤 Пользователь:\n"
        f"• ID: {target_user_id}\n"
        f"• Имя: {user.get('first_name', 'N/A') if user else 'N/A'}\n"
        f"• Username: @{user.get('username', 'N/A') if user else 'N/A'}\n"
        f"• Текущий тариф: {current_tariff_info.get('name', current_tariff)}\n"
        f"• Осталось дней: {days_left}\n\n"
        f"👇 Выберите период продления:",
        reply_markup=get_extend_period_keyboard()
    )

# ========== AI HANDLERS (остаются без изменений, только добавляем проверку срока действия тарифа) ==========
@router.callback_query(F.data == "ai_services")
async def ai_services_menu(callback: CallbackQuery):
    """Меню AI сервисов"""
    user_id = callback.from_user.id
    
    # Проверяем срок действия тарифа
    expires_at = await get_tariff_expires_date(user_id)
    if expires_at and expires_at < datetime.now(MOSCOW_TZ):
        await callback.message.edit_text(
            "❌ Ваш тариф истек!\n\n"
            "Для использования AI-сервисов необходимо продлить тариф.\n\n"
            "👇 Выберите действие:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Продлить тариф", callback_data="extend_tariff")],
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
            ])
        )
        return
    
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
    
    # Проверяем срок действия тарифа
    expires_at = await get_tariff_expires_date(user_id)
    if expires_at and expires_at < datetime.now(MOSCOW_TZ):
        await callback.message.edit_text(
            "❌ Ваш тариф истек!\n\n"
            "Для использования AI-сервисов необходимо продлить тариф.\n\n"
            "👇 Выберите действие:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Продлить тариф", callback_data="extend_tariff")],
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
            ])
        )
        return
    
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

@router.callback_query(F.data == "ai_ideas")
async def start_ideas_generator(callback: CallbackQuery, state: FSMContext):
    """Запуск генератора идей"""
    user_id = callback.from_user.id
    
    # Проверяем срок действия тарифа
    expires_at = await get_tariff_expires_date(user_id)
    if expires_at and expires_at < datetime.now(MOSCOW_TZ):
        await callback.message.edit_text(
            "❌ Ваш тариф истек!\n\n"
            "Для использования AI-сервисов необходимо продлить тариф.\n\n"
            "👇 Выберите действие:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Продлить тариф", callback_data="extend_tariff")],
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
            ])
        )
        return
    
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

# ========== STATISTICS HANDLERS ==========
@router.callback_query(F.data == "my_stats")
async def show_my_stats(callback: CallbackQuery):
    """Показывает статистику пользователя"""
    user_id = callback.from_user.id
    stats = await get_user_stats(user_id)
    
    system_stats = ai_manager.get_system_stats()
    available_keys = system_stats['available_keys']
    total_keys = system_stats['total_keys']
    
    # Форматируем дату окончания тарифа
    expires_str = "Не указано"
    if stats.get('tariff_expires_date'):
        expires_date = stats['tariff_expires_date']
        if isinstance(expires_date, str):
            expires_str = expires_date
        else:
            expires_str = expires_date.strftime("%d.%m.%Y %H:%M")
    
    stats_text = (
        f"📊 Ваша статистика:\n\n"
        f"💎 Тариф: {stats['tariff']}\n"
        f"📅 Тариф до: {expires_str}\n"
        f"⏳ Осталось дней: {stats.get('tariff_expires_days', 0)}\n\n"
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
    
    keyboard_buttons = []
    if stats.get('tariff_expires_days', 0) < 7:
        keyboard_buttons.append([InlineKeyboardButton(text="🔄 Продлить тариф", callback_data="extend_tariff")])
    
    keyboard_buttons.append([InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")])
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
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

async def check_expired_tariffs_task():
    """Проверка истекших тарифов"""
    try:
        expired_users = await execute_query('''
            SELECT id, username, first_name, tariff 
            FROM users 
            WHERE tariff_expires_at < NOW() 
            AND tariff NOT IN ('mini', 'admin')
        ''')
        
        for user in expired_users:
            user_id = user['id']
            username = user.get('username', 'N/A')
            first_name = user.get('first_name', 'N/A')
            old_tariff = user.get('tariff', 'standard')
            
            # Переводим на бесплатный тариф
            await update_user_tariff(user_id, 'mini', 30)
            
            # Уведомляем пользователя
            try:
                await bot.send_message(
                    user_id,
                    f"📢 Уведомление о тарифе\n\n"
                    f"Ваш тариф {old_tariff} истек.\n"
                    f"Вы переведены на бесплатный тариф Mini.\n\n"
                    f"📍 Для продолжения работы с прежними возможностями продлите тариф."
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить пользователя {user_id}: {e}")
        
        if expired_users:
            logger.info(f"✅ Проверка тарифов: {len(expired_users)} пользователей переведены на Mini")
    except Exception as e:
        logger.error(f"Ошибка проверки тарифов: {e}")

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
    logger.info(f"🚀 ЗАПУСК БОТА KOLES-TECH v4.0")
    logger.info(f"🤖 AI сервисы: ВКЛЮЧЕНЫ")
    logger.info(f"🔑 Gemini ключей: {len(GEMINI_API_KEYS)}")
    logger.info(f"👑 Admin ID: {ADMIN_ID}")
    logger.info(f"💎 Тарифы: Mini (бесплатно), Standard ($5), VIP ($10)")
    logger.info(f"📅 Срок тарифов: 30 дней")
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
        
        # Проверка истекших тарифов каждый день
        scheduler.add_job(
            check_expired_tariffs_task,
            trigger='cron',
            hour=1,
            minute=0,
            timezone=MOSCOW_TZ,
            id='check_expired_tariffs'
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
                    f"💎 Тарифы: Mini (бесплатно), Standard ($5), VIP ($10)\n"
                    f"📅 Срок тарифов: 30 дней\n"
                    f"🔄 Система ротации ключей: АКТИВНА\n"
                    f"🌐 Порт Railway: {PORT}\n"
                    f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M:%S')}"
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить админа: {e}")
        
        logger.info("=" * 60)
        logger.info("🎉 БОТ УСПЕШНО ЗАПУЩЕН С ТАРИФНОЙ СИСТЕМОЙ НА 30 ДНЕЙ!")
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
