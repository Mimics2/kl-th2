import os
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from enum import Enum
import json
import random

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
SUPPORT_LINK = os.getenv("SUPPORT_LINK", "https://railway.app")
TRIAL_CHANNEL_LINK = os.getenv("TRIAL_CHANNEL_LINK", "https://t.me/example_channel")
TRIAL_CHANNEL_USERNAME = os.getenv("TRIAL_CHANNEL_USERNAME", "@example_channel")

# ========== AI CONFIG ==========
# Загружаем API ключи из переменных окружения или конфига
GEMINI_API_KEYS = os.getenv("GEMINI_API_KEYS", "")
if GEMINI_API_KEYS:
    try:
        GEMINI_API_KEYS = json.loads(GEMINI_API_KEYS)
    except:
        GEMINI_API_KEYS = [
            "AIzaSyA2j48JnmiuQKf6uAfzHSg0vAW1gkN7ISc",
            "AIzaSyCsq2YBVbc0mxoaQcjnGnd3qasoVZaucQk",
            "AIzaSyCkvLqyIoX4M_dvyG4Tyy1ujpuK_ia-BtQ",
            "AIzaSyBB1KdR3pKOziItOEsCr5QHEGAf2ZED8lo",
            "AIzaSyCJoEWTJfBUhuIPZoIh62KrUqV8IEiPnOo"
        ]
else:
    GEMINI_API_KEYS = [
        "AIzaSyA2j48JnmiuQKf6uAfzHSg0vAW1gkN7ISc",
        "AIzaSyCsq2YBVbc0mxoaQcjnGnd3qasoVZaucQk",
        "AIzaSyCkvLqyIoX4M_dvyG4Tyy1ujpuK_ia-BtQ",
        "AIzaSyBB1KdR3pKOziItOEsCr5QHEGAf2ZED8lo",
        "AIzaSyCJoEWTJfBUhuIPZoIh62KrUqV8IEiPnOo"
    ]

# Модель из переменных окружения
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
# Альтернативные модели на случай ошибок
ALTERNATIVE_MODELS = [
    "gemini-2.5-flash",
    "gemini-1.5-flash",
    "gemini-1.5-pro"
]

# Настройки ротации - 3 попытки перед ошибкой
REQUESTS_PER_KEY = int(os.getenv("REQUESTS_PER_KEY", "3"))
REQUEST_COOLDOWN = int(os.getenv("REQUEST_COOLDOWN", "60"))
KEY_COOLDOWN = int(os.getenv("KEY_COOLDOWN", "300"))

MOSCOW_TZ = pytz.timezone('Europe/Moscow')
POST_CHARACTER_LIMIT = 4000

# ========== TARIFF SYSTEM ==========
class Tariff(Enum):
    MINI = "mini"
    STANDARD = "standard"
    VIP = "vip"
    ADMIN = "admin"
    STANDARD_TRIAL = "standard_trial"

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
        "trial": False
    },
    Tariff.STANDARD.value: {
        "name": "⭐ Standard",
        "price": 4,
        "currency": "USD",
        "channels_limit": 2,
        "daily_posts_limit": 6,
        "ai_copies_limit": 3,
        "ai_ideas_limit": 30,
        "description": "Для активных пользователей",
        "trial": False
    },
    Tariff.VIP.value: {
        "name": "👑 VIP",
        "price": 7,
        "currency": "USD",
        "channels_limit": 3,
        "daily_posts_limit": 12,
        "ai_copies_limit": 7,
        "ai_ideas_limit": 50,
        "description": "Максимальные возможности",
        "trial": False
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
        "trial": False
    },
    Tariff.STANDARD_TRIAL.value: {
        "name": "⭐ Standard (3 дня пробный)",
        "price": 0,
        "currency": "USD",
        "channels_limit": 2,
        "daily_posts_limit": 6,
        "ai_copies_limit": 3,
        "ai_ideas_limit": 30,
        "description": "Пробный период на 3 дня",
        "trial": True,
        "trial_days": 3
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

# ========== УЛУЧШЕННЫЙ AI SESSION MANAGER С ИНТЕЛЛЕКТУАЛЬНОЙ РОТАЦИЕЙ ==========
class AISessionManager:
    def __init__(self):
        self.sessions: Dict[int, Dict] = {}
        self.key_stats = {key: {
            "requests": 0, 
            "errors": 0, 
            "blocked_until": None, 
            "403_errors": 0,
            "last_used": None,
            "success_rate": 100,
            "avg_response_time": 0
        } for key in GEMINI_API_KEYS}
        self.last_request_time: Dict[int, datetime] = {}
        self.current_model_index = 0
        self.models = [GEMINI_MODEL] + ALTERNATIVE_MODELS
        self.user_retry_counts: Dict[int, Dict] = {}
        self.key_rotation_log = []
        
    def get_session(self, user_id: int) -> Dict:
        """Получает или создает сессию пользователя"""
        if user_id not in self.sessions:
            self.sessions[user_id] = {
                'history': [],
                'current_key_index': random.randint(0, len(GEMINI_API_KEYS)-1),
                'request_count': 0,
                'total_requests': 0,
                'copies_used': 0,
                'ideas_used': 0,
                'last_reset': datetime.now(MOSCOW_TZ).date(),
                'retry_count': 0,
                'last_successful_key': None,
                'word_count': 200,
                'current_request_retries': 0,
                'preferred_model': GEMINI_MODEL,
                'failed_keys': set(),
                'key_performance': {}
            }
        return self.sessions[user_id]
    
    def get_available_key(self, user_id: int) -> Tuple[Optional[str], int, str]:
        """Интеллектуальный выбор ключа с учетом статистики"""
        session = self.get_session(user_id)
        
        # 1. Попробовать последний успешный ключ
        if session['last_successful_key'] and session['last_successful_key'] in self.key_stats:
            key_info = self.key_stats[session['last_successful_key']]
            if self._is_key_available(key_info):
                return session['last_successful_key'], GEMINI_API_KEYS.index(session['last_successful_key']), session.get('preferred_model', self.get_current_model())
        
        # 2. Случайный выбор из доступных ключей (распределение нагрузки)
        available_keys = []
        current_time = datetime.now(MOSCOW_TZ)
        
        for key_index, key in enumerate(GEMINI_API_KEYS):
            key_info = self.key_stats[key]
            
            if not self._is_key_available(key_info):
                continue
            
            # Рассчитываем приоритет ключа
            priority = self._calculate_key_priority(key_info, session)
            available_keys.append({
                'key': key,
                'index': key_index,
                'priority': priority,
                'errors': key_info['errors'],
                'last_used': key_info['last_used']
            })
        
        if available_keys:
            # Выбираем ключ с наивысшим приоритетом
            available_keys.sort(key=lambda x: (-x['priority'], x['errors']))
            best_key = available_keys[0]
            
            session['current_key_index'] = best_key['index']
            session['request_count'] += 1
            self.key_stats[best_key['key']]['requests'] += 1
            self.key_stats[best_key['key']]['last_used'] = current_time
            
            return best_key['key'], best_key['index'], session.get('preferred_model', self.get_current_model())
        
        # 3. Если все ключи недоступны, сбросить блокировки и попробовать самый стабильный
        logger.warning(f"Все ключи недоступны для user_{user_id}, сбрасываю блокировки")
        self._reset_all_key_blocks()
        
        # Пробуем первый ключ с минимальными ошибками
        sorted_keys = sorted(self.key_stats.items(), key=lambda x: x[1]['errors'])
        key, key_info = sorted_keys[0]
        
        session['current_key_index'] = GEMINI_API_KEYS.index(key)
        session['request_count'] += 1
        key_info['requests'] += 1
        key_info['last_used'] = current_time
        
        return key, GEMINI_API_KEYS.index(key), session.get('preferred_model', self.get_current_model())
    
    def _is_key_available(self, key_info: Dict) -> bool:
        """Проверяет доступность ключа"""
        now = datetime.now(MOSCOW_TZ)
        
        if key_info['blocked_until'] and key_info['blocked_until'] > now:
            return False
        
        if key_info['403_errors'] >= REQUESTS_PER_KEY:
            return False
        
        # Проверка успешности ключа
        if key_info['requests'] > 10 and key_info['success_rate'] < 30:
            return False
            
        return True
    
    def _calculate_key_priority(self, key_info: Dict, session: Dict) -> float:
        """Рассчитывает приоритет ключа"""
        priority = 100
        
        # Наказываем за ошибки
        priority -= key_info['errors'] * 10
        
        # Наказываем за недавние 403 ошибки
        priority -= key_info['403_errors'] * 20
        
        # Поощряем ключи с высокой успешностью
        priority += key_info['success_rate'] / 2
        
        # Предпочитаем ключи, которые давно не использовались (балансировка нагрузки)
        if key_info['last_used']:
            time_since_last_use = (datetime.now(MOSCOW_TZ) - key_info['last_used']).total_seconds()
            priority += min(time_since_last_use / 300, 20)  # до 20 баллов за 5 минут простоя
        
        # Штрафуем за низкую скорость ответа
        if key_info['avg_response_time'] > 5:
            priority -= (key_info['avg_response_time'] - 5) * 2
        
        return max(priority, 1)
    
    def _reset_all_key_blocks(self):
        """Сбрасывает все блокировки ключей"""
        for key in self.key_stats:
            self.key_stats[key]['blocked_until'] = None
            self.key_stats[key]['403_errors'] = 0
    
    def mark_key_403_error(self, key: str, response_time: float = 0):
        """Отмечает ошибку 403 для ключа с учетом времени ответа"""
        if key in self.key_stats:
            self.key_stats[key]['403_errors'] += 1
            self.key_stats[key]['errors'] += 1
            self.key_stats[key]['avg_response_time'] = (
                self.key_stats[key]['avg_response_time'] * 0.8 + response_time * 0.2
            )
            
            # Обновляем успешность
            total_reqs = self.key_stats[key]['requests']
            if total_reqs > 0:
                self.key_stats[key]['success_rate'] = (
                    (total_reqs - self.key_stats[key]['errors']) / total_reqs * 100
                )
            
            logger.warning(f"Ключ {key[:15]}... получил 403. Всего: {self.key_stats[key]['403_errors']}/{REQUESTS_PER_KEY}")
            
            if self.key_stats[key]['403_errors'] >= REQUESTS_PER_KEY:
                block_time = KEY_COOLDOWN * (self.key_stats[key]['403_errors'] - REQUESTS_PER_KEY + 1)
                self.key_stats[key]['blocked_until'] = datetime.now(MOSCOW_TZ) + timedelta(seconds=block_time)
                logger.warning(f"Ключ {key[:15]}... заблокирован на {block_time} секунд")
                
                # Логируем ротацию
                self.key_rotation_log.append({
                    'timestamp': datetime.now(MOSCOW_TZ),
                    'key': key[:15] + "...",
                    'reason': '403_error',
                    'block_time': block_time
                })
    
    def mark_key_successful(self, key: str, user_id: int, response_time: float):
        """Отмечает ключ как успешный"""
        if key in self.key_stats:
            session = self.get_session(user_id)
            session['last_successful_key'] = key
            session['retry_count'] = 0
            session['current_request_retries'] = 0
            
            # Обновляем статистику ключа
            self.key_stats[key]['avg_response_time'] = (
                self.key_stats[key]['avg_response_time'] * 0.9 + response_time * 0.1
            )
            
            # Сбрасываем счетчики ошибок при успехе
            if response_time < 3:  # Быстрый успешный ответ
                self.key_stats[key]['403_errors'] = max(0, self.key_stats[key]['403_errors'] - 1)
                self.key_stats[key]['errors'] = max(0, self.key_stats[key]['errors'] - 0.5)
            
            # Обновляем успешность
            total_reqs = self.key_stats[key]['requests']
            if total_reqs > 0:
                self.key_stats[key]['success_rate'] = (
                    (total_reqs - self.key_stats[key]['errors']) / total_reqs * 100
                )
    
    def rotate_to_next_key(self, current_key: str) -> Optional[str]:
        """Ротация на следующий ключ"""
        if not GEMINI_API_KEYS:
            return None
        
        try:
            current_index = GEMINI_API_KEYS.index(current_key)
            next_index = (current_index + 1) % len(GEMINI_API_KEYS)
            return GEMINI_API_KEYS[next_index]
        except ValueError:
            return GEMINI_API_KEYS[0] if GEMINI_API_KEYS else None
    
    def increment_user_retry(self, user_id: int):
        """Увеличивает счетчик попыток для пользователя"""
        session = self.get_session(user_id)
        session['current_request_retries'] += 1
        return session['current_request_retries']
    
    def get_user_retry_count(self, user_id: int) -> int:
        """Получает количество попыток для пользователя"""
        return self.get_session(user_id)['current_request_retries']
    
    def get_current_model(self) -> str:
        """Возвращает текущую модель"""
        return self.models[self.current_model_index % len(self.models)]
    
    def rotate_model(self):
        """Переключает на следующую модель"""
        self.current_model_index += 1
        new_model = self.get_current_model()
        logger.info(f"🔄 Ротация модели на: {new_model}")
        
        # Логируем ротацию модели
        self.key_rotation_log.append({
            'timestamp': datetime.now(MOSCOW_TZ),
            'model': new_model,
            'reason': 'model_rotation'
        })
    
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
                session['retry_count'] = 0
                session['current_request_retries'] = 0
    
    def set_word_count(self, user_id: int, word_count: int):
        """Устанавливает количество слов для генерации"""
        session = self.get_session(user_id)
        session['word_count'] = max(50, min(1000, word_count))
    
    def get_word_count(self, user_id: int) -> int:
        """Получает количество слов для генерации"""
        return self.get_session(user_id)['word_count']
    
    def get_rotation_stats(self) -> Dict:
        """Возвращает статистику ротации"""
        total_keys = len(GEMINI_API_KEYS)
        active_keys = sum(1 for key_info in self.key_stats.values() 
                         if self._is_key_available(key_info))
        blocked_keys = total_keys - active_keys
        
        return {
            'total_keys': total_keys,
            'active_keys': active_keys,
            'blocked_keys': blocked_keys,
            'total_requests': sum(key_info['requests'] for key_info in self.key_stats.values()),
            'total_errors': sum(key_info['errors'] for key_info in self.key_stats.values()),
            'rotation_log': self.key_rotation_log[-10:]  # Последние 10 записей
        }

ai_manager = AISessionManager()

# ========== ОБНОВЛЕННЫЙ COPYWRITER_PROMPT ==========
COPYWRITER_PROMPT = """Ты профессиональный копирайтер для Telegram-каналов. Создай продающий текст на основе следующих данных:

🎯 ТЕМА: {topic}
🎨 СТИЛЬ: {style}
📚 ПРИМЕРЫ РАБОТ: {examples}
📝 КОЛИЧЕСТВО СЛОВ: {word_count} слов

📋 ТРЕБОВАНИЯ:
1. Текст должен быть цепляющим и вовлекающим
2. Используй эмодзи уместно (но не переборщи)
3. Структура: заголовок → проблема → решение → призыв к действию
4. ТОЧНО {word_count} слов (±10%)
5. Пиши как для живых людей, без воды
6. Учитывай примеры, но не копируй их

✨ ДОПОЛНИТЕЛЬНО:
- Текущая дата: {current_date}
- Не упоминай что ты ИИ
- Пиши в настоящем времени
- Убедись что текст содержит примерно {word_count} слов

🚀 Верни ТОЛЬКО готовый текст, без пояснений."""

IDEAS_PROMPT = """Ты эксперт по контенту для Telegram. Сгенерируй {count} идей для постов на тему:

🎯 ТЕМА: {topic}

📋 ТРЕБОВАНИЯ К ИДЕЯМ:
1. Каждая идея должна быть конкретной и реализуемой
2. Формат: краткое описание (1-2 предложения)
3. Укажи возможный тип контента (текст, фото, видео, опрос)
4. Идеи должны быть разнообразными

📝 ПРИМЕР ФОРМАТА:
1. [Тип] Название идеи - Краткое описание
2. [Тип] Название идеи - Краткое описание

✨ ДОПОЛНИТЕЛЬНО:
- Учитывай тренды {current_date}
- Идеи должны вовлекать аудиторию
- Не повторяйся

🚀 Верни список идей с нумерацией, каждый с новой строки."""

# ========== УЛУЧШЕННАЯ ФУНКЦИЯ ГЕНЕРАЦИИ С РОТАЦИЕЙ ==========
async def generate_with_gemini(prompt: str, user_id: int, max_retries: int = 3) -> Optional[str]:
    """Генерирует текст через Gemini API с интеллектуальной ротацией"""
    
    start_time = datetime.now(MOSCOW_TZ)
    session = ai_manager.get_session(user_id)
    
    for retry in range(max_retries):
        try:
            # Получаем доступный ключ
            key, key_index, model_name = ai_manager.get_available_key(user_id)
            
            if not key:
                logger.error(f"❌ Нет доступных ключей для user_{user_id}")
                return None
            
            # Настраиваем API
            genai.configure(api_key=key)
            model = genai.GenerativeModel(model_name)
            
            # Отправляем запрос с таймаутом
            response = await asyncio.wait_for(
                model.generate_content(
                    prompt,
                    generation_config={
                        "temperature": 0.8,
                        "top_p": 0.95,
                        "top_k": 40,
                        "max_output_tokens": 4000,
                    }
                ),
                timeout=30
            )
            
            # Рассчитываем время ответа
            response_time = (datetime.now(MOSCOW_TZ) - start_time).total_seconds()
            
            # Отмечаем успех
            ai_manager.mark_key_successful(key, user_id, response_time)
            
            # Логируем успешный запрос
            logger.info(f"✅ AI запрос | user_{user_id} | key_{key_index} | модель: {model_name} | "
                       f"попытка: {retry+1}/{max_retries} | время: {response_time:.2f}с")
            
            return response.text.strip()
            
        except asyncio.TimeoutError:
            logger.warning(f"⏱ Таймаут при попытке {retry+1}/{max_retries} для user_{user_id}")
            if key:
                ai_manager.mark_key_403_error(key, 30)  # Таймаут считается как медленный ответ
            
        except Exception as e:
            error_msg = str(e).lower()
            error_code = str(e)
            response_time = (datetime.now(MOSCOW_TZ) - start_time).total_seconds()
            
            # Логируем ошибку
            logger.error(f"❌ Ошибка при попытке {retry+1}/{max_retries} для user_{user_id}: {error_msg[:100]}")
            
            # Определяем тип ошибки и принимаем меры
            if "429" in error_code or "quota" in error_msg or "resource exhausted" in error_msg:
                logger.warning(f"🔄 Лимит ключа, ротирую...")
                if key:
                    ai_manager.mark_key_403_error(key, response_time)
                    # Пробуем следующий ключ
                    next_key = ai_manager.rotate_to_next_key(key)
                    if next_key and retry + 1 < max_retries:
                        logger.info(f"🔄 Переключаюсь на ключ {next_key[:15]}...")
                
            elif "403" in error_code or "permission denied" in error_msg:
                logger.warning(f"🔒 Ошибка доступа 403")
                if key:
                    ai_manager.mark_key_403_error(key, response_time)
                
            elif "503" in error_code or "unavailable" in error_msg:
                logger.warning(f"🌐 Сервис недоступен, ротирую модель...")
                ai_manager.rotate_model()
                
            elif "500" in error_code or "internal" in error_msg:
                logger.warning(f"⚡ Внутренняя ошибка сервера")
                if key and retry + 1 < max_retries:
                    await asyncio.sleep(2 ** retry)  # Экспоненциальная задержка
                
            else:
                logger.error(f"⚠️ Неизвестная ошибка: {error_msg[:100]}")
            
            # Увеличиваем счетчик попыток
            ai_manager.increment_user_retry(user_id)
            
            # Если это не последняя попытка - ждем и пробуем снова
            if retry + 1 < max_retries:
                wait_time = (retry + 1) * 2  # Экспоненциальная задержка
                logger.info(f"⏳ Жду {wait_time} секунд перед следующей попыткой...")
                await asyncio.sleep(wait_time)
    
    # Все попытки исчерпаны
    logger.error(f"💥 Все {max_retries} попыток исчерпаны для user_{user_id}")
    return None

# ========== ДОБАВЛЕНИЕ НОВЫХ ФУНКЦИЙ ДЛЯ ПРОБНОГО ПЕРИОДА ==========
async def check_channel_subscription(user_id: int, channel_username: str = None) -> bool:
    """Проверяет подписку пользователя на канал"""
    if not channel_username:
        channel_username = TRIAL_CHANNEL_USERNAME
    
    try:
        # Убираем @ если есть
        if channel_username.startswith('@'):
            channel_username = channel_username[1:]
        
        # Получаем chat_id канала
        chat_id = f"@{channel_username}"
        
        # Проверяем статус участника
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        
        # Статусы, которые считаются подпиской
        valid_statuses = ['member', 'administrator', 'creator']
        
        return member.status in valid_statuses
        
    except Exception as e:
        logger.error(f"Ошибка проверки подписки для user_{user_id}: {e}")
        return False

async def activate_trial_period(user_id: int, tariff_id: str = "standard_trial") -> Tuple[bool, str]:
    """Активирует пробный период на 3 дня"""
    try:
        conn = await get_db_connection()
        
        # Проверяем, не использовал ли уже пользователь пробный период
        user = await conn.fetchrow(
            "SELECT trial_used, trial_end_date FROM users WHERE id = $1", 
            user_id
        )
        
        if user and user['trial_used']:
            if user['trial_end_date'] and user['trial_end_date'] > datetime.now(MOSCOW_TZ):
                return False, "❌ Вы уже используете пробный период!"
            else:
                return False, "❌ Вы уже использовали пробный период ранее!"
        
        # Проверяем подписку на канал
        is_subscribed = await check_channel_subscription(user_id)
        if not is_subscribed:
            return False, f"📢 Для активации пробного периода необходимо подписаться на канал: {TRIAL_CHANNEL_LINK}"
        
        # Активируем пробный период
        trial_end_date = datetime.now(MOSCOW_TZ) + timedelta(days=3)
        
        await conn.execute('''
            UPDATE users 
            SET tariff = $1, 
                trial_used = TRUE,
                trial_end_date = $2,
                trial_start_date = $3
            WHERE id = $4
        ''', tariff_id, trial_end_date, datetime.now(MOSCOW_TZ), user_id)
        
        await conn.close()
        
        # Отправляем уведомление админу
        if ADMIN_ID:
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"🎉 АКТИВИРОВАН ПРОБНЫЙ ПЕРИОД!\n\n"
                    f"👤 Пользователь: {user_id}\n"
                    f"💎 Тариф: Standard (3 дня пробный)\n"
                    f"📅 Действует до: {trial_end_date.strftime('%d.%m.%Y %H:%M')}\n"
                    f"🕐 Время активации: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}"
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить админа: {e}")
        
        return True, (
            f"🎉 Пробный период активирован!\n\n"
            f"⭐ Теперь у вас тариф Standard на 3 дня!\n\n"
            f"📊 Ваши новые возможности:\n"
            f"• 2 канала вместо 1\n"
            f"• 6 постов в день вместо 2\n"
            f"• 3 AI-копирайтинга вместо 1\n"
            f"• 30 идей в день вместо 10\n\n"
            f"📅 Действует до: {trial_end_date.strftime('%d.%m.%Y %H:%M')}\n\n"
            f"💡 После окончания пробного периода:\n"
            f"• Тариф вернется к Mini\n"
            f"• Вы сможете оплатить полную версию"
        )
        
    except Exception as e:
        logger.error(f"Ошибка активации пробного периода: {e}")
        return False, f"❌ Ошибка при активации пробного периода: {str(e)}"

async def check_trial_expiry():
    """Проверяет истечение пробных периодов"""
    try:
        conn = await get_db_connection()
        
        # Находим пользователей с истекшим пробным периодом
        users = await conn.fetch('''
            SELECT id, username, trial_end_date 
            FROM users 
            WHERE trial_used = TRUE 
            AND trial_end_date < $1
            AND tariff = 'standard_trial'
        ''', datetime.now(MOSCOW_TZ))
        
        for user in users:
            # Возвращаем на тариф mini
            await conn.execute('''
                UPDATE users 
                SET tariff = 'mini'
                WHERE id = $1
            ''', user['id'])
            
            # Отправляем уведомление пользователю
            try:
                await bot.send_message(
                    user['id'],
                    f"📢 Ваш пробный период тарифа Standard завершен!\n\n"
                    f"⭐ Спасибо, что попробовали все возможности бота!\n\n"
                    f"🔙 Ваш тариф возвращен к Mini.\n\n"
                    f"💎 Чтобы продолжить использовать расширенные возможности:\n"
                    f"1. Перейдите в раздел 'Тарифы'\n"
                    f"2. Выберите Standard или VIP\n"
                    f"3. Оплатите через менеджера\n\n"
                    f"📊 Что изменилось:\n"
                    f"• Каналов: 2 → 1\n"
                    f"• Постов в день: 6 → 2\n"
                    f"• AI-копирайтингов: 3 → 1\n"
                    f"• Идей в день: 30 → 10"
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить пользователя {user['id']}: {e}")
        
        await conn.close()
        logger.info(f"✅ Проверено истечение пробных периодов: {len(users)} пользователей")
        
    except Exception as e:
        logger.error(f"Ошибка проверки пробных периодов: {e}")

# ========== ОБНОВЛЕННЫЕ DATABASE FUNCTIONS ==========
async def init_db():
    try:
        conn = await get_db_connection()
        
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
                created_at TIMESTAMP DEFAULT NOW(),
                trial_used BOOLEAN DEFAULT FALSE,
                trial_end_date TIMESTAMP,
                trial_start_date TIMESTAMP
            )
        ''')
        
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
        
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS ai_rotation_logs (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT,
                key_index INTEGER,
                model TEXT,
                success BOOLEAN,
                error_type TEXT,
                response_time FLOAT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        await conn.close()
        logger.info("✅ Таблицы БД созданы/проверены")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        raise

async def migrate_db():
    try:
        conn = await get_db_connection()
        
        migrations = [
            ('users', 'trial_used', 'BOOLEAN DEFAULT FALSE'),
            ('users', 'trial_end_date', 'TIMESTAMP'),
            ('users', 'trial_start_date', 'TIMESTAMP'),
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
    """Получает текущий тариф пользователя с учетом пробного периода"""
    try:
        conn = await get_db_connection()
        user = await conn.fetchrow(
            "SELECT tariff, is_admin, trial_used, trial_end_date FROM users WHERE id = $1", 
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
        
        # Проверяем пробный период
        if user.get('tariff') == 'standard_trial' and user.get('trial_end_date'):
            if user['trial_end_date'] > datetime.now(MOSCOW_TZ):
                return 'standard_trial'
            else:
                # Пробный период истек, возвращаем на mini
                await update_user_tariff(user_id, 'mini')
                return 'mini'
        
        return user.get('tariff', 'mini')
    except Exception as e:
        logger.error(f"Ошибка получения тарифа: {e}")
        return 'mini'

# ========== ОБНОВЛЕННЫЕ KEYBOARDS ==========
def get_main_menu(user_id: int, is_admin: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="🤖 ИИ-сервисы", callback_data="ai_services")],
        [InlineKeyboardButton(text="📅 Запланировать пост", callback_data="schedule_post")],
        [InlineKeyboardButton(text="📊 Моя статистика", callback_data="my_stats")],
        [InlineKeyboardButton(text="📢 Мои каналы", callback_data="my_channels")],
        [InlineKeyboardButton(text="💎 Тарифы", callback_data="tariffs")],
        [InlineKeyboardButton(text="🎁 Пробный период", callback_data="trial_period")],
        [InlineKeyboardButton(text="🆘 Техподдержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME}")],
    ]
    
    if is_admin:
        buttons.append([InlineKeyboardButton(text="👑 Админ-панель", callback_data="admin_panel")])
    
    # Добавляем кнопку с поддержкой если что-то не работает
    buttons.append([
        InlineKeyboardButton(
            text="⚠️ Если не работает - пишите", 
            url=SUPPORT_LINK
        )
    ])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_tariffs_keyboard(user_tariff: str = 'mini') -> InlineKeyboardMarkup:
    buttons = []
    
    for tariff_id, tariff_info in TARIFFS.items():
        if tariff_id == 'admin' or tariff_id == 'standard_trial':
            continue
            
        name = tariff_info['name']
        price = tariff_info['price']
        
        if tariff_id == user_tariff or (user_tariff == 'standard_trial' and tariff_id == 'standard'):
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
    
    # Кнопка пробного периода
    buttons.append([InlineKeyboardButton(
        text="🎁 3 дня Standard БЕСПЛАТНО",
        callback_data="trial_info"
    )])
    
    buttons.append([InlineKeyboardButton(text="⏰ Проверить время", callback_data="check_time")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_trial_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Подписаться на канал", url=TRIAL_CHANNEL_LINK)],
        [InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_subscription")],
        [InlineKeyboardButton(text="🎁 Активировать пробный период", callback_data="activate_trial")],
        [InlineKeyboardButton(text="⬅️ Назад к тарифам", callback_data="tariffs")]
    ])

def get_tariff_order_keyboard(tariff_id: str, has_trial: bool = False) -> InlineKeyboardMarkup:
    tariff_info = TARIFFS.get(tariff_id)
    
    if not tariff_info:
        return get_tariffs_keyboard()
    
    if tariff_id == 'standard' and not has_trial:
        buttons = [
            [InlineKeyboardButton(text="🎁 Получить 3 дня БЕСПЛАТНО", callback_data="trial_info")],
            [InlineKeyboardButton(text="💳 Заказать тариф", callback_data=f"order_{tariff_id}")],
            [InlineKeyboardButton(text="💬 Связаться с менеджером", url=f"https://t.me/{ADMIN_CONTACT.replace('@', '')}")],
            [InlineKeyboardButton(text="⬅️ Назад к тарифам", callback_data="tariffs")]
        ]
    elif tariff_info['price'] == 0:
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

# ========== ОБНОВЛЕННЫЕ STATES ==========
class TrialStates(StatesGroup):
    waiting_for_subscription_check = State()

# ========== ОБНОВЛЕННЫЕ BASIC HANDLERS ==========
@router.message(CommandStart())
async def cmd_start(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or ""
    first_name = message.from_user.first_name or "Пользователь"
    is_admin = user_id == ADMIN_ID
    
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
        f"🤖 Я — бот KOLES-TECH для планирования постов и AI-контента.\n\n"
        f"💎 Ваш текущий тариф: {tariff_info['name']}\n\n"
        f"✨ Возможности:\n"
        f"• 🤖 AI-копирайтер и генератор идей\n"
        f"• 📅 Запланировать пост с любым контентом\n"
        f"• 📊 Детальная статистика\n"
        f"• 📢 Управление каналами\n"
        f"• ⏰ Автопубликация в нужное время\n"
        f"• 🎁 3 дня Standard БЕСПЛАТНО\n\n"
        f"📍 Время указывается по Москве\n\n"
        f"⚠️ Если что-то не работает - пишите: {SUPPORT_LINK}\n\n"
        f"👇 Выберите действие:"
    )
    
    await message.answer(welcome_text, reply_markup=get_main_menu(user_id, is_admin))

# ========== НОВЫЕ HANDLERS ДЛЯ ПРОБНОГО ПЕРИОДА ==========
@router.callback_query(F.data == "trial_period")
async def trial_period_info(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    # Проверяем текущий статус
    conn = await get_db_connection()
    user = await conn.fetchrow(
        "SELECT trial_used, trial_end_date, tariff FROM users WHERE id = $1", 
        user_id
    )
    await conn.close()
    
    if user and user['trial_used']:
        if user['trial_end_date'] and user['trial_end_date'] > datetime.now(MOSCOW_TZ):
            # Активный пробный период
            time_left = user['trial_end_date'] - datetime.now(MOSCOW_TZ)
            days = time_left.days
            hours = time_left.seconds // 3600
            
            await callback.message.edit_text(
                f"🎁 У вас активен пробный период!\n\n"
                f"⭐ Тариф: Standard (пробный)\n"
                f"⏳ Осталось: {days} дней {hours} часов\n"
                f"📅 Заканчивается: {user['trial_end_date'].strftime('%d.%m.%Y %H:%M')}\n\n"
                f"📊 Ваши возможности:\n"
                f"• 2 канала\n"
                f"• 6 постов в день\n"
                f"• 3 AI-копирайтинга\n"
                f"• 30 идей в день\n\n"
                f"💡 После окончания пробного периода:\n"
                f"• Тариф вернется к Mini\n"
                f"• Вы сможете оплатить полную версию",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="💎 Посмотреть тарифы", callback_data="tariffs")],
                    [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
                ])
            )
            return
        else:
            # Пробный период уже использован
            await callback.message.edit_text(
                f"❌ Вы уже использовали пробный период.\n\n"
                f"⭐ Вы можете оформить полную подписку на тариф Standard:\n\n"
                f"📊 Возможности Standard:\n"
                f"• 2 канала\n"
                f"• 6 постов в день\n"
                f"• 3 AI-копирайтинга\n"
                f"• 30 идей в день\n"
                f"• Полная поддержка\n\n"
                f"💵 Стоимость: 4 USD/месяц",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="💎 Оформить Standard", callback_data="tariff_info_standard")],
                    [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
                ])
            )
            return
    
    # Предложение пробного периода
    trial_text = (
        f"🎁 ПОЛУЧИТЕ 3 ДНЯ STANDARD БЕСПЛАТНО!\n\n"
        f"⭐ Что входит в пробный период:\n"
        f"• Полный доступ к тарифу Standard\n"
        f"• 2 канала для публикаций\n"
        f"• 6 постов в день\n"
        f"• 3 AI-копирайтинга в день\n"
        f"• 30 идей в день\n"
        f"• Все функции бота\n\n"
        f"📋 Условия получения:\n"
        f"1. Подпишитесь на наш канал: {TRIAL_CHANNEL_LINK}\n"
        f"2. Нажмите 'Проверить подписку'\n"
        f"3. Активируйте пробный период\n\n"
        f"⏳ Срок действия: 3 дня с момента активации\n\n"
        f"💡 После пробного периода:\n"
        f"• Тариф автоматически вернется к Mini\n"
        f"• Вы сможете оформить полную подписку"
    )
    
    await callback.message.edit_text(
        trial_text,
        reply_markup=get_trial_keyboard()
    )

@router.callback_query(F.data == "trial_info")
async def trial_info_detailed(callback: CallbackQuery):
    await trial_period_info(callback)

@router.callback_query(F.data == "check_subscription")
async def check_subscription_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    await callback.answer("⏳ Проверяю подписку...", show_alert=False)
    
    is_subscribed = await check_channel_subscription(user_id)
    
    if is_subscribed:
        await callback.answer("✅ Вы подписаны на канал!", show_alert=True)
        
        # Предлагаем активировать пробный период
        await callback.message.edit_text(
            f"✅ Отлично! Вы подписаны на канал!\n\n"
            f"🎁 Теперь вы можете активировать пробный период на 3 дня.\n\n"
            f"⭐ Что вы получите:\n"
            f"• Полный доступ к тарифу Standard\n"
            f"• Все расширенные функции\n"
            f"• 3 дня бесплатного использования\n\n"
            f"👇 Нажмите кнопку ниже для активации:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🎁 Активировать пробный период", callback_data="activate_trial")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="trial_period")]
            ])
        )
    else:
        await callback.answer("❌ Вы не подписаны на канал", show_alert=True)
        
        await callback.message.edit_text(
            f"❌ Вы не подписаны на наш канал.\n\n"
            f"📢 Для получения пробного периода необходимо подписаться:\n"
            f"{TRIAL_CHANNEL_LINK}\n\n"
            f"📋 Инструкция:\n"
            f"1. Перейдите по ссылке выше\n"
            f"2. Нажмите 'Присоединиться' в канале\n"
            f"3. Вернитесь сюда и нажмите 'Проверить подписку'\n\n"
            f"📍 После подписки вы получите 3 дня Standard БЕСПЛАТНО!",
            reply_markup=get_trial_keyboard()
        )

@router.callback_query(F.data == "activate_trial")
async def activate_trial_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    await callback.answer("⏳ Активирую пробный период...", show_alert=False)
    
    # Проверяем подписку еще раз
    is_subscribed = await check_channel_subscription(user_id)
    if not is_subscribed:
        await callback.answer("❌ Сначала подпишитесь на канал", show_alert=True)
        await callback.message.edit_text(
            f"❌ Вы не подписаны на канал!\n\n"
            f"📢 Подпишитесь по ссылке: {TRIAL_CHANNEL_LINK}\n"
            f"📍 Затем нажмите 'Проверить подписку'",
            reply_markup=get_trial_keyboard()
        )
        return
    
    # Активируем пробный период
    success, message = await activate_trial_period(user_id)
    
    if success:
        await callback.message.edit_text(
            message,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🚀 Начать использовать", callback_data="ai_services")],
                [InlineKeyboardButton(text="📊 Моя статистика", callback_data="my_stats")],
                [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
            ])
        )
    else:
        await callback.message.edit_text(
            message,
            reply_markup=get_trial_keyboard()
        )

# ========== ОБНОВЛЕННЫЙ TARIFF INFO HANDLER ==========
@router.callback_query(F.data.startswith("tariff_info_"))
async def tariff_info(callback: CallbackQuery):
    tariff_id = callback.data.split("_")[2]
    tariff_info = TARIFFS.get(tariff_id)
    
    if not tariff_info:
        await callback.answer("Тариф не найден!", show_alert=True)
        return
    
    user_id = callback.from_user.id
    current_tariff = await get_user_tariff(user_id)
    
    # Проверяем, использовал ли уже пробный период
    conn = await get_db_connection()
    user = await conn.fetchrow(
        "SELECT trial_used FROM users WHERE id = $1", 
        user_id
    )
    has_trial_used = user['trial_used'] if user else False
    await conn.close()
    
    info_text = (
        f"💎 {tariff_info['name']}\n\n"
        f"📊 Лимиты:\n"
        f"• 📢 Каналов: {tariff_info['channels_limit']}\n"
        f"• 📅 Постов в день: {tariff_info['daily_posts_limit']}\n"
        f"• 🤖 AI-копирайтингов: {tariff_info['ai_copies_limit']}\n"
        f"• 💡 AI-идей: {tariff_info['ai_ideas_limit']}\n\n"
        f"💵 Стоимость: "
    )
    
    if tariff_info['price'] == 0:
        info_text += "🆓 Бесплатно\n\n"
    else:
        info_text += f"💳 {tariff_info['price']} {tariff_info['currency']} в месяц\n\n"
    
    info_text += f"📝 {tariff_info['description']}\n\n"
    
    if tariff_id == 'mini':
        info_text += "🆓 Это бесплатный тариф, вы можете активировать его сразу"
    elif tariff_id == current_tariff or (current_tariff == 'standard_trial' and tariff_id == 'standard'):
        info_text += "✅ Это ваш текущий тариф"
    elif tariff_id == 'standard' and not has_trial_used:
        info_text += (
            f"🎁 Специальное предложение!\n"
            f"Получите 3 дня БЕСПЛАТНО за подписку на канал!\n\n"
            f"📋 Ваш ID для заказа: {user_id}"
        )
    else:
        info_text += (
            f"💳 Для заказа тарифа нажмите кнопку ниже\n\n"
            f"📋 Ваш ID для заказа: {user_id}"
        )
    
    await callback.message.edit_text(
        info_text,
        reply_markup=get_tariff_order_keyboard(tariff_id, has_trial_used)
    )

# ========== ОБНОВЛЕННЫЕ ADMIN HANDLERS С РОТАЦИЕЙ ==========
@router.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    # Получаем статистику ротации
    rotation_stats = ai_manager.get_rotation_stats()
    
    admin_text = (
        f"👑 Админ-панель KOLES-TECH\n\n"
        f"🔑 Статус ротации ключей:\n"
        f"• Всего ключей: {rotation_stats['total_keys']}\n"
        f"• Активных: {rotation_stats['active_keys']}\n"
        f"• Заблокировано: {rotation_stats['blocked_keys']}\n"
        f"• Всего запросов: {rotation_stats['total_requests']}\n"
        f"• Ошибок: {rotation_stats['total_errors']}\n\n"
        f"👇 Выберите действие:"
    )
    
    await callback.message.edit_text(
        admin_text,
        reply_markup=get_admin_keyboard()
    )

@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    stats = await get_total_stats()
    
    total_copies_used = sum(s['copies_used'] for s in ai_manager.sessions.values())
    total_ideas_used = sum(s['ideas_used'] for s in ai_manager.sessions.values())
    total_ai_requests = sum(s['total_requests'] for s in ai_manager.sessions.values())
    
    rotation_stats = ai_manager.get_rotation_stats()
    
    # Статистика пробных периодов
    conn = await get_db_connection()
    trial_users = await conn.fetchval(
        "SELECT COUNT(*) FROM users WHERE trial_used = TRUE AND trial_end_date > $1",
        datetime.now(MOSCOW_TZ)
    ) or 0
    expired_trials = await conn.fetchval(
        "SELECT COUNT(*) FROM users WHERE trial_used = TRUE AND trial_end_date <= $1",
        datetime.now(MOSCOW_TZ)
    ) or 0
    await conn.close()
    
    stats_text = (
        "📊 📈 ОБЩАЯ СТАТИСТИКА:\n\n"
        f"👥 ПОЛЬЗОВАТЕЛИ: {stats.get('total_users', 0)}\n"
        f"   • 🚀 Mini: {stats.get('mini_users', 0)}\n"
        f"   • ⭐ Standard: {stats.get('standard_users', 0)}\n"
        f"   • 👑 VIP: {stats.get('vip_users', 0)}\n"
        f"   • 🎁 Пробный период: {trial_users}\n"
        f"   • ⌛ Завершенных пробных: {expired_trials}\n\n"
        f"📅 ПОСТЫ:\n"
        f"   • 📊 Всего: {stats.get('total_posts', 0)}\n"
        f"   • ⏳ Активные: {stats.get('active_posts', 0)}\n"
        f"   • ✅ Отправлено: {stats.get('sent_posts', 0)}\n\n"
        f"📢 КАНАЛЫ: {stats.get('total_channels', 0)}\n\n"
        f"🤖 AI-СЕРВИСЫ:\n"
        f"   • 📝 Копирайтингов: {total_copies_used}\n"
        f"   • 💡 Идей сгенерировано: {total_ideas_used}\n"
        f"   • 🔄 Всего AI запросов: {total_ai_requests}\n\n"
        f"🔑 РОТАЦИЯ КЛЮЧЕЙ:\n"
        f"   • 🟢 Активных: {rotation_stats['active_keys']}/{rotation_stats['total_keys']}\n"
        f"   • 🔴 Заблокировано: {rotation_stats['blocked_keys']}\n"
        f"   • 📊 Успешность: {100 - (rotation_stats['total_errors'] / max(rotation_stats['total_requests'], 1) * 100):.1f}%\n\n"
        f"🛒 ЗАКАЗЫ:\n"
        f"   • ⏳ Ожидают: {stats.get('pending_orders', 0)}\n"
        f"   • ✅ Выполнены: {stats.get('completed_orders', 0)}\n\n"
        f"📍 ВРЕМЯ: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M')}"
    )
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=get_admin_keyboard()
    )

# ========== НОВЫЕ ФУНКЦИИ ДЛЯ АДМИНА ==========
@router.callback_query(F.data == "admin_rotation")
async def admin_rotation_stats(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    rotation_stats = ai_manager.get_rotation_stats()
    
    stats_text = "🔑 📊 ДЕТАЛЬНАЯ СТАТИСТИКА РОТАЦИИ:\n\n"
    
    for i, (key, key_info) in enumerate(ai_manager.key_stats.items(), 1):
        status = "🟢" if ai_manager._is_key_available(key_info) else "🔴"
        blocked_until = ""
        
        if key_info['blocked_until']:
            if key_info['blocked_until'] > datetime.now(MOSCOW_TZ):
                time_left = key_info['blocked_until'] - datetime.now(MOSCOW_TZ)
                blocked_until = f"⏳ {int(time_left.total_seconds() // 60)}мин"
            else:
                blocked_until = "🟢 Доступен"
        
        stats_text += (
            f"{i}. {status} {key[:15]}...\n"
            f"   • 📊 Запросов: {key_info['requests']}\n"
            f"   • ❌ Ошибок: {key_info['errors']}\n"
            f"   • 🔒 403 ошибок: {key_info['403_errors']}\n"
            f"   • 📈 Успешность: {key_info['success_rate']:.1f}%\n"
            f"   • ⚡ Время ответа: {key_info['avg_response_time']:.2f}с\n"
            f"   • {blocked_until}\n\n"
        )
    
    # Последние ротации
    stats_text += "🔄 ПОСЛЕДНИЕ РОТАЦИИ:\n"
    for log in rotation_stats['rotation_log']:
        if 'key' in log:
            stats_text += f"• {log['timestamp'].strftime('%H:%M:%S')} - {log['key']} - {log['reason']}\n"
        elif 'model' in log:
            stats_text += f"• {log['timestamp'].strftime('%H:%M:%S')} - Модель: {log['model']}\n"
    
    buttons = [
        [InlineKeyboardButton(text="🔄 Сбросить блокировки", callback_data="reset_key_blocks")],
        [InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin_panel")]
    ]
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )

@router.callback_query(F.data == "reset_key_blocks")
async def reset_key_blocks_handler(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    ai_manager._reset_all_key_blocks()
    
    await callback.answer("✅ Все блокировки ключей сброшены!", show_alert=True)
    await admin_rotation_stats(callback)

# ========== SCHEDULED TASKS ДЛЯ ПРОБНОГО ПЕРИОДА ==========
async def scheduled_check_trials():
    """Проверка истечения пробных периодов по расписанию"""
    await check_trial_expiry()

# ========== ОБНОВЛЕННЫЙ STARTUP ==========
async def on_startup():
    logger.info("=" * 60)
    logger.info(f"🚀 ЗАПУСК БОТА KOLES-TECH")
    logger.info(f"🤖 AI сервисы: ВКЛЮЧЕНЫ")
    logger.info(f"🔑 Gemini ключей: {len(GEMINI_API_KEYS)}")
    logger.info(f"🎁 Пробный период: ВКЛЮЧЕН")
    logger.info(f"📢 Канал для подписки: {TRIAL_CHANNEL_LINK}")
    logger.info(f"👑 Admin ID: {ADMIN_ID}")
    logger.info("=" * 60)
    
    try:
        await init_db()
        await migrate_db()
        await restore_scheduled_jobs()
        
        scheduler.start()
        
        # Существующие задачи
        scheduler.add_job(
            scheduled_reset_posts,
            trigger='cron',
            hour=0,
            minute=1,
            timezone=MOSCOW_TZ,
            id='reset_posts'
        )
        
        scheduler.add_job(
            reset_ai_limits_daily,
            trigger='cron',
            hour=0,
            minute=0,
            timezone=MOSCOW_TZ,
            id='reset_ai_limits'
        )
        
        # Новая задача для проверки пробных периодов
        scheduler.add_job(
            scheduled_check_trials,
            trigger='cron',
            hour=3,
            minute=0,
            timezone=MOSCOW_TZ,
            id='check_trials'
        )
        
        # Задача для сброса статистики ротации
        scheduler.add_job(
            ai_manager._reset_all_key_blocks,
            trigger='cron',
            hour=6,
            minute=0,
            timezone=MOSCOW_TZ,
            id='reset_key_stats'
        )
        
        me = await bot.get_me()
        logger.info(f"✅ Бот @{me.username} запущен (ID: {me.id})")
        
        if ADMIN_ID:
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"🤖 Бот @{me.username} успешно запущен!\n"
                    f"🆔 ID: {me.id}\n"
                    f"🤖 AI сервисы: ВКЛЮЧЕНЫ\n"
                    f"🔑 Gemini ключей: {len(GEMINI_API_KEYS)}\n"
                    f"🎁 Пробный период: АКТИВЕН\n"
                    f"📢 Канал: {TRIAL_CHANNEL_LINK}\n"
                    f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M:%S')}"
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить админа: {e}")
        
        logger.info("=" * 60)
        logger.info("🎉 БОТ УСПЕШНО ЗАПУЩЕН СО ВСЕМИ ФУНКЦИЯМИ!")
        logger.info("=" * 60)
        return True
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА ПРИ ЗАПУСКЕ: {e}")
        return False

# ========== MAIN ==========
async def main():
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
        # Отправляем сообщение админу об ошибке
        if ADMIN_ID:
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"💥 БОТ УПАЛ С ОШИБКОЙ!\n\n"
                    f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%H:%M:%S')}\n"
                    f"❌ Ошибка: {str(e)[:500]}"
                )
            except:
                pass
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
