import os
import asyncio
import logging
import sys
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from enum import Enum
import json

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

# ========== AI CONFIG ==========
# Загружаем API ключи из переменных окружения или конфига
GEMINI_API_KEYS = os.getenv("GEMINI_API_KEYS", "")
if GEMINI_API_KEYS:
    try:
        GEMINI_API_KEYS = json.loads(GEMINI_API_KEYS)
    except:
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
else:
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
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)
scheduler = AsyncIOScheduler(timezone=MOSCOW_TZ)

# ========== AI SESSION MANAGER С УЛУЧШЕННОЙ РОТАЦИЕЙ ==========
class AISessionManager:
    def __init__(self):
        self.sessions: Dict[int, Dict] = {}
        self.key_stats = {key: {"requests": 0, "errors": 0, "blocked_until": None, "403_errors": 0} for key in GEMINI_API_KEYS}
        self.last_request_time: Dict[int, datetime] = {}
        self.current_model_index = 0
        self.models = [GEMINI_MODEL] + ALTERNATIVE_MODELS
        self.user_retry_counts: Dict[int, Dict] = {}  # Счетчик попыток для каждого пользователя
    
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
                'retry_count': 0,
                'last_successful_key': None,
                'word_count': 200,
                'current_request_retries': 0  # Счетчик попыток для текущего запроса
            }
        return self.sessions[user_id]
    
    def get_available_key(self, user_id: int) -> Tuple[Optional[str], int, str]:
        """Получает доступный API ключ с улучшенной ротацией"""
        session = self.get_session(user_id)
        
        # Если есть успешный ключ - пробуем его сначала
        if session['last_successful_key'] and session['last_successful_key'] in self.key_stats:
            key_info = self.key_stats[session['last_successful_key']]
            if not key_info['blocked_until'] or key_info['blocked_until'] < datetime.now(MOSCOW_TZ):
                # Проверяем количество 403 ошибок для этого ключа
                if key_info['403_errors'] < REQUESTS_PER_KEY:
                    return session['last_successful_key'], GEMINI_API_KEYS.index(session['last_successful_key']), self.get_current_model()
        
        # Ищем любой доступный ключ с наименьшим количеством ошибок
        available_keys = []
        for key_index, key in enumerate(GEMINI_API_KEYS):
            key_info = self.key_stats[key]
            
            # Пропускаем заблокированные ключи
            if key_info['blocked_until'] and key_info['blocked_until'] > datetime.now(MOSCOW_TZ):
                continue
            
            # Пропускаем ключи с 3+ ошибками 403
            if key_info['403_errors'] >= REQUESTS_PER_KEY:
                continue
            
            available_keys.append({
                'key': key,
                'index': key_index,
                'errors': key_info['errors'],
                '403_errors': key_info['403_errors'],
                'requests': key_info['requests']
            })
        
        if not available_keys:
            # Если все ключи имеют 3+ ошибок 403, сбрасываем счетчики
            logger.warning("Все ключи имеют 3+ ошибок 403, сбрасываю счетчики")
            for key in self.key_stats:
                self.key_stats[key]['403_errors'] = 0
                self.key_stats[key]['blocked_until'] = None
            
            # Пробуем первый ключ
            key = GEMINI_API_KEYS[0]
            return key, 0, self.get_current_model()
        
        # Выбираем ключ с наименьшим количеством ошибок
        best_key = min(available_keys, key=lambda x: (x['403_errors'], x['errors']))
        
        session['current_key_index'] = best_key['index']
        session['request_count'] += 1
        self.key_stats[best_key['key']]['requests'] += 1
        
        return best_key['key'], best_key['index'], self.get_current_model()
    
    def mark_key_403_error(self, key: str):
        """Отмечает ошибку 403 для ключа"""
        if key in self.key_stats:
            self.key_stats[key]['403_errors'] += 1
            self.key_stats[key]['errors'] += 1
            
            logger.warning(f"Ключ {key[:15]}... получил 403 ошибку. Всего: {self.key_stats[key]['403_errors']}/{REQUESTS_PER_KEY}")
            
            # Если 3 ошибки 403 - блокируем ключ
            if self.key_stats[key]['403_errors'] >= REQUESTS_PER_KEY:
                self.key_stats[key]['blocked_until'] = datetime.now(MOSCOW_TZ) + timedelta(minutes=KEY_COOLDOWN)
                logger.warning(f"Ключ {key[:15]}... заблокирован на {KEY_COOLDOWN} минут (3 ошибки 403)")
    
    def mark_key_successful(self, key: str, user_id: int):
        """Отмечает ключ как успешный для пользователя"""
        if key in self.key_stats:
            self.key_stats[key]['errors'] = 0
            self.key_stats[key]['403_errors'] = 0  # Сбрасываем счетчик 403 ошибок при успехе
            session = self.get_session(user_id)
            session['last_successful_key'] = key
            session['retry_count'] = 0
            session['current_request_retries'] = 0  # Сбрасываем счетчик попыток
    
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
        logger.info(f"Переключили модель на: {self.get_current_model()}")
    
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

ai_manager = AISessionManager()

# ========== ОБНОВЛЕННЫЙ COPYWRITER_PROMPT С УЧЕТОМ КОЛИЧЕСТВА СЛОВ ==========
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

# ========== ОБНОВЛЕННАЯ ФУНКЦИЯ ГЕНЕРАЦИИ С РОТАЦИЕЙ 3 РАЗА ==========
async def generate_with_gemini(prompt: str, user_id: int, max_retries: int = 3) -> Optional[str]:
    """Генерирует текст через Gemini API с ротацией 3 раза перед ошибкой"""
    
    for retry in range(max_retries):
        try:
            key, key_index, model_name = ai_manager.get_available_key(user_id)
            
            if not key:
                logger.error(f"Нет доступных ключей для user_{user_id}")
                return None
            
            genai.configure(api_key=key)
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
            
            ai_manager.mark_key_successful(key, user_id)
            logger.info(f"✅ AI запрос | user_{user_id} | key_{key_index} | модель: {model_name} | попытка: {retry+1}")
            return response.text.strip()
            
        except Exception as e:
            error_msg = str(e).lower()
            current_retry = retry + 1
            
            # Проверяем тип ошибки
            if "429" in str(e) or "quota" in error_msg or "resource exhausted" in error_msg:
                logger.warning(f"Попытка {current_retry}/{max_retries}: Лимит ключа для user_{user_id}")
                ai_manager.mark_key_403_error(key)
                
            elif "403" in str(e) or "permission denied" in error_msg:
                logger.warning(f"Попытка {current_retry}/{max_retries}: Ошибка 403 для user_{user_id}")
                ai_manager.mark_key_403_error(key)
                
            elif "503" in str(e) or "unavailable" in error_msg:
                logger.warning(f"Попытка {current_retry}/{max_retries}: Ошибка 503 для user_{user_id}")
                ai_manager.rotate_model()
                
            else:
                logger.error(f"Попытка {current_retry}/{max_retries}: Неизвестная ошибка для user_{user_id}: {e}")
            
            # Если это не последняя попытка - ждем и пробуем снова
            if current_retry < max_retries:
                wait_time = 1 * current_retry  # Увеличиваем время ожидания с каждой попыткой
                logger.info(f"Жду {wait_time} секунд перед следующей попыткой...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Все {max_retries} попыток исчерпаны для user_{user_id}")
    
    return None

# ========== DATABASE FUNCTIONS ==========
async def get_db_connection():
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
                created_at TIMESTAMP DEFAULT NOW()
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
        
        await conn.close()
        logger.info("✅ Таблицы БД созданы/проверены")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        raise

async def migrate_db():
    try:
        conn = await get_db_connection()
        
        migrations = [
            ('users', 'ai_copies_used', 'INTEGER DEFAULT 0'),
            ('users', 'ai_ideas_used', 'INTEGER DEFAULT 0'),
            ('users', 'ai_last_used', 'TIMESTAMP'),
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

async def update_ai_usage(user_id: int, service_type: str) -> bool:
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

async def create_tariff_order(user_id: int, tariff_id: str) -> bool:
    try:
        conn = await get_db_connection()
        await conn.execute('''
            INSERT INTO tariff_orders (user_id, tariff, status)
            VALUES ($1, $2, 'pending')
        ''', user_id, tariff_id)
        await conn.close()
        
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

async def get_tariff_limits(user_id: int) -> Tuple[int, int, int, int]:
    tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    return (tariff_info['channels_limit'], 
            tariff_info['daily_posts_limit'],
            tariff_info['ai_copies_limit'],
            tariff_info['ai_ideas_limit'])

async def get_user_channels_count(user_id: int) -> int:
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
    try:
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
        scheduled_time_utc
        )
        
        await conn.close()
        return post_id
    except Exception as e:
        logger.error(f"Ошибка сохранения поста: {e}")
        return None

async def get_user_stats(user_id: int) -> Dict:
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
    try:
        conn = await get_db_connection()
        
        total_users = await conn.fetchval("SELECT COUNT(*) FROM users") or 0
        mini_users = await conn.fetchval("SELECT COUNT(*) FROM users WHERE tariff = 'mini'") or 0
        standard_users = await conn.fetchval("SELECT COUNT(*) FROM users WHERE tariff = 'standard'") or 0
        vip_users = await conn.fetchval("SELECT COUNT(*) FROM users WHERE tariff = 'vip'") or 0
        
        total_posts = await conn.fetchval("SELECT COUNT(*) FROM scheduled_posts") or 0
        active_posts = await conn.fetchval("SELECT COUNT(*) FROM scheduled_posts WHERE is_sent = FALSE") or 0
        sent_posts = await conn.fetchval("SELECT COUNT(*) FROM scheduled_posts WHERE is_sent = TRUE") or 0
        
        total_channels = await conn.fetchval("SELECT COUNT(*) FROM channels WHERE is_active = TRUE") or 0
        
        pending_orders = await conn.fetchval("SELECT COUNT(*) FROM tariff_orders WHERE status = 'pending'") or 0
        completed_orders = await conn.fetchval("SELECT COUNT(*) FROM tariff_orders WHERE status = 'completed'") or 0
        
        await conn.close()
        
        return {
            'total_users': total_users,
            'mini_users': mini_users,
            'standard_users': standard_users,
            'vip_users': vip_users,
            'total_posts': total_posts,
            'active_posts': active_posts,
            'sent_posts': sent_posts,
            'total_channels': total_channels,
            'pending_orders': pending_orders,
            'completed_orders': completed_orders
        }
    except Exception as e:
        logger.error(f"Ошибка получения общей статистики: {e}")
        return {}

async def get_all_users() -> List[Dict]:
    try:
        conn = await get_db_connection()
        users = await conn.fetch('''
            SELECT id, username, first_name, tariff, is_admin, created_at
            FROM users 
            ORDER BY created_at DESC
        ''')
        await conn.close()
        return [dict(user) for user in users]
    except Exception as e:
        logger.error(f"Ошибка получения пользователей: {e}")
        return []

async def get_tariff_orders(status: str = None) -> List[Dict]:
    try:
        conn = await get_db_connection()
        if status:
            orders = await conn.fetch('''
                SELECT * FROM tariff_orders 
                WHERE status = $1
                ORDER BY order_date DESC
            ''', status)
        else:
            orders = await conn.fetch('''
                SELECT * FROM tariff_orders 
                ORDER BY order_date DESC
            ''')
        
        await conn.close()
        return [dict(order) for order in orders]
    except Exception as e:
        logger.error(f"Ошибка получения заказов: {e}")
        return []

async def update_order_status(order_id: int, status: str, admin_notes: str = None) -> bool:
    try:
        conn = await get_db_connection()
        
        if admin_notes:
            await conn.execute('''
                UPDATE tariff_orders 
                SET status = $1, processed_date = NOW(), admin_notes = $2
                WHERE id = $3
            ''', status, admin_notes, order_id)
        else:
            await conn.execute('''
                UPDATE tariff_orders 
                SET status = $1, processed_date = NOW()
                WHERE id = $2
            ''', status, order_id)
        
        await conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления статуса заказа: {e}")
        return False

async def get_user_by_id(user_id: int) -> Optional[Dict]:
    try:
        conn = await get_db_connection()
        user = await conn.fetchrow(
            "SELECT id, username, first_name, tariff, is_admin, created_at FROM users WHERE id = $1",
            user_id
        )
        await conn.close()
        
        if user:
            return dict(user)
        return None
    except Exception as e:
        logger.error(f"Ошибка получения пользователя: {e}")
        return None

async def force_update_user_tariff(user_id: int, tariff: str, admin_id: int) -> Tuple[bool, str]:
    try:
        user = await get_user_by_id(user_id)
        if not user:
            return False, f"❌ Пользователь с ID {user_id} не найден"
        
        old_tariff = user.get('tariff', 'mini')
        
        success = await update_user_tariff(user_id, tariff)
        if success:
            conn = await get_db_connection()
            await conn.execute('''
                INSERT INTO tariff_orders (user_id, tariff, status, admin_notes)
                VALUES ($1, $2, 'force_completed', $3)
            ''', user_id, tariff, f"Принудительное обновление админом {admin_id}")
            await conn.close()
            
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

def format_datetime(dt: datetime) -> str:
    moscow_time = dt.astimezone(MOSCOW_TZ)
    return moscow_time.strftime("%d.%m.%Y в %H:%M")

def parse_datetime(date_str: str, time_str: str) -> Optional[datetime]:
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

# ========== KEYBOARDS ==========
def get_main_menu(user_id: int, is_admin: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="🤖 ИИ-сервисы", callback_data="ai_services")],
        [InlineKeyboardButton(text="📅 Запланировать пост", callback_data="schedule_post")],
        [InlineKeyboardButton(text="📊 Моя статистика", callback_data="my_stats")],
        [InlineKeyboardButton(text="📢 Мои каналы", callback_data="my_channels")],
        [InlineKeyboardButton(text="💎 Тарифы", callback_data="tariffs")],
        [InlineKeyboardButton(text="🆘 Техподдержка", url=f"https://t.me/{SUPPORT_BOT_USERNAME}")],
    ]
    
    if is_admin:
        buttons.append([InlineKeyboardButton(text="👑 Админ-панель", callback_data="admin_panel")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_ai_main_menu(user_tariff: str) -> InlineKeyboardMarkup:
    tariff_info = TARIFFS.get(user_tariff, TARIFFS['mini'])
    
    buttons = [
        [InlineKeyboardButton(text="📝 ИИ-копирайтер", callback_data="ai_copywriter")],
        [InlineKeyboardButton(text="💡 Генератор идей", callback_data="ai_ideas")],
        [InlineKeyboardButton(text="📊 Мои AI-лимиты", callback_data="ai_limits")],
        [InlineKeyboardButton(text="📚 Примеры работ", callback_data="ai_examples")],
        [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel")]
    ])

def get_cancel_ai_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
    ])

def get_channels_keyboard(channels: List[Dict]) -> InlineKeyboardMarkup:
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
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, все верно", callback_data="confirm_yes"),
            InlineKeyboardButton(text="🔄 Нет, начать заново", callback_data="confirm_no")
        ]
    ])

def get_style_keyboard() -> InlineKeyboardMarkup:
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

def get_word_count_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
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

def get_custom_word_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_ai")]
    ])

def get_tariffs_keyboard(user_tariff: str = 'mini') -> InlineKeyboardMarkup:
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
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Общая статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="👥 Список пользователей", callback_data="admin_users_list")],
        [InlineKeyboardButton(text="📢 Сделать рассылку", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="🛒 Управление заказами", callback_data="admin_orders")],
        [InlineKeyboardButton(text="🎯 Назначить тариф", callback_data="admin_assign_tariff")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main")]
    ])

def get_admin_orders_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Все заказы", callback_data="admin_all_orders")],
        [InlineKeyboardButton(text="⏳ Ожидающие заказы", callback_data="admin_pending_orders")],
        [InlineKeyboardButton(text="✅ Выполненные заказы", callback_data="admin_completed_orders")],
        [InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin_panel")]
    ])

def get_order_action_keyboard(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Выполнить заказ", callback_data=f"complete_order_{order_id}")],
        [InlineKeyboardButton(text="❌ Отклонить заказ", callback_data=f"reject_order_{order_id}")],
        [InlineKeyboardButton(text="📝 Добавить заметку", callback_data=f"add_note_{order_id}")],
        [InlineKeyboardButton(text="⬅️ Назад к заказам", callback_data="admin_orders")]
    ])

def get_broadcast_confirmation_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, отправить всем", callback_data="confirm_broadcast")],
        [InlineKeyboardButton(text="❌ Нет, отменить", callback_data="admin_panel")]
    ])

def get_tariff_selection_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    
    for tariff_id, tariff_info in TARIFFS.items():
        if tariff_id == 'admin':
            continue
            
        name = tariff_info['name']
        price = tariff_info['price']
        
        if price == 0:
            button_text = f"{name} - Бесплатно"
        else:
            button_text = f"{name} - {price} USD"
        
        buttons.append([InlineKeyboardButton(
            text=button_text,
            callback_data=f"admin_assign_{tariff_id}"
        )])
    
    buttons.append([InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin_panel")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_confirmation_keyboard(user_id: int, tariff: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, назначить", callback_data=f"confirm_assign_{user_id}_{tariff}"),
            InlineKeyboardButton(text="❌ Нет, отменить", callback_data="admin_panel")
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
    waiting_for_confirm_assign = State()

# ========== BASIC HANDLERS ==========
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
        f"• ⏰ Автопубликация в нужное время\n\n"
        f"📍 Время указывается по Москве\n\n"
        f"👇 Выберите действие:"
    )
    
    await message.answer(welcome_text, reply_markup=get_main_menu(user_id, is_admin))

@router.message(Command("help"))
async def cmd_help(message: Message):
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
        "• VIP ($7) - 7 копирайтов, 50 идей, 3 канала, 12 постов\n\n"
        
        f"🆘 Поддержка: @{SUPPORT_BOT_USERNAME}\n"
        f"💬 Вопросы по оплате: @{ADMIN_CONTACT.replace('@', '')}"
    )
    
    await message.answer(help_text)

# ========== NAVIGATION HANDLERS ==========
@router.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    is_admin = user_id == ADMIN_ID
    
    await callback.message.edit_text(
        "🤖 Главное меню\n\n👇 Выберите действие:",
        reply_markup=get_main_menu(user_id, is_admin)
    )

@router.callback_query(F.data == "cancel")
async def cancel_action(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    is_admin = user_id == ADMIN_ID
    
    await callback.message.edit_text(
        "❌ Действие отменено.\n\n👇 Выберите действие:",
        reply_markup=get_main_menu(user_id, is_admin)
    )

@router.callback_query(F.data == "check_time")
async def check_time(callback: CallbackQuery):
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

# ========== AI HANDLERS ==========
@router.callback_query(F.data == "ai_services")
async def ai_services_menu(callback: CallbackQuery):
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
        ideas_limit=TARIFFS.get(tariff, TARIFFS['mini'])['ai_ideas_limit']
    )
    
    await callback.message.edit_text(
        welcome_text,
        reply_markup=get_ai_main_menu(tariff)
    )

@router.callback_query(F.data == "ai_copywriter")
async def start_copywriter(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    # Проверка лимитов
    tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    session = ai_manager.get_session(user_id)
    
    if session['copies_used'] >= tariff_info['ai_copies_limit']:
        await callback.message.edit_text(
            f"❌ Достигнут дневной лимит!\n\n📝 Копирайтинг: {session['copies_used']}/{tariff_info['ai_copies_limit']}",
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
        f"📝 ИИ-копирайтер\n\n"
        f"✅ Доступно: {tariff_info['ai_copies_limit'] - session['copies_used']}/{tariff_info['ai_copies_limit']} текстов сегодня\n\n"
        f"📌 Шаг 1/4\n"
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
    if len(message.text) < 5:
        await message.answer(
            "❌ Тема слишком короткая! Минимум 5 символов.\n\nВведите тему еще раз:",
            reply_markup=get_cancel_ai_keyboard()
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
        "• Ключевые фразы\n\n"
        "Или напишите 'пропустить', если примеров нет:",
        reply_markup=get_cancel_ai_keyboard()
    )

@router.message(AIStates.waiting_for_examples)
async def process_examples(message: Message, state: FSMContext):
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
        reply_markup=get_style_keyboard()
    )

@router.callback_query(F.data.startswith("style_"))
async def process_style(callback: CallbackQuery, state: FSMContext):
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
        reply_markup=get_word_count_keyboard()
    )

@router.callback_query(F.data.startswith("words_"))
async def process_word_count(callback: CallbackQuery, state: FSMContext):
    if callback.data == "words_custom":
        await callback.message.edit_text(
            "📝 Введите нужное количество слов (от 50 до 1000):\n\n"
            "Примеры:\n"
            "• 80 - короткий анонс\n"
            "• 150 - стандартный пост\n"
            "• 400 - подробная статья",
            reply_markup=get_custom_word_keyboard()
        )
        return
    
    try:
        word_count = int(callback.data.split("_")[1])
        await process_generation(callback, state, word_count)
    except ValueError:
        await callback.answer("❌ Ошибка в количестве слов", show_alert=True)

@router.message(AIStates.waiting_for_word_count)
async def process_custom_word_count(message: Message, state: FSMContext):
    try:
        word_count = int(message.text.strip())
        if word_count < 50 or word_count > 1000:
            await message.answer(
                "❌ Количество слов должно быть от 50 до 1000!\n\n"
                "Попробуйте еще раз:",
                reply_markup=get_custom_word_keyboard()
            )
            return
        
        user_id = message.from_user.id
        ai_manager.set_word_count(user_id, word_count)
        
        data = await state.get_data()
        await generate_ai_text(message, data, word_count)
        
    except ValueError:
        await message.answer(
            "❌ Введите число!\n\nПример: 150, 200, 300",
            reply_markup=get_custom_word_keyboard()
        )

async def process_generation(callback: CallbackQuery, state: FSMContext, word_count: int):
    user_id = callback.from_user.id
    ai_manager.set_word_count(user_id, word_count)
    
    data = await state.get_data()
    
    preview_text = (
        f"📋 Ваш запрос:\n\n"
        f"📌 Тема: {data['topic']}\n"
        f"🎨 Стиль: {data['style']}\n"
        f"📝 Слов: {word_count}\n"
        f"📚 Примеры: {data['examples'][:100]}...\n\n"
        f"⏳ Генерирую текст... Пробую разные ключи (макс. 3 попытки)"
    )
    
    await callback.message.edit_text(preview_text)
    
    current_date = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y")
    prompt = COPYWRITER_PROMPT.format(
        topic=data['topic'],
        style=data['style'],
        examples=data['examples'],
        word_count=word_count,
        current_date=current_date
    )
    
    loading_msg = await callback.message.answer("🔄 Пробую ключ #1...")
    
    generated_text = await generate_with_gemini(prompt, user_id, max_retries=3)
    
    if not generated_text:
        await loading_msg.delete()
        await callback.message.edit_text(
            "❌ Не удалось сгенерировать текст после 3 попыток!\n\n"
            "Возможные причины:\n"
            "• Все ключи API временно недоступны\n"
            "• Закончились лимиты на всех ключах\n"
            "• Проблемы с сетью\n\n"
            "Попробуйте позже или обратитесь в поддержку.",
            reply_markup=get_ai_main_menu(await get_user_tariff(user_id))
        )
        await state.clear()
        return
    
    await loading_msg.delete()
    
    session = ai_manager.get_session(user_id)
    tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    
    session['copies_used'] += 1
    await update_ai_usage(user_id, 'copy')
    
    word_count_actual = len(generated_text.split())
    
    result_text = (
        f"✅ Текст готов! (Попытка #{ai_manager.get_user_retry_count(user_id)})\n\n"
        f"📊 Детали:\n"
        f"• Запрошено слов: {word_count}\n"
        f"• Получено слов: {word_count_actual}\n"
        f"• Символов: {len(generated_text)}\n\n"
        f"📝 Результат:\n\n"
        f"{generated_text}\n\n"
        f"📈 Статистика:\n"
        f"• Использовано: {session['copies_used']}/{tariff_info['ai_copies_limit']}"
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
                await callback.message.edit_text(part)
            else:
                await callback.message.answer(part)
    else:
        await callback.message.edit_text(result_text)
    
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
            InlineKeyboardButton(text="⬅️ В меню AI", callback_data="ai_services")
        ]
    ])
    
    await state.update_data(generated_text=generated_text)
    
    await callback.message.answer(
        "👇 Что сделать с текстом?",
        reply_markup=keyboard
    )

async def generate_ai_text(message: Message, data: Dict, word_count: int):
    user_id = message.from_user.id
    
    preview_text = (
        f"📋 Ваш запрос:\n\n"
        f"📌 Тема: {data['topic']}\n"
        f"🎨 Стиль: {data['style']}\n"
        f"📝 Слов: {word_count}\n"
        f"📚 Примеры: {data['examples'][:100]}...\n\n"
        f"⏳ Генерирую текст... Пробую разные ключи (макс. 3 попытки)"
    )
    
    await message.answer(preview_text)
    
    current_date = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y")
    prompt = COPYWRITER_PROMPT.format(
        topic=data['topic'],
        style=data['style'],
        examples=data['examples'],
        word_count=word_count,
        current_date=current_date
    )
    
    loading_msg = await message.answer("🔄 Пробую ключ #1...")
    
    generated_text = await generate_with_gemini(prompt, user_id, max_retries=3)
    
    if not generated_text:
        await loading_msg.delete()
        await message.answer(
            "❌ Не удалось сгенерировать текст после 3 попыток!\n\n"
            "Возможные причины:\n"
            "• Все ключи API временно недоступны\n"
            "• Закончились лимиты на всех ключах\n"
            "• Проблемы с сетью\n\n"
            "Попробуйте позже или обратитесь в поддержку.",
            reply_markup=get_ai_main_menu(await get_user_tariff(user_id))
        )
        return
    
    await loading_msg.delete()
    
    session = ai_manager.get_session(user_id)
    tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    
    session['copies_used'] += 1
    await update_ai_usage(user_id, 'copy')
    
    word_count_actual = len(generated_text.split())
    
    result_text = (
        f"✅ Текст готов! (Попытка #{ai_manager.get_user_retry_count(user_id)})\n\n"
        f"📊 Детали:\n"
        f"• Запрошено слов: {word_count}\n"
        f"• Получено слов: {word_count_actual}\n"
        f"• Символов: {len(generated_text)}\n\n"
        f"📝 Результат:\n\n"
        f"{generated_text}\n\n"
        f"📈 Статистика:\n"
        f"• Использовано: {session['copies_used']}/{tariff_info['ai_copies_limit']}"
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
                await message.answer(part)
            else:
                await message.answer(part)
    else:
        await message.answer(result_text)
    
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
            InlineKeyboardButton(text="⬅️ В меню AI", callback_data="ai_services")
        ]
    ])
    
    await message.answer(
        "👇 Что сделать с текстом?",
        reply_markup=keyboard
    )

@router.callback_query(F.data == "ai_ideas")
async def start_ideas_generator(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    session = ai_manager.get_session(user_id)
    
    if session['ideas_used'] >= tariff_info['ai_ideas_limit']:
        await callback.message.edit_text(
            f"❌ Достигнут дневной лимит!\n\n💡 Идеи: {session['ideas_used']}/{tariff_info['ai_ideas_limit']}",
            reply_markup=get_ai_main_menu(tariff)
        )
        return
    
    can_request, wait_message = ai_manager.can_make_request(user_id)
    if not can_request:
        await callback.answer(wait_message, show_alert=True)
        return
    
    await state.set_state(AIStates.waiting_for_idea_topic)
    
    await callback.message.edit_text(
        f"💡 Генератор идей\n\n"
        f"✅ Доступно: {tariff_info['ai_ideas_limit'] - session['ideas_used']}/{tariff_info['ai_ideas_limit']} идей сегодня\n\n"
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
    count = int(callback.data.split("_")[1])
    data = await state.get_data()
    
    if count > 20:
        count = 20
    
    await callback.message.edit_text(
        f"💡 Генерация {count} идей по теме:\n"
        f"📌 '{data['topic']}'\n\n"
        f"⏳ Это займет 10-30 секунд..."
    )
    
    current_date = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y")
    prompt = IDEAS_PROMPT.format(
        count=count,
        topic=data['topic'],
        current_date=current_date
    )
    
    loading_msg = await callback.message.answer("🔄 Пробую ключ #1...")
    
    generated_ideas = await generate_with_gemini(prompt, callback.from_user.id, max_retries=3)
    
    if not generated_ideas:
        await loading_msg.delete()
        await callback.message.edit_text(
            "❌ Не удалось сгенерировать идеи после 3 попыток!\n\n"
            "Попробуйте позже или обратитесь в поддержку.",
            reply_markup=get_ai_main_menu(await get_user_tariff(callback.from_user.id))
        )
        await state.clear()
        return
    
    await loading_msg.delete()
    
    ideas_list = generated_ideas.split('\n')
    formatted_ideas = []
    
    for i, idea in enumerate(ideas_list[:count], 1):
        if idea.strip():
            formatted_ideas.append(f"{i}. {idea.strip()}")
    
    session = ai_manager.get_session(callback.from_user.id)
    tariff = await get_user_tariff(callback.from_user.id)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    
    session['ideas_used'] += 1
    await update_ai_usage(callback.from_user.id, 'ideas')
    
    result_text = (
        f"✅ Сгенерировано {len(formatted_ideas)} идей! (Попытка #{ai_manager.get_user_retry_count(callback.from_user.id)})\n\n"
        f"📌 Тема: {data['topic']}\n\n"
        f"💡 Идеи:\n\n" +
        "\n".join(formatted_ideas) +
        f"\n\n📊 Статистика:\n"
        f"• Использовано: {session['ideas_used']}/{tariff_info['ai_ideas_limit']}"
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
    user_id = callback.from_user.id
    tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    
    session = ai_manager.get_session(user_id)
    
    today = datetime.now(MOSCOW_TZ).date()
    reset_time = datetime.combine(today + timedelta(days=1), datetime.min.time())
    reset_time = MOSCOW_TZ.localize(reset_time)
    time_left = reset_time - datetime.now(MOSCOW_TZ)
    hours = int(time_left.total_seconds() // 3600)
    minutes = int((time_left.total_seconds() % 3600) // 60)
    
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
        f"📈 Всего AI запросов: {session['total_requests']}"
    )
    
    await callback.message.edit_text(
        limits_text,
        reply_markup=get_ai_main_menu(tariff)
    )

@router.callback_query(F.data == "ai_examples")
async def show_ai_examples(callback: CallbackQuery):
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
async def copy_text_handler(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    generated_text = data.get('generated_text')
    
    if not generated_text:
        await callback.answer("❌ Текст не найден!", show_alert=True)
        return
    
    await callback.message.answer(
        f"📋 Чтобы скопировать текст:\n\n"
        f"1. Нажмите и удерживайте текст ниже\n"
        f"2. Выберите «Скопировать»\n\n"
        f"{generated_text}"
    )
    
    await callback.answer("✅ Текст готов для копирования!")

@router.callback_query(F.data == "send_to_chat")
async def send_to_chat_handler(callback: CallbackQuery, state: FSMContext):
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
    await callback.answer("✅ Текст сохранен (функция в разработке)", show_alert=True)

@router.callback_query(F.data == "cancel_ai")
async def cancel_ai(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    tariff = await get_user_tariff(user_id)
    
    await callback.message.edit_text(
        "❌ Операция отменена",
        reply_markup=get_ai_main_menu(tariff)
    )

# ========== STATISTICS HANDLERS ==========
@router.callback_query(F.data == "my_stats")
async def show_my_stats(callback: CallbackQuery):
    user_id = callback.from_user.id
    stats = await get_user_stats(user_id)
    current_tariff = await get_user_tariff(user_id)
    tariff_info = TARIFFS.get(current_tariff, TARIFFS['mini'])
    posts_today = await get_user_posts_today(user_id)
    
    session = ai_manager.get_session(user_id)
    
    stats_text = (
        f"📊 Ваша статистика:\n\n"
        f"💎 Текущий тариф: {tariff_info['name']}\n\n"
        f"📅 Посты:\n"
        f"• Всего запланировано: {stats['total_posts']}\n"
        f"• Активных постов: {stats['active_posts']}\n"
        f"• Отправлено постов: {stats['sent_posts']}\n"
        f"• Постов сегодня: {posts_today}/{tariff_info['daily_posts_limit']}\n\n"
        f"📢 Каналы:\n"
        f"• Подключено: {stats['channels']}/{tariff_info['channels_limit']}\n\n"
        f"🤖 AI-сервисы:\n"
        f"• Копирайтер: {session['copies_used']}/{tariff_info['ai_copies_limit']}\n"
        f"• Идеи: {session['ideas_used']}/{tariff_info['ai_ideas_limit']}\n\n"
        f"📍 Время по Москве: {datetime.now(MOSCOW_TZ).strftime('%H:%M')}"
    )
    
    if callback.message.text != stats_text:
        await callback.message.edit_text(
            stats_text,
            reply_markup=get_main_menu(user_id, user_id == ADMIN_ID)
        )
    else:
        await callback.answer("✅ Статистика обновлена")

# ========== CHANNELS HANDLERS ==========
@router.callback_query(F.data == "my_channels")
async def show_my_channels(callback: CallbackQuery):
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
    user_id = callback.from_user.id
    
    channels_count = await get_user_channels_count(user_id)
    channels_limit, _, _, _ = await get_tariff_limits(user_id)
    
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
    user_id = callback.from_user.id
    current_tariff = await get_user_tariff(user_id)
    
    tariffs_text = (
        "💎 Доступные тарифы:\n\n"
        "🚀 Mini (Бесплатно):\n"
        "• 1 канал, 2 поста в день\n"
        "• 1 AI-копирайтинг, 10 идей\n"
        "• Базовые функции\n\n"
        "⭐ Standard ($4/месяц):\n"
        "• 2 канала, 6 постов в день\n"
        "• 3 AI-копирайтинга, 30 идей\n"
        "• Все функции Mini\n\n"
        "👑 VIP ($7/месяц):\n"
        "• 3 канала, 12 постов в день\n"
        "• 7 AI-копирайтингов, 50 идей\n"
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
        f"• Постов в день: {tariff_info['daily_posts_limit']}\n"
        f"• AI-копирайтингов: {tariff_info['ai_copies_limit']}\n"
        f"• AI-идей: {tariff_info['ai_ideas_limit']}\n\n"
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
            "• Публиковать до 2 постов в день\n"
            "• Использовать 1 AI-копирайтинг и 10 идей ежедневно",
            reply_markup=get_main_menu(user_id, user_id == ADMIN_ID)
        )
    else:
        await callback.answer("❌ Ошибка при активации тарифа", show_alert=True)

@router.callback_query(F.data.startswith("order_"))
async def order_tariff(callback: CallbackQuery):
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
    user_id = callback.from_user.id
    
    posts_today = await get_user_posts_today(user_id)
    _, daily_limit, _, _ = await get_tariff_limits(user_id)
    
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
    channel_id = int(callback.data.split("_")[1])
    
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
    
    scheduled_time_utc = data['scheduled_time'].astimezone(pytz.UTC)
    scheduler.add_job(
        send_scheduled_post,
        trigger=DateTrigger(run_date=scheduled_time_utc),
        args=(data['channel_id'], data, post_id),
        id=f"post_{post_id}"
    )
    
    posts_today = await get_user_posts_today(user_id)
    _, daily_limit, _, _ = await get_tariff_limits(user_id)
    
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
    await state.clear()
    user_id = callback.from_user.id
    is_admin = user_id == ADMIN_ID
    
    await callback.message.edit_text(
        "❌ Планирование отменено",
        reply_markup=get_main_menu(user_id, is_admin)
    )

async def send_scheduled_post(channel_id: int, post_data: Dict, post_id: int):
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
        
        conn = await get_db_connection()
        await conn.execute('UPDATE scheduled_posts SET is_sent = TRUE WHERE id = $1', post_id)
        await conn.close()
        
        logger.info(f"✅ Пост {post_id} отправлен в канал {channel_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка отправки поста {post_id}: {e}")

# ========== ADMIN HANDLERS ==========
@router.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "👑 Админ-панель\n\n👇 Выберите действие:",
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
    
    stats_text = (
        "📊 Общая статистика:\n\n"
        f"👥 Пользователи: {stats.get('total_users', 0)}\n"
        f"   • Mini: {stats.get('mini_users', 0)}\n"
        f"   • Standard: {stats.get('standard_users', 0)}\n"
        f"   • VIP: {stats.get('vip_users', 0)}\n\n"
        f"📅 Посты:\n"
        f"   • Всего: {stats.get('total_posts', 0)}\n"
        f"   • Активные: {stats.get('active_posts', 0)}\n"
        f"   • Отправлено: {stats.get('sent_posts', 0)}\n\n"
        f"📢 Каналы: {stats.get('total_channels', 0)}\n\n"
        f"🤖 AI-сервисы:\n"
        f"   • Копирайтингов: {total_copies_used}\n"
        f"   • Идей сгенерировано: {total_ideas_used}\n"
        f"   • Всего AI запросов: {total_ai_requests}\n\n"
        f"🛒 Заказы:\n"
        f"   • Ожидают: {stats.get('pending_orders', 0)}\n"
        f"   • Выполнены: {stats.get('completed_orders', 0)}\n\n"
        f"📍 Время: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M')}"
    )
    
    await callback.message.edit_text(
        stats_text,
        reply_markup=get_admin_keyboard()
    )

@router.callback_query(F.data == "admin_users_list")
async def admin_users_list(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    users = await get_all_users()
    
    if not users:
        await callback.message.edit_text(
            "👥 Пользователи не найдены.",
            reply_markup=get_admin_keyboard()
        )
        return
    
    users_text = "👥 Список пользователей:\n\n"
    
    for i, user in enumerate(users[:50], 1):
        username = user.get('username', 'нет')
        first_name = user.get('first_name', 'Пользователь')
        tariff = user.get('tariff', 'mini')
        created_at = user.get('created_at')
        
        if created_at:
            if isinstance(created_at, str):
                created_str = created_at
            else:
                created_str = created_at.strftime("%d.%m.%Y")
        else:
            created_str = "неизвестно"
        
        users_text += f"{i}. ID: {user['id']}\n"
        users_text += f"   Имя: {first_name}\n"
        users_text += f"   Ник: @{username}\n"
        users_text += f"   Тариф: {tariff}\n"
        users_text += f"   Дата: {created_str}\n\n"
    
    if len(users) > 50:
        users_text += f"\n... и еще {len(users) - 50} пользователей"
    
    await callback.message.edit_text(
        users_text,
        reply_markup=get_admin_keyboard()
    )

@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    await state.set_state(AdminStates.waiting_for_broadcast)
    await callback.message.edit_text(
        "📢 Рассылка сообщения\n\n"
        "Отправьте сообщение для рассылки всем пользователям:\n"
        "(текст, фото, видео или документ)",
        reply_markup=get_cancel_keyboard()
    )

@router.message(AdminStates.waiting_for_broadcast)
async def process_broadcast_message(message: Message, state: FSMContext):
    await state.update_data(broadcast_message=message)
    
    users = await get_all_users()
    users_count = len(users)
    
    await message.answer(
        f"📢 Подтверждение рассылки\n\n"
        f"Сообщение будет отправлено {users_count} пользователям.\n\n"
        f"Вы уверены, что хотите отправить это сообщение?",
        reply_markup=get_broadcast_confirmation_keyboard()
    )

@router.callback_query(F.data == "confirm_broadcast")
async def confirm_broadcast(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    data = await state.get_data()
    broadcast_message = data.get('broadcast_message')
    
    if not broadcast_message:
        await callback.answer("❌ Сообщение не найдено", show_alert=True)
        return
    
    users = await get_all_users()
    sent_count = 0
    error_count = 0
    
    await callback.message.edit_text(
        f"📢 Начинаю рассылку...\n\n"
        f"Всего пользователей: {len(users)}\n"
        f"Отправлено: 0/{len(users)}"
    )
    
    for user in users:
        try:
            if broadcast_message.text:
                await bot.send_message(
                    chat_id=user['id'],
                    text=broadcast_message.text
                )
            elif broadcast_message.photo:
                await bot.send_photo(
                    chat_id=user['id'],
                    photo=broadcast_message.photo[-1].file_id,
                    caption=broadcast_message.caption or ''
                )
            elif broadcast_message.video:
                await bot.send_video(
                    chat_id=user['id'],
                    video=broadcast_message.video.file_id,
                    caption=broadcast_message.caption or ''
                )
            elif broadcast_message.document:
                await bot.send_document(
                    chat_id=user['id'],
                    document=broadcast_message.document.file_id,
                    caption=broadcast_message.caption or ''
                )
            
            sent_count += 1
            
            if sent_count % 10 == 0:
                await callback.message.edit_text(
                    f"📢 Рассылка...\n\n"
                    f"Всего пользователей: {len(users)}\n"
                    f"Отправлено: {sent_count}/{len(users)}"
                )
            
            await asyncio.sleep(0.1)
            
        except Exception as e:
            error_count += 1
            logger.error(f"Ошибка при отправке пользователю {user['id']}: {e}")
    
    await state.clear()
    
    await callback.message.edit_text(
        f"✅ Рассылка завершена!\n\n"
        f"📊 Результаты:\n"
        f"• Всего пользователей: {len(users)}\n"
        f"• Успешно отправлено: {sent_count}\n"
        f"• Ошибок: {error_count}",
        reply_markup=get_admin_keyboard()
    )

@router.callback_query(F.data == "admin_orders")
async def admin_orders_menu(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🛒 Управление заказами тарифов\n\n👇 Выберите действие:",
        reply_markup=get_admin_orders_keyboard()
    )

@router.callback_query(F.data == "admin_all_orders")
async def admin_all_orders(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    orders = await get_tariff_orders()
    
    if not orders:
        await callback.message.edit_text(
            "🛒 Заказы не найдены.",
            reply_markup=get_admin_orders_keyboard()
        )
        return
    
    orders_text = "🛒 Все заказы:\n\n"
    
    for i, order in enumerate(orders[:20], 1):
        order_date = order.get('order_date')
        if order_date:
            if isinstance(order_date, str):
                date_str = order_date
            else:
                date_str = order_date.strftime("%d.%m.%Y %H:%M")
        else:
            date_str = "неизвестно"
        
        status_emoji = "⏳" if order['status'] == 'pending' else "✅" if order['status'] == 'completed' else "❌"
        
        orders_text += f"{i}. ID: {order['id']}\n"
        orders_text += f"   Пользователь: {order['user_id']}\n"
        orders_text += f"   Тариф: {order['tariff']}\n"
        orders_text += f"   Статус: {status_emoji} {order['status']}\n"
        orders_text += f"   Дата: {date_str}\n\n"
    
    if len(orders) > 20:
        orders_text += f"\n... и еще {len(orders) - 20} заказов"
    
    await callback.message.edit_text(
        orders_text,
        reply_markup=get_admin_orders_keyboard()
    )

@router.callback_query(F.data == "admin_pending_orders")
async def admin_pending_orders(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    orders = await get_tariff_orders('pending')
    
    if not orders:
        await callback.message.edit_text(
            "⏳ Ожидающих заказов нет.",
            reply_markup=get_admin_orders_keyboard()
        )
        return
    
    orders_text = "⏳ Ожидающие заказы:\n\n"
    
    for i, order in enumerate(orders, 1):
        order_date = order.get('order_date')
        if order_date:
            if isinstance(order_date, str):
                date_str = order_date
            else:
                date_str = order_date.strftime("%d.%m.%Y %H:%M")
        else:
            date_str = "неизвестно"
        
        tariff_info = TARIFFS.get(order['tariff'], {})
        tariff_name = tariff_info.get('name', order['tariff'])
        price = tariff_info.get('price', 0)
        
        orders_text += f"{i}. ID: {order['id']}\n"
        orders_text += f"   Пользователь: {order['user_id']}\n"
        orders_text += f"   Тариф: {tariff_name}\n"
        orders_text += f"   Стоимость: {price} USD\n"
        orders_text += f"   Дата: {date_str}\n\n"
        orders_text += f"   Действия:\n"
        
        buttons = [
            [InlineKeyboardButton(text="✅ Выполнить", callback_data=f"complete_order_{order['id']}")],
            [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_order_{order['id']}")],
            [InlineKeyboardButton(text="📝 Заметка", callback_data=f"add_note_{order['id']}")]
        ]
        
        if i < len(orders):
            orders_text += "\n" + "-" * 30 + "\n\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад к заказам", callback_data="admin_orders")]
    ])
    
    await callback.message.edit_text(
        orders_text,
        reply_markup=keyboard
    )

@router.callback_query(F.data.startswith("complete_order_"))
async def complete_order(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    order_id = int(callback.data.split("_")[2])
    
    success = await update_order_status(order_id, 'completed')
    
    if success:
        await callback.answer("✅ Заказ выполнен!", show_alert=True)
        
        conn = await get_db_connection()
        order = await conn.fetchrow('SELECT user_id, tariff FROM tariff_orders WHERE id = $1', order_id)
        await conn.close()
        
        if order:
            await update_user_tariff(order['user_id'], order['tariff'])
            
            try:
                await bot.send_message(
                    order['user_id'],
                    f"🎉 Ваш заказ тарифа выполнен!\n\n"
                    f"💎 Тариф: {TARIFFS.get(order['tariff'], {}).get('name', order['tariff'])} активирован.\n"
                    f"📅 Дата активации: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M')}\n\n"
                    f"📍 Тариф действителен 30 дней."
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить пользователя {order['user_id']}: {e}")
    
    await admin_pending_orders(callback)

@router.callback_query(F.data.startswith("reject_order_"))
async def reject_order(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    order_id = int(callback.data.split("_")[2])
    
    success = await update_order_status(order_id, 'rejected')
    
    if success:
        await callback.answer("❌ Заказ отклонен!", show_alert=True)
    
    await admin_pending_orders(callback)

@router.callback_query(F.data.startswith("add_note_"))
async def add_note_start(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    order_id = int(callback.data.split("_")[2])
    
    await state.update_data(order_id=order_id)
    await state.set_state(AdminStates.waiting_for_order_note)
    
    await callback.message.edit_text(
        f"📝 Добавление заметки к заказу #{order_id}\n\n"
        f"Введите заметку для этого заказа:",
        reply_markup=get_cancel_keyboard()
    )

@router.message(AdminStates.waiting_for_order_note)
async def process_order_note(message: Message, state: FSMContext):
    data = await state.get_data()
    order_id = data.get('order_id')
    
    if not order_id:
        await message.answer("❌ Ошибка: заказ не найден")
        await state.clear()
        return
    
    success = await update_order_status(order_id, 'pending', message.text)
    
    if success:
        await message.answer(
            f"✅ Заметка добавлена к заказу #{order_id}",
            reply_markup=get_admin_orders_keyboard()
        )
    else:
        await message.answer(
            "❌ Ошибка при добавлении заметки",
            reply_markup=get_admin_orders_keyboard()
        )
    
    await state.clear()

@router.callback_query(F.data == "admin_completed_orders")
async def admin_completed_orders(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    orders = await get_tariff_orders('completed')
    
    if not orders:
        await callback.message.edit_text(
            "✅ Выполненных заказов нет.",
            reply_markup=get_admin_orders_keyboard()
        )
        return
    
    orders_text = "✅ Выполненные заказы:\n\n"
    
    for i, order in enumerate(orders[:20], 1):
        order_date = order.get('order_date')
        processed_date = order.get('processed_date')
        
        if order_date:
            if isinstance(order_date, str):
                date_str = order_date
            else:
                date_str = order_date.strftime("%d.%m.%Y %H:%M")
        else:
            date_str = "неизвестно"
        
        if processed_date:
            if isinstance(processed_date, str):
                proc_str = processed_date
            else:
                proc_str = processed_date.strftime("%d.%m.%Y %H:%M")
        else:
            proc_str = "неизвестно"
        
        orders_text += f"{i}. ID: {order['id']}\n"
        orders_text += f"   Пользователь: {order['user_id']}\n"
        orders_text += f"   Тариф: {order['tariff']}\n"
        orders_text += f"   Заказ: {date_str}\n"
        orders_text += f"   Выполнен: {proc_str}\n\n"
    
    if len(orders) > 20:
        orders_text += f"\n... и еще {len(orders) - 20} заказов"
    
    await callback.message.edit_text(
        orders_text,
        reply_markup=get_admin_orders_keyboard()
    )

# ========== НОВЫЕ АДМИН ФУНКЦИИ ДЛЯ НАЗНАЧЕНИЯ ТАРИФОВ ==========
@router.callback_query(F.data == "admin_assign_tariff")
async def admin_assign_tariff_start(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    await state.set_state(AdminStates.waiting_for_user_id)
    await callback.message.edit_text(
        "🎯 Назначение тарифа пользователю\n\n"
        "Введите ID пользователя, которому хотите назначить тариф:\n\n"
        "📋 ID можно узнать:\n"
        "1. В списке пользователей (/admin)\n"
        "2. Попросив пользователя отправить /start\n"
        "3. Через функцию поиска в боте\n\n"
        "Введите числовой ID пользователя:",
        reply_markup=get_cancel_keyboard()
    )

@router.message(AdminStates.waiting_for_user_id)
async def process_user_id_for_assignment(message: Message, state: FSMContext):
    try:
        target_user_id = int(message.text.strip())
        
        user = await get_user_by_id(target_user_id)
        if not user:
            await message.answer(
                f"❌ Пользователь с ID {target_user_id} не найден!\n\n"
                f"Попробуйте еще раз:",
                reply_markup=get_cancel_keyboard()
            )
            return
        
        await state.update_data(target_user_id=target_user_id, target_user=user)
        
        await message.answer(
            f"👤 Пользователь найден:\n\n"
            f"ID: {user['id']}\n"
            f"Имя: {user.get('first_name', 'N/A')}\n"
            f"Ник: @{user.get('username', 'N/A')}\n"
            f"Текущий тариф: {TARIFFS.get(user.get('tariff', 'mini'), {}).get('name', user.get('tariff', 'mini'))}\n\n"
            f"👇 Выберите тариф для назначения:",
            reply_markup=get_tariff_selection_keyboard()
        )
    except ValueError:
        await message.answer(
            "❌ Введите корректный числовой ID!\n\n"
            "Пример: 123456789\n\n"
            "Попробуйте еще раз:",
            reply_markup=get_cancel_keyboard()
        )

@router.callback_query(F.data.startswith("admin_assign_"))
async def process_tariff_selection(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    tariff_id = callback.data.split("_")[2]
    tariff_info = TARIFFS.get(tariff_id)
    
    if not tariff_info:
        await callback.answer("❌ Тариф не найден!", show_alert=True)
        return
    
    data = await state.get_data()
    target_user_id = data.get('target_user_id')
    target_user = data.get('target_user')
    
    if not target_user_id or not target_user:
        await callback.answer("❌ Данные пользователя не найдены!", show_alert=True)
        return
    
    old_tariff = target_user.get('tariff', 'mini')
    old_tariff_info = TARIFFS.get(old_tariff, {})
    
    await state.update_data(selected_tariff=tariff_id)
    
    confirmation_text = (
        f"🎯 Подтверждение назначения тарифа\n\n"
        f"👤 Пользователь: {target_user.get('first_name', 'N/A')} (@{target_user.get('username', 'N/A')})\n"
        f"🆔 ID: {target_user_id}\n\n"
        f"🔄 Текущий тариф: {old_tariff_info.get('name', old_tariff)}\n"
        f"🎯 Новый тариф: {tariff_info['name']}\n\n"
        f"📊 Изменения:\n"
        f"• Каналов: {old_tariff_info.get('channels_limit', 1)} → {tariff_info['channels_limit']}\n"
        f"• Постов в день: {old_tariff_info.get('daily_posts_limit', 2)} → {tariff_info['daily_posts_limit']}\n"
        f"• AI-копирайтов: {old_tariff_info.get('ai_copies_limit', 1)} → {tariff_info['ai_copies_limit']}\n"
        f"• AI-идей: {old_tariff_info.get('ai_ideas_limit', 10)} → {tariff_info['ai_ideas_limit']}\n\n"
        f"💵 Стоимость: {'Бесплатно' if tariff_info['price'] == 0 else f'{tariff_info['price']} {tariff_info['currency']}/месяц'}\n\n"
        f"✅ Вы уверены, что хотите назначить этот тариф?"
    )
    
    await callback.message.edit_text(
        confirmation_text,
        reply_markup=get_admin_confirmation_keyboard(target_user_id, tariff_id)
    )

@router.callback_query(F.data.startswith("confirm_assign_"))
async def confirm_assign_tariff(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        await callback.answer("⛔ Доступ запрещен!", show_alert=True)
        return
    
    try:
        parts = callback.data.split("_")
        target_user_id = int(parts[2])
        tariff_id = parts[3]
        
        success, message = await force_update_user_tariff(target_user_id, tariff_id, user_id)
        
        if success:
            try:
                tariff_info = TARIFFS.get(tariff_id, {})
                await bot.send_message(
                    target_user_id,
                    f"🎉 Вам назначен новый тариф!\n\n"
                    f"💎 Тариф: {tariff_info.get('name', tariff_id)}\n"
                    f"👑 Назначил: администратор\n"
                    f"📅 Дата: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M')}\n\n"
                    f"📊 Ваши новые лимиты:\n"
                    f"• Каналов: {tariff_info.get('channels_limit', 1)}\n"
                    f"• Постов в день: {tariff_info.get('daily_posts_limit', 2)}\n"
                    f"• AI-копирайтов: {tariff_info.get('ai_copies_limit', 1)}\n"
                    f"• AI-идей: {tariff_info.get('ai_ideas_limit', 10)}\n\n"
                    f"📍 Тариф действителен 30 дней."
                )
            except Exception as e:
                logger.error(f"Не удалось уведомить пользователя {target_user_id}: {e}")
        
        await callback.message.edit_text(
            message,
            reply_markup=get_admin_keyboard()
        )
        
        await state.clear()
        
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {str(e)}", show_alert=True)
        await callback.message.edit_text(
            "❌ Произошла ошибка при назначении тарифа.",
            reply_markup=get_admin_keyboard()
        )
        await state.clear()

# ========== RESTORE JOBS ==========
async def restore_scheduled_jobs():
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

# ========== SCHEDULED TASKS ==========
async def reset_ai_limits_daily():
    ai_manager.reset_daily_limits()
    logger.info("✅ AI лимиты сброшены")

async def scheduled_reset_posts():
    await reset_daily_posts()

# ========== STARTUP/SHUTDOWN ==========
async def on_startup():
    logger.info("=" * 60)
    logger.info(f"🚀 ЗАПУСК БОТА KOLES-TECH")
    logger.info(f"🤖 AI сервисы: ВКЛЮЧЕНЫ")
    logger.info(f"🔑 Gemini ключей: {len(GEMINI_API_KEYS)}")
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
        
        scheduler.add_job(
            reset_ai_limits_daily,
            trigger='cron',
            hour=0,
            minute=0,
            timezone=MOSCOW_TZ,
            id='reset_ai_limits'
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
                    f"🕐 Время: {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M:%S')}"
                )
            except:
                pass
        
        logger.info("=" * 60)
        logger.info("🎉 БОТ УСПЕШНО ЗАПУЩЕН С AI СЕРВИСАМИ!")
        logger.info("=" * 60)
        return True
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА ПРИ ЗАПУСКЕ: {e}")
        return False

async def on_shutdown():
    logger.info("🛑 Выключение бота...")
    if scheduler.running:
        scheduler.shutdown()
    logger.info("👋 Бот выключен")

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
