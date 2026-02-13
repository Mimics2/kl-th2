import logging
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any

import asyncpg
import pytz

from ai_service import TARIFFS

logger = logging.getLogger(__name__)

# ========== DATABASE CONNECTION POOL ==========
class DatabasePool:
    _pool = None
    
    @classmethod
    async def get_pool(cls, database_url=None):
        if cls._pool is None:
            if not database_url:
                logger.error("DATABASE_URL не указан")
                return None
                
            if database_url.startswith("postgres://"):
                conn_string = database_url.replace("postgres://", "postgresql://", 1)
            else:
                conn_string = database_url
            
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

async def execute_query(query: str, *args, database_url=None) -> Any:
    """Выполняет SQL запрос с использованием пула соединений"""
    pool = await DatabasePool.get_pool(database_url)
    if not pool:
        logger.error("Не удалось получить пул соединений")
        return None
        
    async with pool.acquire() as conn:
        try:
            if query.strip().upper().startswith("SELECT"):
                result = await conn.fetch(query, *args)
                return [dict(row) for row in result] if result else []
            else:
                # Для INSERT/UPDATE/DELETE
                result = await conn.fetch(query, *args)
                if result:
                    # Если есть RETURNING, возвращаем список словарей
                    return [dict(row) for row in result]
                # Если нет RETURNING, возвращаем строку статуса
                return "OK"
        except Exception as e:
            logger.error(f"Ошибка запроса: {e}\nЗапрос: {query}")
            raise

# ========== DATABASE INITIALIZATION ==========
async def init_database(database_url=None):
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
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        ''',
        
        # Добавляем уникальное ограничение для ON CONFLICT
        '''
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint 
                WHERE conname = 'channels_user_id_channel_id_key'
            ) THEN
                ALTER TABLE channels 
                ADD CONSTRAINT channels_user_id_channel_id_key 
                UNIQUE (user_id, channel_id);
            END IF;
        END $$;
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
            created_at TIMESTAMPTZ DEFAULT NOW(),
            error_message TEXT,
            retry_count INTEGER DEFAULT 0
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
            await execute_query(query, database_url=database_url)
        logger.info("✅ База данных инициализирована с оптимизированными индексами")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        raise

async def migrate_database(database_url=None):
    """Миграция базы данных"""
    try:
        # Добавляем колонки если их нет
        try:
            await execute_query('''
                ALTER TABLE scheduled_posts 
                ADD COLUMN IF NOT EXISTS error_message TEXT
            ''', database_url=database_url)
            logger.info("✅ Добавлена колонка error_message в таблицу scheduled_posts")
        except Exception as e:
            logger.warning(f"Ошибка добавления колонки error_message: {e}")
        
        try:
            await execute_query('''
                ALTER TABLE scheduled_posts 
                ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0
            ''', database_url=database_url)
            logger.info("✅ Добавлена колонка retry_count в таблицу scheduled_posts")
        except Exception as e:
            logger.warning(f"Ошибка добавления колонки retry_count: {e}")
        
        # Добавляем уникальное ограничение для таблицы channels если его нет
        try:
            await execute_query('''
                DO $$ 
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'channels_user_id_channel_id_key'
                    ) THEN
                        ALTER TABLE channels 
                        ADD CONSTRAINT channels_user_id_channel_id_key 
                        UNIQUE (user_id, channel_id);
                    END IF;
                END $$;
            ''', database_url=database_url)
            logger.info("✅ Добавлено уникальное ограничение для channels (user_id, channel_id)")
        except Exception as e:
            logger.warning(f"Ошибка добавления уникального ограничения для channels: {e}")
        
        logger.info("✅ Миграции завершены")
    except Exception as e:
        logger.error(f"❌ Ошибка миграции БД: {e}")

# ========== DATABASE FUNCTIONS ==========
async def update_user_activity(user_id: int, database_url=None):
    """Обновляет время последней активности пользователя"""
    await execute_query(
        "UPDATE users SET last_seen = NOW() WHERE id = $1",
        user_id,
        database_url=database_url
    )

async def get_user_tariff(user_id: int, database_url=None) -> str:
    """Получает тариф пользователя"""
    await update_user_activity(user_id, database_url)
    
    user = await execute_query(
        "SELECT tariff, is_admin, tariff_expires FROM users WHERE id = $1", 
        user_id,
        database_url=database_url
    )
    
    if not user:
        await execute_query(
            "INSERT INTO users (id, tariff) VALUES ($1, 'mini') ON CONFLICT DO NOTHING",
            user_id,
            database_url=database_url
        )
        return 'mini'
    
    from datetime import datetime
    import pytz
    
    moscow_tz = pytz.timezone('Europe/Moscow')
    
    if user[0].get('is_admin'):
        return 'admin'
    
    # Проверяем срок действия тарифа
    tariff_expires = user[0].get('tariff_expires')
    if tariff_expires and tariff_expires < datetime.now(moscow_tz).date():
        # Тариф истек, возвращаем к минимуму
        await execute_query(
            "UPDATE users SET tariff = 'mini', tariff_expires = NULL, subscription_days = 0 WHERE id = $1",
            user_id,
            database_url=database_url
        )
        return 'mini'
    
    return user[0].get('tariff', 'mini')

async def update_user_subscription(user_id: int, tariff: str, days: int, database_url=None) -> bool:
    """Обновляет подписку пользователя"""
    try:
        from datetime import datetime, timedelta
        import pytz
        
        moscow_tz = pytz.timezone('Europe/Moscow')
        today = datetime.now(moscow_tz).date()
        
        # Получаем текущую дату окончания
        user = await execute_query(
            "SELECT tariff_expires FROM users WHERE id = $1",
            user_id,
            database_url=database_url
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
        ''', tariff, new_expires, days, user_id, database_url=database_url)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления подписки: {e}")
        return False

async def get_user_subscription_info(user_id: int, database_url=None) -> Dict:
    """Получает информацию о подписке пользователя"""
    from datetime import datetime
    import pytz
    
    moscow_tz = pytz.timezone('Europe/Moscow')
    
    user = await execute_query(
        "SELECT tariff, tariff_expires, subscription_days FROM users WHERE id = $1",
        user_id,
        database_url=database_url
    )
    
    if not user:
        return {'tariff': 'mini', 'expires': None, 'days': 0, 'expired': True}
    
    data = user[0]
    tariff_expires = data.get('tariff_expires')
    
    if tariff_expires:
        expired = tariff_expires < datetime.now(moscow_tz).date()
        days_left = (tariff_expires - datetime.now(moscow_tz).date()).days if not expired else 0
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
                             error_message: str = None, database_url=None):
    """Логирует использование AI сервисов"""
    await execute_query('''
        INSERT INTO ai_request_logs 
        (user_id, service_type, prompt_length, response_length, success, 
         error_message, api_key_index, model_name)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ''', user_id, service_type, prompt_length, response_length, 
        success, error_message, api_key_index, model_name,
        database_url=database_url)

async def check_ai_limits(user_id: int, service_type: str, database_url=None, ai_manager=None) -> Tuple[bool, str, Dict]:
    """Проверяет лимиты AI с кешированием"""
    tariff = await get_user_tariff(user_id, database_url)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    
    if not ai_manager:
        return False, "❌ AI менеджер не инициализирован", tariff_info
    
    session = ai_manager.get_session(user_id)
    
    from datetime import datetime, timedelta
    import pytz
    
    moscow_tz = pytz.timezone('Europe/Moscow')
    
    if service_type == 'copy':
        limit = tariff_info['ai_copies_limit']
        used = session['copies_used']
        remaining = limit - used
        
        if used >= limit:
            now = datetime.now(moscow_tz)
            reset_time = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
            reset_time = moscow_tz.localize(reset_time)
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
            now = datetime.now(moscow_tz)
            reset_time = datetime.combine(now.date() + timedelta(days=1), datetime.min.time())
            reset_time = moscow_tz.localize(reset_time)
            time_left = reset_time - now
            hours = int(time_left.total_seconds() // 3600)
            minutes = int((time_left.total_seconds() % 3600) // 60)
            
            return False, f"❌ Достигнут дневной лимит!\n\n💡 Идеи: {used}/{limit}\n⏳ Обновление через: {hours}ч {minutes}м", tariff_info
        
        return True, f"✅ Доступно! Осталось: {remaining}/{limit}", tariff_info
    
    return False, "❌ Неизвестный тип сервиса", tariff_info

async def get_user_channels(user_id: int, database_url=None) -> List[Dict]:
    """Получает каналы пользователя"""
    return await execute_query(
        "SELECT channel_id, channel_name FROM channels WHERE user_id = $1 AND is_active = TRUE ORDER BY created_at DESC",
        user_id,
        database_url=database_url
    )

async def add_user_channel(user_id: int, channel_id: int, channel_name: str, database_url=None) -> bool:
    """Добавляет канал пользователя"""
    try:
        # Сначала проверяем, существует ли канал
        existing = await execute_query(
            "SELECT id FROM channels WHERE user_id = $1 AND channel_id = $2",
            user_id, channel_id,
            database_url=database_url
        )
        
        if existing:
            # Обновляем существующий канал
            await execute_query('''
                UPDATE channels 
                SET channel_name = $1, is_active = TRUE 
                WHERE user_id = $2 AND channel_id = $3
            ''', channel_name, user_id, channel_id, database_url=database_url)
        else:
            # Добавляем новый канал
            await execute_query('''
                INSERT INTO channels (user_id, channel_id, channel_name, is_active)
                VALUES ($1, $2, $3, TRUE)
            ''', user_id, channel_id, channel_name, database_url=database_url)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка добавления канала: {e}")
        return False

async def get_user_channels_count(user_id: int, database_url=None) -> int:
    """Получает количество каналов пользователя"""
    result = await execute_query(
        "SELECT COUNT(*) as count FROM channels WHERE user_id = $1 AND is_active = TRUE",
        user_id,
        database_url=database_url
    )
    return result[0]['count'] if result else 0

async def get_tariff_limits(user_id: int, database_url=None) -> Tuple[int, int, int, int]:
    """Получает лимиты тарифа пользователя"""
    tariff = await get_user_tariff(user_id, database_url)
    tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
    return (tariff_info['channels_limit'], 
            tariff_info['daily_posts_limit'],
            tariff_info['ai_copies_limit'],
            tariff_info['ai_ideas_limit'])

async def get_user_posts_today(user_id: int, database_url=None) -> int:
    """Получает количество постов пользователя сегодня"""
    from datetime import datetime
    import pytz
    
    moscow_tz = pytz.timezone('Europe/Moscow')
    
    result = await execute_query(
        "SELECT posts_today, posts_reset_date FROM users WHERE id = $1",
        user_id,
        database_url=database_url
    )
    
    if not result:
        return 0
    
    user = result[0]
    if user['posts_reset_date'] < datetime.now(moscow_tz).date():
        return 0
    
    return user['posts_today'] or 0

async def increment_user_posts(user_id: int, database_url=None) -> bool:
    """Увеличивает счетчик постов пользователя"""
    try:
        from datetime import datetime
        import pytz
        
        moscow_tz = pytz.timezone('Europe/Moscow')
        
        user = await execute_query(
            "SELECT posts_reset_date FROM users WHERE id = $1",
            user_id,
            database_url=database_url
        )
        
        if not user:
            return False
        
        if user[0]['posts_reset_date'] < datetime.now(moscow_tz).date():
            await execute_query('''
                UPDATE users 
                SET posts_today = 1, posts_reset_date = CURRENT_DATE 
                WHERE id = $1
            ''', user_id, database_url=database_url)
        else:
            await execute_query('''
                UPDATE users 
                SET posts_today = posts_today + 1 
                WHERE id = $1
            ''', user_id, database_url=database_url)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка увеличения счетчика постов: {e}")
        return False

async def save_scheduled_post(user_id: int, channel_id: int, post_data: Dict, scheduled_time: datetime, moscow_tz=None, database_url=None) -> Optional[int]:
    """Сохраняет запланированный пост"""
    try:
        import pytz
        
        if moscow_tz and scheduled_time.tzinfo is None:
            scheduled_time = moscow_tz.localize(scheduled_time)
        scheduled_time_utc = scheduled_time.astimezone(pytz.UTC)
        
        # Правильная обработка post_data
        message_type = post_data.get('message_type', 'text')
        message_text = post_data.get('message_text', '')
        media_file_id = post_data.get('media_file_id', '')
        media_caption = post_data.get('media_caption', '')
        
        # Проверяем корректность данных
        if message_type == 'text' and not message_text:
            logger.error("Текст поста пустой")
            return None
            
        if message_type in ['photo', 'video', 'document'] and not media_file_id:
            logger.error("Медиа файл не указан")
            return None
        
        # Используем параметризованный запрос для избежания SQL инъекций
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
        scheduled_time_utc,
        database_url=database_url
        )
        
        # result уже список словарей благодаря исправленному execute_query
        post_id = result[0]['id'] if result and isinstance(result, list) and len(result) > 0 else None
        
        if post_id and moscow_tz:
            logger.info(f"✅ Пост сохранен в БД с ID: {post_id} на время: {scheduled_time.astimezone(moscow_tz).strftime('%d.%m.%Y %H:%M')} МСК")
        
        return post_id
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения поста в БД: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

async def get_user_stats(user_id: int, database_url=None, ai_manager=None) -> Dict:
    """Получает статистику пользователя"""
    try:
        # Базовая статистика
        tariff = await get_user_tariff(user_id, database_url)
        tariff_info = TARIFFS.get(tariff, TARIFFS['mini'])
        
        # AI статистика
        ai_stats = {}
        if ai_manager:
            ai_stats = ai_manager.get_user_stats(user_id)
        else:
            ai_stats = {
                'copies_used': 0,
                'ideas_used': 0,
                'total_requests': 0
            }
        
        # Посты
        posts_today = await get_user_posts_today(user_id, database_url)
        
        # Каналы
        channels_count = await get_user_channels_count(user_id, database_url)
        
        # Запланированные посты
        scheduled_posts = await execute_query(
            "SELECT COUNT(*) as count FROM scheduled_posts WHERE user_id = $1 AND is_sent = FALSE",
            user_id,
            database_url=database_url
        )
        scheduled_posts = scheduled_posts[0]['count'] if scheduled_posts else 0
        
        # Информация о подписке
        subscription_info = await get_user_subscription_info(user_id, database_url)
        
        return {
            'tariff': tariff_info['name'],
            'posts_today': posts_today,
            'posts_limit': tariff_info['daily_posts_limit'],
            'channels_count': channels_count,
            'channels_limit': tariff_info['channels_limit'],
            'ai_copies_used': ai_stats.get('copies_used', 0),
            'ai_copies_limit': tariff_info['ai_copies_limit'],
            'ai_ideas_used': ai_stats.get('ideas_used', 0),
            'ai_ideas_limit': tariff_info['ai_ideas_limit'],
            'total_ai_requests': ai_stats.get('total_requests', 0),
            'scheduled_posts': scheduled_posts,
            'subscription_expires': subscription_info['expires'],
            'subscription_days_left': subscription_info['days_left'],
            'subscription_expired': subscription_info['expired']
        }
    except Exception as e:
        logger.error(f"Ошибка получения статистики пользователя {user_id}: {e}")
        return {}

async def create_tariff_order(user_id: int, tariff_id: str, database_url=None) -> bool:
    """Создает заказ тарифа"""
    try:
        await execute_query('''
            INSERT INTO tariff_orders (user_id, tariff, status)
            VALUES ($1, $2, 'pending')
        ''', user_id, tariff_id, database_url=database_url)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка создания заказа тарифа: {e}")
        return False

async def get_user_by_id(user_id: int, database_url=None) -> Optional[Dict]:
    """Получает пользователя по ID"""
    result = await execute_query(
        "SELECT id, username, first_name, tariff, is_admin, created_at, tariff_expires, subscription_days FROM users WHERE id = $1",
        user_id,
        database_url=database_url
    )
    
    if result:
        return result[0]
    return None

async def update_user_tariff(user_id: int, tariff: str, database_url=None) -> bool:
    """Обновляет тариф пользователя"""
    try:
        await execute_query('''
            UPDATE users SET tariff = $1 WHERE id = $2
        ''', tariff, user_id, database_url=database_url)
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления тарифа: {e}")
        return False

async def force_update_user_tariff(user_id: int, tariff: str, admin_id: int, database_url=None) -> Tuple[bool, str]:
    """Принудительно обновляет тариф пользователя (админ)"""
    try:
        user = await get_user_by_id(user_id, database_url)
        if not user:
            return False, f"❌ Пользователь с ID {user_id} не найден"
        
        old_tariff = user.get('tariff', 'mini')
        
        success = await update_user_tariff(user_id, tariff, database_url)
        if success:
            await execute_query('''
                INSERT INTO tariff_orders (user_id, tariff, status, admin_notes)
                VALUES ($1, $2, 'force_completed', $3)
            ''', user_id, tariff, f"Принудительное обновление админом {admin_id}", 
            database_url=database_url)
            
            tariff_info = TARIFFS.get(tariff, {})
            old_tariff_info = TARIFFS.get(old_tariff, {})
            
            return True, (
                f"✅ Тариф пользователя {user_id} обновлен!\n\n"
                f"📋 Информация:\n"
                f"👤 Пользователь: {user.get('first_name', 'N/A')} (@{user.get('username', 'N/A')})\n"
                f"🔄 Старый тариф: {old_tariff_info.get('name', old_tariff)}\n"
                f"🆕 Новый тариф: {tariff_info.get('name', tariff)}\n"
                f"👑 Обновил: админ {admin_id}"
            )
        else:
            return False, f"❌ Ошибка при обновлении тарифа пользователя {user_id}"
    except Exception as e:
        logger.error(f"Ошибка принудительного обновления тарифа: {e}")
        return False, f"❌ Ошибка: {str(e)}"

async def get_all_users(database_url=None) -> List[Dict]:
    """Получает всех пользователей"""
    return await execute_query('''
        SELECT id, username, first_name, tariff, is_admin, created_at,
               tariff_expires, subscription_days
        FROM users 
        ORDER BY created_at DESC
    ''', database_url=database_url)

async def get_tariff_orders(status: str = None, database_url=None) -> List[Dict]:
    """Получает заказы тарифов"""
    if status:
        return await execute_query(
            "SELECT * FROM tariff_orders WHERE status = $1 ORDER BY order_date DESC",
            status,
            database_url=database_url
        )
    else:
        return await execute_query(
            "SELECT * FROM tariff_orders ORDER BY order_date DESC",
            database_url=database_url
        )

async def update_order_status(order_id: int, status: str, admin_notes: str = None, database_url=None) -> bool:
    """Обновляет статус заказа"""
    try:
        if admin_notes:
            await execute_query('''
                UPDATE tariff_orders 
                SET status = $1, processed_date = NOW(), admin_notes = $2
                WHERE id = $3
            ''', status, admin_notes, order_id, database_url=database_url)
        else:
            await execute_query('''
                UPDATE tariff_orders 
                SET status = $1, processed_date = NOW()
                WHERE id = $2
            ''', status, order_id, database_url=database_url)
        
        return True
    except Exception as e:
        logger.error(f"Ошибка обновления статуса заказа: {e}")
        return False
