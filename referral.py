"""
Модуль партнерской программы для KOLES-TECH Bot
Позволяет пользователям генерировать реферальные ссылки и получать бонусы за приведенных друзей
"""

import logging
import secrets
import string
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

import pytz
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from database import execute_query, DatabasePool
from ai_service import TARIFFS

logger = logging.getLogger(__name__)

MOSCOW_TZ = pytz.timezone('Europe/Moscow')

# ========== ГЕНЕРАЦИЯ РЕФЕРАЛЬНОГО КОДА ==========
def generate_referral_code(length: int = 8) -> str:
    """Генерирует уникальный реферальный код"""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

# ========== ИНИЦИАЛИЗАЦИЯ ТАБЛИЦ ==========
async def init_referral_tables(database_url=None):
    """Инициализация таблиц для партнерской программы"""
    queries = [
        # Таблица реферальных кодов
        '''
        CREATE TABLE IF NOT EXISTS referral_codes (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL UNIQUE,
            code TEXT NOT NULL UNIQUE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            total_referrals INTEGER DEFAULT 0,
            total_earnings DECIMAL(10, 2) DEFAULT 0.0
        )
        ''',
        
        '''
        CREATE INDEX IF NOT EXISTS idx_ref_code ON referral_codes(code)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_ref_user ON referral_codes(user_id)
        ''',
        
        # Таблица рефералов
        '''
        CREATE TABLE IF NOT EXISTS referrals (
            id BIGSERIAL PRIMARY KEY,
            referrer_id BIGINT NOT NULL,
            referred_id BIGINT NOT NULL UNIQUE,
            referred_username TEXT,
            referred_first_name TEXT,
            status TEXT DEFAULT 'pending',  -- pending, standard, vip, completed
            bonus_amount DECIMAL(10, 2) DEFAULT 0.0,
            bonus_paid BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            upgraded_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ
        )
        ''',
        
        '''
        CREATE INDEX IF NOT EXISTS idx_ref_referrer ON referrals(referrer_id)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_ref_referred ON referrals(referred_id)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_ref_status ON referrals(status)
        ''',
        
        # Таблица баланса пользователей
        '''
        CREATE TABLE IF NOT EXISTS user_balances (
            user_id BIGINT PRIMARY KEY,
            balance DECIMAL(10, 2) DEFAULT 0.0,
            total_earned DECIMAL(10, 2) DEFAULT 0.0,
            total_withdrawn DECIMAL(10, 2) DEFAULT 0.0,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        ''',
        
        '''
        CREATE INDEX IF NOT EXISTS idx_balance_user ON user_balances(user_id)
        ''',
        
        # Таблица запросов на вывод
        '''
        CREATE TABLE IF NOT EXISTS withdrawal_requests (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            amount DECIMAL(10, 2) NOT NULL,
            status TEXT DEFAULT 'pending',  -- pending, completed, cancelled
            admin_notes TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            processed_at TIMESTAMPTZ,
            processed_by BIGINT
        )
        ''',
        
        '''
        CREATE INDEX IF NOT EXISTS idx_withdrawal_user ON withdrawal_requests(user_id)
        ''',
        '''
        CREATE INDEX IF NOT EXISTS idx_withdrawal_status ON withdrawal_requests(status)
        '''
    ]
    
    try:
        for query in queries:
            await execute_query(query, database_url=database_url)
        logger.info("✅ Таблицы партнерской программы инициализированы")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации таблиц партнерской программы: {e}")
        raise

# ========== ФУНКЦИИ ДЛЯ РАБОТЫ С РЕФЕРАЛЬНОЙ СИСТЕМОЙ ==========
async def get_or_create_referral_code(user_id: int, database_url=None) -> str:
    """Получает существующий или создает новый реферальный код для пользователя"""
    try:
        result = await execute_query(
            "SELECT code FROM referral_codes WHERE user_id = $1",
            user_id,
            database_url=database_url
        )
        
        if result and len(result) > 0:
            return result[0]['code']
        
        for _ in range(10):
            code = generate_referral_code()
            try:
                await execute_query('''
                    INSERT INTO referral_codes (user_id, code)
                    VALUES ($1, $2)
                ''', user_id, code, database_url=database_url)
                logger.info(f"✅ Создан реферальный код {code} для пользователя {user_id}")
                return code
            except Exception as e:
                if "duplicate key" in str(e).lower():
                    continue
                else:
                    raise
        
        raise Exception("Не удалось создать уникальный реферальный код")
    
    except Exception as e:
        logger.error(f"Ошибка получения/создания реферального кода для {user_id}: {e}")
        return f"user_{user_id}"

async def get_referrer_by_code(code: str, database_url=None) -> Optional[int]:
    """Получает ID пользователя по реферальному коду"""
    try:
        result = await execute_query(
            "SELECT user_id FROM referral_codes WHERE code = $1",
            code,
            database_url=database_url
        )
        
        if result and len(result) > 0:
            return result[0]['user_id']
        return None
    except Exception as e:
        logger.error(f"Ошибка получения реферера по коду {code}: {e}")
        return None

async def register_referral(referrer_id: int, referred_id: int, referred_username: str = None, 
                           referred_first_name: str = None, database_url=None) -> bool:
    """Регистрирует нового реферала"""
    try:
        existing = await execute_query(
            "SELECT id FROM referrals WHERE referred_id = $1",
            referred_id,
            database_url=database_url
        )
        
        if existing and len(existing) > 0:
            logger.info(f"Пользователь {referred_id} уже зарегистрирован как реферал")
            return False
        
        await execute_query('''
            INSERT INTO referrals 
            (referrer_id, referred_id, referred_username, referred_first_name, status)
            VALUES ($1, $2, $3, $4, 'pending')
        ''', referrer_id, referred_id, referred_username, referred_first_name, 
        database_url=database_url)
        
        await execute_query('''
            UPDATE referral_codes 
            SET total_referrals = total_referrals + 1
            WHERE user_id = $1
        ''', referrer_id, database_url=database_url)
        
        logger.info(f"✅ Зарегистрирован реферал {referred_id} для пользователя {referrer_id}")
        return True
    
    except Exception as e:
        logger.error(f"Ошибка регистрации реферала: {e}")
        return False

async def check_referral_upgrade(referred_id: int, new_tariff: str, database_url=None) -> Tuple[bool, float]:
    """Проверяет, нужно ли начислить бонус за апгрейд реферала"""
    try:
        referral = await execute_query('''
            SELECT id, referrer_id, status 
            FROM referrals 
            WHERE referred_id = $1
        ''', referred_id, database_url=database_url)
        
        if not referral or len(referral) == 0:
            return False, 0.0
        
        referral_data = referral[0]
        current_status = referral_data['status']
        referrer_id = referral_data['referrer_id']
        
        bonus_amount = 0.0
        new_status = current_status
        
        if new_tariff == 'standard' and current_status == 'pending':
            bonus_amount = 1.0
            new_status = 'standard'
        elif new_tariff == 'vip' and current_status in ['pending', 'standard']:
            bonus_amount = 2.0
            new_status = 'vip'
        else:
            return False, 0.0
        
        from datetime import datetime
        import pytz
        
        moscow_tz = pytz.timezone('Europe/Moscow')
        
        await execute_query('''
            UPDATE referrals 
            SET status = $1, 
                upgraded_at = $2,
                bonus_amount = CASE 
                    WHEN bonus_amount IS NULL OR bonus_amount = 0 THEN $3
                    ELSE bonus_amount
                END
            WHERE id = $4
        ''', new_status, datetime.now(moscow_tz), bonus_amount, referral_data['id'],
        database_url=database_url)
        
        if bonus_amount > 0:
            await add_to_balance(referrer_id, bonus_amount, f"Бонус за реферала (тариф {new_tariff})", database_url)
            
            await execute_query('''
                UPDATE referral_codes 
                SET total_earnings = total_earnings + $1
                WHERE user_id = $2
            ''', bonus_amount, referrer_id, database_url=database_url)
        
        return True, bonus_amount
    
    except Exception as e:
        logger.error(f"Ошибка проверки апгрейда реферала: {e}")
        return False, 0.0

# ========== ФУНКЦИИ ДЛЯ РАБОТЫ С БАЛАНСОМ ==========
async def get_user_balance(user_id: int, database_url=None) -> Dict[str, float]:
    """Получает баланс пользователя"""
    try:
        result = await execute_query('''
            SELECT balance, total_earned, total_withdrawn 
            FROM user_balances 
            WHERE user_id = $1
        ''', user_id, database_url=database_url)
        
        if result and len(result) > 0:
            return {
                'balance': float(result[0]['balance'] or 0),
                'total_earned': float(result[0]['total_earned'] or 0),
                'total_withdrawn': float(result[0]['total_withdrawn'] or 0)
            }
        
        await execute_query('''
            INSERT INTO user_balances (user_id, balance, total_earned, total_withdrawn)
            VALUES ($1, 0, 0, 0)
            ON CONFLICT (user_id) DO NOTHING
        ''', user_id, database_url=database_url)
        
        return {'balance': 0.0, 'total_earned': 0.0, 'total_withdrawn': 0.0}
    
    except Exception as e:
        logger.error(f"Ошибка получения баланса для {user_id}: {e}")
        return {'balance': 0.0, 'total_earned': 0.0, 'total_withdrawn': 0.0}

async def add_to_balance(user_id: int, amount: float, reason: str = None, database_url=None) -> bool:
    """Добавляет средства на баланс пользователя"""
    try:
        await execute_query('''
            INSERT INTO user_balances (user_id, balance, total_earned, updated_at)
            VALUES ($1, $2, $2, NOW())
            ON CONFLICT (user_id) DO UPDATE
            SET balance = user_balances.balance + $2,
                total_earned = user_balances.total_earned + $2,
                updated_at = NOW()
        ''', user_id, amount, database_url=database_url)
        
        logger.info(f"✅ Начислено ${amount} пользователю {user_id}. Причина: {reason}")
        return True
    
    except Exception as e:
        logger.error(f"Ошибка начисления баланса для {user_id}: {e}")
        return False

async def withdraw_from_balance(user_id: int, amount: float, admin_id: int = None, database_url=None) -> bool:
    """Списывает средства с баланса пользователя (при выводе)"""
    try:
        balance_info = await get_user_balance(user_id, database_url)
        current_balance = balance_info['balance']
        
        if current_balance < amount:
            logger.warning(f"Недостаточно средств для вывода у {user_id}: {current_balance} < {amount}")
            return False
        
        await execute_query('''
            UPDATE user_balances 
            SET balance = balance - $1,
                total_withdrawn = total_withdrawn + $1,
                updated_at = NOW()
            WHERE user_id = $2 AND balance >= $1
        ''', amount, user_id, database_url=database_url)
        
        logger.info(f"✅ Списано ${amount} с баланса пользователя {user_id} (вывод)")
        return True
    
    except Exception as e:
        logger.error(f"Ошибка списания баланса для {user_id}: {e}")
        return False

async def create_withdrawal_request(user_id: int, amount: float, database_url=None) -> Optional[int]:
    """Создает запрос на вывод средств"""
    try:
        balance_info = await get_user_balance(user_id, database_url)
        if balance_info['balance'] < amount:
            return None
        
        result = await execute_query('''
            INSERT INTO withdrawal_requests (user_id, amount, status)
            VALUES ($1, $2, 'pending')
            RETURNING id
        ''', user_id, amount, database_url=database_url)
        
        if result and len(result) > 0:
            request_id = result[0]['id']
            logger.info(f"✅ Создан запрос на вывод #{request_id} для пользователя {user_id} на сумму ${amount}")
            return request_id
        
        return None
    
    except Exception as e:
        logger.error(f"Ошибка создания запроса на вывод: {e}")
        return None

async def get_withdrawal_requests(status: str = 'pending', database_url=None) -> List[Dict]:
    """Получает запросы на вывод"""
    if status:
        return await execute_query('''
            SELECT * FROM withdrawal_requests 
            WHERE status = $1 
            ORDER BY created_at DESC
        ''', status, database_url=database_url)
    else:
        return await execute_query('''
            SELECT * FROM withdrawal_requests 
            ORDER BY created_at DESC
        ''', database_url=database_url)

async def process_withdrawal_request(request_id: int, admin_id: int, approve: bool = True, 
                                     admin_notes: str = None, database_url=None) -> Tuple[bool, str]:
    """Обрабатывает запрос на вывод средств"""
    try:
        requests = await execute_query('''
            SELECT * FROM withdrawal_requests WHERE id = $1
        ''', request_id, database_url=database_url)
        
        if not requests or len(requests) == 0:
            return False, "Запрос не найден"
        
        request = requests[0]
        
        if request['status'] != 'pending':
            return False, f"Запрос уже обработан (статус: {request['status']})"
        
        from datetime import datetime
        import pytz
        moscow_tz = pytz.timezone('Europe/Moscow')
        
        if approve:
            success = await withdraw_from_balance(
                request['user_id'], 
                float(request['amount']), 
                admin_id, 
                database_url
            )
            
            if not success:
                return False, "Недостаточно средств на балансе пользователя"
            
            await execute_query('''
                UPDATE withdrawal_requests 
                SET status = 'completed', 
                    processed_at = $1, 
                    processed_by = $2,
                    admin_notes = $3
                WHERE id = $4
            ''', datetime.now(moscow_tz), admin_id, admin_notes, request_id, 
            database_url=database_url)
            
            return True, f"Запрос #{request_id} подтвержден, средства списаны"
        
        else:
            await execute_query('''
                UPDATE withdrawal_requests 
                SET status = 'cancelled', 
                    processed_at = $1, 
                    processed_by = $2,
                    admin_notes = $3
                WHERE id = $4
            ''', datetime.now(moscow_tz), admin_id, admin_notes or "Отклонено администратором", 
            request_id, database_url=database_url)
            
            return True, f"Запрос #{request_id} отклонен"
    
    except Exception as e:
        logger.error(f"Ошибка обработки запроса на вывод: {e}")
        return False, f"Ошибка: {str(e)}"

async def reset_user_balance(user_id: int, admin_id: int, database_url=None) -> Tuple[bool, str]:
    """Обнуляет баланс пользователя (админ-функция)"""
    try:
        balance_info = await get_user_balance(user_id, database_url)
        current_balance = balance_info['balance']
        
        if current_balance <= 0:
            return False, f"Баланс пользователя {user_id} уже нулевой"
        
        await execute_query('''
            UPDATE user_balances 
            SET balance = 0,
                total_withdrawn = total_withdrawn + $1,
                updated_at = NOW()
            WHERE user_id = $2
        ''', current_balance, user_id, database_url=database_url)
        
        await execute_query('''
            INSERT INTO withdrawal_requests 
            (user_id, amount, status, admin_notes, processed_at, processed_by)
            VALUES ($1, $2, 'completed', $3, NOW(), $4)
        ''', user_id, current_balance, f"Баланс обнулен администратором {admin_id}", admin_id,
        database_url=database_url)
        
        logger.info(f"✅ Админ {admin_id} обнулил баланс пользователя {user_id} (${current_balance})")
        return True, f"Баланс пользователя {user_id} обнулен (${current_balance})"
    
    except Exception as e:
        logger.error(f"Ошибка обнуления баланса для {user_id}: {e}")
        return False, f"Ошибка: {str(e)}"

# ========== СТАТИСТИКА И ИНФОРМАЦИЯ ==========
async def get_referral_stats(user_id: int, database_url=None) -> Dict:
    """Получает статистику реферальной программы для пользователя"""
    try:
        code_info = await execute_query('''
            SELECT code, total_referrals, total_earnings
            FROM referral_codes 
            WHERE user_id = $1
        ''', user_id, database_url=database_url)
        
        referrals = await execute_query('''
            SELECT referred_id, referred_username, referred_first_name, status, 
                   bonus_amount, created_at, upgraded_at
            FROM referrals 
            WHERE referrer_id = $1
            ORDER BY created_at DESC
        ''', user_id, database_url=database_url)
        
        balance_info = await get_user_balance(user_id, database_url)
        
        status_stats = {
            'pending': 0,
            'standard': 0,
            'vip': 0,
            'completed': 0
        }
        
        total_bonus = 0.0
        
        for ref in referrals or []:
            status = ref.get('status', 'pending')
            if status in status_stats:
                status_stats[status] += 1
            total_bonus += float(ref.get('bonus_amount', 0) or 0)
        
        code = code_info[0]['code'] if code_info and len(code_info) > 0 else await get_or_create_referral_code(user_id, database_url)
        total_refs = code_info[0]['total_referrals'] if code_info and len(code_info) > 0 else len(referrals or [])
        total_earnings = float(code_info[0]['total_earnings'] or 0) if code_info and len(code_info) > 0 else 0
        
        return {
            'code': code,
            'total_referrals': total_refs,
            'total_earnings': total_earnings,
            'balance': balance_info['balance'],
            'total_withdrawn': balance_info['total_withdrawn'],
            'referrals': referrals or [],
            'stats': status_stats
        }
    
    except Exception as e:
        logger.error(f"Ошибка получения реферальной статистики для {user_id}: {e}")
        return {
            'code': await get_or_create_referral_code(user_id, database_url),
            'total_referrals': 0,
            'total_earnings': 0.0,
            'balance': 0.0,
            'total_withdrawn': 0.0,
            'referrals': [],
            'stats': {'pending': 0, 'standard': 0, 'vip': 0, 'completed': 0}
        }

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
BOT_USERNAME = "koles_tech_bot"

def set_bot_username(username: str):
    """Устанавливает username бота"""
    global BOT_USERNAME
    BOT_USERNAME = username

async def get_bot_username():
    """Возвращает username бота"""
    return BOT_USERNAME

# ========== КЛАВИАТУРЫ ==========
def get_referral_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для партнерской программы"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔗 Моя реферальная ссылка", callback_data="ref_my_link")],
        [InlineKeyboardButton(text="📊 Моя статистика", callback_data="ref_my_stats")],
        [InlineKeyboardButton(text="💰 Вывести средства", callback_data="ref_withdraw")],
        [InlineKeyboardButton(text="📋 Правила партнерской программы", callback_data="ref_rules")],
        [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="back_to_main")]
    ])

def get_withdrawal_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для запроса вывода средств"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Запросить вывод", callback_data="ref_request_withdraw")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="referral_program")]
    ])

def get_admin_withdrawal_keyboard(requests: List[Dict]) -> InlineKeyboardMarkup:
    """Клавиатура для обработки запросов на вывод"""
    buttons = []
    for req in requests[:5]:
        if req.get('status') == 'pending':
            buttons.append([
                InlineKeyboardButton(
                    text=f"💰 Запрос #{req['id']} - ${float(req['amount']):.2f}",
                    callback_data=f"admin_withdrawal_{req['id']}"
                )
            ])
    
    buttons.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_withdrawals")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_withdrawal_process_keyboard(request_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для обработки конкретного запроса"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_withdrawal_approve_{request_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_withdrawal_reject_{request_id}")
        ],
        [InlineKeyboardButton(text="⬅️ Назад к списку", callback_data="admin_withdrawals")]
    ])

def get_admin_referral_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для управления партнерской программой в админке"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Запросы на вывод", callback_data="admin_withdrawals")],
        [InlineKeyboardButton(text="🔄 Обнулить баланс", callback_data="admin_reset_balance")],
        [InlineKeyboardButton(text="📊 Статистика партнеров", callback_data="admin_ref_stats")],
        [InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin_panel")]
    ])
