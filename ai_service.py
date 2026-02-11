import logging
import random
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any
from enum import Enum
from collections import defaultdict

import google.generativeai as genai

logger = logging.getLogger(__name__)

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

# ========== PROMPT TEMPLATES ==========
COPYWRITER_PROMPT = """Ты профессиональный копирайтер для Telegram-каналов. Создай продающий текст на основе следующих данных:

ТЕМА: {topic}
СТИЛЬ: {style}
ПРИМЕРЫ РАБОТ: {examples}
КОЛИЧЕСТВОВАНИЕ СЛОВ: {word_count} слов

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

# ========== AI CONFIGURATION ==========
MAX_403_RETRIES = 3
REQUEST_COOLDOWN = 15
KEY_BLOCK_DURATION = 300

class AdvancedAISessionManager:
    """Управление AI сессиями с улучшенной ротацией ключей"""
    
    def __init__(self, gemini_api_keys=None, gemini_model="gemini-2.5-flash", 
                 alternative_models=None, moscow_tz=None):
        self.gemini_api_keys = gemini_api_keys or []
        self.gemini_model = gemini_model
        self.alternative_models = alternative_models or []
        self.moscow_tz = moscow_tz
        
        self.sessions: Dict[int, Dict] = {}
        self.key_stats = {}
        self.last_request_time: Dict[int, datetime] = {}
        self.current_model_index = 0
        self.models = [self.gemini_model] + [m for m in self.alternative_models if m != self.gemini_model]
        self.user_request_counts = defaultdict(int)
        self.last_key_rotation = None
        self.current_key_index = 0
        
    def init_keys(self, gemini_api_keys):
        """Инициализация ключей"""
        self.gemini_api_keys = gemini_api_keys
        self.current_key_index = random.randint(0, len(self.gemini_api_keys) - 1)
        self._init_key_stats()
        self.last_key_rotation = datetime.now(self.moscow_tz) if self.moscow_tz else datetime.now()
        
    def _init_key_stats(self):
        """Инициализация статистики ключей"""
        for key in self.gemini_api_keys:
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
            now = datetime.now(self.moscow_tz) if self.moscow_tz else datetime.now()
            self.sessions[user_id] = {
                'history': [],
                'current_key_index': self.current_key_index,
                'request_count': 0,
                'total_requests': 0,
                'copies_used': 0,
                'ideas_used': 0,
                'last_reset': now.date(),
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
        if not self.gemini_api_keys:
            return None, 0, self.models[0] if self.models else "gemini-2.5-flash"
            
        session = self.get_session(user_id)
        
        # Получаем список доступных ключей с приоритетами
        available_keys = []
        
        for i, key in enumerate(self.gemini_api_keys):
            if self._is_key_available(key, user_id):
                stats = self.key_stats[key]
                priority = stats['priority']
                
                # Понижаем приоритет если ключ уже не сработал для этого пользователя
                if key in session['failed_keys']:
                    priority += 50
                
                # Повышаем приоритет если ключ недавно успешно использовался
                if stats['last_success']:
                    hours_since_success = (datetime.now(self.moscow_tz) - stats['last_success']).total_seconds() / 3600
                    if hours_since_success < 1:
                        priority -= 30
                
                # Повышаем приоритет ключам, которые давно не использовались
                if stats['last_used']:
                    hours_since_use = (datetime.now(self.moscow_tz) - stats['last_used']).total_seconds() / 3600
                    if hours_since_use > 2:
                        priority -= 20
                
                available_keys.append((priority, i, key))
        
        # Если нет доступных ключей, пробуем любой незаблокированный
        if not available_keys:
            for i, key in enumerate(self.gemini_api_keys):
                if self.key_stats[key]['blocked_until'] is None or \
                   self.key_stats[key]['blocked_until'] < datetime.now(self.moscow_tz):
                    self.key_stats[key]['403_errors'] = 0
                    self.key_stats[key]['blocked_until'] = None
                    available_keys.append((50, i, key))
        
        if not available_keys:
            logger.error("❌ Нет доступных ключей!")
            return None, 0, self.models[0] if self.models else "gemini-2.5-flash"
        
        # Сортируем по приоритету (ниже = лучше)
        available_keys.sort(key=lambda x: x[0])
        
        # Выбираем ключ с наилучшим приоритетом
        best_priority, key_index, best_key = available_keys[0]
        
        # Обновляем статистику
        session['current_key_index'] = (key_index + 1) % len(self.gemini_api_keys)
        self._update_key_stats_on_use(best_key)
        
        # Выбираем модель
        model_index = self.current_model_index % len(self.models)
        model_name = self.models[model_index]
        
        return best_key, key_index, model_name
    
    def _update_key_stats_on_use(self, key: str):
        """Обновляет статистику при использовании ключа"""
        if key in self.key_stats:
            self.key_stats[key]['requests'] += 1
            self.key_stats[key]['last_used'] = datetime.now(self.moscow_tz)
    
    def _is_key_available(self, key: str, user_id: int) -> bool:
        """Проверяет, доступен ли ключ для пользователя"""
        stats = self.key_stats.get(key)
        if not stats:
            return False
        
        # Проверяем блокировку
        if stats['blocked_until'] and stats['blocked_until'] > datetime.now(self.moscow_tz):
            return False
        
        # Проверяем количество ошибок 403
        if stats['403_errors'] >= MAX_403_RETRIES:
            return False
        
        # Проверяем, не провалился ли ключ для этого пользователя
        if user_id in stats['failed_users']:
            # Даем шанс через 1 час
            if stats['last_error']:
                hours_since_error = (datetime.now(self.moscow_tz) - stats['last_error']).total_seconds() / 3600
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
        stats['last_error'] = datetime.now(self.moscow_tz)
        
        if user_id:
            stats['failed_users'].add(user_id)
        
        if error_type == "403":
            stats['403_errors'] += 1
            stats['priority'] = min(100, stats['priority'] + 30)
            logger.warning(f"Ключ {key[:15]}... получил 403 ошибку. Приоритет: {stats['priority']}")
            
            if stats['403_errors'] >= MAX_403_RETRIES:
                stats['blocked_until'] = datetime.now(self.moscow_tz) + timedelta(seconds=KEY_BLOCK_DURATION)
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
        stats['last_success'] = datetime.now(self.moscow_tz)
        stats['failed_users'].discard(user_id)
        
        session = self.get_session(user_id)
        session['last_successful_key'] = key
        session['consecutive_errors'] = 0
        session['current_attempts'] = 0
        session['failed_keys'].discard(key)
        session['last_success_time'] = datetime.now(self.moscow_tz)
        
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
        now = datetime.now(self.moscow_tz)
        
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
        if not self.models:
            return "gemini-2.5-flash"
        return self.models[self.current_model_index % len(self.models)]
    
    def rotate_model(self):
        """Переключает на следующую модель"""
        self.current_model_index += 1
        model_name = self.get_current_model()
        logger.info(f"Ротация модели на: {model_name}")
    
    def reset_daily_limits(self):
        """Сбрасывает дневные лимиты"""
        if not self.moscow_tz:
            return
        today = datetime.now(self.moscow_tz).date()
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
                'blocked': stats['blocked_until'] is not None and stats['blocked_until'] > datetime.now(self.moscow_tz),
                'failed_users_count': len(stats['failed_users'])
            }
        
        available_keys = len([k for k, v in self.key_stats.items() if v['blocked_until'] is None or v['blocked_until'] < datetime.now(self.moscow_tz)])
        
        return {
            'total_users': len(self.sessions),
            'total_requests': total_requests,
            'total_copies': total_copies,
            'total_ideas': total_ideas,
            'key_stats': key_stats_summary,
            'active_sessions': len([s for s in self.sessions.values() if s['total_requests'] > 0]),
            'available_keys': available_keys,
            'total_keys': len(self.gemini_api_keys)
        }
    
    def check_and_rotate_keys(self):
        """Проверяет и ротирует ключи если нужно"""
        if not self.moscow_tz:
            return
            
        now = datetime.now(self.moscow_tz)
        
        # Восстанавливаем приоритеты заблокированных ключей
        for key in self.gemini_api_keys:
            stats = self.key_stats[key]
            if stats['blocked_until'] and stats['blocked_until'] < now:
                stats['403_errors'] = 0
                stats['blocked_until'] = None
                stats['priority'] = 50
                stats['failed_users'].clear()
                logger.info(f"✅ Восстановлен ключ {key[:15]}...")
        
        # Повышаем приоритеты редко используемых ключей
        for key in self.gemini_api_keys:
            stats = self.key_stats[key]
            if stats['last_used']:
                hours_since_use = (now - stats['last_used']).total_seconds() / 3600
                if hours_since_use > 1:
                    stats['priority'] = max(1, stats['priority'] - 10)
        
        # Очищаем старые failed_users (старше 1 часа)
        for key in self.gemini_api_keys:
            stats = self.key_stats[key]
            if stats['last_error']:
                hours_since_error = (now - stats['last_error']).total_seconds() / 3600
                if hours_since_error > 1:
                    stats['failed_users'].clear()
        
        logger.info("✅ Выполнена автоматическая ротация ключей")

# Глобальный экземпляр AI менеджера
ai_manager = None

async def generate_with_gemini_advanced(prompt: str, user_id: int, ai_manager_instance, max_retries: int = 8) -> Optional[str]:
    """Усовершенствованная генерация с интеллектуальной ротацией"""
    global ai_manager
    if ai_manager_instance:
        manager = ai_manager_instance
    else:
        manager = ai_manager
    
    if not manager:
        logger.error("❌ AI менеджер не инициализирован")
        return None
    
    # Проверяем и ротируем ключи если нужно
    manager.check_and_rotate_keys()
    
    session = manager.get_session(user_id)
    session['total_requests'] += 1
    
    for attempt in range(1, max_retries + 1):
        try:
            key, key_index, model_name = manager.get_best_key(user_id)
            
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
                    manager.mark_key_success(key, user_id)
                    logger.info(f"✅ Успешно | user_{user_id} | ключ: {key_index} | модель: {model_name} | попытка: {attempt}")
                    return response.text.strip()
                else:
                    raise Exception("Пустой ответ от модели")
                
            except Exception as model_error:
                error_str = str(model_error)
                
                # Пробуем другую модель если текущая не поддерживается
                if "not supported" in error_str.lower() or "not found" in error_str.lower():
                    logger.warning(f"Модель {model_name} не поддерживается, пробую следующую")
                    manager.rotate_model()
                    continue
                else:
                    raise model_error
                    
        except Exception as e:
            error_str = str(e)
            logger.warning(f"Ошибка попытки #{attempt} для user_{user_id}: {error_str[:100]}")
            
            # Анализируем ошибку
            if "429" in error_str or "quota" in error_str or "resource exhausted" in error_str:
                manager.mark_key_error(key, "quota", user_id)
                manager.add_failed_key(user_id, key)
            elif "403" in error_str or "permission denied" in error_str or "leaked" in error_str:
                manager.mark_key_error(key, "403", user_id)
                manager.add_failed_key(user_id, key)
            elif "503" in error_str or "unavailable" in error_str:
                manager.rotate_model()
                manager.add_failed_key(user_id, key)
            else:
                logger.error(f"Неизвестная ошибка: {e}")
                manager.add_failed_key(user_id, key)
            
            attempts = manager.increment_user_attempts(user_id)
            
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
                system_stats = manager.get_system_stats()
                logger.error(f"Статистика ключей: {system_stats['key_stats']}")
                
                # Сбрасываем попытки пользователя
                manager.reset_user_attempts(user_id)
    
    return None

# Для обратной совместимости
import asyncio
