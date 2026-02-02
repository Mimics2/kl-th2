import os
import asyncio
import logging
from datetime import datetime
from typing import Optional

from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

import asyncpg
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

# ========== CONFIG ==========
# Загружаем переменные окружения
API_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = os.getenv("ADMIN_ID", "0")  # По умолчанию 0 если не установлено

# Проверяем обязательные переменные
if not API_TOKEN:
    logging.error("BOT_TOKEN не установлен! Установите переменную окружения BOT_TOKEN")
    exit(1)

if not DATABASE_URL:
    logging.error("DATABASE_URL не установлен! Установите переменную окружения DATABASE_URL")
    exit(1)

try:
    ADMIN_ID = int(ADMIN_ID)
except ValueError:
    logging.error(f"ADMIN_ID должен быть числом, получено: {ADMIN_ID}")
    ADMIN_ID = 0

# ========== SETUP ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

scheduler = AsyncIOScheduler(timezone="UTC")

# ========== DB CONNECTION ==========
async def get_db_connection():
    """Создает соединение с базой данных"""
    try:
        # Добавляем sslmode=require для Railway PostgreSQL
        if DATABASE_URL and "postgresql://" in DATABASE_URL:
            conn_string = DATABASE_URL + "?sslmode=require"
            conn = await asyncpg.connect(conn_string)
        else:
            conn = await asyncpg.connect(DATABASE_URL)
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {e}")
        raise

async def init_db():
    """Инициализация таблиц в PostgreSQL"""
    max_retries = 5
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            conn = await get_db_connection()
            try:
                # Создаем таблицу пользователей
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        id BIGINT PRIMARY KEY,
                        username TEXT,
                        first_name TEXT,
                        registered_at TIMESTAMP DEFAULT NOW()
                    )
                ''')
                
                # Создаем таблицу каналов
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS channels (
                        id BIGSERIAL PRIMARY KEY,
                        user_id BIGINT,
                        channel_id BIGINT UNIQUE,
                        channel_name TEXT,
                        added_at TIMESTAMP DEFAULT NOW()
                    )
                ''')
                
                # Создаем таблицу запланированных постов
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS scheduled_posts (
                        id BIGSERIAL PRIMARY KEY,
                        user_id BIGINT,
                        channel_id BIGINT,
                        message_text TEXT,
                        scheduled_time TIMESTAMP,
                        sent BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                ''')
                
                # Создаем индексы для быстрого поиска
                await conn.execute('''
                    CREATE INDEX IF NOT EXISTS idx_scheduled_posts_time 
                    ON scheduled_posts(scheduled_time) WHERE sent = FALSE
                ''')
                
                logger.info("База данных успешно инициализирована")
                return
                
            finally:
                await conn.close()
                
        except Exception as e:
            logger.warning(f"Попытка {attempt + 1}/{max_retries} не удалась: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                logger.error("Не удалось подключиться к базе данных после нескольких попыток")
                raise

# ========== STATES ==========
class PostStates(StatesGroup):
    waiting_for_channel = State()
    waiting_for_text = State()
    waiting_for_time = State()

# ========== KEYBOARDS ==========
def get_main_keyboard():
    """Основная клавиатура"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Запланировать пост", callback_data="schedule_post")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="broadcast")]
    ])
    return keyboard

def get_cancel_keyboard():
    """Клавиатура для отмены"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])
    return keyboard

# ========== HANDLERS ==========
@router.message(Command("start"))
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    user_id = message.from_user.id
    username = message.from_user.username or "Без имени"
    first_name = message.from_user.first_name or "Пользователь"

    try:
        conn = await get_db_connection()
        try:
            await conn.execute('''
                INSERT INTO users (id, username, first_name)
                VALUES ($1, $2, $3)
                ON CONFLICT (id) DO NOTHING
            ''', user_id, username, first_name)
            logger.info(f"Новый пользователь: {user_id} ({username})")
        finally:
            await conn.close()
    except Exception as e:
        logger.error(f"Ошибка при сохранении пользователя {user_id}: {e}")

    await message.answer(
        f"👋 Привет, {first_name}!\n\n"
        "🤖 Я - бот для планирования постов в Telegram каналах.\n\n"
        "✨ <b>Что я умею:</b>\n"
        "📅 - Запланировать пост на будущее\n"
        "📊 - Показать статистику бота\n"
        "📢 - Сделать рассылку (только для админа)\n\n"
        "👇 Выбери действие:",
        reply_markup=get_main_keyboard(),
        parse_mode="HTML"
    )

@router.callback_query(lambda c: c.data == "schedule_post")
async def process_schedule(callback: types.CallbackQuery, state: FSMContext):
    """Начало планирования поста"""
    await state.set_state(PostStates.waiting_for_channel)
    await callback.message.edit_text(
        "📝 <b>Планирование поста</b>\n\n"
        "Пришлите мне ID канала в формате <code>-1001234567890</code>\n"
        "или просто перешлите любое сообщение из канала.\n\n"
        "⚠️ <i>Убедитесь, что бот добавлен в канал с правами администратора!</i>",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )

@router.message(PostStates.waiting_for_channel)
async def process_channel(message: Message, state: FSMContext):
    """Обработка ID канала"""
    channel_id = None
    
    if message.forward_from_chat:
        channel_id = message.forward_from_chat.id
        channel_name = message.forward_from_chat.title
    elif message.text and message.text.startswith('-100'):
        try:
            channel_id = int(message.text.strip())
            channel_name = f"Канал {channel_id}"
        except ValueError:
            await message.answer("❌ Некорректный ID канала. Попробуйте еще раз.", reply_markup=get_cancel_keyboard())
            return
    else:
        await message.answer("❌ Пожалуйста, отправьте корректный ID канала или перешлите сообщение из канала.", reply_markup=get_cancel_keyboard())
        return
    
    # Сохраняем в состояние
    await state.update_data(channel_id=channel_id, channel_name=channel_name)
    
    # Сохраняем канал в БД
    try:
        conn = await get_db_connection()
        try:
            await conn.execute('''
                INSERT INTO channels (user_id, channel_id, channel_name)
                VALUES ($1, $2, $3)
                ON CONFLICT (channel_id) DO NOTHING
            ''', message.from_user.id, channel_id, channel_name)
            logger.info(f"Пользователь {message.from_user.id} добавил канал {channel_id}")
        finally:
            await conn.close()
    except Exception as e:
        logger.error(f"Ошибка при сохранении канала: {e}")
    
    await state.set_state(PostStates.waiting_for_text)
    await message.answer(
        "✅ Канал добавлен!\n\n"
        "📝 Теперь пришлите текст поста.\n"
        "Поддерживается HTML-разметка:\n"
        "<code>&lt;b&gt;жирный&lt;/b&gt;</code>\n"
        "<code>&lt;i&gt;курсив&lt;/i&gt;</code>\n"
        "<code>&lt;code&gt;код&lt;/code&gt;</code>\n"
        "<code>&lt;a href='ссылка'&gt;текст&lt;/a&gt;</code>",
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )

@router.message(PostStates.waiting_for_text)
async def process_text(message: Message, state: FSMContext):
    """Обработка текста поста"""
    if not message.text:
        await message.answer("❌ Пожалуйста, отправьте текст поста.", reply_markup=get_cancel_keyboard())
        return
    
    await state.update_data(text=message.html_text)
    await state.set_state(PostStates.waiting_for_time)
    
    await message.answer(
        "📝 Текст сохранен!\n\n"
        "⏰ Теперь отправьте дату и время публикации в формате:\n"
        "<code>ГГГГ-ММ-ДД ЧЧ:ММ</code>\n\n"
        "📅 <b>Пример:</b> <code>2024-12-31 20:00</code>\n"
        "🌍 Время указывается по UTC.\n\n"
        "🕐 <i>Текущее время UTC:</i> " + datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
        reply_markup=get_cancel_keyboard(),
        parse_mode="HTML"
    )

@router.message(PostStates.waiting_for_time)
async def process_time(message: Message, state: FSMContext):
    """Обработка времени публикации"""
    try:
        scheduled_time = datetime.strptime(message.text.strip(), "%Y-%m-%d %H:%M")
        now = datetime.utcnow()
        
        # Проверяем, что время в будущем
        if scheduled_time <= now:
            await message.answer(
                f"❌ Время должно быть в будущем!\n"
                f"🕐 Указано: {scheduled_time.strftime('%Y-%m-%d %H:%M')} UTC\n"
                f"🕐 Сейчас: {now.strftime('%Y-%m-%d %H:%M')} UTC",
                reply_markup=get_cancel_keyboard()
            )
            return
            
        # Проверяем, что не слишком далеко (например, не больше года)
        max_future = now.replace(year=now.year + 1)
        if scheduled_time > max_future:
            await message.answer(
                f"❌ Слишком далекая дата!\n"
                f"📅 Максимум можно запланировать на год вперед.",
                reply_markup=get_cancel_keyboard()
            )
            return
            
    except ValueError:
        await message.answer(
            "❌ Некорректный формат даты!\n"
            "✅ Используйте: <code>ГГГГ-ММ-ДД ЧЧ:ММ</code>\n"
            "📅 Пример: <code>2024-12-31 20:00</code>",
            reply_markup=get_cancel_keyboard(),
            parse_mode="HTML"
        )
        return
    
    # Получаем данные из состояния
    data = await state.get_data()
    channel_id = data.get('channel_id')
    channel_name = data.get('channel_name', 'Неизвестный канал')
    text = data.get('text')
    user_id = message.from_user.id
    
    if not all([channel_id, text]):
        await message.answer("❌ Ошибка: данные не сохранены. Начните заново.", reply_markup=get_main_keyboard())
        await state.clear()
        return
    
    # Сохраняем пост в БД
    try:
        conn = await get_db_connection()
        try:
            post_id = await conn.fetchval('''
                INSERT INTO scheduled_posts (user_id, channel_id, message_text, scheduled_time)
                VALUES ($1, $2, $3, $4)
                RETURNING id
            ''', user_id, channel_id, text, scheduled_time)
            
            # Планируем отправку
            scheduler.add_job(
                send_scheduled_post,
                trigger=DateTrigger(run_date=scheduled_time),
                args=(channel_id, text, post_id),
                id=f"post_{post_id}",
                replace_existing=True
            )
            
            logger.info(f"Пост {post_id} запланирован на {scheduled_time}")
            
        finally:
            await conn.close()
    except Exception as e:
        logger.error(f"Ошибка при сохранении поста: {e}")
        await message.answer("❌ Ошибка при сохранении поста. Попробуйте позже.", reply_markup=get_main_keyboard())
        await state.clear()
        return
    
    await state.clear()
    
    await message.answer(
        f"✅ <b>Пост успешно запланирован!</b>\n\n"
        f"📅 <b>Канал:</b> {channel_name}\n"
        f"🆔 <b>ID поста:</b> <code>{post_id}</code>\n"
        f"⏰ <b>Время публикации:</b> {scheduled_time.strftime('%Y-%m-%d %H:%M')} UTC\n"
        f"📝 <b>Текст:</b>\n{text[:100]}...\n\n"
        f"🎯 <i>Пост будет автоматически опубликован в указанное время.</i>",
        reply_markup=get_main_keyboard(),
        parse_mode="HTML"
    )

async def send_scheduled_post(channel_id: int, text: str, post_id: int):
    """Отправка запланированного поста"""
    try:
        await bot.send_message(
            chat_id=channel_id,
            text=text,
            parse_mode="HTML"
        )
        
        # Обновляем статус в БД
        conn = await get_db_connection()
        try:
            await conn.execute('''
                UPDATE scheduled_posts 
                SET sent = TRUE 
                WHERE id = $1
            ''', post_id)
            logger.info(f"✅ Пост {post_id} успешно отправлен в канал {channel_id}")
        finally:
            await conn.close()
            
    except Exception as e:
        logger.error(f"❌ Ошибка отправки поста {post_id}: {e}")
        # Можно добавить повторную попытку или уведомление пользователю

@router.callback_query(lambda c: c.data == "stats")
async def show_stats(callback: types.CallbackQuery):
    """Показ статистики"""
    try:
        conn = await get_db_connection()
        try:
            # Общее количество пользователей
            total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
            
            # Общее количество запланированных постов
            total_posts = await conn.fetchval("SELECT COUNT(*) FROM scheduled_posts")
            
            # Количество активных постов (не отправленных)
            active_posts = await conn.fetchval(
                "SELECT COUNT(*) FROM scheduled_posts WHERE sent = FALSE AND scheduled_time > NOW()"
            )
            
            # Количество каналов
            total_channels = await conn.fetchval("SELECT COUNT(*) FROM channels")
            
            # Количество отправленных постов
            sent_posts = await conn.fetchval("SELECT COUNT(*) FROM scheduled_posts WHERE sent = TRUE")
            
            stats_text = (
                f"📊 <b>Статистика бота</b>\n\n"
                f"👥 <b>Пользователи:</b> {total_users}\n"
                f"📢 <b>Каналы:</b> {total_channels}\n"
                f"📅 <b>Всего постов:</b> {total_posts}\n"
                f"✅ <b>Отправлено:</b> {sent_posts}\n"
                f"⏳ <b>Ожидают:</b> {active_posts}\n\n"
                f"🔄 <i>Обновлено: {datetime.now().strftime('%H:%M:%S')}</i>"
            )
            
            await callback.message.edit_text(
                stats_text, 
                parse_mode="HTML", 
                reply_markup=get_main_keyboard()
            )
            
        finally:
            await conn.close()
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        await callback.message.edit_text(
            "❌ Ошибка при получении статистики. Попробуйте позже.",
            reply_markup=get_main_keyboard()
        )

@router.callback_query(lambda c: c.data == "broadcast")
async def process_broadcast(callback: types.CallbackQuery):
    """Рассылка сообщений (только для админа)"""
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ Эта функция только для администратора!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "📢 <b>Режим рассылки</b>\n\n"
        "Отправьте мне сообщение, которое нужно разослать всем пользователям.\n"
        "Поддерживается HTML-разметка.\n\n"
        "Для отмены нажмите кнопку ниже:",
        parse_mode="HTML",
        reply_markup=get_cancel_keyboard()
    )

@router.callback_query(lambda c: c.data == "cancel")
async def cancel_handler(callback: types.CallbackQuery, state: FSMContext):
    """Отмена текущего действия"""
    current_state = await state.get_state()
    if current_state:
        await state.clear()
    
    await callback.message.edit_text(
        "❌ Действие отменено.\n\n"
        "👇 Что будем делать дальше?",
        reply_markup=get_main_keyboard()
    )

# ========== ADMIN COMMANDS ==========
@router.message(Command("broadcast"))
async def admin_broadcast(message: Message):
    """Команда рассылки для админа"""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ У вас нет прав для этой команды.")
        return
    
    # Простая рассылка всем пользователям
    conn = await get_db_connection()
    success = 0
    failed = 0
    
    try:
        users = await conn.fetch("SELECT id FROM users")
        total = len(users)
        
        status_msg = await message.answer(f"📢 Начинаю рассылку для {total} пользователей...")
        
        for user in users:
            try:
                await bot.send_message(
                    user['id'],
                    "📢 <b>Сообщение от администратора</b>\n\n"
                    "Это тестовое сообщение рассылки.\n\n"
                    "Бот работает в штатном режиме! ✅",
                    parse_mode="HTML"
                )
                success += 1
                await asyncio.sleep(0.05)  # Задержка против ограничений Telegram
                
                # Обновляем статус каждые 10 пользователей
                if success % 10 == 0:
                    await status_msg.edit_text(
                        f"📢 Рассылка: {success}/{total} отправлено..."
                    )
                    
            except Exception as e:
                failed += 1
                logger.error(f"Не удалось отправить пользователю {user['id']}: {e}")
                
    except Exception as e:
        logger.error(f"Ошибка при рассылке: {e}")
        await message.answer(f"❌ Ошибка при рассылке: {e}")
        return
    finally:
        await conn.close()
    
    await status_msg.edit_text(
        f"✅ Рассылка завершена!\n\n"
        f"✅ Успешно: {success}\n"
        f"❌ Не удалось: {failed}\n"
        f"📊 Всего: {total}"
    )

@router.message(Command("status"))
async def cmd_status(message: Message):
    """Проверка статуса бота"""
    try:
        conn = await get_db_connection()
        try:
            # Быстрая проверка подключения к БД
            db_status = "✅ Работает"
            test_query = await conn.fetchval("SELECT 1")
        except Exception as e:
            db_status = f"❌ Ошибка: {e}"
        finally:
            await conn.close()
        
        # Проверка планировщика
        scheduler_status = "✅ Работает" if scheduler.running else "❌ Остановлен"
        
        # Количество активных задач
        jobs_count = len(scheduler.get_jobs())
        
        status_text = (
            f"🤖 <b>Статус бота</b>\n\n"
            f"📊 <b>База данных:</b> {db_status}\n"
            f"⏰ <b>Планировщик:</b> {scheduler_status}\n"
            f"📅 <b>Активных задач:</b> {jobs_count}\n"
            f"🆔 <b>Ваш ID:</b> <code>{message.from_user.id}</code>\n"
            f"👑 <b>Админ ID:</b> <code>{ADMIN_ID}</code>\n\n"
            f"🕐 <i>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>"
        )
        
        await message.answer(status_text, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Ошибка при проверке статуса: {e}")

# ========== UTILITY FUNCTIONS ==========
async def restore_scheduled_jobs():
    """Восстановление запланированных постов из БД при перезапуске бота"""
    try:
        conn = await get_db_connection()
        try:
            posts = await conn.fetch('''
                SELECT id, channel_id, message_text, scheduled_time 
                FROM scheduled_posts 
                WHERE sent = FALSE AND scheduled_time > NOW()
                ORDER BY scheduled_time
            ''')
            
            restored = 0
            for post in posts:
                try:
                    scheduler.add_job(
                        send_scheduled_post,
                        trigger=DateTrigger(run_date=post['scheduled_time']),
                        args=(post['channel_id'], post['message_text'], post['id']),
                        id=f"post_{post['id']}",
                        replace_existing=True
                    )
                    restored += 1
                except Exception as e:
                    logger.error(f"Ошибка восстановления поста {post['id']}: {e}")
            
            logger.info(f"Восстановлено {restored} запланированных постов")
            
        finally:
            await conn.close()
    except Exception as e:
        logger.error(f"Ошибка при восстановлении постов: {e}")

async def on_startup():
    """Действия при запуске бота"""
    logger.info("🤖 Бот запускается...")
    
    # Инициализация БД
    try:
        await init_db()
        logger.info("✅ База данных инициализирована")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")
        return False
    
    # Восстановление задач
    await restore_scheduled_jobs()
    
    # Запуск планировщика
    scheduler.start()
    logger.info("✅ Планировщик запущен")
    
    # Отправка уведомления админу
    try:
        await bot.send_message(
            ADMIN_ID,
            "🤖 Бот успешно запущен!\n"
            f"🕐 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        logger.info(f"✅ Уведомление отправлено админу {ADMIN_ID}")
    except Exception as e:
        logger.warning(f"Не удалось отправить уведомление админу: {e}")
    
    return True

async def on_shutdown():
    """Действия при выключении бота"""
    logger.info("🤖 Бот выключается...")
    scheduler.shutdown()
    logger.info("✅ Планировщик остановлен")

# ========== MAIN ==========
async def main():
    """Основная функция запуска бота"""
    logger.info("🚀 Запуск бота...")
    
    # Запускаем startup процедуры
    if not await on_startup():
        logger.error("❌ Не удалось запустить бота")
        return
    
    try:
        # Запускаем polling
        await dp.start_polling(bot)
    finally:
        # Выполняем shutdown процедуры
        await on_shutdown()

if __name__ == "__main__":
    # Проверяем переменные окружения
    logger.info(f"🆔 Admin ID: {ADMIN_ID}")
    logger.info(f"🌐 Database URL configured: {'Yes' if DATABASE_URL else 'No'}")
    logger.info(f"🤖 Bot token configured: {'Yes' if API_TOKEN else 'No'}")
    
    # Запускаем бота
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"💥 Критическая ошибка: {e}")
