import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any

import pytz

from database import execute_query

logger = logging.getLogger(__name__)

# ========== STATES ==========
from aiogram.fsm.state import State, StatesGroup

class PostStates(StatesGroup):
    waiting_for_channel = State()
    waiting_for_content = State()
    waiting_for_date = State()
    waiting_for_time = State()
    waiting_for_confirmation = State()

# ========== UTILITY FUNCTIONS ==========
def format_datetime(dt: datetime, moscow_tz=None) -> str:
    """Форматирует datetime в строку"""
    if moscow_tz:
        moscow_time = dt.astimezone(moscow_tz)
        return moscow_time.strftime("%d.%m.%Y в %H:%M")
    return dt.strftime("%d.%m.%Y в %H:%M")

def parse_datetime(date_str: str, time_str: str, moscow_tz=None) -> Optional[datetime]:
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
        if moscow_tz:
            return moscow_tz.localize(combined)
        return combined
    except Exception:
        return None

async def schedule_post_in_scheduler(post_id: int, scheduled_time: datetime, scheduler, bot, send_post_func, moscow_tz=None, database_url=None) -> bool:
    """Добавляет пост в планировщик"""
    try:
        if moscow_tz and scheduled_time.tzinfo is None:
            scheduled_time = moscow_tz.localize(scheduled_time)
        
        # Проверяем, что время в будущем
        now = datetime.now(moscow_tz or pytz.UTC)
        if scheduled_time <= now:
            logger.warning(f"Время поста {post_id} уже прошло, отправляю немедленно")
            await send_post_func(post_id, bot, database_url, moscow_tz, scheduler)
            return False
        
        job_id = f"post_{post_id}"
        
        # Удаляем старую задачу если есть
        try:
            scheduler.remove_job(job_id)
        except:
            pass
        
        # Добавляем новую задачу
        scheduler.add_job(
            send_post_func,
            trigger='date',
            run_date=scheduled_time,
            args=[post_id, bot, database_url, moscow_tz, scheduler],
            id=job_id,
            replace_existing=True,
            misfire_grace_time=3600  # 1 час на опоздание
        )
        
        if moscow_tz:
            time_until = (scheduled_time - now).total_seconds() / 3600
            logger.info(f"✅ Пост {post_id} запланирован на {scheduled_time.astimezone(moscow_tz).strftime('%d.%m.%Y %H:%M')} МСК (через {time_until:.1f} часов)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка добавления поста {post_id} в планировщик: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

async def send_scheduled_post(post_id: int, bot, database_url=None, moscow_tz=None, scheduler=None):
    """Отправляет запланированный пост"""
    try:
        # Получаем данные поста
        post_data = await execute_query(
            "SELECT user_id, channel_id, message_type, message_text, media_file_id, media_caption, is_sent, retry_count FROM scheduled_posts WHERE id = $1",
            post_id,
            database_url=database_url
        )
        
        if not post_data or not isinstance(post_data, list) or len(post_data) == 0:
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
        retry_count = post['retry_count'] or 0
        
        # Проверяем количество попыток
        if retry_count >= 3:
            logger.error(f"❌ Пост {post_id} превысил лимит попыток (3)")
            await execute_query('''
                UPDATE scheduled_posts 
                SET error_message = $1 
                WHERE id = $2
            ''', "Превышен лимит попыток отправки", post_id, database_url=database_url)
            return
        
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
            else:
                raise ValueError(f"Неизвестный тип сообщения: {message_type}")
            
            # Обновляем статус поста
            await execute_query('''
                UPDATE scheduled_posts 
                SET is_sent = TRUE, sent_at = NOW(), error_message = NULL, retry_count = 0
                WHERE id = $1
            ''', post_id, database_url=database_url)
            
            # Отправляем уведомление пользователю
            try:
                if moscow_tz:
                    now_str = datetime.now(moscow_tz).strftime('%H:%M:%S')
                else:
                    now_str = datetime.now().strftime('%H:%M:%S')
                    
                await bot.send_message(
                    user_id,
                    f"✅ Пост #{post_id} успешно опубликован!\n\n"
                    f"📢 Канал ID: {channel_id}\n"
                    f"🕐 Время: {now_str}\n\n"
                    f"📍 Пост был автоматически отправлен в запланированное время."
                )
            except Exception as e:
                logger.warning(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
            
            logger.info(f"✅ Пост {post_id} успешно отправлен в канал {channel_id}")
            
        except Exception as e:
            error_msg = str(e)[:500]
            logger.error(f"❌ Ошибка отправки поста {post_id}: {error_msg}")
            
            # Увеличиваем счетчик попыток
            new_retry_count = retry_count + 1
            
            await execute_query('''
                UPDATE scheduled_posts 
                SET error_message = $1, retry_count = $2
                WHERE id = $3
            ''', error_msg, new_retry_count, post_id, database_url=database_url)
            
            # Пытаемся отправить уведомление об ошибке пользователю
            try:
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
            
            # Пробуем еще раз через 5 минут если это первая ошибка
            if scheduler and new_retry_count <= 3:
                if moscow_tz:
                    retry_time = datetime.now(moscow_tz) + timedelta(minutes=5)
                else:
                    retry_time = datetime.now() + timedelta(minutes=5)
                    
                scheduler.add_job(
                    send_scheduled_post,
                    trigger='date',
                    run_date=retry_time,
                    args=[post_id, bot, database_url, moscow_tz, scheduler],
                    id=f"post_{post_id}_retry_{new_retry_count}",
                    replace_existing=True
                )
                logger.warning(f"⚠️ Пост {post_id} будет повторно отправлен через 5 минут (попытка {new_retry_count}/3)")
    
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в send_scheduled_post для поста {post_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())

async def restore_scheduled_posts(scheduler, send_post_func, bot, logger, moscow_tz=None, database_url=None):
    """Восстановление запланированных постов при запуске"""
    try:
        posts = await execute_query('''
            SELECT id, scheduled_time
            FROM scheduled_posts
            WHERE is_sent = FALSE
            ORDER BY scheduled_time ASC
        ''', database_url=database_url)
        
        if not posts or not isinstance(posts, list):
            logger.info("📭 Нет запланированных постов для восстановления")
            return
        
        restored = 0
        now = datetime.now(pytz.UTC)
        
        for post in posts:
            try:
                scheduled_time = post['scheduled_time']
                if scheduled_time.tzinfo is None:
                    scheduled_time = pytz.UTC.localize(scheduled_time)
                
                post_id = post['id']
                job_id = f"post_{post_id}"
                
                # Удаляем старую задачу если есть
                try:
                    scheduler.remove_job(job_id)
                except:
                    pass
                
                # Проверяем время публикации
                if scheduled_time <= now:
                    # Время уже наступило, отправляем немедленно
                    logger.warning(f"⚠️ Время поста {post_id} уже наступило, отправляю немедленно")
                    import asyncio
                    asyncio.create_task(send_post_func(post_id, bot, database_url, moscow_tz, scheduler))
                else:
                    # Время в будущем, планируем
                    scheduler.add_job(
                        send_post_func,
                        trigger='date',
                        run_date=scheduled_time,
                        args=[post_id, bot, database_url, moscow_tz, scheduler],
                        id=job_id,
                        replace_existing=True,
                        misfire_grace_time=3600
                    )
                    
                    if moscow_tz:
                        # Преобразуем в московское время для логирования
                        scheduled_moscow = scheduled_time.astimezone(moscow_tz)
                        time_until = scheduled_time - now
                        hours_until = time_until.total_seconds() / 3600
                        
                        restored += 1
                        logger.info(f"✅ Восстановлен пост {post_id} на {scheduled_moscow.strftime('%d.%m.%Y %H:%M')} МСК (через {hours_until:.1f} часов)")
                
            except Exception as e:
                logger.error(f"Ошибка восстановления поста {post.get('id', 'unknown')}: {e}")
        
        logger.info(f"✅ Восстановлено {restored} запланированных постов")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при восстановлении постов: {e}")
        import traceback
        logger.error(traceback.format_exc())
