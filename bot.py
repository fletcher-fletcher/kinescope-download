import os
import json
import uuid
import asyncio
import logging
import re
import traceback
from pathlib import Path
from typing import Dict
from datetime import datetime

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

from downloader_logic import KinescopeLogic

# Конфигурация
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не установлен в переменных окружения")

DOWNLOADS_DIR = "downloads"
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB

# Создаем папки
Path(DOWNLOADS_DIR).mkdir(exist_ok=True)

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Хранилище задач пользователей
user_tasks: Dict[int, Dict] = {}
active_downloads: Dict[int, str] = {}


class KinescopeBot:
    def __init__(self):
        self.logic = KinescopeLogic(self._log_callback)
        
    def _log_callback(self, message: str):
        """Callback для логирования"""
        logger.info(f"[Kinescope] {message}")
        
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        await update.message.reply_text(
            "🎬 *Kinescope Downloader Bot*\n\n"
            "Я скачиваю видео с Kinescope.\n\n"
            "*Как пользоваться:*\n"
            "1️⃣ Отправьте мне JSON файл\n"
            "2️⃣ Выберите качество видео\n"
            "3️⃣ Получите готовый MP4 файл\n\n"
            "*Команды:*\n"
            "/start - Показать это сообщение\n"
            "/help - Подробная инструкция\n"
            "/cancel - Отменить текущую загрузку",
            parse_mode='Markdown'
        )
    
    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help"""
        await update.message.reply_text(
            "📖 *Инструкция:*\n\n"
            "1. Получите JSON файл с данными о видео\n"
            "2. Отправьте его боту\n"
            "3. Нажмите на кнопку с нужным качеством\n"
            "4. Дождитесь завершения скачивания\n\n"
            "*Важно:*\n"
            "• Видео до 50 MB приходит сразу\n"
            "• Если видео больше, я сообщу об этом\n"
            "• Загрузка может занять несколько минут\n\n"
            "*Поддержка:*\n"
            "По вопросам обращайтесь к администратору",
            parse_mode='Markdown'
        )
    
    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /cancel"""
        user_id = update.effective_user.id
        
        if user_id in active_downloads:
            task_id = active_downloads[user_id]
            if user_id in user_tasks and task_id in user_tasks[user_id]:
                # Удаляем задачу
                json_path = user_tasks[user_id][task_id].get('json_path')
                if json_path and os.path.exists(json_path):
                    os.remove(json_path)
                del user_tasks[user_id][task_id]
                del active_downloads[user_id]
                await update.message.reply_text("⏹️ *Загрузка отменена*", parse_mode='Markdown')
            else:
                await update.message.reply_text("❌ *Нет активных загрузок*", parse_mode='Markdown')
        else:
            await update.message.reply_text("❌ *Нет активных загрузок*", parse_mode='Markdown')
    
    async def handle_json_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка загруженного JSON файла"""
        user_id = update.effective_user.id
        message = update.message
        
        # Проверяем, что это JSON файл
        document = message.document
        if not document.file_name.endswith('.json'):
            await message.reply_text("❌ *Пожалуйста, отправьте JSON файл*", parse_mode='Markdown')
            return
        
        # Отправляем сообщение о начале обработки
        status_msg = await message.reply_text("⏳ *Обрабатываю JSON файл...*", parse_mode='Markdown')
        
        try:
            # Скачиваем JSON файл
            file = await context.bot.get_file(document.file_id)
            json_path = os.path.join(DOWNLOADS_DIR, f"{user_id}_{uuid.uuid4().hex}.json")
            await file.download_to_drive(json_path)
            
            # Извлекаем данные из JSON
            video_list = self.logic.extract_from_json(json_path)
            
            if not video_list:
                await status_msg.edit_text("❌ *Не удалось извлечь данные видео из JSON*", parse_mode='Markdown')
                if os.path.exists(json_path):
                    os.remove(json_path)
                return
            
            # Инициализируем задачи для пользователя
            if user_id not in user_tasks:
                user_tasks[user_id] = {}
            
            # Обрабатываем каждое видео
            for idx, video_info in enumerate(video_list):
                task_id = str(uuid.uuid4())[:8]
                
                # Получаем доступные качества
                qualities = []
                item = video_info['video_data']
                if 'frameRate' in item:
                    qualities = sorted([int(q) for q in item['frameRate'].keys() if q.isdigit()], reverse=True)
                
                # Если качества не найдены, используем стандартные
                if not qualities:
                    qualities = [1080, 720, 480, 360]
                
                user_tasks[user_id][task_id] = {
                    'info': video_info,
                    'json_path': json_path,
                    'qualities': qualities,
                    'title': video_info['title']
                }
                
                # Создаем кнопки для выбора качества
                keyboard = []
                for q in qualities:
                    keyboard.append([InlineKeyboardButton(f"📺 {q}p", callback_data=f"q_{task_id}_{q}")])
                keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data=f"cancel_{task_id}")])
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                # Отправляем сообщение для каждого видео
                if len(video_list) > 1:
                    await message.reply_text(
                        f"🎬 *Видео {idx + 1}/{len(video_list)}*\n"
                        f"📹 *Название:* {video_info['title']}\n"
                        f"📊 *Доступные качества:* {', '.join(map(str, qualities))}p\n\n"
                        f"👇 *Выберите качество:*",
                        parse_mode='Markdown',
                        reply_markup=reply_markup
                    )
                else:
                    await status_msg.edit_text(
                        f"🎬 *Видео:* {video_info['title']}\n"
                        f"📊 *Доступные качества:* {', '.join(map(str, qualities))}p\n\n"
                        f"👇 *Выберите качество:*",
                        parse_mode='Markdown',
                        reply_markup=reply_markup
                    )
            
        except Exception as e:
            logger.error(f"Ошибка при обработке JSON: {e}\n{traceback.format_exc()}")
            await status_msg.edit_text(f"❌ *Ошибка:* {str(e)[:100]}", parse_mode='Markdown')
            if 'json_path' in locals() and os.path.exists(json_path):
                os.remove(json_path)
    
    async def handle_quality_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка выбора качества"""
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        data = query.data
        
        # Обработка отмены
        if data.startswith('cancel_'):
            _, task_id = data.split('_')
            if user_id in user_tasks and task_id in user_tasks[user_id]:
                json_path = user_tasks[user_id][task_id].get('json_path')
                if json_path and os.path.exists(json_path):
                    os.remove(json_path)
                del user_tasks[user_id][task_id]
                await query.edit_message_text("❌ *Загрузка отменена*", parse_mode='Markdown')
            return
        
        # Обработка выбора качества
        if data.startswith('q_'):
            _, task_id, quality = data.split('_')
            quality = int(quality)
            
            # Проверяем существование задачи
            if user_id not in user_tasks or task_id not in user_tasks[user_id]:
                await query.edit_message_text("❌ *Задача не найдена*", parse_mode='Markdown')
                return
            
            # Проверяем, не идет ли уже загрузка
            if user_id in active_downloads:
                await query.edit_message_text(
                    "⚠️ *У вас уже есть активная загрузка*\n"
                    "Дождитесь ее завершения или используйте /cancel",
                    parse_mode='Markdown'
                )
                return
            
            task = user_tasks[user_id][task_id]
            
            # Сообщаем о начале загрузки
            await query.edit_message_text(
                f"🚀 *Начинаю загрузку...*\n\n"
                f"📹 *Видео:* {task['title']}\n"
                f"📺 *Качество:* {quality}p\n\n"
                f"⏳ *Пожалуйста, подождите*\n"
                f"Это может занять несколько минут...",
                parse_mode='Markdown'
            )
            
            # Запускаем загрузку в фоне
            asyncio.create_task(self.download_video(
                query, user_id, task_id, quality, task
            ))
    
    async def download_video(self, query, user_id: int, task_id: str, quality: int, task: dict):
        """Загрузка видео"""
        try:
            # Отмечаем активную загрузку
            active_downloads[user_id] = task_id
            
            # Формируем путь для сохранения
            safe_title = re.sub(r'[^\w\s-]', '', task['title'])
            safe_title = re.sub(r'[\s\\/:*?"<>|]', '_', safe_title).strip()
            filename = f"{user_id}_{safe_title}_{quality}p.mp4"
            save_path = os.path.join(DOWNLOADS_DIR, filename)
            
            # Запускаем скачивание
            success = self.logic.download_pipeline(task['info'], quality, save_path)
            
            if success and os.path.exists(save_path):
                file_size = os.path.getsize(save_path)
                size_mb = file_size / (1024 * 1024)
                
                # Отправляем результат
                if file_size <= MAX_FILE_SIZE:
                    # Отправляем файл напрямую
                    with open(save_path, 'rb') as f:
                        await query.message.reply_document(
                            document=f,
                            filename=filename,
                            caption=(
                                f"✅ *Видео успешно скачано!*\n\n"
                                f"📹 *Название:* {task['title']}\n"
                                f"📺 *Качество:* {quality}p\n"
                                f"📦 *Размер:* {size_mb:.2f} MB\n"
                                f"⏱️ *Готово к скачиванию*"
                            ),
                            parse_mode='Markdown'
                        )
                else:
                    # Файл слишком большой для Telegram
                    await query.message.reply_text(
                        f"✅ *Видео скачано, но превышает лимит Telegram*\n\n"
                        f"📹 *Название:* {task['title']}\n"
                        f"📺 *Качество:* {quality}p\n"
                        f"📦 *Размер:* {size_mb:.2f} MB\n\n"
                        f"⚠️ *Telegram не позволяет отправлять файлы больше 50 MB*\n"
                        f"Видео сохранено на сервере, обратитесь к администратору",
                        parse_mode='Markdown'
                    )
                
                # Удаляем временный файл
                if os.path.exists(save_path):
                    os.remove(save_path)
                    
            else:
                await query.message.reply_text(
                    f"❌ *Ошибка при скачивании видео*\n\n"
                    f"📹 *Видео:* {task['title']}\n"
                    f"📺 *Качество:* {quality}p\n\n"
                    f"*Возможные причины:*\n"
                    f"• Неверные данные в JSON\n"
                    f"• Видео защищено DRM\n"
                    f"• Проблемы с сервером Kinescope\n\n"
                    f"Попробуйте другое качество или проверьте JSON файл",
                    parse_mode='Markdown'
                )
            
        except Exception as e:
            logger.error(f"Ошибка при скачивании: {e}\n{traceback.format_exc()}")
            await query.message.reply_text(
                f"❌ *Критическая ошибка*\n\n"
                f"```\n{str(e)[:200]}\n```\n"
                f"Попробуйте позже или обратитесь к администратору",
                parse_mode='Markdown'
            )
        
        finally:
            # Очищаем данные
            if user_id in active_downloads:
                del active_downloads[user_id]
            
            if user_id in user_tasks and task_id in user_tasks[user_id]:
                json_path = user_tasks[user_id][task_id].get('json_path')
                if json_path and os.path.exists(json_path):
                    os.remove(json_path)
                del user_tasks[user_id][task_id]
    
    def run(self):
        """Запуск бота"""
        # Создаем приложение
        application = Application.builder().token(BOT_TOKEN).build()
        
        # Регистрируем обработчики
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("help", self.help))
        application.add_handler(CommandHandler("cancel", self.cancel))
        application.add_handler(MessageHandler(filters.Document.ALL, self.handle_json_file))
        application.add_handler(CallbackQueryHandler(self.handle_quality_selection))
        
        # Запускаем бота
        logger.info("🚀 Бот запущен и готов к работе!")
        application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    bot = KinescopeBot()
    bot.run()