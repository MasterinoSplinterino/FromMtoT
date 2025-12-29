"""
MAX to Telegram Message Forwarder Bot
Автоматически пересылает сообщения от указанного пользователя из MAX в Telegram
"""

import os
import time
import threading
from datetime import datetime
from typing import Optional

import telebot
from dotenv import load_dotenv
from MaxBridge import MaxAPI

from logger import setup_logger

load_dotenv()

# Настройки
MAX_AUTH_TOKEN = os.getenv("MAX_AUTH_TOKEN")
MAX_CHAT_ID = int(os.getenv("MAX_CHAT_ID", "0"))
TARGET_USER_ID = os.getenv("TARGET_USER_ID", "").strip()
TARGET_USER_NAME = os.getenv("TARGET_USER_NAME", "").strip().lower()  # Фильтр по имени
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = int(os.getenv("TG_CHAT_ID", "0"))
TG_TOPIC_ID_RAW = os.getenv("TG_TOPIC_ID", "").strip()
TG_TOPIC_ID = int(TG_TOPIC_ID_RAW) if TG_TOPIC_ID_RAW else None

# Парсим словарь имён из .env (формат: ID:Имя,ID:Имя)
USER_NAMES_RAW = os.getenv("USER_NAMES", "")
USER_NAMES = {}
if USER_NAMES_RAW:
    for pair in USER_NAMES_RAW.split(","):
        if ":" in pair:
            uid, name = pair.split(":", 1)
            try:
                USER_NAMES[int(uid.strip())] = name.strip()
            except ValueError:
                pass

# Инициализация
logger = setup_logger("forwarder")
tg_bot = telebot.TeleBot(TG_BOT_TOKEN)
max_api: Optional[MaxAPI] = None

# Кэш контактов для получения имён (начинаем с заданных вручную)
contacts_cache = USER_NAMES.copy()


def validate_config():
    """Проверка наличия всех необходимых настроек"""
    errors = []
    if not MAX_AUTH_TOKEN:
        errors.append("MAX_AUTH_TOKEN не задан")
    if not MAX_CHAT_ID:
        errors.append("MAX_CHAT_ID не задан")
    if not TG_BOT_TOKEN:
        errors.append("TG_BOT_TOKEN не задан")
    if not TG_CHAT_ID:
        errors.append("TG_CHAT_ID не задан")

    if errors:
        for err in errors:
            logger.error(err)
        raise ValueError("Проверьте .env файл")


def get_user_name(user_id: int) -> str:
    """Получить имя пользователя по ID"""
    if user_id in contacts_cache:
        return contacts_cache[user_id]

    try:
        details = max_api.get_contact_details([user_id])
        if details and len(details) > 0:
            user = details[0]
            name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
            contacts_cache[user_id] = name or f"User {user_id}"
            return contacts_cache[user_id]
    except Exception as e:
        logger.warning(f"Не удалось получить имя пользователя {user_id}: {e}")

    return f"User {user_id}"


def format_message_for_telegram(sender_name: str, text: str, timestamp: datetime) -> str:
    """Форматирование сообщения для отправки в Telegram"""
    # Названия месяцев на русском
    months = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
              'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
    day = timestamp.day
    month = months[timestamp.month - 1]
    time_str = timestamp.strftime("%H:%M")
    # Формат: "29 октября, 12:53"
    date_str = f"{day} {month}, {time_str}"
    return f"<b>{sender_name}</b>\n{date_str}\n\n<blockquote>{text}</blockquote>"


def send_to_telegram(text: str, parse_mode: str = "HTML"):
    """Отправить сообщение в Telegram чат"""
    try:
        tg_bot.send_message(TG_CHAT_ID, text, parse_mode=parse_mode, message_thread_id=TG_TOPIC_ID)
        logger.info(f"Сообщение отправлено в Telegram")
    except Exception as e:
        logger.error(f"Ошибка отправки в Telegram: {e}")


def send_photo_to_telegram(photo_url: str, caption: str = ""):
    """Отправить фото в Telegram"""
    try:
        tg_bot.send_photo(TG_CHAT_ID, photo_url, caption=caption, parse_mode="HTML", message_thread_id=TG_TOPIC_ID)
        logger.info(f"Фото отправлено в Telegram")
    except Exception as e:
        logger.error(f"Ошибка отправки фото в Telegram: {e}")


def send_document_to_telegram(doc_bytes: bytes, filename: str, caption: str = ""):
    """Отправить документ в Telegram"""
    try:
        tg_bot.send_document(TG_CHAT_ID, doc_bytes, caption=caption,
                            visible_file_name=filename, parse_mode="HTML", message_thread_id=TG_TOPIC_ID)
        logger.info(f"Документ {filename} отправлен в Telegram")
    except Exception as e:
        logger.error(f"Ошибка отправки документа в Telegram: {e}")


def process_attachments(attachments: list, sender_name: str, timestamp: datetime):
    """Обработка вложений сообщения"""
    time_str = timestamp.strftime("%d.%m.%Y %H:%M:%S")
    header = f"📎 <b>{sender_name}</b>\n🕐 {time_str}\n"

    for attach in attachments:
        attach_type = attach.get("type", "")

        if attach_type == "photo":
            # Получаем URL самого большого размера фото
            photo = attach.get("photo", {})
            sizes = photo.get("sizes", [])
            if sizes:
                # Сортируем по размеру и берём самый большой
                largest = max(sizes, key=lambda x: x.get("width", 0) * x.get("height", 0))
                photo_url = largest.get("url", "")
                if photo_url:
                    send_photo_to_telegram(photo_url, header + "Фото")

        elif attach_type == "doc":
            doc = attach.get("doc", {})
            doc_url = doc.get("url", "")
            doc_title = doc.get("title", "document")
            if doc_url:
                try:
                    import requests
                    response = requests.get(doc_url)
                    if response.status_code == 200:
                        send_document_to_telegram(response.content, doc_title, header)
                except Exception as e:
                    logger.error(f"Ошибка загрузки документа: {e}")

        elif attach_type == "video":
            video = attach.get("video", {})
            video_title = video.get("title", "Видео")
            send_to_telegram(header + f"🎬 Видео: {video_title}")

        elif attach_type == "audio_message":
            send_to_telegram(header + "🎤 Голосовое сообщение")

        elif attach_type == "sticker":
            sticker = attach.get("sticker", {})
            # Пытаемся получить изображение стикера
            images = sticker.get("images", [])
            if images:
                sticker_url = images[-1].get("url", "")
                if sticker_url:
                    send_photo_to_telegram(sticker_url, header + "Стикер")


def on_max_event(event: dict):
    """Обработчик событий от MAX"""
    try:
        opcode = event.get("opcode")

        # opcode 128 - новое сообщение
        if opcode != 128:
            return

        payload = event.get("payload", {})
        message = payload.get("message", {})

        # Извлекаем данные из правильных полей MAX API
        # chatId отрицательный, берём абсолютное значение
        chat_id = abs(payload.get("chatId", 0))
        from_id = message.get("sender", 0)
        text = message.get("text", "")
        msg_time = message.get("time", 0)  # время в миллисекундах

        # Проверяем, что сообщение из нужного чата
        if chat_id != MAX_CHAT_ID:
            return

        # Получаем имя отправителя
        sender_name = get_user_name(from_id)

        # Фильтрация по ID пользователя
        if TARGET_USER_ID:
            try:
                target_id = int(TARGET_USER_ID)
                if from_id != target_id:
                    return
            except ValueError:
                logger.warning(f"Некорректный TARGET_USER_ID: {TARGET_USER_ID}")

        # Фильтрация по имени пользователя (частичное совпадение)
        if TARGET_USER_NAME and TARGET_USER_NAME not in sender_name.lower():
            return

        # Время сообщения (конвертируем из миллисекунд)
        timestamp = datetime.fromtimestamp(msg_time / 1000) if msg_time else datetime.now()

        logger.info(f"Новое сообщение от {sender_name} (ID: {from_id}): {text[:50] if text else '[без текста]'}...")

        # Отправляем текст, если есть
        if text:
            formatted = format_message_for_telegram(sender_name, text, timestamp)
            send_to_telegram(formatted)

        # Обрабатываем вложения
        attachments = message.get("attachments", [])
        if attachments:
            process_attachments(attachments, sender_name, timestamp)

        # Обрабатываем пересланные сообщения
        fwd_messages = message.get("fwd_messages", [])
        for fwd in fwd_messages:
            fwd_from = fwd.get("from_id", 0)
            fwd_text = fwd.get("text", "")
            fwd_name = get_user_name(fwd_from)
            if fwd_text:
                fwd_formatted = f"↩️ <b>Переслано от {fwd_name}</b>\n\n{fwd_text}"
                send_to_telegram(fwd_formatted)

    except Exception as e:
        logger.error(f"Ошибка обработки события: {e}")


def run_max_listener():
    """Запуск слушателя MAX"""
    global max_api

    while True:
        try:
            logger.info("Подключение к MAX...")
            max_api = MaxAPI(auth_token=MAX_AUTH_TOKEN, on_event=on_max_event)

            # Подписываемся на чат
            max_api.subscribe_to_chat(MAX_CHAT_ID)
            logger.info(f"Подписка на чат {MAX_CHAT_ID} активна")

            # Держим соединение
            while True:
                time.sleep(1)

        except Exception as e:
            logger.error(f"Ошибка MAX API: {e}")
            logger.info("Переподключение через 10 секунд...")
            time.sleep(10)


def main():
    """Главная функция"""
    logger.info("=" * 50)
    logger.info("MAX -> Telegram Forwarder Bot")
    logger.info("=" * 50)

    validate_config()

    if TARGET_USER_ID:
        logger.info(f"Отслеживание сообщений от пользователя: {TARGET_USER_ID}")
    else:
        logger.info("Отслеживание всех сообщений в чате")

    logger.info(f"MAX чат: {MAX_CHAT_ID}")
    logger.info(f"Telegram чат: {TG_CHAT_ID}")
    if TG_TOPIC_ID:
        logger.info(f"Telegram топик: {TG_TOPIC_ID}")
    logger.info("=" * 50)

    # Запускаем слушатель MAX в отдельном потоке
    max_thread = threading.Thread(target=run_max_listener, daemon=True)
    max_thread.start()

    logger.info("Бот запущен. Ожидание сообщений...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Остановка бота...")


if __name__ == "__main__":
    main()
