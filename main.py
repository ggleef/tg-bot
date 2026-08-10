"""
Telegram-бот точечной модерации.

Возможности:
  /restrict_links   (ответом на сообщение пользователя) — запретить этому
                     пользователю отправлять ссылки в чате.
  /unrestrict_links (ответом на сообщение пользователя) — снять запрет.
  /slowmode <сек>   (ответом на сообщение пользователя) — персональный
                     slow mode: следующее сообщение раньше чем через N секунд
                     после предыдущего будет удалено.
  /unslowmode       (ответом на сообщение пользователя) — снять slow mode.
  /status           (ответом на сообщение пользователя) — показать текущие
                     ограничения этого пользователя.

Работает в группах / супергруппах (в том числе в группе обсуждений,
привязанной к каналу) — писать в сам канал могут только админы, поэтому
модерация обычных подписчиков там не нужна.

Требования к боту:
  - должен быть добавлен в чат как администратор
  - должен иметь право "Удаление сообщений" (Delete messages)
"""

import asyncio
import logging
import os
import re
import sqlite3
import time
from contextlib import closing

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.filters import Command
from aiogram.types import Message
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = os.getenv("DB_PATH", "moderation.db")
WARNING_TTL = 5  # секунд, через сколько удалять предупреждения бота

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("mod_bot")

router = Router()

URL_RE = re.compile(
    r"(https?://|www\.|t\.me/|telegram\.me/)\S+",
    re.IGNORECASE,
)


# --------------------------------------------------------------------------
# База данных
# --------------------------------------------------------------------------

def db_init() -> None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS restrictions (
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                no_links INTEGER NOT NULL DEFAULT 0,
                slow_mode_seconds INTEGER NOT NULL DEFAULT 0,
                last_message_ts REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (chat_id, user_id)
            )
            """
        )
        conn.commit()


def get_restriction(chat_id: int, user_id: int) -> sqlite3.Row | None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            "SELECT * FROM restrictions WHERE chat_id=? AND user_id=?",
            (chat_id, user_id),
        )
        return cur.fetchone()


def upsert_restriction(chat_id: int, user_id: int, **fields) -> None:
    existing = get_restriction(chat_id, user_id)
    with closing(sqlite3.connect(DB_PATH)) as conn:
        if existing:
            sets = ", ".join(f"{k}=?" for k in fields)
            conn.execute(
                f"UPDATE restrictions SET {sets} WHERE chat_id=? AND user_id=?",
                (*fields.values(), chat_id, user_id),
            )
        else:
            cols = ["chat_id", "user_id", *fields.keys()]
            placeholders = ", ".join("?" for _ in cols)
            conn.execute(
                f"INSERT INTO restrictions ({', '.join(cols)}) VALUES ({placeholders})",
                (chat_id, user_id, *fields.values()),
            )
        conn.commit()


def update_last_ts(chat_id: int, user_id: int, ts: float) -> None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute(
            "UPDATE restrictions SET last_message_ts=? WHERE chat_id=? AND user_id=?",
            (ts, chat_id, user_id),
        )
        conn.commit()


# --------------------------------------------------------------------------
# Вспомогательные функции
# --------------------------------------------------------------------------

async def is_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except Exception:
        return False
    return member.status in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR)


def message_has_link(message: Message) -> bool:
    text = message.text or message.caption or ""
    if URL_RE.search(text):
        return True
    entities = message.entities or message.caption_entities or []
    for ent in entities:
        if ent.type in ("url", "text_link", "mention"):
            return True
    return False


async def send_temp_warning(message: Message, text: str) -> None:
    try:
        warn = await message.answer(text)
        await asyncio.sleep(WARNING_TTL)
        await warn.delete()
    except Exception as e:
        log.warning("Не удалось отправить/удалить предупреждение: %s", e)


def extract_target(message: Message) -> tuple[int, str] | None:
    """Берёт цель модерации из сообщения, на которое ответили."""
    if not message.reply_to_message or not message.reply_to_message.from_user:
        return None
    user = message.reply_to_message.from_user
    return user.id, (user.full_name or str(user.id))


# --------------------------------------------------------------------------
# Команды администратора
# --------------------------------------------------------------------------

@router.message(Command("restrict_links"))
async def cmd_restrict_links(message: Message, bot: Bot):
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        return await message.reply("Эта команда доступна только администраторам.")
    target = extract_target(message)
    if not target:
        return await message.reply(
            "Ответьте этой командой на сообщение пользователя, "
            "которому нужно запретить отправку ссылок."
        )
    user_id, name = target
    upsert_restriction(message.chat.id, user_id, no_links=1)
    await message.reply(f"🔒 Пользователю {name} запрещено отправлять ссылки в этом чате.")


@router.message(Command("unrestrict_links"))
async def cmd_unrestrict_links(message: Message, bot: Bot):
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        return await message.reply("Эта команда доступна только администраторам.")
    target = extract_target(message)
    if not target:
        return await message.reply("Ответьте этой командой на сообщение нужного пользователя.")
    user_id, name = target
    upsert_restriction(message.chat.id, user_id, no_links=0)
    await message.reply(f"🔓 Запрет на ссылки для {name} снят.")


@router.message(Command("slowmode"))
async def cmd_slowmode(message: Message, bot: Bot):
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        return await message.reply("Эта команда доступна только администраторам.")
    target = extract_target(message)
    if not target:
        return await message.reply(
            "Ответьте этой командой на сообщение пользователя.\n"
            "Пример: /slowmode 30 (ответом на его сообщение)"
        )
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.reply("Укажите интервал в секундах, например: /slowmode 30")
    seconds = int(parts[1])
    user_id, name = target
    upsert_restriction(message.chat.id, user_id, slow_mode_seconds=seconds, last_message_ts=0)
    await message.reply(f"🐢 Для {name} установлен персональный slow mode: {seconds} сек.")


@router.message(Command("unslowmode"))
async def cmd_unslowmode(message: Message, bot: Bot):
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        return await message.reply("Эта команда доступна только администраторам.")
    target = extract_target(message)
    if not target:
        return await message.reply("Ответьте этой командой на сообщение нужного пользователя.")
    user_id, name = target
    upsert_restriction(message.chat.id, user_id, slow_mode_seconds=0)
    await message.reply(f"⏱ Персональный slow mode для {name} снят.")


@router.message(Command("status"))
async def cmd_status(message: Message, bot: Bot):
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        return await message.reply("Эта команда доступна только администраторам.")
    target = extract_target(message)
    if not target:
        return await message.reply("Ответьте этой командой на сообщение нужного пользователя.")
    user_id, name = target
    row = get_restriction(message.chat.id, user_id)
    if not row:
        return await message.reply(f"У {name} нет активных ограничений.")
    lines = [f"Ограничения для {name}:"]
    lines.append(f"• Запрет ссылок: {'да' if row['no_links'] else 'нет'}")
    lines.append(
        f"• Slow mode: {row['slow_mode_seconds']} сек."
        if row["slow_mode_seconds"]
        else "• Slow mode: выключен"
    )
    await message.reply("\n".join(lines))


@router.message(Command("help", "start"))
async def cmd_help(message: Message):
    await message.reply(
        "Бот точечной модерации.\n\n"
        "Команды (ответом на сообщение нужного пользователя, только для админов):\n"
        "/restrict_links — запретить пользователю отправлять ссылки\n"
        "/unrestrict_links — снять запрет на ссылки\n"
        "/slowmode <секунды> — персональный slow mode для пользователя\n"
        "/unslowmode — снять персональный slow mode\n"
        "/status — показать текущие ограничения пользователя"
    )


# --------------------------------------------------------------------------
# Обработка обычных сообщений (модерация)
# --------------------------------------------------------------------------

@router.message(F.chat.type.in_({"group", "supergroup"}))
async def moderate(message: Message, bot: Bot):
    if not message.from_user or message.from_user.is_bot:
        return

    row = get_restriction(message.chat.id, message.from_user.id)
    if not row:
        return

    # Не трогаем админов, даже если на них когда-то было наложено правило.
    if await is_admin(bot, message.chat.id, message.from_user.id):
        return

    # Запрет на ссылки
    if row["no_links"] and message_has_link(message):
        try:
            await message.delete()
        except Exception as e:
            log.warning("Не удалось удалить сообщение со ссылкой: %s", e)
            return
        await send_temp_warning(
            message,
            f"🔗 {message.from_user.full_name}, отправка ссылок вам запрещена в этом чате.",
        )
        return

    # Персональный slow mode
    if row["slow_mode_seconds"]:
        now = time.time()
        elapsed = now - row["last_message_ts"]
        if elapsed < row["slow_mode_seconds"]:
            wait_left = int(row["slow_mode_seconds"] - elapsed)
            try:
                await message.delete()
            except Exception as e:
                log.warning("Не удалось удалить сообщение (slow mode): %s", e)
                return
            await send_temp_warning(
                message,
                f"⏱ {message.from_user.full_name}, подождите ещё {wait_left} сек. "
                f"перед следующим сообщением.",
            )
            return
        update_last_ts(message.chat.id, message.from_user.id, now)


# --------------------------------------------------------------------------
# Точка входа
# --------------------------------------------------------------------------

async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN. Создайте .env на основе .env.example.")
    db_init()
    bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
    dp = Dispatcher()
    dp.include_router(router)
    log.info("Бот запущен, ожидаю сообщения...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
