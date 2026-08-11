"""
Telegram-бот точечной модерации.

Возможности:
  /restrict_links    (ответом на сообщение пользователя) — запретить этому
                      пользователю отправлять ссылки в чате.
  /unrestrict_links  (ответом на сообщение пользователя) — снять запрет.
  /slowmode (сек)    (ответом на сообщение пользователя) — персональный
                      slow mode: следующее сообщение раньше чем через N секунд
                      после предыдущего будет удалено.
  /unslowmode        (ответом на сообщение пользователя) — снять slow mode.

  Антифлуд (лимит: 5 сообщений за 10 секунд), два уровня:

  На ВЕСЬ чат (без ответа на сообщение, просто команда в чат):
    /antiflood_all_delete       — лишние сообщения удаляются у всех
    /antiflood_all_mute (мин)   — при превышении лимита временный мут всем
                                   (по умолчанию 5 минут)
    /antiflood_all_off          — отключить общий антифлуд

  На ОДНОГО конкретного пользователя (ответом на его сообщение),
  имеет приоритет над общей настройкой чата:
    /antiflood_delete            — лишние сообщения этого юзера удаляются
    /antiflood_mute (мин)        — при превышении лимита юзер получает мут
                                    (по умолчанию 5 минут)
    /antiflood_off                — отключить персональный антифлуд

  /status  (ответом на сообщение пользователя) — показать текущие
           персональные ограничения этого пользователя.

Автоматика:
  - При вступлении нового участника в группу бот присылает приветствие
    и просит написать игровой ник.

Работает в группах / супергруппах (в том числе в группе обсуждений,
привязанной к каналу) — писать в сам канал могут только админы, поэтому
модерация обычных подписчиков там не нужна.

Требования к боту:
  - должен быть добавлен в чат как администратор
  - должен иметь право "Удаление сообщений" (Delete messages)
  - для режимов антифлуда с мутом также нужно право
    "Блокировка участников" (Restrict members / Ban users)
"""

import asyncio
import logging
import os
import re
import sqlite3
import time
from collections import defaultdict, deque
from contextlib import closing
from datetime import datetime, timedelta, timezone
from html import escape as html_escape

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ChatMemberStatus
from aiogram.filters import Command
from aiogram.types import ChatPermissions, Message
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = os.getenv("DB_PATH", "moderation.db")
WARNING_TTL = 5  # секунд, через сколько удалять предупреждения бота

FLOOD_LIMIT = 5    # сообщений
FLOOD_WINDOW = 10  # секунд
DEFAULT_MUTE_MINUTES = 5

# Временный (не персистентный) трекер сообщений для антифлуда.
# При перезапуске бота счётчики обнуляются — это нормально.
_flood_tracker: dict[tuple[int, int], deque] = defaultdict(deque)

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
                flood_mode TEXT NOT NULL DEFAULT 'off',
                flood_mute_minutes INTEGER NOT NULL DEFAULT 5,
                PRIMARY KEY (chat_id, user_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_settings (
                chat_id INTEGER PRIMARY KEY,
                flood_mode TEXT NOT NULL DEFAULT 'off',
                flood_mute_minutes INTEGER NOT NULL DEFAULT 5
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


def get_chat_settings(chat_id: int) -> sqlite3.Row | None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            "SELECT * FROM chat_settings WHERE chat_id=?",
            (chat_id,),
        )
        return cur.fetchone()


def upsert_chat_settings(chat_id: int, **fields) -> None:
    existing = get_chat_settings(chat_id)
    with closing(sqlite3.connect(DB_PATH)) as conn:
        if existing:
            sets = ", ".join(f"{k}=?" for k in fields)
            conn.execute(
                f"UPDATE chat_settings SET {sets} WHERE chat_id=?",
                (*fields.values(), chat_id),
            )
        else:
            cols = ["chat_id", *fields.keys()]
            placeholders = ", ".join("?" for _ in cols)
            conn.execute(
                f"INSERT INTO chat_settings ({', '.join(cols)}) VALUES ({placeholders})",
                (chat_id, *fields.values()),
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


def parse_optional_minutes(message: Message, default: int) -> int:
    parts = message.text.split()
    if len(parts) >= 2 and parts[1].isdigit():
        return int(parts[1])
    return default


# --------------------------------------------------------------------------
# Команды администратора: персональные ограничения
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


# --------------------------------------------------------------------------
# Команды администратора: персональный антифлуд
# --------------------------------------------------------------------------

@router.message(Command("antiflood_delete"))
async def cmd_antiflood_delete(message: Message, bot: Bot):
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        return await message.reply("Эта команда доступна только администраторам.")
    target = extract_target(message)
    if not target:
        return await message.reply(
            "Ответьте этой командой на сообщение пользователя, "
            "для которого нужно включить персональный антифлуд."
        )
    user_id, name = target
    upsert_restriction(message.chat.id, user_id, flood_mode="delete")
    await message.reply(
        f"🚿 Персональный антифлуд для {name} включён (режим: удаление лишних "
        f"сообщений при более чем {FLOOD_LIMIT} сообщениях за {FLOOD_WINDOW} сек.)."
    )


@router.message(Command("antiflood_mute"))
async def cmd_antiflood_mute(message: Message, bot: Bot):
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        return await message.reply("Эта команда доступна только администраторам.")
    target = extract_target(message)
    if not target:
        return await message.reply(
            "Ответьте этой командой на сообщение пользователя.\n"
            "Пример: /antiflood_mute 5 (ответом на его сообщение)"
        )
    minutes = parse_optional_minutes(message, DEFAULT_MUTE_MINUTES)
    user_id, name = target
    upsert_restriction(message.chat.id, user_id, flood_mode="mute", flood_mute_minutes=minutes)
    await message.reply(
        f"🚿 Персональный антифлуд для {name} включён (режим: мут на {minutes} мин. "
        f"при более чем {FLOOD_LIMIT} сообщениях за {FLOOD_WINDOW} сек.)."
    )


@router.message(Command("antiflood_off"))
async def cmd_antiflood_off(message: Message, bot: Bot):
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        return await message.reply("Эта команда доступна только администраторам.")
    target = extract_target(message)
    if not target:
        return await message.reply("Ответьте этой командой на сообщение нужного пользователя.")
    user_id, name = target
    upsert_restriction(message.chat.id, user_id, flood_mode="off")
    await message.reply(f"🚿 Персональный антифлуд для {name} отключён.")


# --------------------------------------------------------------------------
# Команды администратора: антифлуд на весь чат
# --------------------------------------------------------------------------

@router.message(Command("antiflood_all_delete"))
async def cmd_antiflood_all_delete(message: Message, bot: Bot):
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        return await message.reply("Эта команда доступна только администраторам.")
    upsert_chat_settings(message.chat.id, flood_mode="delete")
    await message.reply(
        f"🚿 Антифлуд включён для всех участников чата (режим: удаление лишних "
        f"сообщений при более чем {FLOOD_LIMIT} сообщениях за {FLOOD_WINDOW} сек.)."
    )


@router.message(Command("antiflood_all_mute"))
async def cmd_antiflood_all_mute(message: Message, bot: Bot):
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        return await message.reply("Эта команда доступна только администраторам.")
    minutes = parse_optional_minutes(message, DEFAULT_MUTE_MINUTES)
    upsert_chat_settings(message.chat.id, flood_mode="mute", flood_mute_minutes=minutes)
    await message.reply(
        f"🚿 Антифлуд включён для всех участников чата (режим: мут на {minutes} мин. "
        f"при более чем {FLOOD_LIMIT} сообщениях за {FLOOD_WINDOW} сек.)."
    )


@router.message(Command("antiflood_all_off"))
async def cmd_antiflood_all_off(message: Message, bot: Bot):
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        return await message.reply("Эта команда доступна только администраторам.")
    upsert_chat_settings(message.chat.id, flood_mode="off")
    await message.reply("🚿 Общий антифлуд для чата отключён.")


@router.message(Command("status"))
async def cmd_status(message: Message, bot: Bot):
    if not await is_admin(bot, message.chat.id, message.from_user.id):
        return await message.reply("Эта команда доступна только администраторам.")
    target = extract_target(message)
    if not target:
        return await message.reply("Ответьте этой командой на сообщение нужного пользователя.")
    user_id, name = target
    row = get_restriction(message.chat.id, user_id)
    chat_settings = get_chat_settings(message.chat.id)
    lines = [f"Ограничения для {name}:"]
    if not row:
        lines.append("• Запрет ссылок: нет")
        lines.append("• Slow mode: выключен")
        lines.append("• Персональный антифлуд: выключен")
    else:
        lines.append(f"• Запрет ссылок: {'да' if row['no_links'] else 'нет'}")
        lines.append(
            f"• Slow mode: {row['slow_mode_seconds']} сек."
            if row["slow_mode_seconds"]
            else "• Slow mode: выключен"
        )
        if row["flood_mode"] and row["flood_mode"] != "off":
            mode_label = "удаление сообщений" if row["flood_mode"] == "delete" else f"мут на {row['flood_mute_minutes']} мин."
            lines.append(f"• Персональный антифлуд: включён ({mode_label})")
        else:
            lines.append("• Персональный антифлуд: выключен")
    if chat_settings and chat_settings["flood_mode"] != "off":
        mode_label = "удаление сообщений" if chat_settings["flood_mode"] == "delete" else f"мут на {chat_settings['flood_mute_minutes']} мин."
        lines.append(f"• Общий антифлуд чата: включён ({mode_label})")
    else:
        lines.append("• Общий антифлуд чата: выключен")
    await message.reply("\n".join(lines))


@router.message(Command("help", "start"))
async def cmd_help(message: Message):
    await message.reply(
        "Бот точечной модерации.\n\n"
        "Персональные команды (ответом на сообщение пользователя, только для админов):\n"
        "/restrict_links — запретить пользователю отправлять ссылки\n"
        "/unrestrict_links — снять запрет на ссылки\n"
        "/slowmode (секунды) — персональный slow mode\n"
        "/unslowmode — снять персональный slow mode\n"
        "/antiflood_delete — антифлуд для юзера: удалять лишние сообщения\n"
        "/antiflood_mute (минуты) — антифлуд для юзера: мут при превышении\n"
        "/antiflood_off — отключить персональный антифлуд\n"
        "/status — показать ограничения пользователя\n\n"
        "Команды на весь чат (без ответа на сообщение):\n"
        "/antiflood_all_delete — антифлуд для всех: удалять лишние сообщения\n"
        "/antiflood_all_mute (минуты) — антифлуд для всех: мут при превышении\n"
        "/antiflood_all_off — отключить общий антифлуд\n\n"
        f"Лимит антифлуда: более {FLOOD_LIMIT} сообщений за {FLOOD_WINDOW} сек.\n\n"
        "Автоматически: новым участникам бот присылает приветствие "
        "с просьбой назвать игровой ник."
    )


# --------------------------------------------------------------------------
# Обработка обычных сообщений (модерация)
# --------------------------------------------------------------------------

async def handle_flood(message: Message, bot: Bot, mode: str, minutes: int) -> None:
    chat_id = message.chat.id
    user_id = message.from_user.id
    key = (chat_id, user_id)
    now = time.time()
    dq = _flood_tracker[key]
    dq.append(now)
    while dq and now - dq[0] > FLOOD_WINDOW:
        dq.popleft()

    if len(dq) <= FLOOD_LIMIT:
        return

    if mode == "delete":
        try:
            await message.delete()
        except Exception as e:
            log.warning("Не удалось удалить сообщение (антифлуд): %s", e)
            return
        await send_temp_warning(
            message,
            f"🚿 {message.from_user.full_name}, слишком много сообщений подряд — "
            f"лишние удаляются.",
        )
    elif mode == "mute":
        try:
            await message.delete()
        except Exception as e:
            log.warning("Не удалось удалить сообщение (антифлуд/мут): %s", e)
        until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
        try:
            await bot.restrict_chat_member(
                chat_id,
                user_id,
                permissions=ChatPermissions(can_send_messages=False),
                until_date=until,
            )
        except Exception as e:
            log.warning("Не удалось замутить пользователя (антифлуд): %s", e)
            return
        await send_temp_warning(
            message,
            f"🚿 {message.from_user.full_name} получил мут на {minutes} мин. "
            f"за флуд (более {FLOOD_LIMIT} сообщений за {FLOOD_WINDOW} сек.).",
        )
        dq.clear()


# --------------------------------------------------------------------------
# Приветствие новых участников
# --------------------------------------------------------------------------

@router.message(F.new_chat_members)
async def welcome_new_members(message: Message):
    for user in message.new_chat_members:
        if user.is_bot:
            continue
        safe_name = html_escape(user.full_name)
        text = (
            f'👋 Добро пожаловать, <a href="tg://user?id={user.id}">{safe_name}</a>! '
            f"Напишите свой игровой ник."
        )
        await message.answer(text, parse_mode="HTML")


@router.message(F.chat.type.in_({"group", "supergroup"}))
async def moderate(message: Message, bot: Bot):
    if not message.from_user or message.from_user.is_bot:
        return

    # Не трогаем админов, даже если на них когда-то было наложено правило.
    if await is_admin(bot, message.chat.id, message.from_user.id):
        return

    row = get_restriction(message.chat.id, message.from_user.id)

    # Запрет на ссылки (персональный)
    if row and row["no_links"] and message_has_link(message):
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
    if row and row["slow_mode_seconds"]:
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

    # Антифлуд: персональная настройка имеет приоритет над общей настройкой чата.
    flood_mode = "off"
    flood_minutes = DEFAULT_MUTE_MINUTES
    if row and row["flood_mode"] and row["flood_mode"] != "off":
        flood_mode = row["flood_mode"]
        flood_minutes = row["flood_mute_minutes"] or DEFAULT_MUTE_MINUTES
    else:
        chat_settings = get_chat_settings(message.chat.id)
        if chat_settings and chat_settings["flood_mode"] != "off":
            flood_mode = chat_settings["flood_mode"]
            flood_minutes = chat_settings["flood_mute_minutes"] or DEFAULT_MUTE_MINUTES

    if flood_mode != "off":
        await handle_flood(message, bot, flood_mode, flood_minutes)


# --------------------------------------------------------------------------
# Точка входа
# --------------------------------------------------------------------------

async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN. Создайте .env на основе .env.example.")
    db_init()
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    log.info("Бот запущен, ожидаю сообщения...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
