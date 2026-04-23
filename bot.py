import asyncio
import logging
import os
from datetime import date
from io import BytesIO

import aiomysql
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    PreCheckoutQuery,
    Message,
    BufferedInputFile,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from PIL import Image, ImageDraw, ImageFont

from config import (
    BOT_TOKEN,
    BOT_USERNAME,
    MAX_FREE_MEMES,
    PREMIUM_PRICE_STARS,
    ADMIN_USERNAME,
    ADMIN_IDS,
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_DB,
    MYSQL_USER,
    MYSQL_PASSWORD,
    CHANNEL_USERNAME,
    CHANNEL_ID,
)

# --- Инициализация ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

TEMPLATES_DIR = "templates"
USER_TEMPLATES_DIR = "user_templates"
FONT_PATH = "font/impact.ttf"

os.makedirs(TEMPLATES_DIR, exist_ok=True)
os.makedirs(USER_TEMPLATES_DIR, exist_ok=True)

pool = None

async def create_db_pool():
    global pool
    pool = await aiomysql.create_pool(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DB,
        autocommit=True,
        minsize=1,
        maxsize=5,
    )

async def init_db():
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username VARCHAR(255),
                    premium TINYINT DEFAULT 0,
                    last_reset DATE,
                    memes_today INT DEFAULT 0
                )
                """
            )

async def get_user(user_id: int, username: str = None) -> dict:
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
            row = await cur.fetchone()
            if row is None:
                await cur.execute(
                    "INSERT INTO users (user_id, username, last_reset, memes_today) VALUES (%s, %s, %s, %s)",
                    (user_id, username, str(date.today()), 0),
                )
                return {
                    "user_id": user_id,
                    "username": username,
                    "premium": False,
                    "last_reset": str(date.today()),
                    "memes_today": 0,
                }
            current_username = row[1]
            if username and username != current_username:
                await cur.execute(
                    "UPDATE users SET username = %s WHERE user_id = %s",
                    (username, user_id),
                )
            return {
                "user_id": row[0],
                "username": username or row[1],
                "premium": bool(row[2]),
                "last_reset": str(row[3]),
                "memes_today": row[4],
            }

async def reset_daily_if_needed(user_id: int):
    user = await get_user(user_id)
    today = str(date.today())
    if user["last_reset"] != today:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE users SET memes_today = 0, last_reset = %s WHERE user_id = %s",
                    (today, user_id),
                )
        user["memes_today"] = 0
        user["last_reset"] = today
    return user

async def increment_meme_count(user_id: int):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE users SET memes_today = memes_today + 1 WHERE user_id = %s",
                (user_id,),
            )

async def set_premium(user_id: int, premium: bool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE users SET premium = %s WHERE user_id = %s",
                (int(premium), user_id),
            )

# --- Проверка подписки ---
async def check_subscription(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status not in ("left", "kicked")
    except Exception:
        return False

async def require_subscription(user_id: int) -> bool:
    if await check_subscription(user_id):
        return True
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 Подписаться на канал", url=f"https://t.me/{CHANNEL_USERNAME.lstrip('@')}")],
            [InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_sub")]
        ]
    )
    await bot.send_message(
        user_id,
        "🔔 Чтобы пользоваться ботом, нужно подписаться на наш канал.\n"
        "Подпишись и нажми кнопку «Проверить подписку».",
        reply_markup=keyboard,
    )
    return False

# --- FSM ---
class MemeCreation(StatesGroup):
    choosing_template = State()
    waiting_for_top_text = State()
    waiting_for_bottom_text = State()

# --- Клавиатуры ---
def main_keyboard():
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="🎨 Создать мем", callback_data="create_meme"),
        InlineKeyboardButton(text="💎 Купить премиум", callback_data="buy_premium"),
    )
    builder.row(
        InlineKeyboardButton(text="🖼 Мои шаблоны", callback_data="my_templates"),
        InlineKeyboardButton(text="ℹ️ Помощь", callback_data="help"),
    )
    builder.row(InlineKeyboardButton(text="🔔 Проверить подписку", callback_data="check_sub"))
    return builder.as_markup()

def templates_keyboard(page: int = 0):
    templates = sorted(os.listdir(TEMPLATES_DIR))
    per_page = 5
    total_pages = max(1, (len(templates) + per_page - 1) // per_page)
    page = page % total_pages
    start = page * per_page
    chunk = templates[start : start + per_page]

    builder = InlineKeyboardBuilder()
    for idx, tpl in enumerate(chunk, start=start):
        name = os.path.splitext(tpl)[0]
        builder.row(InlineKeyboardButton(text=name, callback_data=f"tpl_{idx}"))
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"page_{page-1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"page_{page+1}"))
    if nav_buttons:
        builder.row(*nav_buttons)
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="back_main"))
    return builder.as_markup()

async def check_free_limit_and_notify(user_id: int) -> bool:
    user = await reset_daily_if_needed(user_id)
    if user["premium"]:
        return True
    if user["memes_today"] < MAX_FREE_MEMES:
        return True
    await bot.send_message(
        user_id,
        f"❌ Лимит {MAX_FREE_MEMES} мема в день исчерпан.\n"
        f"Купите премиум за {PREMIUM_PRICE_STARS} ⭐, чтобы убрать все ограничения.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="💳 Купить премиум", callback_data="buy_premium")]
            ]
        ),
    )
    return False

# --- Генерация мема ---
async def generate_meme(template_path, top_text, bottom_text, is_premium, user_id):
    img = Image.open(template_path).convert("RGBA")
    draw = ImageDraw.Draw(img)
    w, h = img.size

    font_size = int(h * 0.12)
    try:
        font = ImageFont.truetype(FONT_PATH, size=font_size)
    except:
        font = ImageFont.load_default()

    def draw_text_with_outline(draw, pos, text, font, fill="white", outline="black", width=3):
        x, y = pos
        for dx in range(-width, width+1):
            for dy in range(-width, width+1):
                if dx != 0 or dy != 0:
                    draw.text((x + dx, y + dy), text, font=font, fill=outline)
        draw.text((x, y), text, font=font, fill=fill)

    def wrap_text(text, font, max_width):
        words = text.split()
        lines = []
        current_line = ""
        for word in words:
            test_line = current_line + " " + word if current_line else word
            bbox = draw.textbbox((0, 0), test_line, font=font)
            if bbox[2] - bbox[0] <= max_width:
                current_line = test_line
            else:
                if current_line:
                    lines.append(current_line)
                current_line = word
        if current_line:
            lines.append(current_line)
        return lines

    top = top_text.upper().strip() if top_text else ""
    bottom = bottom_text.upper().strip() if bottom_text else ""

    max_text_width = w * 0.9

    if top:
        lines = wrap_text(top, font, max_text_width)
        y_offset = int(h * 0.05)
        for line in lines:
            bbox = draw.textbbox((0, 0), line, font=font)
            tw = bbox[2] - bbox[0]
            x = (w - tw) / 2
            draw_text_with_outline(draw, (x, y_offset), line, font)
            y_offset += bbox[3] - bbox[1] + 5

    if bottom:
        lines = wrap_text(bottom, font, max_text_width)
        heights = [draw.textbbox((0,0), line, font=font)[3] for line in lines]
        total_h = sum(heights) + (len(lines)-1)*5
        y_start = h - total_h - int(h * 0.05)
        for i, line in enumerate(lines):
            bbox = draw.textbbox((0, 0), line, font=font)
            tw = bbox[2] - bbox[0]
            x = (w - tw) / 2
            draw_text_with_outline(draw, (x, y_start), line, font)
            y_start += heights[i] + 5

    watermark_text = BOT_USERNAME
    if not is_premium:
        wm_size = int(h * 0.2)
        try:
            wmfont = ImageFont.truetype(FONT_PATH, size=wm_size)
        except:
            wmfont = font
        watermark_layer = Image.new("RGBA", img.size, (255, 255, 255, 0))
        wmdraw = ImageDraw.Draw(watermark_layer)
        bbox = wmdraw.textbbox((0, 0), watermark_text, font=wmfont)
        twm, thm = bbox[2] - bbox[0], bbox[3] - bbox[1]
        x = (w - twm) / 2
        y = (h - thm) / 2
        outline_width = 4
        for dx in range(-outline_width, outline_width+1):
            for dy in range(-outline_width, outline_width+1):
                if dx != 0 or dy != 0:
                    wmdraw.text((x+dx, y+dy), watermark_text, font=wmfont, fill=(0, 0, 0, 255))
        wmdraw.text((x, y), watermark_text, font=wmfont, fill=(255, 255, 255, 180))
        img = Image.alpha_composite(img, watermark_layer)
    else:
        try:
            small_font = ImageFont.truetype(FONT_PATH, size=int(h * 0.04))
        except:
            small_font = font
        draw.text((w - 130, h - 25), f"Создано {watermark_text}", font=small_font, fill=(255, 255, 255, 200))

    output = BytesIO()
    img.convert("RGB").save(output, format="JPEG", quality=95)
    output.seek(0)
    return output

# --- Обработчики ---
def is_admin(user: types.User) -> bool:
    return (user.username == ADMIN_USERNAME) or (user.id in ADMIN_IDS)

def admin_only(func):
    async def wrapper(message: Message):
        if not is_admin(message.from_user):
            await message.answer("⛔ Нет доступа.")
            return
        await func(message)
    return wrapper

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await get_user(message.from_user.id, message.from_user.username)
    if not await require_subscription(message.from_user.id):
        return
    await message.answer(
        "🤖 Привет! Я — мемогенератор.\n\n"
        f"Бесплатно: {MAX_FREE_MEMES} мема/день с водяным знаком.\n"
        f"Премиум (всего {PREMIUM_PRICE_STARS} ⭐ навсегда): безлимит, без водяного знака, загрузка своих картинок.\n\n"
        "Жми «Создать мем» и начнём!",
        reply_markup=main_keyboard(),
    )

@dp.callback_query(F.data == "buy_premium")
async def buy_premium(callback: types.CallbackQuery):
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title="Премиум-доступ к Мемогенератору",
        description="Навсегда убирает лимиты, водяной знак и открывает загрузку своих картинок.",
        payload="premium_access",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(label="Премиум навсегда", amount=PREMIUM_PRICE_STARS)],
        max_tip_amount=0,
        suggested_tip_amounts=[],
        start_parameter="premium",
        need_name=False,
        need_phone_number=False,
        need_email=False,
        need_shipping_address=False,
        is_flexible=False,
    )
    await callback.answer()

@dp.pre_checkout_query(lambda q: True)
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)

@dp.message(F.successful_payment)
async def successful_payment(message: Message):
    user_id = message.from_user.id
    payload = message.successful_payment.invoice_payload
    if payload == "premium_access":
        await set_premium(user_id, True)
        await message.answer(
            "🎉 Спасибо за покупку! Премиум-доступ активирован навсегда.\n"
            "Теперь ты можешь создавать безлимитные мемы, загружать свои шаблоны и без водяного знака.",
            reply_markup=main_keyboard(),
        )
    else:
        await message.answer("✅ Платёж получен, но услуга не найдена. Напишите администратору.")

@dp.callback_query(F.data == "back_main")
async def back_to_main(callback: types.CallbackQuery):
    await callback.message.edit_text("Главное меню:", reply_markup=main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "create_meme")
async def show_templates(callback: types.CallbackQuery, state: FSMContext):
    if not await require_subscription(callback.from_user.id):
        await callback.answer()
        return
    await callback.message.edit_text("Выбери шаблон:", reply_markup=templates_keyboard())
    await state.set_state(MemeCreation.choosing_template)
    await callback.answer()

@dp.callback_query(F.data.startswith("page_"))
async def change_templates_page(callback: types.CallbackQuery):
    page = int(callback.data.split("_")[1])
    await callback.message.edit_reply_markup(reply_markup=templates_keyboard(page))
    await callback.answer()

@dp.callback_query(F.data.startswith("tpl_"))
async def template_selected(callback: types.CallbackQuery, state: FSMContext):
    try:
        idx = int(callback.data[4:])
    except ValueError:
        await callback.answer("Ошибка данных", show_alert=True)
        return

    templates = sorted(os.listdir(TEMPLATES_DIR))
    if idx < 0 or idx >= len(templates):
        await callback.answer("Шаблон не найден", show_alert=True)
        return

    tpl_filename = templates[idx]
    tpl_path = os.path.join(TEMPLATES_DIR, tpl_filename)
    await state.update_data(template_path=tpl_path)
    await callback.message.answer("✏️ Введи верхний текст (или `-` чтобы оставить пустым):")
    await state.set_state(MemeCreation.waiting_for_top_text)
    await callback.answer()

@dp.callback_query(F.data == "check_sub")
async def process_check_sub(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if await check_subscription(user_id):
        # Проверяем, не является ли сообщение пригласительным с кнопкой проверки подписки
        if callback.message.reply_markup and callback.message.reply_markup.inline_keyboard:
            first_row = callback.message.reply_markup.inline_keyboard[0]
            if first_row and first_row[0].callback_data == "check_sub":
                # Это пригласительное сообщение — удаляем его и показываем главное меню
                await callback.message.delete()
                await bot.send_message(user_id, "✅ Подписка подтверждена. Добро пожаловать!", reply_markup=main_keyboard())
                await callback.answer()
                return
        # В остальных случаях просто уведомление
        await callback.answer("✅ Подписка активна!", show_alert=False)
    else:
        await callback.answer("❌ Вы ещё не подписаны на канал.", show_alert=True)
    await callback.answer()

@dp.callback_query(F.data == "my_templates")
async def my_templates_list(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if not await require_subscription(user_id):
        await callback.answer()
        return
    user = await get_user(user_id, callback.from_user.username)
    if not user["premium"]:
        await callback.answer("Только для премиум-пользователей", show_alert=True)
        return
    user_dir = os.path.join(USER_TEMPLATES_DIR, str(user_id))
    os.makedirs(user_dir, exist_ok=True)
    files = os.listdir(user_dir)
    if not files:
        await callback.message.edit_text(
            "У вас пока нет своих шаблонов. Отправьте мне картинку командой /upload.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")]]
            ),
        )
        await callback.answer()
        return
    builder = InlineKeyboardBuilder()
    for f in files:
        builder.row(InlineKeyboardButton(text=f, callback_data=f"usertpl_{f}"))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data="back_main"))
    await callback.message.edit_text("🖼 Твои шаблоны:", reply_markup=builder.as_markup())
    await callback.answer()

@dp.callback_query(F.data.startswith("usertpl_"))
async def user_template_selected(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    user = await get_user(user_id, callback.from_user.username)
    if not user["premium"]:
        await callback.answer("Только для премиум", show_alert=True)
        return
    filename = callback.data[9:]
    tpl_path = os.path.join(USER_TEMPLATES_DIR, str(user_id), filename)
    if not os.path.exists(tpl_path):
        await callback.answer("Файл не найден", show_alert=True)
        return
    await state.update_data(template_path=tpl_path)
    await callback.message.answer("✏️ Введи верхний текст (или `-`):")
    await state.set_state(MemeCreation.waiting_for_top_text)
    await callback.answer()

@dp.callback_query(F.data == "help")
async def help_callback(callback: types.CallbackQuery):
    user = await get_user(callback.from_user.id, callback.from_user.username)
    text = (
        "🤖 Я — мемогенератор. Как пользоваться:\n"
        "1. Нажми «Создать мем».\n"
        "2. Выбери шаблон.\n"
        "3. Введи верхний и нижний текст (`-` чтобы оставить пустым).\n"
        "4. Получи готовый мем.\n\n"
        f"Бесплатно: {MAX_FREE_MEMES} мема в день с водяным знаком.\n"
        f"Премиум ({PREMIUM_PRICE_STARS} ⭐): безлимит, загрузка своих картинок, без водяного знака.\n\n"
        "Команды: /upload (для премиум) — загрузить свой шаблон.\n"
    )
    if is_admin(callback.from_user):
        text += (
            "\n🔧 Админские команды:\n"
            "/grant ID — выдать премиум по ID\n"
            "/grantuser @username — выдать премиум по нику\n"
            "/broadcast <текст> или reply с /broadcast — рассылка всем\n"
            "/testpayment — эмулировать платёж (для теста)\n"
            "/revoke ID — отозвать премиум\n"
            "/stats — статистика пользователей"
        )
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="back_main")]]
    ))
    await callback.answer()

@dp.message(Command("help"))
async def cmd_help(message: Message):
    user = await get_user(message.from_user.id, message.from_user.username)
    text = (
        "🤖 Я — мемогенератор. Как пользоваться:\n"
        "1. Нажми «Создать мем».\n"
        "2. Выбери шаблон.\n"
        "3. Введи верхний и нижний текст (`-` чтобы оставить пустым).\n"
        "4. Получи готовый мем.\n\n"
        f"Бесплатно: {MAX_FREE_MEMES} мема в день с водяным знаком.\n"
        f"Премиум ({PREMIUM_PRICE_STARS} ⭐): безлимит, загрузка своих картинок, без водяного знака.\n\n"
        "Команды: /upload (для премиум) — загрузить свой шаблон.\n"
    )
    if is_admin(message.from_user):
        text += (
            "\n🔧 Админские команды:\n"
            "/grant ID — выдать премиум по ID\n"
            "/grantuser @username — выдать премиум по нику\n"
            "/broadcast <текст> или reply с /broadcast — рассылка всем\n"
            "/testpayment — эмулировать платёж (для теста)\n"
            "/revoke ID — отозвать премиум\n"
            "/stats — статистика пользователей"
        )
    await message.answer(text)

@dp.message(Command("upload"))
async def cmd_upload(message: Message):
    if not await require_subscription(message.from_user.id):
        return
    user = await get_user(message.from_user.id, message.from_user.username)
    if not user["premium"]:
        await message.answer("Только для премиум. Купите доступ через кнопку в меню.")
        return
    await message.answer("Отправьте мне картинку (желательно квадратную).")

@dp.message(F.photo)
async def handle_photo_upload(message: Message):
    if not await require_subscription(message.from_user.id):
        return
    user = await get_user(message.from_user.id, message.from_user.username)
    if not user["premium"]:
        return
    photo = message.photo[-1]
    user_dir = os.path.join(USER_TEMPLATES_DIR, str(message.from_user.id))
    os.makedirs(user_dir, exist_ok=True)
    import uuid
    filename = f"{uuid.uuid4().hex}.jpg"
    file_path = os.path.join(user_dir, filename)
    await bot.download(photo, destination=file_path)
    await message.reply(f"✅ Шаблон сохранён как `{filename}`. Доступен в «Мои шаблоны».")

@dp.message(StateFilter(MemeCreation.waiting_for_top_text))
async def enter_top_text(message: Message, state: FSMContext):
    text = message.text.strip()
    if text == "-":
        text = ""
    await state.update_data(top_text=text)
    await message.answer("✏️ Теперь введи нижний текст (или `-`):")
    await state.set_state(MemeCreation.waiting_for_bottom_text)

@dp.message(StateFilter(MemeCreation.waiting_for_bottom_text))
async def enter_bottom_text(message: Message, state: FSMContext):
    if not await require_subscription(message.from_user.id):
        await state.clear()
        return
    text = message.text.strip()
    if text == "-":
        text = ""
    data = await state.get_data()
    top_text = data.get("top_text", "")
    template_path = data.get("template_path")
    user_id = message.from_user.id

    if not await check_free_limit_and_notify(user_id):
        await state.clear()
        return

    user = await get_user(user_id, message.from_user.username)
    is_premium = user["premium"]

    await message.answer("⏳ Генерирую мем...")
    try:
        meme_io = await generate_meme(
            template_path=template_path,
            top_text=top_text,
            bottom_text=text,
            is_premium=is_premium,
            user_id=user_id,
        )
    except Exception as e:
        logging.exception("Ошибка генерации")
        await message.answer(f"❌ Ошибка: {e}")
        await state.clear()
        return

    if not is_premium:
        await increment_meme_count(user_id)

    await message.answer_photo(
        BufferedInputFile(meme_io.read(), filename="meme.jpg"),
        caption="Готово!" if is_premium else "Мем с водяным знаком (лимит 2/день)",
    )
    await message.answer("Что дальше?", reply_markup=main_keyboard())
    await state.clear()

# --- Админские команды ---
@dp.message(Command("grant"))
@admin_only
async def grant_premium(message: Message):
    try:
        target_id = int(message.text.split()[1])
    except (IndexError, ValueError):
        await message.answer("Использование: /grant user_id")
        return
    await set_premium(target_id, True)
    await bot.send_message(target_id, "🎉 Вам вручную выдан премиум!")
    await message.answer(f"✅ Премиум выдан пользователю {target_id}")

@dp.message(Command("grantuser"))
@admin_only
async def grant_user_by_username(message: Message):
    try:
        raw_username = message.text.split()[1]
        username = raw_username.lstrip("@")
    except (IndexError, ValueError):
        await message.answer("Использование: /grantuser @username")
        return
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT user_id FROM users WHERE username = %s", (username,))
            row = await cur.fetchone()
            if row is None:
                await message.answer(f"❌ Пользователь с ником @{username} не найден в базе.")
                return
            target_id = row[0]
    await set_premium(target_id, True)
    await bot.send_message(target_id, "🎉 Вам выдан премиум-доступ администратором!")
    await message.answer(f"✅ Премиум выдан @{username} (ID {target_id}).")

@dp.message(Command("testpayment"))
@admin_only
async def test_payment(message: Message):
    admin_id = message.from_user.id
    await set_premium(admin_id, True)
    await message.answer("🧪 Тестовый платёж выполнен! Премиум активирован для вас.")

@dp.message(Command("revoke"))
@admin_only
async def revoke_premium(message: Message):
    try:
        target_id = int(message.text.split()[1])
    except (IndexError, ValueError):
        await message.answer("Использование: /revoke user_id")
        return
    await set_premium(target_id, False)
    await bot.send_message(target_id, "❌ Премиум отозван.")
    await message.answer(f"Отозван премиум у {target_id}")

@dp.message(Command("stats"))
@admin_only
async def stats(message: Message):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*), SUM(premium) FROM users")
            total, premium_count = await cur.fetchone()
    await message.answer(f"👥 Пользователей: {total}, 💎 премиум: {premium_count or 0}")

@dp.message(Command("broadcast"))
@admin_only
async def broadcast(message: Message):
    if message.reply_to_message:
        target_msg = message.reply_to_message
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT user_id FROM users")
                rows = await cur.fetchall()
        user_ids = [row[0] for row in rows]
        total = len(user_ids)
        success = 0
        fail = 0
        progress_msg = await message.answer(f"⏳ Начинаю рассылку {total} пользователям...")
        for uid in user_ids:
            try:
                await bot.copy_message(
                    chat_id=uid,
                    from_chat_id=target_msg.chat.id,
                    message_id=target_msg.message_id,
                )
                success += 1
            except Exception as e:
                logging.warning(f"Не удалось отправить пользователю {uid}: {e}")
                fail += 1
            await asyncio.sleep(0.05)
        await progress_msg.edit_text(
            f"✅ Рассылка завершена.\n"
            f"📨 Успешно: {success}\n"
            f"❌ Ошибок: {fail}\n"
            f"👥 Всего пользователей: {total}"
        )
    else:
        text = message.text.split(maxsplit=1)
        if len(text) < 2:
            await message.answer("❌ Укажите текст рассылки.\nПример: `/broadcast Всем привет!`")
            return
        broadcast_text = text[1]
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT user_id FROM users")
                rows = await cur.fetchall()
        user_ids = [row[0] for row in rows]
        total = len(user_ids)
        success = 0
        fail = 0
        progress_msg = await message.answer(f"⏳ Рассылаю текст {total} пользователям...")
        for uid in user_ids:
            try:
                await bot.send_message(uid, broadcast_text)
                success += 1
            except Exception as e:
                logging.warning(f"Ошибка отправки {uid}: {e}")
                fail += 1
            await asyncio.sleep(0.05)
        await progress_msg.edit_text(
            f"✅ Рассылка завершена.\n"
            f"📨 Успешно: {success}\n"
            f"❌ Ошибок: {fail}\n"
            f"👥 Всего пользователей: {total}"
        )

async def main():
    await create_db_pool()
    await init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
