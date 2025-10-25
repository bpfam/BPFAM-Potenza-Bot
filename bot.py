# =====================================================
# BPFAM POTENZA BOT – PTB v21+
# - Polling stabile (anti-conflict, webhook guard)
# - Anti-share (protect_content=True)
# - Backup automatici + /backup_db /restore_db
# - DB utenti SQLite
# - Configurazione completa da ENV (Render)
# =====================================================

import os
import csv
import shutil
import logging
import sqlite3
from datetime import datetime, timezone, time as dtime
from pathlib import Path
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
import telegram.error as tgerr
import asyncio as aio

VERSION = "bpfam-potenza-1.0"

# ---------------- LOG ----------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("bpfam-potenza")

# ---------------- ENV ----------------
BOT_TOKEN   = os.environ.get("BOT_TOKEN", "")
ADMIN_ID    = int(os.environ.get("ADMIN_ID", "0") or "0")
DB_FILE     = os.environ.get("DB_FILE", "./data/users.db")
BACKUP_DIR  = os.environ.get("BACKUP_DIR", "./backups")
BACKUP_TIME = os.environ.get("BACKUP_TIME", "03:00")

PHOTO_URL   = os.environ.get(
    "PHOTO_URL",
    "https://i.postimg.cc/Y0XXX/cover-potenza-bpfam.jpg"  # <--- sostituisci con la tua
)
WELCOME_TEXT = os.environ.get(
    "WELCOME_TEXT",
    "🥇BENVENUTO NEL BOT UFFICIALE DI POTENZA 🥇"
)

MENU_PAGE_TEXT = os.environ.get(
    "MENU_PAGE_TEXT",
    "📖 *Menù*\n\nScopri i servizi e vantaggi riservati ai membri BPFAM Potenza."
)
INFO_PAGE_TEXT = os.environ.get(
    "INFO_PAGE_TEXT",
    "📲 *Contatti Ufficiali*\n\n💎 Instagram: @bpfamofficial\n💎 Telegram: @contattobpfam"
)

# ---------------- DB ----------------
def init_db():
    Path(DB_FILE).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            joined TEXT
        )
    """)
    conn.commit()
    conn.close()

def add_user(u):
    if not u:
        return
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""INSERT OR IGNORE INTO users
                   (user_id, username, first_name, last_name, joined)
                   VALUES (?, ?, ?, ?, ?)""",
                (u.id, u.username, u.first_name, u.last_name,
                 datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()

def count_users():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    n = cur.fetchone()[0]
    conn.close()
    return n

def get_all_users():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM users ORDER BY joined DESC")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

# ---------------- BACKUP ----------------
def backup_now_file() -> Path:
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = Path(BACKUP_DIR) / f"backup_{ts}.db"
    shutil.copy2(DB_FILE, dest)
    return dest

async def backup_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        dest = backup_now_file()
        if ADMIN_ID:
            await context.bot.send_document(
                chat_id=ADMIN_ID,
                document=InputFile(open(dest, "rb"), filename=dest.name),
                caption=f"✅ Backup giornaliero completato — {dest.name}",
                protect_content=True,
            )
        log.info(f"[AUTO BACKUP] Creato: {dest}")
    except Exception as e:
        log.error(f"[AUTO BACKUP] Errore: {e}")

def parse_backup_time(txt: str) -> dtime:
    try:
        h, m = map(int, txt.split(":"))
        return dtime(hour=h, minute=m)
    except Exception:
        return dtime(hour=3, minute=0)

# ---------------- HANDLERS ----------------
def kb_home():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 Menù", callback_data="menu")],
        [InlineKeyboardButton("📲 Info-Contatti", callback_data="info")]
    ])

def kb_back():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Torna indietro", callback_data="home")]])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    add_user(update.effective_user)
    try:
        await update.message.reply_photo(
            photo=PHOTO_URL,
            caption=WELCOME_TEXT,
            parse_mode="Markdown",
            reply_markup=kb_home(),
            protect_content=True,
        )
    except Exception as e:
        log.warning(f"Errore foto start: {e}")
        await update.message.reply_text(
            WELCOME_TEXT,
            parse_mode="Markdown",
            reply_markup=kb_home(),
            protect_content=True,
        )

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    await update.callback_query.edit_message_text(
        text=MENU_PAGE_TEXT,
        parse_mode="Markdown",
        reply_markup=kb_back(),
        disable_web_page_preview=True,
        protect_content=True,
    )

async def info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    await update.callback_query.edit_message_text(
        text=INFO_PAGE_TEXT,
        parse_mode="Markdown",
        reply_markup=kb_back(),
        disable_web_page_preview=True,
        protect_content=True,
    )

async def home(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    await update.callback_query.edit_message_caption(
        caption=WELCOME_TEXT,
        parse_mode="Markdown",
        reply_markup=kb_home(),
    )

async def block_non_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Blocca tutti i messaggi non autorizzati."""
    user = update.effective_user
    if not user or user.id == ADMIN_ID:
        return
    chat = update.effective_chat
    try:
        await context.bot.delete_message(chat.id, update.message.message_id)
    except Exception:
        pass

# ---------------- ADMIN ----------------
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(
        f"🤖 *BPFAM POTENZA BOT*\n"
        f"🧩 Versione: {VERSION}\n"
        f"👥 Utenti registrati: {count_users()}\n"
        f"🕒 Ultimo avvio: {datetime.now():%Y-%m-%d %H:%M:%S}",
        parse_mode="Markdown",
        protect_content=True,
    )

async def utenti(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(f"👥 Utenti totali: {count_users()}", protect_content=True)

async def export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    users = get_all_users()
    csv_path = Path(BACKUP_DIR) / f"users_{datetime.now():%Y%m%d_%H%M%S}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["user_id", "username", "first_name", "last_name", "joined"])
        for u in users:
            w.writerow([u["user_id"], u["username"], u["first_name"], u["last_name"], u["joined"]])
    await update.message.reply_document(
        document=InputFile(csv_path),
        caption=f"📤 Export utenti completato ({len(users)})",
        protect_content=True,
    )

async def backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    dest = backup_now_file()
    await update.message.reply_document(
        document=InputFile(dest),
        caption=f"💾 Backup creato — {dest.name}",
        protect_content=True,
    )

async def restore_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    msg = update.effective_message
    if not msg.reply_to_message or not msg.reply_to_message.document:
        await update.message.reply_text(
            "📦 *Per ripristinare:* inviami un file `.db`, poi rispondi a quel messaggio con /restore_db",
            parse_mode="Markdown",
            protect_content=True,
        )
        return

    doc = msg.reply_to_message.document
    if not doc.file_name.endswith(".db"):
        await update.message.reply_text("❌ Il file deve essere .db", protect_content=True)
        return

    tmp_path = Path(BACKUP_DIR) / f"restore_tmp_{doc.file_unique_id}.db"
    file = await doc.get_file()
    await file.download_to_drive(custom_path=str(tmp_path))

    safety_copy = Path(BACKUP_DIR) / f"pre_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
    if Path(DB_FILE).exists():
        shutil.copy2(DB_FILE, safety_copy)

    shutil.copy2(tmp_path, DB_FILE)
    await update.message.reply_text("✅ Database ripristinato con successo.", protect_content=True)
    tmp_path.unlink(missing_ok=True)

# ---------------- MAIN ----------------
def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN mancante")

    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Comandi pubblici
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(~filters.COMMAND, block_non_admin))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("utenti", utenti))
    app.add_handler(CommandHandler("export", export))
    app.add_handler(CommandHandler("backup_db", backup_cmd))
    app.add_handler(CommandHandler("restore_db", restore_db))

    # Callback
    from telegram.ext import CallbackQueryHandler
    app.add_handler(CallbackQueryHandler(menu, pattern="^menu$"))
    app.add_handler(CallbackQueryHandler(info, pattern="^info$"))
    app.add_handler(CallbackQueryHandler(home, pattern="^home$"))

    # Job Backup giornaliero
    hhmm = parse_backup_time(BACKUP_TIME)
    now = datetime.now(timezone.utc)
    first_run = datetime.combine(now.date(), hhmm, tzinfo=timezone.utc)
    if first_run <= now:
        first_run = first_run.replace(day=now.day + 1)
    app.job_queue.run_daily(backup_job, time=hhmm)

    log.info(f"🚀 Avvio BPFAM POTENZA BOT — {VERSION}")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
