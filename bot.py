# =====================================================
# BPFAM POTENZA BOT – PTB v21+
# - Foto + benvenuto + 2 pulsanti (Menù / Info)
# - Anti-share (protect_content=True)
# - DB utenti SQLite + comandi admin
# - Backup automatico giornaliero + /backup_db
# - /restore_db (ripristino DB via reply a file .db)
# - Fix JobQueue su Render (NoneType -> creazione esplicita)
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
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters, JobQueue
)
import telegram.error as tgerr

VERSION = "bpfam-potenza-1.1"

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
BACKUP_DIR  = os.environ.get("BACKUP_DIR", "./backup")
BACKUP_TIME = os.environ.get("BACKUP_TIME", "03:00")  # HH:MM (UTC consigliato)

PHOTO_URL   = os.environ.get(
    "PHOTO_URL",
    "https://i.postimg.cc/5F5DFE41-C80D-4FC2-B4F6-D105844664B3.jpg"  # sostituisci con il tuo logo Potenza
)
WELCOME_TEXT = os.environ.get(
    "WELCOME_TEXT",
    "🥇BENVENUTO NEL BOT UFFICIALE DI POTENZA 🥇"
)
MENU_PAGE_TEXT = os.environ.get(
    "MENU_PAGE_TEXT",
    "📖 *Menù*\n\nQui potrai trovare le sezioni riservate ai membri BPFAM Potenza."
)
INFO_PAGE_TEXT = os.environ.get(
    "INFO_PAGE_TEXT",
    "📲 *Contatti Ufficiali*\n\nInstagram: @bpfamofficial\nTelegram: @contattobpfam"
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
    conn.commit(); conn.close()

def add_user(u):
    if not u: return
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""INSERT OR IGNORE INTO users
                   (user_id, username, first_name, last_name, joined)
                   VALUES (?, ?, ?, ?, ?)""",
                (u.id, u.username, u.first_name, u.last_name,
                 datetime.now(timezone.utc).isoformat()))
    conn.commit(); conn.close()

def count_users() -> int:
    conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    n = cur.fetchone()[0]; conn.close(); return n

def get_all_users():
    conn = sqlite3.connect(DB_FILE); conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT user_id,username,first_name,last_name,joined FROM users ORDER BY joined DESC")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close(); return rows

# ---------------- UTILS ----------------
def kb_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 Menù", callback_data="menu")],
        [InlineKeyboardButton("📲 Contatti-Info", callback_data="info")]
    ])

def kb_back() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Torna indietro", callback_data="home")]])

def parse_hhmm(txt: str) -> dtime:
    try:
        h, m = map(int, txt.strip().split(":")); return dtime(h, m)
    except Exception:
        return dtime(3, 0)

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
                document=InputFile(dest),
                caption=f"✅ Backup giornaliero — {dest.name}",
                protect_content=True
            )
        log.info(f"[AUTO BACKUP] {dest}")
    except Exception as e:
        log.exception(f"[AUTO BACKUP] errore: {e}")

# ---------------- HANDLERS PUBBLICI ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    add_user(update.effective_user)
    try:
        await update.effective_message.reply_photo(
            photo=PHOTO_URL,
            caption=WELCOME_TEXT,
            parse_mode="Markdown",
            reply_markup=kb_home(),
            protect_content=True,  # anti-share / anti-forward
        )
    except Exception as e:
        log.warning(f"Foto start fallita: {e}")
        await update.effective_message.reply_text(
            WELCOME_TEXT, parse_mode="Markdown",
            reply_markup=kb_home(),
            protect_content=True
        )

async def cb_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    await q.edit_message_text(
        text=MENU_PAGE_TEXT, parse_mode="Markdown",
        reply_markup=kb_back(), disable_web_page_preview=True,
        protect_content=True
    )

async def cb_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    await q.edit_message_text(
        text=INFO_PAGE_TEXT, parse_mode="Markdown",
        reply_markup=kb_back(), disable_web_page_preview=True,
        protect_content=True
    )

async def cb_home(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    # Riproponiamo la caption (se è foto) come testo semplice
    await q.edit_message_text(
        text=WELCOME_TEXT, parse_mode="Markdown",
        reply_markup=kb_home(), disable_web_page_preview=True,
        protect_content=True
    )

# Blocca tutto ciò che inviano i non-admin (anti-spam nei gruppi)
async def block_non_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user and user.id == ADMIN_ID:
        return
    chat = update.effective_chat
    if chat and chat.type in ("group", "supergroup"):
        try:
            await context.bot.delete_message(chat.id, update.effective_message.id)
        except tgerr.BadRequest:
            pass

# ---------------- ADMIN ----------------
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    await update.effective_message.reply_text(
        f"🤖 BPFAM POTENZA BOT\n"
        f"🔢 Versione: {VERSION}\n"
        f"👥 Utenti: {count_users()}\n"
        f"⏰ Ora: {datetime.now():%Y-%m-%d %H:%M:%S}",
        protect_content=True
    )

async def utenti_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    await update.effective_message.reply_text(f"👥 Utenti totali: {count_users()}")

async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    users = get_all_users()
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    csv_path = Path(BACKUP_DIR) / f"users_{datetime.now():%Y%m%d_%H%M%S}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["user_id","username","first_name","last_name","joined"])
        for u in users:
            w.writerow([u["user_id"], u["username"] or "", u["first_name"] or "", u["last_name"] or "", u["joined"] or ""])
    await update.effective_message.reply_document(
        document=InputFile(csv_path), caption=f"📤 Export utenti ({len(users)})",
        protect_content=True
    )

async def backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    dest = backup_now_file()
    await update.effective_message.reply_document(
        document=InputFile(dest), caption=f"💾 Backup creato — {dest.name}",
        protect_content=True
    )

async def restore_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    msg = update.effective_message
    if not msg.reply_to_message or not msg.reply_to_message.document:
        await msg.reply_text(
            "📦 Per ripristinare: invia un file `.db`, poi rispondi a quel messaggio con /restore_db",
            protect_content=True
        )
        return
    doc = msg.reply_to_message.document
    if not doc.file_name.endswith(".db"):
        await msg.reply_text("❌ Il file deve essere .db", protect_content=True); return

    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    tmp = Path(BACKUP_DIR) / f"restore_{doc.file_unique_id}.db"
    tg_file = await doc.get_file()
    await tg_file.download_to_drive(custom_path=str(tmp))

    # copia di sicurezza
    pre = Path(BACKUP_DIR) / f"pre_restore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
    if Path(DB_FILE).exists():
        shutil.copy2(DB_FILE, pre)

    shutil.copy2(tmp, DB_FILE)
    tmp.unlink(missing_ok=True)
    await msg.reply_text("✅ Database ripristinato con successo.", protect_content=True)

# ---------------- MAIN ----------------
def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN mancante")

    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # 🔧 Fix JobQueue NoneType su alcuni ambienti (es. Render)
    if not app.job_queue:
        jq = JobQueue()
        jq.set_application(app)
        app.job_queue = jq

    # Pubblici
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(~filters.COMMAND, block_non_admin))

    # Callback pulsanti
    app.add_handler(CallbackQueryHandler(cb_menu, pattern="^menu$"))
    app.add_handler(CallbackQueryHandler(cb_info, pattern="^info$"))
    app.add_handler(CallbackQueryHandler(cb_home, pattern="^home$"))

    # Admin
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("utenti", utenti_cmd))
    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(CommandHandler("backup_db", backup_cmd))
    app.add_handler(CommandHandler("restore_db", restore_db))

    # Job backup giornaliero
    hhmm = parse_hhmm(BACKUP_TIME)
    app.job_queue.run_daily(backup_job, time=hhmm, name="daily_backup")

    log.info(f"🚀 Avvio BPFAM POTENZA BOT — {VERSION}")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()