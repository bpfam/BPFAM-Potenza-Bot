# =====================================================
# BPFAM POTENZA BOT — FULL FIX ADMIN + DB + BACKUP
# =====================================================

import os, csv, shutil, logging, sqlite3, asyncio as aio, aiohttp, zipfile
from pathlib import Path
from datetime import datetime, timezone, timedelta, date, time as dtime
from collections import defaultdict
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, JobQueue, filters
)
from telegram.error import RetryAfter, Forbidden, BadRequest, NetworkError

VERSION = "POTENZA-FULL-FIX-ADMIN-DB-BACKUP"

# ---------------- LOG ----------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("bpfam-potenza")

# ---------------- ENV ----------------
BOT_TOKEN   = os.environ.get("BOT_TOKEN", "")
DB_FILE     = os.environ.get("DB_FILE", "./data/users.db")
BACKUP_DIR  = os.environ.get("BACKUP_DIR", "./backup")
BACKUP_TIME = os.environ.get("BACKUP_TIME", "03:00")
RENDER_URL  = os.environ.get("RENDER_URL")

PHOTO_URL = os.environ.get(
    "PHOTO_URL",
    "https://i.postimg.cc/bv4ssL2t/2A3BDCFD-2D21-41BC-8BFA-9C5D238E5C3B.jpg",
)

WELCOME_TEXT = os.environ.get(
    "WELCOME_TEXT",
    "🥇 BENVENUTO NEL BOT UFFICIALE DI POTENZA 🥇\nScegli un’opzione qui sotto."
)
MENU_PAGE_TEXT = os.environ.get(
    "MENU_PAGE_TEXT",
    "📖 *MENÙ — BPFAM POTENZA*\n• Voce A\n• Voce B\n• Voce C"
)
INFO_PAGE_TEXT = os.environ.get(
    "INFO_PAGE_TEXT",
    "📲 *CONTATTI & INFO — BPFAM POTENZA*"
)

# ---------------- ADMIN (SAFE MODE) ----------------
def build_admin_ids() -> set[int]:
    ids: set[int] = set()
    single = os.environ.get("ADMIN_ID", "").replace(" ", "")
    if single.isdigit():
        ids.add(int(single))
    raw = os.environ.get("ADMIN_IDS", "").replace(" ", "")
    if raw:
        for x in raw.split(","):
            if x.isdigit():
                ids.add(int(x))
    return ids

ADMIN_IDS = build_admin_ids()
log.info("ADMIN_IDS: %s", ADMIN_IDS)

def is_admin(uid: int | None) -> bool:
    # Se ADMIN_IDS è vuoto → tutti possono usare i comandi admin
    if not ADMIN_IDS:
        return True
    return bool(uid) and uid in ADMIN_IDS

# ---------------- DB ----------------
def init_db():
    Path(DB_FILE).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        first_seen TEXT,
        last_seen TEXT
    )""")
    conn.commit()
    conn.close()

def ensure_db_schema():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info('users')")
    cols = {row[1] for row in cur.fetchall()}
    if "first_seen" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN first_seen TEXT")
    if "last_seen" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN last_seen TEXT")
    conn.commit()
    conn.close()

def upsert_user(u):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    cur.execute("SELECT 1 FROM users WHERE user_id=?", (u.id,))
    if cur.fetchone():
        cur.execute("""
        UPDATE users SET username=?, first_name=?, last_name=?, last_seen=? WHERE user_id=?
        """, (u.username, u.first_name, u.last_name, now, u.id))
    else:
        cur.execute("""
        INSERT INTO users(user_id, username, first_name, last_name, first_seen, last_seen)
        VALUES(?,?,?,?,?,?)
        """, (u.id, u.username, u.first_name, u.last_name, now, now))
    conn.commit()
    conn.close()

def count_users():
    conn = sqlite3.connect(DB_FILE)
    n = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    conn.close()
    return n

def get_all_users():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM users ORDER BY first_seen ASC")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

def parse_hhmm(h):
    try:
        h, m = map(int, h.split(":"))
        return dtime(h, m)
    except:
        return dtime(3, 0)

def is_sqlite_db(path: str):
    p = Path(path)
    if not p.exists():
        return False, "Il file non esiste"
    try:
        with open(p, "rb") as f:
            header = f.read(16)
        if header != b"SQLite format 3\x00":
            return False, "Header SQLite mancante"
        conn = sqlite3.connect(path)
        conn.execute("SELECT 1")
        conn.close()
        return True, "OK"
    except Exception as e:
        return False, f"Errore lettura: {e}"

# ---------------- KEYBOARD ----------------
def kb_home():
    return InlineKeyboardMarkup([[ 
        InlineKeyboardButton("📖 MENÙ", callback_data="MENU"),
        InlineKeyboardButton("📲 CONTATTI", callback_data="INFO")
    ]])

def kb_back():
    return InlineKeyboardMarkup([[ 
        InlineKeyboardButton("⬅️ Indietro", callback_data="HOME")
    ]])

# ---------------- HANDLER PUBBLICI ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u:
        upsert_user(u)
    try:
        await update.message.reply_photo(
            PHOTO_URL,
            caption=WELCOME_TEXT,
            reply_markup=kb_home()
        )
    except Exception:
        await update.message.reply_text(
            WELCOME_TEXT,
            reply_markup=kb_home()
        )

async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if q.data == "MENU":
        await q.message.edit_text(MENU_PAGE_TEXT, reply_markup=kb_back(), parse_mode="Markdown")
    elif q.data == "INFO":
        await q.message.edit_text(INFO_PAGE_TEXT, reply_markup=kb_back(), parse_mode="Markdown")
    elif q.data == "HOME":
        await q.message.edit_text(WELCOME_TEXT, reply_markup=kb_home())

# ---------------- ADMIN COMMANDS ----------------
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text(
        f"✅ Online v{VERSION}\n👥 Utenti: {count_users()}\nDB: {DB_FILE}\nBackup dir: {BACKUP_DIR}"
    )

async def utenti_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    users = get_all_users()
    await update.message.reply_text(f"👥 Utenti totali: {len(users)}")

async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Il tuo ID è: <code>{update.effective_user.id}</code>", parse_mode="HTML")

async def backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    ok, why = is_sqlite_db(DB_FILE)
    if not ok:
        await update.message.reply_text(f"⚠️ DB non valido: {why}")
        return

    try:
        Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        db_out  = Path(BACKUP_DIR) / f"backup_{stamp}.db"
        zip_out = Path(BACKUP_DIR) / f"backup_{stamp}.zip"

        shutil.copy2(DB_FILE, db_out)

        with zipfile.ZipFile(zip_out, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.write(db_out, arcname=db_out.name)

        # invio .db
        try:
            with open(db_out, "rb") as fh:
                await update.message.reply_document(
                    document=InputFile(fh, filename=db_out.name),
                    caption=f"✅ Backup .db: {db_out.name}",
                )
        except Exception as e:
            await update.message.reply_text(f"⚠️ Impossibile inviare il .db: {e}")

        # invio .zip
        try:
            with open(zip_out, "rb") as fh:
                await update.message.reply_document(
                    document=InputFile(fh, filename=zip_out.name),
                    caption=f"✅ Backup ZIP: {zip_out.name}",
                )
        except Exception as e:
            await update.message.reply_text(f"⚠️ Impossibile inviare lo ZIP: {e}")

    except Exception as e:
        await update.message.reply_text(f"❌ Errore backup: {e}")

# ---------------- MAIN ----------------
def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN mancante")

    init_db()
    ensure_db_schema()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Pubblici
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(on_button))

    # Admin
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("utenti", utenti_cmd))
    app.add_handler(CommandHandler("id",     id_cmd))
    app.add_handler(CommandHandler("backup", backup_cmd))

    log.info("✅ BOT AVVIATO — %s", VERSION)
    app.run_polling()

if __name__ == "__main__":
    main()