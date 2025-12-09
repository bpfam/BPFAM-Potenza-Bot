# =====================================================
# APULIAN DEALER BOT — FULL v1.6 PROTECT + PIN AUTO
# - Menu + Info con bottone Indietro
# - /status, /utenti (CSV), /backup, /restore_db (MERGE), /broadcast
# - protect_content=True su tutti i contenuti
# - SOLO /backup è SBLOCCATO per il download
# - Messaggio fissato AUTOMATICO: "👥 Iscritti Apulian Dealer {totale}"
# =====================================================

import os, csv, shutil, logging, sqlite3, asyncio as aio, zipfile
from pathlib import Path
from datetime import datetime, timezone
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    ContextTypes
)
from telegram.error import RetryAfter, Forbidden, BadRequest, NetworkError

VERSION = "APULIAN-FULL-1.6-PROTECT-AUTO"

# ---------------- LOG ----------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("apulian-dealer")

# ---------------- ENV ----------------
BOT_TOKEN   = os.environ.get("BOT_TOKEN", "")
DB_FILE     = os.environ.get("DB_FILE", "/var/data/users.db")
BACKUP_DIR  = os.environ.get("BACKUP_DIR", "/var/data/backup")

PHOTO_URL = os.environ.get(
    "PHOTO_URL",
    "https://i.postimg.cc/bv4ssL2t/2A3BDCFD-2D21-41BC-8BFA-9C5D238E5C3B.jpg",
)

WELCOME_TEXT = os.environ.get(
    "WELCOME_TEXT",
    "🥇 BENVENUTO NEL BOT UFFICIALE DI APULIAN DEALER 🥇\nScegli un’opzione qui sotto."
)
MENU_PAGE_TEXT = os.environ.get(
    "MENU_PAGE_TEXT",
    "📖 MENÙ\n• Voce A\n• Voce B\n• Voce C"
)
INFO_PAGE_TEXT = os.environ.get(
    "INFO_PAGE_TEXT",
    "📲 CONTATTI & INFO — APULIAN DEALER"
)

# ---------------- ADMIN ----------------
def build_admin_ids() -> set[int]:
    ids: set[int] = set()
    raw = os.environ.get("ADMIN_IDS", "").replace(" ", "")
    if raw:
        for x in raw.split(","):
            if x.isdigit():
                ids.add(int(x))
    return ids

ADMIN_IDS = build_admin_ids()
log.info("ADMIN_IDS: %s", ADMIN_IDS)

def is_admin(uid: int | None) -> bool:
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

def upsert_user(u):
    if not u:
        return
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
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📖 MENÙ", callback_data="MENU"),
            InlineKeyboardButton("📲 CONTATTI", callback_data="INFO")
        ],
        [
            InlineKeyboardButton(
                "🎥 VETRINA",
                url="https://bpfam.github.io/Apulian-Dealer/index.html"
            )
        ]
    ])

def kb_back():
    return InlineKeyboardMarkup([[ 
        InlineKeyboardButton("⬅️ Indietro", callback_data="HOME")
    ]])

# ---------------- START + PIN AUTO ----------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    chat = update.effective_chat
    if u:
        upsert_user(u)

    try:
        await chat.send_photo(PHOTO_URL, protect_content=True)
    except Exception as e:
        log.warning(f"Errore invio foto: {e}")

    try:
        await chat.send_message(
            WELCOME_TEXT,
            reply_markup=kb_home(),
            protect_content=True
        )
    except Exception as e:
        log.warning(f"Errore invio welcome: {e}")

    try:
        total = count_users()
        stats_msg = await chat.send_message(
            f"👥 Iscritti Apulian Dealer {total}",
            protect_content=True
        )
        try:
            await context.bot.pin_chat_message(
                chat_id=chat.id,
                message_id=stats_msg.message_id,
                disable_notification=True
            )
        except Exception as e:
            log.warning(f"Errore pin messaggio stats: {e}")
    except Exception as e:
        log.warning(f"Errore invio stats: {e}")

# ---------------- BOTTONI ----------------
async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    if q.data == "MENU":
        await q.message.edit_text(MENU_PAGE_TEXT, reply_markup=kb_back())
    elif q.data == "INFO":
        await q.message.edit_text(INFO_PAGE_TEXT, reply_markup=kb_back())
    elif q.data == "HOME":
        await q.message.edit_text(WELCOME_TEXT, reply_markup=kb_home())

# ---------------- ADMIN COMANDI ----------------
async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text(
        f"✅ Online v{VERSION}\n👥 Utenti: {count_users()}\nDB: {DB_FILE}\nBackup dir: {BACKUP_DIR}",
        protect_content=True
    )

async def utenti_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    users = get_all_users()
    n = len(users)

    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_path = Path(BACKUP_DIR) / f"users_{stamp}.csv"

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["user_id", "username", "first_name", "last_name", "first_seen", "last_seen"])
        for u in users:
            w.writerow([
                u.get("user_id", ""),
                u.get("username") or "",
                u.get("first_name") or "",
                u.get("last_name") or "",
                u.get("first_seen") or "",
                u.get("last_seen") or "",
            ])

    await update.message.reply_text(f"👥 Utenti totali: {n}", protect_content=True)

    with open(csv_path, "rb") as fh:
        await update.message.reply_document(
            document=InputFile(fh, filename=csv_path.name),
            protect_content=True
        )

# ✅ BACKUP SBLOCCATO (scaricabile)
async def backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    ok, why = is_sqlite_db(DB_FILE)
    if not ok:
        await update.message.reply_text(f"⚠️ DB non valido: {why}")
        return

    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    db_out  = Path(BACKUP_DIR) / f"backup_{stamp}.db"

    shutil.copy2(DB_FILE, db_out)

    with open(db_out, "rb") as fh:
        await update.message.reply_document(
            document=InputFile(fh, filename=db_out.name),
            caption="✅ Backup pronto da scaricare"
        )

async def restore_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    msg = update.effective_message
    if not msg.reply_to_message or not msg.reply_to_message.document:
        await update.message.reply_text(
            "Per ripristinare: rispondi a un file .db con /restore_db",
            protect_content=True
        )
        return

    doc = msg.reply_to_message.document
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    tmp = Path(BACKUP_DIR) / f"restore_{doc.file_unique_id}.db"

    tg_file = await doc.get_file()
    await tg_file.download_to_drive(custom_path=str(tmp))

    ok, why = is_sqlite_db(str(tmp))
    if not ok:
        await update.message.reply_text(f"❌ Il file non è un DB SQLite valido: {why}", protect_content=True)
        tmp.unlink(missing_ok=True)
        return

    main = sqlite3.connect(DB_FILE)
    imp  = sqlite3.connect(tmp)

    rows = imp.execute(
        "SELECT user_id,username,first_name,last_name,first_seen,last_seen FROM users"
    ).fetchall()

    sql = """
    INSERT INTO users (user_id, username, first_name, last_name, first_seen, last_seen)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_id) DO UPDATE SET
        username   = excluded.username,
        first_name = excluded.first_name,
        last_name  = excluded.last_name,
        last_seen  = excluded.last_seen
    """
    main.executemany(sql, rows)
    main.commit()

    await update.message.reply_text("✅ Restore completato", protect_content=True)

    imp.close()
    main.close()
    tmp.unlink(missing_ok=True)

# ---------------- BROADCAST ----------------
async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    m = update.effective_message
    users = get_all_users()
    total = len(users)
    if total == 0:
        await m.reply_text("Nessun utente nel DB.", protect_content=True)
        return

    text_body = None
    mode = "text"
    if m.reply_to_message:
        mode = "copy"
        text_preview = (
            m.reply_to_message.text
            or m.reply_to_message.caption
            or "(media)"
        )
    else:
        text_body = " ".join(context.args) if context.args else None
        if not text_body:
            await m.reply_text(
                "Uso: /broadcast <testo> oppure in reply a un contenuto /broadcast",
                protect_content=True
            )
            return
        text_preview = (text_body[:120] + "…") if len(text_body) > 120 else text_body

    sent = blocked = failed = 0
    info_msg = await m.reply_text(
        f"📣 Broadcast iniziato\nUtenti: {total}\nAnteprima: {text_preview}",
        protect_content=True
    )

    for u in users:
        chat_id = u["user_id"]
        try:
            if mode == "copy" and m.reply_to_message:
                await m.reply_to_message.copy(chat_id=chat_id, protect_content=True)
            else:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=text_body,
                    protect_content=True
                )
            sent += 1
        except Forbidden:
            blocked += 1
        except RetryAfter as e:
            await aio.sleep(e.retry_after + 1)
            try:
                if mode == "copy" and m.reply_to_message:
                    await m.reply_to_message.copy(chat_id=chat_id, protect_content=True)
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=text_body,
                        protect_content=True
                    )
                sent += 1
            except Exception:
                failed += 1
        except (BadRequest, NetworkError, Exception):
            failed += 1

        await aio.sleep(0.05)

    await info_msg.edit_text(
        f"✅ Broadcast finito\nTotali: {total}\nInviati: {sent}\nBloccati: {blocked}\nErrori: {failed}",
        protect_content=True
    )

# ---------------- MAIN ----------------
def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN mancante")

    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(on_button))

    app.add_handler(CommandHandler("status",    status_cmd))
    app.add_handler(CommandHandler("utenti",    utenti_cmd))
    app.add_handler(CommandHandler("backup",    backup_cmd))
    app.add_handler(CommandHandler("restore_db", restore_db))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))

    log.info("✅ BOT AVVIATO — %s", VERSION)
    app.run_polling()

if __name__ == "__main__":
    main()