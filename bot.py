# =====================================================
# BPFAM POTENZA BOT — FULL v1.3 PROTECT
# - Menu + Info con bottone Indietro
# - /status, /utenti (CSV), /backup, /restore_db (MERGE), /broadcast
# - Admin safe (se ADMIN_IDS vuoto => tutti admin)
# - protect_content=True su contenuti bot (no inoltro)
# =====================================================

import os, csv, shutil, logging, sqlite3, asyncio as aio, aiohttp, zipfile
from pathlib import Path
from datetime import datetime, timezone, timedelta, date, time as dtime
from collections import defaultdict
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)
from telegram.error import RetryAfter, Forbidden, BadRequest, NetworkError

VERSION = "POTENZA-FULL-1.3-PROTECT"

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
    "📖 MENÙ — BPFAM POTENZA\n• Voce A\n• Voce B\n• Voce C"
)
INFO_PAGE_TEXT = os.environ.get(
    "INFO_PAGE_TEXT",
    "📲 CONTATTI & INFO — BPFAM POTENZA"
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
    chat = update.effective_chat
    if u:
        upsert_user(u)

    # 1) foto protetta (non inoltrabile)
    try:
        await chat.send_photo(
            PHOTO_URL,
            protect_content=True
        )
    except Exception as e:
        log.warning(f"Errore invio foto start: {e}")

    # 2) messaggio di testo con bottoni, protetto
    try:
        await chat.send_message(
            WELCOME_TEXT,
            reply_markup=kb_home(),
            protect_content=True
        )
    except Exception as e:
        log.warning(f"Errore invio testo start: {e}")

async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    # il messaggio dei bottoni è già stato creato con protect_content=True,
    # quindi resta protetto anche quando viene editato.
    if q.data == "MENU":
        await q.message.edit_text(MENU_PAGE_TEXT, reply_markup=kb_back())
    elif q.data == "INFO":
        await q.message.edit_text(INFO_PAGE_TEXT, reply_markup=kb_back())
    elif q.data == "HOME":
        await q.message.edit_text(WELCOME_TEXT, reply_markup=kb_home())

# ---------------- ADMIN BASE ----------------
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

    try:
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
    except Exception as e:
        await update.message.reply_text(f"❌ Errore creazione CSV utenti: {e}", protect_content=True)
        return

    await update.message.reply_text(f"👥 Utenti totali: {n}", protect_content=True)

    try:
        with open(csv_path, "rb") as fh:
            await update.message.reply_document(
                document=InputFile(fh, filename=csv_path.name),
                caption=f"📂 Lista utenti esportata ({n} righe)",
                protect_content=True
            )
    except Exception as e:
        await update.message.reply_text(f"⚠️ CSV creato ma non inviato: {e}", protect_content=True)

async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"Il tuo ID è: <code>{update.effective_user.id}</code>",
        parse_mode="HTML",
        protect_content=True
    )

# ---------------- BACKUP ----------------
async def backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    ok, why = is_sqlite_db(DB_FILE)
    if not ok:
        await update.message.reply_text(f"⚠️ DB non valido: {why}", protect_content=True)
        return

    try:
        Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        db_out  = Path(BACKUP_DIR) / f"backup_{stamp}.db"
        zip_out = Path(BACKUP_DIR) / f"backup_{stamp}.zip"

        shutil.copy2(DB_FILE, db_out)

        with zipfile.ZipFile(zip_out, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.write(db_out, arcname=db_out.name)

        try:
            with open(db_out, "rb") as fh:
                await update.message.reply_document(
                    document=InputFile(fh, filename=db_out.name),
                    caption=f"✅ Backup .db: {db_out.name}",
                    protect_content=True
                )
        except Exception as e:
            await update.message.reply_text(f"⚠️ Impossibile inviare il .db: {e}", protect_content=True)

        try:
            with open(zip_out, "rb") as fh:
                await update.message.reply_document(
                    document=InputFile(fh, filename=zip_out.name),
                    caption=f"✅ Backup ZIP: {zip_out.name}",
                    protect_content=True
                )
        except Exception as e:
            await update.message.reply_text(f"⚠️ Impossibile inviare lo ZIP: {e}", protect_content=True)

    except Exception as e:
        await update.message.reply_text(f"❌ Errore backup: {e}", protect_content=True)

# ---------------- RESTORE_DB (MERGE) ----------------
async def restore_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    msg = update.effective_message
    if not msg.reply_to_message or not msg.reply_to_message.document:
        await update.message.reply_text(
            "Per usare /restore_db:\n"
            "1️⃣ Invia un file .db al bot\n"
            "2️⃣ Tieni premuto sul file ➜ *Rispondi*\n"
            "3️⃣ Scrivi /restore_db come risposta a QUEL file",
            parse_mode="Markdown",
            protect_content=True
        )
        return

    doc = msg.reply_to_message.document
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    tmp = Path(BACKUP_DIR) / f"restore_{doc.file_unique_id}.db"

    try:
        tg_file = await doc.get_file()
        await tg_file.download_to_drive(custom_path=str(tmp))
    except Exception as e:
        await update.message.reply_text(f"❌ Errore download file: {e}", protect_content=True)
        return

    ok, why = is_sqlite_db(str(tmp))
    if not ok:
        await update.message.reply_text(f"❌ Il file non è un DB SQLite valido: {why}", protect_content=True)
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        return

    try:
        main = sqlite3.connect(DB_FILE)
        imp  = sqlite3.connect(tmp)

        main.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            first_seen TEXT,
            last_seen TEXT
        )""")
        main.commit()

        cols_imp = {r[1] for r in imp.execute("PRAGMA table_info('users')").fetchall()}
        now_iso = datetime.now(timezone.utc).isoformat()

        if {"first_seen", "last_seen"}.issubset(cols_imp):
            rows = imp.execute(
                "SELECT user_id,username,first_name,last_name,first_seen,last_seen FROM users"
            ).fetchall()
        else:
            rows = [
                (uid, un, fn, ln, now_iso, now_iso)
                for (uid, un, fn, ln) in imp.execute(
                    "SELECT user_id,username,first_name,last_name FROM users"
                ).fetchall()
            ]

        before = main.execute("SELECT COUNT(*) FROM users").fetchone()[0]

        sql = """
        INSERT INTO users (user_id, username, first_name, last_name, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username   = COALESCE(excluded.username, users.username),
            first_name = COALESCE(excluded.first_name, users.first_name),
            last_name  = COALESCE(excluded.last_name,  users.last_name),
            first_seen = COALESCE(users.first_seen,   excluded.first_seen),
            last_seen  = COALESCE(excluded.last_seen, users.last_seen)
        """
        main.executemany(sql, rows)
        main.commit()

        after = main.execute("SELECT COUNT(*) FROM users").fetchone()[0]

        await update.message.reply_text(
            f"✅ Restore completato.\n"
            f"Utenti prima: {before}\n"
            f"Utenti dopo:  {after}\n"
            f"Aggiunti/aggiornati: {after-before}",
            protect_content=True
        )

    except Exception as e:
        await update.message.reply_text(f"❌ Errore restore: {e}", protect_content=True)
    finally:
        try:
            imp.close()
        except Exception:
            pass
        try:
            main.close()
        except Exception:
            pass
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

# ---------------- BROADCAST ----------------
BCAST_SLEEP = 0.08
BCAST_PROGRESS_EVERY = 200

async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    m = update.effective_message
    users = get_all_users()
    total = len(users)
    if total == 0:
        await m.reply_text("Nessun utente nel DB.", protect_content=True)
        return

    context.application.bot_data["broadcast_stop"] = False

    if m.reply_to_message:
        mode = "copy"
        text_preview = (
            m.reply_to_message.text
            or m.reply_to_message.caption
            or "(media)"
        )
        text_body = None
    else:
        mode = "text"
        text_body = " ".join(context.args) if context.args else None
        if not text_body:
            await m.reply_text(
                "Uso: /broadcast <testo> oppure in reply a un contenuto /broadcast",
                protect_content=True
            )
            return
        text_preview = (text_body[:120] + "…") if len(text_body) > 120 else text_body

    sent = failed = blocked = 0
    start_msg = await m.reply_text(
        f"📣 Broadcast iniziato\nUtenti: {total}\nAnteprima: {text_preview}",
        protect_content=True
    )

    for i, u in enumerate(users, start=1):
        if context.application.bot_data.get("broadcast_stop"):
            break
        chat_id = u["user_id"]
        try:
            if mode == "copy":
                await m.reply_to_message.copy(
                    chat_id=chat_id,
                    protect_content=True
                )
            else:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=text_body,
                    protect_content=True,
                    disable_web_page_preview=True,
                )
            sent += 1
        except Forbidden:
            blocked += 1
        except RetryAfter as e:
            await aio.sleep(e.retry_after + 1)
            try:
                if mode == "copy":
                    await m.reply_to_message.copy(
                        chat_id=chat_id,
                        protect_content=True
                    )
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=text_body,
                        protect_content=True,
                        disable_web_page_preview=True,
                    )
                sent += 1
            except Forbidden:
                blocked += 1
            except Exception:
                failed += 1
        except (BadRequest, NetworkError, Exception):
            failed += 1

        if i % BCAST_PROGRESS_EVERY == 0:
            try:
                await start_msg.edit_text(
                    f"📣 In corso… {sent}/{total} | Bloccati {blocked} | Errori {failed}"
                )
            except Exception:
                pass

        await aio.sleep(BCAST_SLEEP)

    stopped = context.application.bot_data.get("broadcast_stop", False)
    status = "⏹️ Interrotto" if stopped else "✅ Completato"
    await start_msg.edit_text(
        f"{status}\nTotali: {total}\nInviati: {sent}\nBloccati: {blocked}\nErrori: {failed}"
    )

async def broadcast_stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    context.application.bot_data["broadcast_stop"] = True
    await update.message.reply_text("⏹️ Broadcast: verrà interrotto al prossimo step.", protect_content=True)

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
    app.add_handler(CommandHandler("status",       status_cmd))
    app.add_handler(CommandHandler("utenti",       utenti_cmd))
    app.add_handler(CommandHandler("id",           id_cmd))
    app.add_handler(CommandHandler("backup",       backup_cmd))
    app.add_handler(CommandHandler("restore_db",   restore_db))
    app.add_handler(CommandHandler("broadcast",    broadcast_cmd))
    app.add_handler(CommandHandler("broadcast_stop", broadcast_stop_cmd))

    log.info("✅ BOT AVVIATO — %s", VERSION)
    app.run_polling()

if __name__ == "__main__":
    main()