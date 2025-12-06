# ============================================================
# bot.py — BPFAM TARANTO (ptb v21+)
# 2 bottoni interni (Menù / Contatti-Info) + "⬅️ Torna indietro"
# Testi da ENV (Render): WELCOME_TEXT, MENU_PAGE_TEXT, INFO_PAGE_TEXT
# Anti-condivisione: protect_content=True su tutti i messaggi inviati
# Utenti su SQLite + Admin avanzato (multi-admin) + Backup (auto+manuale)
# Restore, /utenti, /diag, broadcast, anti-flood, keep-alive (polling)
# ============================================================

import os
import csv
import sqlite3
import logging
import shutil
import zipfile
from io import BytesIO
from datetime import datetime, timezone, time as dtime, timedelta, date
from pathlib import Path
from collections import defaultdict

import asyncio as aio
import aiohttp

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    JobQueue,
    filters,
)
from telegram.error import RetryAfter, Forbidden, BadRequest, NetworkError

VERSION = "2btn-secure-restore-1.7-bpfarm-admin-fix"

# ===== LOGGING =====
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("bpfam-taranto-bot")

# ===== ENV / CONFIG =====
BOT_TOKEN = os.environ.get("BOT_TOKEN")  # <-- mettilo su Render (NON nel codice)

DB_FILE     = os.environ.get("DB_FILE", "./data/users.db")
BACKUP_DIR  = os.environ.get("BACKUP_DIR", "./backups")
BACKUP_TIME = os.environ.get("BACKUP_TIME", "03:00")        # UTC HH:MM
RENDER_URL  = os.environ.get("RENDER_URL")                  # opzionale, per keep-alive

PHOTO_URL   = os.environ.get(
    "PHOTO_URL",
    "https://i.postimg.cc/bv4ssL2t/2A3BDCFD-2D21-41BC-8BFA-9C5D238E5C3B.jpg",
)

# === Testi gestibili da Render ===
WELCOME_TEXT = os.environ.get(
    "WELCOME_TEXT",
    "🥇Benvenuti nel bot ufficiale di BPFAM-TARANTO🥇\nScegli un’opzione qui sotto."
)
MENU_PAGE_TEXT = os.environ.get(
    "MENU_PAGE_TEXT",
    "📖 *MENÙ — BPFAM TARANTO*\n"
    "Benvenuto nel menù interno del bot.\n\n"
    "• Voce A\n• Voce B\n• Voce C\n"
)
INFO_PAGE_TEXT = os.environ.get(
    "INFO_PAGE_TEXT",
    "📲 *CONTATTI & INFO — BPFAM TARANTO*\n"
    "Canali verificati e contatti ufficiali.\n\n"
    "Instagram: @bpfamofficial\n"
    "Canale Telegram: t.me/...\n"
    "Contatto diretto: @contattobpfam\n"
)

# ===== ADMIN (FIX: prende sia ADMIN_ID che ADMIN_IDS) =====
def build_admin_ids() -> set[int]:
    ids: set[int] = set()

    # ADMIN_ID singolo (vecchia versione)
    single = os.environ.get("ADMIN_ID", "").replace(" ", "")
    if single.isdigit():
        ids.add(int(single))

    # ADMIN_IDS lista
    multi_raw = os.environ.get("ADMIN_IDS", "")
    multi_raw = multi_raw.replace(" ", "").strip()
    if multi_raw:
        for part in multi_raw.split(","):
            if part.isdigit():
                ids.add(int(part))

    return ids

ADMIN_IDS = build_admin_ids()
logger.info("ADMIN_IDS caricati: %s", ADMIN_IDS)

def is_admin(uid: int | None) -> bool:
    return bool(uid) and uid in ADMIN_IDS

def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id if update.effective_user else None
        if not is_admin(uid):
            # logghiamo per debug
            logger.info("Comando admin negato a uid=%s per %s", uid, func.__name__)
            return
        return await func(update, context)
    return wrapper

# ===== DB =====
def init_db():
    Path(DB_FILE).parent.mkdir(parents=True, exist_ok=True)
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id     INTEGER PRIMARY KEY,
            username    TEXT,
            first_name  TEXT,
            last_name   TEXT,
            first_seen  TEXT,
            last_seen   TEXT
        )
    """)
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
            UPDATE users SET username=?, first_name=?, last_name=?, last_seen=?
            WHERE user_id=?
        """, (u.username, u.first_name, u.last_name, now, u.id))
    else:
        cur.execute("""
            INSERT INTO users(user_id, username, first_name, last_name, first_seen, last_seen)
            VALUES(?,?,?,?,?,?)
        """, (u.id, u.username, u.first_name, u.last_name, now, now))
    conn.commit()
    conn.close()

def get_all_users():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id, username, first_name, last_name, first_seen, last_seen FROM users ORDER BY first_seen ASC"
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

def count_users():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    n = cur.fetchone()[0]
    conn.close()
    return n

# ===== UTILS DB / BACKUP =====
def parse_backup_time(hhmm: str) -> dtime:
    try:
        hh, mm = hhmm.split(":")
        return dtime(hour=int(hh), minute=int(mm))
    except Exception:
        return dtime(hour=3, minute=0)

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
        try:
            conn.execute("SELECT 1")
            conn.close()
        except Exception as e:
            conn.close()
            return False, f"Query fallita: {e}"
        return True, "OK"
    except Exception as e:
        return False, f"Errore lettura: {e}"

def last_backup_file():
    p = Path(BACKUP_DIR)
    if not p.exists():
        return None
    files = sorted(p.glob("backup_*.db"), reverse=True)
    return files[0] if files else None

# --- KEYBOARDS ---
def kb_home() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("📖 MENÙ", callback_data="OPEN_MENU"),
        InlineKeyboardButton("📲 CONTATTI-INFO", callback_data="OPEN_INFO"),
    ]])

def kb_back() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Torna indietro", callback_data="BACK_HOME")]
    ])

# --- EDIT HELPER (no messaggi doppi) ---
async def edit_view(q_msg, text: str, markup: InlineKeyboardMarkup, parse_mode: str = "Markdown"):
    try:
        if getattr(q_msg, "photo", None):
            await q_msg.edit_caption(caption=text, reply_markup=markup, parse_mode=parse_mode)
        else:
            await q_msg.edit_text(text, reply_markup=markup, parse_mode=parse_mode, disable_web_page_preview=True)
    except Exception as e:
        logger.warning("edit_view fallita: %s", e)

# --- VISTE (schermate) ---
async def show_home_from_start(chat, context: ContextTypes.DEFAULT_TYPE):
    """Home iniziale usata da /start: invia FOTO + didascalia (protetta)."""
    try:
        await chat.send_photo(
            photo=PHOTO_URL,
            caption=WELCOME_TEXT,
            reply_markup=kb_home(),
            protect_content=True
        )
    except Exception as e:
        logger.warning("Foto non inviata (%s), invio solo testo.", e)
        await chat.send_message(
            WELCOME_TEXT,
            reply_markup=kb_home(),
            protect_content=True
        )

async def show_home_from_callback(q):
    """Home via callback (stesso messaggio, già protetto)."""
    await edit_view(q.message, WELCOME_TEXT, kb_home())

async def show_menu(q):
    await edit_view(q.message, MENU_PAGE_TEXT, kb_back())

async def show_info(q):
    await edit_view(q.message, INFO_PAGE_TEXT, kb_back())

# ===== HANDLERS USER =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    if user:
        upsert_user(user)
    if not chat:
        return
    await show_home_from_start(chat, context)

async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    data = (q.data or "").strip()

    if data == "OPEN_MENU":
        await show_menu(q)
    elif data == "OPEN_INFO":
        await show_info(q)
    elif data == "BACK_HOME":
        await show_home_from_callback(q)
    else:
        await q.answer("Comando non riconosciuto.", show_alert=True)
        return
    await q.answer()

# ===== ANTI-FLOOD =====
USER_MSG_COUNT = defaultdict(int)

async def flood_guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    uid = update.effective_user.id
    USER_MSG_COUNT[uid] += 1
    if USER_MSG_COUNT[uid] > 10:  # >10 msg in 10s
        try:
            await context.bot.send_message(uid, "⛔ Flood rilevato. Attendi 10 secondi.")
        except Exception:
            pass
        USER_MSG_COUNT[uid] = 0

async def reset_flood(context: ContextTypes.DEFAULT_TYPE):
    USER_MSG_COUNT.clear()

# ===== ADMIN COMMANDS =====
def _send_protected_text(chat, text):
    return chat.send_message(text, protect_content=True)

@admin_only
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    n = count_users()
    last = last_backup_file()
    last_name = last.name if last else "nessuno"
    msg = (
        f"✅ Online v{VERSION}\n"
        f"👥 Utenti salvati: {n}\n"
        f"💾 DB: {DB_FILE}\n"
        f"🗂️ Backup dir: {BACKUP_DIR}\n"
        f"📦 Ultimo backup: {last_name}\n"
        f"⏰ Backup auto (UTC): {BACKUP_TIME}"
    )
    await _send_protected_text(update.effective_chat, msg)

@admin_only
async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
    cur.execute("SELECT user_id, username, first_name, last_name, last_seen FROM users ORDER BY last_seen DESC LIMIT 50")
    rows = cur.fetchall(); conn.close()
    if not rows:
        await _send_protected_text(update.effective_chat, "Nessun utente registrato.")
        return
    lines = []
    for uid, un, fn, ln, ls in rows:
        tag = f"@{un}" if un else "-"
        name = " ".join([x for x in [fn, ln] if x]) or "-"
        ts = (ls or "")[:19].replace("T", " ")
        lines.append(f"• {uid} {tag} — {name} — {ts}Z")
    text = "Ultimi 50 utenti:\n" + "\n".join(lines)
    await _send_protected_text(update.effective_chat, text)

@admin_only
async def export_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = sqlite3.connect(DB_FILE); cur = conn.cursor()
    cur.execute("SELECT user_id, username, first_name, last_name, first_seen, last_seen FROM users")
    rows = cur.fetchall(); conn.close()

    buf = BytesIO()
    buf.write("user_id,username,first_name,last_name,first_seen,last_seen\n".encode())
    for r in rows:
        safe = ["" if v is None else str(v).replace(",", " ") for v in r]
        buf.write((",".join(safe) + "\n").encode())
    buf.seek(0)
    filename = f"users_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    await update.effective_chat.send_document(
        document=InputFile(buf, filename=filename),
        protect_content=True
    )

@admin_only
async def utenti_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Alias stile BPFARM: /utenti -> totale + CSV file su disco + invio."""
    try:
        users = get_all_users()
    except Exception as e:
        await update.message.reply_text(
            f"❌ Errore lettura utenti dal DB: {e}",
            protect_content=True,
        )
        return

    n = len(users)
    if n == 0:
        await update.message.reply_text(
            "👥 Nessun utente nel DB.",
            protect_content=True,
        )
        return

    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_path = Path(BACKUP_DIR) / f"users_{ts}.csv"

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
        await update.message.reply_text(
            f"❌ Errore creazione CSV: {e}",
            protect_content=True,
        )
        return

    await update.message.reply_text(
        f"👥 Utenti totali: {n}",
        protect_content=True,
    )
    try:
        with open(csv_path, "rb") as fh:
            await update.message.reply_document(
                document=InputFile(fh, filename=csv_path.name),
                protect_content=True,
            )
    except Exception as e:
        await update.message.reply_text(
            f"⚠️ CSV salvato ma non inviato: {e}",
            protect_content=True,
        )

@admin_only
async def diag(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ok, why = is_sqlite_db(DB_FILE)
    size = Path(DB_FILE).stat().st_size if Path(DB_FILE).exists() else 0
    rows = 0
    try:
        conn = sqlite3.connect(DB_FILE)
        has = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='users'"
        ).fetchone()[0]
        rows = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] if has else 0
        conn.close()
    except Exception:
        pass
    txt = (
        f"🔎 DIAG\n"
        f"DB_FILE: {DB_FILE}\n"
        f"BACKUP_DIR: {BACKUP_DIR}\n"
        f"Esiste: {'sì' if Path(DB_FILE).exists() else 'no'}\n"
        f"Valido: {'sì' if ok else 'no'} ({why})\n"
        f"Dimensione: {size} byte\n"
        f"Righe users: {rows}\n"
    )
    await update.message.reply_text(txt, protect_content=True)

# --- backup manuale (come BPFARM) ---
async def _do_backup(update: Update, context: ContextTypes.DEFAULT_TYPE, zip_only: bool = False):
    ok, why = is_sqlite_db(DB_FILE)
    if not ok:
        await update.message.reply_text(
            f"⚠️ DB non valido: {why}\nControlla disk/variabili. Backup annullato.",
            protect_content=True,
        )
        return
    try:
        Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        db_out = Path(BACKUP_DIR) / f"backup_{stamp}.db"
        zip_out = Path(BACKUP_DIR) / f"backup_{stamp}.zip"

        shutil.copy2(DB_FILE, db_out)
        with zipfile.ZipFile(zip_out, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.write(db_out, arcname=db_out.name)

        if not zip_only:
            try:
                with open(db_out, "rb") as fh:
                    await update.message.reply_document(
                        document=InputFile(fh, filename=db_out.name),
                        caption=f"✅ Backup .db: {db_out.name}",
                        protect_content=False,
                    )
            except Exception as e:
                await update.message.reply_text(
                    f"⚠️ Impossibile inviare il .db: {e}",
                    protect_content=True,
                )

        try:
            with open(zip_out, "rb") as fh:
                await update.message.reply_document(
                    document=InputFile(fh, filename=zip_out.name),
                    caption=f"✅ Backup ZIP: {zip_out.name}",
                    protect_content=False,
                )
        except Exception as e:
            await update.message.reply_text(
                f"⚠️ Impossibile inviare lo ZIP: {e}",
                protect_content=True,
            )

    except Exception as e:
        await update.message.reply_text(f"❌ Errore backup: {e}", protect_content=True)

@admin_only
async def backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _do_backup(update, context, zip_only=False)

@admin_only
async def backup_zip_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _do_backup(update, context, zip_only=True)

@admin_only
async def backup_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _do_backup(update, context, zip_only=False)

# --- restore_db (senza merge, ma con controllo header) ---
@admin_only
async def restore_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg or not msg.reply_to_message or not msg.reply_to_message.document:
        await update.effective_chat.send_message(
            "Per ripristinare: invia un file .db al bot e poi fai *rispondi* a quel file con /restore_db",
            parse_mode="Markdown",
            protect_content=True
        )
        return

    doc = msg.reply_to_message.document
    try:
        file = await doc.get_file()
        tmp_path = Path(BACKUP_DIR) / ("restore_tmp_" + doc.file_unique_id + ".db")
        Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
        await file.download_to_drive(custom_path=str(tmp_path))
    except Exception as e:
        await _send_protected_text(update.effective_chat, f"❌ Errore download file: {e}")
        return

    ok, why = is_sqlite_db(str(tmp_path))
    if not ok:
        await _send_protected_text(update.effective_chat, f"❌ Il file caricato non è un DB SQLite valido: {why}")
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return

    try:
        safety_copy = Path(BACKUP_DIR) / f"pre_restore_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.bak"
        if Path(DB_FILE).exists():
            shutil.copyfile(DB_FILE, safety_copy)
    except Exception as e:
        await _send_protected_text(update.effective_chat, f"❌ Errore copia di sicurezza: {e}")
        return

    try:
        Path(DB_FILE).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(tmp_path, DB_FILE)
        await _send_protected_text(update.effective_chat, "✅ Database ripristinato con successo. Usa /status per verificare.")
    except Exception as e:
        await _send_protected_text(update.effective_chat, f"❌ Errore ripristino DB: {e}")
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass

# --- help + broadcast ---
@admin_only
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        f"<b>🛡 Pannello Admin — v{VERSION}</b>\n\n"
        "/status — stato bot / utenti / backup\n"
        "/list — ultimi 50 utenti\n"
        "/export — CSV utenti\n"
        "/utenti — totale + CSV (stile BPFARM)\n"
        "/diag — diagnostica DB/storage\n"
        "/backup — backup .db + .zip\n"
        "/backup_zip — solo ZIP\n"
        "/restore_db — ripristina DB da file .db (reply)\n"
        "/broadcast <testo> — invia a tutti\n"
        "/broadcast (reply) — copia contenuto a tutti\n"
        "/broadcast_stop — interrompe il broadcast\n"
        "/id — mostra il tuo ID (anche non admin)"
    )
    await update.message.reply_text(
        msg, parse_mode="HTML", protect_content=True
    )

BCAST_SLEEP = 0.08
BCAST_PROGRESS_EVERY = 200

@admin_only
async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
                protect_content=True,
            )
            return
        text_preview = (
            (text_body[:120] + "…") if len(text_body) > 120 else text_body
        )

    sent = failed = blocked = 0
    start_msg = await m.reply_text(
        f"📣 Broadcast iniziato\nUtenti: {total}\nAnteprima: {text_preview}",
        protect_content=True,
    )

    for i, u in enumerate(users, start=1):
        if context.application.bot_data.get("broadcast_stop"):
            break
        chat_id = u["user_id"]
        try:
            if mode == "copy":
                await m.reply_to_message.copy(
                    chat_id=chat_id, protect_content=True
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
                        chat_id=chat_id, protect_content=True
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
                    f"📣 In corso… {sent}/{total} | Bloccati {blocked} | Errori {failed}",
                )
            except Exception:
                pass
        await aio.sleep(BCAST_SLEEP)

    stopped = context.application.bot_data.get("broadcast_stop", False)
    status_txt = "⏹️ Interrotto" if stopped else "✅ Completato"
    await start_msg.edit_text(
        f"{status_txt}\nTotali: {total}\nInviati: {sent}\nBloccati: {blocked}\nErrori: {failed}"
    )

@admin_only
async def broadcast_stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.application.bot_data["broadcast_stop"] = True
    await update.message.reply_text(
        "⏹️ Broadcast: verrà interrotto al prossimo step.",
        protect_content=True,
    )

# --- KEEP-ALIVE (opzionale) ---
async def keep_alive_job(context: ContextTypes.DEFAULT_TYPE):
    if not RENDER_URL:
        return
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(RENDER_URL) as r:
                if r.status == 200:
                    logger.info("Ping keep-alive OK ✅")
                else:
                    logger.warning(f"Ping keep-alive fallito: {r.status}")
    except Exception as e:
        logger.warning(f"Errore keep-alive: {e}")

# --- BACKUP AUTOMATICO NOTTE ---
async def backup_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out = Path(BACKUP_DIR) / f"backup_{stamp}.db"
        shutil.copy2(DB_FILE, out)

        now = datetime.now(timezone.utc)
        for f in Path(BACKUP_DIR).glob("backup_*.db"):
            try:
                ts = datetime.strptime("_".join(f.stem.split("_")[1:]), "%Y%m%d_%H%M%S")
                if (now - ts).days > 7:
                    f.unlink(missing_ok=True)
            except Exception:
                pass

        for admin_id in ADMIN_IDS:
            try:
                with open(out, "rb") as fh:
                    await context.bot.send_document(
                        chat_id=admin_id,
                        document=InputFile(fh, filename=out.name),
                        caption=f"✅ Backup completato: {out.name}",
                        protect_content=False,
                    )
            except Exception as e:
                try:
                    await context.bot.send_message(
                        admin_id, f"⚠️ Errore invio backup notturno: {e}"
                    )
                except Exception:
                    pass
    except Exception as e:
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id, f"❌ Errore backup notturno: {e}"
                )
            except Exception:
                pass

# --- /id (anche non admin) ---
async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    uid = update.effective_user.id
    await update.message.reply_text(
        f"Il tuo ID Telegram è: <code>{uid}</code>",
        parse_mode="HTML",
        protect_content=True,
    )

# --- Webhook guard (anti-conflict) ---
async def _post_init(app):
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
        logger.info("Webhook rimosso (guard) — polling sicuro.")
    except Exception as e:
        logger.warning("Impossibile rimuovere webhook: %s", e)

# ===== MAIN =====
def main():
    if not BOT_TOKEN:
        raise SystemExit("Errore: variabile d'ambiente BOT_TOKEN mancante.")
    if not ADMIN_IDS:
        logger.warning("Nessun admin configurato! (ADMIN_ID / ADMIN_IDS vuoti)")

    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).post_init(_post_init).build()

    if not getattr(app, "job_queue", None):
        jq = JobQueue()
        jq.set_application(app)
        app.job_queue = jq

    # User
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(CommandHandler("id", id_cmd))

    # Admin
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("list", list_users))
    app.add_handler(CommandHandler("export", export_users))
    app.add_handler(CommandHandler("utenti", utenti_cmd))
    app.add_handler(CommandHandler("diag", diag))
    app.add_handler(CommandHandler("backup", backup_cmd))
    app.add_handler(CommandHandler("backup_zip", backup_zip_cmd))
    app.add_handler(CommandHandler("backup_db", backup_db))
    app.add_handler(CommandHandler("restore_db", restore_db))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))
    app.add_handler(CommandHandler("broadcast_stop", broadcast_stop_cmd))

    # Anti-flood su messaggi normali
    app.add_handler(MessageHandler(~filters.COMMAND, flood_guard))
    # (utile per /restore_db)
    app.add_handler(MessageHandler(filters.Document.ALL, lambda *_: None))

    # Jobs
    bt = parse_backup_time(BACKUP_TIME)
    now = datetime.now(timezone.utc)
    first_run = datetime.combine(date.today(), bt, tzinfo=timezone.utc)
    if first_run <= now:
        first_run += timedelta(days=1)
    app.job_queue.run_repeating(backup_job, interval=86400, first=first_run, name="night-backup")
    app.job_queue.run_repeating(reset_flood, interval=10, first=10, name="reset-flood")
    app.job_queue.run_repeating(keep_alive_job, interval=600, first=60, name="keep-alive")

    logger.info("Bot avviato — v%s (polling, worker mode)", VERSION)
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()