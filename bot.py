# =====================================================
# BPFAM POTENZA BOT – 2 bottoni + Admin style v3.6.5
# - 2 bottoni: MENÙ / CONTATTI-INFO + "⬅️ Torna indietro"
# - Testi da ENV: WELCOME_TEXT, MENU_PAGE_TEXT, INFO_PAGE_TEXT
# - Admin/DB/Backup/Restore/Broadcast come BPFARM v3.6.5-secure-full
# - FIX DB: aggiunge automaticamente first_seen / last_seen se mancano
# - Admin: legge sia ADMIN_ID che ADMIN_IDS
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

VERSION = "2btn-potenza-based-on-3.6.5"

# ---------------- LOG ----------------
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("bpfam-potenza-bot")

# ---------------- ENV / TEXT ----------------
def _txt(key, default=""):
    v = os.environ.get(key)
    if not v:
        return default
    v = v.replace("\\n", "\n")
    if v.startswith("file://"):
        try:
            with open(v[7:], "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            log.warning(f"Impossibile leggere {key}: {e}")
    return v

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

DB_FILE     = os.environ.get("DB_FILE", "./data/users.db")
BACKUP_DIR  = os.environ.get("BACKUP_DIR", "./backup")
BACKUP_TIME = os.environ.get("BACKUP_TIME", "03:00")   # UTC HH:MM
RENDER_URL  = os.environ.get("RENDER_URL")

PHOTO_URL = os.environ.get(
    "PHOTO_URL",
    "https://i.postimg.cc/bv4ssL2t/2A3BDCFD-2D21-41BC-8BFA-9C5D238E5C3B.jpg",
)

WELCOME_TEXT = _txt(
    "WELCOME_TEXT",
    "🥇 BENVENUTO NEL BOT UFFICIALE DI POTENZA 🥇\nScegli un’opzione qui sotto."
)
MENU_PAGE_TEXT = _txt(
    "MENU_PAGE_TEXT",
    "📖 *MENÙ — BPFAM POTENZA*\n"
    "Benvenuto nel menù interno del bot.\n\n"
    "• Voce A\n• Voce B\n• Voce C\n"
)
INFO_PAGE_TEXT = _txt(
    "INFO_PAGE_TEXT",
    "📲 *CONTATTI & INFO — BPFAM POTENZA*\n"
    "Canali verificati e contatti ufficiali.\n\n"
    "Instagram: @bpfamofficial\n"
    "Canale Telegram: t.me/...\n"
    "Contatto diretto: @contattobpfam\n"
)

# ---------------- DB (schema con first_seen / last_seen) ----------------
def init_db():
    Path(DB_FILE).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        last_name TEXT,
        first_seen TEXT,
        last_seen TEXT
    )"""
    )
    conn.commit()
    conn.close()

def ensure_db_schema():
    """
    Garantisce che la tabella users abbia sempre le colonne first_seen e last_seen.
    Serve per i DB vecchi che non le avevano (FIX 'no such column: first_seen').
    """
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info('users')")
        cols = {row[1] for row in cur.fetchall()}  # row[1] = nome colonna

        if "first_seen" not in cols:
            try:
                cur.execute("ALTER TABLE users ADD COLUMN first_seen TEXT")
                log.info("Colonna first_seen aggiunta alla tabella users.")
            except sqlite3.OperationalError as e:
                log.warning(f"Impossibile aggiungere first_seen: {e}")

        if "last_seen" not in cols:
            try:
                cur.execute("ALTER TABLE users ADD COLUMN last_seen TEXT")
                log.info("Colonna last_seen aggiunta alla tabella users.")
            except sqlite3.OperationalError as e:
                log.warning(f"Impossibile aggiungere last_seen: {e}")

        conn.commit()
    except Exception as e:
        log.warning(f"Errore ensure_db_schema: {e}")
    finally:
        conn.close()

def upsert_user(u):
    if not u:
        return
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    cur.execute("SELECT 1 FROM users WHERE user_id=?", (u.id,))
    if cur.fetchone():
        cur.execute(
            """
            UPDATE users SET
                username=?,
                first_name=?,
                last_name=?,
                last_seen=?
            WHERE user_id=?
            """,
            (u.username, u.first_name, u.last_name, now, u.id),
        )
    else:
        cur.execute(
            """
            INSERT INTO users(user_id, username, first_name, last_name, first_seen, last_seen)
            VALUES (?,?,?,?,?,?)
            """,
            (u.id, u.username, u.first_name, u.last_name, now, now),
        )
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
    cur.execute(
        "SELECT user_id, username, first_name, last_name, first_seen, last_seen FROM users ORDER BY first_seen ASC"
    )
    out = [dict(r) for r in cur.fetchall()]
    conn.close()
    return out

# ---------------- ADMIN UTILS (ADMIN_ID + ADMIN_IDS) ----------------
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
log.info("ADMIN_IDS caricati: %s", ADMIN_IDS)

def is_admin(uid: int | None) -> bool:
    return bool(uid) and uid in ADMIN_IDS

# ---------------- UTILS VARI ----------------
def parse_hhmm(h):
    try:
        h, m = map(int, h.split(":"))
        return dtime(h, m)
    except Exception:
        return dtime(3, 0)

def next_backup_utc():
    t = parse_hhmm(BACKUP_TIME)
    now = datetime.now(timezone.utc)
    nxt = datetime.combine(date.today(), t, tzinfo=timezone.utc)
    return nxt if nxt > now else nxt + timedelta(days=1)

def last_backup_file():
    p = Path(BACKUP_DIR)
    if not p.exists():
        return None
    f = sorted(p.glob("*.db"), reverse=True)
    return f[0] if f else None

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

# ---------------- KEYBOARD / VISTE PUBBLICHE ----------------
def kb_home():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📖 MENÙ", callback_data="OPEN_MENU"),
                InlineKeyboardButton("📲 CONTATTI-INFO", callback_data="OPEN_INFO"),
            ]
        ]
    )

def kb_back():
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ Torna indietro", callback_data="BACK_HOME")]]
    )

async def edit_view(q_msg, text: str, markup: InlineKeyboardMarkup, parse_mode: str = "Markdown"):
    try:
        if getattr(q_msg, "photo", None):
            await q_msg.edit_caption(caption=text, reply_markup=markup, parse_mode=parse_mode)
        else:
            await q_msg.edit_text(
                text,
                reply_markup=markup,
                parse_mode=parse_mode,
                disable_web_page_preview=True
            )
    except Exception as e:
        log.warning(f"edit_view fallita: {e}")

async def show_home_from_start(chat, context: ContextTypes.DEFAULT_TYPE):
    try:
        await chat.send_photo(
            photo=PHOTO_URL,
            caption=WELCOME_TEXT,
            reply_markup=kb_home(),
            protect_content=True,
        )
    except Exception as e:
        log.warning(f"Foto non inviata ({e}), invio solo testo.")
        await chat.send_message(
            WELCOME_TEXT,
            reply_markup=kb_home(),
            protect_content=True,
        )

async def show_home_from_callback(q):
    await edit_view(q.message, WELCOME_TEXT, kb_home())

async def show_menu(q):
    await edit_view(q.message, MENU_PAGE_TEXT, kb_back())

async def show_info(q):
    await edit_view(q.message, INFO_PAGE_TEXT, kb_back())

# ---------------- HANDLER PUBBLICI ----------------
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

# ---------------- ANTI-FLOOD ----------------
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

# ---------------- ADMIN COMMANDS ----------------
def admin_only(update: Update) -> bool:
    return update.effective_user and is_admin(update.effective_user.id)

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not admin_only(update):
        return
    now = datetime.now(timezone.utc)
    nxt = next_backup_utc()
    last = last_backup_file()
    await update.message.reply_text(
        f"🔎 Stato bot v{VERSION}\nUTC {now:%H:%M}\nUtenti {count_users()}\nUltimo backup {last.name if last else 'nessuno'}\nProssimo {nxt:%H:%M}",
        protect_content=True,
    )

async def diag_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not admin_only(update):
        return
    ok, why = is_sqlite_db(DB_FILE)
    size = Path(DB_FILE).stat().st_size if Path(DB_FILE).exists() else 0
    rows = 0
    try:
        conn = sqlite3.connect(DB_FILE)
        has = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='users'"
        ).fetchone()[0]
        rows = (
            conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            if has
            else 0
        )
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

# --- /backup (db + zip)
async def backup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not admin_only(update):
        return
    ok, why = is_sqlite_db(DB_FILE)
    if not ok:
        await update.message.reply_text(
            f"⚠️ DB non valido: {why}\nControlla Disk/variabili. Backup annullato."
        )
        return
    try:
        Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        db_out = Path(BACKUP_DIR) / f"backup_{stamp}.db"
        zip_out = Path(BACKUP_DIR) / f"backup_{stamp}.zip"

        shutil.copy2(DB_FILE, db_out)
        with zipfile.ZipFile(
            zip_out, "w", compression=zipfile.ZIP_DEFLATED
        ) as z:
            z.write(db_out, arcname=db_out.name)

        # invio .db
        try:
            with open(db_out, "rb") as fh:
                await update.message.reply_document(
                    document=InputFile(fh, filename=db_out.name),
                    caption=f"✅ Backup .db: {db_out.name}",
                    protect_content=False,
                )
        except Exception as e:
            await update.message.reply_text(
                f"⚠️ Impossibile inviare il .db: {e}"
            )

        # invio .zip
        try:
            with open(zip_out, "rb") as fh:
                await update.message.reply_document(
                    document=InputFile(fh, filename=zip_out.name),
                    caption=f"✅ Backup ZIP: {zip_out.name}",
                    protect_content=False,
                )
        except Exception as e:
            await update.message.reply_text(
                f"⚠️ Impossibile inviare lo ZIP: {e}"
            )

    except Exception as e:
        await update.message.reply_text(f"❌ Errore backup: {e}")

# --- /backup_zip (solo zip)
async def backup_zip_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not admin_only(update):
        return
    ok, why = is_sqlite_db(DB_FILE)
    if not ok:
        await update.message.reply_text(
            f"⚠️ DB non valido: {why}\nControlla Disk/variabili. Backup annullato."
        )
        return
    try:
        Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        db_out = Path(BACKUP_DIR) / f"backup_{stamp}.db"
        zip_out = Path(BACKUP_DIR) / f"backup_{stamp}.zip"
        shutil.copy2(DB_FILE, db_out)
        with zipfile.ZipFile(
            zip_out, "w", compression=zipfile.ZIP_DEFLATED
        ) as z:
            z.write(db_out, arcname=db_out.name)
        with open(zip_out, "rb") as fh:
            await update.message.reply_document(
                document=InputFile(fh, filename=zip_out.name),
                caption=f"✅ Backup ZIP: {zip_out.name}",
                protect_content=False,
            )
    except Exception as e:
        await update.message.reply_text(f"❌ Errore backup_zip: {e}")

# --- backup notturno + rotazione 7 giorni
async def backup_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        out = Path(BACKUP_DIR) / f"backup_{stamp}.db"
        shutil.copy2(DB_FILE, out)

        now = datetime.now(timezone.utc)
        for f in Path(BACKUP_DIR).glob("backup_*.db"):
            try:
                ts = datetime.strptime(
                    "_".join(f.stem.split("_")[1:]), "%Y%m%d_%H%M%S"
                )
                if (now - ts).days > 7:
                    f.unlink(missing_ok=True)
            except Exception:
                pass

        if ADMIN_IDS:
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
                            admin_id,
                            f"⚠️ Errore invio backup notturno: {e}",
                        )
                    except Exception:
                        pass

    except Exception as e:
        if ADMIN_IDS:
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        admin_id, f"❌ Errore backup notturno: {e}"
                    )
                except Exception:
                    pass

# --- /restore_db (MERGE, supporta anche DB senza first_seen/last_seen)
async def restore_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not admin_only(update):
        return
    m = update.effective_message
    if (
        not m
        or not m.reply_to_message
        or not m.reply_to_message.document
    ):
        await update.message.reply_text(
            "📦 Rispondi a un file .db con /restore_db (meglio: il riquadro con 'Backup .db: ...')."
        )
        return

    d = m.reply_to_message.document
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    tmp = Path(BACKUP_DIR) / f"import_{d.file_unique_id}.db"  # forzo estensione .db

    tg_file = await d.get_file()
    await tg_file.download_to_drive(custom_path=str(tmp))

    ok_imp, why_imp = is_sqlite_db(str(tmp))
    if not ok_imp:
        await update.message.reply_text(
            f"❌ Il file caricato non è un DB SQLite valido: {why_imp}"
        )
        tmp.unlink(missing_ok=True)
        return

    try:
        main = sqlite3.connect(DB_FILE)
        imp = sqlite3.connect(tmp)

        main.execute(
            """CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name  TEXT,
            first_seen TEXT,
            last_seen  TEXT
        )"""
        )
        main.commit()

        cols_imp = {
            r[1] for r in imp.execute("PRAGMA table_info('users')").fetchall()
        }

        if "first_seen" in cols_imp and "last_seen" in cols_imp:
            rows = imp.execute(
                "SELECT user_id,username,first_name,last_name,first_seen,last_seen FROM users"
            ).fetchall()
        else:
            # DB vecchi senza colonne → metto now come first/last_seen
            now_iso = datetime.now(timezone.utc).isoformat()
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
            last_seen  = COALESCE(excluded.last_seen,  users.last_seen)
        """
        main.executemany(sql, rows)
        main.commit()

        after = main.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        await update.message.reply_text(
            f"✅ Merge completato.\n👥 Totale: {after} (+{after-before})",
            protect_content=True,
        )
    except Exception as e:
        await update.message.reply_text(
            f"❌ Errore merge DB: {e}", protect_content=True
        )
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

# --- /utenti
async def utenti_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not admin_only(update):
        return
    users = get_all_users()
    n = len(users)
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    csv_path = (
        Path(BACKUP_DIR)
        / f"users_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.csv"
    )
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["user_id", "username", "first_name", "last_name", "first_seen", "last_seen"])
        for u in users:
            w.writerow(
                [
                    u.get("user_id", ""),
                    u.get("username") or "",
                    u.get("first_name") or "",
                    u.get("last_name") or "",
                    u.get("first_seen") or "",
                    u.get("last_seen") or "",
                ]
            )
    await update.message.reply_text(
        f"👥 Utenti totali: {n}", protect_content=True
    )
    with open(csv_path, "rb") as fh:
        await update.message.reply_document(
            document=InputFile(fh, filename=csv_path.name),
            protect_content=True,
        )

# --- /help
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not admin_only(update):
        return
    msg = (
        f"<b>🛡 Pannello Admin — v{VERSION}</b>\n\n"
        "/status — stato bot / utenti / backup\n"
        "/diag — diagnostica DB/storage\n"
        "/backup — backup immediato (.db + .zip)\n"
        "/backup_zip — solo ZIP (iOS friendly)\n"
        "/restore_db — rispondi al riquadro 'Backup .db: ...'\n"
        "/utenti — totale e CSV degli utenti\n"
        "/broadcast <testo> — invia a tutti\n"
        "/broadcast (in reply) — copia contenuto a tutti\n"
        "/broadcast_stop — interrompe l'invio\n"
        "/id — mostra il tuo ID Telegram"
    )
    await update.message.reply_text(
        msg, parse_mode="HTML", protect_content=True
    )

# --- /id (anche non admin, ma utile per configurare)
async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user:
        return
    uid = update.effective_user.id
    await update.message.reply_text(
        f"Il tuo ID Telegram è: <code>{uid}</code>",
        parse_mode="HTML",
        protect_content=True,
    )

# --- /broadcast
BCAST_SLEEP = 0.08
BCAST_PROGRESS_EVERY = 200

async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not admin_only(update):
        return
    m = update.effective_message
    users = get_all_users()
    total = len(users)
    if total == 0:
        await m.reply_text("Nessun utente nel DB.")
        return

    context.application.bot_data["broadcast_stop"] = False

    text_body = None
    if m.reply_to_message:
        mode = "copy"
        text_preview = (
            m.reply_to_message.text
            or m.reply_to_message.caption
            or "(media)"
        )
    else:
        mode = "text"
        text_body = " ".join(context.args) if context.args else None
        if not text_body:
            await m.reply_text(
                "Uso: /broadcast <testo> oppure in reply a un contenuto /broadcast"
            )
            return
        text_preview = (
            (text_body[:120] + "…") if len(text_body) > 120 else text_body
        )

    sent = failed = blocked = 0
    start_msg = await m.reply_text(
        f"📣 Broadcast iniziato\nUtenti: {total}\nAnteprima: {text_preview}"
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
    if not admin_only(update):
        return
    context.application.bot_data["broadcast_stop"] = True
    await update.message.reply_text(
        "⏹️ Broadcast: verrà interrotto al prossimo step."
    )

# --- blocco messaggi in gruppi (non admin)
async def block_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if (
        update.effective_chat
        and update.effective_chat.type in ("group", "supergroup")
        and (not update.effective_user or not is_admin(update.effective_user.id))
    ):
        try:
            await context.bot.delete_message(
                update.effective_chat.id, update.effective_message.id
            )
        except Exception:
            pass

# --- keep-alive per Render
async def keep_alive_job(context: ContextTypes.DEFAULT_TYPE):
    if not RENDER_URL:
        return
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(RENDER_URL) as r:
                if r.status == 200:
                    log.info("Ping keep-alive OK ✅")
                else:
                    log.warning(f"Ping keep-alive fallito: {r.status}")
    except Exception as e:
        log.warning(f"Errore keep-alive: {e}")

# ---------------- MAIN ----------------
def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN mancante")
    init_db()
    ensure_db_schema()   # <<< FIX colonne mancanti sui DB vecchi

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # anti-conflict: rimuove webhook prima del polling
    try:
        aio.get_event_loop().run_until_complete(
            app.bot.delete_webhook(drop_pending_updates=True)
        )
    except Exception as e:
        log.warning(f"Webhook reset fallito: {e}")

    # Pubblici
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(MessageHandler(~filters.COMMAND & ~filters.StatusUpdate.ALL, flood_guard))
    # blocco messaggi in gruppi (non admin)
    app.add_handler(MessageHandler(filters.ALL & ~filters.StatusUpdate.ALL, block_all))

    # Admin
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("diag", diag_cmd))
    app.add_handler(CommandHandler("restore_db", restore_db))
    app.add_handler(CommandHandler("backup", backup_cmd))
    app.add_handler(CommandHandler("backup_zip", backup_zip_cmd))
    app.add_handler(CommandHandler("utenti", utenti_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))
    app.add_handler(CommandHandler("broadcast_stop", broadcast_stop_cmd))
    app.add_handler(CommandHandler("id", id_cmd))

    # Jobs
    hhmm = parse_hhmm(BACKUP_TIME)
    now = datetime.now(timezone.utc)
    first = datetime.combine(now.date(), hhmm, tzinfo=timezone.utc)
    if first <= now:
        first += timedelta(days=1)
    app.job_queue.run_repeating(backup_job, 86400, first=first)        # backup ogni 24h
    app.job_queue.run_repeating(reset_flood, 10)                       # reset anti-flood
    app.job_queue.run_repeating(keep_alive_job, 600, first=60)         # keep-alive 10 min

    log.info(f"🚀 BPFAM POTENZA BOT avviato — v{VERSION}")
    app.run_polling(
        drop_pending_updates=True, allowed_updates=Update.ALL_TYPES
    )

if __name__ == "__main__":
    main()