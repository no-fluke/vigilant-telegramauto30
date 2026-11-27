"""
scheduler.py — Auto-Update Scheduler for AppX Bot
Built on main.py's core functions. Topic-wise supergroup threads.

Commands: /addauto, /myautos, /delauto, /runnow

Flow:
  /addauto  → user picks api_domain, login, course, subjects,
              what to track (lectures/pdfs/quizzes/tests), supergroup,
              time in IST.
            → Bot auto-creates one supergroup topic per APPX TOPIC
              (not per subject) inside the chosen supergroup.
            → Saves everything to MongoDB.

  Scheduler → fires every minute, checks which autos are due (IST HH:MM match).
            → For each topic thread: fetches latest lectures, skips already-seen IDs,
              downloads & sends new items using the EXACT same captions as manual flow.

  /myautos  → lists all configured autos.
  /delauto  → delete entire auto or specific subjects.
  /runnow   → trigger immediately.

Captions used (identical to run_lecture_loop in main.py):
  Video → [🎥] `NNN) Title [quality]`\n<blockquote>Batch: CourseName</blockquote>\nExtracted by ➤ Brand
  PDF   → [📕] `NNN) Title.pdf`\n<blockquote>Batch: CourseName</blockquote>\nExtracted by ➤ Brand
  Quiz  → 📝 `Title`\n`N questions`\n`i/total` | Batch: CourseName
  Test  → 📝 `TestTitle`\n`N questions`\nSeries: SeriesName | Brand
"""


import os
import re
import asyncio
import random
import logging
import tempfile
import subprocess
import time
import aiohttp
import requests

from datetime import datetime, timezone, timedelta
from pyromod import listen as _pyromod_listen  # patches Client with .listen()
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait
from pyrogram.handlers import MessageHandler, CallbackQueryHandler

from telethon import TelegramClient
from telethon.tl.types import InputPeerChannel, DocumentAttributeVideo
from telethon.errors import FloodWaitError

import devgagantools
from devgagantools import fast_upload, progress_bar_str

HTML = enums.ParseMode.HTML   # shorthand used throughout this file

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from db import db
from vars import CREDIT, BOT_TOKEN, API_ID, API_HASH

MAX_AUTOS_PER_USER = 100
IST = timezone(timedelta(hours=5, minutes=30))
logger = logging.getLogger(__name__)

# Tracks active /runnow tasks so they can be cancelled cleanly on restart
_active_runnow_tasks: dict = {}


def _m():
    import sys
    # When run as `python main.py`, the module is registered as '__main__',
    # not 'main'. Doing `import main` would re-execute main.py and crash.
    return sys.modules.get('main') or sys.modules['__main__']


# ── MongoDB helpers ──────────────────────────────────────────────────────────

def _aw():
    return db.db["auto_watch"]

def _ensure_indexes():
    try:
        _aw().create_index(
            [("user_id", 1), ("auto_id", 1)], unique=True,
            name="auto_watch_identity"
        )
    except Exception:
        pass

def _next_auto_id(user_id: int):
    used = {doc["auto_id"] for doc in _aw().find({"user_id": user_id}, {"auto_id": 1})}
    for i in range(1, MAX_AUTOS_PER_USER + 1):
        if i not in used:
            return i
    return None

def _get_autos(user_id: int) -> list:
    return list(_aw().find({"user_id": user_id}, {"_id": 0}).sort("auto_id", 1))

def _get_auto(user_id: int, auto_id: int):
    return _aw().find_one({"user_id": user_id, "auto_id": auto_id}, {"_id": 0})

def _save_auto(doc: dict):
    _aw().update_one(
        {"user_id": doc["user_id"], "auto_id": doc["auto_id"]},
        {"$set": doc},
        upsert=True
    )

def _delete_auto(user_id: int, auto_id: int):
    _aw().delete_one({"user_id": user_id, "auto_id": auto_id})

def _mark_seen(user_id: int, auto_id: int, subj_id: str, topic_id: str,
               field: str, item_id: str):
    """
    Atomically add item_id to the seen-IDs list for a specific topic.
    Called immediately after a successful send so a crash never causes re-download.
    field is one of: last_lecture_ids, last_pdf_ids, last_quiz_ids
    """
    try:
        _aw().update_one(
            {"user_id": user_id, "auto_id": auto_id},
            {"$addToSet": {
                f"subjects.$[s].topics.$[t].{field}": item_id
            }},
            array_filters=[
                {"s.subject_id": subj_id},
                {"t.topic_id":   topic_id},
            ]
        )
    except Exception as e:
        logger.error(f"[Scheduler] _mark_seen failed uid={user_id} auto={auto_id} "
                     f"subj={subj_id} topic={topic_id} field={field} id={item_id}: {e}")

def _mark_done(user_id: int, auto_id: int, subj_id: str, topic_id: str,
               lec_id: str, ts: float):
    """Remember WHEN a lecture was first seen complete (starts its re-check window)."""
    try:
        _aw().update_one(
            {"user_id": user_id, "auto_id": auto_id},
            {"$set": {f"subjects.$[s].topics.$[t].lec_done_at.{lec_id}": ts}},
            array_filters=[{"s.subject_id": subj_id}, {"t.topic_id": topic_id}]
        )
    except Exception as e:
        logger.error(f"[Scheduler] _mark_done failed lec={lec_id}: {e}")

def _unmark_seen(user_id: int, auto_id: int, subj_id: str, topic_id: str,
                 field: str, item_id: str):
    try:
        _aw().update_one(
            {"user_id": user_id, "auto_id": auto_id},
            {"$pull": {f"subjects.$[s].topics.$[t].{field}": item_id}},
            array_filters=[{"s.subject_id": subj_id}, {"t.topic_id": topic_id}]
        )
    except Exception as e:
        logger.error(f"[Scheduler] _unmark_seen failed {field}={item_id}: {e}")

def _mark_test_seen_auto(user_id: int, auto_id: int, test_id: str):
    """Add test_id to the AUTO-level seen list (test series are per course, not per subject)."""
    try:
        _aw().update_one({"user_id": user_id, "auto_id": auto_id},
                         {"$addToSet": {"last_test_ids": test_id}})
    except Exception as e:
        logger.error(f"[Scheduler] _mark_test_seen_auto failed test={test_id}: {e}")

def _ts_key(series_id) -> str:
    return re.sub(r"[^0-9A-Za-z_-]", "_", str(series_id))

def _mark_test_seen(user_id: int, auto_id: int, subj_id: str, test_id: str):
    """Atomically add test_id to subject-level last_test_ids."""
    try:
        _aw().update_one(
            {"user_id": user_id, "auto_id": auto_id},
            {"$addToSet": {"subjects.$[s].last_test_ids": test_id}},
            array_filters=[{"s.subject_id": subj_id}]
        )
    except Exception as e:
        logger.error(f"[Scheduler] _mark_test_seen failed uid={user_id} auto={auto_id} "
                     f"subj={subj_id} test={test_id}: {e}")


# ── Fresh login (non-interactive) ────────────────────────────────────────────

async def _fresh_login(session, api_base, phone, password):
    main = _m()
    headers = {**main.APPX_HEADERS, "content-type": "application/x-www-form-urlencoded"}
    for endpoint in [
        f"{api_base}/post/userLogin",
        f"{api_base}/post/userLogin?extra_details=0",
        f"{api_base}/api/v2/user/login",
    ]:
        try:
            async with session.post(
                endpoint, headers=headers,
                data={"email": phone, "phone": phone, "password": password,
                      "source": "website", "extra_details": "1"},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                r = await resp.json(content_type=None)
                d = r.get("data") or {}
                token = d.get("token") or r.get("token", "")
                if token:
                    uid = str(d.get("userid") or d.get("user_id") or
                              main._extract_user_id_from_jwt(token) or "")
                    return token, uid
        except Exception:
            continue
    return None, None

async def _validate_token(session, api_base, token, appx_uid) -> bool:
    main = _m()
    hdr = main.make_headers(token, appx_uid)
    for attempt in range(3):
        hits = _RL["hits"]
        courses = await main.get_courses(session, api_base, appx_uid, hdr)
        if courses:
            return True
        if _RL["hits"] == hits:          # no 429 involved → token really is invalid
            return False
        await asyncio.sleep(45)          # rate-limited, not invalid → wait and retry
    return False


# ── Thumbnail ────────────────────────────────────────────────────────────────

def _make_thumb(video_path: str):
    thumb = video_path + "_thumb.jpg"
    try:
        result = subprocess.run(
            ["ffmpeg", "-y", "-ss", "00:00:03", "-i", video_path,
             "-vframes", "1", "-vf", "scale=320:-1", "-q:v", "5", thumb],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30
        )
        if result.returncode == 0 and os.path.exists(thumb):
            return thumb
    except Exception:
        pass
    return None


def _get_video_duration(path: str) -> int:
    """Return video duration in seconds via ffprobe, or 0 on failure."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=30
        )
        return int(float(result.stdout.strip()))
    except Exception:
        return 0





# ── AppX API rate-limit helper ───────────────────────────────────────────────

# NOTE: All AppX API calls go through main.py's api_get() which already has:
#   - 0.3s delay before every request
#   - 429 detection with escalating backoff: 15s → 30s → 45s
#   - 3 retries with 2s sleep on error
# We do NOT add extra sleeps around those calls — that would double-hammer.
# The only inter-call delay we keep is TOPIC_ITER_DELAY between topic iterations
# (not between individual API calls) so the scheduler doesn't blast all topics back-to-back.

TOPIC_ITER_DELAY = 1.0   # seconds between processing each topic/subject iteration
RAND_DELAY_MIN   = 5     # random pause (seconds) before each AppX lookup — rate-limit safety
RAND_DELAY_MAX   = 20

RL_ABORT_STREAK  = 2     # this many consecutive 429-failed lookups → stop the run, resume later
MAX_RL_RETRIES   = 6     # auto-resume attempts per day before giving up until tomorrow
RECHECK_RECENT   = 0     # 0 = a finished lecture is NEVER looked up again. Set e.g. 5 to re-check
                         # the newest 5 lectures of each topic for RECHECK_DAYS (late-added PDFs).

class _RateLimitAbort(Exception):
    """AppX keeps answering 429 — stop now, resume automatically after a cool-off."""

_QUIZ_TYPES = {"QUIZ", "TEST", "ATTEMPT", "EXERCISE", "ASSIGNMENT"}

def _is_pure_quiz_lec(lec: dict) -> bool:
    mat = lec.get("material_type", "") or ""
    return mat.upper() in _QUIZ_TYPES or ("quiz" in mat.lower() and mat.upper() != "VIDEO")

def _prioritised(lectures: list):
    """(index, lecture) pairs with videos/PDFs FIRST and pure quizzes last.
    Original numbering is kept, so captions don't change."""
    return sorted(enumerate(lectures, 1), key=lambda t: _is_pure_quiz_lec(t[1]))

# Shared AppX rate-limit state (updated by the patched api_get below)
_RL = {"hits": 0, "until": 0.0, "last_hit": 0.0}

async def _rand_pause():
    """Random 5-20s pause; doubled for 10 minutes after AppX last said 429."""
    mult = 2 if (time.time() - _RL["last_hit"]) < 600 else 1
    await asyncio.sleep(random.uniform(RAND_DELAY_MIN, RAND_DELAY_MAX) * mult)

async def _api_get_tracked(session, url, headers, retries: int = 5):
    """
    Drop-in replacement for main.api_get (installed in start_scheduler).
    Same behaviour, but on HTTP 429 it sets ONE shared cooldown that every
    caller honours, uses longer jittered back-off, and retries 5 times.
    """
    for attempt in range(retries):
        left = _RL["until"] - time.time()
        if left > 0:
            await asyncio.sleep(left)
        try:
            await asyncio.sleep(0.3)
            async with session.get(url, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 429:
                    wait = min(30 * (attempt + 1), 180) + random.uniform(0, 10)
                    _RL["hits"]    += 1
                    _RL["last_hit"] = time.time()
                    _RL["until"]    = time.time() + wait
                    logger.warning(f"[Scheduler] AppX 429 — shared cooldown {wait:.0f}s "
                                   f"(attempt {attempt+1}/{retries})")
                    continue
                if resp.status != 200:
                    return {}
                return await resp.json(content_type=None)
        except Exception as e:
            if attempt == retries - 1:
                logger.warning(f"[Scheduler] AppX request error: {e}")
            await asyncio.sleep(2)
    return {}
AUTO_GAP_DELAY   = 10    # seconds between two autos due in the same minute
RECHECK_DAYS     = 7     # keep re-checking a finished lecture this long for late-added PDFs/videos


# ── Logged wrappers: failures in these used to be completely silent ──────────

async def _safe_dl_pdf(url, path):
    try:
        r = await _m().download_pdf(url, path)
        if not r or not os.path.exists(r):
            logger.warning(f"[Scheduler] download_pdf returned nothing: {url[:90]} -> {path}")
            return None
        return r
    except Exception as e:
        logger.error(f"[Scheduler] download_pdf crashed ({url[:90]}): {e}", exc_info=True)
        return None

async def _safe_dl_item(*args, **kwargs):
    try:
        return await _m().download_item(*args, **kwargs)
    except Exception as e:
        logger.error(f"[Scheduler] download_item crashed: {e}", exc_info=True)
        return False


# ── Supergroup topic helpers ──────────────────────────────────────────────────

async def _create_topic(supergroup_id: int, name: str):
    try:
        r = await asyncio.to_thread(
            lambda: requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/createForumTopic",
                json={"chat_id": supergroup_id, "name": f"📚 {name}"},
                timeout=15
            ).json()
        )
        if r.get("ok"):
            return r["result"]["message_thread_id"]
        logger.error(f"[Scheduler] Topic creation failed: {r}")
    except Exception as e:
        logger.error(f"[Scheduler] Topic creation error: {e}")
    return None

async def _delete_topic(supergroup_id: int, thread_id: int) -> bool:
    try:
        r = await asyncio.to_thread(
            lambda: requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/deleteForumTopic",
                json={"chat_id": supergroup_id, "message_thread_id": int(thread_id)},
                timeout=15
            ).json()
        )
        if not r.get("ok"):
            logger.warning(f"[Scheduler] deleteForumTopic {thread_id} failed: {r}")
        return bool(r.get("ok"))
    except Exception as e:
        logger.warning(f"[Scheduler] deleteForumTopic {thread_id} error: {e}")
        return False


async def _get_or_create_topic(supergroup_id: int, name: str,
                                topic_map: dict, map_key: str):
    """
    FIX #duplicate-threads: return existing thread_id from topic_map if
    already created, otherwise create a new one and store it in the map.
    Prevents duplicate Telegram topics on every scheduler run.
    """
    existing = topic_map.get(str(map_key))
    if existing:
        return existing
    thread_id = await _create_topic(supergroup_id, name)
    if thread_id:
        topic_map[str(map_key)] = thread_id
    return thread_id


# ── Telethon client for uploads ──────────────────────────────────────────────
# Telethon resolves peer IDs automatically — no peer cache needed.
# Used ONLY for send_video / send_document in _send_to_topic.
# The Pyrogram bot (passed as `bot`) is still used for all commands/messages.

_tl_client: TelegramClient = None

async def _get_tl_client() -> TelegramClient:
    """Return a connected Telethon bot client, creating it if needed."""
    global _tl_client
    if _tl_client is None:
        _tl_client = TelegramClient(
            "scheduler_uploader", API_ID, API_HASH,
            connection_retries=5,
        )
        await _tl_client.start(bot_token=BOT_TOKEN)
        logger.info("[Scheduler] Telethon upload client started")
    elif not _tl_client.is_connected():
        await _tl_client.connect()
        logger.info("[Scheduler] Telethon upload client reconnected")
    return _tl_client


# ── Progress message helpers (mirrors main.py safe_progress_edit) ─────────────

async def _safe_progress_edit(bot, chat_id: int, msg, text: str, **kwargs):
    """
    Edit a progress message.  If it was deleted, send a new one and return it
    so the caller can keep updating it — identical pattern to main.py.
    """
    try:
        await msg.edit_text(text, **kwargs)
        return msg
    except Exception:
        try:
            await msg.delete()
        except Exception:
            pass
        try:
            new_msg = await bot.send_message(chat_id, text, **kwargs)
            return new_msg
        except Exception as e:
            logger.error(f"[Scheduler] _safe_progress_edit send failed: {e}")
            return msg


def _fmt_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 ** 3:
        return f"{size_bytes / (1024**2):.1f} MB"
    return f"{size_bytes / (1024**3):.2f} GB"


def _make_progress_bar(current: int, total: int, width: int = 10) -> str:
    if total <= 0:
        return "░" * width
    filled = int(width * current / total)
    return "█" * filled + "░" * (width - filled)


# ── Send file to topic WITH progress ─────────────────────────────────────────

async def _send_to_topic(bot, supergroup_id, topic_id, file_path, caption,
                         is_video=False, display_name="",
                         progress_chat_id: int = None,
                         progress_title: str = ""):
    """
    Send a file to a supergroup forum topic.

    Strategy:
      1. devgagantools.fast_upload() — parallel MTProto upload via Telethon,
         gives real multi-MB/s speeds instead of the 0.2 MB/s single-connection cap.
      2. Telethon tl.send_file() with the returned InputFile handle — Telethon
         resolves peer IDs from integer automatically, so no PeerIdInvalid ever.

    Progress bar is shown to the user via the Pyrogram bot (progress_chat_id).
    """
    if not file_path or not os.path.exists(file_path):
        return False
    if not display_name:
        display_name = os.path.basename(file_path)

    file_size  = os.path.getsize(file_path)
    thumb_path = _make_thumb(file_path) if is_video else None
    prog_msg   = None

    # ── Initial progress message ──────────────────────────────────────────
    if progress_chat_id:
        label = progress_title or display_name[:50]
        try:
            prog_msg = await bot.send_message(
                progress_chat_id,
                f"📤 <b>Uploading…</b>\n<code>{label}</code>\n"
                f"📦 Size: {_fmt_size(file_size)}",
                parse_mode=HTML
            )
        except Exception:
            pass

    try:
        tl = await _get_tl_client()

        # ── Progress state shared between fast_upload callback and edit loop ──
        _progress_state = [0, file_size]  # [done, total]
        _start          = [time.time()]

        def _pgbar(done, total):
            bar   = _make_progress_bar(done, total)
            pct   = done * 100 // total if total else 0
            speed = done / max(time.time() - _start[0], 0.1) / (1024 * 1024)
            return (
                f"📤 <b>Uploading…</b>\n"
                f"<code>{display_name[:45]}</code>\n"
                f"[{bar}] {pct}%\n"
                f"{_fmt_size(done)} / {_fmt_size(total)} @ {speed:.1f} MB/s"
            )

        # FIX #progress-not-showing: fast_upload skips progress_bar_function
        # entirely when reply=None. Instead we use Telethon's native
        # progress_callback on send_file which always fires, and update
        # _progress_state from it so the async edit_loop can render it.
        def _tl_progress(done, total):
            _progress_state[0] = done
            _progress_state[1] = total

        # Background task: edits the Pyrogram message every 3 s.
        # Runs during both fast_upload (upload phase) and tl.send_file (send phase).
        async def _edit_loop():
            nonlocal prog_msg
            while True:
                await asyncio.sleep(3)
                if prog_msg and progress_chat_id:
                    try:
                        done  = _progress_state[0]
                        total = _progress_state[1]
                        if total > 0 and done > 0:
                            prog_msg = await _safe_progress_edit(
                                bot, progress_chat_id, prog_msg,
                                _pgbar(done, total), parse_mode=HTML
                            )
                    except Exception:
                        pass

        # Wrapper for fast_upload's progress_bar_function — also updates state
        # so the edit_loop shows something even if fast_upload does call it.
        def _pgbar_with_state(done, total):
            _progress_state[0] = done
            _progress_state[1] = total
            return _pgbar(done, total)

        # Start background edit loop — covers both upload and send phases
        edit_task = asyncio.create_task(_edit_loop())

        try:
            # ── FAST UPLOAD via devgagantools parallel MTProto ────────────
            uploaded_file = await fast_upload(
                client=tl,
                file_location=file_path,
                reply=None,                        # we handle progress ourselves
                name=display_name,
                progress_bar_function=_pgbar_with_state,
                user_id=progress_chat_id,
            )
        finally:
            edit_task.cancel()
            try:
                await edit_task
            except asyncio.CancelledError:
                pass

        logger.info(f"[Scheduler] Fast upload done: {display_name} ({_fmt_size(file_size)})")

        # ── Compute video duration once, before the retry loop ───────────
        # FIX #pdf-fail: computing _duration inside the loop and passing
        # attributes=None / duration=None explicitly to tl.send_file caused
        # Telethon to strip all auto-generated document attributes, which
        # broke the uploaded file's media-type declaration and made Telegram
        # reject subsequent sends (including PDFs that share the same client).
        # Solution: compute duration once outside the loop; never pass
        # attributes or duration when their value would be None/0.
        _duration  = _get_video_duration(file_path) if is_video else 0
        _vid_attrs = (
            [DocumentAttributeVideo(
                duration=_duration,
                w=1280,
                h=720,
                supports_streaming=True,
            )]
            if is_video and _duration
            else []          # empty list → Telethon uses its own defaults
        )

        # Reset progress state for the send phase so the edit loop shows it
        _progress_state[0] = 0
        _progress_state[1] = file_size
        _start[0]          = time.time()
        edit_task = asyncio.create_task(_edit_loop())

        try:
            # ── Send via Telethon using the uploaded file handle ──────────
            # Telethon resolves supergroup_id (int) automatically — no PeerIdInvalid.
            sent_ok = False
            for attempt in range(4):
                try:
                    if is_video:
                        send_kwargs = dict(
                            caption=caption,
                            parse_mode="html",
                            reply_to=topic_id,
                            supports_streaming=True,
                            thumb=thumb_path,
                            video_note=False,
                            # FIX: only pass duration/attributes when we have real values
                            **({"duration": _duration} if _duration else {}),
                            **({"attributes": _vid_attrs} if _vid_attrs else {}),
                        )
                        await tl.send_file(
                            supergroup_id,
                            uploaded_file,
                            progress_callback=_tl_progress,  # FIX #progress-not-showing
                            **send_kwargs,
                        )
                    else:
                        await tl.send_file(
                            supergroup_id,
                            uploaded_file,
                            caption=caption,
                            parse_mode="html",
                            reply_to=topic_id,
                            force_document=True,
                            progress_callback=_tl_progress,  # FIX #progress-not-showing
                        )
                    logger.info(f"[Scheduler] Sent to topic: {display_name}")
                    sent_ok = True
                    break

                except FloodWaitError as fw:
                    logger.warning(f"[Scheduler] FloodWait {fw.seconds}s on send attempt {attempt+1}")
                    await asyncio.sleep(fw.seconds + 2)
                except Exception as e:
                    logger.error(f"[Scheduler] Send error attempt {attempt+1} ({display_name}): {e}", exc_info=True)
                    await asyncio.sleep(5)
        finally:
            edit_task.cancel()
            try:
                await edit_task
            except asyncio.CancelledError:
                pass

        if not sent_ok:
            raise RuntimeError(f"send_file failed after 4 attempts: {display_name}")

        # ── Done ──────────────────────────────────────────────────────────
        if prog_msg:
            try:
                await prog_msg.delete()   # no "✅ Uploaded" message left in DM
            except Exception:
                pass
        return True

    except Exception as e:
        logger.error(f"[Scheduler] _send_to_topic error: {e}")
        if prog_msg:
            try:
                await prog_msg.edit_text(
                    f"❌ <b>Upload failed:</b> <code>{display_name[:50]}</code>\n{e}"
                )
            except Exception:
                pass
        return False
    finally:
        if thumb_path and os.path.exists(thumb_path):
            try:
                os.remove(thumb_path)
            except Exception:
                pass


# ════════════════════════════════════════════════════════════════════════════
#  CORE AUTO-UPDATE JOB
# ════════════════════════════════════════════════════════════════════════════

async def _run_auto_update_inner(bot, doc):
    main      = _m()
    user_id   = doc["user_id"]
    auto_id   = doc["auto_id"]
    api_base  = doc["api_base"]
    course_id = str(doc["course_id"])
    cname     = doc.get("course_name", "Course")
    sg_id     = doc["supergroup_id"]
    brand     = doc.get("brand", CREDIT)

    phone    = doc.get("phone", "")
    password = doc.get("password", "")
    token    = doc.get("appx_token", "")
    appx_uid = doc.get("appx_user_id", "")

    track_lec   = doc.get("track_lectures",  True)
    track_pdf   = doc.get("track_pdfs",      True)
    track_quiz  = doc.get("track_quizzes",   True)
    track_tests = doc.get("track_tests",     True)
    prefer_q    = doc.get("prefer_quality",  "720p")

    tmp_dir = os.path.join(tempfile.gettempdir(), "auto_dl")
    os.makedirs(tmp_dir, exist_ok=True)

    # ── Send job-start progress message to user ───────────────────────────
    prog_msg = None
    try:
        prog_msg = await bot.send_message(
            user_id,
            f"<blockquote><b>🤖 Auto #{auto_id} — {cname}</b>\n"
            f"🔄 Checking for new content…</blockquote>",
            parse_mode=HTML
        )
    except Exception as e:
        logger.error(f"[Scheduler] Could not send start message to user {user_id}: {e}")

    async with aiohttp.ClientSession() as session:

        # ── Token check / refresh ────────────────────────────────────────
        token_ok = False
        if token and appx_uid:
            token_ok = await _validate_token(session, api_base, token, appx_uid)

        if not token_ok:
            if phone and password:
                token, appx_uid = await _fresh_login(session, api_base, phone, password)
                if token:
                    token_ok = True
                    _aw().update_one(
                        {"user_id": user_id, "auto_id": auto_id},
                        {"$set": {"appx_token": token, "appx_user_id": appx_uid}}
                    )
            if not token_ok:
                logger.warning(f"[Scheduler] Re-login failed — auto #{auto_id} paused")
                _aw().update_one(
                    {"user_id": user_id, "auto_id": auto_id},
                    {"$set": {"is_active": False}}
                )
                try:
                    await bot.send_message(
                        user_id,
                        f"⚠️ <b>Auto #{auto_id} — Login Failed!</b>\n\n"
                        f"Could not log in to <code>{api_base}</code>.\n"
                        f"Auto #{auto_id} has been <b>paused</b>.\n"
                        f"Use /addauto to create a new one."
                    )
                except Exception:
                    pass
                if prog_msg:
                    try:
                        await prog_msg.delete()
                    except Exception:
                        pass
                return

        hdr = main.make_headers(token, appx_uid)
        updated_subjects = []
        total_new = 0
        failed_items = []
        ts_list_cache = None
        rl_streak = 0

        for subj in doc.get("subjects", []):
            subj_id   = str(subj["subject_id"])
            subj_name = subj["subject_name"]
            updated_topics = []

            ts_topic_map = subj.get("ts_topic_map", {})

            # ── New topics added on AppX after /addauto → create thread + track ──
            try:
                known_tids = {str(t["topic_id"]) for t in subj.get("topics", [])}
                await _rand_pause()
                live_topics = await main.get_topics(session, api_base, course_id, subj_id, hdr)
                for t in (live_topics or []):
                    new_tid = str(t.get("topicid", ""))
                    if not new_tid or new_tid in known_tids:
                        continue
                    new_tname = t.get("topic_name", "Topic")
                    new_thread = await _create_topic(sg_id, f"{subj_name} › {new_tname}")
                    if not new_thread:
                        continue
                    new_doc = {
                        "topic_id": new_tid, "topic_name": new_tname,
                        "topic_thread_id": new_thread,
                        "last_lecture_ids": [], "last_pdf_ids": [],
                        "last_quiz_ids": [], "lec_done_at": {},
                    }
                    _aw().update_one(
                        {"user_id": user_id, "auto_id": auto_id},
                        {"$push": {"subjects.$[s].topics": new_doc}},
                        array_filters=[{"s.subject_id": subj_id}]
                    )
                    subj.setdefault("topics", []).append(new_doc)
                    known_tids.add(new_tid)
                    logger.info(f"[Scheduler] New topic discovered: {subj_name} › {new_tname}")
                    await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"[Scheduler] topic discovery failed for {subj_name}: {e}", exc_info=True)

            for topic_doc in subj.get("topics", []):
                topic_id   = str(topic_doc["topic_id"])
                topic_name = topic_doc["topic_name"]
                thread_id  = topic_doc.get("topic_thread_id")

                if not thread_id:
                    updated_topics.append(topic_doc)
                    continue

                # Normalise to str so int IDs stored by main.py still match
                seen_lec_ids  = set(str(x) for x in topic_doc.get("last_lecture_ids", []))
                seen_pdf_ids  = set(str(x) for x in topic_doc.get("last_pdf_ids",     []))
                seen_quiz_ids = set(str(x) for x in topic_doc.get("last_quiz_ids",    []))
                pdf_retry_ids = set(str(x) for x in topic_doc.get("last_pdfretry_ids", []))
                now_ts  = time.time()
                done_at = {str(k): float(v)
                           for k, v in (topic_doc.get("lec_done_at") or {}).items()}
                window  = RECHECK_DAYS * 86400
                new_lec_ids   = set()
                new_pdf_ids   = set()
                new_quiz_ids  = set()

                await _rand_pause()
                lectures = await main.get_lectures(
                    session, api_base, course_id, subj_id, topic_id, hdr
                )
                total = len(lectures)
                recent_ids = ({str(l.get("id")) for l in lectures[-RECHECK_RECENT:]}
                              if RECHECK_RECENT > 0 else set())

                for i, lec in _prioritised(lectures):
                    lec_id = str(lec.get("id") or "")   # always str for consistent seen-ID checks
                    title  = lec.get("Title") or f"Lecture_{lec_id}"
                    mat    = lec.get("material_type", "")
                    name   = main.safe_filename(title, i)
                    out_path = os.path.join(tmp_dir, name)

                    if not lec_id:
                        continue

                    # ── Live progress update (mirrors run_lecture_loop) ───
                    if prog_msg:
                        prog_msg = await _safe_progress_edit(
                            bot, user_id, prog_msg,
                            f"<blockquote>"
                            f"🤖 <b>Auto #{auto_id} — {cname}</b>\n"
                            f"📚 {subj_name} → {topic_name}\n"
                            f"🔍 Checking {i}/{total}: {title[:45]}"
                            f"</blockquote>",
                            parse_mode=HTML
                        )

                    # ── Captions — IDENTICAL to run_lecture_loop ─────────
                    cc = (
                        f"[🎥] <code>{str(i).zfill(3)}) {title[:60]} [{prefer_q}]</code>\n"
                        f"<blockquote>Batch: {cname}</blockquote>\n"
                        f"Extracted by ➤ {brand}"
                    )
                    cc1 = (
                        f"[📕] <code>{str(i).zfill(3)}) {title[:60]}.pdf</code>\n"
                        f"<blockquote>Batch: {cname}</blockquote>\n"
                        f"Extracted by ➤ {brand}"
                    )

                    # ── Quiz detection — mirrors main.py run_lecture_loop exactly ──
                    QUIZ_TYPES = {"QUIZ", "TEST", "ATTEMPT", "EXERCISE", "ASSIGNMENT"}
                    # Match main.py field names exactly (no "quizid" without underscore)
                    raw_qid = str(lec.get("quiz_title_id") or lec.get("quiz_id") or "")
                    if raw_qid in ("", "0", "-1", "None", "null"):
                        raw_qid = ""

                    is_pure_quiz = (
                        mat.upper() in QUIZ_TYPES
                        or ("quiz" in mat.lower() and mat.upper() != "VIDEO")
                    )
                    # Match main.py: has_quiz is true if pure quiz OR has a quiz ID
                    has_quiz = is_pure_quiz or bool(raw_qid)

                    if has_quiz and track_quiz:
                        quiz_id = raw_qid or lec_id
                        if quiz_id and quiz_id not in seen_quiz_ids:
                            await asyncio.sleep(random.uniform(2, 5))
                            _qh = _RL["hits"]
                            try:
                                questions = await main.fetch_quiz(
                                    session, api_base, quiz_id, appx_uid, hdr
                                )
                            except Exception as e:
                                logger.error(f"[Scheduler] fetch_quiz qid={quiz_id}: {e}")
                                questions = []
                            if not questions and _RL["hits"] > _qh:
                                rl_streak += 1
                                if rl_streak >= RL_ABORT_STREAK:
                                    raise _RateLimitAbort("quiz lookups hit 429")
                            if questions:
                                qfile = main.save_quiz_html(
                                    questions, out_path, title, watermark=brand
                                )
                                if qfile and os.path.exists(qfile):
                                    q_cap = (
                                        f"📝 <code>{title[:60]}</code>\n"
                                        f"<code>{len(questions)} questions</code>\n"
                                        f"<code>{i}/{total}</code> | Batch: {cname}"
                                    )
                                    sent = await _send_to_topic(
                                        bot, sg_id, thread_id, qfile, q_cap,
                                        is_video=False,
                                        display_name=main.safe_filename(title) + "_quiz.html",
                                        progress_chat_id=user_id,
                                        progress_title=f"Quiz: {title[:40]}",
                                    )
                                    if sent:
                                        # Mark seen only after confirmed send
                                        new_quiz_ids.add(quiz_id)
                                        seen_quiz_ids.add(quiz_id)
                                        _mark_seen(user_id, auto_id, subj_id, topic_id,
                                                   "last_quiz_ids", quiz_id)
                                        total_new += 1
                                    try:
                                        os.remove(qfile)
                                    except Exception:
                                        pass
                            # If questions is empty or fetch failed → not marked seen → retried next run

                    if is_pure_quiz:
                        continue

                    # ── LEDGER CHECK — runs BEFORE any AppX call ─────────────────
                    # A lecture that is already downloaded is never looked up again:
                    #   • in the done-ledger (lec_done_at), or
                    #   • its video is already in the seen list, or
                    #   • PDF-only tracking / PDF-type lecture whose PDF is already sent.
                    # Only a lecture whose attached PDF failed earlier (pdf_retry_ids) is
                    # looked up again. Anything unsent / not released yet stays unseen and
                    # is retried on every run.
                    done_ts      = done_at.get(lec_id)
                    have_video   = track_lec and lec_id in seen_lec_ids
                    pdf_only_ok  = (not track_lec) and track_pdf and lec_id in seen_pdf_ids
                    pdf_type_ok  = track_pdf and lec_id in seen_pdf_ids and mat.upper() == "PDF"
                    finished     = bool(done_ts) or (
                        (have_video or pdf_only_ok or pdf_type_ok) and lec_id not in pdf_retry_ids)
                    if finished:
                        if not done_ts:
                            done_at[lec_id] = now_ts
                            _mark_done(user_id, auto_id, subj_id, topic_id, lec_id, now_ts)
                            continue
                        if not (lec_id in recent_ids and (now_ts - done_ts) <= window):
                            continue          # done → no AppX call at all

                    # ── Resolve video / PDF URL ───────────────────────────
                    await _rand_pause()
                    try:
                        _lec_hits = _RL["hits"]
                        for _try in range(2):
                            _hits = _RL["hits"]
                            vtitle, url, xor_key, vtype, class_pdfs, all_cands = \
                                await main.resolve_video(
                                    session, api_base, course_id, lec_id, hdr,
                                    prefer_quality=prefer_q
                                )
                            empty = not url and not class_pdfs and not all_cands
                            if empty and _RL["hits"] > _hits:
                                logger.warning(f"[Scheduler] lec={lec_id} empty because of 429 — retry {_try+1}/2")
                                await asyncio.sleep(30)
                                continue
                            break
                    except Exception as e:
                        logger.error(f"[Scheduler] resolve_video crashed lec={lec_id} "
                                     f"'{title[:40]}': {e}", exc_info=True)
                        failed_items.append(title)
                        continue

                    logger.info(f"[Scheduler] lec={lec_id} '{title[:40]}' mat={mat} "
                                f"vtype={vtype} url={'yes' if url else 'NO'} "
                                f"pdfs={len(class_pdfs or [])} cands={len(all_cands or [])}")

                    if not url and not class_pdfs and not all_cands:
                        logger.warning(f"[Scheduler] resolve_video gave nothing for lec={lec_id} '{title[:40]}'")
                        failed_items.append(title)
                        if _RL["hits"] > _lec_hits:
                            rl_streak += 1
                            if rl_streak >= RL_ABORT_STREAK:
                                raise _RateLimitAbort(f"{rl_streak} lookups in a row hit 429")
                        continue
                    rl_streak = 0

                    if vtype != "pdf" and lec_id not in done_at and \
                            ((not track_lec) or lec_id in seen_lec_ids):
                        done_at[lec_id] = now_ts          # window starts now
                        _mark_done(user_id, auto_id, subj_id, topic_id, lec_id, now_ts)

                    if track_lec and vtype != "pdf" and not url and all_cands:
                        logger.warning(f"[Scheduler] lec={lec_id} has candidates but no primary url "
                                       f"— video skipped by 'and url' check")

                    # ── PDF-type lecture ──────────────────────────────────
                    if vtype == "pdf":
                        if track_pdf and lec_id not in seen_pdf_ids:
                            if url:
                                # Show download progress
                                if prog_msg:
                                    prog_msg = await _safe_progress_edit(
                                        bot, user_id, prog_msg,
                                        f"<blockquote>⬇️ <b>Downloading PDF…</b>\n"
                                        f"<code>{str(i).zfill(3)}) {title[:55]}</code>\n"
                                        f"📚 {subj_name} → {topic_name}</blockquote>",
                                        parse_mode=HTML
                                    )
                                pr = await _safe_dl_pdf(url, out_path)
                                if pr and os.path.exists(pr):
                                    sent = await _send_to_topic(
                                        bot, sg_id, thread_id, pr, cc1,
                                        is_video=False,
                                        display_name=main.safe_filename(title) + ".pdf",
                                        progress_chat_id=user_id,
                                        progress_title=f"PDF: {title[:40]}",
                                    )
                                    if sent:
                                        # Mark seen only after confirmed send
                                        new_pdf_ids.add(lec_id)
                                        seen_pdf_ids.add(lec_id)
                                        _mark_seen(user_id, auto_id, subj_id, topic_id,
                                                   "last_pdf_ids", lec_id)
                                        total_new += 1
                                    try:
                                        os.remove(pr)
                                    except Exception:
                                        pass
                                # If download failed or file missing → not marked seen → retried next run
                            for idx2, (suffix, purl) in enumerate(class_pdfs, start=2):
                                if not purl:
                                    continue
                                pid = f"{lec_id}{suffix}"
                                if pid in seen_pdf_ids:
                                    continue
                                pr2 = await _safe_dl_pdf(
                                    purl, f"{out_path}_extra_{idx2}"
                                )
                                if pr2 and os.path.exists(pr2):
                                    cc_extra = (
                                        f"[📕] <code>{str(i).zfill(3)}) "
                                        f"{title[:55]}_p{idx2}.pdf</code>\n"
                                        f"<blockquote>Batch: {cname}</blockquote>\n"
                                        f"Extracted by ➤ {brand}"
                                    )
                                    sent2 = await _send_to_topic(
                                        bot, sg_id, thread_id, pr2, cc_extra,
                                        is_video=False,
                                        display_name=main.safe_filename(
                                            f"{title}_p{idx2}"
                                        ) + ".pdf",
                                        progress_chat_id=user_id,
                                        progress_title=f"PDF p{idx2}: {title[:35]}",
                                    )
                                    if sent2:
                                        new_pdf_ids.add(pid)
                                        seen_pdf_ids.add(pid)
                                        _mark_seen(user_id, auto_id, subj_id, topic_id,
                                                   "last_pdf_ids", pid)
                                        total_new += 1
                                    try:
                                        os.remove(pr2)
                                    except Exception:
                                        pass
                        if lec_id not in done_at and \
                                ((not track_pdf) or lec_id in seen_pdf_ids):
                            done_at[lec_id] = now_ts
                            _mark_done(user_id, auto_id, subj_id, topic_id, lec_id, now_ts)
                        continue

                    # ── PDFs attached to a video lecture ──────────────────
                    if track_pdf and class_pdfs:
                        for suffix, purl in class_pdfs:
                            if not purl:
                                continue
                            pid = f"{lec_id}{suffix}"
                            if pid in seen_pdf_ids:
                                continue
                            pr = await _safe_dl_pdf(purl, f"{out_path}{suffix}")
                            if pr and os.path.exists(pr):
                                sent = await _send_to_topic(
                                    bot, sg_id, thread_id, pr, cc1,
                                    is_video=False,
                                    display_name=main.safe_filename(title) + ".pdf",
                                    progress_chat_id=user_id,
                                    progress_title=f"PDF: {title[:40]}",
                                )
                                if sent:
                                    new_pdf_ids.add(pid)
                                    seen_pdf_ids.add(pid)
                                    _mark_seen(user_id, auto_id, subj_id, topic_id,
                                               "last_pdf_ids", pid)
                                    total_new += 1
                                try:
                                    os.remove(pr)
                                except Exception:
                                    pass

                    # ── YouTube type — notify topic, skip download ─────────
                    if vtype == "youtube":
                        if track_lec and lec_id not in seen_lec_ids:
                            try:
                                await bot.send_message(
                                    sg_id,
                                    f"🎬 <b>YouTube lecture (manual download needed)</b>\n"
                                    f"<code>{str(i).zfill(3)}) {title[:60]}</code>\n"
                                    f"<blockquote>Batch: {cname}</blockquote>\n"
                                    f"🔗 {url}",
                                    reply_to_message_id=thread_id,
                                )
                                # Mark seen only after confirmed notify
                                new_lec_ids.add(lec_id)
                                seen_lec_ids.add(lec_id)
                                _mark_seen(user_id, auto_id, subj_id, topic_id,
                                           "last_lecture_ids", lec_id)
                                total_new += 1
                            except Exception as e:
                                logger.error(f"[Scheduler] YT notify error: {e}")
                                # Not marked seen → retried next run
                        continue

                    # ── encrypted.m URL — extract key before download ──────
                    appx_enc_key = None
                    if url and "encrypted.m" in url:
                        parts = url.split("*", 1)
                        url   = parts[0]
                        if len(parts) > 1:
                            appx_enc_key = parts[1]
                        if len(parts) > 1 and not xor_key:
                            xor_key = parts[1]

                    # ── New video lecture ─────────────────────────────────
                    if track_lec and lec_id not in seen_lec_ids and url:

                        # ── Download progress (mirrors run_lecture_loop) ──
                        if prog_msg:
                            prog_msg = await _safe_progress_edit(
                                bot, user_id, prog_msg,
                                f"<blockquote>"
                                f"⬇️ <b>Downloading video…</b>\n"
                                f"<code>{str(i).zfill(3)}) {title[:55]} [{prefer_q}]</code>\n"
                                f"📚 {subj_name} → {topic_name}\n"
                                f"💃 Batch: {cname}"
                                f"</blockquote>",
                                parse_mode=HTML
                            )

                        # Pass class_pdfs=[] and want_pdf=False to download_item so it
                        # only downloads the video — we send class PDFs ourselves below
                        # (so we can track them in seen_pdf_ids and send with proper caption).
                        # Same downloaders as the manual flow (main.run_lecture_loop):
                        # saini helper first (handles encrypted.m + yt-dlp headers),
                        # then main.download_item as fallback.
                        vid_file = None
                        try:
                            helper = main.helper
                            if appx_enc_key:
                                res_file = await helper.download_and_decrypt_video(
                                    url, "", out_path, appx_enc_key)
                            else:
                                q_num = prefer_q.replace("p", "")
                                ytf   = f"b[height<={q_num}]/bv[height<={q_num}]+ba/b/bv+ba"
                                hargs = (f'--add-header "Referer: {main.MEDIA_REFERER}" '
                                         f'--add-header "User-Agent: {main.MEDIA_UA}" ')
                                if main._is_mp4_url(url) or url.lower().split("?")[0].endswith(".mp4"):
                                    cmd = (f'yt-dlp {hargs} -f "{ytf}" -R 25 '
                                           f'--fragment-retries 25 "{url}" -o "{out_path}.mp4"')
                                else:
                                    cmd = (f'yt-dlp {hargs} -f "{ytf}" --concurrent-fragments 16 '
                                           f'--no-part -R 25 --fragment-retries 25 '
                                           f'"{url}" -o "{out_path}.mp4"')
                                res_file = await helper.download_video(url, cmd, out_path)
                            if res_file and os.path.exists(str(res_file)):
                                vid_file = str(res_file)
                            else:
                                logger.warning(f"[Scheduler] helper download gave nothing for '{title[:40]}'")
                        except Exception as e:
                            logger.error(f"[Scheduler] helper download crashed '{title[:40]}': {e}",
                                         exc_info=True)

                        ok = bool(vid_file)
                        if not ok:
                            ok = await _safe_dl_item(
                                vtitle or title, url, xor_key, vtype,
                                out_path, hdr=hdr,
                                class_pdfs=[], want_pdf=False,
                                all_candidates=all_cands
                            )
                        if ok:
                            if not vid_file:
                                for ext in ("mp4", "mkv", "webm", "ts"):
                                    fp = f"{out_path}.{ext}"
                                    if os.path.exists(fp):
                                        vid_file = fp
                                        break
                            if vid_file:
                                file_mb = os.path.getsize(vid_file) / (1024 * 1024)
                                if prog_msg:
                                    prog_msg = await _safe_progress_edit(
                                        bot, user_id, prog_msg,
                                        f"<blockquote>"
                                        f"✅ <b>Downloaded!</b> ({file_mb:.1f} MB)\n"
                                        f"<code>{title[:55]}</code>\n"
                                        f"📤 Uploading to topic…"
                                        f"</blockquote>",
                                        parse_mode=HTML
                                    )
                                vid_ext = os.path.splitext(vid_file)[-1].lstrip(".")
                                sent = await _send_to_topic(
                                    bot, sg_id, thread_id, vid_file, cc,
                                    is_video=True,
                                    display_name=main.safe_filename(title) + f".{vid_ext}",
                                    progress_chat_id=user_id,
                                    progress_title=f"Video: {title[:40]}",
                                )
                                if sent:
                                    # Mark seen only after confirmed send
                                    new_lec_ids.add(lec_id)
                                    seen_lec_ids.add(lec_id)
                                    _mark_seen(user_id, auto_id, subj_id, topic_id,
                                               "last_lecture_ids", lec_id)
                                    total_new += 1
                                try:
                                    os.remove(vid_file)
                                except Exception:
                                    pass
                            # If vid_file not found after download → not marked seen → retried next run

                            # ── Send class PDFs attached to this video lecture ──
                            if track_pdf and class_pdfs:
                                for suffix, purl in class_pdfs:
                                    if not purl:
                                        continue
                                    pid = f"{lec_id}{suffix}"
                                    if pid in seen_pdf_ids:
                                        continue
                                    pr = await _safe_dl_pdf(purl, f"{out_path}{suffix}")
                                    if pr and os.path.exists(pr):
                                        sent = await _send_to_topic(
                                            bot, sg_id, thread_id, pr, cc1,
                                            is_video=False,
                                            display_name=main.safe_filename(title) + ".pdf",
                                            progress_chat_id=user_id,
                                            progress_title=f"PDF: {title[:40]}",
                                        )
                                        if sent:
                                            new_pdf_ids.add(pid)
                                            seen_pdf_ids.add(pid)
                                            _mark_seen(user_id, auto_id, subj_id, topic_id,
                                                       "last_pdf_ids", pid)
                                            total_new += 1
                                        try:
                                            os.remove(pr)
                                        except Exception:
                                            pass
                        else:
                            logger.warning(f"[Scheduler] Download failed: {title}")
                            if prog_msg:
                                prog_msg = await _safe_progress_edit(
                                    bot, user_id, prog_msg,
                                    f"<blockquote>"
                                    f"⚠️ <b>Download failed:</b>\n"
                                    f"<code>{title[:55]}</code>"
                                    f"</blockquote>",
                                    parse_mode=HTML
                                )
                            # Video failed → not marked seen → retried next run
                            # Still try class PDFs even when video download fails
                            if track_pdf and class_pdfs:
                                for suffix, purl in class_pdfs:
                                    if not purl:
                                        continue
                                    pid = f"{lec_id}{suffix}"
                                    if pid in seen_pdf_ids:
                                        continue
                                    pr = await _safe_dl_pdf(purl, f"{out_path}{suffix}")
                                    if pr and os.path.exists(pr):
                                        sent = await _send_to_topic(
                                            bot, sg_id, thread_id, pr, cc1,
                                            is_video=False,
                                            display_name=main.safe_filename(title) + ".pdf",
                                            progress_chat_id=user_id,
                                            progress_title=f"PDF: {title[:40]}",
                                        )
                                        if sent:
                                            new_pdf_ids.add(pid)
                                            seen_pdf_ids.add(pid)
                                            _mark_seen(user_id, auto_id, subj_id, topic_id,
                                                       "last_pdf_ids", pid)
                                            total_new += 1
                                        try:
                                            os.remove(pr)
                                        except Exception:
                                            pass

                    if vtype != "pdf" and ((not track_lec) or lec_id in seen_lec_ids):
                        pdfs_ok = (not track_pdf) or all(
                            f"{lec_id}{sfx}" in seen_pdf_ids for sfx, _u in (class_pdfs or []))
                        if pdfs_ok:
                            if lec_id not in done_at:
                                done_at[lec_id] = now_ts
                                _mark_done(user_id, auto_id, subj_id, topic_id, lec_id, now_ts)
                            if lec_id in pdf_retry_ids:
                                pdf_retry_ids.discard(lec_id)
                                _unmark_seen(user_id, auto_id, subj_id, topic_id,
                                             "last_pdfretry_ids", lec_id)
                        elif lec_id not in pdf_retry_ids:
                            # video is done but a PDF failed → look this lecture up again next run
                            pdf_retry_ids.add(lec_id)
                            _mark_seen(user_id, auto_id, subj_id, topic_id,
                                       "last_pdfretry_ids", lec_id)

                    await asyncio.sleep(TOPIC_ITER_DELAY)  # brief pause between lectures

                # ── Persist updated seen-ID sets ──────────────────────────
                topic_doc["last_lecture_ids"] = list(seen_lec_ids | new_lec_ids)
                topic_doc["last_pdf_ids"]     = list(seen_pdf_ids | new_pdf_ids)
                topic_doc["last_quiz_ids"]    = list(seen_quiz_ids | new_quiz_ids)
                topic_doc["lec_done_at"]      = done_at
                topic_doc["last_pdfretry_ids"] = list(pdf_retry_ids)
                topic_doc.pop("last_nopdf_ids", None)
                topic_doc.pop("ts_topic_map", None)
                topic_doc.pop("last_test_ids", None)
                updated_topics.append(topic_doc)

            # ── Persist subject-level data ────────────────────────────────
            subj["topics"]        = updated_topics
            subj["ts_topic_map"]  = ts_topic_map
            subj["last_test_ids"] = list(subj.get("last_test_ids", []))
            updated_subjects.append(subj)

        # Keep subjects added (via the "new subject" button) while this run was going
        fresh_now = _get_auto(user_id, auto_id)
        if not fresh_now:
            logger.warning(f"[Scheduler] Auto #{auto_id} was deleted during the run")
            return
        _have = {str(x["subject_id"]) for x in updated_subjects}
        updated_subjects += [x for x in fresh_now.get("subjects", [])
                             if str(x["subject_id"]) not in _have]
        _aw().update_one(
            {"user_id": user_id, "auto_id": auto_id},
            {"$set": {"subjects": updated_subjects, "last_run_at": datetime.utcnow()}}
        )

        # ── Test series: once per auto, EVERY series, EVERY series-subject ──
        if track_tests:
            try:
                total_new += await _sync_test_series(
                    bot, session, user_id, auto_id, api_base, course_id,
                    appx_uid, hdr, sg_id, brand, tmp_dir)
            except Exception as e:
                logger.error(f"[Scheduler] test-series sync failed: {e}", exc_info=True)

        # ── New subject on AppX? ask the owner ──
        try:
            await _check_new_subjects(bot, session, user_id, auto_id,
                                      api_base, course_id, hdr, cname)
        except Exception as e:
            logger.error(f"[Scheduler] new-subject check failed: {e}", exc_info=True)

        # ── Final summary to user (mirrors main.py completion message) ────
        summary = (
            f"<b>-┈━═.•° ✅ Auto #{auto_id} Done °•.═━┈-</b>\n"
            f"<blockquote><b>🎯 Batch: {cname}</b>\n"
            f"📦 New items sent: {total_new}</blockquote>"
        )
        if failed_items:
            summary += (f"\n⚠️ <b>{len(failed_items)} item(s) could not be fetched</b> "
                        f"(see bot logs): " + ", ".join(t[:25] for t in failed_items[:5]))
        if prog_msg:
            try:
                await prog_msg.edit_text(summary, parse_mode=HTML)
            except Exception:
                try:
                    await bot.send_message(user_id, summary, parse_mode=HTML)
                except Exception:
                    pass
        elif total_new > 0:
            try:
                await bot.send_message(user_id, summary, parse_mode=HTML)
            except Exception:
                pass

        logger.info(f"[Scheduler] ✅ Auto #{auto_id} for user {user_id} done. {total_new} new items.")


async def _run_auto_update(bot, doc):
    """Runs one auto. If AppX keeps rate-limiting, stops cleanly and schedules an
    automatic resume (30 → 60 → 120 → 240 min). Progress is never lost: every sent
    item is already saved as seen."""
    user_id, auto_id = doc["user_id"], doc["auto_id"]
    key = {"user_id": user_id, "auto_id": auto_id}
    try:
        await _run_auto_update_inner(bot, doc)
        _aw().update_one(key, {"$unset": {"retry_at": "", "retry_attempts": ""}})
    except _RateLimitAbort as e:
        fresh    = _get_auto(user_id, auto_id) or {}
        attempts = int(fresh.get("retry_attempts", 0)) + 1
        if attempts > MAX_RL_RETRIES:
            _aw().update_one(key, {"$unset": {"retry_at": ""}, "$set": {"retry_attempts": 0}})
            text = (f"⚠️ <b>Auto #{auto_id} — AppX keeps rate-limiting.</b>\n"
                    f"Stopped for today; it will try again at the next scheduled time "
                    f"(or send <code>/runnow {auto_id}</code> later).")
        else:
            delay_min = min(30 * 2 ** (attempts - 1), 240)
            retry_at  = time.time() + delay_min * 60
            _aw().update_one(key, {"$set": {"retry_at": retry_at, "retry_attempts": attempts}})
            when = datetime.fromtimestamp(retry_at, IST).strftime("%H:%M")
            text = (f"⏸️ <b>Auto #{auto_id} paused — AppX rate limit.</b>\n"
                    f"Nothing is lost. Resuming automatically at <code>{when}</code> IST "
                    f"(attempt {attempts}/{MAX_RL_RETRIES}).")
        logger.warning(f"[Scheduler] Auto #{auto_id} rate-limit abort: {e}")
        try:
            await bot.send_message(user_id, text, parse_mode=HTML)
        except Exception:
            pass


async def _sync_test_series(bot, session, user_id, auto_id, api_base, course_id,
                            appx_uid, hdr, sg_id, brand, tmp_dir) -> int:
    """
    Mirrors the manual flow: every test series of the course, every subject inside
    the series, every test. ONE Telegram thread per series, created only when there
    is something new to send, and its id is saved to the DB immediately so it is never
    duplicated and /delauto can always remove it.
    """
    main = _m()
    fresh = _get_auto(user_id, auto_id)
    if not fresh:
        return 0

    ts_map = {_ts_key(k): v for k, v in (fresh.get("ts_topic_map") or {}).items()}
    seen   = set(str(x) for x in fresh.get("last_test_ids", []))
    for s_ in fresh.get("subjects", []):                 # legacy per-subject data
        seen.update(str(x) for x in s_.get("last_test_ids", []))
        for k, v in (s_.get("ts_topic_map") or {}).items():
            ts_map.setdefault(_ts_key(k), v)             # reuse old thread, no new duplicate

    ts_list = await main.get_test_series_list(session, api_base, appx_uid, course_id, hdr) or []
    names   = [(t.get("title") or t.get("name") or "") for t in ts_list]
    sent_count = 0

    for ts in ts_list:
        series_id = str(ts.get("id") or ts.get("test_series_id") or "")
        if not series_id:
            continue
        series_name = ts.get("title") or ts.get("name") or f"Series_{series_id}"
        label = series_name if names.count(series_name) == 1 else f"{series_name} #{series_id}"

        await _rand_pause()
        ts_subjects = await main.get_testseries_subjects(session, api_base, series_id, hdr)
        if not ts_subjects:
            ts_subjects = [{"subjectid": 0, "subject_name": "All Tests", "id": 0}]

        all_tests = []
        for ts_subj in ts_subjects:
            sid_   = ts_subj.get("subjectid") or ts_subj.get("id") or 0
            sname_ = ts_subj.get("subject_name") or ts_subj.get("name") or ""
            tests  = await main.get_testseries_tests(session, api_base, series_id, sid_, hdr)
            for t in tests:
                t["_subject_name"] = sname_
            all_tests.extend(tests)
            await asyncio.sleep(TOPIC_ITER_DELAY)

        new_tests, batch = [], set()
        for t in all_tests:
            tid = str(t.get("id") or t.get("test_id") or "")
            if tid and tid not in seen and tid not in batch:
                batch.add(tid)
                new_tests.append((tid, t))
        logger.info(f"[Scheduler] series '{series_name}': {len(all_tests)} tests, {len(new_tests)} new")
        if not new_tests:
            continue                                   # nothing to send → no thread created

        key       = _ts_key(series_id)
        thread_id = ts_map.get(key)
        if not thread_id:
            thread_id = await _create_topic(sg_id, label)
            if not thread_id:
                logger.error(f"[Scheduler] could not create thread for series '{series_name}'")
                continue
            ts_map[key] = thread_id
        _aw().update_one({"user_id": user_id, "auto_id": auto_id},
                         {"$set": {f"ts_topic_map.{key}": thread_id}})   # persist NOW

        for tid, test in new_tests:
            title = test.get("title") or test.get("name") or f"Test_{tid}"
            await asyncio.sleep(random.uniform(2, 5))
            questions = await main.fetch_testseries_questions(session, api_base, test, hdr)
            if not questions:
                continue                               # not available yet → retried next run
            path = main.save_quiz_html(questions, os.path.join(tmp_dir, f"test_{tid}"),
                                       title, watermark=brand)
            if not path or not os.path.exists(path):
                continue
            marks = test.get("marks") or test.get("total_marks") or ""
            mins  = test.get("time")  or test.get("duration")    or ""
            meta  = [x for x in (f"{marks} marks" if marks else "", f"{mins} min" if mins else "") if x]
            meta_str = f" <code>[{', '.join(meta)}]</code>" if meta else ""
            subj_line = (f"Subject: {test['_subject_name']}\n"
                         if test.get("_subject_name") else "")
            cap = (f"📝 <code>{title[:55]}</code>{meta_str}\n"
                   f"<code>{len(questions)} questions</code>\n"
                   f"{subj_line}Series: {series_name} | {brand}")
            sent = await _send_to_topic(
                bot, sg_id, thread_id, path, cap, is_video=False,
                display_name=main.safe_filename(title) + ".html",
                progress_chat_id=user_id, progress_title=f"Test: {title[:40]}")
            if sent:
                seen.add(tid)
                _mark_test_seen_auto(user_id, auto_id, tid)
                sent_count += 1
            try:
                os.remove(path)
            except Exception:
                pass
            await asyncio.sleep(TOPIC_ITER_DELAY)
    return sent_count


def _subj_ident(s: dict) -> str:
    return str(s.get("id") or s.get("subjectid") or s.get("subject_id", ""))


async def _check_new_subjects(bot, session, user_id, auto_id, api_base,
                              course_id, hdr, cname):
    """Ask the owner (once per subject) whether a newly appeared AppX subject should be added."""
    main  = _m()
    fresh = _get_auto(user_id, auto_id)
    if not fresh:
        return
    live = await main.get_subjects(session, api_base, course_id, hdr) or []
    live_ids = {_subj_ident(s) for s in live if _subj_ident(s)}
    if "course_subject_ids" not in fresh:
        # First run after this feature: remember what exists now, don't nag about it.
        _aw().update_one({"user_id": user_id, "auto_id": auto_id},
                         {"$set": {"course_subject_ids": sorted(live_ids)}})
        return
    known = {str(x) for x in fresh["course_subject_ids"]}
    known |= {str(x["subject_id"]) for x in fresh.get("subjects", [])}
    for s in live:
        sid = _subj_ident(s)
        if not sid or sid in known:
            continue
        sname = s.get("subject_name") or s.get("name") or "Subject"
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Add to auto-update", callback_data=f"asub:{auto_id}:{sid}:y"),
            InlineKeyboardButton("❌ Ignore",             callback_data=f"asub:{auto_id}:{sid}:n"),
        ]])
        try:
            await bot.send_message(
                user_id,
                f"🆕 <b>New subject found</b>\n"
                f"<blockquote>Batch: {cname}\nSubject: <b>{sname}</b></blockquote>\n"
                f"Add it to Auto #{auto_id}? Topic threads will be created for it.",
                reply_markup=kb, parse_mode=HTML)
            _aw().update_one({"user_id": user_id, "auto_id": auto_id},
                             {"$addToSet": {"course_subject_ids": sid}})   # ask only once
        except Exception as e:
            logger.error(f"[Scheduler] could not send new-subject prompt: {e}")


async def _ensure_token(session, doc):
    token, appx_uid = doc.get("appx_token", ""), doc.get("appx_user_id", "")
    api_base = doc["api_base"]
    if token and appx_uid and await _validate_token(session, api_base, token, appx_uid):
        return token, appx_uid
    if doc.get("phone") and doc.get("password"):
        token, appx_uid = await _fresh_login(session, api_base, doc["phone"], doc["password"])
        if token:
            _aw().update_one({"user_id": doc["user_id"], "auto_id": doc["auto_id"]},
                             {"$set": {"appx_token": token, "appx_user_id": appx_uid}})
            return token, appx_uid
    return None, None


async def newsubject_cb(bot: Client, cq):
    main = _m()
    try:
        _, auto_s, sid, ans = cq.data.split(":")
        auto_id = int(auto_s)
    except Exception:
        await cq.answer("Invalid button.", show_alert=True)
        return
    uid = cq.from_user.id
    doc = _get_auto(uid, auto_id)
    if not doc:
        await cq.answer("This auto no longer exists.", show_alert=True)
        return
    if ans != "y":
        await cq.answer("Ignored.")
        await cq.message.edit_text("🚫 Subject ignored.")
        return
    if any(str(x["subject_id"]) == sid for x in doc.get("subjects", [])):
        await cq.answer("Already added.")
        await cq.message.edit_text("✅ Subject already added.")
        return

    await cq.answer("Adding…")
    await cq.message.edit_text("⏳ Adding subject and creating topic threads…")
    sg_id = doc["supergroup_id"]
    async with aiohttp.ClientSession() as session:
        token, appx_uid = await _ensure_token(session, doc)
        if not token:
            await cq.message.edit_text("❌ Login failed — could not add the subject.")
            return
        hdr  = main.make_headers(token, appx_uid)
        subs = await main.get_subjects(session, doc["api_base"], str(doc["course_id"]), hdr) or []
        sname = next((s.get("subject_name") or s.get("name") for s in subs
                      if _subj_ident(s) == sid), None) or "Subject"
        topics_raw = await main.get_topics(session, doc["api_base"], str(doc["course_id"]), sid, hdr)
        if not topics_raw:
            topics_raw = [{"topicid": "0", "topic_name": sname}]
        topics_docs = []
        for t in topics_raw:
            tname = t.get("topic_name", "Topic")
            thread = await _create_topic(sg_id, f"{sname} › {tname}")
            await asyncio.sleep(0.5)
            if not thread:
                continue
            topics_docs.append({
                "topic_id": str(t.get("topicid", "")), "topic_name": tname,
                "topic_thread_id": thread, "last_lecture_ids": [],
                "last_pdf_ids": [], "last_quiz_ids": [], "lec_done_at": {},
            })
    if not topics_docs:
        await cq.message.edit_text("❌ Could not create topic threads (bot needs Manage Topics).")
        return
    _aw().update_one(
        {"user_id": uid, "auto_id": auto_id},
        {"$push": {"subjects": {
            "subject_id": sid, "subject_name": sname, "topics": topics_docs,
            "ts_topic_map": {}, "last_test_ids": []}}}
    )
    await cq.message.edit_text(
        f"✅ <b>{sname}</b> added to Auto #{auto_id} ({len(topics_docs)} topic thread(s)).\n"
        f"It will be fetched on the next run — or send <code>/runnow {auto_id}</code>.",
        parse_mode=HTML)


async def run_all_due_autos(bot):
    _now    = datetime.now(IST)
    now_ist = _now.strftime("%H:%M")
    today   = _now.strftime("%Y-%m-%d")
    # Catch-up: run if the scheduled time has PASSED and it hasn't run today
    # (covers bot restarts / a long previous run that swallowed the exact minute).
    due = list(_aw().find({
        "is_active": True,
        "$or": [
            {"check_time_ist": {"$lte": now_ist}, "last_scheduled_date": {"$ne": today}},
            {"retry_at": {"$lte": time.time()}},          # resume after a rate-limit pause
        ],
    }))
    if not due:
        return
    logger.info(f"[Scheduler] {len(due)} auto(s) due at {now_ist} IST")
    for n, doc in enumerate(due):
        if n:
            await asyncio.sleep(AUTO_GAP_DELAY)
        try:
            await _run_auto_update(bot, doc)
            _aw().update_one({"user_id": doc["user_id"], "auto_id": doc["auto_id"]},
                             {"$set": {"last_scheduled_date": today}})
        except Exception as e:
            _aw().update_one({"user_id": doc["user_id"], "auto_id": doc["auto_id"]},
                             {"$set": {"last_scheduled_date": today}})
            logger.error(f"[Scheduler] Error in auto #{doc.get('auto_id')}: {e}", exc_info=True)
            try:
                await bot.send_message(
                    doc["user_id"],
                    f"❌ <b>Auto #{doc.get('auto_id')} crashed:</b>\n<code>{e}</code>",
                    parse_mode=HTML)
            except Exception:
                pass


# ════════════════════════════════════════════════════════════════════════════
#  /addauto COMMAND
# ════════════════════════════════════════════════════════════════════════════

async def addauto_handler(bot: Client, m: Message):
    uid  = m.from_user.id
    main = _m()

    if _aw().count_documents({"user_id": uid}) >= MAX_AUTOS_PER_USER:
        await m.reply_text(
            f"❌ You have reached the <b>{MAX_AUTOS_PER_USER} auto-updates</b> limit.\n"
            "Use /delauto to remove one first."
        )
        return

    auto_id = _next_auto_id(uid)
    if auto_id is None:
        await m.reply_text("❌ No auto-ID available.")
        return

    # Step 1: Domain
    ask = await m.reply_text(
        "<b>🔧 Auto-Setup — Step 1/7</b>\n\n"
        "🌐 <b>Enter your AppX API domain:</b>\n"
        "<blockquote>e.g. <code>parmaracademyapi.teachx.in</code>\n"
        "Send without <code>https://</code>\n\nSend /cancel to abort.</blockquote>"
    )
    try:
        msg = await bot.listen(chat_id=m.chat.id, timeout=120)
    except Exception:
        await ask.edit_text("⏰ Timed out.")
        return
    if (msg.text or "").strip() == "/cancel":
        await m.reply_text("❌ Cancelled.")
        return
    raw_domain = (msg.text or "").strip()
    await msg.delete(); await ask.delete()
    api_base = f"https://{raw_domain}" if not raw_domain.startswith("http") else raw_domain

    # Step 2: Login
    ask = await m.reply_text(
        "<b>🔧 Auto-Setup — Step 2/7</b>\n\n"
        "🔐 <b>Login method:</b>\n\n"
        "<code>1</code> — Phone/Email + Password <i>(recommended — auto-refreshes token)</i>\n"
        "<code>2</code> — JWT Token only\n\nSend <code>1</code> or <code>2</code>:"
    )
    try:
        msg = await bot.listen(chat_id=m.chat.id, timeout=60)
    except Exception:
        await ask.edit_text("⏰ Timed out.")
        return
    login_choice = (msg.text or "").strip()
    await msg.delete(); await ask.delete()

    phone = password = token = appx_uid = ""

    if login_choice == "2":
        ask = await m.reply_text("<b>Send your JWT Token:</b>")
        try:
            msg = await bot.listen(chat_id=m.chat.id, timeout=120)
        except Exception:
            await ask.edit_text("⏰ Timed out.")
            return
        token = (msg.text or "").strip()
        await msg.delete(); await ask.delete()
        appx_uid = str(main._extract_user_id_from_jwt(token) or "")
        if not appx_uid:
            ask2 = await m.reply_text(
                "⚠️ Could not extract User ID. Send it manually:"
            )
            try:
                msg2 = await bot.listen(chat_id=m.chat.id, timeout=60)
                appx_uid = (msg2.text or "").strip()
                await msg2.delete(); await ask2.delete()
            except Exception:
                await ask2.edit_text("⏰ Timed out.")
                return
    else:
        ask = await m.reply_text("<b>🔧 Step 2a — Phone / Email:</b>")
        try:
            msg = await bot.listen(chat_id=m.chat.id, timeout=120)
        except Exception:
            await ask.edit_text("⏰ Timed out.")
            return
        phone = (msg.text or "").strip()
        await msg.delete(); await ask.delete()

        ask = await m.reply_text("<b>🔧 Step 2b — Password:</b>")
        try:
            msg = await bot.listen(chat_id=m.chat.id, timeout=120)
        except Exception:
            await ask.edit_text("⏰ Timed out.")
            return
        password = (msg.text or "").strip()
        await msg.delete(); await ask.delete()

        wait = await m.reply_text("⏳ Logging in…")
        async with aiohttp.ClientSession() as session:
            token, appx_uid = await _fresh_login(session, api_base, phone, password)
        if not token:
            await wait.edit_text("❌ <b>Login failed.</b> Check credentials.")
            return
        await wait.edit_text("✅ Login successful!")

    # Validate & fetch courses
    wait = await m.reply_text("⏳ Fetching your courses…")
    async with aiohttp.ClientSession() as session:
        hdr     = main.make_headers(token, appx_uid)
        courses = await main.get_courses(session, api_base, appx_uid, hdr)

    if not courses:
        await wait.edit_text("❌ No courses found or token invalid.")
        return

    # Step 3: Course
    ct = "<b>📚 Auto-Setup — Step 3/7 — Select Course</b>\n\nYour courses:\n\n"
    for i, c in enumerate(courses[:20], 1):
        ct += f"<code>{i}.</code> {c.get('course_name') or c.get('title') or f'Course {i}'}\n"
    ct += "\n<b>Send the course number:</b>"
    await wait.edit_text(ct)
    try:
        msg = await bot.listen(chat_id=m.chat.id, timeout=60)
    except Exception:
        await m.reply_text("⏰ Timed out.")
        return
    try:
        ci = int((msg.text or "").strip()) - 1
        assert 0 <= ci < len(courses)
    except Exception:
        await m.reply_text("❌ Invalid selection.")
        return
    await msg.delete()
    chosen_course = courses[ci]
    course_id     = str(chosen_course.get("id") or chosen_course.get("courseid", ""))
    course_name   = chosen_course.get("course_name") or chosen_course.get("title", "")

    # Step 4: Subjects
    wait2 = await m.reply_text("⏳ Fetching subjects…")
    async with aiohttp.ClientSession() as session:
        hdr      = main.make_headers(token, appx_uid)
        subj_raw = await main.get_subjects(session, api_base, course_id, hdr)

    if not subj_raw:
        await wait2.edit_text("❌ No subjects found.")
        return

    st = "<b>📖 Auto-Setup — Step 4/7 — Select Subjects</b>\n\nAvailable:\n\n"
    for i, s in enumerate(subj_raw[:30], 1):
        st += f"<code>{i}.</code> {s.get('subject_name','?')}\n"
    st += "\n<i>Send numbers comma-separated, or <code>all</code>:</i>"
    await wait2.edit_text(st)
    try:
        msg = await bot.listen(chat_id=m.chat.id, timeout=60)
    except Exception:
        await m.reply_text("⏰ Timed out.")
        return
    raw_in = (msg.text or "").strip().lower()
    await msg.delete()

    if raw_in in ("all", ""):
        chosen_subjects = subj_raw
    else:
        try:
            idxs            = [int(x.strip()) - 1 for x in raw_in.split(",")]
            chosen_subjects = [subj_raw[i] for i in idxs if 0 <= i < len(subj_raw)]
        except Exception:
            await m.reply_text("❌ Invalid selection.")
            return
    if not chosen_subjects:
        await m.reply_text("❌ No subjects selected.")
        return

    # Step 5: What to track
    ask = await m.reply_text(
        "<b>🔍 Auto-Setup — Step 5/7 — What to Track</b>\n\n"
        "<code>1</code> 📹 Lectures (videos)\n"
        "<code>2</code> 📄 PDFs\n"
        "<code>3</code> 📝 Lecture Quizzes\n"
        "<code>4</code> 📋 Test Series\n\n"
        "<i>Send numbers comma-separated. e.g. <code>1,2,3,4</code> for everything</i>"
    )
    try:
        msg = await bot.listen(chat_id=m.chat.id, timeout=60)
    except Exception:
        await ask.edit_text("⏰ Timed out.")
        return
    track_nums = set()
    for x in (msg.text or "").split(","):
        try:
            track_nums.add(int(x.strip()))
        except Exception:
            pass
    await msg.delete(); await ask.delete()

    track_lec   = 1 in track_nums
    track_pdf   = 2 in track_nums
    track_quiz  = 3 in track_nums
    track_tests = 4 in track_nums
    if not any([track_lec, track_pdf, track_quiz, track_tests]):
        await m.reply_text("❌ Nothing selected.")
        return

    # Step 5.5: Video Quality
    ask = await m.reply_text(
        "<b>🎬 Auto-Setup — Step 5.5/7 — Preferred Video Quality</b>\n\n"
        "<code>1</code> — 1080p\n"
        "<code>2</code> — 720p <i>(recommended)</i>\n"
        "<code>3</code> — 480p\n"
        "<code>4</code> — 360p\n\n"
        "<i>Send the number, or /d for default (720p):</i>"
    )
    try:
        msg = await bot.listen(chat_id=m.chat.id, timeout=60)
    except Exception:
        await ask.edit_text("⏰ Timed out.")
        return
    _q_map = {"1": "1080p", "2": "720p", "3": "480p", "4": "360p"}
    _q_raw = (msg.text or "").strip()
    prefer_q = _q_map.get(_q_raw, "720p") if _q_raw != "/d" else "720p"
    await msg.delete(); await ask.delete()

    # Step 6: Brand
    ask = await m.reply_text(
        "<b>🏷️ Auto-Setup — Step 6/7 — Brand Name</b>\n\n"
        "Send your channel/watermark <i>(e.g. <code>@MyChannel</code>)</i> "
        "or <code>/d</code> for default:"
    )
    try:
        msg    = await bot.listen(chat_id=m.chat.id, timeout=30)
        raw_br = (msg.text or "").strip()
        brand  = CREDIT if raw_br == "/d" else raw_br
        await msg.delete(); await ask.delete()
    except Exception:
        brand = CREDIT
        await ask.delete()

    # Step 7a: Supergroup ID
    ask = await m.reply_text(
        "<b>🏠 Auto-Setup — Step 7a/7 — Supergroup ID</b>\n\n"
        "<blockquote>e.g. <code>-1001234567890</code>\n\n"
        "✅ Bot must be <b>admin</b> with <b>Manage Topics</b> permission.</blockquote>"
    )
    try:
        msg = await bot.listen(chat_id=m.chat.id, timeout=60)
    except Exception:
        await ask.edit_text("⏰ Timed out.")
        return
    try:
        sg_id = int((msg.text or "").strip())
    except Exception:
        await m.reply_text("❌ Invalid Supergroup ID.")
        return
    await msg.delete(); await ask.delete()

    # Step 7b: Time
    ask = await m.reply_text(
        "<b>🕒 Auto-Setup — Step 7b/7 — Schedule Time (IST)</b>\n\n"
        "<blockquote>HH:MM 24-hour format. e.g. <code>08:00</code> or <code>20:30</code></blockquote>"
    )
    try:
        msg = await bot.listen(chat_id=m.chat.id, timeout=60)
    except Exception:
        await ask.edit_text("⏰ Timed out.")
        return
    time_input = (msg.text or "").strip()
    await msg.delete(); await ask.delete()

    if not re.match(r"^\d{2}:\d{2}$", time_input):
        await m.reply_text("❌ Invalid time. Use HH:MM like <code>09:00</code>.")
        return
    h, mi = map(int, time_input.split(":"))
    if not (0 <= h <= 23 and 0 <= mi <= 59):
        await m.reply_text("❌ Invalid time values.")
        return

    # Fetch topics per subject and create Telegram topic threads
    creating = await m.reply_text(
        f"⏳ Fetching topics and creating Telegram threads for "
        f"{len(chosen_subjects)} subject(s)…\n"
        f"<i>(One thread per AppX topic — this may take a moment)</i>"
    )

    subjects_docs  = []
    total_threads  = 0
    failed_threads = []

    async with aiohttp.ClientSession() as session:
        hdr = main.make_headers(token, appx_uid)
        for s in chosen_subjects:
            sname = s.get("subject_name") or s.get("name") or "Subject"
            sid   = str(s.get("id") or s.get("subjectid") or s.get("subject_id", ""))

            topics_raw = await main.get_topics(session, api_base, course_id, sid, hdr)
            if not topics_raw:
                topics_raw = [{"topicid": "0", "topic_name": sname}]

            topics_docs = []
            for t in topics_raw:
                tname = t.get("topic_name", "Topic")
                tid   = str(t.get("topicid", ""))

                thread_id = None
                existing_auto = _aw().find_one(
                    {"user_id": uid, "supergroup_id": sg_id,
                     "subjects.topics.topic_id": tid},
                    {"subjects.$": 1}
                )
                if existing_auto:
                    for ex_subj in existing_auto.get("subjects", []):
                        for ex_topic in ex_subj.get("topics", []):
                            if str(ex_topic.get("topic_id")) == tid:
                                thread_id = ex_topic.get("topic_thread_id")
                                break
                        if thread_id:
                            break

                if not thread_id:
                    thread_id = await _create_topic(sg_id, f"{sname} › {tname}")
                    await asyncio.sleep(0.5)

                if thread_id is None:
                    failed_threads.append(f"{sname} › {tname}")
                    continue
                topics_docs.append({
                    "topic_id":         tid,
                    "topic_name":       tname,
                    "topic_thread_id":  thread_id,
                    "last_lecture_ids": [],
                    "last_pdf_ids":     [],
                    "last_quiz_ids":    [],
                })
                total_threads += 1

            if topics_docs:
                subjects_docs.append({
                    "subject_id":    sid,
                    "subject_name":  sname,
                    "topics":        topics_docs,
                    "ts_topic_map":  {},
                    "last_test_ids": [],
                })

    if not subjects_docs:
        await creating.edit_text(
            "❌ Could not create any topic threads.\n"
            "Make sure the bot is <b>admin</b> with <b>Manage Topics</b> permission."
        )
        return

    doc = {
        "user_id":        uid,
        "auto_id":        auto_id,
        "api_base":       api_base,
        "course_id":      course_id,
        "course_name":    course_name,
        "appx_token":     token,
        "appx_user_id":   appx_uid,
        "phone":          phone,
        "password":       password,
        "supergroup_id":  sg_id,
        "check_time_ist": time_input,
        "brand":          brand,
        "prefer_quality": prefer_q,
        "track_lectures": track_lec,
        "track_pdfs":     track_pdf,
        "track_quizzes":  track_quiz,
        "track_tests":    track_tests,
        "subjects":       subjects_docs,
        "is_active":      True,
        "last_scheduled_date": (datetime.now(IST).strftime("%Y-%m-%d")
                                if time_input <= datetime.now(IST).strftime("%H:%M") else None),
        "added_at":       datetime.utcnow(),
        "last_run_at":    None,
    }
    _save_auto(doc)

    track_labels = []
    if track_lec:   track_labels.append("📹 Lectures")
    if track_pdf:   track_labels.append("📄 PDFs")
    if track_quiz:  track_labels.append("📝 Quizzes")
    if track_tests: track_labels.append("📋 Tests")

    result = (
        f"✅ <b>Auto #{auto_id} Created!</b>\n\n"
        f"📚 <b>Course:</b> {course_name}\n"
        f"🧵 <b>Topic threads created:</b> {total_threads}\n"
        f"📖 <b>Subjects:</b> {len(subjects_docs)}\n"
        f"🔍 <b>Tracking:</b> {' | '.join(track_labels)}\n"
        f"🎬 <b>Quality:</b> <code>{prefer_q}</code>\n"
        f"🕒 <b>Daily at:</b> <code>{time_input}</code> IST\n"
        f"🏠 <b>Supergroup:</b> <code>{sg_id}</code>\n"
        f"🏷️ <b>Brand:</b> <code>{brand}</code>\n"
    )
    if failed_threads:
        result += (
            f"\n⚠️ Thread creation failed for: "
            f"{', '.join(failed_threads[:5])}"
        )
    result += (
        f"\n\n<b>Commands:</b>\n"
        f"/myautos — view all autos\n"
        f"/runnow {auto_id} — run immediately\n"
        f"/delauto — delete an auto"
    )
    await creating.edit_text(result)


# ════════════════════════════════════════════════════════════════════════════
#  /myautos
# ════════════════════════════════════════════════════════════════════════════

async def myautos_handler(bot: Client, m: Message):
    uid   = m.from_user.id
    autos = _get_autos(uid)
    if not autos:
        await m.reply_text(
            "📋 <b>No auto-updates configured yet.</b>\n\nUse /addauto!"
        )
        return

    text = f"📋 <b>Your Auto-Updates ({len(autos)}/{MAX_AUTOS_PER_USER})</b>\n\n"
    for doc in autos:
        subjects      = doc.get("subjects", [])
        total_threads = sum(len(s.get("topics", [])) for s in subjects)
        subj_names    = [s["subject_name"] for s in subjects]
        tracks = []
        if doc.get("track_lectures"):  tracks.append("📹 Lec")
        if doc.get("track_pdfs"):      tracks.append("📄 PDF")
        if doc.get("track_quizzes"):   tracks.append("📝 Quiz")
        if doc.get("track_tests"):     tracks.append("📋 Test")
        status   = "🟢 Active" if doc.get("is_active") else "🔴 Paused"
        last_run = doc.get("last_run_at")
        last_str = (last_run.strftime("%d %b %H:%M UTC")
                    if isinstance(last_run, datetime) else "Never")
        text += (
            f"<b>#{doc['auto_id']} — {doc['course_name']}</b>\n"
            f"  🌐 <code>{doc.get('api_base','?')}</code>\n"
            f"  📖 {', '.join(subj_names[:3])}{'…' if len(subj_names) > 3 else ''}\n"
            f"  🧵 {total_threads} topic thread(s)\n"
            f"  🔍 {' + '.join(tracks) if tracks else 'Nothing'}\n"
            f"  🎬 Quality: <code>{doc.get('prefer_quality', '720p')}</code>\n"
            f"  🕒 <code>{doc.get('check_time_ist','?')}</code> IST | {status}\n"
            f"  🏠 <code>{doc.get('supergroup_id','?')}</code>\n"
            f"  ⏱️ Last run: {last_str}\n\n"
        )
    text += "<i>Use /runnow &lt;id&gt; to trigger now | /delauto to remove</i>"
    await m.reply_text(text)


# ════════════════════════════════════════════════════════════════════════════
#  /runnow
# ════════════════════════════════════════════════════════════════════════════

async def runnow_handler(bot: Client, m: Message):
    uid     = m.from_user.id
    args    = m.text.split()
    auto_id = None
    reset   = len(args) > 2 and args[2].lower() == "reset"
    if len(args) > 1:
        try:
            auto_id = int(args[1])
        except Exception:
            pass

    if auto_id is None:
        autos = _get_autos(uid)
        if not autos:
            await m.reply_text("❌ No autos configured. Use /addauto first.")
            return
        if len(autos) == 1:
            auto_id = autos[0]["auto_id"]
        else:
            list_text = "<b>Which auto do you want to run?</b>\n\n"
            for a in autos:
                list_text += (
                    f"<code>{a['auto_id']}.</code> {a['course_name']} "
                    f"| {a.get('check_time_ist','?')} IST\n"
                )
            list_text += "\nSend the number:"
            ask = await m.reply_text(list_text)
            try:
                msg     = await bot.listen(chat_id=m.chat.id, timeout=30)
                auto_id = int((msg.text or "").strip())
                await msg.delete(); await ask.delete()
            except Exception:
                await ask.edit_text("⏰ Timed out / invalid.")
                return

    doc = _get_auto(uid, auto_id)
    if not doc:
        await m.reply_text(f"❌ Auto #{auto_id} not found.")
        return

    if reset:
        for _s in doc.get("subjects", []):
            for _t in _s.get("topics", []):
                _t["last_lecture_ids"] = []
                _t["last_pdf_ids"]     = []
                _t["lec_done_at"]      = {}
                _t["last_pdfretry_ids"] = []
        _aw().update_one({"user_id": uid, "auto_id": auto_id},
                         {"$set": {"subjects": doc["subjects"]}})
        await m.reply_text("♻️ Cleared the seen video/PDF list — everything will be re-sent.")

    await m.reply_text(
        f"🚀 <b>Running Auto #{auto_id} — {doc['course_name']} now…</b>\n"
        "Progress updates will appear below."
    )

    # Cancel any existing run for this auto_id before starting a new one
    existing = _active_runnow_tasks.get(auto_id)
    if existing and not existing.done():
        existing.cancel()
        try:
            await existing
        except (asyncio.CancelledError, Exception):
            pass

    async def _run_and_catch():
        try:
            await _run_auto_update(bot, doc)
        except asyncio.CancelledError:
            logger.warning(f"[Scheduler] /runnow #{auto_id} cancelled cleanly")
            raise
        except Exception as e:
            logger.error(f"[Scheduler] /runnow crashed for auto #{auto_id}: {e}", exc_info=True)
            try:
                await bot.send_message(
                    uid,
                    f"❌ <b>Auto #{auto_id} crashed:</b>\n<code>{e}</code>",
                    parse_mode=HTML
                )
            except Exception:
                pass
        finally:
            _active_runnow_tasks.pop(auto_id, None)

    task = asyncio.create_task(_run_and_catch())
    _active_runnow_tasks[auto_id] = task


# ════════════════════════════════════════════════════════════════════════════
#  /delauto
# ════════════════════════════════════════════════════════════════════════════

async def delauto_handler(bot: Client, m: Message):
    uid   = m.from_user.id
    autos = _get_autos(uid)
    if not autos:
        await m.reply_text("📋 You have no auto-updates to delete.")
        return

    text = "🗑️ <b>Delete Auto-Update</b>\n\nYour autos:\n\n"
    for doc in autos:
        text += (
            f"<code>{doc['auto_id']}.</code> <b>{doc['course_name']}</b> "
            f"| <code>{doc.get('check_time_ist','?')}</code> IST\n"
        )
    text += "\n<b>Send the auto number to delete:</b>"
    await m.reply_text(text)
    try:
        pick = await bot.listen(chat_id=m.chat.id, timeout=60)
    except Exception:
        await m.reply_text("⏰ Timed out.")
        return
    try:
        auto_id = int((pick.text or "").strip())
    except Exception:
        await m.reply_text("❌ Invalid number.")
        return

    doc = _get_auto(uid, auto_id)
    if not doc:
        await m.reply_text(f"❌ Auto #{auto_id} not found.")
        return

    sg_id = doc.get("supergroup_id")
    ask   = await m.reply_text(
        f"<b>Delete Auto #{auto_id} — {doc['course_name']}</b>\n\n"
        "<code>1</code> — Delete entire auto (removes ALL topic threads)\n"
        "<code>2</code> — Remove specific subjects only\n\n"
        "<b>Send 1 or 2:</b>"
    )
    try:
        choice_msg = await bot.listen(chat_id=m.chat.id, timeout=60)
    except Exception:
        await ask.edit_text("⏰ Timed out.")
        return
    choice = (choice_msg.text or "").strip()
    await choice_msg.delete(); await ask.delete()

    if choice == "1":
        removed = 0
        for subj in doc.get("subjects", []):
            for t in subj.get("topics", []):
                tid = t.get("topic_thread_id")
                if tid and sg_id:
                    await _delete_topic(sg_id, tid)
                    removed += 1
                for ts_tid in t.get("ts_topic_map", {}).values():
                    try:
                        await _delete_topic(sg_id, int(ts_tid))
                        removed += 1
                    except Exception:
                        pass
            for ts_tid in subj.get("ts_topic_map", {}).values():
                try:
                    await _delete_topic(sg_id, int(ts_tid))
                    removed += 1
                except Exception:
                    pass
        for ts_tid in (doc.get("ts_topic_map") or {}).values():
            if await _delete_topic(sg_id, ts_tid):
                removed += 1
        _delete_auto(uid, auto_id)
        await m.reply_text(
            f"✅ <b>Auto #{auto_id} deleted.</b>\n"
            f"🗑️ Removed {removed} topic thread(s) including test-series."
        )

    elif choice == "2":
        subjects  = doc.get("subjects", [])
        subj_text = "<b>Select subjects to remove:</b>\n\n"
        for i, s in enumerate(subjects, 1):
            n    = len(s.get("topics", []))
            ts_n = len(s.get("ts_topic_map", {}))
            subj_text += (
                f"<code>{i}.</code> {s['subject_name']} "
                f"({n} topic thread(s)"
                f"{f', {ts_n} test-series thread(s)' if ts_n else ''})\n"
            )
        subj_text += "\n<i>Send numbers comma-separated (e.g. <code>1,3</code>)</i>"
        await m.reply_text(subj_text)
        try:
            subj_pick = await bot.listen(chat_id=m.chat.id, timeout=60)
        except Exception:
            await m.reply_text("⏰ Timed out.")
            return
        try:
            rm_idxs = [int(x.strip()) - 1 for x in (subj_pick.text or "").split(",")]
        except Exception:
            await m.reply_text("❌ Invalid input.")
            return

        removed_names = []
        kept_subjects = []
        for i, s in enumerate(subjects):
            if i in rm_idxs:
                for t in s.get("topics", []):
                    tid = t.get("topic_thread_id")
                    if tid and sg_id:
                        await _delete_topic(sg_id, tid)
                    for ts_tid in t.get("ts_topic_map", {}).values():
                        try:
                            await _delete_topic(sg_id, int(ts_tid))
                        except Exception:
                            pass
                for ts_tid in s.get("ts_topic_map", {}).values():
                    try:
                        await _delete_topic(sg_id, int(ts_tid))
                    except Exception:
                        pass
                removed_names.append(s["subject_name"])
            else:
                kept_subjects.append(s)

        if not kept_subjects:
            for ts_tid in (doc.get("ts_topic_map") or {}).values():
                await _delete_topic(sg_id, ts_tid)
            _delete_auto(uid, auto_id)
            await m.reply_text(
                f"✅ All subjects removed — <b>Auto #{auto_id} fully deleted.</b>"
            )
        else:
            _aw().update_one(
                {"user_id": uid, "auto_id": auto_id},
                {"$set": {"subjects": kept_subjects}}
            )
            await m.reply_text(
                f"✅ Removed: {', '.join(removed_names)}\n"
                f"📖 Remaining subjects: {len(kept_subjects)}"
            )
    else:
        await m.reply_text("❌ Invalid choice. Use /delauto again.")


# ════════════════════════════════════════════════════════════════════════════
#  SCHEDULER STARTUP — call once from main.py
# ════════════════════════════════════════════════════════════════════════════

async def start_scheduler(bot: Client):
    _ensure_indexes()

    for cmd, handler in [
        ("addauto", addauto_handler),
        ("myautos", myautos_handler),
        ("runnow",  runnow_handler),
        ("delauto", delauto_handler),
    ]:
        bot.add_handler(
            MessageHandler(handler, filters.command(cmd) & filters.private)
        )

    try:
        _m().api_get = _api_get_tracked      # every main.* AppX call now shares one cooldown
        logger.info("[Scheduler] main.api_get replaced with rate-limit-aware version")
    except Exception as e:
        logger.error(f"[Scheduler] could not patch api_get: {e}")

    # group=-1 so no other callback handler in main.py can swallow it first
    bot.add_handler(CallbackQueryHandler(newsubject_cb, filters.regex(r"^asub:")), group=-1)

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        run_all_due_autos,
        CronTrigger(minute="*"),
        args=[bot],
        id="auto_update_job",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    logger.info("[Scheduler] ✅ Started — topic-wise, checks every minute.")
    print("\033[92m✓ Auto-Update Scheduler started (topic-wise, checks every minute)\033[0m")
