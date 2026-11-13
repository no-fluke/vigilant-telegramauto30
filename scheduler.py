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
import logging
import tempfile
import subprocess
import aiohttp
import requests

from datetime import datetime, timezone, timedelta
from pyromod import listen as _pyromod_listen  # patches Client with .listen()
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait
from pyrogram.handlers import MessageHandler

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from db import db
from vars import CREDIT, BOT_TOKEN

MAX_AUTOS_PER_USER = 100
IST = timezone(timedelta(hours=5, minutes=30))
logger = logging.getLogger(__name__)


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
    courses = await main.get_courses(session, api_base, appx_uid, hdr)
    return bool(courses)


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


# ── Flood-wait wrapper ───────────────────────────────────────────────────────

async def _safe_send(coro):
    for _ in range(5):
        try:
            return await coro
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 2)
        except Exception as e:
            logger.error(f"[Scheduler] Send error: {e}")
            return None
    return None


# ── Supergroup topic helpers ──────────────────────────────────────────────────

async def _create_topic(supergroup_id: int, name: str):
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/createForumTopic",
            json={"chat_id": supergroup_id, "name": f"📚 {name}"},
            timeout=15
        ).json()
        if r.get("ok"):
            return r["result"]["message_thread_id"]
        logger.error(f"[Scheduler] Topic creation failed: {r}")
    except Exception as e:
        logger.error(f"[Scheduler] Topic creation error: {e}")
    return None

async def _delete_topic(supergroup_id: int, thread_id: int):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/deleteForumTopic",
            json={"chat_id": supergroup_id, "message_thread_id": thread_id},
            timeout=10
        )
    except Exception:
        pass


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


# ── Peer cache primer ────────────────────────────────────────────────────────

async def _prime_peer(bot, supergroup_id: int, retries: int = 3):
    """
    Force Pyrogram to resolve and cache the peer for supergroup_id.
    Must be called before any send — both pyrogram and requests paths need
    the peer in cache for flood-safe operations.
    Exponential back-off: 1s → 2s → 4s across retries.
    """
    for attempt in range(retries):
        try:
            await bot.get_chat(supergroup_id)
            return True
        except Exception as e:
            logger.warning(f"[Scheduler] _prime_peer attempt {attempt + 1}: {e}")
            await asyncio.sleep(2 ** attempt)
    return False


# ── Send file to topic ───────────────────────────────────────────────────────

async def _send_to_topic(bot, supergroup_id, topic_id, file_path, caption,
                         is_video=False, display_name=""):
    """
    Send a file to a supergroup forum topic.

    FIX #PeerInvalid  : always prime the peer cache BEFORE choosing the
                        upload path (not just inside the pyrogram branch).
    FIX #parse_mode   : use HTML so <blockquote> captions render correctly.
    FIX #small-files  : files ≤50 MB also benefit from peer priming so the
                        requests fallback never hits PeerInvalid indirectly
                        via flood-wait wrappers.
    """
    if not file_path or not os.path.exists(file_path):
        return
    if not display_name:
        display_name = os.path.basename(file_path)

    # ── Always prime peer cache first ────────────────────────────────────
    peer_ok = await _prime_peer(bot, supergroup_id)
    if not peer_ok:
        logger.error(
            f"[Scheduler] PeerInvalid — could not resolve {supergroup_id}. "
            "Check bot is admin in the supergroup."
        )
        return

    file_size    = os.path.getsize(file_path)
    use_pyrogram = file_size > 50 * 1024 * 1024
    thumb_path   = _make_thumb(file_path) if is_video else None

    try:
        if use_pyrogram:
            if is_video:
                duration = _get_video_duration(file_path)
                await _safe_send(bot.send_video(
                    supergroup_id, file_path, caption=caption,
                    parse_mode="html",
                    reply_to_message_id=topic_id, supports_streaming=True,
                    thumb=thumb_path,
                    duration=duration,
                    width=1280, height=720,
                ))
            else:
                await _safe_send(bot.send_document(
                    supergroup_id, file_path, caption=caption,
                    parse_mode="html",
                    reply_to_message_id=topic_id, file_name=display_name,
                ))
        else:
            api_url = f"https://api.telegram.org/bot{BOT_TOKEN}/"
            method  = "sendVideo" if is_video else "sendDocument"
            field   = "video"     if is_video else "document"
            ext     = os.path.splitext(display_name)[-1].lower()
            mime_map = {".pdf": "application/pdf", ".mp4": "video/mp4",
                        ".mkv": "video/x-matroska", ".webm": "video/webm",
                        ".html": "text/html"}
            mime = mime_map.get(ext, "application/octet-stream")

            post_data = {
                "chat_id":            supergroup_id,
                "message_thread_id":  topic_id,
                "caption":            caption[:1024],
                "parse_mode":         "HTML",
                "supports_streaming": "true" if is_video else "false",
            }
            if is_video:
                post_data["duration"] = str(_get_video_duration(file_path))

            def _do_upload():
                with open(file_path, "rb") as f:
                    files = {field: (display_name, f, mime)}
                    if thumb_path and os.path.exists(thumb_path):
                        with open(thumb_path, "rb") as tf:
                            files["thumbnail"] = ("thumb.jpg", tf, "image/jpeg")
                            return requests.post(
                                api_url + method,
                                data=post_data, files=files, timeout=300
                            ).json()
                    return requests.post(
                        api_url + method,
                        data=post_data, files=files, timeout=300
                    ).json()

            # Retry the HTTP upload up to 3 times on Telegram-side errors
            for _attempt in range(3):
                resp = await asyncio.get_event_loop().run_in_executor(None, _do_upload)
                if resp.get("ok"):
                    break
                err = resp.get("description", "")
                logger.error(f"[Scheduler] Send failed (attempt {_attempt + 1}): {err}")
                if "retry after" in err.lower():
                    wait_sec = int(re.search(r"\d+", err).group() or 5)
                    await asyncio.sleep(wait_sec)
                elif "peer_id_invalid" in err.lower():
                    # Peer cache stale mid-session — re-prime and retry once
                    await _prime_peer(bot, supergroup_id, retries=5)
                else:
                    break  # Non-retriable error

    except Exception as e:
        logger.error(f"[Scheduler] Send error: {e}")
    finally:
        if thumb_path and os.path.exists(thumb_path):
            try:
                os.remove(thumb_path)
            except Exception:
                pass


# ════════════════════════════════════════════════════════════════════════════
#  CORE AUTO-UPDATE JOB
#
#  MongoDB structure (per auto_watch doc):
#
#  subjects: [
#    {
#      subject_id, subject_name,
#      topics: [
#        {
#          topic_id, topic_name,
#          topic_thread_id,         ← Telegram forum topic ID
#          last_lecture_ids: [],
#          last_pdf_ids:     [],
#          last_quiz_ids:    [],
#          last_test_ids:    [],
#        }
#      ],
#      ts_topic_map: {}             ← {series_id: thread_id}  MOVED to subject level
#    }
#  ]
# ════════════════════════════════════════════════════════════════════════════

async def _run_auto_update(bot, doc):
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

    # Prime peer cache once per job run so all sends in this run are safe
    await _prime_peer(bot, sg_id, retries=5)

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
                return

        hdr = main.make_headers(token, appx_uid)
        updated_subjects = []

        for subj in doc.get("subjects", []):
            subj_id   = str(subj["subject_id"])
            subj_name = subj["subject_name"]
            updated_topics = []

            # ── FIX #2: ts_topic_map moved to subject level ──────────────
            # Previously this was nested inside each topic_doc, causing
            # get_test_series_list to run once per topic (duplicates).
            # Now fetched once per subject, after all topics are processed.
            ts_topic_map = subj.get("ts_topic_map", {})

            for topic_doc in subj.get("topics", []):
                topic_id   = str(topic_doc["topic_id"])
                topic_name = topic_doc["topic_name"]
                thread_id  = topic_doc.get("topic_thread_id")

                if not thread_id:
                    updated_topics.append(topic_doc)
                    continue

                seen_lec_ids  = set(topic_doc.get("last_lecture_ids", []))
                seen_pdf_ids  = set(topic_doc.get("last_pdf_ids",     []))
                seen_quiz_ids = set(topic_doc.get("last_quiz_ids",    []))
                new_lec_ids   = set()
                new_pdf_ids   = set()
                new_quiz_ids  = set()

                lectures = await main.get_lectures(
                    session, api_base, course_id, subj_id, topic_id, hdr
                )
                total = len(lectures)

                for i, lec in enumerate(lectures, 1):
                    lec_id = str(lec.get("id", ""))
                    title  = lec.get("Title") or f"Lecture_{lec_id}"
                    mat    = lec.get("material_type", "")
                    name   = main.safe_filename(title, i)
                    out_path = os.path.join(tmp_dir, name)

                    if not lec_id:
                        continue

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

                    # ── Quiz detection (same as run_lecture_loop) ─────────
                    QUIZ_TYPES = {"QUIZ", "TEST", "ATTEMPT", "EXERCISE", "ASSIGNMENT"}
                    _raw_qid_val = (lec.get("quiz_title_id") or
                                    lec.get("quizid") or lec.get("quiz_id") or "")
                    raw_qid = str(_raw_qid_val).strip()
                    if raw_qid in ("", "0", "-1", "None", "null"):
                        raw_qid = ""

                    is_pure_quiz = (
                        mat.upper() in QUIZ_TYPES
                        or ("quiz" in mat.lower() and mat.upper() != "VIDEO")
                    )
                    has_quiz = is_pure_quiz or bool(raw_qid)

                    if has_quiz and track_quiz:
                        # FIX #3: fall back to lec_id when raw_qid is empty,
                        # matching main.py: quiz_id = _raw_qid or str(lec_id)
                        quiz_id = raw_qid or lec_id
                        if quiz_id and quiz_id not in seen_quiz_ids:
                            new_quiz_ids.add(quiz_id)
                            try:
                                questions = await main.fetch_quiz(
                                    session, api_base, quiz_id, appx_uid, hdr
                                )
                            except Exception as e:
                                logger.error(f"[Scheduler] fetch_quiz qid={quiz_id}: {e}")
                                questions = []
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
                                    await _send_to_topic(
                                        bot, sg_id, thread_id, qfile, q_cap,
                                        is_video=False,
                                        display_name=main.safe_filename(title) + "_quiz.html",
                                    )
                                    try:
                                        os.remove(qfile)
                                    except Exception:
                                        pass
                            else:
                                logger.warning(
                                    f"[Scheduler] Quiz qid={quiz_id} no questions"
                                )

                    if is_pure_quiz:
                        continue

                    # ── Resolve video / PDF URL ───────────────────────────
                    vtitle, url, xor_key, vtype, class_pdfs, all_cands = \
                        await main.resolve_video(
                            session, api_base, course_id, lec_id, hdr,
                            prefer_quality=prefer_q
                        )

                    if not url and not class_pdfs and not all_cands:
                        continue

                    # ── PDF-type lecture ──────────────────────────────────
                    if vtype == "pdf":
                        if track_pdf and lec_id not in seen_pdf_ids:
                            new_pdf_ids.add(lec_id)
                            if url:
                                pr = await main.download_pdf(url, out_path)
                                if pr and os.path.exists(pr):
                                    await _send_to_topic(
                                        bot, sg_id, thread_id, pr, cc1,
                                        is_video=False,
                                        display_name=main.safe_filename(title) + ".pdf",
                                    )
                                    try:
                                        os.remove(pr)
                                    except Exception:
                                        pass
                            for idx2, (suffix, purl) in enumerate(class_pdfs, start=2):
                                if not purl:
                                    continue
                                pid = f"{lec_id}{suffix}"
                                if pid in seen_pdf_ids:
                                    continue
                                new_pdf_ids.add(pid)
                                pr2 = await main.download_pdf(
                                    purl, f"{out_path}_extra_{idx2}"
                                )
                                if pr2 and os.path.exists(pr2):
                                    cc_extra = (
                                        f"[📕] <code>{str(i).zfill(3)}) "
                                        f"{title[:55]}_p{idx2}.pdf</code>\n"
                                        f"<blockquote>Batch: {cname}</blockquote>\n"
                                        f"Extracted by ➤ {brand}"
                                    )
                                    await _send_to_topic(
                                        bot, sg_id, thread_id, pr2, cc_extra,
                                        is_video=False,
                                        display_name=main.safe_filename(
                                            f"{title}_p{idx2}"
                                        ) + ".pdf",
                                    )
                                    try:
                                        os.remove(pr2)
                                    except Exception:
                                        pass
                        continue

                    # ── PDFs attached to a video lecture ──────────────────
                    if track_pdf and class_pdfs:
                        for suffix, purl in class_pdfs:
                            if not purl:
                                continue
                            pid = f"{lec_id}{suffix}"
                            if pid in seen_pdf_ids:
                                continue
                            new_pdf_ids.add(pid)
                            pr = await main.download_pdf(purl, f"{out_path}{suffix}")
                            if pr and os.path.exists(pr):
                                await _send_to_topic(
                                    bot, sg_id, thread_id, pr, cc1,
                                    is_video=False,
                                    display_name=main.safe_filename(title) + ".pdf",
                                )
                                try:
                                    os.remove(pr)
                                except Exception:
                                    pass

                    # ── FIX #4: YouTube type — notify topic, skip download ─
                    # Scheduler has no yt-dlp cookie support for auto mode.
                    # Send a text message to the topic instead of silently skipping.
                    if vtype == "youtube":
                        if track_lec and lec_id not in seen_lec_ids:
                            new_lec_ids.add(lec_id)
                            try:
                                await bot.send_message(
                                    sg_id,
                                    f"🎬 <b>YouTube lecture (manual download needed)</b>\n"
                                    f"<code>{str(i).zfill(3)}) {title[:60]}</code>\n"
                                    f"<blockquote>Batch: {cname}</blockquote>\n"
                                    f"🔗 {url}",
                                    reply_to_message_id=thread_id,
                                )
                            except Exception as e:
                                logger.error(f"[Scheduler] YT notify error: {e}")
                        continue

                    # ── FIX #5: encrypted.m URL — extract key before download
                    # main.py splits on '*' to get appxkey when url contains
                    # 'encrypted.m'. download_item / _download_single_url do NOT
                    # do this split, so we must do it here before calling them.
                    if url and "encrypted.m" in url:
                        parts = url.split("*", 1)
                        url   = parts[0]
                        if len(parts) > 1 and not xor_key:
                            xor_key = parts[1]

                    # ── New video lecture ─────────────────────────────────
                    if track_lec and lec_id not in seen_lec_ids and url:
                        new_lec_ids.add(lec_id)
                        ok = await main.download_item(
                            vtitle or title, url, xor_key, vtype,
                            out_path, hdr=hdr,
                            class_pdfs=[], want_pdf=False,
                            all_candidates=all_cands
                        )
                        if ok:
                            vid_file = None
                            for ext in ("mp4", "mkv", "webm", "ts"):
                                fp = f"{out_path}.{ext}"
                                if os.path.exists(fp):
                                    vid_file = fp
                                    break
                            if vid_file:
                                vid_ext = os.path.splitext(vid_file)[-1].lstrip(".")
                                await _send_to_topic(
                                    bot, sg_id, thread_id, vid_file, cc,
                                    is_video=True,
                                    display_name=main.safe_filename(title) + f".{vid_ext}",
                                )
                                try:
                                    os.remove(vid_file)
                                except Exception:
                                    pass

                    await asyncio.sleep(0.5)

                # ── Persist updated seen-ID sets (lectures only) ──────────
                topic_doc["last_lecture_ids"] = list(seen_lec_ids | new_lec_ids)
                topic_doc["last_pdf_ids"]     = list(seen_pdf_ids | new_pdf_ids)
                topic_doc["last_quiz_ids"]    = list(seen_quiz_ids | new_quiz_ids)
                # Remove old per-topic ts_topic_map if it exists (migration)
                topic_doc.pop("ts_topic_map", None)
                topic_doc.pop("last_test_ids", None)
                updated_topics.append(topic_doc)

            # ── FIX #2: Test Series ONCE per subject ──────────────────────
            # Previously inside the topic loop → ran N times and sent
            # the same tests into every topic thread. Now runs once per
            # subject and uses a dedicated ts_topic_map at subject level.
            seen_test_ids = set(subj.get("last_test_ids", []))
            new_test_ids  = set()

            if track_tests:
                ts_list = await main.get_test_series_list(
                    session, api_base, appx_uid, course_id, hdr
                )

                for ts in ts_list:
                    series_id   = str(ts.get("id") or ts.get("test_series_id", ""))
                    series_name = (ts.get("title") or ts.get("name")
                                   or f"Series_{series_id}")
                    if not series_id:
                        continue

                    # Create / reuse a dedicated Telegram topic for this series
                    # FIX #duplicate-threads: _get_or_create_topic checks the
                    # map first so a topic is never created twice for the same
                    # series_id across runs.
                    ts_thread_id = await _get_or_create_topic(
                        sg_id, f"{subj_name} — {series_name}",
                        ts_topic_map, series_id
                    )
                    if not ts_thread_id:
                        # Fallback: use first topic thread of this subject
                        ts_thread_id = next(
                            (t.get("topic_thread_id")
                             for t in updated_topics
                             if t.get("topic_thread_id")),
                            None
                        )
                        if not ts_thread_id:
                            continue
                    await asyncio.sleep(0.5)

                    ts_subjects = await main.get_testseries_subjects(
                        session, api_base, series_id, hdr
                    )
                    matched_subj = None
                    for ts_subj in ts_subjects:
                        ts_sid   = str(ts_subj.get("id") or ts_subj.get("subjectid") or
                                       ts_subj.get("subject_id", ""))
                        ts_sname = (ts_subj.get("subject_name") or
                                    ts_subj.get("name") or "").lower()
                        if ts_sid == subj_id:
                            matched_subj = ts_subj
                            break
                        if (subj_name.lower() in ts_sname or
                                ts_sname in subj_name.lower()):
                            matched_subj = ts_subj
                            break
                    if not matched_subj and ts_subjects:
                        for ts_subj in ts_subjects:
                            ts_sid = str(ts_subj.get("id") or ts_subj.get("subjectid") or
                                         ts_subj.get("subject_id", "0"))
                            if ts_sid in ("0", ""):
                                matched_subj = ts_subj
                                break
                    if not matched_subj:
                        continue

                    matched_sid = str(matched_subj.get("id") or
                                      matched_subj.get("subjectid") or
                                      matched_subj.get("subject_id", "0"))
                    tests = await main.get_testseries_tests(
                        session, api_base, series_id, matched_sid, hdr
                    )

                    for test in tests:
                        test_id    = str(test.get("id") or test.get("test_id", ""))
                        test_title = (test.get("title") or test.get("name")
                                      or f"Test_{test_id}")
                        if not test_id or test_id in seen_test_ids:
                            continue
                        new_test_ids.add(test_id)

                        questions = await main.fetch_testseries_questions(
                            session, api_base, test, hdr
                        )
                        if not questions:
                            continue

                        tbase       = os.path.join(tmp_dir, f"test_{test_id}")
                        actual_path = main.save_quiz_html(
                            questions, tbase, test_title, watermark=brand
                        )
                        if not actual_path or not os.path.exists(actual_path):
                            continue

                        marks    = test.get("marks") or test.get("total_marks") or ""
                        mins     = test.get("time")  or test.get("duration")    or ""
                        meta     = []
                        if marks: meta.append(f"{marks} marks")
                        if mins:  meta.append(f"{mins} min")
                        meta_str = f" <code>[{', '.join(meta)}]</code>" if meta else ""
                        t_cap = (
                            f"📝 <code>{test_title[:55]}</code>{meta_str}\n"
                            f"<code>{len(questions)} questions</code>\n"
                            f"Series: {series_name} | {brand}"
                        )
                        await _send_to_topic(
                            bot, sg_id, ts_thread_id, actual_path, t_cap,
                            is_video=False,
                            display_name=main.safe_filename(test_title) + ".html",
                        )
                        try:
                            os.remove(actual_path)
                        except Exception:
                            pass
                        await asyncio.sleep(0.3)

            # ── Persist subject-level data ────────────────────────────────
            subj["topics"]        = updated_topics
            subj["ts_topic_map"]  = ts_topic_map
            subj["last_test_ids"] = list(seen_test_ids | new_test_ids)
            updated_subjects.append(subj)

        _aw().update_one(
            {"user_id": user_id, "auto_id": auto_id},
            {"$set": {"subjects": updated_subjects, "last_run_at": datetime.utcnow()}}
        )
        logger.info(f"[Scheduler] ✅ Auto #{auto_id} for user {user_id} done.")


async def run_all_due_autos(bot):
    now_ist = datetime.now(IST).strftime("%H:%M")
    due = list(_aw().find({"check_time_ist": now_ist, "is_active": True}))
    if not due:
        return
    logger.info(f"[Scheduler] {len(due)} auto(s) due at {now_ist} IST")
    for doc in due:
        try:
            await _run_auto_update(bot, doc)
        except Exception as e:
            logger.error(f"[Scheduler] Error in auto #{doc.get('auto_id')}: {e}")


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

                # FIX #duplicate-threads: reuse an existing Telegram thread if
                # this user already has one for this topic in this supergroup.
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
                    "ts_topic_map":  {},       # FIX #2: subject-level, not topic-level
                    "last_test_ids": [],       # FIX #2: subject-level seen tests
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

    await m.reply_text(
        f"🚀 <b>Running Auto #{auto_id} — {doc['course_name']} now…</b>\n"
        "Updates will be posted directly to the supergroup topics."
    )
    asyncio.create_task(_run_auto_update(bot, doc))


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
            # Delete regular topic threads
            for t in subj.get("topics", []):
                tid = t.get("topic_thread_id")
                if tid and sg_id:
                    await _delete_topic(sg_id, tid)
                    removed += 1
                # Also delete any legacy per-topic ts_topic_map entries
                for ts_tid in t.get("ts_topic_map", {}).values():
                    await _delete_topic(sg_id, int(ts_tid))
                    removed += 1
            # Delete subject-level test-series topic threads
            for ts_tid in subj.get("ts_topic_map", {}).values():
                await _delete_topic(sg_id, int(ts_tid))
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
                # Delete regular topic threads
                for t in s.get("topics", []):
                    tid = t.get("topic_thread_id")
                    if tid and sg_id:
                        await _delete_topic(sg_id, tid)
                    # Legacy per-topic ts_topic_map
                    for ts_tid in t.get("ts_topic_map", {}).values():
                        await _delete_topic(sg_id, int(ts_tid))
                # Subject-level test-series threads
                for ts_tid in s.get("ts_topic_map", {}).values():
                    await _delete_topic(sg_id, int(ts_tid))
                removed_names.append(s["subject_name"])
            else:
                kept_subjects.append(s)

        if not kept_subjects:
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
#
#  Add at the bottom of main.py, before bot.run():
#      from scheduler import start_scheduler
#      bot.loop.run_until_complete(start_scheduler(bot))
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
