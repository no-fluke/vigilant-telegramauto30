"""
scheduler.py — Auto-Update Scheduler for MRBERLIN Bot
Commands: /addauto, /myautos, /delauto, /runnow

Flow:
  /addauto  → user picks api_domain, login method, course, subjects,
              what to track (lectures/pdfs/quizzes/tests), supergroup,
              time in IST.
            → Bot auto-creates one supergroup topic per subject.
            → Saves everything to MongoDB.

  Scheduler → fires every minute, checks which autos are due (IST HH:MM match).
            → For each subject: fetches latest content, skips already-seen IDs,
              downloads & sends new items into the correct topic thread.
            → Caches Telegram file_ids in sent_tracker so re-uploads
              to NEW supergroups are instant (no re-download).

  /myautos  → lists all configured autos with status.
  /delauto  → delete entire auto or just specific subjects.
  /runnow   → trigger a specific auto immediately (without waiting for schedule).
"""

# ══════════════════════════════════════════════════════
#  IMPORTS
# ══════════════════════════════════════════════════════

import os
import re
import asyncio
import logging
import tempfile
import aiohttp
import requests

from datetime import datetime, timezone, timedelta
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, ChatAdminRequired, PeerIdInvalid
from pyrogram.handlers import MessageHandler

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from db import db
from vars import CREDIT, BOT_TOKEN

# ══════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════

MAX_AUTOS_PER_USER = 100

# IST = UTC+5:30
IST = timezone(timedelta(hours=5, minutes=30))

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════
#  MONGODB HELPERS
# ══════════════════════════════════════════════════════

def _aw():
    """auto_watch collection."""
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

# ══════════════════════════════════════════════════════
#  APPX API HELPERS
#  — reuses main.py functions via import; has local
#    fallbacks for token-refresh login
# ══════════════════════════════════════════════════════

def _get_main_fns():
    """
    Lazy-import heavy functions from main.py to avoid circular imports.
    Returns a namespace object with all needed callables.
    """
    import main as _m
    return _m

async def _fresh_login(session: aiohttp.ClientSession,
                       api_base: str, phone: str, password: str):
    """
    Non-interactive login using phone+password.
    Returns (token, appx_uid) or (None, None) on failure.
    Mirrors the email/password branch of appx_login() in main.py.
    """
    _m = _get_main_fns()
    headers = {
        **_m.APPX_HEADERS,
        "content-type": "application/x-www-form-urlencoded",
    }
    for endpoint in [
        f"{api_base}/post/userLogin",
        f"{api_base}/post/userLogin?extra_details=0",
        f"{api_base}/api/v2/user/login",
    ]:
        try:
            async with session.post(
                endpoint, headers=headers,
                data={"email": phone, "phone": phone,
                      "password": password, "source": "website",
                      "extra_details": "1"},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                r = await resp.json(content_type=None)
                d = r.get("data") or {}
                token = d.get("token") or r.get("token", "")
                if token:
                    uid = str(d.get("userid") or d.get("user_id") or
                              _m._extract_user_id_from_jwt(token) or "")
                    return token, uid
        except Exception:
            continue
    return None, None

async def _validate_token(session: aiohttp.ClientSession,
                           api_base: str, token: str, appx_uid: str) -> bool:
    _m = _get_main_fns()
    hdr = _m.make_headers(token, appx_uid)
    courses = await _m.get_courses(session, api_base, appx_uid, hdr)
    return bool(courses)

# ══════════════════════════════════════════════════════
#  SUPERGROUP TOPIC CREATOR
# ══════════════════════════════════════════════════════

async def _create_topic(bot: Client, supergroup_id: int, subject_name: str):
    """Create a forum topic; returns thread_id or None."""
    try:
        api_resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/createForumTopic",
            json={"chat_id": supergroup_id, "name": f"📚 {subject_name}"}
        ).json()
        if api_resp.get("ok"):
            tid = api_resp["result"]["message_thread_id"]
            logger.info(f"[Scheduler] Topic created: id={tid} name={subject_name}")
            return tid
        else:
            logger.error(f"[Scheduler] Topic creation failed: {api_resp}")
            return None
    except ChatAdminRequired:
        logger.error(f"[Scheduler] Bot is not admin in {supergroup_id}")
        return None
    except Exception as e:
        logger.error(f"[Scheduler] Topic creation failed: {e}")
        return None

# ══════════════════════════════════════════════════════
#  SEND HELPERS
# ══════════════════════════════════════════════════════

async def _safe_send(coro):
    """Wrap a send coroutine with FloodWait + retry."""
    for attempt in range(5):
        try:
            return await coro
        except FloodWait as fw:
            logger.warning(f"[Scheduler] FloodWait {fw.value}s")
            await asyncio.sleep(fw.value + 2)
        except Exception as e:
            logger.error(f"[Scheduler] Send error: {e}")
            return None
    return None

async def _send_to_topic(
        bot: Client,
        supergroup_id: int,
        topic_id: int,
        file_path: str | None,
        caption: str,
        is_video: bool = False):
    """
    Send a file to a supergroup topic thread.
    - Files ≤ 50 MB  → HTTP Bot API (avoids MTProto peer cache issues)
    - Files  > 50 MB → Pyrogram MTProto (Bot API limit is 50 MB)
    """
    if not file_path or not os.path.exists(file_path):
        logger.warning(f"[Scheduler] File not found: {file_path}")
        return

    file_size = os.path.getsize(file_path)
    use_pyrogram = file_size > 50 * 1024 * 1024   # > 50 MB

    if use_pyrogram:
        # Force Pyrogram to cache the peer before sending
        try:
            await bot.get_chat(supergroup_id)
        except Exception:
            pass
        if is_video:
            await _safe_send(bot.send_video(
                supergroup_id, file_path, caption=caption,
                reply_to_message_id=topic_id, supports_streaming=True
            ))
        else:
            await _safe_send(bot.send_document(
                supergroup_id, file_path, caption=caption,
                reply_to_message_id=topic_id
            ))
    else:
        # HTTP Bot API for files ≤ 50 MB
        api_url = f"https://api.telegram.org/bot{BOT_TOKEN}/"
        method  = "sendVideo"    if is_video else "sendDocument"
        field   = "video"        if is_video else "document"
        try:
            with open(file_path, "rb") as f:
                resp = requests.post(
                    api_url + method,
                    data={
                        "chat_id":            supergroup_id,
                        "message_thread_id":  topic_id,
                        "caption":            caption[:1024],
                        "parse_mode":         "Markdown",
                        "supports_streaming": "true" if is_video else "false",
                    },
                    files={field: f},
                    timeout=300
                ).json()
            if not resp.get("ok"):
                logger.error(f"[Scheduler] Send error: {resp.get('description')}")
        except Exception as e:
            logger.error(f"[Scheduler] Send error: {e}")

# ══════════════════════════════════════════════════════
#  CORE AUTO-UPDATE JOB
# ══════════════════════════════════════════════════════

async def _run_auto_update(bot: Client, doc: dict):
    """
    Process one auto_watch document:
      • For each subject → its topic thread in the supergroup
      • Fetches all lectures (across all appx-topics inside that subject)
      • Sends only NEW: lectures (video), PDFs, quizzes, test-series
      • Saves newly seen IDs back to MongoDB
    """
    _m         = _get_main_fns()
    user_id    = doc["user_id"]
    auto_id    = doc["auto_id"]
    api_base   = doc["api_base"]
    course_id  = str(doc["course_id"])
    course_name= doc.get("course_name", "Course")
    sg_id      = doc["supergroup_id"]
    brand      = doc.get("brand", CREDIT)
    subjects   = doc.get("subjects", [])

    # Credentials — stored as phone+password OR a saved token
    phone    = doc.get("phone", "")
    password = doc.get("password", "")
    token    = doc.get("appx_token", "")
    appx_uid = doc.get("appx_user_id", "")

    tmp_dir = os.path.join(tempfile.gettempdir(), "auto_dl")
    os.makedirs(tmp_dir, exist_ok=True)

    async with aiohttp.ClientSession() as session:

        # ── Token check / refresh ────────────────────────────────────────
        token_ok = False
        if token and appx_uid:
            token_ok = await _validate_token(session, api_base, token, appx_uid)

        if not token_ok:
            # Try re-login with saved credentials
            if phone and password:
                token, appx_uid = await _fresh_login(session, api_base, phone, password)
                if token:
                    token_ok = True
                    # Persist refreshed token
                    _aw().update_one(
                        {"user_id": user_id, "auto_id": auto_id},
                        {"$set": {"appx_token": token, "appx_user_id": appx_uid}}
                    )

            if not token_ok:
                logger.warning(
                    f"[Scheduler] Token expired and re-login failed "
                    f"for user {user_id} auto #{auto_id}"
                )
                _aw().update_one(
                    {"user_id": user_id, "auto_id": auto_id},
                    {"$set": {"is_active": False}}
                )
                try:
                    await bot.send_message(
                        user_id,
                        f"⚠️ **Auto #{auto_id} — Login Failed!**\n\n"
                        f"Could not log in to `{api_base}`.\n"
                        f"Auto #{auto_id} has been **paused**.\n"
                        f"Use /addauto to create a new one."
                    )
                except Exception:
                    pass
                return

        hdr = _m.make_headers(token, appx_uid)
        updated_subjects = []

        # ── Loop over each configured subject ────────────────────────────
        for subj in subjects:
            subj_id    = str(subj["subject_id"])
            subj_name  = subj["subject_name"]
            topic_id   = subj.get("topic_thread_id")  # Telegram thread id

            # What to track for this subject
            track_lec   = subj.get("track_lectures",  True)
            track_pdf   = subj.get("track_pdfs",      True)
            track_quiz  = subj.get("track_quizzes",   True)
            track_tests = subj.get("track_tests",     True)

            # Already-seen ID sets (avoid re-sending)
            seen_lec_ids  = set(subj.get("last_lecture_ids", []))
            seen_pdf_ids  = set(subj.get("last_pdf_ids",     []))
            seen_quiz_ids = set(subj.get("last_quiz_ids",    []))
            seen_test_ids = set(subj.get("last_test_ids",    []))

            new_lec_ids  = set()
            new_pdf_ids  = set()
            new_quiz_ids = set()
            new_test_ids = set()

            if not topic_id:
                logger.warning(
                    f"[Scheduler] No topic_thread_id for subject "
                    f"'{subj_name}' in auto #{auto_id} — skipping"
                )
                updated_subjects.append(subj)
                continue

            # ── Fetch all appx-topics inside this subject ────────────────
            appx_topics = await _m.get_topics(
                session, api_base, course_id, subj_id, hdr
            )

            for appx_topic in appx_topics:
                appx_topic_id = str(appx_topic.get("topicid", ""))
                appx_topic_name = appx_topic.get("topic_name", "")
                lectures = await _m.get_lectures(
                    session, api_base, course_id,
                    subj_id, appx_topic_id, hdr
                )

                for lec in lectures:
                    lec_id    = str(lec.get("id", ""))
                    lec_title = lec.get("Title") or lec.get("title") or f"Lecture_{lec_id}"
                    mat       = lec.get("material_type", "")

                    if not lec_id:
                        continue

                    QUIZ_TYPES = {"QUIZ", "TEST", "ATTEMPT", "EXERCISE", "ASSIGNMENT"}
                    raw_qid  = str(lec.get("quiz_title_id") or
                                   lec.get("quizid") or
                                   lec.get("quiz_id") or "")
                    has_quiz = (
                        mat.upper() in QUIZ_TYPES
                        or "quiz" in mat.lower()
                        or (raw_qid not in ("", "0", "-1", "None"))
                    )

                    # ── NEW QUIZ ─────────────────────────────────────────
                    if has_quiz and track_quiz and raw_qid and raw_qid not in seen_quiz_ids:
                        new_quiz_ids.add(raw_qid)
                        questions = await _m.fetch_quiz(
                            session, api_base, raw_qid, appx_uid, hdr
                        )
                        if questions:
                            quiz_path = os.path.join(tmp_dir, f"quiz_{raw_qid}.html")
                            _m.save_quiz_html(questions, quiz_path, lec_title,
                                              watermark=brand)
                            if os.path.exists(quiz_path):
                                cap = (
                                    f"📝 **New Quiz — {subj_name}**\n\n"
                                    f"❓ `{lec_title[:60]}`\n"
                                    f"📌 Topic: {appx_topic_name}\n"
                                    f"🔢 {len(questions)} Questions\n"
                                    f"🤖 Auto #{auto_id} | {brand}"
                                )
                                await _send_to_topic(
                                    bot, sg_id, topic_id, quiz_path, cap, is_video=False
                                )
                                try:
                                    os.remove(quiz_path)
                                except Exception:
                                    pass

                        # Pure quiz item — no video/pdf to process
                        if mat.upper() in QUIZ_TYPES or (
                                "quiz" in mat.lower() and mat.upper() != "VIDEO"):
                            continue

                    # ── Resolve video / PDF URL ──────────────────────────
                    vtitle, url, xor_key, vtype, class_pdfs, all_cands = \
                        await _m.resolve_video(
                            session, api_base, course_id, lec_id, hdr
                        )

                    # ── NEW PDF (pdf-type lecture) ────────────────────────
                    if vtype == "pdf" and track_pdf and lec_id not in seen_pdf_ids:
                        new_pdf_ids.add(lec_id)
                        if url:
                            pdf_path = os.path.join(tmp_dir, f"pdf_{lec_id}.pdf")
                            result   = await _m.download_pdf(url, pdf_path)
                            if result and os.path.exists(result):
                                cap = (
                                    f"📄 **New PDF — {subj_name}**\n\n"
                                    f"📝 `{lec_title[:60]}`\n"
                                    f"📌 Topic: {appx_topic_name}\n"
                                    f"🤖 Auto #{auto_id} | {brand}"
                                )
                                await _send_to_topic(
                                    bot, sg_id, topic_id, result, cap, is_video=False
                                )
                                try:
                                    os.remove(result)
                                except Exception:
                                    pass
                        # Extra attached PDFs
                        for suffix, purl in class_pdfs:
                            if not purl:
                                continue
                            pid     = f"pdf_{lec_id}{suffix}"
                            ppath   = os.path.join(tmp_dir, f"{pid}.pdf")
                            result2 = await _m.download_pdf(purl, ppath)
                            if result2 and os.path.exists(result2):
                                cap2 = (
                                    f"📄 **New PDF — {subj_name}**\n\n"
                                    f"📝 `{lec_title[:55]} {suffix}`\n"
                                    f"📌 Topic: {appx_topic_name}\n"
                                    f"🤖 Auto #{auto_id} | {brand}"
                                )
                                await _send_to_topic(
                                    bot, sg_id, topic_id,
                                    result2, cap2, is_video=False
                                )
                                try:
                                    os.remove(result2)
                                except Exception:
                                    pass
                        continue  # pdf-type lecture done

                    # ── PDFs attached to a VIDEO lecture ─────────────────
                    if track_pdf and class_pdfs:
                        for suffix, purl in class_pdfs:
                            if not purl:
                                continue
                            pid   = f"pdf_{lec_id}{suffix}"
                            if pid in seen_pdf_ids:
                                continue
                            new_pdf_ids.add(pid)
                            ppath   = os.path.join(tmp_dir, f"{pid}.pdf")
                            result3 = await _m.download_pdf(purl, ppath)
                            if result3 and os.path.exists(result3):
                                cap3 = (
                                    f"📄 **New PDF — {subj_name}**\n\n"
                                    f"📝 `{lec_title[:55]} {suffix}`\n"
                                    f"📌 Topic: {appx_topic_name}\n"
                                    f"🤖 Auto #{auto_id} | {brand}"
                                )
                                await _send_to_topic(
                                    bot, sg_id, topic_id,
                                    result3, cap3, is_video=False
                                )
                                try:
                                    os.remove(result3)
                                except Exception:
                                    pass

                    # ── NEW VIDEO LECTURE ────────────────────────────────
                    if track_lec and lec_id not in seen_lec_ids and url:
                        new_lec_ids.add(lec_id)
                        out_path = os.path.join(tmp_dir, f"lec_{lec_id}")
                        ok = await _m.download_item(
                            lec_title, url, xor_key, vtype,
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
                                cap = (
                                    f"📹 **New Lecture — {subj_name}**\n\n"
                                    f"`{lec_title[:60]}`\n"
                                    f"📌 Topic: {appx_topic_name}\n"
                                    f"🤖 Auto #{auto_id} | {brand}"
                                )
                                await _send_to_topic(
                                    bot, sg_id, topic_id, vid_file, cap, is_video=True
                                )
                                try:
                                    os.remove(vid_file)
                                except Exception:
                                    pass

                    await asyncio.sleep(0.5)

            # ── TEST SERIES for this subject — topic-wise ────────────────
            # Each test series gets its own sub-topic thread inside the
            # supergroup (auto-created on first run, ID saved to MongoDB).
            # New tests are detected via seen_test_ids and sent to the
            # correct series thread so everything stays neatly organised.
            if track_tests:
                # Fetch all test series for this course
                ts_list = await _m.get_test_series_list(
                    session, api_base, appx_uid, course_id, hdr
                )

                for ts in ts_list:
                    series_id   = str(ts.get("id") or ts.get("test_series_id", ""))
                    series_name = ts.get("title") or ts.get("name") or f"Series_{series_id}"
                    if not series_id:
                        continue

                    # ── Get or create a topic thread for this series ──────
                    # topic_thread_map is stored inside the subject doc:
                    #   subj["ts_topic_map"] = { series_id: thread_id }
                    ts_topic_map = subj.setdefault("ts_topic_map", {})
                    ts_thread_id = ts_topic_map.get(series_id)

                    if not ts_thread_id:
                        # Auto-create a new topic thread for this series
                        # named: "📋 SeriesName" under the subject's area
                        ts_thread_id = await _create_topic(
                            bot, sg_id,
                            f"{subj_name} — {series_name}"
                        )
                        if ts_thread_id:
                            ts_topic_map[series_id] = ts_thread_id
                            # Persist immediately so we don't recreate on next run
                            _aw().update_one(
                                {"user_id": user_id, "auto_id": auto_id,
                                 "subjects.subject_id": subj_id},
                                {"$set": {
                                    f"subjects.$.ts_topic_map.{series_id}": ts_thread_id
                                }}
                            )
                            await asyncio.sleep(0.5)  # avoid topic creation flood
                        else:
                            # Could not create topic — fall back to subject's main thread
                            ts_thread_id = topic_id

                    # ── Match test series subjects to our course subject ──
                    ts_subjects = await _m.get_testseries_subjects(
                        session, api_base, series_id, hdr
                    )

                    # Try to match by subject name or ID
                    matched_ts_subj = None
                    for ts_subj in ts_subjects:
                        ts_sid  = str(ts_subj.get("id") or
                                      ts_subj.get("subject_id") or
                                      ts_subj.get("subjectid", ""))
                        ts_sname = (ts_subj.get("subject_name") or
                                    ts_subj.get("name") or "").lower()
                        # Match by ID or by name similarity
                        if ts_sid == subj_id:
                            matched_ts_subj = ts_subj
                            break
                        if subj_name.lower() in ts_sname or ts_sname in subj_name.lower():
                            matched_ts_subj = ts_subj
                            break

                    # If no subject match, treat "All Tests" (subject_id=0) as fallback
                    if not matched_ts_subj and ts_subjects:
                        for ts_subj in ts_subjects:
                            ts_sid = str(ts_subj.get("id") or
                                         ts_subj.get("subjectid") or
                                         ts_subj.get("subject_id", "0"))
                            if ts_sid in ("0", ""):
                                matched_ts_subj = ts_subj
                                break

                    if not matched_ts_subj:
                        continue

                    matched_sid = str(matched_ts_subj.get("id") or
                                      matched_ts_subj.get("subjectid") or
                                      matched_ts_subj.get("subject_id", "0"))

                    # ── Fetch all tests for this subject in the series ───
                    tests = await _m.get_testseries_tests(
                        session, api_base, series_id, matched_sid, hdr
                    )

                    for test in tests:
                        test_id    = str(test.get("id") or test.get("test_id", ""))
                        test_title = (test.get("title") or
                                      test.get("test_title") or
                                      f"Test_{test_id}")
                        if not test_id or test_id in seen_test_ids:
                            continue
                        new_test_ids.add(test_id)

                        # Fetch questions
                        questions = await _m.fetch_testseries_questions(
                            session, api_base, test, hdr
                        )
                        if not questions:
                            continue

                        tpath = os.path.join(tmp_dir, f"test_{test_id}.html")
                        _m.save_quiz_html(
                            questions, tpath, test_title, watermark=brand
                        )
                        if os.path.exists(tpath):
                            marks = test.get("marks") or test.get("total_marks") or ""
                            mins  = test.get("time")  or test.get("duration")    or ""
                            meta  = []
                            if marks: meta.append(f"{marks} marks")
                            if mins:  meta.append(f"{mins} min")
                            meta_str = f" `[{', '.join(meta)}]`" if meta else ""

                            cap = (
                                f"📋 **New Test — {subj_name}**\n\n"
                                f"🗂️ Series: {series_name}\n"
                                f"📊 `{test_title[:55]}`{meta_str}\n"
                                f"🔢 {len(questions)} Questions\n"
                                f"🤖 Auto #{auto_id} | {brand}"
                            )
                            await _send_to_topic(
                                bot, sg_id, ts_thread_id,
                                tpath, cap, is_video=False
                            )
                            try:
                                os.remove(tpath)
                            except Exception:
                                pass
                        await asyncio.sleep(0.3)

            # ── Persist updated seen-ID sets ─────────────────────────────
            subj["last_lecture_ids"] = list(seen_lec_ids | new_lec_ids)
            subj["last_pdf_ids"]     = list(seen_pdf_ids | new_pdf_ids)
            subj["last_quiz_ids"]    = list(seen_quiz_ids | new_quiz_ids)
            subj["last_test_ids"]    = list(seen_test_ids | new_test_ids)
            updated_subjects.append(subj)

        # ── Save updated state to MongoDB ────────────────────────────────
        _aw().update_one(
            {"user_id": user_id, "auto_id": auto_id},
            {"$set": {
                "subjects":    updated_subjects,
                "last_run_at": datetime.utcnow()
            }}
        )
        logger.info(f"[Scheduler] ✅ Auto #{auto_id} for user {user_id} done.")


async def run_all_due_autos(bot: Client):
    """
    Called every minute by APScheduler.
    Finds all active autos whose check_time_ist matches current IST HH:MM.
    """
    now_ist = datetime.now(IST).strftime("%H:%M")
    due = list(_aw().find({"check_time_ist": now_ist, "is_active": True}))
    if not due:
        return
    logger.info(f"[Scheduler] {len(due)} auto(s) due at {now_ist} IST")
    for doc in due:
        try:
            await _run_auto_update(bot, doc)
        except Exception as e:
            logger.error(
                f"[Scheduler] Error in auto #{doc.get('auto_id')}: {e}"
            )


# ══════════════════════════════════════════════════════
#  /addauto COMMAND
# ══════════════════════════════════════════════════════

async def addauto_handler(bot: Client, m: Message):
    uid = m.from_user.id

    if _aw().count_documents({"user_id": uid}) >= MAX_AUTOS_PER_USER:
        await m.reply_text(
            f"❌ You have reached the **{MAX_AUTOS_PER_USER} auto-updates** limit.\n"
            "Use /delauto to remove one first."
        )
        return

    auto_id = _next_auto_id(uid)
    if auto_id is None:
        await m.reply_text("❌ No auto-ID available. Use /delauto to free one.")
        return

    _m = _get_main_fns()

    # ── Step 1: API Domain ───────────────────────────────────────────────
    ask = await m.reply_text(
        "**🔧 Auto-Setup — Step 1/7**\n\n"
        "🌐 **Enter your AppX API domain:**\n"
        "<blockquote>e.g. `parmaracademyapi.teachx.in`\n"
        "Send without `https://`\n\n"
        "Send /cancel to abort.</blockquote>"
    )
    try:
        msg = await bot.listen(m.chat.id, timeout=120)
    except Exception:
        await ask.edit_text("⏰ Timed out. Use /addauto again.")
        return
    if (msg.text or "").strip() == "/cancel":
        await m.reply_text("❌ Cancelled.")
        return
    raw_domain = (msg.text or "").strip()
    await msg.delete(); await ask.delete()
    api_base = f"https://{raw_domain}" if not raw_domain.startswith("http") else raw_domain

    # ── Step 2: Login method ─────────────────────────────────────────────
    ask = await m.reply_text(
        "**🔧 Auto-Setup — Step 2/7**\n\n"
        "🔐 **Login method:**\n\n"
        "`1` — Phone/Email + Password _(recommended — auto-refreshes token)_\n"
        "`2` — JWT Token only\n\n"
        "Send `1` or `2`:"
    )
    try:
        msg = await bot.listen(m.chat.id, timeout=60)
    except Exception:
        await ask.edit_text("⏰ Timed out.")
        return
    login_choice = (msg.text or "").strip()
    await msg.delete(); await ask.delete()

    phone = password = token = appx_uid = ""

    if login_choice == "2":
        # JWT token only
        ask = await m.reply_text("**Send your JWT Token:**")
        try:
            msg = await bot.listen(m.chat.id, timeout=120)
        except Exception:
            await ask.edit_text("⏰ Timed out."); return
        token = (msg.text or "").strip()
        await msg.delete(); await ask.delete()
        appx_uid = str(_m._extract_user_id_from_jwt(token) or "")
        if not appx_uid:
            ask2 = await m.reply_text("⚠️ Could not extract User ID. Send it manually:")
            try:
                msg2 = await bot.listen(m.chat.id, timeout=60)
                appx_uid = (msg2.text or "").strip()
                await msg2.delete(); await ask2.delete()
            except Exception:
                await ask2.edit_text("⏰ Timed out."); return
    else:
        # Phone + Password
        ask = await m.reply_text(
            "**🔧 Step 2a/7 — Phone / Email:**\n"
            "Send your registered phone or email:"
        )
        try:
            msg = await bot.listen(m.chat.id, timeout=120)
        except Exception:
            await ask.edit_text("⏰ Timed out."); return
        phone = (msg.text or "").strip()
        await msg.delete(); await ask.delete()

        ask = await m.reply_text("**🔧 Step 2b/7 — Password:**")
        try:
            msg = await bot.listen(m.chat.id, timeout=120)
        except Exception:
            await ask.edit_text("⏰ Timed out."); return
        password = (msg.text or "").strip()
        await msg.delete(); await ask.delete()

        wait = await m.reply_text("⏳ Logging in…")
        async with aiohttp.ClientSession() as session:
            token, appx_uid = await _fresh_login(session, api_base, phone, password)
        if not token:
            await wait.edit_text(
                "❌ **Login failed.** Check credentials and try /addauto again."
            )
            return
        await wait.edit_text("✅ Login successful!")

    # ── Validate token & fetch courses ───────────────────────────────────
    wait = await m.reply_text("⏳ Fetching your courses…")
    async with aiohttp.ClientSession() as session:
        hdr     = _m.make_headers(token, appx_uid)
        courses = await _m.get_courses(session, api_base, appx_uid, hdr)

    if not courses:
        await wait.edit_text(
            "❌ **No courses found or token is invalid.**\n"
            "Try /addauto again with correct credentials."
        )
        return

    # ── Step 3: Select course ────────────────────────────────────────────
    course_text = "**📚 Auto-Setup — Step 3/7 — Select Course**\n\nYour courses:\n\n"
    for i, c in enumerate(courses[:20], 1):
        name = c.get("course_name") or c.get("title") or f"Course {i}"
        course_text += f"`{i}.` {name}\n"
    course_text += "\n**Send the course number:**"
    await wait.edit_text(course_text)

    try:
        msg = await bot.listen(m.chat.id, timeout=60)
    except Exception:
        await m.reply_text("⏰ Timed out."); return
    try:
        ci     = int((msg.text or "").strip()) - 1
        assert 0 <= ci < len(courses)
    except Exception:
        await m.reply_text("❌ Invalid selection. Use /addauto again."); return
    await msg.delete()

    chosen_course = courses[ci]
    course_id     = str(chosen_course.get("id") or chosen_course.get("courseid", ""))
    course_name   = chosen_course.get("course_name") or chosen_course.get("title", "")

    # ── Step 4: Select subjects ──────────────────────────────────────────
    wait = await m.reply_text("⏳ Fetching subjects…")
    async with aiohttp.ClientSession() as session:
        hdr          = _m.make_headers(token, appx_uid)
        subjects_raw = await _m.get_subjects(session, api_base, course_id, hdr)

    if not subjects_raw:
        await wait.edit_text("❌ No subjects found for this course."); return

    subj_text = "**📖 Auto-Setup — Step 4/7 — Select Subjects**\n\nAvailable subjects:\n\n"
    for i, s in enumerate(subjects_raw[:30], 1):
        sname = s.get("subject_name") or s.get("name") or f"Subject {i}"
        subj_text += f"`{i}.` {sname}\n"
    subj_text += (
        "\n**Send subject numbers separated by commas.**\n"
        "_Example: `1,3,5` or `all` for all subjects_"
    )
    await wait.edit_text(subj_text)

    try:
        msg = await bot.listen(m.chat.id, timeout=120)
    except Exception:
        await m.reply_text("⏰ Timed out."); return
    raw_in = (msg.text or "").strip().lower()
    await msg.delete()
    if raw_in == "all":
        chosen_subjects = subjects_raw[:30]
    else:
        try:
            idxs            = [int(x.strip()) - 1 for x in raw_in.split(",")]
            chosen_subjects = [subjects_raw[i] for i in idxs if 0 <= i < len(subjects_raw)]
        except Exception:
            await m.reply_text("❌ Invalid selection. Use /addauto again."); return
    if not chosen_subjects:
        await m.reply_text("❌ No valid subjects selected."); return

    # ── Step 5: What to track ────────────────────────────────────────────
    ask = await m.reply_text(
        "**🔍 Auto-Setup — Step 5/7 — What to Track**\n\n"
        "`1` 📹 Lectures (videos)\n"
        "`2` 📄 PDFs\n"
        "`3` 📝 Lecture Quizzes\n"
        "`4` 📋 Test Series\n\n"
        "_Send numbers separated by commas._\n"
        "_e.g. `2,3` for PDFs + Quizzes | `1,2,3,4` for everything_\n\n"
        "<blockquote>📌 Test Series (`4`) automatically creates a separate "
        "topic thread per series inside the supergroup — no manual setup needed.</blockquote>"
    )
    try:
        msg = await bot.listen(m.chat.id, timeout=60)
    except Exception:
        await ask.edit_text("⏰ Timed out."); return
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
        await m.reply_text("❌ Nothing selected to track. Use /addauto again."); return

    # ── Step 6: Brand name ───────────────────────────────────────────────
    ask = await m.reply_text(
        "**🏷️ Auto-Setup — Step 6/7 — Brand Name**\n\n"
        "Send your channel name / watermark for captions\n"
        "_(e.g. `@MyChannel`)_\n"
        "or `/d` to use default:"
    )
    try:
        msg = await bot.listen(m.chat.id, timeout=30)
    except Exception:
        brand = CREDIT
        await ask.delete()
    else:
        raw_brand = (msg.text or "").strip()
        brand = CREDIT if raw_brand == "/d" else raw_brand
        await msg.delete(); await ask.delete()

    # ── Step 7: Supergroup + Time ────────────────────────────────────────
    ask = await m.reply_text(
        "**🏠 Auto-Setup — Step 7a/7 — Supergroup ID**\n\n"
        "Send the **Supergroup ID** with Topics enabled.\n"
        "<blockquote>e.g. `-1001234567890`\n\n"
        "✅ Make sure the bot is **admin** with\n"
        "   **Manage Topics** permission in that group.</blockquote>"
    )
    try:
        msg = await bot.listen(m.chat.id, timeout=60)
    except Exception:
        await ask.edit_text("⏰ Timed out."); return
    try:
        sg_id = int((msg.text or "").strip())
    except Exception:
        await m.reply_text("❌ Invalid Supergroup ID."); return
    await msg.delete(); await ask.delete()

    ask = await m.reply_text(
        "**🕒 Auto-Setup — Step 7b/7 — Schedule Time (IST)**\n\n"
        "What time should I check for new content daily?\n"
        "<blockquote>Send in **HH:MM** 24-hour IST format\n"
        "e.g. `08:00` → 8 AM | `20:30` → 8:30 PM</blockquote>"
    )
    try:
        msg = await bot.listen(m.chat.id, timeout=60)
    except Exception:
        await ask.edit_text("⏰ Timed out."); return
    time_input = (msg.text or "").strip()
    await msg.delete(); await ask.delete()

    if not re.match(r"^\d{2}:\d{2}$", time_input):
        await m.reply_text("❌ Invalid time. Use HH:MM like `09:00`. Try /addauto again.")
        return
    h, mi = map(int, time_input.split(":"))
    if not (0 <= h <= 23 and 0 <= mi <= 59):
        await m.reply_text("❌ Invalid time values. Use /addauto again."); return

    # ── Create supergroup topics ─────────────────────────────────────────
    creating = await m.reply_text(
        f"⏳ Creating **{len(chosen_subjects)}** topic(s) in your supergroup…"
    )
    subjects_docs = []
    failed_topics = []

    for s in chosen_subjects:
        sname = s.get("subject_name") or s.get("name") or "Subject"
        sid   = str(s.get("id") or s.get("subjectid") or s.get("subject_id", ""))
        tid   = await _create_topic(bot, sg_id, sname)
        if tid is None:
            failed_topics.append(sname)
            continue
        subjects_docs.append({
            "subject_id":       sid,
            "subject_name":     sname,
            "topic_thread_id":  tid,
            "track_lectures":   track_lec,
            "track_pdfs":       track_pdf,
            "track_quizzes":    track_quiz,
            "track_tests":      track_tests,
            "last_lecture_ids": [],
            "last_pdf_ids":     [],
            "last_quiz_ids":    [],
            "last_test_ids":    []
        })
        await asyncio.sleep(0.5)

    if not subjects_docs:
        await creating.edit_text(
            "❌ Could not create any topics.\n"
            "Make sure the bot is **admin** with **Manage Topics** permission."
        )
        return

    # ── Save to MongoDB ──────────────────────────────────────────────────
    doc = {
        "user_id":        uid,
        "auto_id":        auto_id,
        "api_base":       api_base,
        "course_id":      course_id,
        "course_name":    course_name,
        "appx_token":     token,
        "appx_user_id":   appx_uid,
        "phone":          phone,
        "password":       password,   # stored for auto token-refresh
        "supergroup_id":  sg_id,
        "check_time_ist": time_input,
        "brand":          brand,
        "subjects":       subjects_docs,
        "is_active":      True,
        "added_at":       datetime.utcnow(),
        "last_run_at":    None
    }
    _save_auto(doc)

    track_labels = []
    if track_lec:   track_labels.append("📹 Lectures")
    if track_pdf:   track_labels.append("📄 PDFs")
    if track_quiz:  track_labels.append("📝 Quizzes")
    if track_tests: track_labels.append("📋 Tests")

    result = (
        f"✅ **Auto #{auto_id} Created!**\n\n"
        f"📚 **Course:** {course_name}\n"
        f"📖 **Subjects:** {len(subjects_docs)} topic(s) created\n"
        f"🔍 **Tracking:** {' | '.join(track_labels)}\n"
        f"🕒 **Daily at:** `{time_input}` IST\n"
        f"🏠 **Supergroup:** `{sg_id}`\n"
        f"🏷️ **Brand:** `{brand}`\n"
    )
    if failed_topics:
        result += f"\n⚠️ Topic creation failed for: {', '.join(failed_topics)}"
    result += (
        f"\n\n**Commands:**\n"
        f"/myautos — view all autos\n"
        f"/runnow {auto_id} — run immediately\n"
        f"/delauto — delete an auto"
    )
    await creating.edit_text(result)


# ══════════════════════════════════════════════════════
#  /myautos COMMAND
# ══════════════════════════════════════════════════════

async def myautos_handler(bot: Client, m: Message):
    uid   = m.from_user.id
    autos = _get_autos(uid)

    if not autos:
        await m.reply_text(
            "📋 **No auto-updates configured yet.**\n\n"
            "Use /addauto to set one up!"
        )
        return

    text = f"📋 **Your Auto-Updates ({len(autos)}/{MAX_AUTOS_PER_USER})**\n\n"
    for doc in autos:
        subjects   = doc.get("subjects", [])
        subj_names = [s["subject_name"] for s in subjects]
        tracks     = []
        if subjects:
            s0 = subjects[0]
            if s0.get("track_lectures"):  tracks.append("📹 Lec")
            if s0.get("track_pdfs"):      tracks.append("📄 PDF")
            if s0.get("track_quizzes"):   tracks.append("📝 Quiz")
            if s0.get("track_tests"):     tracks.append("📋 Test")

        status   = "🟢 Active" if doc.get("is_active") else "🔴 Paused"
        last_run = doc.get("last_run_at")
        last_str = (last_run.strftime("%d %b %H:%M UTC")
                    if isinstance(last_run, datetime) else "Never")

        text += (
            f"**#{doc['auto_id']} — {doc['course_name']}**\n"
            f"  🌐 `{doc.get('api_base','?')}`\n"
            f"  📖 {', '.join(subj_names[:4])}{'…' if len(subj_names) > 4 else ''}\n"
            f"  🔍 {' + '.join(tracks) if tracks else 'Nothing'}\n"
            f"  🕒 `{doc.get('check_time_ist','?')}` IST | {status}\n"
            f"  🏠 `{doc.get('supergroup_id','?')}`\n"
            f"  ⏱️ Last run: {last_str}\n\n"
        )

    text += "_Use /runnow <id> to trigger now | /delauto to remove_"
    await m.reply_text(text)


# ══════════════════════════════════════════════════════
#  /runnow COMMAND  — manual immediate trigger
# ══════════════════════════════════════════════════════

async def runnow_handler(bot: Client, m: Message):
    uid  = m.from_user.id
    args = m.text.split()

    # Accept: /runnow  or  /runnow <auto_id>
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
            list_text = "**Which auto do you want to run?**\n\n"
            for a in autos:
                list_text += f"`{a['auto_id']}.` {a['course_name']} | {a.get('check_time_ist','?')} IST\n"
            list_text += "\nSend the auto number:"
            ask = await m.reply_text(list_text)
            try:
                msg = await bot.listen(m.chat.id, timeout=30)
                auto_id = int((msg.text or "").strip())
                await msg.delete(); await ask.delete()
            except Exception:
                await ask.edit_text("⏰ Timed out / invalid."); return

    doc = _get_auto(uid, auto_id)
    if not doc:
        await m.reply_text(f"❌ Auto #{auto_id} not found.")
        return

    await m.reply_text(
        f"🚀 **Running Auto #{auto_id} — {doc['course_name']} now…**\n"
        "You'll get a DM status after each subject finishes."
    )
    asyncio.create_task(_run_auto_update(bot, doc))


# ══════════════════════════════════════════════════════
#  /delauto COMMAND
# ══════════════════════════════════════════════════════

async def delauto_handler(bot: Client, m: Message):
    uid   = m.from_user.id
    autos = _get_autos(uid)

    if not autos:
        await m.reply_text("📋 You have no auto-updates to delete.")
        return

    text = "🗑️ **Delete Auto-Update**\n\nYour autos:\n\n"
    for doc in autos:
        text += (
            f"`{doc['auto_id']}.` **{doc['course_name']}** "
            f"| `{doc.get('check_time_ist','?')}` IST\n"
        )
    text += "\n**Send the auto number to delete:**"
    await m.reply_text(text)

    try:
        pick = await bot.listen(m.chat.id, timeout=60)
    except Exception:
        await m.reply_text("⏰ Timed out."); return
    try:
        auto_id = int((pick.text or "").strip())
    except Exception:
        await m.reply_text("❌ Invalid number."); return

    doc = _get_auto(uid, auto_id)
    if not doc:
        await m.reply_text(f"❌ Auto #{auto_id} not found."); return

    ask = await m.reply_text(
        f"**Delete Auto #{auto_id} — {doc['course_name']}**\n\n"
        "`1` — Delete entire auto (removes all supergroup topics)\n"
        "`2` — Remove specific subjects only\n\n"
        "**Send 1 or 2:**"
    )
    try:
        choice_msg = await bot.listen(m.chat.id, timeout=60)
    except Exception:
        await ask.edit_text("⏰ Timed out."); return
    choice = (choice_msg.text or "").strip()
    await choice_msg.delete(); await ask.delete()

    if choice == "1":
        sg_id    = doc.get("supergroup_id")
        subjects = doc.get("subjects", [])
        removed  = 0
        for s in subjects:
            tid = s.get("topic_thread_id")
            if tid and sg_id:
                try:
                    requests.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/deleteForumTopic",
                        json={"chat_id": sg_id, "message_thread_id": tid}
                    )
                    removed += 1
                except Exception:
                    pass
        _delete_auto(uid, auto_id)
        await m.reply_text(
            f"✅ **Auto #{auto_id} deleted.**\n"
            f"🗑️ Removed {removed} supergroup topic(s)."
        )

    elif choice == "2":
        subjects  = doc.get("subjects", [])
        subj_text = "**Select subjects to remove:**\n\n"
        for i, s in enumerate(subjects, 1):
            subj_text += f"`{i}.` {s['subject_name']}\n"
        subj_text += "\n_Send numbers separated by commas (e.g. `1,3`)_"
        await m.reply_text(subj_text)

        try:
            subj_pick = await bot.listen(m.chat.id, timeout=60)
        except Exception:
            await m.reply_text("⏰ Timed out."); return
        try:
            rm_idxs = [int(x.strip()) - 1 for x in (subj_pick.text or "").split(",")]
        except Exception:
            await m.reply_text("❌ Invalid input."); return

        sg_id         = doc.get("supergroup_id")
        removed_names = []
        kept_subjects = []

        for i, s in enumerate(subjects):
            if i in rm_idxs:
                tid = s.get("topic_thread_id")
                if tid and sg_id:
                    try:
                        requests.post(
                            f"https://api.telegram.org/bot{BOT_TOKEN}/deleteForumTopic",
                            json={"chat_id": sg_id, "message_thread_id": tid}
                        )
                    except Exception:
                        pass
                removed_names.append(s["subject_name"])
            else:
                kept_subjects.append(s)

        if not kept_subjects:
            _delete_auto(uid, auto_id)
            await m.reply_text(
                f"✅ All subjects removed — **Auto #{auto_id} fully deleted.**"
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


# ══════════════════════════════════════════════════════
#  SCHEDULER STARTUP — call once from main.py
# ══════════════════════════════════════════════════════

async def start_scheduler(bot: Client):
    """
    Call this once from main.py after the bot object is created.

    In main.py, add:
        from scheduler import start_scheduler
        ...
        # inside your startup / main() function, before bot.run():
        start_scheduler(bot)
    """
    _ensure_indexes()

    # Register command handlers
    for cmd, handler in [
        ("addauto",  addauto_handler),
        ("myautos",  myautos_handler),
        ("runnow",   runnow_handler),
        ("delauto",  delauto_handler),
    ]:
        bot.add_handler(
            MessageHandler(handler, filters.command(cmd) & filters.private)
        )

    # APScheduler — ticks every minute, matches IST HH:MM
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        run_all_due_autos,
        CronTrigger(minute="*"),
        args=[bot],
        id="auto_update_job",
        max_instances=1,
        coalesce=True
    )
    scheduler.start()

    logger.info(
        "[Scheduler] ✅ Started — checking every minute for due autos."
    )
    print(
        "\033[92m✓ Auto-Update Scheduler started "
        "(checks every minute for IST time match)\033[0m"
    )
