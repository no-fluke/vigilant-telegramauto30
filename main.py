"""
╔══════════════════════════════════════════════════════╗
║         AppX / Parmar Academy Downloader             ║
║           Telegram Bot Edition — v1.3                ║
║   Fixes applied:                                     ║
║     1. Multi-PDF per lecture (class_pdfs[1:] skip)  ║
║     2. MP4 URL freshness check + 1MB chunk aiohttp  ║
║     3. Option 6 — Download EVERYTHING in one shot   ║
║     4. Auto-Update Scheduler integrated              ║
╚══════════════════════════════════════════════════════╝

COMMANDS:
  /start   — Welcome screen
  /appx    — Start AppX course downloader (main flow)
  /stop    — Cancel running task
  /id      — Get your chat/user ID
  /info    — Your Telegram info
  /logs    — View bot logs
  /cookies — Upload YouTube cookies
  /getcookies — Get current cookies file
  /mfile   — Get this main.py file
  /reset   — Restart bot (owner only)
  /addauth <user_id> <days> — Add premium user (owner only)
  /rmauth  <user_id>        — Remove user (owner only)
  /users   — List premium users (owner only)
  /plan    — Check your subscription
  /broadcast — Broadcast to all users (owner only)
  /broadusers — List broadcast users (owner only)

  AUTO-UPDATE COMMANDS:
  /addauto   — Set up a new auto-update watcher
  /myautos   — List all your auto-update configs
  /delauto   — Delete an auto-update config
  /runnow    — Trigger an auto-update immediately
"""

import os
import re
import sys
import json
import base64
import mmap
import time
import asyncio
import struct
import shutil
import tempfile
import requests
import subprocess
import urllib
import urllib.parse
import logging as _logging

import aiohttp
import aiofiles

from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

from logs import logging
import saini as helper
from utils import progress_bar
from vars import API_ID, API_HASH, BOT_TOKEN, OWNER, CREDIT, AUTH_USERS, TOTAL_USERS, VIMEO_WORKER_URL
from db import db
from auth import (
    add_user_cmd, remove_user_cmd, list_users_cmd, my_plan_cmd,
    check_auth
)

import pyromod  # noqa: F401 — patches pyrogram.Client with .listen()
from pyrogram import Client, filters
from pyrogram.types import Message, InputMediaPhoto
from pyrogram.errors import FloodWait, PeerIdInvalid, UserIsBlocked, InputUserDeactivated
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.handlers import MessageHandler
from subprocess import getstatusoutput
from aiohttp import ClientSession

# ── Scheduler imports ─────────────────────────────────────────────────
from datetime import datetime, timezone, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ══════════════════════════════════════════════════════
#  CONSTANTS  (AppX / HAR-verified)
# ══════════════════════════════════════════════════════

AES_KEY     = b'638udh3829162018'
FALLBACK_IV = b'fedcba9876543210'

APPX_HEADERS = {
    "client-service":       "Appx",
    "auth-key":             "appxapi",
    "language":             "en",
    "device-type":          "ANDROID",
    "x-tenant-app-version": "420",
    "user-agent":           "okhttp/5.3.2",
}

CDN_HEADERS = {
    "Referer":         "https://player-strict.akamai.net.in/",
    "User-Agent":      "okhttp/5.3.2",
    "Accept-Encoding": "gzip",
}

MANIFEST_HEADERS = {
    "Referer":         "https://player-strict.akamai.net.in/",
    "User-Agent":      "Dalvik/2.1.0 (Linux; U; Android 11; RMX2001 Build/RP1A.200720.011)",
    "Accept-Encoding": "gzip",
    "Range":           "bytes=0-",
}

PDF_HEADERS = {
    "Referer":         "https://player-strict.akamai.net.in/",
    "User-Agent":      "Dalvik/2.1.0 (Linux; U; Android 11; RMX2001 Build/RP1A.200720.011)",
    "Accept":          "application/pdf,*/*",
    "Accept-Encoding": "gzip",
    "Connection":      "Keep-Alive",
}

MEDIA_REFERER = "https://player-strict.akamai.net.in/"
MEDIA_UA      = "Dalvik/2.1.0 (Linux; U; Android 11; RMX2001 Build/RP1A.200720.011)"

cookies_file_path = os.getenv("cookies_file_path", "youtube_cookies.txt")

# ── Proxy for signed CDN mp4 URLs that fail direct download ──────────
PROXY_BASE = "https://crispy-sporkvercel.vercel.app/?url="


def proxy_mp4_url(url: str) -> str:
    """Wrap a signed CDN mp4 URL through the middleman proxy."""
    if not url:
        return url
    return PROXY_BASE + urllib.parse.quote(url, safe="")

# ══════════════════════════════════════════════════════
#  SCHEDULER CONSTANTS
# ══════════════════════════════════════════════════════

MAX_AUTOS_PER_USER = 100
IST = timezone(timedelta(hours=5, minutes=30))

# ══════════════════════════════════════════════════════
#  BOT INIT
# ══════════════════════════════════════════════════════

bot = Client(
    "appx_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

processing_request = {}
cancel_requested   = {}
user_brand_name    = {}   # per-user session brand name (caption credit)

BUTTONS_CONTACT = InlineKeyboardMarkup([[
    InlineKeyboardButton(text="📞 Contact", url=f"tg://openmessage?user_id={OWNER}")
]])

# ══════════════════════════════════════════════════════
#  SAFE PROGRESS-MESSAGE EDIT
# ══════════════════════════════════════════════════════

async def safe_progress_edit(bot_client, chat_id, msg, text, **kwargs):
    try:
        await msg.edit_text(text, **kwargs)
        return msg
    except Exception:
        try:
            await msg.delete()
        except Exception:
            pass
        new_msg = await bot_client.send_message(chat_id, text, **kwargs)
        try:
            await bot_client.pin_chat_message(
                chat_id, new_msg.id, disable_notification=True
            )
        except Exception:
            pass
        return new_msg

# ══════════════════════════════════════════════════════
#  AES / XOR DECRYPTION
# ══════════════════════════════════════════════════════

def decrypt_url(enc_path: str) -> str:
    if not enc_path:
        return ""
    try:
        parts  = enc_path.split(":")
        ct_b64 = parts[0]
        iv     = base64.b64decode(parts[1]) if len(parts) > 1 else FALLBACK_IV
        ct     = base64.b64decode(ct_b64)
        cipher = AES.new(AES_KEY, AES.MODE_CBC, iv)
        try:
            raw  = unpad(cipher.decrypt(ct), AES.block_size)
            text = raw.decode("utf-8", errors="ignore")
        except Exception:
            cipher2 = AES.new(AES_KEY, AES.MODE_CBC, iv)
            text    = cipher2.decrypt(ct).decode("latin-1")
        m = re.search(r'https?://[\x20-\x7e]+', text)
        if not m:
            return ""
        return m.group(0).rstrip(
            "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10"
        ).strip()
    except Exception:
        return ""


def decode_b64(s: str) -> str:
    try:
        return base64.b64decode(s).decode("utf-8")
    except Exception:
        return ""


def xor_decrypt_file(path: str, key: str) -> bool:
    try:
        size = os.path.getsize(path)
        if size == 0:
            return False
        n = min(28, size)
        with open(path, "r+b") as f:
            with mmap.mmap(f.fileno(), length=n, access=mmap.ACCESS_WRITE) as mm:
                for i in range(n):
                    mm[i] ^= ord(key[i % len(key)])
        return True
    except Exception as e:
        print(f"[XOR] error: {e}")
        return False

# ══════════════════════════════════════════════════════
#  HTTP HELPERS
# ══════════════════════════════════════════════════════

def make_headers(token=None, user_id=None) -> dict:
    h = dict(APPX_HEADERS)
    if token:   h["authorization"] = token
    if user_id: h["user-id"] = str(user_id)
    return h


async def api_get(session: aiohttp.ClientSession, url: str,
                  headers: dict, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            await asyncio.sleep(0.3)
            async with session.get(
                    url, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 429:
                    wait = 15 * (attempt + 1)
                    print(f"[AppX] Rate limited — waiting {wait}s…")
                    await asyncio.sleep(wait)
                    continue
                if resp.status != 200:
                    return {}
                return await resp.json(content_type=None)
        except Exception as e:
            if attempt == retries - 1:
                print(f"[AppX] Request error: {e}")
            await asyncio.sleep(2)
    return {}


async def api_post(session: aiohttp.ClientSession, url: str,
                   headers: dict, data: dict) -> dict:
    try:
        async with session.post(url, headers=headers, data=data,
                                timeout=aiohttp.ClientTimeout(total=30)) as resp:
            return await resp.json(content_type=None)
    except Exception as e:
        print(f"[AppX] POST error: {e}")
        return {}

# ══════════════════════════════════════════════════════
#  APPX LOGIN
# ══════════════════════════════════════════════════════

def _extract_user_id_from_jwt(token: str) -> str:
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return ""
        payload_b64  = parts[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload = json.loads(
            base64.urlsafe_b64decode(payload_b64).decode("utf-8"))
        uid = (payload.get("id") or payload.get("userid")
               or payload.get("user_id") or "")
        return str(uid)
    except Exception:
        return ""


async def appx_login(bot_client: Client, m: Message, api_base: str):
    """Interactive AppX login. Returns (token, user_id, user_name)."""
    ask = await m.reply_text(
        "**🔐 AppX Login**\n\n"
        "Choose login method:\n"
        "**1** — Email / Password\n"
        "**2** — JWT Token\n\n"
        "Reply with `1` or `2`"
    )
    try:
        choice_msg: Message = await bot_client.listen(m.chat.id, timeout=60)
        choice = choice_msg.text.strip() if choice_msg.text else ""
        await choice_msg.delete()
        await ask.delete()
    except asyncio.TimeoutError:
        await ask.edit_text("⏰ Timed out.")
        return None, None, None

    if choice in ("2", "JWT", "jwt"):
        tok_ask = await m.reply_text("**Send your JWT Token:**")
        try:
            tok_msg: Message = await bot_client.listen(m.chat.id, timeout=120)
            token = tok_msg.text.strip() if tok_msg.text else ""
            await tok_msg.delete()
            await tok_ask.delete()
        except asyncio.TimeoutError:
            await tok_ask.edit_text("⏰ Timed out.")
            return None, None, None
        if not token:
            await m.reply_text("❌ Token is required.")
            return None, None, None
        user_id = _extract_user_id_from_jwt(token)
        if not user_id:
            uid_ask = await m.reply_text(
                "⚠️ Could not extract User ID from token.\n**Send your User ID manually:**")
            try:
                uid_msg: Message = await bot_client.listen(m.chat.id, timeout=60)
                user_id = uid_msg.text.strip() if uid_msg.text else ""
                await uid_msg.delete()
                await uid_ask.delete()
            except asyncio.TimeoutError:
                await uid_ask.edit_text("⏰ Timed out.")
                return None, None, None
        if not user_id:
            await m.reply_text("❌ User ID is required.")
            return None, None, None
        return token, user_id, f"User #{user_id}"

    # Email/Password login
    email_ask = await m.reply_text("**📧 Send your Email / Mobile:**")
    try:
        email_msg: Message = await bot_client.listen(m.chat.id, timeout=120)
        email = email_msg.text.strip() if email_msg.text else ""
        await email_msg.delete()
        await email_ask.delete()
    except asyncio.TimeoutError:
        await email_ask.edit_text("⏰ Timed out.")
        return None, None, None

    pass_ask = await m.reply_text("**🔒 Send your Password:**")
    try:
        pass_msg: Message = await bot_client.listen(m.chat.id, timeout=120)
        password = pass_msg.text.strip() if pass_msg.text else ""
        await pass_msg.delete()
        await pass_ask.delete()
    except asyncio.TimeoutError:
        await pass_ask.edit_text("⏰ Timed out.")
        return None, None, None

    if not email or not password:
        await m.reply_text("❌ Email and password are required.")
        return None, None, None

    status = await m.reply_text("⏳ **Logging in…**")
    async with aiohttp.ClientSession() as session:
        r = await api_post(session, f"{api_base}/post/userLogin",
            headers={**APPX_HEADERS,
                     "content-type": "application/x-www-form-urlencoded"},
            data={"email": email, "password": password,
                  "devicetoken": f"tgbot_{int(time.time())}",
                  "mydeviceid": "telegram_bot"})

        if r.get("status") == 200:
            d = r["data"]
            await status.edit_text(
                f"✅ **Logged in as:** `{d.get('name', email)}`\n\n"
                f"**Save this JWT token for next time:**\n`{d['token']}`"
            )
            return d["token"], str(d["userid"]), d.get("name", email)

        r2 = await api_post(session,
            f"{api_base}/post/userLogin?extra_details=0",
            headers={"auth-key": "appxapi", "client-service": "Appx",
                     "source": "website", "user-agent": "okhttp/5.3.2",
                     "content-type": "application/x-www-form-urlencoded"},
            data={"email": email, "phone": email, "password": password,
                  "source": "website", "extra_details": "1"})

        if r2.get("status") == 200:
            d = r2["data"]
            await status.edit_text(
                f"✅ **Logged in as:** `{d.get('name', email)}`\n\n"
                f"**Save this JWT token for next time:**\n`{d['token']}`"
            )
            return d["token"], str(d["userid"]), d.get("name", email)

        msg = r.get("message") or r2.get("message") or "Unknown error"
        await status.edit_text(f"❌ **Login failed:** `{msg}`")
        return None, None, None

# ══════════════════════════════════════════════════════
#  APPX API — COURSES / SUBJECTS / TOPICS / LECTURES
# ══════════════════════════════════════════════════════

async def get_courses(session, api_base, user_id, hdr):
    r = await api_get(session,
        f"{api_base}/get/mycoursev2?userid={user_id}&type=all", hdr)
    if r.get("data"):
        return r["data"]
    r2 = await api_get(session,
        f"{api_base}/get/get_all_purchases?userid={user_id}&item_type=10", hdr)
    if r2.get("data"):
        courses = []
        for item in r2["data"]:
            for ct in item.get("coursedt", []):
                courses.append(ct)
        return courses
    return []


async def get_subjects(session, api_base, course_id, hdr):
    r = await api_get(session,
        f"{api_base}/get/allsubjectfrmlivecourseclass"
        f"?courseid={course_id}&start=-1", hdr)
    return r.get("data", [])


async def get_topics(session, api_base, course_id, subject_id, hdr):
    r = await api_get(session,
        f"{api_base}/get/alltopicfrmlivecourseclass"
        f"?courseid={course_id}&subjectid={subject_id}&start=-1", hdr)
    return sorted(r.get("data", []), key=lambda x: x.get("topicid", 0))


async def get_lectures(session, api_base, course_id, subject_id, topic_id, hdr):
    url = (f"{api_base}/get/livecourseclassbycoursesubtopconceptapiv3"
           f"?courseid={course_id}&subjectid={subject_id}"
           f"&topicid={topic_id}&conceptid=&start=-1")
    r = await api_get(session, url, hdr)
    return sorted(r.get("data", []), key=lambda x: x.get("id", 0))


async def get_folder_contents(session, api_base, course_id, parent_id, hdr):
    r = await api_get(session,
        f"{api_base}/get/folder_contentsv2"
        f"?course_id={course_id}&parent_id={parent_id}", hdr)
    return r.get("data", [])

# ══════════════════════════════════════════════════════
#  CDN / URL HELPERS
# ══════════════════════════════════════════════════════

_CDN_MIGRATIONS = {
    "appxcontent.kaxa.in":         "appx-content-v2.classx.co.in",
    "appx-content.classx.co.in":   "appx-content-v2.classx.co.in",
    "appxcontentcdn.classx.co.in":  "appx-content-v2.classx.co.in",
}


def _norm_q(q) -> str:
    s = str(q).strip()
    if s.isdigit():
        return s + "p"
    return s


def _migrate_cdn(url: str) -> str:
    if not url:
        return url
    for old, new in _CDN_MIGRATIONS.items():
        if old in url:
            return url.replace(old, new)
    return url


def _is_drm_url(url: str) -> bool:
    if not url:
        return False
    lower = url.lower()
    if "hls-drm" in lower:
        return True
    if "transcoded-recordings.classx.co.in" in lower and "drm-hls" in lower:
        return True
    return False


def _is_mp4_url(url: str) -> bool:
    if not url:
        return False
    lower = url.lower()
    path  = lower.split("?")[0]
    if path.endswith(".pdf"):
        return False
    if "static-db.appx.co.in" in lower:
        return True
    if "appx-content-v2.classx.co.in" in lower:
        return True
    if path.endswith(".mp4"):
        return True
    return False


def _mp4_headers(url: str) -> dict:
    from urllib.parse import urlparse
    host = urlparse(url).netloc
    base = {
        "Referer":         MEDIA_REFERER,
        "Icy-MetaData":    "1",
        "Accept-Encoding": "identity",
        "User-Agent":      MEDIA_UA,
        "Host":            host,
        "Connection":      "Keep-Alive",
        "Range":           "bytes=0-",
    }
    if "appx-content-v2.classx.co.in" in url.lower():
        return {
            "client-service":       "Appx",
            "auth-key":             "appxapi",
            "language":             "en",
            "device-type":          "ANDROID",
            "x-tenant-app-version": "420",
            **base,
        }
    return base


def _best_download_link(d: dict, prefer_quality: str = "720p") -> str:
    raw   = d.get("download_links", "")
    links = []
    if isinstance(raw, list):
        links = raw
    elif isinstance(raw, str) and raw.strip() not in ("", "[]"):
        try:
            links = json.loads(raw.replace("'", '"'))
        except Exception:
            pass
    if not links:
        return decrypt_url(d.get("download_link", ""))
    quality_order = [prefer_quality, "1080p", "720p", "480p", "360p", "240p"]
    by_quality: dict = {}
    for lnk in links:
        q   = _norm_q(lnk.get("bitrate") or lnk.get("quality") or "")
        dec = decrypt_url(lnk.get("path", ""))
        if dec:
            by_quality[q] = dec
    for q in quality_order:
        if q in by_quality:
            return by_quality[q]
    return next(iter(by_quality.values()), decrypt_url(d.get("download_link", "")))


def decrypt_all_links(d: dict) -> list:
    seen: set   = set()
    results: list = []

    def _add(quality: str, url: str):
        if url and url not in seen:
            seen.add(url)
            results.append((quality or "?", url))

    for link in d.get("encrypted_links", []):
        q = _norm_q(link.get("quality") or "?")
        for field in ("path", "backup_url", "backup_url2"):
            raw = link.get(field, "")
            if raw:
                url = _migrate_cdn(decrypt_url(raw))
                if url:
                    _add(q, url)

    raw_dl  = d.get("download_links", "")
    dl_list = raw_dl if isinstance(raw_dl, list) else []
    if isinstance(raw_dl, str) and raw_dl.strip() not in ("", "[]"):
        try:
            dl_list = json.loads(raw_dl.replace("'", '"'))
        except Exception:
            pass
    for lnk in dl_list:
        q = _norm_q(lnk.get("bitrate") or lnk.get("quality") or "?")
        for field in ("path", "backup_url", "backup_url2"):
            raw = lnk.get(field, "")
            if raw:
                url = _migrate_cdn(decrypt_url(raw))
                if url:
                    _add(q, url)

    fl = _migrate_cdn(decrypt_url(d.get("file_link", "")))
    if fl:
        _add("file_link", fl)
    dl = _migrate_cdn(decrypt_url(d.get("download_link", "")))
    if dl:
        _add("download_link", dl)

    return results


def _resolve_pdf_url(pdf_link: str, pdf_key: str) -> str:
    if not pdf_link:
        return ""
    if pdf_link.startswith("http"):
        return _migrate_cdn(pdf_link)
    url = decrypt_url(pdf_link)
    if url:
        return _migrate_cdn(url)
    if pdf_key and ":" in pdf_key:
        iv_b64   = pdf_key.split(":", 1)[1]
        ct_part  = pdf_link.split(":", 1)[0]
        combined = ct_part + ":" + iv_b64
        url      = decrypt_url(combined)
        if url:
            return _migrate_cdn(url)
    if pdf_key:
        url = decrypt_url(pdf_key)
        if url and url.startswith("http"):
            return _migrate_cdn(url)
    return ""


def _extract_class_pdfs(d: dict) -> list:
    seen: set = set()
    pdfs: list = []

    def _add(suffix: str, url: str):
        url = (url or "").strip()
        if url and url not in seen:
            seen.add(url)
            pdfs.append((suffix, url))

    p1 = _resolve_pdf_url(d.get("pdf_link", ""), d.get("pdf_encryption_key", ""))
    if not p1:
        p1 = d.get("pdf_summary_link", "") or d.get("notes_link", "") or d.get("notes_pdf", "")
    _add("", p1)

    p2 = _resolve_pdf_url(d.get("pdf_link2", ""), d.get("pdf2_encryption_key", ""))
    if not p2:
        p2 = d.get("pdf2_summary_link", "")
    _add("_2", p2)

    for suffix, key in [
        ("_3",  "pdf_link3"),
        ("_4",  "pdf_link4"),
        ("_n",  "notes"),
        ("_n2", "notes2"),
        ("_c",  "classPdf"),
        ("_c2", "classPdf2"),
        ("_u",  "pdf_url"),
        ("_u2", "pdf_url2"),
        ("_d",  "download_pdf"),
        ("_s",  "study_material"),
    ]:
        val = d.get(key, "")
        if val:
            resolved = _resolve_pdf_url(val, "") or val
            if resolved.startswith("http"):
                _add(suffix, resolved)

    return pdfs


def is_direct_video_url(url: str) -> bool:
    if not url:
        return False
    lower = url.lower()
    return (
        ".m3u8" not in lower
        and (
            "encrypted-" in lower
            or "/encrypted" in lower
            or any(h in lower for h in [
                "static-trans-v2.appx.co.in",
                "static-trans.appx.co.in",
                "cdn.appx.co.in",
                "akamai.net.in",
                "static-wsb.appx.co.in",
                "static-db-v2.appx.co.in",
            ])
        )
    )

# ══════════════════════════════════════════════════════
#  VIDEO URL RESOLVER
# ══════════════════════════════════════════════════════

async def resolve_video(session, api_base, course_id, video_id, hdr,
                        prefer_quality: str = "720p"):
    api_url = (f"{api_base}/get/fetchVideoDetailsById"
               f"?course_id={course_id}&video_id={video_id}"
               f"&ytflag=0&folder_wise_course=0")
    r = await api_get(session, api_url, hdr)
    d = r.get("data", {})
    if not d:
        return None, None, None, None, [], []

    title         = d.get("Title") or f"video_{video_id}"
    class_pdfs    = _extract_class_pdfs(d)
    all_candidates = decrypt_all_links(d)

    yt_enc = d.get("video_id", "")
    if yt_enc:
        yt_id = decrypt_url(yt_enc)
        if yt_id and not yt_id.startswith("http"):
            return (title, f"https://youtu.be/{yt_id}",
                    None, "youtube", class_pdfs, all_candidates)

    if str(d.get("ytFlag", "")) == "1" and d.get("embed_url"):
        return (title, d["embed_url"],
                None, "youtube", class_pdfs, all_candidates)

    ALL_QUALITIES = ["1080p", "720p", "480p", "360p", "240p", "144p"]
    by_quality: dict = {}
    for qlabel, cand_url in all_candidates:
        if cand_url and ".pdf" not in cand_url and not _is_drm_url(cand_url):
            if qlabel not in by_quality:
                by_quality[qlabel] = cand_url

    prefer_order = [prefer_quality] + [q for q in ALL_QUALITIES if q != prefer_quality] + ["file_link", "download_link"]
    chosen_url   = None
    for q in prefer_order:
        if q in by_quality:
            chosen_url = by_quality[q]
            break
    if not chosen_url and by_quality:
        chosen_url = next(iter(by_quality.values()))

    if chosen_url:
        vtype = "mp4" if _is_mp4_url(chosen_url) else "hls"

        pref_idx = ALL_QUALITIES.index(prefer_quality) if prefer_quality in ALL_QUALITIES else 1
        lower    = ALL_QUALITIES[pref_idx + 1:]
        higher   = list(reversed(ALL_QUALITIES[:pref_idx]))
        fallback_order = [prefer_quality] + lower + higher + ["file_link", "download_link"]

        by_quality_all: dict = {}
        for qlabel, cand_url in all_candidates:
            if not _is_drm_url(cand_url):
                by_quality_all.setdefault(qlabel, []).append(cand_url)

        seen_urls: set = set()
        ordered: list  = []

        for q in fallback_order:
            for url_q in by_quality_all.get(q, []):
                if url_q not in seen_urls:
                    seen_urls.add(url_q)
                    ordered.append((q, url_q))

        for qlabel, cand_url in all_candidates:
            if cand_url not in seen_urls and not _is_drm_url(cand_url):
                seen_urls.add(cand_url)
                ordered.append((qlabel, cand_url))

        return title, chosen_url, None, vtype, class_pdfs, ordered

    quality_rank = {"1080p": 0, "720p": 1, "480p": 2, "360p": 3, "240p": 4}
    enc_links = sorted(
        d.get("encrypted_links", []),
        key=lambda x: quality_rank.get(_norm_q(x.get("quality", "")), 99)
    )
    for link in enc_links:
        raw_path = link.get("path", "")
        raw_key  = link.get("key",  "")
        if not raw_path:
            continue
        video_url = _migrate_cdn(decrypt_url(raw_path))
        if not video_url or _is_drm_url(video_url):
            continue
        xor_key = decode_b64(decrypt_url(raw_key)) if raw_key else ""
        vtype   = "mp4" if _is_mp4_url(video_url) else "hls"
        return title, video_url, xor_key or None, vtype, class_pdfs, all_candidates

    if class_pdfs:
        primary_pdf = class_pdfs[0][1]
        extra_pdfs  = class_pdfs[1:]
        return title, primary_pdf, None, "pdf", extra_pdfs, all_candidates

    return title, None, None, None, [], all_candidates

# ══════════════════════════════════════════════════════
#  QUIZ / TEST SERIES
# ══════════════════════════════════════════════════════

async def fetch_quiz(session, api_base, quiz_title_id: str,
                     user_id: str, hdr: dict) -> list:
    endpoints = [
        f"{api_base}/get/quizquestion?start=-1&quizid={quiz_title_id}",
        f"{api_base}/get/quiztitlebyid?quiz_title_id={quiz_title_id}&userid={user_id}",
        f"{api_base}/get/quiztitlebyid?quiztitleid={quiz_title_id}&userid={user_id}",
        f"{api_base}/get/quizbyid?quiz_id={quiz_title_id}&userid={user_id}",
    ]
    for url in endpoints:
        r = await api_get(session, url, hdr)
        data = r.get("data") or []
        if isinstance(data, list) and len(data) > 0:
            return data
        if isinstance(data, dict):
            for key in ("questions", "question_list", "quiz_questions", "data"):
                nested = data.get(key)
                if isinstance(nested, list) and nested:
                    return nested
    return []


def _strip_html(raw: str) -> str:
    if not raw:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', raw, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = (text
            .replace('&nbsp;', ' ')
            .replace('&amp;', '&')
            .replace('&lt;', '<')
            .replace('&gt;', '>')
            .replace('&quot;', '"')
            .replace('&#39;', "'"))
    return re.sub(r'\s+', ' ', text).strip()


def save_quiz_html(questions: list, out_path: str, title: str, watermark: str = ""):
    """Save quiz using quiz_template.html from the bot's root directory."""
    if not questions:
        return None
    out = f"{out_path}_quiz.html"
    try:
        template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "quiz_template.html")
        with open(template_path, "r", encoding="utf-8") as f:
            template = f.read()

        import json as _json
        js_questions = []
        for idx, q in enumerate(questions, 1):
            q_text = (
                q.get("question") or q.get("question_en")
                or q.get("que")   or q.get("title") or ""
            )
            answer_raw = str(
                q.get("answer") or q.get("correct_answer")
                or q.get("correct_option") or q.get("correct") or "1"
            ).strip()
            try:
                int(answer_raw)
                answer_str = answer_raw
            except ValueError:
                answer_str = str(ord(answer_raw.upper()) - ord("A") + 1)

            obj = {
                "id":             idx,
                "question":       q_text,
                "answer":         answer_str,
                "correct_score":  q.get("correct_score",  1),
                "negative_score": q.get("negative_score", 0),
                "solution_text":  q.get("solution_text") or q.get("explanation") or q.get("solution") or "",
            }
            for n in range(1, 6):
                val = q.get(f"option_{n}", "")
                if val:
                    obj[f"option_{n}"] = val
            if "option_1" not in obj:
                raw_list = q.get("options") or q.get("option") or []
                if isinstance(raw_list, list):
                    for oi, opt in enumerate(raw_list, 1):
                        t = (opt.get("text") or opt.get("option") or ""
                             if isinstance(opt, dict) else str(opt))
                        if t:
                            obj[f"option_{oi}"] = t

            js_questions.append(obj)

        questions_array = _json.dumps(js_questions, ensure_ascii=False, indent=2)
        seconds = max(60, len(questions) * 120)

        wm_text = watermark if watermark else "RANKUP"
        html = template
        html = html.replace("{quiz_name}",        title)
        html = html.replace("{seconds}",          str(seconds))
        html = html.replace("{questions_array}",  questions_array)
        html = html.replace(">RANKUP<",  f">{wm_text}<")
        html = html.replace(">@RANKUP<", f">@{wm_text}<")

        with open(out, "w", encoding="utf-8") as f:
            f.write(html)
        return out
    except Exception as e:
        print(f"[Quiz] save error: {e}")
        return None


def save_quiz_txt(questions: list, out_path: str, title: str, watermark: str = ""):
    return save_quiz_html(questions, out_path, title, watermark)


async def get_test_series_list(session, api_base, user_id, course_id, hdr) -> list:
    r = await api_get(session,
        f"{api_base}/get/test_seriesbycourseid"
        f"?start=-1&userid={user_id}&courseid={course_id}"
        f"&folder_wise_course=0", hdr)
    data = r.get("data") or r.get("result") or []
    return data if isinstance(data, list) else []


async def get_testseries_subjects(session, api_base, series_id, hdr) -> list:
    r = await api_get(session,
        f"{api_base}/get/testseries_subjects?testseries_id={series_id}", hdr)
    data = r.get("data") or r.get("subjects") or r.get("result") or []
    return data if isinstance(data, list) else []


async def get_testseries_tests(session, api_base, series_id, subject_id, hdr) -> list:
    all_tests = []
    start     = 0
    while True:
        r = await api_get(session,
            f"{api_base}/get/test_titlev2"
            f"?testseriesid={series_id}&subject_id={subject_id}"
            f"&chapter_id=-1&userid=0&search=&start={start}", hdr)
        items = (r.get("data") or r.get("tests") or
                 r.get("test_titles") or r.get("result") or [])
        if not isinstance(items, list) or not items:
            break
        all_tests.extend(items)
        if len(items) < 10:
            break
        start += len(items)
        await asyncio.sleep(0.3)
    return all_tests


async def fetch_testseries_questions(session, api_base, test: dict, hdr) -> list:
    cdn_url = test.get("test_questions_url") or ""
    if cdn_url:
        try:
            async with aiohttp.ClientSession() as cdn_sess:
                async with cdn_sess.get(cdn_url, timeout=aiohttp.ClientTimeout(total=30),
                                        headers={"User-Agent": "okhttp/5.3.2"}) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        qs   = (data if isinstance(data, list)
                                else data.get("questions") or data.get("data") or [])
                        if qs:
                            return qs
        except Exception:
            pass
    test_id = str(test.get("id") or test.get("test_id") or "")
    if test_id:
        r  = await api_get(session,
            f"{api_base}/get/quizquestion?start=-1&quizid={test_id}", hdr)
        qs = r.get("data") or []
        if isinstance(qs, list) and qs:
            return qs
    return []

# ══════════════════════════════════════════════════════
#  TOOL DETECTION
# ══════════════════════════════════════════════════════

def _find_tool(*candidates: str):
    for name in candidates:
        found = shutil.which(name)
        if found:
            return found
    return None

_YTDLP  = _find_tool("yt-dlp", "yt_dlp")
_FFMPEG = _find_tool("ffmpeg")
_ARIA2C = _find_tool("aria2c")

# ══════════════════════════════════════════════════════
#  FILENAME HELPERS
# ══════════════════════════════════════════════════════

def safe_filename(name: str, idx: int = None) -> str:
    name = name[:80]
    for ch in r'/\:*?"<>|' + "\n":
        name = name.replace(ch, "_")
    name = name.strip(". ")
    prefix = f"{str(idx).zfill(4)}_" if idx is not None else ""
    return prefix + name

# ══════════════════════════════════════════════════════
#  MP4 URL FRESHNESS CHECK
# ══════════════════════════════════════════════════════

async def _check_url_fresh(url: str, headers: dict, timeout: int = 8) -> bool:
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as s:
            async with s.head(
                url, headers=headers, allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as resp:
                if resp.status in (200, 206):
                    return True
                if resp.status == 405:
                    async with s.get(
                        url, headers=headers, allow_redirects=True,
                        timeout=aiohttp.ClientTimeout(total=timeout)
                    ) as gr:
                        return gr.status in (200, 206)
                print(f"[URL-check] HTTP {resp.status} — URL may be expired")
                return False
    except Exception as e:
        print(f"[URL-check] {e}")
        return False

# ══════════════════════════════════════════════════════
#  HLS FRAGMENT DOWNLOADER
# ══════════════════════════════════════════════════════

async def hls_fragment_download(url: str, out_path: str):
    from urllib.parse import urljoin
    out_mp4 = f"{out_path}.mp4"
    tmp_dir = tempfile.mkdtemp(prefix="hls_frags_")

    async def fetch_bytes(session, fetch_url, retries=5):
        for attempt in range(retries):
            try:
                async with session.get(
                        fetch_url, headers=CDN_HEADERS,
                        timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    if resp.status == 200:
                        return await resp.read()
                    if resp.status == 404:
                        return None
            except Exception as ex:
                print(f"[HLS] fetch attempt {attempt+1} failed: {ex}")
            await asyncio.sleep(1.5 * (attempt + 1))
        return None

    def abs_url(base, ref):
        if ref.startswith("http"):
            return ref
        return urljoin(base, ref)

    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            manifest_data = await fetch_bytes(session, url)
            if not manifest_data:
                return None
            manifest_text = manifest_data.decode("utf-8", errors="ignore")
            base_url = url.rsplit("/", 1)[0] + "/"

            aes_key = None
            key_match = re.search(r'#EXT-X-KEY:.*?URI="([^"]+)"', manifest_text)
            if key_match:
                key_url  = abs_url(base_url, key_match.group(1))
                key_data = await fetch_bytes(session, key_url)
                if key_data and len(key_data) == 16:
                    aes_key = key_data

            iv_match = re.search(r'#EXT-X-KEY:.*?IV=0x([0-9a-fA-F]+)', manifest_text)
            aes_iv = bytes.fromhex(iv_match.group(1).zfill(32)) if iv_match else b'\x00' * 16

            segments = []
            for line in manifest_text.splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    segments.append(abs_url(base_url, line))

            if not segments:
                return None

            combined = os.path.join(tmp_dir, "combined.ts")
            with open(combined, "wb") as out_ts:
                for seg_url in segments:
                    seg_data = await fetch_bytes(session, seg_url)
                    if seg_data is None:
                        manifest_data = await fetch_bytes(session, url)
                        if manifest_data:
                            manifest_text = manifest_data.decode("utf-8", errors="ignore")
                            segments = []
                            for line in manifest_text.splitlines():
                                line = line.strip()
                                if line and not line.startswith("#"):
                                    segments.append(abs_url(base_url, line))
                        continue
                    if aes_key:
                        cipher = AES.new(aes_key, AES.MODE_CBC, aes_iv)
                        seg_data = cipher.decrypt(seg_data)
                    out_ts.write(seg_data)

            if os.path.exists(combined) and os.path.getsize(combined) > 1024:
                proc = await asyncio.create_subprocess_exec(
                    "ffmpeg", "-y", "-i", combined,
                    "-c", "copy", out_mp4,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                await proc.wait()
                if os.path.exists(out_mp4) and os.path.getsize(out_mp4) > 1024:
                    return out_mp4
    except Exception as e:
        print(f"[HLS] Fragment download error: {e}")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    return None

# ══════════════════════════════════════════════════════
#  DOWNLOAD METHODS
# ══════════════════════════════════════════════════════

async def ytdlp_download(url: str, out_path: str,
                         extra_headers: dict = None):
    if not _YTDLP:
        return None
    hdr_args = []
    if extra_headers:
        for k, v in extra_headers.items():
            hdr_args += ["--add-header", f"{k}:{v}"]
    EXTS = ["mp4", "mkv", "webm", "ts", "m4v"]
    base_args = [
        _YTDLP,
        "--no-check-certificate",
        "-R", "25",
        "--fragment-retries", "25",
        "--hls-use-mpegts",
        "-o", f"{out_path}.%(ext)s",
    ] + hdr_args

    async def _run(*args) -> int:
        try:
            proc = await asyncio.create_subprocess_exec(*args)
            return await proc.wait()
        except FileNotFoundError:
            return -1

    is_proxy = url.startswith(PROXY_BASE)

    if _ARIA2C and not is_proxy:
        ret = await _run(*base_args,
            "--external-downloader", "aria2c",
            "--downloader-args", "aria2c:-x 16 -j 32 -k 1M",
            url)
        if ret == 0:
            for ext in EXTS:
                if os.path.exists(f"{out_path}.{ext}"):
                    return f"{out_path}.{ext}"

    ret2 = await _run(*base_args, url)
    if ret2 == 0:
        for ext in EXTS:
            if os.path.exists(f"{out_path}.{ext}"):
                return f"{out_path}.{ext}"
    return None


async def ffmpeg_download(url: str, out_path: str):
    if not _FFMPEG:
        return None
    out = f"{out_path}.mp4"
    referer = MEDIA_REFERER
    proc = await asyncio.create_subprocess_exec(
        _FFMPEG, "-y",
        "-headers", f"Referer: {referer}\r\nOrigin: https://player-strict.akamai.net.in\r\n",
        "-allowed_extensions", "ALL",
        "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
        "-i", url,
        "-c", "copy",
        out,
    )
    if await proc.wait() == 0 and os.path.exists(out) and os.path.getsize(out) > 1024:
        return out
    return None


async def download_pdf(url: str, out_path: str):
    out = f"{out_path}.pdf"
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get(url, headers=PDF_HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status in (200, 206):
                    data = await resp.read()
                    if len(data) > 512:
                        async with aiofiles.open(out, "wb") as f:
                            await f.write(data)
                        if os.path.exists(out) and os.path.getsize(out) > 512:
                            return out
                else:
                    print(f"[PDF] HTTP {resp.status} for {url[:80]}")
    except Exception as e:
        print(f"[PDF] aiohttp failed: {e}")
    if _YTDLP:
        rc = subprocess.run(
            [_YTDLP, "-o", out, "-R", "5", "--fragment-retries", "10", url],
            capture_output=True
        )
        if os.path.exists(out) and os.path.getsize(out) > 512:
            return out
    return None


async def direct_stream_download(url: str, out_path: str):
    mp4_hdrs = _mp4_headers(url)
    out      = f"{out_path}.mp4"

    try:
        sock_read = 1800 if url.startswith(PROXY_BASE) else 90
        connector = aiohttp.TCPConnector(ssl=False, limit=4, keepalive_timeout=60)
        async with aiohttp.ClientSession(connector=connector) as dl_sess:
            async with dl_sess.get(
                    url, headers=mp4_hdrs,
                    timeout=aiohttp.ClientTimeout(total=3600, connect=15, sock_read=sock_read),
                    allow_redirects=True) as resp:
                if resp.status == 404:
                    print(f"[Direct] 404 — URL expired or not found")
                    return None
                if resp.status == 403:
                    print(f"[Direct] 403 — CDN rejected request")
                    return None
                if resp.status not in (200, 206):
                    return None
                expected   = int(resp.headers.get("Content-Length") or 0)
                downloaded = 0
                last_log   = time.time()
                start_t    = time.time()

                async with aiofiles.open(out, "wb") as f:
                    async for chunk in resp.content.iter_chunked(1048576):
                        await f.write(chunk)
                        downloaded += len(chunk)
                        now = time.time()
                        if now - last_log >= 5:
                            speed = downloaded / (now - start_t) / (1024 * 1024)
                            if expected:
                                pct = downloaded / expected * 100
                                print(f"[Direct] {downloaded/(1024*1024):.1f}/"
                                      f"{expected/(1024*1024):.1f} MB "
                                      f"({pct:.0f}%) @ {speed:.1f} MB/s", end="\r")
                            else:
                                print(f"[Direct] {downloaded/(1024*1024):.1f} MB "
                                      f"@ {speed:.1f} MB/s", end="\r")
                            last_log = now
                print()

                actual = os.path.getsize(out) if os.path.exists(out) else 0
                if expected and actual < expected * 0.98:
                    print(f"[Direct] Truncated ({actual/(1024*1024):.1f} MB "
                          f"of {expected/(1024*1024):.1f} MB) — discarding")
                    try: os.remove(out)
                    except Exception: pass
                    return None

        if os.path.exists(out) and os.path.getsize(out) > 1024:
            return out
    except Exception as e:
        print(f"[Direct] aiohttp failed: {e}")
        try: os.remove(out)
        except Exception: pass

    if _YTDLP:
        res = await ytdlp_download(url, out_path, extra_headers=mp4_hdrs)
        if res:
            return res
    return None


async def _download_single_url(url: str, vtype: str, xor_key: str,
                               out_path: str):
    if _is_drm_url(url):
        print(f"[Download] Skipping DRM URL: {url[:80]}")
        return None

    media_headers = {
        "Referer":    MEDIA_REFERER,
        "User-Agent": MEDIA_UA,
    }

    if _is_mp4_url(url) or url.lower().split("?")[0].endswith(".mp4"):
        proxied    = proxy_mp4_url(url)
        proxy_hdrs = {"User-Agent": MEDIA_UA, "Referer": MEDIA_REFERER}
        print(f"[MP4] Routing through proxy…")

        result = await direct_stream_download(proxied, out_path)
        if result:
            return result

        result = await ytdlp_download(proxied, out_path, extra_headers=proxy_hdrs)
        if result:
            return result

        print(f"[MP4] Proxy methods failed for this URL")
        return None

    elif ".m3u8" in url.lower():
        result = await ytdlp_download(url, out_path, extra_headers=media_headers)
        if not result:
            result = await ffmpeg_download(url, out_path)
        if not result:
            result = await hls_fragment_download(url, out_path)
    elif is_direct_video_url(url):
        result = await ytdlp_download(url, out_path, extra_headers=media_headers)
        if not result:
            result = await direct_stream_download(url, out_path)
        if not result:
            result = await ffmpeg_download(url, out_path)
    else:
        result = await ytdlp_download(url, out_path, extra_headers=media_headers)
        if not result:
            result = await direct_stream_download(url, out_path)
        if not result:
            result = await ffmpeg_download(url, out_path)

    if result and xor_key and "encrypted.mkv" in url.lower():
        print(f"[Download] XOR decrypting…")
        xor_decrypt_file(result, xor_key)

    return result


async def download_item(title: str, url: str, xor_key: str, vtype: str,
                        out_path: str, hdr: dict = None,
                        class_pdfs: list = None,
                        want_pdf: bool = True,
                        all_candidates: list = None) -> bool:
    hdr            = hdr or {}
    class_pdfs     = class_pdfs or []
    all_candidates = all_candidates or []
    result         = None

    if vtype == "youtube":
        result = await ytdlp_download(url, out_path)
    elif vtype == "pdf":
        result = await download_pdf(url, out_path)
    else:
        tried: set = set()

        async def _attempt(cand_url, cand_xor, label):
            if not cand_url or cand_url in tried:
                return None
            tried.add(cand_url)
            if _is_drm_url(cand_url):
                return None
            return await _download_single_url(cand_url, vtype, cand_xor, out_path)

        if url:
            result = await _attempt(url, xor_key or "", "primary")

        if not result and all_candidates:
            for qlabel, cand_url in all_candidates:
                if cand_url in tried or _is_drm_url(cand_url):
                    tried.add(cand_url)
                    continue
                result = await _attempt(cand_url, "", f"fallback {qlabel}")
                if result:
                    break

    if result and os.path.exists(result):
        size = os.path.getsize(result) / (1024 * 1024)
        print(f"[Download] ✓ Saved: {os.path.basename(result)} ({size:.1f} MB)")

    if want_pdf and class_pdfs:
        for suffix, pdf_url in class_pdfs:
            if not pdf_url:
                continue
            pdf_result = await download_pdf(pdf_url, f"{out_path}{suffix}")
            if pdf_result and os.path.exists(pdf_result):
                sz = os.path.getsize(pdf_result) / (1024 * 1024)
                print(f"[Download] ✓ PDF{suffix}: {os.path.basename(pdf_result)} ({sz:.1f} MB)")
            else:
                print(f"[Download] ✗ PDF{suffix} failed")

    return bool(result and os.path.exists(result))

# ══════════════════════════════════════════════════════
#  COLLECT ALL LECTURES
# ══════════════════════════════════════════════════════

async def _bot_pick(bot_client: Client, m: Message,
                    items: list, label_fn, title: str,
                    allow_range: bool = True) -> list:
    if not items:
        return []
    lines = [f"**{title}**\n"]
    for i, item in enumerate(items, 1):
        lines.append(f"`{str(i).zfill(3)})` {label_fn(item)}")
    lines.append(
        "\n_Send number(s), range (e.g. `2-5`), comma list, or `all`_"
    )
    text = "\n".join(lines)
    if len(text) > 4000:
        chunk_size = 50
        for idx in range(0, len(items), chunk_size):
            chunk = items[idx:idx + chunk_size]
            chunk_lines = [f"**{title} (continued)**\n"]
            for i, item in enumerate(chunk, idx + 1):
                chunk_lines.append(f"`{str(i).zfill(3)})` {label_fn(item)}")
            await m.reply_text("\n".join(chunk_lines))
        ask = await m.reply_text(
            f"_Send number(s) from 1-{len(items)}, range, comma list, or `all`:_"
        )
    else:
        ask = await m.reply_text(text)

    try:
        reply_msg: Message = await bot_client.listen(m.chat.id, timeout=120)
        raw = reply_msg.text.strip().lower() if reply_msg.text else "all"
        await reply_msg.delete()
        await ask.delete()
    except asyncio.TimeoutError:
        await ask.edit_text("⏰ Timed out — selecting all.")
        return list(items)

    if raw in ("all", ""):
        return list(items)

    selected = []
    for part in raw.replace(" ", "").split(","):
        if "-" in part and allow_range:
            try:
                a, b = part.split("-", 1)
                for idx in range(int(a) - 1, int(b)):
                    if 0 <= idx < len(items):
                        selected.append(items[idx])
            except Exception:
                pass
        else:
            try:
                idx = int(part) - 1
                if 0 <= idx < len(items):
                    selected.append(items[idx])
            except Exception:
                pass
    return selected if selected else list(items)


async def _collect_folder_recursive(session, api_base, course_id,
                                    parent_id, hdr, out):
    items = await get_folder_contents(session, api_base, course_id, parent_id, hdr)
    for item in items:
        if item.get("material_type") == "FOLDER":
            await _collect_folder_recursive(
                session, api_base, course_id, item["id"], hdr, out)
        else:
            out.append(item)


async def collect_all_lectures(bot_client: Client, m: Message,
                               session, api_base, course_id, hdr) -> list:
    lectures: list = []

    folder_data = await get_folder_contents(session, api_base, course_id, -1, hdr)
    if folder_data:
        await m.reply_text("📁 Folder-based course detected, collecting all lectures…")
        await _collect_folder_recursive(
            session, api_base, course_id, -1, hdr, lectures)
        return lectures

    subjects = await get_subjects(session, api_base, course_id, hdr)
    chosen_subjects = await _bot_pick(
        bot_client, m, subjects,
        lambda s: s.get("subject_name", "?"),
        "📚 Select Subject(s)"
    )
    if not chosen_subjects:
        return []

    for sub in chosen_subjects:
        si = sub.get("subjectid")
        sn = sub.get("subject_name", "")
        topics = await get_topics(session, api_base, course_id, si, hdr)
        chosen_topics = await _bot_pick(
            bot_client, m, topics,
            lambda t: t.get("topic_name", "?"),
            f"📌 Topics in '{sn}'"
        )
        if not chosen_topics:
            continue
        for topic in chosen_topics:
            ti   = topic.get("topicid")
            tn   = topic.get("topic_name", "")
            lecs = await get_lectures(session, api_base, course_id, si, ti, hdr)
            for lec in lecs:
                lec["_subject"] = sn
                lec["_topic"]   = tn
            lectures.extend(lecs)
            await asyncio.sleep(0.3)

    return lectures

# ══════════════════════════════════════════════════════
#  TEST SERIES FLOW
# ══════════════════════════════════════════════════════

async def download_test_series(bot_client: Client, m: Message,
                               session, api_base, user_id, cid, hdr,
                               course_dir: str,
                               channel_id: int = None,
                               cname: str = "",
                               auto_all: bool = False,
                               session_credit: str = ""):
    channel_id = channel_id or m.chat.id
    status = await m.reply_text("⏳ Fetching test series list…")
    series_list = await get_test_series_list(session, api_base, user_id, cid, hdr)
    if not series_list:
        await status.edit_text("❌ No test series found inside this batch.")
        return
    await status.delete()

    def _series_label(s):
        sid   = s.get("id") or "?"
        name  = s.get("title") or s.get("name") or f"Series #{sid}"
        total = s.get("totaltesttitle") or ""
        free  = s.get("freetest") or "0"
        paid  = s.get("paidtest") or "0"
        parts = []
        if total: parts.append(f"{total} subjects")
        if free != "0" or paid != "0":
            parts.append(f"{free} free/{paid} paid")
        suffix = f" [{', '.join(parts)}]" if parts else ""
        return f"{name}{suffix}"

    if auto_all:
        chosen_series = series_list
    else:
        chosen_series = await _bot_pick(
            bot_client, m, series_list, _series_label, "🧪 Select Test Series"
        )
    if not chosen_series:
        return

    for series in chosen_series:
        sid   = str(series.get("id") or "")
        sname = series.get("title") or series.get("name") or f"Series_{sid}"

        subjects = await get_testseries_subjects(session, api_base, sid, hdr)
        if not subjects:
            subjects = [{"subjectid": 0, "subject_name": "All Tests", "id": 0}]

        if auto_all:
            chosen_subjects = subjects
        else:
            chosen_subjects = await _bot_pick(
                bot_client, m, subjects,
                lambda s: s.get("subject_name") or s.get("name") or "?",
                f"📌 Subjects in '{sname}'"
            )
        if not chosen_subjects:
            continue

        all_tests = []
        for subj in chosen_subjects:
            subj_id   = subj.get("subjectid") or subj.get("id") or 0
            subj_name = subj.get("subject_name") or subj.get("name") or "?"
            tests = await get_testseries_tests(session, api_base, sid, subj_id, hdr)
            for t in tests:
                t["_subject_name"] = subj_name
                t["_series_name"]  = sname
            all_tests.extend(tests)
            await asyncio.sleep(0.3)

        if not all_tests:
            await m.reply_text(f"⚠️ No tests found in '{sname}'")
            continue

        def _test_label(t):
            name  = t.get("title") or t.get("name") or f"ID:{t.get('id','')}"
            marks = t.get("marks") or t.get("total_marks") or ""
            mins  = t.get("time")  or t.get("duration")    or ""
            subj  = t.get("_subject_name", "")
            parts = []
            if subj:  parts.append(subj)
            if marks: parts.append(f"{marks} marks")
            if mins:  parts.append(f"{mins} min")
            return f"{name}{' [' + ', '.join(parts) + ']' if parts else ''}"

        if auto_all:
            chosen_tests = all_tests
        else:
            chosen_tests = await _bot_pick(
                bot_client, m, all_tests, _test_label,
                f"📝 Tests in '{sname}'"
            )
        if not chosen_tests:
            continue

        series_dir = os.path.join(course_dir, "TestSeries", safe_filename(sname))
        os.makedirs(series_dir, exist_ok=True)

        success = failed = 0
        total   = len(chosen_tests)
        prog    = await bot_client.send_message(
            channel_id,
            f"⏳ Downloading {total} tests from **{sname}**…"
        )

        for i, test in enumerate(chosen_tests, 1):
            tname = test.get("title") or test.get("name") or f"Test_{test.get('id','?')}"
            subj  = test.get("_subject_name", "")
            out_dir = series_dir
            if subj:
                out_dir = os.path.join(series_dir, safe_filename(subj))
                os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, safe_filename(tname, i))

            prog = await safe_progress_edit(
                bot_client, channel_id, prog,
                f"🧪 **Test {i}/{total}**\n"
                f"`{tname[:60]}`\n"
                f"📁 Subject: {subj}"
            )

            questions = await fetch_testseries_questions(session, api_base, test, hdr)
            if not questions:
                failed += 1
                await asyncio.sleep(0.5)
                continue

            qfile = save_quiz_html(questions, out_path, tname, watermark=session_credit)
            if qfile:
                await bot_client.send_document(
                    channel_id,
                    document=qfile,
                    caption=f"📝 `{os.path.basename(qfile)}`\n`{len(questions)} questions`"
                )
                os.remove(qfile)
                success += 1
            else:
                failed += 1
            await asyncio.sleep(0.5)

        await safe_progress_edit(
            bot_client, channel_id, prog,
            f"✅ **Test Series Complete: {sname}**\n"
            f"✓ Saved: {success}  ✗ Failed: {failed}"
        )

# ══════════════════════════════════════════════════════
#  LECTURE DOWNLOAD LOOP
# ══════════════════════════════════════════════════════

async def run_lecture_loop(
    bot_client: Client,
    m: Message,
    session,
    api_base: str,
    cid: str,
    cname: str,
    hdr: dict,
    appx_user_id: str,
    channel_id: int,
    want_video: bool,
    want_pdf: bool,
    want_quiz: bool,
    prefer_quality: str,
    batch_watermark,
    session_credit: str,
    lectures: list,
    start_idx: int,
    downloads_dir: str,
):
    success = failed = 0
    total   = len(lectures)

    prog_msg = await bot_client.send_message(
        channel_id,
        f"<blockquote><b>🎯 Batch: {cname}</b></blockquote>"
    )
    try:
        await bot_client.pin_chat_message(channel_id, prog_msg.id)
    except Exception:
        pass

    for i, lec in enumerate(lectures, start=start_idx + 1):
        if cancel_requested.get(m.from_user.id, False):
            await m.reply_text("🚦 **STOPPED**")
            break

        title   = lec.get("Title") or f"Lecture_{lec.get('id','?')}"
        lec_id  = lec.get("id")
        mat     = lec.get("material_type", "")
        subject = lec.get("_subject", "")
        topic   = lec.get("_topic", "")

        prog_msg = await safe_progress_edit(
            bot_client, channel_id, prog_msg,
            f"<blockquote>"
            f"🚀 Progress: {i}/{total} ({i*100//total}%)\n"
            f"📚 Title: {title[:50]}\n"
            f"📁 {subject} → {topic}\n"
            f"💃 Batch: {cname}"
            f"</blockquote>\n"
            f"🛑 /stop to cancel"
        )

        name     = safe_filename(title, i)
        out_path = os.path.join(downloads_dir, name)

        QUIZ_TYPES = {"QUIZ", "TEST", "ATTEMPT", "EXERCISE", "ASSIGNMENT"}
        _raw_qid   = str(lec.get("quiz_title_id") or lec.get("quiz_id") or "")
        _has_quiz  = (
            mat.upper() in QUIZ_TYPES
            or "quiz" in mat.lower()
            or (_raw_qid not in ("", "0", "-1", "None"))
        )
        if _has_quiz:
            quiz_id = _raw_qid or str(lec_id) or ""
            if want_quiz and quiz_id:
                questions = await fetch_quiz(session, api_base, quiz_id, appx_user_id, hdr)
                if questions:
                    qfile = save_quiz_html(questions, out_path, title, watermark=session_credit)
                    if qfile:
                        await bot_client.send_document(
                            channel_id,
                            document=qfile,
                            caption=(
                                f"📝 `{title[:60]}`\n"
                                f"`{len(questions)} questions`\n"
                                f"`{i}/{total}` | Batch: {cname}"
                            )
                        )
                        os.remove(qfile)
                        success += 1
                    else:
                        failed += 1
                else:
                    failed += 1

            if mat.upper() in QUIZ_TYPES or (
                    "quiz" in mat.lower() and mat.upper() != "VIDEO"):
                continue

        vtitle, url, xor_key, vtype, class_pdfs, all_candidates = \
            await resolve_video(
                session, api_base, cid, lec_id, hdr,
                prefer_quality=prefer_quality)

        if not url and not class_pdfs and not all_candidates:
            failed += 1
            continue

        cc  = (
            f"[🎥] `{str(i).zfill(3)}) {title[:60]} [{prefer_quality}]`\n"
            f"<blockquote>Batch: {cname}</blockquote>\n"
            f"Extracted by ➤ {session_credit}"
        )
        cc1 = (
            f"[📕] `{str(i).zfill(3)}) {title[:60]}.pdf`\n"
            f"<blockquote>Batch: {cname}</blockquote>\n"
            f"Extracted by ➤ {session_credit}"
        )

        if vtype == "pdf" and want_pdf:
            pr = await download_pdf(url, out_path)
            if pr:
                await bot_client.send_document(channel_id, document=pr, caption=cc1)
                os.remove(pr)
                success += 1
            else:
                failed += 1

            for idx2, (suffix, purl) in enumerate(class_pdfs, start=2):
                if not purl:
                    continue
                pr2 = await download_pdf(purl, f"{out_path}_extra_{idx2}")
                if pr2:
                    cc_extra = (
                        f"[📕] `{str(i).zfill(3)}) {title[:55]}_p{idx2}.pdf`\n"
                        f"<blockquote>Batch: {cname}</blockquote>\n"
                        f"Extracted by ➤ {session_credit}"
                    )
                    await bot_client.send_document(channel_id, document=pr2, caption=cc_extra)
                    os.remove(pr2)
            continue

        if vtype == "pdf" and not want_pdf:
            continue

        if vtype not in ("pdf",) and not want_video:
            if want_pdf and class_pdfs:
                for suffix, purl in class_pdfs:
                    if not purl:
                        continue
                    pr = await download_pdf(purl, f"{out_path}{suffix}")
                    if pr:
                        await bot_client.send_document(channel_id, document=pr, caption=cc1)
                        os.remove(pr)
            continue

        if vtype == "youtube":
            ytf = f"bv*[height<={prefer_quality[:-1]}][ext=mp4]+ba[ext=m4a]/b[height<=?{prefer_quality[:-1]}]"
            cmd = (
                f'yt-dlp --cookies {cookies_file_path} '
                f'--extractor-args "youtube:player_client=android" '
                f'-f "{ytf}" -R 25 --fragment-retries 25 '
                f'"{url}" -o "{out_path}.mp4"'
            )
            await bot_client.send_message(channel_id,
                f"⬇️ YouTube: `{title[:60]}`\n`{i}/{total}`")
            filename = await helper.download_video(url, cmd, out_path)
            if filename:
                await helper.send_vid(
                    bot_client, m, cc, filename, "/d", name,
                    prog_msg, channel_id, watermark_text=batch_watermark)
                success += 1
            else:
                failed += 1
            await asyncio.sleep(1)
            continue

        if 'encrypted.m' in (url or ""):
            appxkey  = url.split('*')[1]
            url      = url.split('*')[0]
            res_file = await helper.download_and_decrypt_video(url, "", out_path, appxkey)
        else:
            q_num = prefer_quality.replace("p", "")
            ytf   = f"b[height<={q_num}]/bv[height<={q_num}]+ba/b/bv+ba"
            header_args = (
                f'--add-header "Referer: {MEDIA_REFERER}" '
                f'--add-header "User-Agent: {MEDIA_UA}" '
            )
            if _is_mp4_url(url) or url.lower().split("?")[0].endswith(".mp4"):
                cmd = (
                    f'yt-dlp {header_args} '
                    f'-f "{ytf}" -R 25 --fragment-retries 25 '
                    f'"{url}" -o "{out_path}.mp4"'
                )
            else:
                cmd = (
                    f'yt-dlp {header_args} '
                    f'-f "{ytf}" --concurrent-fragments 16 --no-part '
                    f'-R 25 --fragment-retries 25 '
                    f'"{url}" -o "{out_path}.mp4"'
                )
            res_file = await helper.download_video(url, cmd, out_path)

        if res_file:
            await helper.send_vid(
                bot_client, m, cc, res_file, "/d", name,
                prog_msg, channel_id, watermark_text=batch_watermark)
            if want_pdf and class_pdfs:
                for suffix, purl in class_pdfs:
                    if not purl:
                        continue
                    pr = await download_pdf(purl, f"{out_path}{suffix}")
                    if pr:
                        await bot_client.send_document(channel_id, document=pr, caption=cc1)
                        os.remove(pr)
            success += 1
        else:
            ok = await download_item(
                vtitle or title, url, xor_key, vtype, out_path,
                hdr=hdr, class_pdfs=[], want_pdf=False,
                all_candidates=all_candidates)
            if ok:
                for ext in ["mp4", "mkv", "webm", "ts"]:
                    fpath = f"{out_path}.{ext}"
                    if os.path.exists(fpath):
                        await helper.send_vid(
                            bot_client, m, cc, fpath, "/d", name,
                            prog_msg, channel_id, watermark_text=batch_watermark)
                        break
                if want_pdf and class_pdfs:
                    for suffix, purl in class_pdfs:
                        if not purl:
                            continue
                        pr = await download_pdf(purl, f"{out_path}{suffix}")
                        if pr:
                            await bot_client.send_document(channel_id, document=pr, caption=cc1)
                            os.remove(pr)
                success += 1
            else:
                await bot_client.send_message(
                    channel_id,
                    f"⚠️ **Failed:** `{str(i).zfill(3)}) {title[:60]}`"
                )
                if want_pdf and class_pdfs:
                    for suffix, purl in class_pdfs:
                        if not purl:
                            continue
                        pr = await download_pdf(purl, f"{out_path}{suffix}")
                        if pr:
                            await bot_client.send_document(channel_id, document=pr, caption=cc1)
                            os.remove(pr)
                failed += 1

        await asyncio.sleep(1)

    return success, failed

# ══════════════════════════════════════════════════════
#  MAIN APPX DOWNLOAD FLOW
# ══════════════════════════════════════════════════════

async def appx_main_flow(bot_client: Client, m: Message, user_id_tg: int):
    domain_ask = await m.reply_text(
        "**🌐 Enter API Domain**\n\n"
        "<blockquote>Examples:\n"
        "• `parmaracademyapi.teachx.in`\n"
        "• `e1coachingcenterapi.classx.co.in`\n\n"
        "Send **without** `https://`</blockquote>"
    )
    try:
        domain_msg: Message = await bot_client.listen(m.chat.id, timeout=120)
        api_raw = domain_msg.text.strip() if domain_msg.text else ""
        await domain_msg.delete()
        await domain_ask.delete()
    except asyncio.TimeoutError:
        await domain_ask.edit_text("⏰ Timed out.")
        return

    if not api_raw:
        await m.reply_text("❌ No API domain entered.")
        return

    api_base = (f"https://{api_raw}" if not api_raw.startswith("http") else api_raw)

    token, appx_user_id, user_name = await appx_login(bot_client, m, api_base)
    if not token:
        return

    hdr = make_headers(token, appx_user_id)
    await m.reply_text(f"✅ **Authenticated as:** `{user_name}`")

    status = await m.reply_text("⏳ Fetching your courses…")
    async with aiohttp.ClientSession() as session:
        courses = await get_courses(session, api_base, appx_user_id, hdr)
        await status.delete()

        if not courses:
            await m.reply_text("❌ No courses found.")
            return

        chosen_courses = await _bot_pick(
            bot_client, m, courses,
            lambda c: f"{c.get('course_name','?')} `(id:{c.get('id','?')})`",
            "📚 Your Courses"
        )
        if not chosen_courses:
            await m.reply_text("❌ No course selected.")
            return

        for course in chosen_courses:
            if not processing_request.get(user_id_tg, True):
                break

            cid   = str(course.get("id", ""))
            cname = course.get("course_name", f"course_{cid}")

            await m.reply_text(f"**📚 Course: {cname}**")

            dl_type_ask = await m.reply_text(
                "**What to download?**\n\n"
                "`1` — Videos only\n"
                "`2` — PDFs only\n"
                "`3` — Everything (videos + PDFs + quizzes)\n"
                "`4` — Quizzes only\n"
                "`5` — Test Series\n"
                "`6` — 🔥 Download EVERYTHING (videos + PDFs + quizzes + ALL test series)\n\n"
                "_Send a number (20s timeout → default 3):_"
            )
            try:
                dl_msg: Message = await bot_client.listen(m.chat.id, timeout=20)
                dl_choice = dl_msg.text.strip() if dl_msg.text else "3"
                await dl_msg.delete()
                await dl_type_ask.delete()
            except asyncio.TimeoutError:
                dl_choice = "3"
                await dl_type_ask.delete()

            downloads_dir = os.path.join(tempfile.gettempdir(), "downloads")
            os.makedirs(downloads_dir, exist_ok=True)

            # ── OPTION 5 — Test Series only ───────────────────────────────
            if dl_choice == "5":
                brand_ask = await m.reply_text(
                    "**🏷️ Brand name for captions & HTML watermark?**\n"
                    "Send your brand/channel name (e.g. `@MyChannel`) or `/d` to use default.\n"
                    "_20s timeout → default:_"
                )
                try:
                    brand_msg: Message = await bot_client.listen(m.chat.id, timeout=20)
                    raw_brand = brand_msg.text.strip() if brand_msg.text else "/d"
                    await brand_msg.delete()
                    await brand_ask.delete()
                except asyncio.TimeoutError:
                    raw_brand = "/d"
                    await brand_ask.delete()
                session_credit = CREDIT if raw_brand == "/d" else raw_brand
                user_brand_name[user_id_tg] = session_credit

                wm_ask = await m.reply_text(
                    "**🖊️ Add watermark text?**\n"
                    "Send text (e.g. `@YourChannel`) or `/d` to skip.\n_20s timeout:_"
                )
                try:
                    wm_msg: Message = await bot_client.listen(m.chat.id, timeout=20)
                    raw_wm = wm_msg.text.strip() if wm_msg.text else "/d"
                    await wm_msg.delete()
                    await wm_ask.delete()
                except asyncio.TimeoutError:
                    raw_wm = "/d"
                    await wm_ask.delete()
                batch_watermark = None if raw_wm == "/d" else raw_wm

                chan_ask = await m.reply_text(
                    "**Upload to channel/group?**\n"
                    "Send Channel ID (e.g. `-100xxxxxxxxxx`) or `/d` to upload here.\n"
                    "_20s timeout → upload here:_"
                )
                try:
                    chan_msg: Message = await bot_client.listen(m.chat.id, timeout=20)
                    raw_chan = chan_msg.text.strip() if chan_msg.text else "/d"
                    await chan_msg.delete()
                    await chan_ask.delete()
                except asyncio.TimeoutError:
                    raw_chan = "/d"
                    await chan_ask.delete()
                channel_id = (m.chat.id if "/d" in raw_chan
                              else (int(raw_chan) if raw_chan.lstrip("-").isdigit() else m.chat.id))

                course_dir = os.path.join(tempfile.gettempdir(), "downloads")
                os.makedirs(course_dir, exist_ok=True)
                await download_test_series(
                    bot_client, m, session, api_base,
                    appx_user_id, cid, hdr, course_dir,
                    channel_id=channel_id, cname=cname,
                    session_credit=session_credit)
                continue

            # ── OPTION 6 — Download EVERYTHING ───────────────────────────
            if dl_choice == "6":
                await m.reply_text(
                    "🔥 **Download EVERYTHING mode**\n\n"
                    "Will download in this order:\n"
                    "1️⃣ All videos + PDFs + lecture quizzes\n"
                    "2️⃣ All test series (every series, every subject, every test)\n\n"
                    "No further prompts — starting now…"
                )

                q_ask = await m.reply_text(
                    "**Video quality?**\n"
                    "`1` 1080p  `2` 720p (default)  `3` 480p\n"
                    "`4` 360p   `5` 240p\n"
                    "_Send choice (20s timeout → 720p):_"
                )
                try:
                    q_msg: Message = await bot_client.listen(m.chat.id, timeout=20)
                    q_map = {"1":"1080p","2":"720p","3":"480p","4":"360p","5":"240p"}
                    prefer_quality = q_map.get(q_msg.text.strip() if q_msg.text else "2", "720p")
                    await q_msg.delete()
                    await q_ask.delete()
                except asyncio.TimeoutError:
                    prefer_quality = "720p"
                    await q_ask.delete()

                brand_ask = await m.reply_text(
                    "**🏷️ Brand name for captions & HTML watermark?**\n"
                    "Send your brand/channel name (e.g. `@MyChannel`) or `/d` to use default.\n"
                    "_20s timeout → default:_"
                )
                try:
                    brand_msg: Message = await bot_client.listen(m.chat.id, timeout=20)
                    raw_brand = brand_msg.text.strip() if brand_msg.text else "/d"
                    await brand_msg.delete()
                    await brand_ask.delete()
                except asyncio.TimeoutError:
                    raw_brand = "/d"
                    await brand_ask.delete()
                session_credit = CREDIT if raw_brand == "/d" else raw_brand
                user_brand_name[user_id_tg] = session_credit

                wm_ask = await m.reply_text(
                    "**🖊️ Add watermark text?**\n"
                    "Send text (e.g. `@YourChannel`) or `/d` to skip.\n_20s timeout:_"
                )
                try:
                    wm_msg: Message = await bot_client.listen(m.chat.id, timeout=20)
                    raw_wm = wm_msg.text.strip() if wm_msg.text else "/d"
                    await wm_msg.delete()
                    await wm_ask.delete()
                except asyncio.TimeoutError:
                    raw_wm = "/d"
                    await wm_ask.delete()
                batch_watermark = None if raw_wm == "/d" else raw_wm

                chan_ask = await m.reply_text(
                    "**Upload to channel/group?**\n"
                    "Send Channel ID (e.g. `-100xxxxxxxxxx`) or `/d` to upload here.\n"
                    "_20s timeout → upload here:_"
                )
                try:
                    chan_msg: Message = await bot_client.listen(m.chat.id, timeout=20)
                    raw_chan = chan_msg.text.strip() if chan_msg.text else "/d"
                    await chan_msg.delete()
                    await chan_ask.delete()
                except asyncio.TimeoutError:
                    raw_chan = "/d"
                    await chan_ask.delete()
                channel_id = (m.chat.id if "/d" in raw_chan
                              else (int(raw_chan) if raw_chan.lstrip("-").isdigit() else m.chat.id))

                await bot_client.send_message(
                    channel_id,
                    f"<blockquote><b>📦 Phase 1/2 — Lectures\nBatch: {cname}</b></blockquote>"
                )

                lec_status = await m.reply_text("⏳ Fetching all lectures…")
                all_lectures: list = []
                folder_data = await get_folder_contents(session, api_base, cid, -1, hdr)
                if folder_data:
                    await _collect_folder_recursive(
                        session, api_base, cid, -1, hdr, all_lectures)
                else:
                    subjects = await get_subjects(session, api_base, cid, hdr)
                    for sub in subjects:
                        si = sub.get("subjectid")
                        sn = sub.get("subject_name", "")
                        topics = await get_topics(session, api_base, cid, si, hdr)
                        for topic in topics:
                            ti   = topic.get("topicid")
                            tn   = topic.get("topic_name", "")
                            lecs = await get_lectures(session, api_base, cid, si, ti, hdr)
                            for lec in lecs:
                                lec["_subject"] = sn
                                lec["_topic"]   = tn
                            all_lectures.extend(lecs)
                            await asyncio.sleep(0.2)

                await lec_status.delete()
                await m.reply_text(f"✅ Found **{len(all_lectures)}** lectures — starting download…")

                lec_success, lec_failed = await run_lecture_loop(
                    bot_client=bot_client,
                    m=m,
                    session=session,
                    api_base=api_base,
                    cid=cid,
                    cname=cname,
                    hdr=hdr,
                    appx_user_id=appx_user_id,
                    channel_id=channel_id,
                    want_video=True,
                    want_pdf=True,
                    want_quiz=True,
                    prefer_quality=prefer_quality,
                    batch_watermark=batch_watermark,
                    session_credit=session_credit,
                    lectures=all_lectures,
                    start_idx=0,
                    downloads_dir=downloads_dir,
                )

                await bot_client.send_message(
                    channel_id,
                    f"<blockquote><b>✅ Phase 1 Complete — Lectures</b>\n"
                    f"✓ Success: {lec_success}  ✗ Failed: {lec_failed}</blockquote>"
                )

                if cancel_requested.get(user_id_tg, False):
                    await m.reply_text("🚦 Stopped before Phase 2 (Test Series).")
                else:
                    await bot_client.send_message(
                        channel_id,
                        f"<blockquote><b>📦 Phase 2/2 — Test Series\nBatch: {cname}</b></blockquote>"
                    )
                    course_dir = os.path.join(tempfile.gettempdir(), "downloads")
                    await download_test_series(
                        bot_client, m, session, api_base,
                        appx_user_id, cid, hdr, course_dir,
                        channel_id=channel_id, cname=cname,
                        auto_all=True,
                        session_credit=session_credit
                    )

                await bot_client.send_message(
                    channel_id,
                    f"<b>-┈━═.•° 🔥 EVERYTHING Downloaded ✅ °•.═━┈-</b>\n"
                    f"<blockquote><b>🎯 Batch: {cname}</b>\n"
                    f"📹 Lectures  ✓ {lec_success}  ✗ {lec_failed}\n"
                    f"🧪 Test Series — see above for individual counts</blockquote>"
                )
                continue

            # ── OPTIONS 1-4 ───────────────────────────────────────────────
            want_video = dl_choice in ("1", "3", "")
            want_pdf   = dl_choice in ("2", "3")
            want_quiz  = dl_choice in ("3", "4")

            lec_status = await m.reply_text("⏳ Fetching content…")
            lectures = await collect_all_lectures(
                bot_client, m, session, api_base, cid, hdr)
            await lec_status.delete()

            if not lectures:
                await m.reply_text("❌ No lectures found.")
                continue

            await m.reply_text(f"✅ Found **{len(lectures)}** lectures")

            start_ask = await m.reply_text(
                f"**Start from lecture number?** [1-{len(lectures)}, default 1]\n_20s timeout:_"
            )
            try:
                start_msg: Message = await bot_client.listen(m.chat.id, timeout=20)
                start_raw = start_msg.text.strip() if start_msg.text else "1"
                await start_msg.delete()
                await start_ask.delete()
            except asyncio.TimeoutError:
                start_raw = "1"
                await start_ask.delete()

            start_idx = (int(start_raw) - 1) if start_raw.isdigit() else 0
            lectures  = lectures[start_idx:]

            prefer_quality = "720p"
            if want_video:
                q_ask = await m.reply_text(
                    "**Video quality?**\n"
                    "`1` 1080p  `2` 720p (default)  `3` 480p\n"
                    "`4` 360p   `5` 240p\n"
                    "_Send choice (20s timeout → 720p):_"
                )
                try:
                    q_msg: Message = await bot_client.listen(m.chat.id, timeout=20)
                    q_map = {"1":"1080p","2":"720p","3":"480p","4":"360p","5":"240p"}
                    prefer_quality = q_map.get(q_msg.text.strip() if q_msg.text else "2", "720p")
                    await q_msg.delete()
                    await q_ask.delete()
                except asyncio.TimeoutError:
                    await q_ask.delete()

            brand_ask = await m.reply_text(
                "**🏷️ Brand name for captions & HTML watermark?**\n"
                "Send your brand/channel name (e.g. `@MyChannel`) or `/d` to use default.\n"
                "_20s timeout → default:_"
            )
            try:
                brand_msg: Message = await bot_client.listen(m.chat.id, timeout=20)
                raw_brand = brand_msg.text.strip() if brand_msg.text else "/d"
                await brand_msg.delete()
                await brand_ask.delete()
            except asyncio.TimeoutError:
                raw_brand = "/d"
                await brand_ask.delete()
            session_credit = CREDIT if raw_brand == "/d" else raw_brand
            user_brand_name[user_id_tg] = session_credit

            wm_ask = await m.reply_text(
                "**🖊️ Add watermark text?**\n"
                "Send text (e.g. `@YourChannel`) or `/d` to skip.\n_20s timeout:_"
            )
            try:
                wm_msg: Message = await bot_client.listen(m.chat.id, timeout=20)
                raw_wm = wm_msg.text.strip() if wm_msg.text else "/d"
                await wm_msg.delete()
                await wm_ask.delete()
            except asyncio.TimeoutError:
                raw_wm = "/d"
                await wm_ask.delete()
            batch_watermark = None if raw_wm == "/d" else raw_wm

            chan_ask = await m.reply_text(
                "**Upload to channel/group?**\n"
                "Send Channel ID (e.g. `-100xxxxxxxxxx`) or `/d` to upload here.\n"
                "_20s timeout → upload here:_"
            )
            try:
                chan_msg: Message = await bot_client.listen(m.chat.id, timeout=20)
                raw_chan = chan_msg.text.strip() if chan_msg.text else "/d"
                await chan_msg.delete()
                await chan_ask.delete()
            except asyncio.TimeoutError:
                raw_chan = "/d"
                await chan_ask.delete()
            channel_id = (m.chat.id if "/d" in raw_chan
                          else (int(raw_chan) if raw_chan.lstrip("-").isdigit() else m.chat.id))

            success, failed = await run_lecture_loop(
                bot_client=bot_client,
                m=m,
                session=session,
                api_base=api_base,
                cid=cid,
                cname=cname,
                hdr=hdr,
                appx_user_id=appx_user_id,
                channel_id=channel_id,
                want_video=want_video,
                want_pdf=want_pdf,
                want_quiz=want_quiz,
                prefer_quality=prefer_quality,
                batch_watermark=batch_watermark,
                session_credit=session_credit,
                lectures=lectures,
                start_idx=start_idx,
                downloads_dir=downloads_dir,
            )

            await bot_client.send_message(
                channel_id,
                f"<b>-┈━═.•° ✅ Completed ✅ °•.═━┈-</b>\n"
                f"<blockquote><b>🎯 Batch: {cname}</b></blockquote>\n"
                f"<blockquote>"
                f"✅ Success: {success}\n"
                f"❌ Failed:  {failed}\n"
                f"📦 Total:   {len(lectures)}"
                f"</blockquote>"
            )

    processing_request[user_id_tg] = False

# ══════════════════════════════════════════════════════
#  SCHEDULER — MONGODB HELPERS
# ══════════════════════════════════════════════════════

def _aw():
    """Return the auto_watch MongoDB collection."""
    return db.db["auto_watch"]


def _ensure_scheduler_indexes():
    try:
        _aw().create_index(
            [("user_id", 1), ("auto_id", 1)], unique=True,
            name="auto_watch_identity"
        )
    except Exception:
        pass


def _next_auto_id(user_id: int) -> int | None:
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
#  SCHEDULER — SILENT LOGIN (for token refresh)
# ══════════════════════════════════════════════════════

async def _fresh_login(session: aiohttp.ClientSession,
                       api_base: str, phone: str, password: str):
    """Non-interactive login. Returns (token, appx_uid) or (None, None)."""
    headers = {**APPX_HEADERS, "content-type": "application/x-www-form-urlencoded"}
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
                              _extract_user_id_from_jwt(token) or "")
                    return token, uid
        except Exception:
            continue
    return None, None


async def _validate_token(session: aiohttp.ClientSession,
                           api_base: str, token: str, appx_uid: str) -> bool:
    hdr = make_headers(token, appx_uid)
    courses = await get_courses(session, api_base, appx_uid, hdr)
    return bool(courses)

# ══════════════════════════════════════════════════════
#  SCHEDULER — SUPERGROUP TOPIC HELPERS
# ══════════════════════════════════════════════════════

async def _create_topic(supergroup_id: int, subject_name: str):
    """Create a forum topic. Returns thread_id or None."""
    try:
        api_resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/createForumTopic",
            json={"chat_id": supergroup_id, "name": f"📚 {subject_name}"}
        ).json()
        if api_resp.get("ok"):
            tid = api_resp["result"]["message_thread_id"]
            print(f"[Scheduler] Topic created: id={tid} name={subject_name}")
            return tid
        else:
            print(f"[Scheduler] Topic creation failed: {api_resp}")
            return None
    except Exception as e:
        print(f"[Scheduler] Topic creation failed: {e}")
        return None


async def _safe_send(coro):
    """Wrap a send coroutine with FloodWait + retry."""
    for attempt in range(5):
        try:
            return await coro
        except FloodWait as fw:
            print(f"[Scheduler] FloodWait {fw.value}s")
            await asyncio.sleep(fw.value + 2)
        except Exception as e:
            print(f"[Scheduler] Send error: {e}")
            return None
    return None


async def _send_to_topic(
        supergroup_id: int,
        topic_id: int,
        file_path: str | None,
        caption: str,
        is_video: bool = False):
    """
    Send a file to a supergroup topic thread.
    Files ≤ 50 MB  → HTTP Bot API
    Files  > 50 MB → Pyrogram MTProto
    """
    if not file_path or not os.path.exists(file_path):
        print(f"[Scheduler] File not found: {file_path}")
        return

    file_size    = os.path.getsize(file_path)
    use_pyrogram = file_size > 50 * 1024 * 1024

    if use_pyrogram:
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
                print(f"[Scheduler] Send error: {resp.get('description')}")
        except Exception as e:
            print(f"[Scheduler] Send error: {e}")

# ══════════════════════════════════════════════════════
#  SCHEDULER — CORE AUTO-UPDATE JOB
# ══════════════════════════════════════════════════════

async def _run_auto_update(doc: dict):
    """
    Process one auto_watch document:
    - For each subject → fetch new lectures/pdfs/quizzes/tests
    - Send only NEW items to the correct supergroup topic thread
    - Save updated seen-ID sets back to MongoDB
    """
    user_id     = doc["user_id"]
    auto_id     = doc["auto_id"]
    api_base    = doc["api_base"]
    course_id   = str(doc["course_id"])
    course_name = doc.get("course_name", "Course")
    sg_id       = doc["supergroup_id"]
    brand       = doc.get("brand", CREDIT)
    subjects    = doc.get("subjects", [])

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
            if phone and password:
                token, appx_uid = await _fresh_login(session, api_base, phone, password)
                if token:
                    token_ok = True
                    _aw().update_one(
                        {"user_id": user_id, "auto_id": auto_id},
                        {"$set": {"appx_token": token, "appx_user_id": appx_uid}}
                    )

            if not token_ok:
                print(f"[Scheduler] Token expired and re-login failed for user {user_id} auto #{auto_id}")
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

        hdr = make_headers(token, appx_uid)
        updated_subjects = []

        for subj in subjects:
            subj_id    = str(subj["subject_id"])
            subj_name  = subj["subject_name"]
            topic_id   = subj.get("topic_thread_id")

            track_lec   = subj.get("track_lectures",  True)
            track_pdf   = subj.get("track_pdfs",      True)
            track_quiz  = subj.get("track_quizzes",   True)
            track_tests = subj.get("track_tests",     True)

            seen_lec_ids  = set(subj.get("last_lecture_ids", []))
            seen_pdf_ids  = set(subj.get("last_pdf_ids",     []))
            seen_quiz_ids = set(subj.get("last_quiz_ids",    []))
            seen_test_ids = set(subj.get("last_test_ids",    []))

            new_lec_ids  = set()
            new_pdf_ids  = set()
            new_quiz_ids = set()
            new_test_ids = set()

            if not topic_id:
                print(f"[Scheduler] No topic_thread_id for subject '{subj_name}' in auto #{auto_id} — skipping")
                updated_subjects.append(subj)
                continue

            appx_topics = await get_topics(session, api_base, course_id, subj_id, hdr)

            for appx_topic in appx_topics:
                appx_topic_id   = str(appx_topic.get("topicid", ""))
                appx_topic_name = appx_topic.get("topic_name", "")
                lectures        = await get_lectures(
                    session, api_base, course_id, subj_id, appx_topic_id, hdr
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
                        questions = await fetch_quiz(session, api_base, raw_qid, appx_uid, hdr)
                        if questions:
                            quiz_path = os.path.join(tmp_dir, f"quiz_{raw_qid}.html")
                            save_quiz_html(questions, quiz_path, lec_title, watermark=brand)
                            if os.path.exists(quiz_path):
                                cap = (
                                    f"📝 **New Quiz — {subj_name}**\n\n"
                                    f"❓ `{lec_title[:60]}`\n"
                                    f"📌 Topic: {appx_topic_name}\n"
                                    f"🔢 {len(questions)} Questions\n"
                                    f"🤖 Auto #{auto_id} | {brand}"
                                )
                                await _send_to_topic(sg_id, topic_id, quiz_path, cap, is_video=False)
                                try:
                                    os.remove(quiz_path)
                                except Exception:
                                    pass

                        if mat.upper() in QUIZ_TYPES or (
                                "quiz" in mat.lower() and mat.upper() != "VIDEO"):
                            continue

                    # ── Resolve video / PDF URL ──────────────────────────
                    vtitle, url, xor_key, vtype, class_pdfs, all_cands = \
                        await resolve_video(session, api_base, course_id, lec_id, hdr)

                    # ── NEW PDF-type lecture ──────────────────────────────
                    if vtype == "pdf" and track_pdf and lec_id not in seen_pdf_ids:
                        new_pdf_ids.add(lec_id)
                        if url:
                            pdf_path = os.path.join(tmp_dir, f"pdf_{lec_id}.pdf")
                            result   = await download_pdf(url, pdf_path)
                            if result and os.path.exists(result):
                                cap = (
                                    f"📄 **New PDF — {subj_name}**\n\n"
                                    f"📝 `{lec_title[:60]}`\n"
                                    f"📌 Topic: {appx_topic_name}\n"
                                    f"🤖 Auto #{auto_id} | {brand}"
                                )
                                await _send_to_topic(sg_id, topic_id, result, cap, is_video=False)
                                try:
                                    os.remove(result)
                                except Exception:
                                    pass
                        for suffix, purl in class_pdfs:
                            if not purl:
                                continue
                            pid     = f"pdf_{lec_id}{suffix}"
                            ppath   = os.path.join(tmp_dir, f"{pid}.pdf")
                            result2 = await download_pdf(purl, ppath)
                            if result2 and os.path.exists(result2):
                                cap2 = (
                                    f"📄 **New PDF — {subj_name}**\n\n"
                                    f"📝 `{lec_title[:55]} {suffix}`\n"
                                    f"📌 Topic: {appx_topic_name}\n"
                                    f"🤖 Auto #{auto_id} | {brand}"
                                )
                                await _send_to_topic(sg_id, topic_id, result2, cap2, is_video=False)
                                try:
                                    os.remove(result2)
                                except Exception:
                                    pass
                        continue

                    # ── PDFs attached to a VIDEO lecture ──────────────────
                    if track_pdf and class_pdfs:
                        for suffix, purl in class_pdfs:
                            if not purl:
                                continue
                            pid = f"pdf_{lec_id}{suffix}"
                            if pid in seen_pdf_ids:
                                continue
                            new_pdf_ids.add(pid)
                            ppath   = os.path.join(tmp_dir, f"{pid}.pdf")
                            result3 = await download_pdf(purl, ppath)
                            if result3 and os.path.exists(result3):
                                cap3 = (
                                    f"📄 **New PDF — {subj_name}**\n\n"
                                    f"📝 `{lec_title[:55]} {suffix}`\n"
                                    f"📌 Topic: {appx_topic_name}\n"
                                    f"🤖 Auto #{auto_id} | {brand}"
                                )
                                await _send_to_topic(sg_id, topic_id, result3, cap3, is_video=False)
                                try:
                                    os.remove(result3)
                                except Exception:
                                    pass

                    # ── NEW VIDEO LECTURE ────────────────────────────────
                    if track_lec and lec_id not in seen_lec_ids and url:
                        new_lec_ids.add(lec_id)
                        out_path = os.path.join(tmp_dir, f"lec_{lec_id}")
                        ok = await download_item(
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
                                await _send_to_topic(sg_id, topic_id, vid_file, cap, is_video=True)
                                try:
                                    os.remove(vid_file)
                                except Exception:
                                    pass

                    await asyncio.sleep(0.5)

            # ── TEST SERIES for this subject ─────────────────────────────
            if track_tests:
                ts_list = await get_test_series_list(
                    session, api_base, appx_uid, course_id, hdr
                )

                for ts in ts_list:
                    series_id   = str(ts.get("id") or ts.get("test_series_id", ""))
                    series_name = ts.get("title") or ts.get("name") or f"Series_{series_id}"
                    if not series_id:
                        continue

                    ts_topic_map = subj.setdefault("ts_topic_map", {})
                    ts_thread_id = ts_topic_map.get(series_id)

                    if not ts_thread_id:
                        ts_thread_id = await _create_topic(
                            sg_id, f"{subj_name} — {series_name}"
                        )
                        if ts_thread_id:
                            ts_topic_map[series_id] = ts_thread_id
                            _aw().update_one(
                                {"user_id": user_id, "auto_id": auto_id,
                                 "subjects.subject_id": subj_id},
                                {"$set": {
                                    f"subjects.$.ts_topic_map.{series_id}": ts_thread_id
                                }}
                            )
                            await asyncio.sleep(0.5)
                        else:
                            ts_thread_id = topic_id

                    ts_subjects = await get_testseries_subjects(
                        session, api_base, series_id, hdr
                    )

                    matched_ts_subj = None
                    for ts_subj in ts_subjects:
                        ts_sid   = str(ts_subj.get("id") or ts_subj.get("subject_id") or ts_subj.get("subjectid", ""))
                        ts_sname = (ts_subj.get("subject_name") or ts_subj.get("name") or "").lower()
                        if ts_sid == subj_id:
                            matched_ts_subj = ts_subj
                            break
                        if subj_name.lower() in ts_sname or ts_sname in subj_name.lower():
                            matched_ts_subj = ts_subj
                            break

                    if not matched_ts_subj and ts_subjects:
                        for ts_subj in ts_subjects:
                            ts_sid = str(ts_subj.get("id") or ts_subj.get("subjectid") or ts_subj.get("subject_id", "0"))
                            if ts_sid in ("0", ""):
                                matched_ts_subj = ts_subj
                                break

                    if not matched_ts_subj:
                        continue

                    matched_sid = str(matched_ts_subj.get("id") or
                                      matched_ts_subj.get("subjectid") or
                                      matched_ts_subj.get("subject_id", "0"))

                    tests = await get_testseries_tests(
                        session, api_base, series_id, matched_sid, hdr
                    )

                    for test in tests:
                        test_id    = str(test.get("id") or test.get("test_id", ""))
                        test_title = (test.get("title") or test.get("test_title") or f"Test_{test_id}")
                        if not test_id or test_id in seen_test_ids:
                            continue
                        new_test_ids.add(test_id)

                        questions = await fetch_testseries_questions(session, api_base, test, hdr)
                        if not questions:
                            continue

                        tpath = os.path.join(tmp_dir, f"test_{test_id}.html")
                        save_quiz_html(questions, tpath, test_title, watermark=brand)
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
                            await _send_to_topic(sg_id, ts_thread_id, tpath, cap, is_video=False)
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

        # ── Save updated state ───────────────────────────────────────────
        _aw().update_one(
            {"user_id": user_id, "auto_id": auto_id},
            {"$set": {
                "subjects":    updated_subjects,
                "last_run_at": datetime.utcnow()
            }}
        )
        print(f"[Scheduler] ✅ Auto #{auto_id} for user {user_id} done.")


async def run_all_due_autos():
    """Called every minute by APScheduler. Fires autos whose IST HH:MM matches now."""
    now_ist = datetime.now(IST).strftime("%H:%M")
    due = list(_aw().find({"check_time_ist": now_ist, "is_active": True}))
    if not due:
        return
    print(f"[Scheduler] {len(due)} auto(s) due at {now_ist} IST")
    for doc in due:
        try:
            await _run_auto_update(doc)
        except Exception as e:
            print(f"[Scheduler] Error in auto #{doc.get('auto_id')}: {e}")

# ══════════════════════════════════════════════════════
#  /addauto COMMAND
# ══════════════════════════════════════════════════════

@bot.on_message(filters.command("addauto") & filters.private)
async def addauto_handler(bot_client: Client, m: Message):
    uid = m.from_user.id

    bot_info = await bot_client.get_me()
    if not db.is_user_authorized(uid, bot_info.username):
        await m.reply_text(
            f"<blockquote>❌ **Access Denied!**\n"
            f"You are not a Premium member.\n"
            f"Your User ID: `{uid}`</blockquote>"
        )
        return

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

    # ── Step 1: API Domain ───────────────────────────────────────────────
    ask = await m.reply_text(
        "**🔧 Auto-Setup — Step 1/7**\n\n"
        "🌐 **Enter your AppX API domain:**\n"
        "<blockquote>e.g. `parmaracademyapi.teachx.in`\n"
        "Send without `https://`\n\n"
        "Send /cancel to abort.</blockquote>"
    )
    try:
        msg = await bot_client.listen(m.chat.id, timeout=120)
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
        msg = await bot_client.listen(m.chat.id, timeout=60)
    except Exception:
        await ask.edit_text("⏰ Timed out.")
        return
    login_choice = (msg.text or "").strip()
    await msg.delete(); await ask.delete()

    phone = password = token = appx_uid = ""

    if login_choice == "2":
        ask = await m.reply_text("**Send your JWT Token:**")
        try:
            msg = await bot_client.listen(m.chat.id, timeout=120)
        except Exception:
            await ask.edit_text("⏰ Timed out."); return
        token = (msg.text or "").strip()
        await msg.delete(); await ask.delete()
        appx_uid = str(_extract_user_id_from_jwt(token) or "")
        if not appx_uid:
            ask2 = await m.reply_text("⚠️ Could not extract User ID. Send it manually:")
            try:
                msg2 = await bot_client.listen(m.chat.id, timeout=60)
                appx_uid = (msg2.text or "").strip()
                await msg2.delete(); await ask2.delete()
            except Exception:
                await ask2.edit_text("⏰ Timed out."); return
    else:
        ask = await m.reply_text(
            "**🔧 Step 2a/7 — Phone / Email:**\n"
            "Send your registered phone or email:"
        )
        try:
            msg = await bot_client.listen(m.chat.id, timeout=120)
        except Exception:
            await ask.edit_text("⏰ Timed out."); return
        phone = (msg.text or "").strip()
        await msg.delete(); await ask.delete()

        ask = await m.reply_text("**🔧 Step 2b/7 — Password:**")
        try:
            msg = await bot_client.listen(m.chat.id, timeout=120)
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

    # ── Fetch courses ────────────────────────────────────────────────────
    wait = await m.reply_text("⏳ Fetching your courses…")
    async with aiohttp.ClientSession() as session:
        hdr     = make_headers(token, appx_uid)
        courses = await get_courses(session, api_base, appx_uid, hdr)

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
        msg = await bot_client.listen(m.chat.id, timeout=60)
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
        hdr          = make_headers(token, appx_uid)
        subjects_raw = await get_subjects(session, api_base, course_id, hdr)

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
        msg = await bot_client.listen(m.chat.id, timeout=120)
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
        "topic thread per series inside the supergroup.</blockquote>"
    )
    try:
        msg = await bot_client.listen(m.chat.id, timeout=60)
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
        msg = await bot_client.listen(m.chat.id, timeout=30)
        raw_brand = (msg.text or "").strip()
        brand = CREDIT if raw_brand == "/d" else raw_brand
        await msg.delete(); await ask.delete()
    except Exception:
        brand = CREDIT
        await ask.delete()

    # ── Step 7a: Supergroup ID ───────────────────────────────────────────
    ask = await m.reply_text(
        "**🏠 Auto-Setup — Step 7a/7 — Supergroup ID**\n\n"
        "Send the **Supergroup ID** with Topics enabled.\n"
        "<blockquote>e.g. `-1001234567890`\n\n"
        "✅ Make sure the bot is **admin** with\n"
        "   **Manage Topics** permission in that group.</blockquote>"
    )
    try:
        msg = await bot_client.listen(m.chat.id, timeout=60)
    except Exception:
        await ask.edit_text("⏰ Timed out."); return
    try:
        sg_id = int((msg.text or "").strip())
    except Exception:
        await m.reply_text("❌ Invalid Supergroup ID."); return
    await msg.delete(); await ask.delete()

    # ── Step 7b: Schedule time ───────────────────────────────────────────
    ask = await m.reply_text(
        "**🕒 Auto-Setup — Step 7b/7 — Schedule Time (IST)**\n\n"
        "What time should I check for new content daily?\n"
        "<blockquote>Send in **HH:MM** 24-hour IST format\n"
        "e.g. `08:00` → 8 AM | `20:30` → 8:30 PM</blockquote>"
    )
    try:
        msg = await bot_client.listen(m.chat.id, timeout=60)
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
        tid   = await _create_topic(sg_id, sname)
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
        "password":       password,
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

    result_text = (
        f"✅ **Auto #{auto_id} Created!**\n\n"
        f"📚 **Course:** {course_name}\n"
        f"📖 **Subjects:** {len(subjects_docs)} topic(s) created\n"
        f"🔍 **Tracking:** {' | '.join(track_labels)}\n"
        f"🕒 **Daily at:** `{time_input}` IST\n"
        f"🏠 **Supergroup:** `{sg_id}`\n"
        f"🏷️ **Brand:** `{brand}`\n"
    )
    if failed_topics:
        result_text += f"\n⚠️ Topic creation failed for: {', '.join(failed_topics)}"
    result_text += (
        f"\n\n**Commands:**\n"
        f"/myautos — view all autos\n"
        f"/runnow {auto_id} — run immediately\n"
        f"/delauto — delete an auto"
    )
    await creating.edit_text(result_text)

# ══════════════════════════════════════════════════════
#  /myautos COMMAND
# ══════════════════════════════════════════════════════

@bot.on_message(filters.command("myautos") & filters.private)
async def myautos_handler(bot_client: Client, m: Message):
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
#  /runnow COMMAND
# ══════════════════════════════════════════════════════

@bot.on_message(filters.command("runnow") & filters.private)
async def runnow_handler(bot_client: Client, m: Message):
    uid  = m.from_user.id

    bot_info = await bot_client.get_me()
    if not db.is_user_authorized(uid, bot_info.username):
        await m.reply_text(
            f"<blockquote>❌ **Access Denied!**\nYour User ID: `{uid}`</blockquote>"
        )
        return

    args = m.text.split()
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
                msg = await bot_client.listen(m.chat.id, timeout=30)
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
        "You'll receive updates as new content is found."
    )
    asyncio.create_task(_run_auto_update(doc))

# ══════════════════════════════════════════════════════
#  /delauto COMMAND
# ══════════════════════════════════════════════════════

@bot.on_message(filters.command("delauto") & filters.private)
async def delauto_handler(bot_client: Client, m: Message):
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
        pick = await bot_client.listen(m.chat.id, timeout=60)
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
        choice_msg = await bot_client.listen(m.chat.id, timeout=60)
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
            subj_pick = await bot_client.listen(m.chat.id, timeout=60)
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
#  STANDARD BOT HANDLERS  (unchanged from original)
# ══════════════════════════════════════════════════════

m_file_path = "main.py"

@bot.on_message(filters.command("users") & filters.private)
async def _list_users(client, message):
    await list_users_cmd(client, message)

@bot.on_message(filters.command("plan") & filters.private)
async def _my_plan(client, message):
    await my_plan_cmd(client, message)

@bot.on_message(filters.command("addauth") & filters.private)
async def add_auth_user(client: Client, message: Message):
    if message.from_user.id != OWNER:
        return
    args = message.command[1:]
    if len(args) < 1:
        await message.reply_text("**Usage:** `/addauth <user_id> <days>`")
        return
    try:
        new_user_id = int(args[0])
        days = int(args[1]) if len(args) >= 2 else 30
    except ValueError:
        await message.reply_text("❌ user_id and days must be numbers.")
        return
    bot_info = await client.get_me()
    try:
        user = await client.get_users(new_user_id)
        name = user.first_name + (f" {user.last_name}" if user.last_name else "")
    except Exception:
        name = f"User {new_user_id}"
    success, expiry = db.add_user(new_user_id, name, days, bot_info.username)
    if success:
        await message.reply_text(
            f"**✅ User Added!**\n\n"
            f"<blockquote>👤 {name}\n"
            f"🆔 `{new_user_id}`\n"
            f"⏳ {days} days\n"
            f"📅 Expiry: {expiry.strftime('%d-%m-%Y %H:%M:%S')}</blockquote>"
        )
    else:
        await message.reply_text("❌ Failed to add user.")

@bot.on_message(filters.command("rmauth") & filters.private)
async def remove_auth_user(client: Client, message: Message):
    if message.from_user.id != OWNER:
        return
    args = message.command[1:]
    if len(args) < 1:
        await message.reply_text("**Usage:** `/rmauth <user_id>`")
        return
    try:
        user_id = int(args[0])
    except ValueError:
        await message.reply_text("❌ Invalid user ID.")
        return
    bot_info = await client.get_me()
    if db.remove_user(user_id, bot_info.username):
        await message.reply_text(f"✅ User `{user_id}` removed.")
        try:
            await client.send_message(user_id, "**⚠️ Subscription Ended**\n\nYour access has been revoked.")
        except Exception:
            pass
    else:
        await message.reply_text(f"❌ User `{user_id}` not found.")

@bot.on_message(filters.command("broadcast") & filters.private)
async def broadcast_handler(client: Client, message: Message):
    if message.chat.id != OWNER:
        return
    if not message.reply_to_message:
        await message.reply_text("**Reply to a message with /broadcast to send it to all users.**")
        return
    success = fail = 0
    for uid in list(set(TOTAL_USERS)):
        try:
            if message.reply_to_message.text:
                await client.send_message(uid, message.reply_to_message.text)
            elif message.reply_to_message.photo:
                await client.send_photo(uid, photo=message.reply_to_message.photo.file_id,
                                        caption=message.reply_to_message.caption or "")
            elif message.reply_to_message.video:
                await client.send_video(uid, video=message.reply_to_message.video.file_id,
                                        caption=message.reply_to_message.caption or "")
            elif message.reply_to_message.document:
                await client.send_document(uid, document=message.reply_to_message.document.file_id,
                                           caption=message.reply_to_message.caption or "")
            else:
                await client.forward_messages(uid, message.chat.id, message.reply_to_message.id)
            success += 1
        except Exception:
            fail += 1
    await message.reply_text(
        f"<b>Broadcast complete!</b>\n"
        f"<blockquote><b>✅ Success: {success}\n❎ Failed: {fail}</b></blockquote>"
    )

@bot.on_message(filters.command("broadusers") & filters.private)
async def broadusers_handler(client: Client, message: Message):
    if message.chat.id != OWNER:
        return
    if not TOTAL_USERS:
        await message.reply_text("**No Broadcasted Users**")
        return
    user_infos = []
    for uid in list(set(TOTAL_USERS)):
        try:
            u     = await client.get_users(int(uid))
            fname = u.first_name or " "
            user_infos.append(f"[{u.id}](tg://openmessage?user_id={u.id}) | `{fname}`")
        except Exception:
            user_infos.append(f"[{uid}](tg://openmessage?user_id={uid})")
    total = len(user_infos)
    for i in range(0, max(1, total), 50):
        chunk = user_infos[i:i + 50]
        header = f"<blockquote><b>Total Users: {total}</b></blockquote>\n" if i == 0 else ""
        await message.reply_text(header + "\n".join(chunk))

@bot.on_message(filters.command("cookies") & filters.private)
async def cookies_handler(client: Client, m: Message):
    editable = await m.reply_text("**Please upload the YouTube Cookies file (.txt format).**")
    try:
        input_message: Message = await client.listen(m.chat.id)
        if not input_message.document or not input_message.document.file_name.endswith(".txt"):
            await m.reply_text("Invalid file type. Please upload a .txt file.")
            return
        downloaded_path = await input_message.download()
        with open(downloaded_path, "r") as f:
            cookies_content = f.read()
        db.delete_cookies()
        saved = db.save_cookies(cookies_content)
        with open(cookies_file_path, "w") as f:
            f.write(cookies_content)
        await editable.delete()
        await input_message.delete()
        db_status = "✅ Saved to MongoDB" if saved else "⚠️ MongoDB save failed, local only"
        await m.reply_text(
            f"✅ **Cookies updated!**\n"
            f"📂 Saved as `{cookies_file_path}`\n"
            f"🗄️ {db_status}"
        )
    except Exception as e:
        await m.reply_text(f"**Failed:**\n<blockquote>{str(e)}</blockquote>")

@bot.on_message(filters.command("getcookies") & filters.private)
async def getcookies_handler(client: Client, m: Message):
    try:
        await client.send_document(chat_id=m.chat.id, document=cookies_file_path,
                                   caption="Here is the `youtube_cookies.txt` file.")
    except Exception as e:
        await m.reply_text(f"⚠️ {str(e)}")

@bot.on_message(filters.command("mfile") & filters.private)
async def mfile_handler(client: Client, m: Message):
    try:
        await client.send_document(chat_id=m.chat.id, document=m_file_path,
                                   caption="Here is the `main.py` file.")
    except Exception as e:
        await m.reply_text(f"⚠️ {str(e)}")

@bot.on_message(filters.command("reset"))
async def restart_handler(_, m):
    if m.chat.id != OWNER:
        return
    await m.reply_text("𝐁𝐨𝐭 𝐢𝐬 𝐑𝐞𝐬𝐞𝐭𝐭𝐢𝐧𝐠...")
    os.execl(sys.executable, sys.executable, *sys.argv)

@bot.on_message(filters.command("stop"))
async def cancel_handler(client: Client, m: Message):
    user_id = m.from_user.id
    bot_info = await client.get_me()
    if not db.is_user_authorized(m.chat.id, bot_info.username):
        await m.reply_text(
            f"<blockquote>❌ **Access Denied!**\n"
            f"You are not a Premium member.\n"
            f"Your User ID: `{m.chat.id}`</blockquote>"
        )
        return
    if processing_request.get(user_id, False):
        cancel_requested[user_id] = True
        await m.delete()
        await m.reply_text("**🚦 Cancel request received. Stopping after current item…**")
    else:
        await m.reply_text("**⚡ No active process to cancel.**")

@bot.on_message(filters.command("start"))
async def start(bot_client, m: Message):
    user_id = m.chat.id
    if user_id not in TOTAL_USERS:
        TOTAL_USERS.append(user_id)
    bot_info = await bot_client.get_me()
    if db.is_user_authorized(m.chat.id, bot_info.username):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✨ Commands", callback_data="cmd_command"),
             InlineKeyboardButton("💳 Plans", callback_data="upgrade_command")],
            [InlineKeyboardButton("📞 Contact", url=f"tg://openmessage?user_id={OWNER}"),
             InlineKeyboardButton("🛠️ Repo", url="https://github.com/nikhilsainiop/saini-txt-direct")],
        ])
        await m.reply_text(
            f"🌟 Welcome **{m.from_user.first_name}**!\n\n"
            f"✅ You are a **Premium member**.\n"
            f"Use `/appx` to start the AppX Course Downloader!\n\n"
            f"Contact [{CREDIT}](tg://openmessage?user_id={OWNER}) for support.",
            disable_web_page_preview=True, reply_markup=keyboard
        )
    else:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✨ Commands", callback_data="cmd_command"),
             InlineKeyboardButton("💳 Plans", callback_data="upgrade_command")],
            [InlineKeyboardButton("📞 Contact", url=f"tg://openmessage?user_id={OWNER}")],
        ])
        await m.reply_text(
            f"🎉 Welcome **{m.from_user.first_name}** to **AppX Downloader Bot**!\n\n"
            f"You are on the **free tier**. Contact [{CREDIT}](tg://openmessage?user_id={OWNER}) "
            f"to get Premium access.\n\n"
            f"Your ID: `{m.chat.id}`",
            disable_web_page_preview=True, reply_markup=keyboard
        )

@bot.on_callback_query(filters.regex("cmd_command"))
async def cmd_callback(client, cq):
    caption = (
        f"💥 **Bot Commands**\n"
        f"{'▰'*16}\n\n"
        f"➥ /start — Check bot status\n"
        f"➥ /appx — **AppX Course Downloader**\n"
        f"➥ /stop — Cancel running task\n"
        f"➥ /plan — Your subscription\n"
        f"➥ /id — Get your ID\n"
        f"➥ /info — Your Telegram info\n"
        f"➥ /logs — View bot logs\n"
        f"➥ /cookies — Update YouTube cookies\n\n"
        f"**Auto-Update:**\n"
        f"➥ /addauto — New auto-update watcher\n"
        f"➥ /myautos — View your autos\n"
        f"➥ /runnow — Trigger immediately\n"
        f"➥ /delauto — Delete an auto\n\n"
        f"{'▰'*16}\n"
        f"Owner commands: /addauth /rmauth /users /broadcast /reset\n"
    )
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔙 Back", callback_data="back_main")
    ]])
    await cq.message.edit_text(caption, reply_markup=keyboard)
    await cq.answer()

@bot.on_callback_query(filters.regex("upgrade_command"))
async def upgrade_callback(client, cq):
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📞 Get Access", url=f"tg://openmessage?user_id={OWNER}"),
        InlineKeyboardButton("🔙 Back", callback_data="back_main")
    ]])
    await cq.message.edit_text(
        f"**💳 Premium Plans**\n\n"
        f"Get access to download from:\n"
        f"<blockquote>• AppX Encrypted courses\n• Classplus (DRM + NDRM)\n"
        f"• PhysicsWallah • CareerWill\n• Vimeo • YouTube + more</blockquote>\n\n"
        f"**Monthly: 100 INR**\n\n"
        f"Contact [{CREDIT}](tg://openmessage?user_id={OWNER}) to subscribe.",
        reply_markup=keyboard
    )
    await cq.answer()

@bot.on_callback_query(filters.regex("back_main"))
async def back_main_callback(client, cq):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✨ Commands", callback_data="cmd_command"),
         InlineKeyboardButton("💳 Plans", callback_data="upgrade_command")],
        [InlineKeyboardButton("📞 Contact", url=f"tg://openmessage?user_id={OWNER}")],
    ])
    await cq.message.edit_text(
        f"🌟 **AppX Downloader Bot**\n\nUse `/appx` to start downloading!",
        reply_markup=keyboard
    )
    await cq.answer()

@bot.on_message(filters.command("id"))
async def id_command(client, message: Message):
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(text="Send to Owner", url=f"tg://openmessage?user_id={OWNER}")
    ]])
    await message.reply_text(
        f"<blockquote><b>Your Chat ID:</b></blockquote>\n`{message.chat.id}`",
        reply_markup=keyboard
    )

@bot.on_message(filters.command("info"))
async def info_cmd(bot_client: Client, update: Message):
    u = update.from_user
    await update.reply_text(
        f"╭────────────────╮\n"
        f"│✨ **Your Telegram Info** ✨\n"
        f"├────────────────\n"
        f"├🔹 **Name:** `{u.first_name} {u.last_name or ''}`\n"
        f"├🔹 **Username:** @{u.username or 'N/A'}\n"
        f"├🔹 **TG ID:** `{u.id}`\n"
        f"├🔹 **Profile:** {u.mention}\n"
        f"╰────────────────╯",
        disable_web_page_preview=True, reply_markup=BUTTONS_CONTACT
    )

@bot.on_message(filters.command("logs"))
async def send_logs(client: Client, m: Message):
    try:
        with open("logs.txt", "rb") as file:
            sent = await m.reply_text("**📤 Sending logs…**")
            await m.reply_document(document=file)
            await sent.delete()
    except Exception as e:
        await m.reply_text(f"**Error:** `{e}`")

# ══════════════════════════════════════════════════════
#  MAIN /appx COMMAND — ENTRY POINT
# ══════════════════════════════════════════════════════

@bot.on_message(filters.command("appx"))
async def appx_handler(bot_client: Client, m: Message):
    user_id = m.from_user.id

    bot_info = await bot_client.get_me()
    if not db.is_user_authorized(user_id, bot_info.username):
        await m.reply_text(
            f"<blockquote>❌ **Access Denied!**\n"
            f"You are not a Premium member.\n"
            f"Your User ID: `{user_id}`</blockquote>\n\n"
            f"Contact [{CREDIT}](tg://openmessage?user_id={OWNER}) to get access.",
            disable_web_page_preview=True
        )
        return

    if processing_request.get(user_id, False):
        await m.reply_text("⚠️ You already have a running task. Use /stop to cancel it first.")
        return

    processing_request[user_id] = True
    cancel_requested[user_id]   = False

    try:
        await appx_main_flow(bot_client, m, user_id)
    except Exception as e:
        await m.reply_text(f"**❌ Error:**\n<blockquote>{str(e)}</blockquote>")
        logging.error(f"appx_handler error: {e}")
    finally:
        processing_request[user_id] = False
        cancel_requested[user_id]   = False

# ══════════════════════════════════════════════════════
#  STARTUP
# ══════════════════════════════════════════════════════

def notify_owner():
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": OWNER, "text": "🤖 AppX Bot Started Successfully ✅"},
            timeout=10
        )
    except Exception:
        pass

def reset_and_set_commands():
    base = f"https://api.telegram.org/bot{BOT_TOKEN}"
    requests.post(f"{base}/setMyCommands", json={"commands": []}, timeout=10)
    commands = [
        {"command": "start",       "description": "✅ Start the bot"},
        {"command": "appx",        "description": "📥 AppX Course Downloader"},
        {"command": "stop",        "description": "🚫 Stop running task"},
        {"command": "plan",        "description": "💳 Check your plan"},
        {"command": "id",          "description": "🆔 Get your ID"},
        {"command": "info",        "description": "ℹ️ Your Telegram info"},
        {"command": "logs",        "description": "👁️ View bot logs"},
        {"command": "cookies",     "description": "🍪 Upload YouTube cookies"},
        {"command": "getcookies",  "description": "📁 Get cookies file"},
        {"command": "mfile",       "description": "📄 Get main.py"},
        {"command": "reset",       "description": "🔄 Restart bot (owner)"},
        {"command": "addauth",     "description": "▶️ Add user (owner)"},
        {"command": "rmauth",      "description": "⏸️ Remove user (owner)"},
        {"command": "users",       "description": "👥 List users (owner)"},
        {"command": "broadcast",   "description": "📢 Broadcast (owner)"},
        {"command": "broadusers",  "description": "👨‍👩‍👧‍👦 Broadcast users (owner)"},
        {"command": "addauto",     "description": "🔔 Set up auto-update watcher"},
        {"command": "myautos",     "description": "📋 List your auto-updates"},
        {"command": "runnow",      "description": "🚀 Trigger auto-update now"},
        {"command": "delauto",     "description": "🗑️ Delete an auto-update"},
    ]
    requests.post(f"{base}/setMyCommands", json={"commands": commands}, timeout=10)


def load_cookies_from_db():
    try:
        cookies = db.get_cookies()
        if cookies:
            with open(cookies_file_path, "w") as f:
                f.write(cookies)
            print(f"\033[92m✓ YouTube cookies loaded from MongoDB → {cookies_file_path}\033[0m")
        else:
            print("\033[93m⚠ No cookies in MongoDB\033[0m")
    except Exception as e:
        print(f"\033[91m✕ Failed to load cookies from DB: {e}\033[0m")


async def _start_scheduler():
    """
    Initialise MongoDB indexes and start the APScheduler.
    Must be called from inside a running event loop
    (i.e. from bot's on_start / startup hook).
    """
    _ensure_scheduler_indexes()
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        run_all_due_autos,
        CronTrigger(minute="*"),
        id="auto_update_job",
        max_instances=1,
        coalesce=True
    )
    scheduler.start()
    print("\033[92m✓ Auto-Update Scheduler started (checks every minute for IST time match)\033[0m")


@bot.on_message(filters.command("_dummy_never_matches_xz99"))
async def _dummy(_c, _m):
    pass


async def _on_startup():
    """Runs inside the event loop right after Pyrogram connects."""
    load_cookies_from_db()
    os.makedirs(os.path.join(tempfile.gettempdir(), "downloads"), exist_ok=True)
    os.makedirs(os.path.join(tempfile.gettempdir(), "auto_dl"),   exist_ok=True)
    await _start_scheduler()
    print("\033[92m✓ AppX Downloader Bot started\033[0m")


async def _startup_hook():
    await _on_startup()
    print("\033[92m✓ Bot is online and listening for updates\033[0m")


if __name__ == "__main__":
    reset_and_set_commands()
    notify_owner()
    bot.run(_startup_hook())
