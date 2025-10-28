import os
import re
import time
import threading
import requests
import traceback
import pandas as pd
import dateutil.parser
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request
from dotenv import load_dotenv
from supabase import create_client, Client
import telebot
from apscheduler.schedulers.background import BackgroundScheduler
import json
import html  # <- You need this at the top

text = "<b>hello</b>"
escaped = html.escape(text)  # now this works//////////////////

def safe_send(chat_id, text, parse_mode=None):
    """Safely send a message, splitting it if it's too long."""
    try:
        MAX_LEN = 4000
        if len(text) > MAX_LEN:
            for i in range(0, len(text), MAX_LEN):
                bot.send_message(chat_id, text[i:i+MAX_LEN], parse_mode=parse_mode)
        else:
            bot.send_message(chat_id, text, parse_mode=parse_mode)
    except Exception as e:
        print(f"[ERROR] Sending message failed: {e}")



# ---------------------------
# CONFIG
# ---------------------------
load_dotenv()
TZ = ZoneInfo("Asia/Yangon")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
NEWS_GROUP_ID = int(os.getenv("NEWS_GROUP_ID", "0"))
SUPPLIER_GROUP_ID = int(os.getenv("SUPPLIER_GROUP_ID", "0"))
K2BOOST_GROUP_ID = int(os.getenv("K2BOOST_GROUP_ID", "0"))
GROUP_ID = int(os.getenv("GROUP_ID", "0"))
REPORT_GROUP_ID = int(os.getenv("REPORT_GROUP_ID", "0"))
SMMGEN_API_KEY = os.getenv("SMMGEN_API_KEY")
SMMGEN_URL = os.getenv("SMMGEN_URL", "https://smmgen.com/api/v2")
USD_TO_MMK = float(os.getenv("USD_TO_MMK", "4500"))

if not (SUPABASE_URL and SUPABASE_KEY and BOT_TOKEN):
    raise RuntimeError("Please provide SUPABASE_URL, SUPABASE_KEY and TELEGRAM_TOKEN in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")
db_lock = threading.Lock()
app = Flask(__name__)
scheduler = BackgroundScheduler(timezone="UTC")

# ---------------------------
# UTIL / HELPERS
# ---------------------------
_escape_re = re.compile(r'([_*[\]()~`>#+\-=|{}.!])')

def now_yangon():
    return datetime.now(TZ)

def iso_now():
    return datetime.utcnow().isoformat()

def escape_markdown(text: str) -> str:
    if text is None:
        return ""
    return _escape_re.sub(r'\\\1', str(text))

def try_parse_iso(s):
    try:
        return dateutil.parser.isoparse(s) if s else None
    except Exception:
        return None

def is_transient_exception(e: Exception) -> bool:
    name = type(e).__name__
    msg = str(e).lower()
    if isinstance(e, requests.exceptions.RequestException):
        return True
    for k in ("connection reset", "broken pipe", "connection aborted", "timed out", "timeout", "remote protocol error"):
        if k in msg:
            return True
    return False

def safe_execute(func, retries=5, base_delay=0.5, *args, **kwargs):
    last_exc = None
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            if is_transient_exception(e):
                delay = base_delay * (2 ** attempt)
                print(f"[safe_execute] transient error ({e}), retrying in {delay:.2f}s (attempt {attempt+1}/{retries})")
                time.sleep(delay)
                continue
            else:
                raise
    print(f"[safe_execute] operation failed after {retries} attempts: {last_exc}")
    raise last_exc

def safe_request(method, url, retries=3, timeout=25, **kwargs):
    last_exc = None
    for attempt in range(retries):
        try:
            r = requests.request(method, url, timeout=timeout, **kwargs)
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            if is_transient_exception(e) and attempt + 1 < retries:
                delay = 1 + attempt * 2
                print(f"[safe_request] transient {e}, retrying in {delay}s (attempt {attempt+1}/{retries})")
                time.sleep(delay)
                continue
            else:
                raise
    raise last_exc

def safe_send(chat_id, text, parse_mode=None):
    """Safely send Telegram messages with optional Markdown/HTML formatting"""
    try:
        if parse_mode:
            bot.send_message(chat_id, text, parse_mode=parse_mode)
        else:
            bot.send_message(chat_id, text)
    except Exception as e:
        print("Telegram send error:", e)

app = Flask(__name__)

sent_ids = set()

def poll_supportbox():
    """Check SupportBox table every 10s for pending tickets"""
    while True:
        try:
            response = supabase.table("SupportBox").select("*").eq("status", "Pending").execute()
            rows = response.data or []

            for row in rows:
                id_ = row.get("id")
                if id_ in sent_ids:
                    continue

                email = row.get("email", "")
                subject = row.get("subject", "Other")
                message = row.get("message", "")
                order_id = row.get("order_id", "")

                text = (
                    "📢 New Support Ticket\n"
                    f"ID - {id_}\n"
                    f"Email - {email}\n"
                    f"Subject - {subject}\n"
                    f"Order ID - {order_id}\n\n"
                    f"Message:\n{message}\n\n"
                    "Commands:\n"
                    f"/Answer {id_} [reply message]\n"
                    f"/Close {id_}"
                )

                try:
                    bot.send_message(NEWS_GROUP_ID, text)
                    supabase.table("SupportBox").update({"status": "Sent"}).eq("id", id_).execute()
                    sent_ids.add(id_)
                    print(f"[SENT] Ticket {id_} sent to group.")
                except Exception as send_err:
                    print(f"[ERROR] Sending message failed: {send_err}")

        except Exception as e:
            print(f"[ERROR] Polling failed: {e}")

        time.sleep(10)


@bot.message_handler(commands=['Answer'])
def handle_answer(message):
    try:
        parts = message.text.split(' ', 2)
        if len(parts) < 3:
            bot.reply_to(message, "Usage: /Answer ID reply_message")
            return

        id_ = int(parts[1])
        reply_text = parts[2]

        supabase.table("SupportBox").update({
            "reply_text": reply_text,
            "status": "Replied",
            "replied_at": "now()",
            "UserStatus": "Unread"
        }).eq("id", id_).execute()

        bot.reply_to(message, f"Replied to ticket ID {id_}")
        print(f"[REPLY] Ticket {id_} answered.")

    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        print(f"[ERROR] /Answer failed: {e}")


@bot.message_handler(commands=['Close'])
def handle_close(message):
    try:
        parts = message.text.split(' ', 1)
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /Close ID")
            return

        id_ = int(parts[1])

        supabase.table("SupportBox").update({
            "status": "Closed",
            "UserStatus": "Unread"
        }).eq("id", id_).execute()

        bot.reply_to(message, f"Closed ticket ID {id_}")
        print(f"[CLOSED] Ticket {id_} closed.")

    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        print(f"[ERROR] /Close failed: {e}")






sent_ids = set()

def update_user_balance(email, amount):
    """Add USD balance to user"""
    try:
        # Get user
        res = supabase.table("users").select("balance_usd").eq("email", email).execute()
        if not res.data:
            print(f"[WARN] User not found: {email}")
            return False

        current_balance = float(res.data[0]["balance_usd"] or 0)
        new_balance = current_balance + float(amount)

        supabase.table("users").update({"balance_usd": new_balance}).eq("email", email).execute()
        print(f"[OK] Updated balance for {email}: {current_balance} → {new_balance}")
        return True
    except Exception as e:
        print(f"[ERROR] Balance update failed: {e}")
        return False


def poll_affiliate():
    """Poll affiliate table for Pending entries every 10s"""
    while True:
        try:
            res = supabase.table("affiliate").select("*").eq("status", "Pending").execute()
            rows = res.data or []

            for row in rows:
                aff_id = row["id"]
                if aff_id in sent_ids:
                    continue

                email = row["email"]
                amount = float(row["amount"])
                method = row["method"]
                phone_id = row.get("phone_id") or "-"
                name = row.get("name") or "-"

                # First mark as processing
                supabase.table("affiliate").update({"status": "Processing"}).eq("id", aff_id).execute()

                if method.lower() == "topup":
                    ok = update_user_balance(email, amount)
                    if ok:
                        supabase.table("affiliate").update({"status": "Accepted"}).eq("id", aff_id).execute()

                        msg = (
                            "💰 Affiliate Topup\n\n"
                            f"🆔 ID = {aff_id}\n"
                            f"📧 Email = {email}\n"
                            f"💳 Method = {method}\n"
                            f"💵 Amount USD = {amount}\n"
                            f"🇲🇲 Amount MMK = {amount * USD_TO_MMK:,.0f}"
                        )
                        bot.send_message(GROUP_ID, msg)
                        print(f"[TopUp] Accepted ID {aff_id} for {email}")
                else:
                    msg = (
                        "🆕 New Affiliate Request\n\n"
                        f"🆔 ID = {aff_id}\n"
                        f"📧 Email = {email}\n"
                        f"💰 Amount = {amount}\n"
                        f"💳 Method = {method}\n"
                        f"📱 Phone ID = {phone_id}\n"
                        f"👤 Name = {name}\n\n"
                        f"🇲🇲 Amount MMK = {amount * USD_TO_MMK:,.0f}\n"
                        "🛠 Admin Actions:\n"
                        f"/Accept {aff_id}\n"
                        f"/Failed {aff_id}"
                    )
                    bot.send_message(GROUP_ID, msg)
                    print(f"[Request] New Affiliate Request ID {aff_id}")

                sent_ids.add(aff_id)

        except Exception as e:
            print(f"[ERROR] Polling affiliate failed: {e}")

        time.sleep(10)


@bot.message_handler(commands=['Accept'])
def handle_accept(message):
    try:
        parts = message.text.split(' ', 1)
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /Accept ID")
            return

        aff_id = int(parts[1])
        res = supabase.table("affiliate").select("*").eq("id", aff_id).execute()
        if not res.data:
            bot.reply_to(message, "Affiliate ID not found.")
            return
        row = res.data[0]
        email = row["email"]
        amount = float(row["amount"])

        ok = update_user_balance(email, amount)
        if ok:
            supabase.table("affiliate").update({"status": "Accepted"}).eq("id", aff_id).execute()
            bot.reply_to(message, f"✅ Accepted ID {aff_id} ({email})")
            print(f"[Accept] ID {aff_id} accepted for {email}")
        else:
            bot.reply_to(message, f"⚠️ Balance update failed for {email}")

    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        print(f"[ERROR] /Accept failed: {e}")


@bot.message_handler(commands=['Failed'])
def handle_failed(message):
    try:
        parts = message.text.split(' ', 1)
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /Failed ID")
            return

        aff_id = int(parts[1])
        supabase.table("affiliate").update({"status": "Failed"}).eq("id", aff_id).execute()
        bot.reply_to(message, f"❌ Failed ID {aff_id}")
        print(f"[Fail] ID {aff_id} marked as failed.")

    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
        print(f"[ERROR] /Failed failed: {e}")



USD_TO_MMK = 4500
POLL_INTERVAL = 10  # seconds

# =================================
# INITIAL SETUP
# ================================
processed_ids = set()


# =================================
# FUNCTIONS
# =================================
def update_transaction_status(tx_id, status):
    supabase.table("transactions").update({"status": status}).eq("id", tx_id).execute()


def update_verify_status(txid, status):
    supabase.table("VerifyPayment").update({"status": status}).eq("transaction_id", txid).execute()


def update_user_balance(email, amount):
    user = supabase.table("users").select("balance_usd").eq("email", email).single().execute()
    if user.data:
        old_balance = float(user.data["balance_usd"])
        new_balance = old_balance + float(amount)
        supabase.table("users").update({"balance_usd": new_balance}).eq("email", email).execute()
        return True
    return False


# =================================
# POLLING LOOP
# =================================
def poll_transactions():
    while True:
        try:
            result = supabase.table("transactions").select("*").eq("status", "Pending").execute()
            transactions = result.data or []

            for tx in transactions:
                txid = tx.get("transaction_id")
                email = tx.get("email")
                method = tx.get("method")
                amount = float(tx.get("amount") or 0)
                tx_db_id = tx.get("id")

                if tx_db_id in processed_ids:
                    continue

                # Mark as processing
                update_transaction_status(tx_db_id, "Processing")
                processed_ids.add(tx_db_id)

                # Find matching VerifyPayment
                verify = (
                    supabase.table("VerifyPayment")
                    .select("*")
                    .eq("transaction_id", txid)
                    .eq("method", method)
                    .eq("status", "unused")
                    .execute()
                )

                match = None
                if verify.data:
                    for v in verify.data:
                        if abs(float(v["amount_usd"]) - amount) < 0.0001:
                            match = v
                            break

                # CASE 1: Auto Verified
                if match:
                    update_verify_status(txid, "used")
                    update_user_balance(email, amount)
                    update_transaction_status(tx_db_id, "Accepted")

                    mmk = amount * USD_TO_MMK
                    message = (
                        "✅ Auto Top-up Completed\n\n"
                        + f"👤 User: {email}\n"
                        + f"💳 Method: {method}\n"
                        + f"💰 Amount USD: {amount}\n"
                        + f"🇲🇲 Amount MMK: {mmk:,.0f}\n"
                        + f"🧾 Transaction ID: {txid}"
                    )
                    bot.send_message(GROUP_ID, message)

                # CASE 2: Unverified
                else:
                    mmk = amount * USD_TO_MMK
                    message = (
                        "🆕 New Unverified Transaction\n\n"
                        + f"🆔 ID: {tx_db_id}\n"
                        + f"📧 Email: {email}\n"
                        + f"💳 Method: {method}\n"
                        + f"💵 Amount USD: {amount}\n"
                        + f"🇲🇲 Amount MMK: {mmk:,.0f}\n"
                        + f"🧾 Transaction ID: {txid}\n\n"
                        + "🛠 Admin Commands:\n"
                        + f"/Yes {tx_db_id}\n"
                        + f"/No {tx_db_id}"
                    )
                    bot.send_message(GROUP_ID, message)

        except Exception as e:
            print("Polling Error:", e)

        time.sleep(POLL_INTERVAL)


# =================================
# ADMIN COMMANDS
# =================================
@bot.message_handler(commands=["Yes"])
def yes_command(message):
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /Yes ID")
            return

        tx_id = int(parts[1])
        data = supabase.table("transactions").select("*").eq("id", tx_id).single().execute().data

        if not data:
            bot.reply_to(message, "Transaction not found.")
            return

        email = data["email"]
        amount = float(data["amount"])
        update_user_balance(email, amount)
        update_transaction_status(tx_id, "Accepted")

        bot.reply_to(message, f"Transaction #{tx_id} marked as Accepted and balance updated.")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")


@bot.message_handler(commands=["No"])
def no_command(message):
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /No ID")
            return

        tx_id = int(parts[1])
        update_transaction_status(tx_id, "Failed")
        bot.reply_to(message, f"Transaction #{tx_id} marked as Failed.")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")


@bot.message_handler(commands=["Use"])
def use_command(message):
    try:
        parts = message.text.split()
        if len(parts) < 2:
            bot.reply_to(message, "Usage: /Use TransactionID")
            return

        txid = parts[1]
        update_verify_status(txid, "used")
        bot.reply_to(message, f"VerifyPayment {txid} marked as used.")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")


# =================================
# MAIN
# ===============================

# === Helper functions ===

import os
import re
import time
import threading
import requests
import traceback
import pandas as pd
import dateutil.parser
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, jsonify, request
from dotenv import load_dotenv
from supabase import create_client, Client
import telebot
from apscheduler.schedulers.background import BackgroundScheduler
import json
import html  # <- You need this at the top

text = "<b>hello</b>"
escaped = html.escape(text)  # now this works

# ---------------------------
# CONFIG
# ---------------------------
load_dotenv()
TZ = ZoneInfo("Asia/Yangon")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
NEWS_GROUP_ID = int(os.getenv("NEWS_GROUP_ID", "0"))
SUPPLIER_GROUP_ID = int(os.getenv("SUPPLIER_GROUP_ID", "0"))
K2BOOST_GROUP_ID = int(os.getenv("K2BOOST_GROUP_ID", "0"))
GROUP_ID = int(os.getenv("GROUP_ID", "0"))
REPORT_GROUP_ID = int(os.getenv("REPORT_GROUP_ID", "0"))
SMMGEN_API_KEY = os.getenv("SMMGEN_API_KEY")
SMMGEN_URL = os.getenv("SMMGEN_URL", "https://smmgen.com/api/v2")
USD_TO_MMK = float(os.getenv("USD_TO_MMK", "4500"))

if not (SUPABASE_URL and SUPABASE_KEY and BOT_TOKEN):
    raise RuntimeError("Please provide SUPABASE_URL, SUPABASE_KEY and TELEGRAM_TOKEN in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")
db_lock = threading.Lock()
app = Flask(__name__)
scheduler = BackgroundScheduler(timezone="UTC")

# ---------------------------
# UTIL / HELPERS
# ---------------------------
_escape_re = re.compile(r'([_*[\]()~`>#+\-=|{}.!])')

def now_yangon():
    return datetime.now(TZ)

def iso_now():
    return datetime.utcnow().isoformat()

def escape_markdown(text: str) -> str:
    if text is None:
        return ""
    return _escape_re.sub(r'\\\1', str(text))

def try_parse_iso(s):
    try:
        return dateutil.parser.isoparse(s) if s else None
    except Exception:
        return None

def is_transient_exception(e: Exception) -> bool:
    name = type(e).__name__
    msg = str(e).lower()
    if isinstance(e, requests.exceptions.RequestException):
        return True
    for k in ("connection reset", "broken pipe", "connection aborted", "timed out", "timeout", "remote protocol error"):
        if k in msg:
            return True
    return False

def safe_execute(func, retries=5, base_delay=0.5, *args, **kwargs):
    last_exc = None
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            if is_transient_exception(e):
                delay = base_delay * (2 ** attempt)
                print(f"[safe_execute] transient error ({e}), retrying in {delay:.2f}s (attempt {attempt+1}/{retries})")
                time.sleep(delay)
                continue
            else:
                raise
    print(f"[safe_execute] operation failed after {retries} attempts: {last_exc}")
    raise last_exc

def safe_request(method, url, retries=3, timeout=25, **kwargs):
    last_exc = None
    for attempt in range(retries):
        try:
            r = requests.request(method, url, timeout=timeout, **kwargs)
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            if is_transient_exception(e) and attempt + 1 < retries:
                delay = 1 + attempt * 2
                print(f"[safe_request] transient {e}, retrying in {delay}s (attempt {attempt+1}/{retries})")
                time.sleep(delay)
                continue
            else:
                raise
    raise last_exc

def safe_send(chat_id, text, parse_mode=None):
    """Safely send Telegram messages with optional Markdown/HTML formatting"""
    try:
        if parse_mode:
            bot.send_message(chat_id, text, parse_mode=parse_mode)
        else:
            bot.send_message(chat_id, text)
    except Exception as e:
        print("Telegram send error:", e)


def update_user_balance(email, amount):
    try:
        with db_lock:
            user = safe_execute(lambda: supabase.table("users").select("balance_usd").eq("email", email).execute())
            if not user or not getattr(user, "data", None):
                print("No user for", email)
                return False
            bal = float(user.data[0].get("balance_usd") or 0)
            new = bal + float(amount)
            safe_execute(lambda: supabase.table("users").update({"balance_usd": new}).eq("email", email).execute())
        return True
    except Exception as e:
        print("Balance update error:", e)
        traceback.print_exc()
        return False

# ---------------------------
# SUPPORT BOX
# ---------------------------

# ---------------------------
# TRANSACTION HANDLERS
# ---------------------------
USD_TO_MMK = 4500

# ===============================
# SAFE HELPERS
# ===============================

def safe_execute(func):
    """Safely execute Supabase or other DB calls."""
    try:
        return func()
    except Exception as e:
        print("[ERROR] safe_execute failed:", e)
        traceback.print_exc()
        return type('Result', (object,), {'data': None})()


def escape_md2(text):
    """Escape Telegram MarkdownV2 special characters."""
    escape_chars = r"_*[]()~`>#+-=|{}.!\\"
    return "".join(f"\\{c}" if c in escape_chars else c for c in str(text or ""))


def safe_send(chat_id, text):
    """Send message to Telegram safely with MarkdownV2 escaping."""
    try:
        bot.send_message(chat_id, text, parse_mode="MarkdownV2")
    except Exception as e:
        print("[ERROR] Telegram send error:", e)
        traceback.print_exc()


# ===============================
# CORE FUNCTIONS
# ===============================

def update_user_balance(email, amount):
    """Increase user's USD balance safely."""
    try:
        with db_lock:
            user = safe_execute(lambda: supabase.table("users")
                .select("balance_usd")
                .eq("email", email)
                .execute()
            )

            if not user or not getattr(user, "data", None):
                print(f"[WARN] No user found for {email}")
                return False

            bal = float(user.data[0].get("balance_usd") or 0)
            new = bal + float(amount)

            safe_execute(lambda: supabase.table("users")
                .update({"balance_usd": new})
                .eq("email", email)
                .execute()
            )

            print(f"[INFO] Balance updated for {email}: {bal} ➜ {new}")
        return True
    except Exception as e:
        print("[ERROR] Balance update error:", e)
        traceback.print_exc()
        return False


def format_unverified_tx_message(tx):
    """Format an unverified transaction message for admins."""
    id_ = escape_md2(str(tx.get('id', '')))
    email = escape_md2(str(tx.get('email', '')))
    method = escape_md2(str(tx.get('method', '')))
    amount = float(tx.get('amount', 0) or 0)
    txid = escape_md2(str(tx.get('transaction_id', '')))
    mmk = amount * USD_TO_MMK

    return (
        "🆕 *New Unverified Transaction*\n\n"
        f"🆔 ID = {id_}\n"
        f"📧 Email = {email}\n"
        f"💳 Method = {method}\n"
        f"💵 Amount USD = {amount}\n\n"
        f"🇲🇲 Amount MMK = {mmk:.0f}\n"
        f"🧾 Transaction ID = {txid}\n\n"
        "🛠 *Admin Commands:*\n"
        f"/Yes {id_}\n"
        f"/No {id_}"
    )


def handle_transaction(tx):
    """Check and process a pending transaction."""
    try:
        email = tx.get("email")
        method = tx.get("method")
        amount = tx.get("amount")
        txid = tx.get("transaction_id")
        record_id = tx.get("id")

        vp = safe_execute(lambda: supabase.table("VerifyPayment")
            .select("*")
            .eq("method", method)
            .eq("amount_usd", amount)
            .eq("status", "unused")
            .eq("transaction_id", txid)
            .execute()
        )

        if vp.data:
            vp_id = vp.data[0]["id"]
            ok = update_user_balance(email, amount)
            if ok:
                safe_execute(lambda: supabase.table("VerifyPayment")
                    .update({"status": "used"})
                    .eq("id", vp_id)
                    .execute()
                )
                safe_execute(lambda: supabase.table("transactions")
                    .update({"status": "Accepted"})
                    .eq("id", record_id)
                    .execute()
                )

                usd = float(amount)
                mmk = usd * USD_TO_MMK
                msg = (
                    "✅ *Auto Top-up Completed*\n\n"
                    f"👤 User = {escape_md2(email)}\n"
                    f"💳 Method = {escape_md2(method)}\n"
                    f"💰 Amount USD = {usd}\n"
                    f"🇲🇲 Amount MMK = {mmk:.0f}\n"
                    f"🧾 Transaction ID = {escape_md2(txid)}"
                )
                safe_send(GROUP_ID, msg)
            else:
                print("[WARN] Could not top-up user balance for", email)
        else:
            safe_execute(lambda: supabase.table("transactions")
                .update({"status": "Unverified"})
                .eq("id", record_id)
                .execute()
            )
            safe_send(GROUP_ID, format_unverified_tx_message(tx))
    except Exception as e:
        print("[ERROR] handle_transaction:", e)
        traceback.print_exc()


def check_new_transactions_loop():
    """Continuously check for new pending transactions."""
    print("[INFO] Transaction monitor started.")
    while True:
        try:
            res = safe_execute(lambda: supabase.table("transactions")
                .select("*")
                .eq("status", "Pending")
                .order("id")
                .execute()
            )

            for tx in res.data or []:
                txid = tx.get("id")
                print(f"[INFO] Checking transaction {txid}")
                safe_execute(lambda: supabase.table("transactions")
                    .update({"status": "Checking"})
                    .eq("id", txid)
                    .execute()
                )
                handle_transaction(tx)

        except Exception as e:
            print("[ERROR] Transaction loop error:", e)
            traceback.print_exc()
            time.sleep(2)

        time.sleep(5)

# ===============================
# ADMIN COMMAND HANDLERS
# ===============================

@bot.message_handler(commands=['Yes'])
def approve_tx_cmd(message):
    """Approve a transaction manually."""
    try:
        parts = message.text.split()
        if len(parts) < 2:
            return bot.reply_to(message, "⚠️ Usage: /Yes <transaction_id>")
        tx_id = int(parts[1])

        tx_res = safe_execute(lambda: supabase.table("transactions")
            .select("*")
            .eq("id", tx_id)
            .execute()
        )

        if not tx_res.data:
            return bot.reply_to(message, "❌ Transaction not found.")
        tx = tx_res.data[0]

        ok = update_user_balance(tx.get("email"), tx.get("amount"))
        if ok:
            safe_execute(lambda: supabase.table("transactions")
                .update({"status": "Accepted"})
                .eq("id", tx_id)
                .execute()
            )

            safe_execute(lambda: supabase.table("VerifyPayment")
                .update({"status": "used"})
                .eq("transaction_id", tx.get("transaction_id"))
                .execute()
            )

            msg = f"✅ Transaction {tx_id} approved by admin"
            safe_send(GROUP_ID, escape_md2(msg))
        else:
            bot.reply_to(message, "⚠️ Could not update balance.")
    except Exception as e:
        print("[ERROR] approve_tx_cmd:", e)
        traceback.print_exc()
        bot.reply_to(message, f"⚠️ Error: {e}")


@bot.message_handler(commands=['No'])
def reject_tx_cmd(message):
    """Reject a transaction manually."""
    try:
        parts = message.text.split()
        if len(parts) < 2:
            return bot.reply_to(message, "⚠️ Usage: /No <transaction_id>")
        tx_id = int(parts[1])

        safe_execute(lambda: supabase.table("transactions")
            .update({"status": "Failed"})
            .eq("id", tx_id)
            .execute()
        )

        msg = f"❌ Transaction {tx_id} rejected by admin"
        safe_send(GROUP_ID, escape_md2(msg))
    except Exception as e:
        print("[ERROR] reject_tx_cmd:", e)
        traceback.print_exc()
        bot.reply_to(message, f"⚠️ Error: {e}")


@bot.message_handler(commands=['Use'])
def use_verifypayment_cmd(message):
    """Manually mark a VerifyPayment record as used."""
    try:
        parts = message.text.split()
        if len(parts) < 2:
            return bot.reply_to(message, "⚠️ Usage: /Use <transaction_id>")
        txid = parts[1]

        vp_res = safe_execute(lambda: supabase.table("VerifyPayment")
            .select("*")
            .eq("transaction_id", txid)
            .eq("status", "unused")
            .execute()
        )

        if not vp_res.data:
            return bot.reply_to(message, f"⚠️ No unused VerifyPayment found for Transaction ID: {txid}")

        vp_id = vp_res.data[0]["id"]

        safe_execute(lambda: supabase.table("VerifyPayment")
            .update({"status": "used"})
            .eq("id", vp_id)
            .execute()
        )

        msg = f"✅ VerifyPayment Transaction {txid} marked as USED by admin"
        safe_send(GROUP_ID, escape_md2(msg))

    except Exception as e:
        print("[ERROR] use_verifypayment_cmd:", e)
        traceback.print_exc()
        bot.reply_to(message, f"⚠️ Error: {e}")


# ---------------------------

# ---------------------------
# WEBSITE ORDERS + SMMGEN
# ---------------------------
USD_TO_MMK = 4500  # MMK conversion rate

def safe_send(chat_id, text, parse_mode=None):
    try:
        bot.send_message(chat_id, text, parse_mode=parse_mode)
    except Exception as e:
        print("safe_send error:", e)


def send_to_smmgen(order):
    """Send order to SMMGEN API and handle response/errors safely"""
    payload = {
        "key": SMMGEN_API_KEY,
        "action": "add",
        "service": order.get("supplier_service_id"),
        "link": order.get("link"),
        "quantity": order.get("quantity"),
    }

    if order.get("comments"):
        payload["comments"] = ",".join(order["comments"])

    try:
        r = safe_request("POST", SMMGEN_URL, data=payload, timeout=20)
        data = r.json()
    except Exception as e:
        print("send_to_smmgen request error:", e)

        # Mark order as canceled + default supplier_order_id
        safe_execute(
            lambda: supabase.table("WebsiteOrders")
            .update({
                "status": "Canceled",
                "supplier_order_id": "123456"
            })
            .eq("id", order["id"])
            .execute()
        )

        # Adjust service quantity
        try:
            adjust_service_qty_on_status_change(order, order.get("status"), "Canceled")
        except Exception as err:
            print("adjust_service_qty_on_status_change error:", err)

        # Notify supplier group
        safe_send(
            SUPPLIER_GROUP_ID,
            f"❌ SMMGEN API Request Failed\n"
            f"ID: {order.get('id')}\n"
            f"Email: {order.get('email')}\n"
            f"Error: {str(e)}",
            parse_mode="HTML"
        )

        return {"success": False, "error": str(e)}

    # ✅ Handle valid JSON response
    if isinstance(data, dict) and "order" in data:
        return {"success": True, "order_id": data["order"]}

    else:
        print("send_to_smmgen response error:", data)

        # Mark order as canceled + default supplier_order_id
        safe_execute(
            lambda: supabase.table("WebsiteOrders")
            .update({
                "status": "Canceled",
                "supplier_order_id": "123456"
            })
            .eq("id", order["id"])
            .execute()
        )

        # Adjust quantity
        try:
            adjust_service_qty_on_status_change(order, order.get("status"), "Canceled")
        except Exception as err:
            print("adjust_service_qty_on_status_change error:", err)

        # Notify supplier group
        safe_send(
            SUPPLIER_GROUP_ID,
            f"⚠️ SMMGEN API Response Error\n"
            f"ID: {order.get('id')}\n"
            f"Email: {order.get('email')}\n"
            f"Response: {json.dumps(data, ensure_ascii=False)}",
            parse_mode="HTML"
        )

        return {"success": False, "error": data}


def check_new_orders_loop():
    while True:
        try:
            res = safe_execute(
                lambda: supabase.table("WebsiteOrders")
                .select("*")
                .eq("status", "Pending")
                .execute()
            )
            orders = res.data or []

            for o in orders:
                status = (o.get("status") or "").lower()
                supplier_order_id = o.get("supplier_order_id")
                supplier_name = (o.get("supplier_name") or "").lower()

                if status in ["refunded", "canceled"]:
                    continue

                # ❌ supplier_order_id ရှိပြီးသားဆိုရင် SKIP (SMMGEN case only)
                # ✅ အခုက 0 ဖြစ်နေတာကို မဖြစ်စေဖို့ ပြင်ထား
                if supplier_name == "smmgen" and supplier_order_id not in [None, "", 0, "0"]:
                    continue

                # ✅ smmgen orders
                if supplier_name == "smmgen":
                    result = send_to_smmgen(o)
                    if result.get("success"):
                        safe_execute(lambda: supabase.table("WebsiteOrders")
                            .update({
                                "status": "Processing",
                                "supplier_order_id": str(result["order_id"])
                            })
                            .eq("id", o["id"])
                            .execute()
                        )
                        msg = (
                            f"🚀 New Order Sent to SMMGEN\n\n"
                            f"🆔 {o.get('id')}\n"
                            f"📦 Service: {o.get('service')}\n"
                            f"🔢 Quantity: {o.get('quantity')}\n"
                            f"🔗 Link: {o.get('link')}\n"
                            f"💰 Sell Charge (USD): {o.get('sell_charge')}\n"
                            f"💵 Sell Charge (MMK): {o.get('sell_charge') * USD_TO_MMK:,.0f}\n"
                            f"📧 Email: {o.get('email')}\n"
                            f"🧾 Supplier Order ID: {result['order_id']}\n"
                            f"✅ Status: Processing"
                        )
                        safe_send(SUPPLIER_GROUP_ID, msg, parse_mode="HTML")

                # ✅ K2BOOST orders
                elif supplier_name == "k2boost":
                    msg = (
                        f"⚡️ New Order to K2BOOST\n\n"
                        f"🆔 {o.get('id')}\n"
                        f"📧 Email: {o.get('email')}\n"
                        f"📦 Service: {o.get('service')}\n"
                        f"🔢 Quantity: {o.get('quantity')}\n"
                        f"🔗 Link: {o.get('link')}\n"
                        f"📆 Day: {o.get('day')}\n"
                        f"⏳ Remain: {o.get('remain')}\n"
                        f"💰 Sell Charge (USD): {o.get('sell_charge')}\n"
                        f"💵 Sell Charge (MMK): {o.get('sell_charge') * USD_TO_MMK:,.0f}\n"
                        f"🏷 Supplier: {o.get('supplier_name')}\n"
                        f"🕒 Created: {o.get('created_at')}\n"
                        f"💬 Used Type: {o.get('UsedType')}"
                    )
                    safe_send(K2BOOST_GROUP_ID, msg, parse_mode="HTML")

                    # Update order status
                    safe_execute(
                        lambda: supabase.table("WebsiteOrders")
                        .update({"status": "Processing"})
                        .eq("id", o["id"])
                        .execute()
                    )

        except Exception as e:
            print("check_new_orders_loop error:", e)
            traceback.print_exc()
            time.sleep(3)


@bot.message_handler(commands=['D'])
def admin_mark_completed(message):
    try:
        parts = message.text.split()
        if len(parts) < 2:
            return bot.reply_to(message, "Usage: /D <OrderID>")
        order_id = int(parts[1])
        cur = supabase.table("WebsiteOrders").select("*").eq("id", order_id).execute()
        if not cur.data:
            return bot.reply_to(message, "Order not found.")
        order = cur.data[0]
        old_status = order.get("status")

        supabase.table("WebsiteOrders").update({
            "status": "Completed",
            "completed_at": datetime.utcnow().isoformat()
        }).eq("id", order_id).execute()

        bot.reply_to(message, f"✅ Order {order_id} marked as Completed")

        try:
            adjust_service_qty_on_status_change(order, old_status, "Completed")
        except Exception as e:
            print("adjust_service_qty_on_status_change error:", e)
    except Exception as e:
        bot.reply_to(message, f"⚠️ Error: {e}")


@bot.message_handler(commands=['F'])
def admin_mark_failed(message):
    try:
        parts = message.text.split()
        if len(parts) < 2:
            return bot.reply_to(message, "Usage: /F <OrderID>")
        order_id = int(parts[1])
        cur = supabase.table("WebsiteOrders").select("*").eq("id", order_id).execute()
        if not cur.data:
            return bot.reply_to(message, "Order not found.")
        order = cur.data[0]
        old_status = order.get("status")

        supabase.table("WebsiteOrders").update({"status": "Canceled"}).eq("id", order_id).execute()
        bot.reply_to(message, f"❌ Order {order_id} marked as Canceled")

        try:
            adjust_service_qty_on_status_change(order, old_status, "Canceled")
        except Exception as e:
            print("adjust_service_qty_on_status_change error:", e)
    except Exception as e:
        bot.reply_to(message, f"⚠️ Error: {e}")



def find_service_for_order(order):
    try:
        svc_name = order.get("service")
        if svc_name:
            r = supabase.table("services").select("*").eq("service_name", svc_name).execute()
            if r.data:
                return r.data[0]
            rows = supabase.table("services").select("*").ilike("service_name", f"%{svc_name}%").limit(1).execute()
            if rows.data:
                return rows.data[0]
    except Exception as e:
        print("find_service_for_order error:", e)
    return None

def adjust_service_qty_on_status_change(order, old_status, new_status):
    try:
        old = (old_status or "").lower()
        new = (new_status or "").lower()
        qty = int(order.get("quantity") or 0)
        remain = int(order.get("remain") or 0) if order.get("remain") is not None else 0
        sell_price = float(order.get("sell_charge") or order.get("price") or 0)
        email = order.get("email")
        service_name = order.get("service")

        svc = find_service_for_order(order)
        if not svc:
            print("Service not found for order", order.get("id"))
            return
        svc_id = svc.get("id")

        def notify_supplier(title, refund_amount=0, spend_amount=0, done_qty=0):
            msg = (
                f"📦 {title}\n"
                f"🧾 Order ID: {order.get('id')}\n"
                f"🧩 Service: {service_name}\n"
                f"👤 User: {email}\n"
                f"📊 Quantity: {qty}\n"
                f"⏳ Remain: {remain}\n"
                f"✅ Done Qty: {done_qty}\n"
                f"💰 Amount: ${sell_price:.4f}\n"
                f"💸 Refund: ${refund_amount:.4f}\n"
                f"📈 Spend Added: ${spend_amount:.4f}\n"
                f"🔄 New Status: {new.capitalize()}\n"
                f"🕒 Time: {datetime.now(ZoneInfo('Asia/Yangon')).strftime('%Y-%m-%d %H:%M:%S')}"
            )
            safe_send(SUPPLIER_GROUP_ID, msg)


        def handle_referral_and_bonus(amount, add=True):
            user_data = supabase.table("users").select("ref_owner_id", "total_spend").eq("email", email).execute().data
            if not user_data:
                return
            user_info = user_data[0]
            ref_owner = user_info.get("ref_owner_id")
            if ref_owner:
                delta = amount * 0.04
                if not add:
                    delta = -delta
                current_withdraw = supabase.table("users").select("withdrawable_balance").eq("id", ref_owner).execute().data[0].get("withdrawable_balance") or 0
                supabase.table("users").update({"withdrawable_balance": current_withdraw + delta}).eq("id", ref_owner).execute()
                safe_send(GROUP_ID, f"💰 Referral Owner reward {'added' if add else 'deducted'}: ${delta:.4f} for ref_owner_id {ref_owner}")
            total_spend = float(user_info.get("total_spend") or 0)
            if total_spend > 10:
                bonus = amount * 0.01
                if not add:
                    bonus = -bonus
                update_user_balance(email, bonus)
                safe_send(GROUP_ID, f"🎁 User bonus {'added' if add else 'deducted'}: ${bonus:.4f} for {email}")

        if new == "completed" and old != "completed":
            cur_qty = int(svc.get("total_sold_qty") or 0)
            supabase.table("services").update({"total_sold_qty": cur_qty + qty}).eq("id", svc_id).execute()
            if email and sell_price:
                user = supabase.table("users").select("total_spend").eq("email", email).execute().data
                if user:
                    total_spend = float(user[0].get("total_spend") or 0) + sell_price
                    supabase.table("users").update({"total_spend": total_spend}).eq("email", email).execute()
            handle_referral_and_bonus(sell_price, add=True)
            notify_supplier("✅ Completed Order", refund_amount=0, spend_amount=sell_price, done_qty=qty)

        elif old == "completed" and new in ("partial", "canceled", "cancelled"):
            cur_qty = int(svc.get("total_sold_qty") or 0)
            supabase.table("services").update({"total_sold_qty": max(0, cur_qty - qty)}).eq("id", svc_id).execute()
            if email and qty and sell_price:
                refund_amount = (remain / qty) * sell_price if remain else sell_price
                user = supabase.table("users").select("total_spend").eq("email", email).execute().data
                if user:
                    total_spend = float(user[0].get("total_spend") or 0) - refund_amount
                    supabase.table("users").update({"total_spend": max(0, total_spend)}).eq("email", email).execute()
                update_user_balance(email, refund_amount)
                supabase.table("WebsiteOrders").update({"refund_amount": refund_amount, "status": "Refunded"}).eq("id", order.get("id")).execute()
                handle_referral_and_bonus(refund_amount, add=False)
                notify_supplier("♻️ Completed → Refunded", refund_amount=refund_amount, done_qty=0)
                safe_send(GROUP_ID, f"🔁 Refunded ${refund_amount:.4f} to {email} for order {order.get('id')} (remain {remain})", )

        elif new in ("partial", "canceled", "cancelled") and old not in ("completed", "partial", "canceled", "cancelled"):
            done_qty = max(0, qty - remain)
            cur_qty = int(svc.get("total_sold_qty") or 0)
            supabase.table("services").update({"total_sold_qty": cur_qty + done_qty}).eq("id", svc_id).execute()
            if qty > 0 and sell_price > 0:
                refund_amount = (sell_price / qty) * remain
                spend_amount = sell_price - refund_amount
                user = supabase.table("users").select("total_spend").eq("email", email).execute().data
                if user:
                    total_spend = float(user[0].get("total_spend") or 0) + spend_amount
                    supabase.table("users").update({"total_spend": total_spend}).eq("email", email).execute()
                update_user_balance(email, refund_amount)
                supabase.table("WebsiteOrders").update({"refund_amount": refund_amount, "status": "Refunded"}).eq("id", order.get("id")).execute()
                notify_supplier("💸 Partial/Canceled Order", refund_amount=refund_amount, spend_amount=spend_amount, done_qty=done_qty)
                safe_send(GROUP_ID, f"💸 {email} refunded ${refund_amount:.4f} for {service_name} (remain {remain})")
    except Exception as e:
        print("adjust_service_qty_on_status_change error:", e)
        traceback.print_exc()

def smmgen_status_loop():
    while True:
        try:
            rows = supabase.table("WebsiteOrders").select("*").eq("supplier_name","smmgen").not_.is_("supplier_order_id", None).neq("status", "Completed").execute().data or []
            for r in rows:
                oid = r.get("supplier_order_id")
                if not oid:
                    continue
                payload = {"key": SMMGEN_API_KEY, "action": "status", "orders": str(oid)}
                try:
                    resp = requests.post(SMMGEN_URL, data=payload, timeout=25).json()
                except Exception as e:
                    print("SMMGEN status request error:", e)
                    continue
                info = resp.get(str(oid)) or resp.get(oid) or resp
                if not info:
                    continue
                new_status = info.get("status")
                updates = {}
                if "remains" in info:
                    try: updates["remain"] = int(float(info["remains"]))
                    except: pass
                if "start_count" in info:
                    try: updates["start_count"] = int(float(info["start_count"]))
                    except: pass
                if "charge" in info:
                    try: updates["buy_charge"] = float(info["charge"])
                    except: pass
                if new_status:
                    updates["status"] = new_status
                if updates:
                    cur = supabase.table("WebsiteOrders").select("*").eq("supplier_order_id", str(oid)).execute()
                    old_order = cur.data[0] if cur and cur.data else {}
                    old_status = old_order.get("status", "")
                    supabase.table("WebsiteOrders").update(updates).eq("supplier_order_id", str(oid)).execute()
                    if new_status and old_status.lower() != new_status.lower():
                        adjust_service_qty_on_status_change(old_order, old_status, new_status)
                        msg = f"✅ Order #{oid} Status Changed\n🕒 Old: {old_status}\n🚀 New: {new_status}"
                        bot.send_message(SUPPLIER_GROUP_ID, msg)
        except Exception as e:
            print("smmgen_status_loop error:", e)
        time.sleep(60)

# ---------------------------
# PROFIT CALCULATION


def calculate_profit():
    try:
        # Fetch services with sold quantities
        services_res = safe_execute(lambda: supabase.table("services").select("*").gt("total_sold_qty", 0).execute())
        services = services_res.data or []
        if not services:
            safe_send(REPORT_GROUP_ID, "📊 No sold services found today.", parse_mode="HTML")
            return

        total_profit_usd = 0
        profit_rows = []
        service_lines = []

        # Calculate per service profit
        for idx, s in enumerate(services, start=1):
            service_name = (s.get("service_name", "Unknown"))
            sell_price = float(s.get("sell_price") or 0)
            buy_price = float(s.get("buy_price") or 0)
            qty = int(s.get("total_sold_qty") or 0)
            per_qty = int(s.get("per_quantity") or 1000)

            # ✅ Corrected profit formula (per 1000 or per_quantity base)
            profit_usd = ((sell_price - buy_price) / per_qty) * qty
            profit_mmk = profit_usd * USD_TO_MMK
            total_profit_usd += profit_usd

            profit_rows.append({
                "Service Name": service_name,
                "Quantity": qty,
                "Buy Price ($)": buy_price,
                "Sell Price ($)": sell_price,
                "Profit (USD)": round(profit_usd, 2),
                "Profit (MMK)": round(profit_mmk, 0)
            })

            service_lines.append(
                f"{idx}. {service_name}\n"
                f"   • Qty: {qty}\n"
                f"   • Buy: ${buy_price:.3f} | Sell: ${sell_price:.3f} (per {per_qty})\n"
                f"   • Profit: ${profit_usd:.2f} ({profit_mmk:,.0f} Ks)"
            )

        # Totals
        total_profit_mmk = total_profit_usd * USD_TO_MMK
        users_res = safe_execute(lambda: supabase.table("users").select("balance_usd").execute())
        users = users_res.data or []
        total_balance_usd = sum(float(u.get("balance_usd") or 0) for u in users)
        total_balance_mmk = total_balance_usd * USD_TO_MMK

        # Save Excel report
        df = pd.DataFrame(profit_rows)
        df.loc[len(df.index)] = ["TOTAL", "", "", "", round(total_profit_usd, 2), round(total_profit_mmk, 0)]
        report_filename = f"./DailyProfitReport_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        df.to_excel(report_filename, index=False)

        # Summary text
        service_report = "\n\n".join(service_lines)
        summary_text = (
            "📊 *K2 Daily Profit Report*\n\n"
            f"💰 *Total Profit:*\n"
            f"- USD: ${total_profit_usd:.2f}\n"
            f"- MMK: {total_profit_mmk:,.0f} Ks\n\n"
            f"👥 *User Balances:*\n"
            f"- USD: ${total_balance_usd:.2f}\n"
            f"- MMK: {total_balance_mmk:,.0f} Ks\n\n"
            "━━━━━━━━━━━━━━━\n"
            "📦 *Service-wise Profits*\n\n"
            f"{service_report}\n"
            "━━━━━━━━━━━━━━━\n"
            f"🕒 Report Time: {datetime.now().strftime('%I:%M %p, %d-%b-%Y')}\n"
            "✅ Total sold quantities reset to 0."
        )

        # Telegram message size guard (split long messages)
        parts = [summary_text[i:i + 3500] for i in range(0, len(summary_text), 3500)]
        for part in parts:
            safe_send(REPORT_GROUP_ID, part)

        # Send Excel file
        try:
            with open(report_filename, "rb") as doc:
                bot.send_document(REPORT_GROUP_ID, doc)
        except Exception as e:
            print("Failed to send report file:", e)

        # Reset totals
        for s in services:
            safe_execute(lambda sid=s["id"]: supabase.table("services").update({"total_sold_qty": 0}).eq("id", sid).execute())

    except Exception as e:
        print("calculate_profit error:", e)
        traceback.print_exc()
        safe_send(REPORT_GROUP_ID, f"⚠️ Profit calculation failed:\n{(str(e))}")


# Manual trigger command
@bot.message_handler(commands=["calculate", "Calculate"])
def manual_calculate(message):
    if message.chat.id == REPORT_GROUP_ID or is_admin_chat(message.chat.id):
        threading.Thread(target=calculate_profit, daemon=True).start()
    else:
        bot.reply_to(message, "❌ This command is only for the report group or admins.")

# ---------------------------
# SMMGEN RATE CHECK
# ---------------------------
def check_smmgen_service_rates():
    try:
        res = supabase.table("services").select("*").eq("source", "smmgen").execute()
        services_rows = res.data or []
        payload = {"key": SMMGEN_API_KEY, "action": "services"}
        r = safe_request("POST", SMMGEN_URL, data=payload, timeout=15)
        smmgen_services = r.json()
        for row in services_rows:
            service_id = row.get("service_id")
            row_buy_price = float(row.get("buy_price", 0))
            api_service = next((s for s in smmgen_services if str(s.get("service")) == str(service_id)), None)
            if api_service:
                api_rate = float(api_service.get("rate", 0))
                if row_buy_price != api_rate:
                    msg = (
                        "⚠️ <b>SMMGEN Rate Mismatch</b>\n\n"
                        f"🆔 Service Row ID: {row.get('id')}\n"
                        f"📦 Service Name: {row.get('service')}\n"
                        f"💰 Local Buy Price: {row_buy_price}\n"
                        f"💵 SMMGEN API Rate: {api_rate}\n\n"
                        "✅ Updating local buy_price to API rate..."
                    )
                    safe_send(GROUP_ID, msg)
                    safe_execute(lambda: supabase.table("services").update({"buy_price": api_rate}).eq("id", row.get("id")).execute())
    except Exception as e:
        print("check_smmgen_service_rates error:", e)
        traceback.print_exc()



if __name__ == "__main__":
    threading.Thread(target=poll_transactions, daemon=True).start()
    threading.Thread(target=poll_affiliate, daemon=True).start()
    threading.Thread(target=poll_supportbox, daemon=True).start()
    threading.Thread(target=check_new_orders_loop, daemon=True).start()
    threading.Thread(target=smmgen_status_loop, daemon=True).start()
     

    # Run Flask server on background
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000))), daemon=True).start()

    # Start Telegram bot
    bot.polling(none_stop=True)


# ---------------------------
# FLASK ROUTES (web service triggers)
# ---------------------------

if __name__ == "__main__":
    try:
        scheduler.add_job(calculate_profit, 'cron', hour=8, minute=0)              # 08:00 UTC == 14:30 Yangon (approx)
        scheduler.add_job(check_smmgen_service_rates, 'cron', hour=8, minute=30)   # run rates check daily ~14:30 Yangon
        scheduler.start()
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
    except (KeyboardInterrupt, SystemExit):
        pass


