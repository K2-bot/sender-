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
ADMIN_CHAT_IDS = [int(x) for x in os.getenv("ADMIN_CHAT_IDS"," ").split(",") if x.strip().isdigit()]

if not (SUPABASE_URL and SUPABASE_KEY and BOT_TOKEN):
    raise RuntimeError("Please provide SUPABASE_URL, SUPABASE_KEY and TELEGRAM_TOKEN in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
bot = telebot.TeleBot(BOT_TOKEN)  # don't set global parse_mode

# Threading / locks
db_lock = threading.Lock()
threads_started_lock = threading.Lock()
threads_started = False

app = Flask(__name__)
scheduler = BackgroundScheduler(timezone="UTC")

# ---------------------------
# UTIL / HELPERS
# ---------------------------
_md_v2_chars = '_*[]()~`>#+-=|{}.!'

def now_yangon():
    return datetime.now(TZ)

def iso_now():
    return datetime.utcnow().isoformat()

def escape_md2(text: str) -> str:
    """Escape text for Telegram MarkdownV2 by prefixing reserved chars with backslash."""
    if text is None:
        return ""
    return ''.join(f'\\{c}' if c in _md_v2_chars else c for c in str(text))


def escape_html(text: str) -> str:
    """Minimal HTML escaping for Telegram HTML parse_mode."""
    if text is None:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def try_parse_iso(s):
    try:
        return dateutil.parser.isoparse(s) if s else None
    except Exception:
        return None


def is_transient_exception(e: Exception) -> bool:
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
            if is_transient_exception(e) and attempt + 1 < retries:
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


def safe_send(chat_id, text, parse_mode="HTML", disable_web_page_preview=True):
    """Send message safely: automatically escape text according to parse_mode and retry on transient failures."""
    def _send():
        if parse_mode in ("MarkdownV2", "Markdown"):
            # user requested markdown style
            if parse_mode == "MarkdownV2":
                payload = escape_md2(text)
            else:
                # Basic Markdown (not MarkdownV2) - escape backticks to be safe
                payload = str(text).replace('`', '\\`')
        else:
            payload = escape_html(text)
        bot.send_message(chat_id, payload, disable_web_page_preview=disable_web_page_preview, parse_mode=parse_mode)

    try:
        safe_execute(_send, retries=4, base_delay=0.5)
    except Exception as e:
        print("Telegram send error:", e)


# ----- helpers for admin checks -----

def is_admin_chat(chat_id: int) -> bool:
    if chat_id in ADMIN_CHAT_IDS:
        return True
    # accept configured group ids as admin
    if chat_id in (GROUP_ID, REPORT_GROUP_ID, SUPPLIER_GROUP_ID, K2BOOST_GROUP_ID, NEWS_GROUP_ID):
        return True
    return False


# ---------------------------
# DATABASE / ACCOUNT HELPERS
# ---------------------------

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
last_checked_support = None

def escape_md2(text):
    # Escape all MarkdownV2 special characters
    escape_chars = r"_*[]()~`>#+-=|{}.!\\"
    return ''.join(f'\\{c}' if c in escape_chars else c for c in text)

def send_news_to_group(row):
    id_ = escape_md2(str(row.get("id") or ""))
    email_ = escape_md2(str(row.get("email") or ""))
    subject = escape_md2(str(row.get("subject") or ""))
    order_id = escape_md2(str(row.get("order_id") or ""))
    message = escape_md2(str(row.get("message") or ""))

    # Escape dashes in your template text
    msg = (
        "📢 New Support Ticket \n"
        f"📦 ID - {id_}\n"
        f"📧 Email - {email_}\n"
        f"📝 Subject - {subject}\n"
        f"🆔 Order ID - {order_id}\n\n"
        "💬 Message:\n"
        f"{message}\n\n"
        "Commands:\n"
        f"/Answer {id_} [reply message]\n"
        f"/Close {id_}"
    )
    safe_send(NEWS_GROUP_ID, msg, parse_mode="MarkdownV2")


def update_support_status(id, status, reply_message=None):
    updates = {"status": status}
    if reply_message:
        updates["reply_text"] = reply_message  # match your table
        updates["replied_at"] = datetime.utcnow().isoformat()
    try:
        safe_execute(lambda: supabase.table("SupportBox")
                     .update(updates)
                     .eq("id", id)
                     .execute())
    except Exception as e:
        print("Support update error:", e)



def poll_supportbox_loop():
    global last_checked_support
    while True:
        try:
            res = safe_execute(lambda: supabase.table("SupportBox").select("*").order("created_at").execute())
            rows = res.data or []
            for row in rows:
                created = try_parse_iso(row.get("created_at")) or datetime.utcnow()
                if (not last_checked_support or created > last_checked_support) and row.get("status") == "Pending":
                    send_news_to_group(row)
                    last_checked_support = created
        except Exception as e:
            print("SupportBox polling error:", e)
            traceback.print_exc()
        time.sleep(5)


# Telegram handlers for support
@bot.message_handler(commands=['Answer'])
def handle_answer(message):
    try:
        parts = message.text.split(maxsplit=2)
        if len(parts) < 3:
            return bot.reply_to(message, "❌ Usage: /Answer ID Reply Message")
        id = int(parts[1])
        reply_text = parts[2]
        update_support_status(id, "Answered", reply_text)
        bot.reply_to(message, f"✅ Support ID {id} marked as Answered.")
    except Exception as e:
        bot.reply_to(message, f"⚠️ Error: {e}")

@bot.message_handler(commands=['Close'])
def handle_close(message):
    try:
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            return bot.reply_to(message, "❌ Usage: /Close ID")
        id = int(parts[1])
        update_support_status(id, "Closed")
        bot.reply_to(message, f"✅ Support ID {id} Closed.")
    except Exception as e:
        bot.reply_to(message, f"⚠️ Error: {e}")


# ---------------------------
# AFFILIATE
# ---------------------------

def handle_affiliate(row):
    email = row.get("email")
    method = row.get("method")
    amount = float(row.get("amount") or 0)
    aff_id = row.get("id")
    phone_id = row.get("phone_id", "N/A")
    name = row.get("name", "N/A")

    if method and method.lower() == "topup":
        ok = update_user_balance(email, amount)
        if ok:
            safe_execute(lambda: supabase.table("affiliate").update({"status": "Accepted"}).eq("id", aff_id).execute())
            msg = (
                "💰 Affiliate Topup \n\n"
                f"🆔 ID = {escape_html(str(aff_id))}\n"
                f"📧 Email = {escape_html(email)}\n"
                f"💳 Method = {escape_html(method)}\n"
                f"💵 Amount USD = {escape_html(str(amount))}\n"
                f"🇲🇲 Amount MMK = {escape_html(f'{amount * USD_TO_MMK:,.0f}') }"
            )
            safe_send(GROUP_ID, msg, parse_mode="HTML")
        return

    msg = (
        "🆕 New Affiliate Request \n\n"
        f"🆔 ID = {escape_html(str(aff_id))}\n"
        f"📧 Email = {escape_html(str(email))}\n"
        f"💰 Amount = {escape_html(str(amount))}\n"
        f"💳 Method = {escape_html(str(method))}\n"
        f"📱 Phone ID = {escape_html(str(phone_id))}\n"
        f"👤 Name = {escape_html(str(name))}\n\n"
        "🛠 <b>Admin Actions:</b>\n"
        f"/Accept {escape_html(str(aff_id))}\n"
        f"/Failed {escape_html(str(aff_id))}"
    )
    safe_send(GROUP_ID, msg, parse_mode="HTML")


def check_affiliate_rows_loop():
    last_id = 0
    while True:
        try:
            res = safe_execute(lambda: supabase.table("affiliate").select("*").eq("status", "Pending").gt("id", last_id).order("id").execute())
            for row in res.data or []:
                last_id = row["id"]
                handle_affiliate(row)
        except Exception as e:
            print("Affiliate error:", e)
            traceback.print_exc()
            time.sleep(2)
        time.sleep(5)


@bot.message_handler(commands=['Accept'])
def accept_aff_cmd(message):
    try:
        if not is_admin_chat(message.chat.id):
            return bot.reply_to(message, "❌ You are not authorized to use this command.")
        aff_id = int(message.text.split()[1])
        row_res = safe_execute(lambda: supabase.table("affiliate").select("*").eq("id", aff_id).execute())
        row = row_res.data if row_res else None
        if not row:
            return bot.reply_to(message, "Affiliate not found.")
        row = row[0]
        ok = update_user_balance(row.get("email"), float(row.get("amount") or 0))
        if ok:
            safe_execute(lambda: supabase.table("affiliate").update({"status":"Accepted"}).eq("id", aff_id).execute())
            safe_send(GROUP_ID, f"✅ Affiliate #{aff_id} Accepted", parse_mode="HTML")
        else:
            bot.reply_to(message, "⚠️ Could not update balance.")
    except Exception as e:
        bot.reply_to(message, f"⚠️ Error: {e}")


@bot.message_handler(commands=['Failed'])
def failed_aff_cmd(message):
    try:
        if not is_admin_chat(message.chat.id):
            return bot.reply_to(message, "❌ You are not authorized to use this command.")
        aff_id = int(message.text.split()[1])
        safe_execute(lambda: supabase.table("affiliate").update({"status":"Failed"}).eq("id", aff_id).execute())
        safe_send(GROUP_ID, f"❌ Affiliate #{aff_id} Failed", parse_mode="HTML")
    except Exception as e:
        bot.reply_to(message, f"⚠️ Error: {e}")


# ---------------------------
# TRANSACTIONS
# ---------------------------

def format_unverified_tx_message(tx):
    id_ = escape_md2(str(tx.get('id')))
    email = escape_md2(str(tx.get('email')))
    method = escape_md2(str(tx.get('method')))
    amount = escape_md2(str(tx.get('amount')))
    txid = escape_md2(str(tx.get('transaction_id')))
def format_unverified_tx_message(tx):
    return (
        "🆕 *New Unverified Transaction*\n\n"
        f"🆔 ID = {tx.get('id')}\n"
        f"📧 Email = {tx.get('email')}\n"
        f"💳 Method = {tx.get('method')}\n"
        f"💵 Amount USD = {tx.get('amount')}\n\n"
        f"🧾 Transaction ID = {tx.get('transaction_id')}\n\n"
        "🛠 *Admin Commands:*\n"
        f"/Yes {tx.get('id')}\n"
        f"/No {tx.get('id')}"
    )

def handle_transaction(tx):
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
                mmk = usd * 4500
                msg = (
                    "✅ Auto Top-up Completed\n"
                    f"👤 User = {email}\n"
                    f"💳 Method = {method}\n"
                    f"💰 Amount USD = {usd}\n"
                    f"🇲🇲 Amount MMK = {mmk:.0f}\n"
                    f"🧾 Transaction ID = {txid}"
                )
                safe_send(GROUP_ID, msg)
            else:
                print("Could not top-up user balance for", email)
        else:
            safe_execute(lambda: supabase.table("transactions")
                .update({"status": "Unverified"})
                .eq("id", record_id)
                .execute()
            )
            safe_send(GROUP_ID, format_unverified_tx_message(tx))
    except Exception as e:
        print("handle_transaction error:", e)
        traceback.print_exc()

def check_new_transactions_loop():
    while True:
        try:
            res = safe_execute(lambda: supabase.table("transactions").select("*").eq("status","Pending").order("id").execute())
            for tx in res.data or []:
                safe_execute(lambda: supabase.table("transactions").update({"status":"Checking"}).eq("id", tx["id"]).execute())
                handle_transaction(tx)
        except Exception as e:
            print("Transaction error:", e)
            traceback.print_exc()
            time.sleep(2)
        time.sleep(5)
        
@bot.message_handler(commands=['Yes'])
def approve_tx_cmd(message):
    try:
        tx_id = int(message.text.split()[1])
        tx = supabase.table("transactions").select("*").eq("id", tx_id).execute().data
        if not tx:
            return bot.reply_to(message, "Transaction not found.")
        tx = tx[0]
        ok = update_user_balance(tx.get("email"), tx.get("amount"))
        if ok:
            supabase.table("transactions").update({"status":"Accepted"}).eq("id", tx_id).execute()
            # mark verifypayment as used if matching transaction_id exists
            supabase.table("VerifyPayment").update({"status":"used"}).eq("transaction_id", tx.get("transaction_id")).execute()
            bot.send_message(GROUP_ID, f"✅ Transaction {tx_id} approved")
        else:
            bot.reply_to(message, "⚠️ Could not update balance.")
    except Exception as e:
        bot.reply_to(message, f"⚠️ Error: {e}")

@bot.message_handler(commands=['No'])
def reject_tx_cmd(message):
    try:
        tx_id = int(message.text.split()[1])
        supabase.table("transactions").update({"status":"Failed"}).eq("id", tx_id).execute()
        bot.send_message(GROUP_ID, f"❌ Transaction {tx_id} rejected")
    except Exception as e:
        bot.reply_to(message, f"⚠️ Error: {e}")
        # -------------------
# /Use Command Handler
# -------------------
@bot.message_handler(commands=['Use'])
def use_verifypayment_cmd(message):
    try:
        # /Use transaction_id
        parts = message.text.split()
        if len(parts) < 2:
            return bot.reply_to(message, "⚠️ Usage: /Use <transaction_id>")

        txid = parts[1]

        # Verify if transaction exists and unused
        vp_res = supabase.table("VerifyPayment")\
            .select("*")\
            .eq("transaction_id", txid)\
            .eq("status", "unused")\
            .execute()

        if not vp_res.data:
            return bot.reply_to(message, f"⚠️ No unused VerifyPayment found for Transaction ID: {txid}")

        vp_id = vp_res.data[0]["id"]

        # Update VerifyPayment to used
        supabase.table("VerifyPayment").update({"status":"used"}).eq("id", vp_id).execute()

        bot.send_message(GROUP_ID, f"✅ VerifyPayment Transaction {txid} marked as USED")

    except Exception as e:
        bot.reply_to(message, f"⚠️ Error: {e}")
# ---------------------------
# WEBSITE ORDERS + SMMGEN
# ---------------------------

def send_to_smmgen(order):
    payload = {
        "key": SMMGEN_API_KEY,
        "action": "add",
        "service": order.get("supplier_service_id"),
        "link": order.get("link"),
        "quantity": order.get("quantity")
    }
    if order.get("comments"):
        payload["comments"] = ",".join(order["comments"])
    try:
        r = safe_request("POST", SMMGEN_URL, data=payload, timeout=20)
        data = r.json()
    except Exception as e:
        print("send_to_smmgen request error:", e)
        return {"success": False, "error": str(e)}
    if isinstance(data, dict) and "order" in data:
        return {"success": True, "order_id": data["order"]}
    else:
        return {"success": False, "error": data}


def check_new_orders_loop():
    while True:
        try:
            res = safe_execute(lambda: supabase.table("WebsiteOrders").select("*").eq("status", "Pending").execute())
            orders = res.data or []
            for o in orders:
                if o.get("supplier_name") == "smmgen":
                    result = send_to_smmgen(o)
                    if result.get("success"):
                        safe_execute(lambda: supabase.table("WebsiteOrders").update({
                            "status": "Processing",
                            "supplier_order_id": str(result["order_id"])
                        }).eq("id", o["id"]).execute())
                        msg = (
                            "🚀 New Order to SMMGEN\n\n"
                            f"🆔 {escape_html(str(o.get('id')))}\n"
                            f"📦 Service: {escape_html(str(o.get('service')))}\n"
                            f"🔢 Quantity: {escape_html(str(o.get('quantity')))}\n"
                            f"🔗 Link: {escape_html(str(o.get('link')))}\n"
                            f"👤 Email: {escape_html(str(o.get('email')))}\n"
                            f"👤 Order Id: {escape_html(str(result['order_id']))}\n"
                            f"✅ Status: Processing\n"
                        )
                        safe_send(SUPPLIER_GROUP_ID, msg, parse_mode="HTML")
                elif o.get("supplier_name") == "k2boost":
                    msg = (
                        "⚡️ New Order to K2BOOST\n\n"
                        f"🆔 {escape_html(str(o.get('id')))}\n"
                        f"📧 Email = {escape_html(str(o.get('email')))}\n"
                        f"📦 Service: {escape_html(str(o.get('service')))}\n"
                        f"🔢 Quantity: {escape_html(str(o.get('quantity')))}\n"
                        f"🔗 Link: {escape_html(str(o.get('link')))}\n"
                        f"📆 Day: {escape_html(str(o.get('day')))}\n"
                        f"⏳ Remain: {escape_html(str(o.get('remain')))}\n"
                        f"💰 Sell Charge: {escape_html(str(o.get('sell_charge')))}\n"
                        f"🏷 Supplier: {escape_html(str(o.get('supplier_name')))}\n"
                        f"🕒 Created: {escape_html(str(o.get('created_at')))}\n"
                        f"💬 Used Type: {escape_html(str(o.get('UsedType')))}\n\n"
                        f"/D {escape_html(str(o.get('id')))}\n"
                        f"/F {escape_html(str(o.get('id')))}\n"
                    )
    
                    safe_send(K2BOOST_GROUP_ID, msg, parse_mode="HTML")
                    safe_execute(lambda: supabase.table("WebsiteOrders").update({"status": "Processing"}).eq("id", o["id"]).execute())
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


# ❌ Admin Command: Mark Failed / Canceled
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
                f"📦 {escape_html(title)}\n"
                f"🧾 Order ID: {escape_html(str(order.get('id')))}\n"
                f"🧩 Service: {escape_html(str(service_name))}\n"
                f"👤 User: {escape_html(str(email))}\n"
                f"📊 Quantity: {escape_html(str(qty))}\n"
                f"⏳ Remain: {escape_html(str(remain))}\n"
                f"✅ Done Qty: {escape_html(str(done_qty))}\n"
                f"💰 Amount: ${escape_html(f'{sell_price:.4f}')}\n"
                f"💸 Refund: ${escape_html(f'{refund_amount:.4f}')}\n"
                f"📈 Spend Added: ${escape_html(f'{spend_amount:.4f}')}\n"
                f"🔄 New Status: {escape_html(new.capitalize())}\n"
                f"🕒 Time: {escape_html(datetime.now(ZoneInfo('Asia/Yangon')).strftime('%Y-%m-%d %H:%M:%S'))}"
            )
            safe_send(SUPPLIER_GROUP_ID, msg, parse_mode="HTML")
            safe_send(GROUP_ID, msg, parse_mode="HTML")

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
                safe_send(GROUP_ID, f"💰 Referral Owner reward {'added' if add else 'deducted'}: ${delta:.4f} for ref_owner_id {escape_html(str(ref_owner))}", parse_mode="HTML")
            total_spend = float(user_info.get("total_spend") or 0)
            if total_spend > 10:
                bonus = amount * 0.01
                if not add:
                    bonus = -bonus
                update_user_balance(email, bonus)
                safe_send(GROUP_ID, f"🎁 User bonus {'added' if add else 'deducted'}: ${bonus:.4f} for {escape_html(str(email))}", parse_mode="HTML")

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
                safe_send(GROUP_ID, f"🔁 Refunded ${refund_amount:.4f} to {escape_html(str(email))} for order {escape_html(str(order.get('id')))} (remain {escape_html(str(remain))})", parse_mode="HTML")

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
                safe_send(GROUP_ID, f"💸 {escape_html(str(email))} refunded ${refund_amount:.4f} for {escape_html(str(service_name))} (remain {escape_html(str(remain))})", parse_mode="HTML")
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
                    resp = safe_request("POST", SMMGEN_URL, data=payload, timeout=25).json()
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
                        msg = f"✅ Order #{escape_html(str(oid))} Status Changed\n🕒 Old: {escape_html(str(old_status))}\n🚀 New: {escape_html(str(new_status))}"
                        safe_send(SUPPLIER_GROUP_ID, msg, parse_mode="HTML")
        except Exception as e:
            print("smmgen_status_loop error:", e)
            traceback.print_exc()
        time.sleep(60)


# ---------------------------
# PROFIT CALCULATION
# ---------------------------

def calculate_profit():
    try:
        services_res = safe_execute(lambda: supabase.table("services").select("*").gt("total_sold_qty", 0).execute())
        services = services_res.data or []
        if not services:
            safe_send(REPORT_GROUP_ID, "📊 No sold services found today.", parse_mode="HTML")
            return

        total_profit_usd = 0
        profit_rows = []
        service_lines = []

        for idx, s in enumerate(services, start=1):
            service_name = s.get("service_name", "Unknown")
            sell_price = float(s.get("sell_price") or 0)
            buy_price = float(s.get("buy_price") or 0)
            qty = int(s.get("total_sold_qty") or 0)
            profit_usd = (sell_price - buy_price) * qty
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
                f"   • Buy: ${buy_price:.3f} | Sell: ${sell_price:.3f}\n"
                f"   • Profit: ${profit_usd:.2f} ({profit_mmk:,.0f} Ks)"
            )

        total_profit_mmk = total_profit_usd * USD_TO_MMK
        users_res = safe_execute(lambda: supabase.table("users").select("balance_usd").execute())
        users = users_res.data or []
        total_balance_usd = sum(float(u.get("balance_usd") or 0) for u in users)
        total_balance_mmk = total_balance_usd * USD_TO_MMK

        df = pd.DataFrame(profit_rows)
        df.loc[len(df.index)] = ["TOTAL", "", "", "", round(total_profit_usd, 2), round(total_profit_mmk, 0)]
        report_filename = f"./DailyProfitReport_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        df.to_excel(report_filename, index=False)

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

        # Telegram message size guard: split into chunks and send as plain text (escaped for HTML)
        parts = [summary_text[i:i+3500] for i in range(0, len(summary_text), 3500)]
        for part in parts:
            safe_send(REPORT_GROUP_ID, part, parse_mode="HTML")

        try:
            with open(report_filename, "rb") as doc:
                bot.send_document(REPORT_GROUP_ID, doc)
        except Exception as e:
            print("Failed to send report file:", e)

        for s in services:
            safe_execute(lambda sid=s["id"]: supabase.table("services").update({"total_sold_qty": 0}).eq("id", sid).execute())

    except Exception as e:
        print("calculate_profit error:", e)
        traceback.print_exc()
        safe_send(REPORT_GROUP_ID, f"⚠️ Profit calculation failed:\n{escape_html(str(e))}", parse_mode="HTML")


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
        res = safe_execute(lambda: supabase.table("services").select("*").eq("source", "smmgen").execute())
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
                        "⚠️ SMMGEN Rate Mismatch\n\n"
                        f"Service Row ID: {escape_html(str(row.get('id')))}\n"
                        f"Service Name: {escape_html(str(row.get('service')))}\n"
                        f"Local Buy Price: {escape_html(str(row_buy_price))}\n"
                        f"SMMGEN API Rate: {escape_html(str(api_rate))}\n\n"
                        "Updating local buy_price to API rate..."
                    )
                    safe_send(GROUP_ID, msg, parse_mode="HTML")
                    safe_execute(lambda: supabase.table("services").update({"buy_price": api_rate}).eq("id", row.get("id")).execute())
    except Exception as e:
        print("check_smmgen_service_rates error:", e)
        traceback.print_exc()


# ---------------------------
# FLASK ROUTES (web service triggers)
# ---------------------------
@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "running", "time": now_yangon().isoformat()})


@app.route("/run_all", methods=["GET"])
def run_all_once():
    try:
        start_background_threads()
        return jsonify({"status": "started", "note": "background tasks triggered"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/trigger/supportbox", methods=["GET"])
def trigger_supportbox():
    threading.Thread(target=poll_supportbox_loop, daemon=True).start()
    return jsonify({"status": "supportbox triggered"}), 200


@app.route("/trigger/affiliates", methods=["GET"])
def trigger_affiliates():
    threading.Thread(target=check_affiliate_rows_loop, daemon=True).start()
    return jsonify({"status": "affiliates triggered"}), 200


@app.route("/trigger/transactions", methods=["GET"])
def trigger_transactions():
    threading.Thread(target=check_new_transactions_loop, daemon=True).start()
    return jsonify({"status": "transactions triggered"}), 200


@app.route("/trigger/orders", methods=["GET"])
def trigger_orders():
    threading.Thread(target=check_new_orders_loop, daemon=True).start()
    return jsonify({"status": "orders triggered"}), 200


@app.route("/trigger/smmgen-status", methods=["GET"])
def trigger_smmgen_status():
    threading.Thread(target=smmgen_status_loop, daemon=True).start()
    return jsonify({"status": "smmgen status triggered"}), 200


@app.route("/trigger/profit", methods=["GET"])
def trigger_profit():
    threading.Thread(target=calculate_profit, daemon=True).start()
    return jsonify({"status": "profit triggered"}), 200


# ---------------------------
# STARTUP
# ---------------------------

def start_bot_polling():
    try:
        bot.remove_webhook()
    except Exception:
        pass
    bot.infinity_polling()


def start_background_threads():
    global threads_started
    with threads_started_lock:
        if threads_started:
            return
        threads_started = True
        threading.Thread(target=poll_supportbox_loop, daemon=True).start()
        threading.Thread(target=check_affiliate_rows_loop, daemon=True).start()
        threading.Thread(target=check_new_transactions_loop, daemon=True).start()
        threading.Thread(target=check_new_orders_loop, daemon=True).start()
        threading.Thread(target=smmgen_status_loop, daemon=True).start()


if __name__ == "__main__":
    try:
        start_background_threads()
        threading.Thread(target=start_bot_polling, daemon=True).start()
        # schedule daily jobs (UTC)
        scheduler.add_job(calculate_profit, 'cron', hour=8, minute=0)              # 08:00 UTC
        scheduler.add_job(check_smmgen_service_rates, 'cron', hour=8, minute=30)
        scheduler.start()
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    try:
        start_background_threads()
        threading.Thread(target=start_bot_polling, daemon=True).start()
        # schedule daily jobs (UTC)
        scheduler.add_job(calculate_profit, 'cron', hour=8, minute=0)              # 08:00 UTC
        scheduler.add_job(check_smmgen_service_rates, 'cron', hour=8, minute=30)
        scheduler.start()
        app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
    except (KeyboardInterrupt, SystemExit):
        pass


