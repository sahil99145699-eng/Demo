import os
import random
import string
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import websocket
from flask import Flask, jsonify

# ==================== ACCOUNTS IMPORT ====================
try:
    from accounts import ACCOUNTS
    print(f"✅ accounts.py loaded! Total accounts: {len(ACCOUNTS)}")
except ImportError:
    print("❌ accounts.py not found! Please create accounts.py with ACCOUNTS list.")
    sys.exit(1)

# ==================== CONFIG ====================
IST = timezone(timedelta(hours=5, minutes=30))

RESTART_HOUR = int(os.environ.get("RESTART_HOUR", 4))
RESTART_MINUTE = int(os.environ.get("RESTART_MINUTE", 0))
RESTART_GAP = int(os.environ.get("RESTART_GAP", 15))
RECONNECT_DELAY = int(os.environ.get("RECONNECT_DELAY", 5))

BASE_ONLINE_FRAME_HEX = "0A29500118002205656E5F696E3A00480032001098EEE7D7FE330A0D696B78645F6F6E6C696E655F6442002A04180110001003"
WS_URL = "wss://i-875.olaparty.com/ikxd_cproxy"

# ==================== GLOBAL STATE ====================
active_bots = []
bots_lock = threading.Lock()
last_restart_time = None
restart_in_progress = False

# ==================== FLASK SERVER ====================
app = Flask(__name__)


@app.route("/")
def home():
    with bots_lock:
        total = len(active_bots)
        connected = sum(1 for b in active_bots if b.connected)
    return f"""
    <html>
        <head><title>OlaParty Online Only</title>
        <style>
            body {{ font-family: Arial; margin: 50px; background: #f0f0f0; }}
            .container {{ background: white; padding: 30px; border-radius: 10px; max-width: 650px; margin: auto; }}
            h1 {{ color: #4CAF50; }}
            .status {{ color: green; font-weight: bold; }}
            .btn {{ background:#f44336;color:white;padding:10px 18px;border-radius:6px;text-decoration:none; }}
        </style></head>
        <body>
            <div class="container">
                <h1>🟢 OlaParty Online-Only Mode</h1>
                <p>Status: <span class="status">✅ Online Only (No Room Join)</span></p>
                <p>Total Bots: <b>{total}</b> | Connected: <b>{connected}</b></p>
                <p>Next auto-restart: <b>{RESTART_HOUR:02d}:{RESTART_MINUTE:02d} IST (daily)</b></p>
                <p>
                    <a href="/status">📊 Status</a> &nbsp;|&nbsp;
                    <a class="btn" href="/restart">🔄 Restart Now</a>
                </p>
            </div>
        </body>
    </html>
    """


@app.route("/status")
def status():
    with bots_lock:
        total = len(active_bots)
        connected = sum(1 for b in active_bots if b.connected)
    return jsonify({
        "mode": "online_only",
        "status": "running",
        "total_bots": total,
        "connected_bots": connected,
        "restart_in_progress": restart_in_progress,
        "last_restart_time": last_restart_time.isoformat() if last_restart_time else None,
        "next_restart_ist": get_next_restart_time().isoformat(),
        "restart_hour_ist": RESTART_HOUR,
        "restart_minute_ist": RESTART_MINUTE,
        "timestamp": datetime.now(IST).isoformat(),
    })


@app.route("/health")
def health():
    return "OK", 200


@app.route("/restart")
def manual_restart():
    threading.Thread(target=restart_all_bots, args=("manual",), daemon=True).start()
    return jsonify({
        "ok": True,
        "message": "Restart triggered. Check /status in 30 seconds.",
        "triggered_at": datetime.now(IST).isoformat(),
    })


def keep_alive():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


# ==================== LOGGING & HELPERS ====================
os.makedirs("logs", exist_ok=True)


def log_message(uid, msg):
    ts = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}\n"
    print(f"📩 [{uid}] {msg[:200]}...")
    try:
        with open(f"logs/{uid}.txt", "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass


def generate_device_id():
    return "".join(random.choices(string.hexdigits.lower(), k=32))


def build_base_online_frame(uid):
    try:
        clean_hex = "".join(BASE_ONLINE_FRAME_HEX.split()).upper()
        old_uid_hex = b"1786255293228560621211".hex().upper()
        new_uid_hex = str(uid).encode("utf-8").hex().upper()
        if len(old_uid_hex) != len(new_uid_hex):
            log_message(uid, "⚠️ Base online UID length mismatch; keeping raw/replaced hex.")
        updated_hex = clean_hex.replace(old_uid_hex, new_uid_hex)
        return bytes.fromhex(updated_hex)
    except Exception as e:
        log_message(uid, f"⚠️ Base online frame error: {e}")
        return None


# ==================== ONLINE-ONLY BOT CLASS ====================
class OlaPartyOnlineBot:
    def __init__(self, account):
        self.account = account
        self.uid = account.get("uid", "unknown")
        self.auth_token = account.get("auth_token", "")
        self.device_id = account.get("device_id") or generate_device_id()

        self.ws = None
        self.running = True
        self.connected = False
        self.session_stop = threading.Event()
        self.state_lock = threading.Lock()
        self.reconnect_scheduled = False

        self.base_online_frame = build_base_online_frame(self.uid)

    def _safe_send_binary(self, ws, data, label):
        if not self.running or self.session_stop.is_set() or not self.connected or not ws:
            return False
        try:
            ws.send(data, websocket.ABNF.OPCODE_BINARY)
            return True
        except Exception as e:
            log_message(self.uid, f"{label} send error: {e}")
            self.connected = False
            self.session_stop.set()
            return False

    def on_open(self, ws):
        with self.state_lock:
            self.session_stop.clear()
            self.connected = True
            self.reconnect_scheduled = False

        print(f"🟢 [{self.uid}] Connected (Online Only)!")
        log_message(self.uid, "Connected in ONLINE-ONLY mode (no room join)")

        if self.base_online_frame:
            if not self._safe_send_binary(ws, self.base_online_frame, "Base Online Frame"):
                return
            print(f"🟢 [{self.uid}] Sent Base Online Frame")

        self.start_heartbeat(ws, self.session_stop)

    def start_heartbeat(self, ws, stop_event):
        def base_heartbeat():
            while self.running and not stop_event.is_set():
                if stop_event.wait(25):
                    break
                if not self._safe_send_binary(ws, self.base_online_frame, "Base Heartbeat"):
                    break

        threading.Thread(target=base_heartbeat, daemon=True).start()
        print(f"💓 [{self.uid}] Online heartbeat started (25s)")
        log_message(self.uid, "Online heartbeat started (25s)")

    def on_message(self, ws, msg):
        try:
            if isinstance(msg, bytes):
                log_message(self.uid, f"BINARY: {msg.hex()[:300]}...")
            else:
                log_message(self.uid, f"TEXT: {str(msg)[:300]}")
        except Exception:
            pass

    def on_ping(self, ws, data):
        try:
            if self.running and not self.session_stop.is_set():
                ws.send(data, websocket.ABNF.OPCODE_PONG)
        except Exception:
            pass

    def on_error(self, ws, err):
        print(f"⚠️ [{self.uid}] Error: {err}")
        log_message(self.uid, f"Error: {err}")
        self.connected = False
        self.session_stop.set()

    def on_close(self, ws, close_status_code, close_msg):
        print(f"❌ [{self.uid}] Disconnected (code={close_status_code})")
        log_message(self.uid, f"Disconnected (code={close_status_code}, msg={close_msg})")
        self.connected = False
        self.session_stop.set()
        self.schedule_reconnect()

    def schedule_reconnect(self):
        if not self.running:
            return
        with self.state_lock:
            if self.reconnect_scheduled:
                return
            self.reconnect_scheduled = True

        def _worker():
            print(f"🔄 [{self.uid}] Reconnecting in {RECONNECT_DELAY} seconds...")
            log_message(self.uid, f"Reconnecting in {RECONNECT_DELAY}s...")
            time.sleep(RECONNECT_DELAY)
            with self.state_lock:
                self.reconnect_scheduled = False
            if self.running:
                self.start()

        threading.Thread(target=_worker, daemon=True).start()

    def start(self):
        if not self.running:
            return
        if not self.base_online_frame:
            self.base_online_frame = build_base_online_frame(self.uid)
        if not self.base_online_frame:
            print(f"❌ [{self.uid}] Cannot start without base online frame")
            return

        headers = {
            "X-Auth-Token": self.auth_token,
            "X-DeviceId": self.device_id,
            "X-DeviceType": "Google Pixel 4",
            "X-App-Name": "olaparty",
            "X-OsType": "android",
            "X-CpuArch": "aarch64",
            "X-App-Channel": "official",
            "X-Sdk-Ver": "30",
            "X-SimCIso": "in",
            "X-Client-Net": "1",
            "X-BuildCode": "3185",
            "X-App-LastVer": "",
            "X-Apk-Abi": "abi64",
            "X-Lang": "en_in",
            "X-App-Ver": "52303",
            "X-App-Real-Ver": "52303",
            "X-Os-Ver": "11",
            "X-Pcid": f"115292150462477{str(self.uid)[-4:]}",
            "X-Request-WsId": str(int(time.time() * 1000)),
            "X-Last-Seqid": "0",
            "Origin": "https://i-875.olaparty.com",
            "User-Agent": "com.live.party/3185 (Linux; U; Android 11; en_IN; Pixel 4; Build/RD2A.211001.002; Cronet/93.0.4533.0)",
        }

        try:
            self.ws = websocket.WebSocketApp(
                WS_URL,
                header=headers,
                on_open=self.on_open,
                on_message=self.on_message,
                on_ping=self.on_ping,
                on_error=self.on_error,
                on_close=self.on_close,
            )
            self.ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"⚠️ [{self.uid}] Connection error: {e}")
            log_message(self.uid, f"Connection error: {e}")
            self.connected = False
            self.session_stop.set()
            if self.running:
                self.schedule_reconnect()

    def stop(self):
        self.running = False
        self.connected = False
        self.session_stop.set()
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        print(f"🛑 [{self.uid}] Stopped")
        log_message(self.uid, "Bot stopped")


# ==================== BOT MANAGER & AUTO RESTART ====================
def create_and_start_bots():
    global active_bots

    try:
        bot_count = int(os.environ.get("BOT_COUNT", len(ACCOUNTS)))
        if bot_count < 1 or bot_count > len(ACCOUNTS):
            bot_count = len(ACCOUNTS)
    except Exception:
        bot_count = len(ACCOUNTS)

    selected_accounts = ACCOUNTS[:bot_count]
    print(f"🟢 Starting {bot_count} bots in ONLINE-ONLY mode...")

    new_bots = []
    for idx, acc in enumerate(selected_accounts, 1):
        uid = acc.get("uid", f"acc_{idx}")
        print(f"🔄 Bot {idx}/{bot_count} (UID: {uid}) going online...")
        bot = OlaPartyOnlineBot(acc)
        threading.Thread(target=bot.start, daemon=True).start()
        new_bots.append(bot)
        time.sleep(2)

    with bots_lock:
        active_bots = new_bots

    print(f"✅ {len(new_bots)} bots online\n")


def stop_all_bots():
    with bots_lock:
        old = list(active_bots)
    print(f"🛑 Stopping {len(old)} bots...")
    for b in old:
        try:
            b.stop()
        except Exception as e:
            print(f"⚠️ stop error for {b.uid}: {e}")
    time.sleep(3)


def restart_all_bots(trigger="auto"):
    global last_restart_time, restart_in_progress

    if restart_in_progress:
        print("⚠️ Restart already in progress, skipping...")
        return

    restart_in_progress = True
    try:
        print("\n" + "=" * 70)
        print(f"♻️  RESTARTING ALL BOTS ({trigger.upper()}) @ {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
        print("=" * 70)

        stop_all_bots()
        print(f"⏳ Waiting {RESTART_GAP}s before restart...")
        time.sleep(RESTART_GAP)
        create_and_start_bots()

        last_restart_time = datetime.now(IST)
        print(f"✅ Restart complete @ {last_restart_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print("=" * 70 + "\n")
    except Exception as e:
        print(f"❌ Restart error: {e}")
    finally:
        restart_in_progress = False


def get_next_restart_time():
    now = datetime.now(IST)
    target = now.replace(hour=RESTART_HOUR, minute=RESTART_MINUTE, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


def auto_restart_scheduler():
    print(f"⏰ Auto-restart scheduler started. Daily at {RESTART_HOUR:02d}:{RESTART_MINUTE:02d} IST")
    while True:
        try:
            next_run = get_next_restart_time()
            wait_seconds = (next_run - datetime.now(IST)).total_seconds()
            while wait_seconds > 0:
                chunk = min(60, wait_seconds)
                time.sleep(chunk)
                wait_seconds -= chunk
                now = datetime.now(IST)
                if now.hour == RESTART_HOUR and now.minute == RESTART_MINUTE:
                    break
            restart_all_bots(trigger="auto-4am")
        except Exception as e:
            print(f"⚠️ Scheduler error: {e}, retrying in 60s")
            time.sleep(60)


# ==================== MAIN ====================
def main():
    print("\n" + "=" * 70)
    print("🟢 OLA PARTY BOT - ONLINE ONLY MODE")
    print("=" * 70)
    print(f"📱 Total Accounts: {len(ACCOUNTS)}")
    print(f"⏰ Daily auto-restart: {RESTART_HOUR:02d}:{RESTART_MINUTE:02d} IST")
    print("=" * 70 + "\n")

    threading.Thread(target=keep_alive, daemon=True).start()
    time.sleep(2)
    threading.Thread(target=auto_restart_scheduler, daemon=True).start()
    create_and_start_bots()

    try:
        while True:
            time.sleep(60)
            with bots_lock:
                total = len(active_bots)
                connected = sum(1 for b in active_bots if b.connected)
            next_r = get_next_restart_time().strftime("%H:%M IST")
            print(f"📊 Status: {connected}/{total} online | Next restart: {next_r}")
    except KeyboardInterrupt:
        print("\n🛑 Stopping all bots...")
        stop_all_bots()
        print("✅ All bots stopped. Goodbye!")


if __name__ == "__main__":
    main()
