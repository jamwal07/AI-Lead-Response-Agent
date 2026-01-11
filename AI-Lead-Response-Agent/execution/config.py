import os
from dotenv import load_dotenv

# Load .env (gracefully handle missing/unreadable .env)
try:
    load_dotenv()
except Exception:
    # .env is missing or unreadable - continue with environment variables or defaults
    pass

# --- TIMEZONE SETTING ---
# Canonical format (e.g., America/Los_Angeles, America/New_York)
TIMEZONE = os.getenv("TIMEZONE", "America/Los_Angeles")

# --- TWILIO CREDENTIALS ---
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER", "+15550000000")
TWILIO_MESSAGING_SERVICE_SID = os.getenv("TWILIO_MESSAGING_SERVICE_SID")  # Required for A2P 10DLC (US SMS)

# --- TELEGRAM ALERTS ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Validation: Fail Fast if Keys are placeholders or missing in production
# (We assume production if running inside docker or typical deploy)
if TWILIO_ACCOUNT_SID and "YOUR_SID" in TWILIO_ACCOUNT_SID:
    # Allow mock mode, but warn loudly
    print("⚠️ [CONFIG] Placeholder Credentials detected. App will run in Mock Mode.")
    TWILIO_ACCOUNT_SID = None # Force Mock

# --- APP CONFIG ---
PLUMBER_PHONE_NUMBER = os.getenv("PLUMBER_PHONE_NUMBER", "+15551234567")
AI_API_KEY = os.getenv("AI_API_KEY", "")

# --- SAFE MODE (Kill Switch) ---
# SAFE_MODE blocks all real SMS sends. Defaults to ON for local/dev.
# IMPORTANT: Always set SAFE_MODE explicitly in .env:
#   - LOCAL/DEV: SAFE_MODE=ON (always blocks real sends)
#   - PRODUCTION: SAFE_MODE=OFF (must be explicit, never rely on default)
# Accepts: "ON", "OFF", "true", "false", "1", "0" (case-insensitive)
_safe_mode_val = os.getenv("SAFE_MODE", "ON").upper().strip()
if _safe_mode_val in ("OFF", "FALSE", "0"):
    SAFE_MODE = False
else:
    SAFE_MODE = True  # Default to ON (safe) - blocks all SMS sends
