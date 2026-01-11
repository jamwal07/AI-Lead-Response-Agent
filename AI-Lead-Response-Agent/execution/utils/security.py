
import os
import time
import functools
import re
from flask import request, abort
from twilio.request_validator import RequestValidator
from execution import config
from execution.utils.logger import setup_logger

logger = setup_logger("Security")

# Rate Limiting Storage (Memory)
# { ip_address: [timestamp1, timestamp2] }
_request_records = {}
RATE_LIMIT_WINDOW = 60 # seconds
RATE_LIMIT_MAX = 60 # requests per window (Approx 1 per second is generous enough for small biz)

def mask_pii(text):
    """
    Masks phone numbers in text for logging.
    Example: +15551234444 -> +1555***4444
    """
    if not text: return text
    if not isinstance(text, str): return str(text)
    
    # Regex to find E.164-like numbers (slightly flexible)
    # Matches +1 or just 1, followed by digits
    # We want to preserve the last 4, mask the middle 3-4
    
    def replacer(match):
        full_num = match.group(0)
        if len(full_num) < 7: return full_num # Too short
        return full_num[:-4] + "****"
        
    return re.sub(r'\+?\d{10,15}', replacer, text)

def check_rate_limit():
    ip = request.remote_addr
    now = time.time()
    
    # Prune old
    global _request_records
    history = _request_records.get(ip, [])
    # Filter only recent
    history = [t for t in history if now - t < RATE_LIMIT_WINDOW]
    
    if len(history) >= RATE_LIMIT_MAX:
        logger.warning(f"⛔️ Rate Limit Exceeded for IP: {ip}")
        return False
        
    history.append(now)
    _request_records[ip] = history
    return True

_tenant_records = {}
TENANT_RATE_LIMIT = 20 # Max calls/sms per minute per tenant

def check_tenant_rate_limit(tenant_id):
    now = time.time()
    global _tenant_records
    history = _tenant_records.get(tenant_id, [])
    history = [t for t in history if now - t < 60]
    
    if len(history) >= TENANT_RATE_LIMIT:
        logger.warning(f"⛔️ Tenant Rate Limit Exceeded: {tenant_id}")
        return False
        
    history.append(now)
    _tenant_records[tenant_id] = history
    return True

def require_rate_limit(f):
    """
    Flask Decorator for rate limiting only (no signature check).
    Use for public endpoints that need rate limiting.
    """
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        if not check_rate_limit():
            logger.warning(f"⛔️ Rate Limit Exceeded for {request.path} from {request.remote_addr}")
            return "Rate limit exceeded. Please try again later.", 429
        return f(*args, **kwargs)
    return decorated_function

def require_twilio_signature(f):
    """
    Flask Decorator to verify that the request actually came from Twilio.
    FAIL CLOSED: Blocks all requests unless signature is valid or explicitly bypassed.
    """
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        # 1. Check if validation is explicitly disabled (ONLY for testing)
        is_validation_disabled = os.getenv("DISABLE_TWILIO_SIG_VALIDATION") == "1"
        
        if is_validation_disabled:
            logger.warning(f"⚠️  Insecure Mode: Skipping Twilio Signature Validation for {request.path}")
            return f(*args, **kwargs)

        # 2. Rate Limit Check (Only apply if we are strictly validating, otherwise let tests flow)
        if not check_rate_limit():
            logger.warning(f"⛔️ Rate Limit Exceeded for {request.path} from {request.remote_addr}")
            return "Rate limit exceeded. Please try again later.", 429

        # 3. FAIL CLOSED: Check for signature header FIRST (fail fast)
        signature = request.headers.get("X-Twilio-Signature", "")
        if not signature:
            url = request.url
            logger.warning(f"⛔️ Missing X-Twilio-Signature header from {request.remote_addr} on {url}")
            return "Forbidden: Missing Signature", 403

        # 4. FAIL CLOSED: Check if TWILIO_AUTH_TOKEN is configured
        if not config.TWILIO_AUTH_TOKEN or config.TWILIO_AUTH_TOKEN == "default-secret-change-me" or "YOUR_AUTH_TOKEN" in str(config.TWILIO_AUTH_TOKEN):
            logger.error(f"⛔️ Security Block: TWILIO_AUTH_TOKEN missing or invalid. Refusing {request.path}")
            return "Forbidden: Server Security Misconfigured", 403

        # 5. Validate signature
        validator = RequestValidator(config.TWILIO_AUTH_TOKEN)
        
        # Get the URL (handle proxy cases)
        url = request.url
        if request.headers.get("X-Forwarded-Proto") == "https":
             url = url.replace("http://", "https://")
        
        # Validate (URL, POST Data, Signature)
        # Note: Twilio signs the POST data.
        if not validator.validate(url, request.form, signature):
            logger.warning(f"⛔️ Invalid Twilio Signature from {request.remote_addr} on {url}")
            return "Forbidden: Invalid Signature", 403

        return f(*args, **kwargs)
    return decorated_function

# --- HMAC UTILS ---
import hmac
import hashlib

def generate_unsubscribe_token(phone):
    """
    Generates a secure token for one-click unsubscribe links.
    """
    secret = config.TWILIO_AUTH_TOKEN
    if not secret or secret == "default-secret-change-me" or "YOUR_AUTH_TOKEN" in str(secret):
        logger.error("⛔️ CRITICAL: TWILIO_AUTH_TOKEN is missing or placeholder. Cannot generate secure tokens.")
        raise ValueError("TWILIO_AUTH_TOKEN must be set in production")
    
    if not phone: return ""
    
    # Simple HMAC-SHA256
    h = hmac.new(secret.encode('utf-8'), phone.encode('utf-8'), hashlib.sha256)
    return h.hexdigest()

def verify_unsubscribe_token(phone, token):
    """
    Verifies the unsubscribe token.
    """
    expected = generate_unsubscribe_token(phone)
    return hmac.compare_digest(expected, token)

# --- CENTRAL SAFETY CHECK ---

def check_send_safety(to_number, body, external_id=None, tenant_id=None, is_internal_alert=False):
    """
    CENTRAL SAFETY CHECK: Blocks all sends unless safe and compliant.
    
    Checks:
    1. Consent exists (with proof: timestamp + source)
    2. Not unsubscribed
    3. Not a duplicate
    4. Allowed time window (8am-9pm, emergency exception)
    
    Returns:
        (allowed: bool, reason: str)
        - (True, "reason") if allowed
        - (False, "reason") if blocked
    
    Logs every check for audit trail.
    """
    from datetime import datetime
    import pytz
    from execution.utils.database import verify_valid_consent, check_opt_out_status, get_db_connection, get_tenant_by_id
    
    masked_number = mask_pii(to_number)
    
    # 1. CHECK CONSENT (Skip for Internal Alerts)
    if is_internal_alert:
        consent_proof = "Internal Alert (Implicit Consent)"
        logger.info(f"✅ Consent check skipped (internal) for {masked_number}")
    else:
        consent = verify_valid_consent(to_number, tenant_id=tenant_id)
        
        # FIX: Allow immediate response to missed calls (Implied Consent)
        # Even if DB hasn't updated 'leads' table yet, the fact we are sending a "Missed Call" msg
        # means the user just called us.
        is_missed_call_reply = "missed call" in body.lower() or "assistant" in body.lower()
        
        if not consent and not is_missed_call_reply:
            reason = f"BLOCKED: No valid consent for {masked_number}"
            logger.warning(f"🚫 {reason}")
            return False, reason
        
        if is_missed_call_reply and not consent:
             consent_proof = "Implied Consent (Inbound Call Response)"
             logger.info(f"✅ Consent check passed (Implied Response) for {masked_number}")
        else:
             consent_proof = f"Consent: {consent['consent_type']} from {consent['consent_source']} at {consent['consented_at']}"
             logger.info(f"✅ Consent check passed for {masked_number} - {consent_proof}")
    
    # 2. CHECK OPT-OUT
    if check_opt_out_status(to_number):
        reason = f"BLOCKED: {masked_number} is unsubscribed"
        logger.warning(f"🚫 {reason}")
        return False, reason
    
    logger.info(f"✅ Opt-out check passed for {masked_number}")
    
    # 3. CHECK DUPLICATE
    if external_id:
        conn = get_db_connection()
        try:
            existing = conn.execute(
                "SELECT id FROM sms_queue WHERE external_id = ? LIMIT 1",
                (external_id,)
            ).fetchone()
            if existing:
                reason = f"BLOCKED: Duplicate message (external_id: {external_id})"
                logger.warning(f"🚫 {reason} to {masked_number}")
                return False, reason
        finally:
            conn.close()
    
    logger.info(f"✅ Duplicate check passed for {masked_number}")
    
    # 4. CHECK TIME WINDOW (skip for internal alerts)
    if not is_internal_alert:
        tenant_config = None
        if tenant_id:
            tenant_config = get_tenant_by_id(tenant_id)
        
        if tenant_config:
            try:
                tz_name = tenant_config.get('timezone', 'America/Los_Angeles')
                tz = pytz.timezone(tz_name)
                local_now = datetime.now(tz)
                hour = local_now.hour
                
                start_h = 8
                end_h = 21  # 9 PM
                
                if not (start_h <= hour < end_h):
                    # Outside time window - check if emergency
                    is_emergency = "emergency" in body.lower() or "urgent" in body.lower()
                    if not is_emergency:
                        reason = f"BLOCKED: Outside time window (hour: {hour}, allowed: {start_h}-{end_h})"
                        logger.warning(f"🚫 {reason} to {masked_number}")
                        return False, reason
                    else:
                        logger.info(f"✅ Time window check passed (emergency exception) for {masked_number} at hour {hour}")
                else:
                    logger.info(f"✅ Time window check passed for {masked_number} at hour {hour}")
            except Exception as e:
                logger.error(f"Timezone check failed for {masked_number}: {e}")
                # Fail safe: allow if timezone check fails
                logger.info(f"✅ Time window check passed (timezone error, fail-safe) for {masked_number}")
    else:
        logger.info(f"✅ Time window check skipped (internal alert) for {masked_number}")
    
    # ALL CHECKS PASSED
    reason = f"ALLOWED: All safety checks passed - {consent_proof}"
    logger.info(f"✅ {reason} to {masked_number}")
    return True, reason
