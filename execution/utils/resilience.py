
import os
import uuid
import time
from datetime import datetime
from execution.utils.logger import setup_logger
from execution.utils import database
from execution.utils.alert_system import send_critical_alert

logger = setup_logger("Resilience")

# 1. WEBHOOK VALIDATION
def validate_webhook_input(caller_number, to_number, call_sid):
    """
    Validates that a webhook request has all required Twilio parameters.
    Returns (is_valid, error_msg)
    """
    if not caller_number:
        return False, "Missing 'From' (Caller) number"
    if not to_number:
        return False, "Missing 'To' (Twilio) number"
    if not call_sid:
        return False, "Missing 'CallSid' or 'MessageSid' for identification"
    
    # Optional: Check if numbers look like phone numbers
    if len(str(caller_number)) < 7:
        return False, f"Invalid 'From' number: {caller_number}"
        
    return True, None

# short-term memory (shared across threads in same process)
_webhook_cache = {} 
_opt_out_cache = {} # Cache for blocked numbers

def check_opt_out_cache(phone):
    """Returns True if number is in the local opt-out cache."""
    return _opt_out_cache.get(phone, False)

def add_to_opt_out_cache(phone, is_blocked=True):
    """Adds a phone number to the local opt-out cache."""
    _opt_out_cache[phone] = is_blocked

def check_webhook_processed_safe(provider_id):
    """
    Checks if a webhook was already processed, using both DB and local cache.
    Returns (is_duplicate: bool, internal_id: str or None, used_fallback: bool)
    """
    # 1. Check local cache (Fastest)
    if provider_id in _webhook_cache:
        logger.info(f"♻️ Cache Hit: Duplicate webhook {provider_id}")
        return True, _webhook_cache[provider_id], False
        
    # 2. Check Database (Source of truth)
    try:
        is_duplicate, internal_id = database.check_webhook_processed(provider_id)
        if is_duplicate:
            # Backfill cache
            _webhook_cache[provider_id] = internal_id
            return True, internal_id, False
    except Exception as e:
        logger.error(f"❌ DB Check Failed for webhook {provider_id}: {e}")
        # If DB fails, we risk double-processing, but we MUST return something.
        # Returning False allows processing (safer than blocking a lead).
        return False, None, True
        
    return False, None, False

def add_to_webhook_cache(provider_id, internal_id):
    """Adds a webhook ID to the local short-term cache."""
    _webhook_cache[provider_id] = internal_id
    # Rotate cache if it gets too big (simple LRU)
    if len(_webhook_cache) > 1000:
        # Remove oldest entry
        first_key = next(iter(_webhook_cache))
        del _webhook_cache[first_key]

# 3. TENANT LOOKUP (SAFE)
def get_tenant_safe(to_number):
    """
    Safely retrieves a tenant by phone number with fallback logic.
    Returns (tenant: dict or None, error: str or None)
    """
    try:
        tenant = database.get_tenant_by_twilio_number(to_number)
        if tenant:
            return tenant, None
        return None, f"No tenant found for number {to_number}"
    except Exception as e:
        logger.error(f"❌ Tenant Lookup Failed for {to_number}: {e}")
        # Self-Healing: If DB is locked/failed, we might have a serious problem.
        send_critical_alert("Database Error - Tenant Lookup", f"Error looking up {to_number}: {e}")
        return None, str(e)

# 4. QUEUE RETRY (FUTURE PROOFING)
def queue_webhook_for_retry(sid, from_number, to_number, body, type="sms"):
    """
    If any downstream service (like AI) fails, we can queue the entire webhook
    metadata to be retried by a background process.
    (Placeholder for advanced resilience logic)
    """
    logger.warning(f"🔄 Webhook {sid} queued for retry (Simulated)")
    # logic to store in a 'webhooks_to_retry' table could go here
    return True

# 5. COMPLIANCE (SAFE)
def process_stop_safe(phone, tenant_id=None, keyword="STOP"):
    """
    Executes a STOP request safely, ensuring DB updates won't crash the handler.
    """
    try:
        database.set_opt_out(phone, True)
        database.revoke_consent(phone, reason=keyword, tenant_id=tenant_id)
        logger.info(f"🚫 STOP processed safely for {phone}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to process STOP for {phone}: {e}")
        send_critical_alert("Compliance Failure", f"Failed to opt-out {phone}: {e}")
        return False
