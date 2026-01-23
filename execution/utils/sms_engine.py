import time
import sys
import os
from datetime import datetime
import pytz

# Ensure we can find the database module
# (Absolute import assuming execution as main package)
try:
    from execution.utils.database import init_db, add_sms_to_queue, claim_pending_sms, update_sms_status, log_conversation_event, update_lead_status, check_opt_out_status, process_alert_buffer, update_sms_twilio_sid, get_tenant_by_id
    from execution.utils.logger import setup_logger
    from execution.utils.alert_system import send_critical_alert
    from execution.utils.security import mask_pii
except ImportError:
    # If running as script from root maybe
    from execution.utils.database import init_db, add_sms_to_queue, claim_pending_sms, update_sms_status, log_conversation_event, update_lead_status, check_opt_out_status, process_alert_buffer, update_sms_twilio_sid
    from execution.utils.logger import setup_logger
    from execution.utils.alert_system import send_critical_alert

    from execution.utils.security import mask_pii
    from execution.utils.database import get_tenant_by_id

logger = setup_logger("SMSEngine")

MAX_RETRIES = 5 # Lower count, but longer wait text time

# Initialize DB
try:
    init_db()
except Exception as e:
    logger.critical(f"Database Init Failed: {e}")
    send_critical_alert("Database Init Failed", str(e))
    sys.exit(1)

from execution.services.twilio_service import get_twilio_service
try:
    from execution.config import PLUMBER_PHONE_NUMBER
except ImportError:
    PLUMBER_PHONE_NUMBER = None

def add_to_queue(to_number, body, external_id=None, tenant_id=None, delay_seconds=0):
    """Adds a message to the pending queue (SQLite)"""
    # Validate phone number format (basic E.164 check)
    if not to_number or len(str(to_number).strip()) < 10:
        logger.warning(f"⛔️ Invalid phone number format: {mask_pii(to_number)}")
        return False
    
    # Validate tenant_id if provided
    if tenant_id and tenant_id != "system_alert":
        from execution.utils.database import get_tenant_by_id
        tenant_config = get_tenant_by_id(tenant_id)
        if not tenant_config:
            logger.warning(f"⛔️ Invalid tenant_id: {tenant_id}. Message not queued.")
            return False
    
    # IMMEDIATE STOP CHECK: Check opt-out FIRST before any other processing
    # Check in-memory cache first (faster, works even if DB is down)
    from execution.utils.resilience import check_opt_out_cache
    if check_opt_out_cache(to_number):
        logger.warning(f"⛔️ IMMEDIATE BLOCK (cache): {mask_pii(to_number)} is unsubscribed. Message not queued.")
        return False
    
    # Also check database (for persistence)
    try:
        if check_opt_out_status(to_number):
            logger.warning(f"⛔️ IMMEDIATE BLOCK (DB): {mask_pii(to_number)} is unsubscribed. Message not queued.")
            return False
    except Exception as e:
        logger.warning(f"Failed to check opt-out status in DB: {e}. Using cache result only.")
        # Continue - cache check already passed
    
    # CENTRAL SAFETY CHECK: Block unless safe and compliant
    from execution.utils.security import check_send_safety
    
    # Check if internal alert (plumber phone)
    is_internal = False
    
    # Case A: Global Admin/Plumber Phone
    if PLUMBER_PHONE_NUMBER and to_number == PLUMBER_PHONE_NUMBER:
        is_internal = True
        logger.info(f"✅ Internal alert detected: {mask_pii(to_number)} matches Global Plumber Phone")
        
    # Case B: Tenant Specific Plumber Phone
    if not is_internal and tenant_id:
        from execution.utils.database import get_tenant_by_id
        tenant_config = get_tenant_by_id(tenant_id)
        if tenant_config:
            plumber_phone = tenant_config.get('plumber_phone_number')
            if plumber_phone and plumber_phone == to_number:
                is_internal = True
                logger.info(f"✅ Internal alert detected: {mask_pii(to_number)} is plumber for tenant {tenant_id}")
    
    allowed, reason = check_send_safety(to_number, body, external_id=external_id, tenant_id=tenant_id, is_internal_alert=is_internal)
    if not allowed:
        logger.warning(f"⛔️ Dropping message to {mask_pii(to_number)} - {reason}")
        return False

    # Pass delay_seconds to DB function
    added = add_sms_to_queue(to_number, body, external_id=external_id, tenant_id=tenant_id, delay_seconds=delay_seconds)
    if added:
        if delay_seconds > 0:
            logger.info(f"Queued DELAYED message for {mask_pii(to_number)} (+{delay_seconds}s)")
        else:
            logger.info(f"Queued message for {mask_pii(to_number)} (Tenant: {tenant_id})")
        return True
    else:
        logger.info(f"Skipped duplicate message for {mask_pii(to_number)} (Ref: {external_id})")
        return False

def calculate_backoff(attempt):
    """Exponential Backoff: 0 for first, then 5s, 30s, 2m, 10m, 30m"""
    if attempt == 0: return 0 
    if attempt == 1: return 5
    if attempt == 2: return 30
    if attempt == 3: return 120
    if attempt == 4: return 600
    return 1800 # Cap at 30 mins

# Cache for alert buffer processing to avoid checking every cycle
_alert_buffer_last_check = 0
_alert_buffer_check_interval = 5  # Check every 5 seconds instead of every cycle

def process_queue():
    """Reads queue from DB, attempts to send pending messages"""
    
    # Fetch pending from DB
    
    # 0. Process Alert Buffer (Anti-Annoyance)
    # This checks for grouped alerts that are ready to send.
    # Only check periodically to avoid unnecessary DB queries
    global _alert_buffer_last_check
    import time
    now = time.time()
    if now - _alert_buffer_last_check > _alert_buffer_check_interval:
        try:
            # Quick check if any alerts exist before processing
            from execution.utils.database import get_db_connection
            conn = get_db_connection()
            if conn:
                try:
                    now_iso = datetime.now().isoformat()
                    count = conn.execute("SELECT COUNT(*) FROM alert_buffer WHERE send_at <= ?", (now_iso,)).fetchone()[0]
                    if count > 0:
                        processed_alerts = process_alert_buffer()
                        if processed_alerts > 0:
                            logger.info(f"Released {processed_alerts} buffered groups to SMS queue.")
                    # Update timestamp even if no alerts (to avoid checking every cycle)
                    _alert_buffer_last_check = now
                finally:
                    conn.close()
            else:
                _alert_buffer_last_check = now  # Update on connection failure
        except Exception as e:
            logger.error(f"Error Processing Alert Buffer: {e}")
            _alert_buffer_last_check = now  # Update even on error to prevent spam

    # Atomic Claim from DB
    queue = claim_pending_sms(limit=10)
    
    if not queue:
        return [] # No work
    else:
        logger.info(f"Processing SMS Queue ({len(queue)} items)...")

    # Get Single Twilio Instance
    twilio = get_twilio_service()

    for msg in queue:
        logger.info(f"🔍 DEBUG: Worker picked up msg_id {msg['id']}")
        # Map DB columns to variables
        msg_id = msg['id']
        tenant_id = msg.get('tenant_id') # New field
        to_number = msg['to_number']
        body = msg['body']
        attempts = msg['attempts']
        last_attempt_str = msg['last_attempt']
        
        # CENTRAL SAFETY CHECK (Double Check before sending)
        from execution.utils.security import check_send_safety
        
        # Get tenant config for safety check
        tenant_config = None
        is_internal_alert = False
        
        # Case A: Global Admin
        if PLUMBER_PHONE_NUMBER and to_number == PLUMBER_PHONE_NUMBER:
            is_internal_alert = True
        
        # Case B: Tenant Specific
        if tenant_id:
            tenant_config = get_tenant_by_id(tenant_id)
            if tenant_config and tenant_config.get('plumber_phone_number') == to_number:
                is_internal_alert = True
            elif not tenant_config:
                logger.warning(f"⚠️ Tenant {tenant_id} not found. Skipping safety check for internal alert detection.")
        
        allowed, reason = check_send_safety(to_number, body, external_id=msg.get('external_id'), tenant_id=tenant_id, is_internal_alert=is_internal_alert)
        if not allowed:
            logger.warning(f"⛔️ Dropping queued message to {mask_pii(to_number)} - {reason}")
            update_sms_status(msg_id, 'failed_safety', attempts)
            continue
        
        if attempts >= MAX_RETRIES:
            # DEAD-LETTER QUEUE: Move to failed_permanent after max retries
            error_msg = f"DEAD-LETTER: Message {msg_id} failed after {attempts} attempts. To: {mask_pii(to_number)}. Last error: Max retries exceeded."
            logger.error(error_msg)
            
            try:
                update_sms_status(msg_id, 'failed_permanent', attempts, last_attempt=datetime.now().isoformat())
            except Exception as update_error:
                logger.critical(f"CRITICAL: Failed to mark message {msg_id} as failed_permanent: {update_error}")
            
            # 🚨 LOUD ALERT: Message failed - very visible log
            logger.critical(f"🚨 MESSAGE FAILED - ID: {msg_id} | Reason: Max retries exceeded ({attempts} attempts) | To: {mask_pii(to_number)}")
            
            # ALERT: Clear error message for monitoring
            send_critical_alert(
                "SMS Dead-Letter Queue", 
                f"Message moved to dead-letter after {attempts} retries.\n"
                f"To: {mask_pii(to_number)}\n"
                f"Message ID: {msg_id}\n"
                f"Body preview: {body[:100]}..."
            )
            continue

        # --- A. COMPLIANCE GATE (Usage: Footer Check) ---
        # Rule: Every outbound message must have a way to opt-out.
        # Exception: Critical Alerts to the Plumber (internal) don't need footers.
        # tenant_config already loaded above for safety check
        
        if not is_internal_alert:
            body_lower = body.lower()
            required_footers = ["stop", "unsubscribe", "cancel", "opt out", "opt-out"]
            
            # Soft Check: If it's short, it might be a conversation. 
            # Strict Rule: "Make sure every outbound message includes... one-click or Reply STOP"
            # We enforce "STOP" presence.
            if not any(f in body_lower for f in required_footers):
                # FIX: Auto-append footer instead of blocking
                # This ensures compliance while allowing messages to send
                logger.info(f"⚠️ Auto-appending compliance footer to message {msg_id}")
                footer = "\n\nReply STOP to unsubscribe."
                body += footer
                
                # CRITICAL: Update body in database to prevent duplicate footers on retry
                # If DB update fails, we still send the modified body (graceful degradation)
                try:
                    from execution.utils.database import update_sms_body
                    if not update_sms_body(msg_id, body):
                        logger.warning(f"⚠️ Failed to update body in DB for {msg_id}, but will send modified body")
                except Exception as e:
                    logger.warning(f"⚠️ Error updating body in DB: {e}. Will send modified body anyway.")


        # --- C. URL SHORTENER CHECK (Deliverability) ---
        # Rule: Avoid bit.ly, tinyurl, etc. in pilot phase to prevent filtering.
        shorteners = ["bit.ly", "tinyurl.com", "goo.gl", "t.co", "is.gd", "buff.ly"]
        if any(s in body.lower() for s in shorteners):
            logger.warning(f"⚠️ DELIVERABILITY WARNING: Message {msg_id} contains a URL shortener. This may be blocked by carriers.")
            # We don't block it yet, but we log the warning for the pilot team.

        # --- B. TIMEZONE GATE (Reliability) ---
        # Don't text customers at 3 AM unless it's an emergency response they just asked for.
        if tenant_config and not is_internal_alert:
            # Check Timezone - use same logic as security.py for consistency
            try:
                tz_name = tenant_config.get('timezone', 'America/Los_Angeles')
                tz = pytz.timezone(tz_name)
                local_now = datetime.now(tz)
                hour = local_now.hour
                
                # Use tenant config for time window (consistent with security.py)
                start_h = int(tenant_config.get('business_hours_start', 8))
                end_h = int(tenant_config.get('business_hours_end', 21)) # Default to 9 PM
                
                if not (start_h <= hour < end_h):
                    # It's night time.
                    # ONLY allow if it's an "Emergency" response or explicit "After Hours" flow initiated by user.
                    # We check body keywords as a proxy for "Response"
                    is_response = "assistant" in body.lower() or "emergency" in body.lower()
                    
                    if not is_response:
                        logger.warning(f"⏳ Timezone Guard: Holding message to {mask_pii(to_number)} until {start_h}am. (Hour: {hour})")
                        # Requeue with backoff (wait 1 hour or check later)
                        # We just leave it pending but update last_attempt so we don't spin
                        status_update = "pending"
                        update_sms_status(msg_id, status_update, attempts, last_attempt=datetime.now().isoformat())
                        continue
            except Exception as e:
                logger.error(f"Timezone Check Failed: {e}")
        
        # Send with timeout protection
        logger.info(f"Attempt #{attempts+1} for {mask_pii(to_number)}...")
        send_success = False
        send_error = None
        message_sid = None
        
        try:
            # Send with explicit timeout handling
            # send_sms now returns MessageSid on success, False on failure
            result = twilio.send_sms(to_number, body, tenant_id=tenant_id, external_id=msg.get('external_id'))
            if result:
                send_success = True
                message_sid = result  # Store the MessageSid
                logger.info(f"✅ SMS sent successfully. MessageSid: {message_sid}")
            else:
                send_success = False
        except Exception as e:
            # API timeout or crash - log clearly
            send_error = str(e)
            error_type = "API_TIMEOUT" if "timeout" in str(e).lower() or "timed out" in str(e).lower() else "API_ERROR"
            logger.error(f"{error_type}: Failed to send to {mask_pii(to_number)}: {e}")
            send_success = False
        
        # CRITICAL: Always update status, even if send failed or status update fails
        # This prevents infinite retries and double-sending
        try:
            if send_success:
                # Success: Mark as sent
                update_sms_status(msg_id, 'sent', attempts+1, sent_at=datetime.now().isoformat())
                
                # Store Twilio MessageSid for status callback tracking
                if message_sid:
                    update_sms_twilio_sid(msg_id, message_sid)
                    logger.info(f"📝 Stored MessageSid {message_sid} for message {msg_id}")
                
                # LEAD STATE UPDATE
                log_conversation_event(to_number, 'outbound', body, external_id=f"out_{msg_id}", tenant_id=tenant_id)
                update_lead_status(to_number, 'contacted', tenant_id=tenant_id)
            else:
                # Failure: Requeue with backoff (if not at max retries)
                if attempts + 1 < MAX_RETRIES:
                    update_sms_status(msg_id, 'pending', attempts+1, last_attempt=datetime.now().isoformat())
                    logger.warning(f"Retry scheduled for {mask_pii(to_number)} (attempt {attempts+1}/{MAX_RETRIES})")
                else:
                    # At max retries: Move to dead-letter
                    update_sms_status(msg_id, 'failed_permanent', attempts+1, last_attempt=datetime.now().isoformat())
                    logger.error(f"DEAD-LETTER: Message {msg_id} moved to failed_permanent after {attempts+1} attempts. Error: {send_error or 'Send returned False'}")
                    
                    # 🚨 LOUD ALERT: Message failed - very visible log
                    reason = send_error or 'Send returned False'
                    logger.critical(f"🚨 MESSAGE FAILED - ID: {msg_id} | Reason: {reason} | Attempts: {attempts+1}/{MAX_RETRIES} | To: {mask_pii(to_number)}")
                    
                    send_critical_alert(
                        "SMS Dead-Letter Queue",
                        f"Message moved to dead-letter after {attempts+1} retries.\n"
                        f"To: {mask_pii(to_number)}\n"
                        f"Message ID: {msg_id}\n"
                        f"Error: {send_error or 'Unknown error'}"
                    )
        except Exception as update_error:
            # CRITICAL: If status update fails, mark as failed_permanent to prevent infinite retries
            logger.critical(f"CRITICAL: Failed to update status for {msg_id}: {update_error}. Marking as failed_permanent to prevent infinite retries.")
            try:
                update_sms_status(msg_id, 'failed_permanent', attempts+1, last_attempt=datetime.now().isoformat())
                # 🚨 LOUD ALERT: Message failed due to status update error
                logger.critical(f"🚨 MESSAGE FAILED - ID: {msg_id} | Reason: Status update failed ({update_error}) | To: {mask_pii(to_number)}")
            except:
                # Last resort: log and continue (message will be picked up by stuck message recovery)
                logger.critical(f"FATAL: Cannot update status for {msg_id}. Manual intervention required.")
                logger.critical(f"🚨 MESSAGE FAILED - ID: {msg_id} | Reason: Cannot update status (database error) | To: {mask_pii(to_number)}")

    return queue

def run_worker():
    """Continuous loop to process queue"""
    logger.info("🚀 SMS Engine Worker Started (Adaptive Polling)...")
    failure_streak = 0
    current_backoff = 1
    
    while True:
        try:
            from execution import config
            if config.KILL_SWITCH:
                logger.warning("🛑 SMS Worker PAUSED (KILL_SWITCH=ON). Waiting...")
                time.sleep(10)
                continue

            # Process queue and check if work was found
            queue = process_queue()
            if queue and len(queue) > 0:
                # Work was processed (even if some failed)
                # Reset backoff since we're actively processing
                current_backoff = 1
                failure_streak = 0
                # Don't sleep if we found work, maybe a small yield
                time.sleep(0.1)
                continue
                
            # No work found (empty queue), increase backoff (Capped at 2s for instant OTP response)
            current_backoff = min(current_backoff * 1.5, 2)
            failure_streak = 0 # Reset on success run
            
        except Exception as e:
            failure_streak += 1
            logger.error(f"Worker Loop Error: {e}")
            current_backoff = 10 # Reliable backoff on error
            
            if failure_streak >= 3:
                 logger.critical("Worker crashing repeatedly!")
                 send_critical_alert("SMS Worker Repeated Crashes", str(e))
                 time.sleep(60) # Sleep longer to let things calm down
                 
        logger.debug(f"Worker sleeping for {current_backoff:.1f}s")
        time.sleep(current_backoff)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        # Test Run
        logger.info("Testing DB Migration & Queue...")
        add_to_queue("+15551234444", "Hello SQLite World")
        process_queue()
    else:
        # Default to Worker Mode
        run_worker()

