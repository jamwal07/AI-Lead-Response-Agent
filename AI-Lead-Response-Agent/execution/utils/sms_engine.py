import time
import sys
import os
from datetime import datetime
import pytz

# Ensure we can find the database module
# (Absolute import assuming execution as main package)
try:
    from execution.utils.database import init_db, add_sms_to_queue, claim_pending_sms, update_sms_status, log_conversation_event, update_lead_status, check_opt_out_status, process_alert_buffer
    from execution.utils.logger import setup_logger
    from execution.utils.alert_system import send_critical_alert
    from execution.utils.security import mask_pii
except ImportError:
    # If running as script from root maybe
    from execution.utils.database import init_db, add_sms_to_queue, claim_pending_sms, update_sms_status, log_conversation_event, update_lead_status, check_opt_out_status, process_alert_buffer
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

def add_to_queue(to_number, body, external_id=None, tenant_id=None):
    """Adds a message to the pending queue (SQLite)"""
    # IMMEDIATE STOP CHECK: Check opt-out FIRST before any other processing
    # This ensures STOP works even if message is delayed in queue
    if check_opt_out_status(to_number):
        logger.warning(f"⛔️ IMMEDIATE BLOCK: {mask_pii(to_number)} is unsubscribed. Message not queued.")
        return False
    
    # CENTRAL SAFETY CHECK: Block unless safe and compliant
    from execution.utils.security import check_send_safety
    
    # Check if internal alert (plumber phone)
    is_internal = False
    if tenant_id:
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

    added = add_sms_to_queue(to_number, body, external_id=external_id, tenant_id=tenant_id)
    if added:
        logger.info(f"Queued message for {mask_pii(to_number)} (Tenant: {tenant_id})")
    else:
        logger.info(f"Skipped duplicate message for {mask_pii(to_number)} (Ref: {external_id})")

def calculate_backoff(attempt):
    """Exponential Backoff: 0 for first, then 5s, 30s, 2m, 10m, 30m"""
    if attempt == 0: return 0 
    if attempt == 1: return 5
    if attempt == 2: return 30
    if attempt == 3: return 120
    if attempt == 4: return 600
    return 1800 # Cap at 30 mins

def process_queue():
    """Reads queue from DB, attempts to send pending messages"""
    
    # Fetch pending from DB
    
    # 0. Process Alert Buffer (Anti-Annoyance)
    # This checks for grouped alerts that are ready to send.
    try:
        processed_alerts = process_alert_buffer()
        if processed_alerts > 0:
            logger.info(f"Released {processed_alerts} buffered groups to SMS queue.")
    except Exception as e:
        logger.error(f"Error Processing Alert Buffer: {e}")

    # Atomic Claim from DB
    queue = claim_pending_sms(limit=10)
    
    if not queue:
        pass
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
        if tenant_id:
            tenant_config = get_tenant_by_id(tenant_id)
            if tenant_config and tenant_config.get('plumber_phone_number') == to_number:
                is_internal_alert = True
        
        allowed, reason = check_send_safety(to_number, body, external_id=msg.get('external_id'), tenant_id=tenant_id, is_internal_alert=is_internal_alert)
        if not allowed:
            logger.warning(f"⛔️ Dropping queued message to {mask_pii(to_number)} - {reason}")
            update_sms_status(msg_id, 'failed_safety', attempts)
            continue
        
        # Check Backoff Timing
        if last_attempt_str:
            last_attempt = datetime.fromisoformat(last_attempt_str)
            seconds_since = (datetime.now() - last_attempt).total_seconds()
            required_wait = calculate_backoff(attempts)
            
            if seconds_since < required_wait:
                # Not ready yet - revert status so another worker (or this one later) can pick it up
                update_sms_status(msg_id, 'pending', attempts, last_attempt=last_attempt_str)
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
                logger.error(f"⛔️ COMPLIANCE BLOCKED: Message {msg_id} missing footer. Body: {body}")
                update_sms_status(msg_id, 'failed_compliance', attempts)
                # We log it but maybe don't wake up the admin for every missing footer, 
                # unless it's a systemic issue.
                continue

        # --- B. TIMEZONE GATE (Reliability) ---
        # Don't text customers at 3 AM unless it's an emergency response they just asked for.
        if tenant_config and not is_internal_alert:
            # Check Timezone
            try:
                tz = pytz.timezone(tenant_config.get('timezone', 'America/Los_Angeles'))
                local_now = datetime.now(tz)
                hour = local_now.hour
                
                # Allow: 8 AM to 9 PM (Typical TCPA Safe Harbor is 8am-9pm local)
                # But we use tenant config if available
                # If it's an immediate reply to an inbound msg (within 5 mins), usually it's okay.
                # Here we implement a hard safety net.
                
                start_h = 8
                end_h = 21 # 9 PM
                
                if not (start_h <= hour < end_h):
                    # It's night time.
                    # ONLY allow if it's an "Emergency" response or explicit "After Hours" flow initiated by user.
                    # We check body keywords as a proxy for "Response"
                    is_response = "assistant" in body.lower() or "emergency" in body.lower()
                    
                    if not is_response:
                        logger.warning(f"⏳ Timezone Guard: Holding message to {mask_pii(to_number)} until 8am. (Hour: {hour})")
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
        
        try:
            # Send with explicit timeout handling
            success = twilio.send_sms(to_number, body, tenant_id=tenant_id, external_id=msg.get('external_id'))
            send_success = success
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
                
                # LEAD STATE UPDATE
                log_conversation_event(to_number, 'outbound', body, external_id=f"out_{msg_id}", tenant_id=tenant_id)
                update_lead_status(to_number, 'contacted')
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

def run_worker():
    """Continuous loop to process queue"""
    logger.info("🚀 SMS Engine Worker Started (Polling every 10s)...")
    failure_streak = 0
    
    while True:
        try:
            process_queue()
            failure_streak = 0 # Reset on success run
        except Exception as e:
            failure_streak += 1
            logger.error(f"Worker Loop Error: {e}")
            
            if failure_streak >= 3:
                 logger.critical("Worker crashing repeatedly!")
                 send_critical_alert("SMS Worker Repeated Crashes", str(e))
                 time.sleep(60) # Sleep longer to let things calm down
                 
        time.sleep(10)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        # Test Run
        print("Testing DB Migration & Queue...")
        add_to_queue("+15551234444", "Hello SQLite World")
        process_queue()
    else:
        # Default to Worker Mode
        run_worker()
