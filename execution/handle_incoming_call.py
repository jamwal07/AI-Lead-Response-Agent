from flask import Flask, request, render_template, jsonify
from flask_cors import CORS
import os
from twilio.twiml.voice_response import VoiceResponse, Gather
from twilio.rest import Client
import pytz
import uuid
from datetime import datetime, timedelta

# Import Config (Absolute Import from execution package)
from execution import config
from execution.utils.database import get_all_sms, create_or_update_lead, update_lead_status, log_conversation_event, get_lead_funnel_stats, set_opt_out, get_tenant_by_twilio_number, get_tenant_by_id, record_consent, revoke_consent, update_sms_status_by_message_sid, update_lead_intent, get_revenue_stats
from execution.utils.security import require_twilio_signature, require_rate_limit, mask_pii, check_tenant_rate_limit, verify_unsubscribe_token
from execution.utils.logger import setup_logger
from execution.utils.alert_system import send_critical_alert
from execution.services.twilio_service import get_twilio_service
from execution.utils.resilience import (
    validate_webhook_input, check_webhook_processed_safe, get_tenant_safe,
    queue_webhook_for_retry, add_to_webhook_cache, process_stop_safe
)
from execution.dashboard_api import dashboard_bp 
from execution.utils.database import cancel_pending_sms # Added for Nudge
import random

logger = setup_logger("FlaskWeb")

app = Flask(__name__, template_folder='../templates') # Point to templates folder
# Set secret key for session management (Use stable key for development)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "plumber-ai-secret-development-key-8291")
# Configure default session expiration (24 hours)
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=24)

@app.before_request
def handle_session_permanence():
    from flask import session
    session.permanent = True
    # If the user selected "Remember Me", extend to 30 days
    if session.get('remember'):
        app.permanent_session_lifetime = timedelta(days=30)
    else:
        app.permanent_session_lifetime = timedelta(hours=24)

# Restrict CORS to specific origins for security
allowed_origins = os.getenv("CORS_ORIGINS", "http://localhost:3001,http://localhost:3000").split(",")
CORS(app, origins=[origin.strip() for origin in allowed_origins], supports_credentials=True) # Enable CORS for Next.js (Port 3001)

# Register Dashboard API
app.register_blueprint(dashboard_bp)

# --- GLOBAL ERROR HANDLER (Safety Net) ---
@app.errorhandler(Exception)
def handle_global_error(e):
    """
    Catch-all for unhandled exceptions.
    Ensures the server never crashes silently and always returns JSON.
    """
    logger.critical(f"🔥 UNHANDLED EXCEPTION: {e}", exc_info=True)
    # Optional: Send Alert
    # send_critical_alert("Server 500 Error", str(e))
    return jsonify({
        "error": "Internal Server Error",
        "message": "Something went wrong, but we caught it.",
        "type": type(e).__name__
    }), 500

@app.errorhandler(404)
def handle_not_found(e):
    return jsonify({"error": "Resource not found"}), 404

# --- ROUTES ---ALTH CHECK ---
@app.route("/health")
def health():
    """Simple health check endpoint for smoke tests"""
    from execution import config
    return {
        "status": "ok",
        "safe_mode": config.SAFE_MODE,
        "twilio_configured": bool(config.TWILIO_ACCOUNT_SID and config.TWILIO_AUTH_TOKEN)
    }, 200

# --- DASHBOARD LOGIC ---
@app.route("/dashboard")
def dashboard():
    """Admin Dashboard to view logs"""
    # Load Queue from DB
    queue = get_all_sms()
            
    # Calc Stats
    stats = {"missed_calls": 0, "reminders": 0, "errors": 0}
    funnel = get_lead_funnel_stats()
    revenue_stats = get_revenue_stats()
    
    for msg in queue:
        body = msg['body'].lower()
        if "wrapped up" in body: stats['missed_calls'] += 1
        if "scheduled" in body: stats['reminders'] += 1
        if "failed" in msg['status']: stats['errors'] += 1
        
    # Queue is already sorted new->old, so we don't need to reverse it.
    return render_template('dashboard.html', queue=queue, stats=stats, funnel=funnel, revenue_stats=revenue_stats, queue_len=len(queue))

# --- CONFIGURATION (Loaded from execution/config.py) ---
ACCOUNT_SID = config.TWILIO_ACCOUNT_SID
AUTH_TOKEN = config.TWILIO_AUTH_TOKEN

# Mock Client (Safe if no keys present)
try:
    if not config.TWILIO_ACCOUNT_SID or not config.TWILIO_AUTH_TOKEN:
        logger.warning("⚠️  Running in MOCK mode (No Twilio Keys found)")
        twilio_client = None
    else:
        twilio_client = Client(config.TWILIO_ACCOUNT_SID, config.TWILIO_AUTH_TOKEN)
        logger.info("✅ Twilio Client Initialized")
except Exception as e:
    logger.error(f"Error initializing Twilio: {e}")
    twilio_client = None

# --- PILOT SHIELDING: CONFIGURATIONS ---
AUTO_REPLY_KEYWORDS = ['driving', 'away from my phone', 'auto-reply', 'out of office', 'unavailable', 'vacation']

MISSED_CALL_TEMPLATES = [
    "Hi, this is {business_name}'s automated assistant. We missed your call! Are you looking for emergency service or a standard quote?\nReply STOP to unsubscribe.",
    "Hello! This is {business_name}'s assistant. Sorry we missed you. Do you need emergency plumbing help or just a standard quote?\nReply STOP to unsubscribe.",
    "Hi there from {business_name}! We're busy helping another client. Are you needing emergency service right now or a standard quote?\nReply STOP to unsubscribe.",
    "Thanks for calling {business_name}. Our team is currently on a job. Are you looking for an emergency tech or a standard service quote?\nReply STOP to unsubscribe."
]

@app.route("/voice", methods=['GET', 'POST'])
@require_twilio_signature
def voice_handler():
    """
    Handles incoming voice calls from Twilio.
    
    This is the main entry point for all incoming phone calls. It determines
    the call routing based on business hours, tenant configuration, and caller
    type (mobile vs landline).
    
    Call Flow:
        1. Validates webhook input and checks idempotency
        2. Resolves tenant from phone number
        3. Determines business hours (daytime/evening/sleep)
        4. Looks up caller info (mobile/landline, caller name)
        5. Routes call:
           - Daytime/Evening: Rings plumber, falls back to SMS if missed
           - Sleep mode: SMS for mobile, voicemail for landline
           - Emergency mode: Option to press 1 for immediate connection
    
    Error Handling:
        - Wrapped in comprehensive try-catch
        - Returns valid TwiML even on errors
        - Queues failed webhooks for retry
        - Logs all errors with full context
    
    Returns:
        TwiML response (XML string) for Twilio to execute
    """
    # KILL SWITCH CHECK (early exit)
    if config.KILL_SWITCH:
        logger.warning("🛑 KILL SWITCH ACTIVE: Rejecting Incoming Call.")
        resp = VoiceResponse()
        resp.say("System is currently under maintenance. Please try again later.", voice='Polly.Matthew-Neural')
        resp.hangup()
        return str(resp), 200

    # WRAP ENTIRE HANDLER IN TRY-CATCH
    try:
        from execution.utils.database import record_webhook_processed
        from execution.utils.sms_engine import add_to_queue
        
        twilio = get_twilio_service()
        
        caller_number = request.values.get('From')
        to_number = request.values.get('To')
        call_sid = request.values.get('CallSid')
        
        # INPUT VALIDATION
        is_valid, error_msg = validate_webhook_input(caller_number, to_number, call_sid)
        if not is_valid:
            logger.error(f"Invalid voice webhook: {error_msg}")
            send_critical_alert("Invalid Voice Webhook", 
                f"Validation error: {error_msg}\nFrom: {caller_number}\nTo: {to_number}\nSID: {call_sid}")
            resp = VoiceResponse()
            resp.say("System error. Please try again later.", voice='Polly.Matthew-Neural')
            return str(resp), 200
        
        # IDEMPOTENCY CHECK WITH FALLBACK
        is_duplicate = False
        internal_id = None
        used_fallback = False
        
        if call_sid:
            is_duplicate, internal_id, used_fallback = check_webhook_processed_safe(call_sid)
            if used_fallback and not is_duplicate:
                queue_webhook_for_retry(call_sid, caller_number, to_number, '', 'voice')
                logger.info(f"Voice webhook queued for retry (DB unavailable): {call_sid}")
                resp = VoiceResponse()
                resp.say("Thank you. Please check your text messages.", voice='Polly.Matthew-Neural', language='en-US')
                return str(resp), 200
        
        if is_duplicate:
            logger.info(f"♻️  Duplicate webhook ignored: CallSid {call_sid}")
            resp = VoiceResponse()
            resp.say("Thank you. Please check your text messages.", voice='Polly.Matthew-Neural', language='en-US')
            return str(resp), 200
        
        # TENANT RESOLUTION WITH FALLBACK
        tenant, tenant_used_fallback = get_tenant_safe(to_number)
        
        if not tenant:
            logger.error(f"❌ Unknown Tenant for number '{to_number}'")
            send_critical_alert("Tenant Resolution Failed (Voice)", 
                f"Could not resolve tenant for {to_number}. CallSid: {call_sid}")
            resp = VoiceResponse()
            resp.say("System Configuration Error. Please contact support.", voice='Polly.Matthew-Neural')
            return str(resp), 200

        tenant_id = tenant['id']

        # AI KILL SWITCH
        ai_active = tenant.get('ai_active', 1)
        if not ai_active:
            plumber_phone = tenant.get('plumber_phone_number')
            logger.warning(f"🛑 AI INACTIVE: Forwarding Call from {caller_number} to {plumber_phone}")
            resp = VoiceResponse()
            resp.dial(plumber_phone)
            return str(resp), 200
        
        # Record webhook as processed (with error handling)
        if not internal_id:
            internal_id = str(uuid.uuid4())
        
        try:
            record_webhook_processed(call_sid, 'voice', tenant_id=tenant_id, internal_id=internal_id)
            if used_fallback:
                add_to_webhook_cache(call_sid, internal_id)
        except Exception as e:
            logger.warning(f"Failed to record voice webhook: {e}. Will retry async.")
            queue_webhook_for_retry(call_sid, caller_number, to_number, '', 'voice')
        
        # Tenant Rate Limit (non-blocking)
        try:
            if not check_tenant_rate_limit(tenant_id):
                resp = VoiceResponse()
                resp.say("Busy. Please try again later.", voice='Polly.Matthew-Neural')
                return str(resp), 429
        except Exception as e:
            logger.warning(f"Rate limit check failed: {e}. Allowing request (fail-open).")

        plumber_name = tenant['name']
        business_name = tenant.get('name', 'PlumberAI')
        plumber_phone = tenant['plumber_phone_number']
        emergency_mode = tenant.get('emergency_mode', 0)
        
        # Check if this is an "Emergency Press 1" event
        digits = request.values.get('Digits')
        if digits == '1' and emergency_mode:
            logger.info(f"🚨 EMERGENCY OVERRIDE: Connecting caller {caller_number} to {plumber_phone}")
            try:
                update_lead_intent(caller_number, 'emergency', tenant_id=tenant_id)
            except Exception as e:
                logger.warning(f"Failed to update lead intent: {e}")
            
            resp = VoiceResponse()
            resp.say("Connecting you to the plumber now. Please hold.", voice='Polly.Matthew-Neural', language='en-US')
            resp.dial(plumber_phone)
            return str(resp), 200
        
        # Answer with specific TwiML
        resp = VoiceResponse()
        
        # Timezone Logic
        tz_name = tenant.get('timezone', 'America/Los_Angeles')
        try:
            tz = pytz.timezone(tz_name)
        except:
            tz = pytz.timezone('America/Los_Angeles')
        
        local_time = datetime.now(tz)
        hour = local_time.hour
        
        # Log for debugging
        logger.info(f"📞 INCOMING CALL FROM: {mask_pii(caller_number)} TO: {to_number} (Tenant: {plumber_name})")
        
        start_hour = tenant.get('business_hours_start', 7)
        day_end_hour = tenant.get('business_hours_end', 17)
        evening_end_hour = tenant.get('evening_hours_end', 17) # Default to same if not set
        
        is_daytime = start_hour <= hour < day_end_hour
        is_evening = day_end_hour <= hour < evening_end_hour
        
        logger.info(f"🕒 Local Time: {local_time.strftime('%I:%M %p')} (Hour: {hour}) | Mode: {'Day' if is_daytime else 'Evening' if is_evening else 'Sleep'}")

        # --- PILOT POLISH: LANDLINE & CNAM LOOKUP ---
        # This determines if caller is on mobile (can SMS) or landline (voicemail only)
        # Error handling ensures lookup failure doesn't block call processing
        twilio = get_twilio_service()
        try:
            lookup = twilio.lookup_number(caller_number)
            line_type = lookup.get('line_type', 'mobile')
            caller_name = lookup.get('caller_name')
        except Exception as e:
            # If lookup fails, assume mobile to allow SMS fallback
            logger.warning(f"Lookup failed for {caller_number}: {e}. Assuming mobile.")
            lookup = {'line_type': 'mobile', 'caller_name': None}
            line_type = 'mobile'
            caller_name = None
        
        is_landline = line_type == 'landline'

        # --- LEAD & CONSENT (Always Record) ---
        # Pass bypass_check=True for inbound calls (system-initiated, valid consent)
        lead_id, _ = create_or_update_lead(caller_number, tenant_id=tenant_id, source="voice_inbound", bypass_check=True, name=caller_name)
        record_consent(
            phone=caller_number,
            consent_type='implied',
            consent_source='inbound_call',
            tenant_id=tenant_id,
            metadata={'CallSid': call_sid, 'to_number': to_number}
        )

        # Phase 1: The Missed Call SMS (Rotation for Deliverability)
        template = random.choice(MISSED_CALL_TEMPLATES)
        sms_body = template.format(business_name=business_name)

        if is_daytime:
            # DAYTIME: Ring Plumber for 15s -> If No Answer -> AI Intercept
            logger.info(f"☀️ BUSINESS HOURS: Ringing Plumber... Fallback to AI.")
            
            # 1. Try to Connect (Smart Dialing)
            # We use an action URL to determine if the call was actually answered
            # Pass machineDetection to handle voicemail appropriately 
            resp.dial(plumber_phone, timeout=15, action='/voice/status', method='POST', machineDetection='Enable')
            
            # Note: If action is set, TwiML processing stops here and waits for the callback.
            return str(resp)
            
        elif is_evening:
            # EVENING: Ring Plumber for 15s -> If No Answer -> AI Intercept
            logger.info(f"🌙 EVENING MODE: Ringing Plumber... Fallback to AI.")
            
            # Ring Plumber with Machine Detection enabled
            resp.dial(plumber_phone, timeout=15, action='/voice/status', method='POST', machineDetection='Enable')
            return str(resp)

        else:
            # NIGHT/SLEEP: 
            if emergency_mode:
                gather = Gather(input='dtmf', num_digits=1, timeout=5, action='/voice', method='POST')
                gather.say(f"Hi, you’ve reached {plumber_name}. We’re currently assisting another customer. I'm sending you a text right now so we can prioritize your request. Please check your mobile. If this is an emergency, press 1 to reach our on-call tech.", voice='Polly.Matthew-Neural', language='en-US')
                resp.append(gather)
                 # If they don't press 1, fall through
                resp.say("Thank you. Please check your text messages.", voice='Polly.Matthew-Neural', language='en-US')
                resp.hangup()
            else:
                # OPTION A: Standard Sleep Mode
                if is_landline:
                    resp.say(f"Hi, you’ve reached {plumber_name}. We’re currently assisting another customer. Since you are calling from a landline, please leave a message after the beep and we’ll call you back shortly.", voice='Polly.Matthew-Neural', language='en-US')
                    resp.record(action='/voice/voicemail', maxLength=60, finishOnKey='#')
                else:
                    resp.say(f"Hi, you’ve reached {plumber_name}. We’re currently assisting another customer. I'm sending you a text right now so we can prioritize your request. Please check your mobile.", voice='Polly.Matthew-Neural', language='en-US')
                resp.hangup()
            

        
        # 3. Create Lead and Queue the SMS Alert
        # We do NOT send SMS via Twilio Client here. We ADD TO QUEUE for the engine.
        # from execution.utils.sms_engine import add_to_queue # Removed redundant import
        
        # 3. Create Lead and Consent moved to top
        
        if caller_number and not is_landline:
            add_to_queue(caller_number, sms_body, external_id=call_sid, tenant_id=tenant_id)
            
            # 4. Notify the Plumber (Click-to-Call formatting)
            tenant_plumber_phone = tenant.get('plumber_phone_number')
            clean_name = caller_name or 'New Customer'
            alert_msg = f"🔔 ({plumber_name}) Lead Alert: Caught a missed call from {clean_name}. I have texted them back.\n\nClick to Call:\n{caller_number}"
            add_to_queue(tenant_plumber_phone, alert_msg, external_id=f"{call_sid}:plumber", tenant_id=tenant_id)
            
            # 5. LOG TO GOOGLE SHEET (If configured)
            # Use async/background processing to avoid blocking webhook
            sheet_id = tenant.get('google_sheet_id')
            if sheet_id:
                try:
                    from execution.utils.sheets_engine import append_lead_to_sheet
                    import threading
                    
                    def log_to_sheet_async():
                        try:
                            logger.info(f"📝 Logging Missed Call Lead to Sheet: {sheet_id}")
                            # We assume Intent="Inquiry", Status="New" for missed calls
                            success = append_lead_to_sheet(sheet_id, {
                                'name': caller_name or 'Unknown',
                                'phone': caller_number,
                                'message': f"(Missed Call - {line_type})",
                                'intent': 'Inquiry',
                                'status': 'New'
                            })
                            if not success:
                                logger.warning(f"⚠️ Sheet logging failed for missed call from {caller_number}")
                        except Exception as e:
                            logger.error(f"Failed to log to sheet: {e}")
                    
                    # Run in background thread to avoid blocking webhook
                    thread = threading.Thread(target=log_to_sheet_async)
                    thread.daemon = True
                    thread.start()
                except Exception as e:
                    logger.error(f"Failed to start sheet logging thread: {e}")

        return str(resp), 200
        
    except Exception as e:
        # CATCH-ALL: Never let exceptions escape
        logger.critical(f"CRITICAL: Voice handler crashed: {e}", exc_info=True)
        send_critical_alert("Voice Handler Crash", 
            f"Webhook crashed: {e}\n"
            f"From: {request.values.get('From')}\n"
            f"To: {request.values.get('To')}\n"
            f"CallSid: {request.values.get('CallSid')}")
        
        # Queue for retry
        try:
            from execution.utils.resilience import queue_webhook_for_retry
            queue_webhook_for_retry(
                request.values.get('CallSid'),
                request.values.get('From'),
                request.values.get('To'),
                '',
                'voice'
            )
        except Exception as e2:
            logger.error(f"Failed to queue webhook for retry: {e2}")
        
        # Always return valid TwiML
        resp = VoiceResponse()
        resp.say("System error. Please try again later.", voice='Polly.Matthew-Neural')
        return str(resp), 200



@app.route("/sms", methods=['GET', 'POST'])
@require_twilio_signature
def sms_handler():
    """
    Handles incoming SMS messages from customers.
    
    This endpoint processes all SMS replies from customers, including:
    - STOP/UNSUBSCRIBE requests (highest priority, works even if DB is down)
    - HELP requests (compliance requirement)
    - Emergency vs Standard classification
    - Review feedback (positive/negative)
    - General inquiries
    
    Message Processing Flow:
        1. Input validation (phone numbers, message SID)
        2. Idempotency check (prevent duplicate processing)
        3. Tenant resolution
        4. STOP processing (if detected)
        5. Classification (emergency vs standard)
        6. Response generation and queuing
        7. Lead state updates
        8. Sheet logging (async)
    
    Error Handling:
        - Comprehensive try-catch around entire handler
        - Always returns 200 OK to prevent Twilio retry storms
        - Queues failed webhooks for async retry
        - Logs all errors with full context
    
    Returns:
        TwiML MessagingResponse (XML string) - always returns 200 OK
    """
    # KILL SWITCH CHECK (early exit, no DB needed)
    if config.KILL_SWITCH:
        logger.warning("🛑 KILL SWITCH ACTIVE: Rejecting Incoming SMS.")
        from twilio.twiml.messaging_response import MessagingResponse
        return str(MessagingResponse()), 200

    # WRAP ENTIRE HANDLER IN TRY-CATCH
    try:
        from twilio.twiml.messaging_response import MessagingResponse
        from execution.utils.sms_engine import add_to_queue
        from execution.utils.database import record_webhook_processed, log_conversation_event, get_lead_by_phone, get_or_create_magic_token, insert_or_update_alert_buffer
        from execution.utils.constants import STOP_KEYWORDS, EMERGENCY_KEYWORDS
        import re
        
        # INPUT VALIDATION FIRST (before any DB calls)
        from_number = request.values.get('From')
        to_number = request.values.get('To')
        body = request.values.get('Body', '').strip()
        msg_sid = request.values.get('MessageSid')
        
        # 🛡️ BUG #15 FIX: INFINITE LOOP PREVENTION
        # Twilio sends status updates (sent, delivered, etc.) to the same webhook if configured.
        # We MUST ignore these, otherwise we might reply to a confirmation -> infinite loop.
        sms_status = request.values.get('SmsStatus')
        if sms_status in ['sent', 'delivered', 'undelivered', 'failed', 'queued', 'sending']:
            logger.info(f"Ignoring status update: {sms_status} for {msg_sid}")
            return str(MessagingResponse()), 200

        # Validate required fields
        # Validate required fields
        is_valid, error_msg = validate_webhook_input(from_number, to_number, msg_sid)
        if not is_valid:
            logger.error(f"Invalid webhook: {error_msg}. Headers: {dict(request.headers)}")
            send_critical_alert("Invalid Webhook Received", 
                f"Validation error: {error_msg}\nFrom: {from_number}\nTo: {to_number}\nSID: {msg_sid}")
            return str(MessagingResponse()), 200  # Return 200 to prevent retries
        
        # IDEMPOTENCY CHECK WITH FALLBACK
        is_duplicate = False
        internal_id = None
        used_fallback = False
        
        if msg_sid:
            is_duplicate, internal_id, used_fallback = check_webhook_processed_safe(msg_sid)
            if used_fallback and not is_duplicate:
                # DB unavailable - queue for async processing
                queue_webhook_for_retry(msg_sid, from_number, to_number, body, 'sms')
                logger.info(f"Webhook queued for retry (DB unavailable): {msg_sid}")
                return str(MessagingResponse()), 200  # Return OK to prevent retries
        
        if is_duplicate:
            logger.info(f"♻️  Duplicate webhook ignored: MessageSid {msg_sid} (already processed as {internal_id})")
            return str(MessagingResponse()), 200
        
        # TENANT RESOLUTION WITH FALLBACK
        tenant, tenant_used_fallback = get_tenant_safe(to_number)
        if not tenant:
            logger.error(f"Unknown tenant for {to_number}. Webhook: {msg_sid}")
            send_critical_alert("Tenant Resolution Failed", 
                f"Could not resolve tenant for {to_number}. This may indicate provisioning issue.")
            return str(MessagingResponse()), 200  # Return 200 to prevent retries
        
        tenant_id = tenant['id']
        business_name = tenant.get('name', 'PlumberAI')
        tenant_plumber_phone = tenant.get('plumber_phone_number')
        
        # Record webhook as processed (with error handling)
        if not internal_id:
            internal_id = str(uuid.uuid4())
        
        try:
            record_webhook_processed(msg_sid, 'sms', tenant_id=tenant_id, internal_id=internal_id)
            if used_fallback:
                # Also cache it for future reference
                add_to_webhook_cache(msg_sid, internal_id)
        except Exception as e:
            logger.warning(f"Failed to record webhook: {e}. Will retry async.")
            queue_webhook_for_retry(msg_sid, from_number, to_number, body, 'sms')
            # Continue processing - don't fail the request
        
        # Rate limiting (non-blocking)
        try:
            if not check_tenant_rate_limit(tenant_id):
                logger.warning(f"Rate limit exceeded for tenant {tenant_id}")
                return "Too Many Requests", 429
        except Exception as e:
            logger.warning(f"Rate limit check failed: {e}. Allowing request (fail-open).")
            # Fail-open for rate limiting
        
        logger.info(f"📩 INCOMING SMS from {mask_pii(from_number)}: {mask_pii(body)} (SID: {msg_sid}) Tenant: {tenant_id}")
        
        # CRITICAL: STOP PROCESSING (HIGHEST PRIORITY - works even if DB is down)
        body_lower = body.strip().lower() if body else ""
        is_stop = False
        stop_keyword = None
        
        # Check for exact matches first (fast path)
        if body_lower in STOP_KEYWORDS:
            is_stop = True
            stop_keyword = body_lower
        else:
            # Check for partial matches
            for pattern in STOP_KEYWORDS:
                if re.search(r'\b' + pattern + r'\b', body_lower):
                    is_stop = True
                    stop_keyword = pattern
                    break
        
        if is_stop:
            # ... existing STOP logic ...
            logger.warning(f"🚫 IMMEDIATE STOP detected: {mask_pii(from_number)} said '{body}'")
            process_stop_safe(from_number, tenant_id, stop_keyword.upper() if stop_keyword else "STOP")
            try:
                log_conversation_event(from_number, 'inbound', body, external_id=msg_sid, tenant_id=tenant_id)
            except Exception as e:
                logger.warning(f"Failed to log STOP event: {e}")
            resp = MessagingResponse()
            resp.message("You have been unsubscribed and will receive no further messages.")
            return str(resp), 200

        # BUG #3: AUTO-REPLY IMMUNITY (Bot-on-Bot loop prevention)
        if any(keyword in body_lower for keyword in AUTO_REPLY_KEYWORDS):
            logger.warning(f"🤖 AUTO-REPLY detected from {mask_pii(from_number)}: '{body}'. Killing response loop.")
            try:
                log_conversation_event(from_number, 'inbound', f"(Auto-Reply) {body}", external_id=msg_sid, tenant_id=tenant_id)
            except: pass
            return str(MessagingResponse()), 200
        
        # Lead State Machine: Log & Update (only if not STOP)
        try:
            log_conversation_event(from_number, 'inbound', body, external_id=msg_sid, tenant_id=tenant_id)
            record_consent(
                phone=from_number,
                consent_type='implied',
                consent_source='inbound_sms',
                tenant_id=tenant_id,
                metadata={'MessageSid': msg_sid, 'to_number': to_number}
            )
            update_lead_status(from_number, 'replied', tenant_id=tenant_id)
        except Exception as e:
            logger.error(f"Failed to update lead state: {e}. Continuing with message processing.")
            # Don't fail the request - log and continue
        
        except Exception as e:
            logger.error(f"Failed to update lead state: {e}. Continuing with message processing.")
            # Don't fail the request - log and continue
        
        # 🚨 CANCEL NUDGE (User Replied!)
        try:
            if cancel_pending_sms(f"nudge_{from_number}"):
                logger.info(f"✅ Cancelled pending Nudge for {from_number} (User Replied)")
        except Exception as e:
            logger.warning(f"Failed to cancel nudge: {e}")

        body_clean = body.lower().strip()
        
        # COMPLIANCE KEYWORDS (HELP / UNSTOP)
        help_keywords = ['help', 'info', 'aide']
        if body_clean in help_keywords:
            try:
                tenant_config = get_tenant_by_id(tenant_id)
                business_name = tenant_config.get('business_name', 'PlumberAI') if tenant_config else business_name
            except Exception as e:
                logger.warning(f"Failed to get tenant config for help: {e}")
            resp = MessagingResponse()
            resp.message(f"{business_name}: Text us anytime for service. Call for emergencies. Reply STOP to unsubscribe.")
            return str(resp), 200

        if body_clean in ['start', 'unstop']:
            try:
                from execution.utils.database import set_opt_out
                set_opt_out(from_number, False)
                record_consent(from_number, 'express', 'inbound_sms', tenant_id, metadata={'keyword': body})
            except Exception as e:
                logger.error(f"Failed to process UNSTOP: {e}")
            resp = MessagingResponse()
            return str(resp), 200
    
        # AI KILL SWITCH (Global Pause)
        ai_active = tenant.get('ai_active', 1)
        if not ai_active:
            logger.warning(f"🛑 AI INACTIVE: Forwarding SMS from {from_number} (Pass-through)")
            
            # Forward to Plumber
            try:
                fwd_body = f"Message from {from_number}:\n{body}"
                add_to_queue(tenant_plumber_phone, fwd_body, external_id=f"fwd_{msg_sid}", tenant_id=tenant_id)
            except Exception as e:
                logger.error(f"Failed to forward message: {e}")
            
            # Log to Sheet (async, non-blocking)
            sheet_id = tenant.get('google_sheet_id')
            if sheet_id:
                try:
                    from execution.utils.sheets_engine import append_lead_to_sheet
                    import threading
                    
                    lead_info = None
                    try:
                        lead_info = get_lead_by_phone(from_number, tenant_id)
                    except Exception as e:
                        logger.warning(f"Failed to get lead info: {e}")
                    
                    cust_name = lead_info.get('name', 'Unknown') if lead_info else 'Unknown'
                    
                    def log_killswitch_async():
                        try:
                            append_lead_to_sheet(sheet_id, {
                                'name': cust_name,
                                'phone': from_number,
                                'message': body,
                                'intent': 'Passthrough',
                                'status': 'Manual'
                            })
                        except Exception as e:
                            logger.error(f"Sheet Log Error (KillSwitch): {e}")
                    
                    t = threading.Thread(target=log_killswitch_async)
                    t.daemon = True
                    t.start()
                except Exception as e:
                    logger.error(f"Failed to start sheet logging thread: {e}")
                    
            return str(MessagingResponse()), 200

        
        # SMART REVIEW LOGIC
        clean_body = body.lower().strip()
        
        # POSITIVE FEEDBACK
        if clean_body in ['good', 'great', 'awesome', 'excellent', 'yes']:
            try:
                review_link = tenant.get('google_review_link')
                if review_link:
                    reply_msg = f"{business_name}: That's music to our ears! 🎵 It would help us SO much if you could leave that on Google: {review_link} \n\nThanks again!"
                    add_to_queue(from_number, reply_msg, external_id=f"{msg_sid}_review_link", tenant_id=tenant_id)
                    boss_msg = f"⭐ 5-STAR POTENTIAL: {from_number} said '{body}'. I sent them the link."
                    add_to_queue(tenant.get('plumber_phone_number'), boss_msg, tenant_id=tenant_id)
            except Exception as e:
                logger.error(f"Failed to process positive feedback: {e}")
            return str(MessagingResponse()), 200

        # NEGATIVE FEEDBACK
        if clean_body in ['bad', 'poor', 'terrible', 'horrible', 'no', 'worst']:
            try:
                reply_msg = f"{business_name}: I am so sorry to hear that. I have just alerted the owner directly, and he will be calling you shortly to make this right."
                add_to_queue(from_number, reply_msg, external_id=f"{msg_sid}_apology", tenant_id=tenant_id)
                boss_msg = f"🚨 NEGATIVE FEEDBACK: Customer says '{body}'.\n\nCall Now:\n{from_number}"
                add_to_queue(tenant.get('plumber_phone_number'), boss_msg, tenant_id=tenant_id)
            except Exception as e:
                logger.error(f"Failed to process negative feedback: {e}")
            return str(MessagingResponse()), 200
        
        # CLASSIFY REQUEST URGENCY using improved classification logic
        # This uses intelligent keyword matching with context awareness
        from execution.utils.classification import classify_from_sms
        
        try:
            classification = classify_from_sms(body, use_ai=False)  # Fast keyword-based
            is_urgent = classification.get('urgency') == 'emergency'
            confidence = classification.get('confidence', 0.5)
            reasoning = classification.get('reasoning', '')
            
            logger.info(f"📊 Classification: {classification.get('urgency')} (confidence: {confidence:.2f}) - {reasoning}")
        except Exception as e:
            # Fallback to simple keyword matching if classification fails
            logger.warning(f"Classification failed, using fallback: {e}")
            is_urgent = any(k in body.lower() for k in EMERGENCY_KEYWORDS)
            confidence = 0.5
        
        # Get lead info (with error handling)
        lead_info = None
        cust_name = 'Unknown'
        try:
            lead_info = get_lead_by_phone(from_number, tenant_id)
            cust_name = lead_info.get('name', 'Unknown') if lead_info else 'Unknown'
        except Exception as e:
            logger.warning(f"Failed to get lead info: {e}")
        
        if is_urgent:
            alert_header = "🚨 EMERGENCY"
            try:
                update_lead_intent(from_number, 'emergency', tenant_id=tenant_id)
            except Exception as e:
                logger.warning(f"Failed to update lead intent: {e}")
            
            # EMERGENCY RESPONSE (Phase 2 - No STOP)
            try:
                emerg_resp = f"{business_name}: ⚠️ Understood. I have flagged this as an EMERGENCY. I am paging the on-call plumber right now. Please hold tight."
                add_to_queue(from_number, emerg_resp, external_id=f"{msg_sid}_emerg_ack", tenant_id=tenant_id)
            except Exception as e:
                logger.error(f"Failed to send emergency response: {e}")

            # ESCALATION (Critical Alert - Click-to-Call)
            try:
                clean_name = cust_name if cust_name != 'Unknown' else 'New Customer'
                boss_alert = f"🚨 EMERGENCY LEADS: {clean_name} says: '{body}'\n\nTap to Dial:\n{from_number}"
                add_to_queue(tenant.get('plumber_phone_number'), boss_alert, external_id=f"{msg_sid}_boss_alert", tenant_id=tenant_id)
            except Exception as e:
                logger.error(f"Failed to send emergency alert: {e}")
            
            # LOG TO SHEET (Emergency - async)
            sheet_id = tenant.get('google_sheet_id')
            if sheet_id:
                try:
                    from execution.utils.sheets_engine import append_lead_to_sheet
                    import threading
                    
                    def log_emergency_async():
                        try:
                            append_lead_to_sheet(sheet_id, {
                                'name': cust_name,
                                'phone': from_number,
                                'message': body,
                                'intent': 'Emergency',
                                'status': 'Emergency'
                            })
                        except Exception as e:
                            logger.error(f"Sheet Log Error (Emergency): {e}")
                    
                    t = threading.Thread(target=log_emergency_async)
                    t.daemon = True
                    t.start()
                except Exception as e:
                    logger.error(f"Failed to start emergency sheet logging: {e}")
            
            return str(MessagingResponse()), 200

        # STANDARD SERVICE (Phase 2 - Click-to-Call)
        clean_name = cust_name if cust_name != 'Unknown' else 'New Customer'
        alert_msg = f"🔔 STANDARD SERVICE: Msg - '{body}'\nFrom: {clean_name}\n\nCall Now:\n{from_number}"
        
        # ALERT BUFFERING ("Anti-Annoyance")
        try:
            insert_or_update_alert_buffer(tenant_id, from_number, tenant_plumber_phone, alert_msg)
            logger.info(f"⏳ buffered alert for {from_number}")
        except Exception as e:
            logger.error(f"⚠️ Error buffering alert: {e}. Falling back to immediate send.")
            try:
                add_to_queue(tenant_plumber_phone, alert_msg, external_id=f"{msg_sid}_copy", tenant_id=tenant_id)
            except Exception as e2:
                logger.error(f"Failed to send alert: {e2}")
        
        # Acknowledgement to Customer (Phase 2 - No STOP)
        try:
            ack_body = f"Thanks! I've sent your details to {plumber_name}. We will get back to you shortly with a quote."
            add_to_queue(from_number, ack_body, external_id=f"{msg_sid}_ack", tenant_id=tenant_id)
        except Exception as e:
            logger.error(f"Failed to send acknowledgement: {e}")
        
        # LOG TO SHEET (Async to avoid blocking webhook)
        sheet_id = tenant.get('google_sheet_id')
        if sheet_id:
            try:
                from execution.utils.sheets_engine import append_lead_to_sheet
                import threading
                
                status = "Emergency" if is_urgent else "Inquiry"
                
                def log_to_sheet_async():
                    try:
                        success = append_lead_to_sheet(sheet_id, {
                            'name': cust_name,
                            'phone': from_number,
                            'message': body,
                            'intent': 'Emergency' if is_urgent else 'Inquiry',
                            'status': status
                        })
                        if not success:
                            logger.warning(f"⚠️ Sheet logging failed for SMS from {mask_pii(from_number)}")
                    except Exception as e:
                        logger.error(f"Sheet Log Error: {e}")
                
                thread = threading.Thread(target=log_to_sheet_async)
                thread.daemon = True
                thread.start()
            except Exception as e:
                logger.error(f"Failed to start sheet logging thread: {e}")

        return str(MessagingResponse()), 200
        
    except Exception as e:
        # CATCH-ALL: Never let exceptions escape
        logger.critical(f"CRITICAL: SMS handler crashed: {e}", exc_info=True)
        send_critical_alert("SMS Handler Crash", 
            f"Webhook crashed: {e}\n"
            f"From: {request.values.get('From')}\n"
            f"To: {request.values.get('To')}\n"
            f"SID: {request.values.get('MessageSid')}\n"
            f"Body: {request.values.get('Body', '')[:100]}")
        
        # Queue webhook for retry processing
        try:
            from execution.utils.resilience import queue_webhook_for_retry
            queue_webhook_for_retry(
                request.values.get('MessageSid'),
                request.values.get('From'),
                request.values.get('To'),
                request.values.get('Body', ''),
                'sms'
            )
        except Exception as e2:
            logger.error(f"Failed to queue webhook for retry: {e2}")
        
        # Always return 200 to prevent retry storms
        from twilio.twiml.messaging_response import MessagingResponse
        return str(MessagingResponse()), 200
    
@app.route("/sms/status", methods=['POST'])
@require_twilio_signature
def sms_status_handler():
    """
    Twilio Status Callback: Receives delivery status updates for sent SMS messages.
    Updates the message status in the database based on Twilio's status.
    Resilient version with error handling.
    """
    try:
        from execution.utils.security import mask_pii
        
        message_sid = request.values.get('MessageSid')
        message_status = request.values.get('MessageStatus')
        from_number = request.values.get('From')
        to_number = request.values.get('To')
        
        if not message_sid:
            logger.error("⚠️ SMS Status Callback: Missing MessageSid")
            return "Missing MessageSid", 400
        
        if not message_status:
            logger.error("⚠️ SMS Status Callback: Missing MessageStatus")
            return "Missing MessageStatus", 400
        
        # Log the status change
        status_display = {
            'delivered': '✅ SMS Delivered',
            'undelivered': '❌ SMS Undelivered (Blocked)',
            'failed': '❌ SMS Failed',
            'sent': '📤 SMS Sent',
            'queued': '⏳ SMS Queued'
        }.get(message_status.lower(), f'📊 SMS Status: {message_status}')
        
        logger.info(f"{status_display} | MessageSid: {message_sid} | From: {mask_pii(from_number)} | To: {mask_pii(to_number)}")
        
        # Update the message status in the database (with error handling)
        try:
            updated = update_sms_status_by_message_sid(message_sid, message_status)
            if updated:
                logger.info(f"✅ Updated SMS status in database for MessageSid: {message_sid}")
            else:
                logger.warning(f"⚠️ Could not find SMS in database for MessageSid: {message_sid} (may have been sent before tracking was added)")
        except Exception as e:
            logger.error(f"Failed to update SMS status: {e}")
            # Don't fail the webhook - Twilio expects 200 OK
        
        # Return 200 OK to acknowledge receipt (Twilio expects this)
        return "", 200
        
    except Exception as e:
        logger.error(f"Error in SMS status handler: {e}")
        # Always return 200 to prevent Twilio retries
        return "", 200

@app.route("/voice/voicemail", methods=['POST'])
@require_twilio_signature
def voicemail_handler():
    """
    Handles incoming voicemails from landline users.
    
    This endpoint is called when a landline caller leaves a voicemail message.
    It processes the recording, transcribes it (async), and alerts the plumber.
    
    Error Handling:
        - Wrapped in try-catch to prevent crashes
        - Returns 200 OK even on errors to prevent Twilio retries
        - Logs all errors for debugging
    """
    try:
        from execution.utils.database import log_conversation_event, update_lead_status, create_or_update_lead
        from execution.utils.sms_engine import add_to_queue
        from execution.utils.resilience import get_tenant_safe
        from execution.utils.transcription import transcribe_recording_async
        
        caller_number = request.values.get('From')
        to_number = request.values.get('To')
        recording_url = request.values.get('RecordingUrl')
        call_sid = request.values.get('CallSid')
        
        # Resolve tenant with error handling
        tenant, _ = get_tenant_safe(to_number)
        if not tenant:
            logger.error(f"Could not resolve tenant for voicemail: {to_number}")
            return str(VoiceResponse()), 200
             
        tenant_id = tenant['id']
        plumber_phone = tenant.get('plumber_phone_number')
        
        logger.info(f"🎙️ VOICEMAIL RECEIVED from {caller_number}: {recording_url}")
        
        # Create/update lead with error handling
        lead_id = None
        try:
            lead_id, _ = create_or_update_lead(caller_number, tenant_id=tenant_id, source="voice_voicemail", bypass_check=True)
        except Exception as e:
            logger.error(f"Failed to create lead for voicemail: {e}")
        
        # Log to DB with error handling
        try:
            log_conversation_event(caller_number, 'inbound', f"(Voicemail) {recording_url}", tenant_id=tenant_id)
            update_lead_status(caller_number, 'replied', tenant_id=tenant_id)
        except Exception as e:
            logger.error(f"Failed to log voicemail event: {e}")
        
        # TRANSCRIPTION: Process recording asynchronously (non-blocking)
        # This allows the webhook to return immediately while transcription happens in background
        if recording_url and call_sid:
            try:
                transcribe_recording_async(recording_url, call_sid, caller_number, tenant_id, lead_id)
                logger.info(f"🚀 Transcription queued for async processing: {call_sid}")
            except Exception as e:
                logger.error(f"Failed to queue transcription: {e}")
                # Continue - transcription failure shouldn't block voicemail processing
        
        # Alert Plumber with error handling
        if plumber_phone:
            try:
                alert_msg = f"🎙️ NEW VOICEMAIL: A landline customer left you a message.\nListen: {recording_url}\n\nReturn Call:\n{caller_number}"
                add_to_queue(plumber_phone, alert_msg, tenant_id=tenant_id)
            except Exception as e:
                logger.error(f"Failed to queue plumber alert: {e}")
            
        return str(VoiceResponse()), 200
        
    except Exception as e:
        # CATCH-ALL: Never let exceptions escape
        logger.critical(f"CRITICAL: Voicemail handler crashed: {e}", exc_info=True)
        send_critical_alert("Voicemail Handler Crash", 
            f"Webhook crashed: {e}\n"
            f"From: {request.values.get('From')}\n"
            f"RecordingUrl: {request.values.get('RecordingUrl')}")
        return str(VoiceResponse()), 200

@app.route("/unsubscribe", methods=['GET'])
@require_rate_limit  # Rate limit public unsubscribe endpoint to prevent abuse
def unsubscribe():
    """
    Public One-Click Unsubscribe (Protected by HMAC and Rate Limiting)
    """
    phone = request.args.get('phone')
    token = request.args.get('token')
    
    if not phone or not token:
        return "Invalid Request. Missing phone or token.", 400
        
    if not verify_unsubscribe_token(phone, token):
        return "Invalid Security Token.", 403
        
    set_opt_out(phone, True)
    revoke_consent(phone, reason="One-Click Link")
    
    return "<h1>Unsubscribed</h1><p>You have been successfully removed from our list.</p>"

@app.route("/voice/status", methods=['POST'])
@require_twilio_signature
def voice_status_handler():
    """
    Callback from <Dial> - Resilient version.
    If call was 'completed' (answered), do nothing.
    If 'busy', 'no-answer', 'failed', 'canceled', Trigger AI Fallback.
    """
    try:
        from execution.utils.sms_engine import add_to_queue
        from execution.utils.database import record_webhook_processed, create_or_update_lead, record_consent, get_db_connection
        from execution.utils.resilience import check_webhook_processed_safe, get_tenant_safe, queue_webhook_for_retry
        import uuid
        
        call_status = request.values.get('DialCallStatus')
        answered_by = request.values.get('AnsweredBy', 'unknown')
        to_number = request.values.get('To')
        caller_number = request.values.get('From')
        call_sid = request.values.get('CallSid')
        
        # IDEMPOTENCY CHECK WITH FALLBACK
        if call_sid:
            status_webhook_id = f"{call_sid}_status_{call_status}"
            is_duplicate, internal_id, used_fallback = check_webhook_processed_safe(status_webhook_id)
            if is_duplicate:
                logger.info(f"♻️  Duplicate webhook ignored: {status_webhook_id}")
                return str(VoiceResponse()), 200
        
        # RESOLVE TENANT WITH FALLBACK
        tenant, _ = get_tenant_safe(to_number)
        
        # Try 'From' Number if 'To' didn't work
        if not tenant:
            tenant, _ = get_tenant_safe(caller_number)
        
        # Try Plumber Phone lookup (fallback)
        if not tenant:
            try:
                conn = get_db_connection()
                if conn:
                    row = conn.execute("SELECT * FROM tenants WHERE plumber_phone_number = ?", (to_number,)).fetchone()
                    if not row:
                        row = conn.execute("SELECT * FROM tenants WHERE plumber_phone_number = ?", (caller_number,)).fetchone()
                    if row:
                        tenant = dict(row)
                    conn.close()
            except Exception as e:
                logger.error(f"DB Error resolving tenant in callback: {e}")

        if not tenant:
            logger.error(f"Could not resolve tenant in callback. To: {to_number}, From: {caller_number}")
            send_critical_alert(
                "Tenant Resolution Failed (Voice Status)",
                f"Voice status callback could not resolve tenant. To: {to_number}, From: {caller_number}, CallSid: {call_sid}"
            )
            return str(VoiceResponse()), 200  # Return OK to prevent retries
            
        tenant_id = tenant['id']
        plumber_name = tenant['name']
        
        logger.info(f"📞 DIAL STATUS: {call_status} for {plumber_name} (Tenant: {tenant_id})")
        
        if call_status == 'completed' or answered_by == 'human':
            logger.info(f"✅ Call Answered by {answered_by}. No AI needed.")
            try:
                if call_sid:
                    status_webhook_id = f"{call_sid}_status_{call_status}"
                    record_webhook_processed(status_webhook_id, 'voice_status', tenant_id=tenant_id, internal_id=f"completed_{call_sid}")
            except Exception as e:
                logger.warning(f"Failed to record completed webhook: {e}")
            return str(VoiceResponse()), 200
            
        # BUG #4: VOICEMAIL DETECTION (Machine Handling)
        if answered_by in ['machine_start', 'machine_end_beep', 'machine_end_silence', 'fax']:
            logger.warning(f"🤖 VOICEMAIL detected ({answered_by}). Skipping AI speech.")
        else:
            # Standard Greeting for humans/unknowns
            # Note: At this point we might not have is_landline if this is a dial callback, 
            # but we assume the initial handler already sorted it.
            resp.say(f"Hi, you’ve reached {plumber_name}. We’re currently assisting another customer. I'm sending you a text right now so we can prioritize your request. Please check your mobile.", voice='Polly.Matthew-Neural', language='en-US')
        
        resp.hangup()
        
        # Create Lead if not exists (with error handling)
        lead_id = None
        try:
            lead_id, _ = create_or_update_lead(caller_number, tenant_id=tenant_id, source="voice_missed", bypass_check=True)
            record_consent(caller_number, 'implied', 'inbound_call', tenant_id=tenant_id, metadata={'CallSid': call_sid})
        except Exception as e:
            logger.error(f"Failed to create lead/consent: {e}. Continuing with SMS.")
        
        # Missed Call SMS (Phase 1 Rotation)
        try:
            business_name = tenant.get('name', 'PlumberAI')
            template = random.choice(MISSED_CALL_TEMPLATES)
            sms_body = template.format(business_name=business_name)
            add_to_queue(caller_number, sms_body, external_id=f"{call_sid}_missed", tenant_id=tenant_id)
            
            # Notify the Plumber (Phase 1 Alert - Click-to-Call)
            tenant_plumber_phone = tenant.get('plumber_phone_number')
            if tenant_plumber_phone:
                alert_msg = f"🔔 ({plumber_name}) Missed Call: I've texted the customer to start the intake.\n\nReturn Call:\n{caller_number}"
                add_to_queue(tenant_plumber_phone, alert_msg, external_id=f"{call_sid}_alert", tenant_id=tenant_id)
        except Exception as e:
            logger.error(f"Failed to queue SMS: {e}")
        
        return str(resp), 200
        
    except Exception as e:
        logger.critical(f"CRITICAL: Voice status handler crashed: {e}", exc_info=True)
        send_critical_alert("Voice Status Handler Crash", 
            f"Webhook crashed: {e}\n"
            f"CallSid: {request.values.get('CallSid')}\n"
            f"Status: {request.values.get('DialCallStatus')}")
        return str(VoiceResponse()), 200

if __name__ == "__main__":
    logger.info(f"🔧 Plumber Agent Listening on Port 5002")
    # We use 5002 to avoid conflict with your Webhook Server (5001)
    app.run(port=5002)

