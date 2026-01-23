# PlumberAI SaaS - Technical Review Bundle

## 1. Architecture Overview
**PlumberAI** is a vertical SaaS solution for plumbing businesses. It provides an AI-powered "Dispatcher" that:
1.  **Intercepts Calls/SMS:** Uses Twilio webhooks to handle incoming communication.
2.  **Classifies Urgency:** Uses OpenAI (GPT-4o) + Regex keywords to detect "Emergency" vs "Standard" leads.
3.  **Auto-Responds:** Sends instant SMS acknowledgement to customers.
4.  **Dispatches:** Alerts the plumber via SMS/Phone Call if the lead is high-priority.
5.  **Dashboard:** A Next.js 13 (App Router) Real-time Dashboard for the plumber to view leads and revenue.

### Tech Stack
-   **Backend:** Python 3.10+, Flask, PostgreSQL (Production) / SQLite (Dev), Twilio SDK, OpenAI SDK.
-   **Queue:** Redis + RQ (Redis Queue) for background transcription jobs.
-   **Frontend:** Next.js 14, Tailwind CSS, SWR (polling), Lucide React.
-   **Infrastructure:** Kamatera VPS (Ubuntu 24.04), Nginx (reverse proxy + SSL), PM2 (process manager).
-   **Resilience:** "Ghost Bug" hardened (Global Error Handlers, Atomic DB locks, Retry Logic, Log Rotation).

### Production Infrastructure
| Component | Details |
|-----------|---------|
| **Server** | Kamatera VPS (78.138.17.193) |
| **Domain** | `api.yourplumberai.com` |
| **SSL** | Let's Encrypt (auto-renews, expires Apr 2026) |
| **Database** | PostgreSQL 16 (localhost:5432) |
| **Cache/Queue** | Redis (localhost:6379) |
| **Process Manager** | PM2 (plumber-web, plumber-worker) |
| **Reverse Proxy** | Nginx → Flask (127.0.0.1:5002) |
| **Firewall** | UFW (SSH + Nginx allowed, Postgres/Redis blocked) |
| **Backup** | Daily cron at 2am (7-day retention) |

### API Endpoints (Production)
| Endpoint | Purpose |
|----------|---------|
| `https://api.yourplumberai.com/health` | Health check (DB, queue status) |
| `https://api.yourplumberai.com/voice` | Twilio Voice webhook |
| `https://api.yourplumberai.com/sms` | Twilio SMS webhook |
| `https://api.yourplumberai.com/dashboard` | Admin dashboard |

## 2. Core Features
-   **Emergency AI Detection:** `classification.py` uses hybrid AI/Keyword logic.
-   **Voice Transcription:** `transcription.py` handles Voicemail -> Text -> AI Analysis (via RQ).
-   **Resilient SMS Engine:** `sms_engine.py` manages queueing, rate-limiting, and opt-outs.
-   **Business Health Suite:** Dashboard tracks Revenue, Leads, and AI Costs.
-   **Safety Net:** Global Error Boundaries (Frontend & Backend) prevent total crashes.

---

## 3. Source Code

### File: `execution/run_app.py`
```python
"""
PlumberAI Flask Application Entry Point

This is the main entry point for the Flask web server. It imports the 
configured app object from handle_incoming_call and runs the server.

Usage:
    python3 execution/run_app.py
    OR
    pm2 start execution/run_app.py --interpreter python3

Environment Variables:
    - PORT: Server port (default: 5002)
    - HOST: Server host (default: 127.0.0.1 for security)
"""

import os
import sys

# Fix PYTHONPATH for PM2 - find project root and add to path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)  # Go up from execution/
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from execution.handle_incoming_call import app

if __name__ == "__main__":
    port = int(os.getenv('PORT', 5002))
    host = os.getenv('HOST', '127.0.0.1')  # Bind to localhost only (nginx handles public)
    
    # Run in debug mode only if explicitly enabled
    debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    
    print(f"🚀 Starting PlumberAI Server on {host}:{port}")
    app.run(host=host, port=port, debug=debug)
```
---

### File: `execution/handle_incoming_call.py`
```python
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

@app.route("/voice/status", methods=['POST'])
@require_twilio_signature
def voice_status_handler():
    """
    Handles the callback from the <Dial> action.
    Determines if the plumber answered or if we missed the call.
    """
    resp = VoiceResponse()
    dial_status = request.values.get('DialCallStatus')
    call_sid = request.values.get('CallSid')
    caller_number = request.values.get('From')
    to_number = request.values.get('To')
    
    tenant, _ = get_tenant_safe(to_number)
    tenant_id = tenant['id'] if tenant else None
    
    logger.info(f"📞 Dial Status for {call_sid}: {dial_status}")
    
    if dial_status in ['completed', 'answered']:
        # Call was connected successfully. No Nudge needed.
        logger.info(f"✅ Call connected successfully to plumber.")
        resp.hangup()
        return str(resp), 200
        
    else:
        # Call was NOT connected (busy, no-answer, failed, machine)
        logger.info(f"⚠️ Call NOT connected ({dial_status}). Triggering AI Fallback & Nudge.")
        
        # 1. Immediate Text (if mobile)
        # This duplicates/reinforces the initial text, but customizes it for "Missed"
        # Actually, let's rely on the Nudge for the follow-up, 
        # but we should ensure the user knows we are texting.
        
        resp.say("Sorry we missed you. I'm texting you right now to see how we can help.", voice='Polly.Matthew-Neural')
        resp.hangup()
        
        # 2. Schedule Nudge (Audio Fallback) - 2 Minutes Delay
        # "Hi, sorry we missed your call! I'm just an AI assistant, but if you text me back here I can get you a quote immediately."
        nudge_body = "Hi! Sorry we missed your call just now. I can help you schedule service or get a quote right here via text. How can I help?"
        
        # Determine if landline or mobile (re-lookup or assume mobile for now)
        # Actually, if we are here, we might have already checked landline in voice_handler.
        # But we don't have that state easily. 
        # We'll queue the nudge. If it's a landline, the SMS engine might fail or convert to voice (TBD), but usually we assume mobile for SMS queue.
        
        # 2. Schedule Nudge (Audio Fallback) - 2 Minutes Delay
        # "Hi, sorry we missed your call! I'm just an AI assistant, but if you text me back here I can get you a quote immediately."
        nudge_body = "Hi! Sorry we missed your call just now. I can help you schedule service or get a quote right here via text. How can I help?"
        
        # Determine if landline or mobile (re-lookup or assume mobile for now)
        # Actually, if we are here, we might have already checked landline in voice_handler.
        # But we don't have that state easily. 
        # We'll queue the nudge. If it's a landline, the SMS engine might fail or convert to voice (TBD), but usually we assume mobile for SMS queue.
        
        from execution.utils.sms_engine import add_to_queue
        # Use delay of 120 seconds (2 mins)
        add_to_queue(caller_number, nudge_body, external_id=f"nudge_{caller_number}", tenant_id=tenant_id, delay_seconds=120)
        
        return str(resp), 200

@app.route("/voice/voicemail", methods=['POST'])
@require_twilio_signature
def voicemail_handler():
    """
    Handles the recording callback from <Record>.
    Triggers async transcription (Whisper/Twilio).
    """
    resp = VoiceResponse()
    recording_url = request.values.get('RecordingUrl')
    call_sid = request.values.get('CallSid')
    caller_number = request.values.get('From')
    to_number = request.values.get('To')
    
    tenant, _ = get_tenant_safe(to_number)
    tenant_id = tenant['id'] if tenant else None
    
    if recording_url:
        logger.info(f"📼 Voicemail recorded for {caller_number}: {recording_url}")
        
        # Trigger Async Transcription (Supports Whisper via transcription.py update)
        try:
            from execution.utils.transcription import transcribe_recording_async
            # Resolving lead_id might be duplicate work, let the async func handle or pass None
            transcribe_recording_async(recording_url, call_sid, caller_number, tenant_id)
        except Exception as e:
            logger.error(f"Failed to trigger transcription: {e}")
            
        resp.hangup()
    else:
        logger.warning(f"⚠️ Voicemail handler called but no RecordingUrl (CallSid: {call_sid})")
        resp.hangup()
        
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

```
---

### File: `execution/utils/database.py`
```python
import sqlite3
import json
import os
import time
import uuid
from datetime import datetime
import contextlib
from execution.utils.logger import setup_logger

logger = setup_logger("Database")

# Define DB Path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Default Path (fallback)
DEFAULT_DB_PATH = os.path.join(BASE_DIR, 'data', 'plumber.db')
DATA_DIR = os.path.join(BASE_DIR, 'data')

# --- POSTGRES SUPPORT WRAPPER ---
try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

class PostgresCursorWrapper:
    def __init__(self, cursor):
        self.cursor = cursor
        
    def execute(self, query, params=()):
        # Convert SQLite ? placeholders to Postgres %s
        pg_query = query.replace('?', '%s')
        return self.cursor.execute(pg_query, params)
            
    def __getattr__(self, name):
        return getattr(self.cursor, name)
        
    def __iter__(self):
        return iter(self.cursor)

class PostgresConnectionWrapper:
    def __init__(self, dsn):
        # Use DictCursor to emulate sqlite3.Row (access by name)
        self.conn = psycopg2.connect(dsn, cursor_factory=psycopg2.extras.DictCursor)
        self.row_factory = None
        
    def execute(self, query, params=()):
        cursor = self.cursor()
        cursor.execute(query, params)
        return cursor
        
    def cursor(self):
        return PostgresCursorWrapper(self.conn.cursor())
        
    def commit(self):
        self.conn.commit()
    
    def rollback(self):
        self.conn.rollback()
        
    def close(self):
        self.conn.close()

# --------------------------------


def get_db_connection():
    """
    Gets database connection with retry logic.
    Supports both SQLite (local) and Postgres (production).
    """
    # 1. TRY POSTGRES (Production)
    db_url = os.getenv('DATABASE_URL')
    if db_url and 'postgresql' in db_url:
        if not psycopg2:
             logger.warning("DATABASE_URL set but psycopg2 not installed. Falling back to SQLite.")
        else:
            try:
                return PostgresConnectionWrapper(db_url)
            except Exception as e:
                logger.error(f"❌ Failed to connect to Postgres: {e}. Falling back to SQLite.")

    # 2. FALLBACK TO SQLITE (Local/Dev)
    db_path = os.getenv('PLUMBER_DB_PATH', DEFAULT_DB_PATH)
    directory = os.path.dirname(db_path)
    if directory and db_path != ":memory:":
        os.makedirs(directory, exist_ok=True)
    
    attempts = 0
    max_attempts = 3
    base_delay = 0.1
    
    while attempts < max_attempts:
        try:
            conn = sqlite3.connect(db_path, timeout=30.0)
            conn.row_factory = sqlite3.Row
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA busy_timeout=30000")
            except Exception:
                pass
            return conn
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempts < max_attempts - 1:
                attempts += 1
                delay = min(base_delay * (2 ** attempts), 2.0)
                time.sleep(delay)
                continue
            else:
                logger.error(f"Database connection failed: {e}")
                raise e
    
    raise sqlite3.OperationalError(f"Failed to connect to database after {max_attempts} attempts: {db_path}")

@contextlib.contextmanager
def get_db_cursor(commit=False):
    """
    🛡️ BUG #14 FIX: Zombie Processes (Context Manager)
    Ensures connection is ALWAYS closed, even on error.
    Usage:
        with get_db_cursor(commit=True) as (conn, cursor):
            cursor.execute(...)
    """
    conn = get_db_connection()
    try:
        yield conn, conn.cursor()
        if commit:
            conn.commit()
    except Exception:
        if commit:
            conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Validates that tables exist, creates them if not."""
    conn = get_db_connection()
    c = conn.cursor()
    
    # 1. Create JOBS Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT,
            client_id TEXT NOT NULL,
            customer_name TEXT,
            customer_phone TEXT,
            job_date TEXT,
            status TEXT DEFAULT 'scheduled',
            notes TEXT
        )
    ''')
    
    # 1.5 Create TENANTS Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS tenants (
            id TEXT PRIMARY KEY,
            name TEXT,
            twilio_phone_number TEXT UNIQUE, -- The key to identify tenant
            plumber_phone_number TEXT,
            timezone TEXT DEFAULT 'America/Los_Angeles',
            business_hours_start INTEGER DEFAULT 7,
            business_hours_end INTEGER DEFAULT 19,
            created_at TIMESTAMP,
            emergency_mode BOOLEAN DEFAULT 0,
            evening_hours_end INTEGER DEFAULT 19 -- Default same as business_end if not used
        )
    ''')

    # 2. Create SMS_QUEUE Table (Moved Up for Migration Safety)
    c.execute('''
        CREATE TABLE IF NOT EXISTS sms_queue (
            id TEXT PRIMARY KEY,
            tenant_id TEXT,
            external_id TEXT UNIQUE,
            to_number TEXT NOT NULL,
            body TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            last_attempt TEXT,
            created_at TEXT,
            sent_at TEXT
        )
    ''')

    # 3. Create OTP Codes Table (Login)
    c.execute('''
        CREATE TABLE IF NOT EXISTS otp_codes (
            phone TEXT PRIMARY KEY,
            code TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            attempts INTEGER DEFAULT 0,
            created_at TEXT
        )
    ''')

    
    # 1.6 Migration for Schedule
    try:
        cursor = c.execute("PRAGMA table_info(tenants)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'evening_hours_end' not in columns:
            logger.info("🔧 Migrating DB: Adding schedule cols to tenants...")
            c.execute("ALTER TABLE tenants ADD COLUMN evening_hours_end INTEGER DEFAULT 19")
    except Exception as e:
        logger.warning(f"⚠️ Migration warning (schedule): {e}")

    # 1.6.5 Migration for Average Job Value (Revenue Metric)
    try:
        cursor = c.execute("PRAGMA table_info(tenants)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'average_job_value' not in columns:
            logger.info("🔧 Migrating DB: Adding average_job_value col to tenants...")
            c.execute("ALTER TABLE tenants ADD COLUMN average_job_value INTEGER DEFAULT 350")
    except Exception as e:
        logger.warning(f"⚠️ Migration warning (average_job_value): {e}")

    # 1.7 Migration for Calendar ID
    try:
        cursor = c.execute("PRAGMA table_info(tenants)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'calendar_id' not in columns:
            logger.info("🔧 Migrating DB: Adding calendar_id col to tenants...")
            c.execute("ALTER TABLE tenants ADD COLUMN calendar_id TEXT")
    except Exception as e:
        logger.warning(f"⚠️ Migration warning (calendar_id): {e}")
        
    # 1.8 Migration for Review Link
    try:
        cursor = c.execute("PRAGMA table_info(tenants)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'google_review_link' not in columns:
            logger.info("🔧 Migrating DB: Adding google_review_link col to tenants...")
            c.execute("ALTER TABLE tenants ADD COLUMN google_review_link TEXT")
    except Exception as e:
        logger.warning(f"⚠️ Migration warning (google_review_link): {e}")
    
    # 1.9 Migration for Twilio MessageSid
    try:
        cursor = c.execute("PRAGMA table_info(sms_queue)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'twilio_message_sid' not in columns:
            logger.info("🔧 Migrating DB: Adding twilio_message_sid col to sms_queue...")
            c.execute("ALTER TABLE sms_queue ADD COLUMN twilio_message_sid TEXT")
            c.execute("CREATE INDEX IF NOT EXISTS idx_sms_queue_twilio_sid ON sms_queue(twilio_message_sid)")
    except Exception as e:
        logger.warning(f"⚠️ Migration warning (twilio_message_sid): {e}")

    # 1.9.5 Migration for locked_at (Atomic Worker Claiming)
    try:
        cursor = c.execute("PRAGMA table_info(sms_queue)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'locked_at' not in columns:
            logger.info("🔧 Migrating DB: Adding locked_at col to sms_queue...")
            c.execute("ALTER TABLE sms_queue ADD COLUMN locked_at TEXT")
            c.execute("CREATE INDEX IF NOT EXISTS idx_sms_queue_locked_at ON sms_queue(locked_at)")
    except Exception as e:
        logger.warning(f"⚠️ Migration warning (locked_at): {e}")

    # 1.9.6 Migration for scheduled_for (Message Scheduling)
    try:
        cursor = c.execute("PRAGMA table_info(sms_queue)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'scheduled_for' not in columns:
            logger.info("🔧 Migrating DB: Adding scheduled_for col to sms_queue...")
            c.execute("ALTER TABLE sms_queue ADD COLUMN scheduled_for TEXT")
            c.execute("CREATE INDEX IF NOT EXISTS idx_sms_queue_scheduled_for ON sms_queue(scheduled_for)")
    except Exception as e:
        logger.warning(f"⚠️ Migration warning (scheduled_for): {e}")



    # 1.10 Migration for Google Sheet ID
    try:
        cursor = c.execute("PRAGMA table_info(tenants)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'google_sheet_id' not in columns:
            logger.info("🔧 Migrating DB: Adding google_sheet_id col to tenants...")
            c.execute("ALTER TABLE tenants ADD COLUMN google_sheet_id TEXT")
    except Exception as e:
        logger.warning(f"⚠️ Migration warning (google_sheet_id): {e}")

    # 1.11 Migration for Business Health Suite (Resilience)
    try:
        cursor = c.execute("PRAGMA table_info(tenants)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Onboarding Funnel
        if 'onboarding_step' not in columns:
            logger.info("🔧 Migrating DB: Adding onboarding_step to tenants...")
            c.execute("ALTER TABLE tenants ADD COLUMN onboarding_step TEXT DEFAULT 'signup'")
            
        # Subscription Status (Involuntary Churn)
        if 'subscription_status' not in columns:
            logger.info("🔧 Migrating DB: Adding subscription_status to tenants...")
            c.execute("ALTER TABLE tenants ADD COLUMN subscription_status TEXT DEFAULT 'active'")
            
        # Financial Visibility
        if 'estimated_cost' not in columns:
            logger.info("🔧 Migrating DB: Adding estimated_cost to tenants...")
            c.execute("ALTER TABLE tenants ADD COLUMN estimated_cost REAL DEFAULT 0.0")
            
    except Exception as e:
        logger.warning(f"⚠️ Migration warning (Health Suite): {e}")
    
    # 3. Create LEADS Table
    # 3. Create LEADS Table
    c.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id TEXT PRIMARY KEY,
            tenant_id TEXT,
            phone TEXT NOT NULL,
            status TEXT DEFAULT 'new',
            priority INTEGER DEFAULT 1,
            opt_out INTEGER DEFAULT 0,
            created_at TEXT,
            last_contact_at TEXT,
            notes TEXT,
            quality_score INTEGER DEFAULT 0,
            intent TEXT, -- 'emergency', 'service', 'inquiry'
            summary TEXT,
            name TEXT,
            magic_token TEXT
        )
    """)

    # 1.10 Migration for Magic Token in Leads
    try:
        # Check if column exists using PRAGMA - Safer for concurrency
        cursor = c.execute("PRAGMA table_info(leads)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'magic_token' not in columns:
            logger.info("🔧 Migrating DB: Adding magic_token col to leads...")
            c.execute("ALTER TABLE leads ADD COLUMN magic_token TEXT")
    except Exception as e:
        logger.warning(f"⚠️ Migration warning (magic_token): {e}")

    # 1.11 Migration for Name in Leads
    try:
        cursor = c.execute("PRAGMA table_info(leads)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'name' not in columns:
            logger.info("🔧 Migrating DB: Adding name col to leads...")
            c.execute("ALTER TABLE leads ADD COLUMN name TEXT")
    except Exception as e:
        logger.warning(f"⚠️ Migration warning (name): {e}")
    
    # 3.5 Migration for Quality Columns
    try:
        cursor = c.execute("PRAGMA table_info(leads)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'quality_score' not in columns:
            logger.info("🔧 Migrating DB: Adding quality_score col to leads...")
            c.execute("ALTER TABLE leads ADD COLUMN quality_score INTEGER DEFAULT 0")
        if 'intent' not in columns:
            logger.info("🔧 Migrating DB: Adding intent col to leads...")
            c.execute("ALTER TABLE leads ADD COLUMN intent TEXT")
        if 'summary' not in columns:
            logger.info("🔧 Migrating DB: Adding summary col to leads...")
            c.execute("ALTER TABLE leads ADD COLUMN summary TEXT")
    except Exception as e:
        logger.warning(f"⚠️ Migration warning (quality cols): {e}")

    # 4. Create CONVERSATION_LOGS Table
    c.execute("""
        CREATE TABLE IF NOT EXISTS conversation_logs (
            id TEXT PRIMARY KEY,
            lead_id TEXT,
            direction TEXT, -- inbound, outbound
            body TEXT,
            external_id TEXT,
            created_at TEXT,
            FOREIGN KEY (lead_id) REFERENCES leads (id)
        )
    """)
    
    # 4.5 Create ALERT BUFFER Table (Anti-Annoyance)
    # This table holds messages for 30 seconds to group them before alerting the plumber.
    c.execute('''
        CREATE TABLE IF NOT EXISTS alert_buffer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT,
            customer_phone TEXT,
            plumber_phone TEXT,
            messages_text TEXT,
            message_count INTEGER DEFAULT 1,
            send_at TIMESTAMP,
            created_at TIMESTAMP,
            UNIQUE(tenant_id, customer_phone)
        )
    ''')

    # 4.6 Create WEBHOOK_EVENTS Table (Idempotency)
    # Tracks processed webhooks by provider ID (MessageSid, CallSid) to prevent duplicate processing
    c.execute('''
        CREATE TABLE IF NOT EXISTS webhook_events (
            id TEXT PRIMARY KEY,
            provider_id TEXT UNIQUE NOT NULL, -- Twilio MessageSid or CallSid
            webhook_type TEXT NOT NULL, -- 'sms', 'voice', 'voice_status'
            tenant_id TEXT,
            processed_at TEXT NOT NULL,
            internal_id TEXT -- Our internal message/event ID
        )
    ''')
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_webhook_events_provider_id ON webhook_events(provider_id)')
    
    # 4.6.5 Create index on alert_buffer for efficient queries
    c.execute('CREATE INDEX IF NOT EXISTS idx_alert_buffer_send_at ON alert_buffer(send_at)')
    
    # 4.7 Create RATE_LIMITS Table
    c.execute('''
        CREATE TABLE IF NOT EXISTS rate_limits (
            key TEXT PRIMARY KEY,
            count INTEGER DEFAULT 0,
            reset_at REAL
        )
    ''')
    
    # PERFORMANCE INDEXES
    c.execute("CREATE INDEX IF NOT EXISTS idx_sms_queue_status_created ON sms_queue(status, created_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_webhook_processed ON webhook_events(processed_at)")

    # 5. Create CONSENT_RECORDS Table (CASL Compliance - Canada's Anti-Spam Legislation)
    # This table stores proof of consent for every lead, required by CRTC for regulatory audits.
    c.execute("""
        CREATE TABLE IF NOT EXISTS consent_records (
            id TEXT PRIMARY KEY,
            lead_id TEXT NOT NULL,
            tenant_id TEXT,
            phone TEXT NOT NULL,
            consent_type TEXT NOT NULL, -- 'implied' (they called us) or 'express' (form submission)
            consent_source TEXT NOT NULL, -- 'inbound_call', 'inbound_sms', 'web_form', 'manual'
            ip_address TEXT, -- Required for web form consent (CASL proof)
            user_agent TEXT, -- Browser/device info for web forms
            form_url TEXT, -- URL of form if applicable
            consent_text TEXT, -- The exact text they agreed to
            consented_at TEXT NOT NULL, -- ISO 8601 timestamp
            expires_at TEXT, -- Implied consent expires after 2 years per CASL
            revoked_at TEXT, -- When they opted out
            revocation_reason TEXT, -- 'STOP', 'unsubscribe', etc.
            metadata TEXT, -- JSON for additional context (CallSid, MessageSid, etc.)
            FOREIGN KEY (lead_id) REFERENCES leads (id)
        )
    """)
    
    # Simple migration: Checking if external_id exists, if not add it
    try:
        cursor = c.execute("PRAGMA table_info(sms_queue)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'external_id' not in columns:
            logger.info("🔧 Migrating DB: Adding external_id col to sms_queue...")
            c.execute("ALTER TABLE sms_queue ADD COLUMN external_id TEXT")
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sms_queue_external_id ON sms_queue(external_id)")
    except Exception as e:
        logger.warning(f"⚠️ Migration warning (external_id): {e}")

    # Migration for OPT_OUT
    try:
        cursor = c.execute("PRAGMA table_info(leads)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'opt_out' not in columns:
            logger.info("🔧 Migrating DB: Adding opt_out col to leads...")
            c.execute("ALTER TABLE leads ADD COLUMN opt_out INTEGER DEFAULT 0")
    except Exception as e:
        logger.warning(f"⚠️ Migration warning (opt_out): {e}")
    
    # Migration for TENANT_ID (Ensure each table has it)
    tables = ['sms_queue', 'leads', 'conversation_logs', 'jobs']
    for t in tables:
        try:
            # Check if column exists
            cursor = conn.execute(f"PRAGMA table_info({t})")
            columns = [row[1] for row in cursor.fetchall()]
            if 'tenant_id' not in columns:
                logger.info(f"🔧 Migrating DB: Adding tenant_id col to {t}...")
                conn.execute(f"ALTER TABLE {t} ADD COLUMN tenant_id TEXT")
        except Exception as e:
            logger.warning(f"⚠️  Migration failed for table {t}: {e}")

    # Migration for conversation_logs UNIQUE index (Idempotency)
    try:
        # Check if index exists or just try to create it (IF NOT EXISTS is safe)
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_conversation_logs_external_id ON conversation_logs(external_id)")
    except Exception as e:
        logger.warning(f"⚠️  Migration failed for conversation_logs index: {e}")

    # Migration for leads UNIQUE constraint (phone, tenant_id) - Multi-tenant support
    # This allows the same phone number to exist for different tenants
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_leads_phone_tenant ON leads(phone, tenant_id)")
    except Exception as e:
        logger.warning(f"⚠️  Migration failed for leads unique index: {e}")

    # Create Default Tenant if Empty
    c.execute("SELECT count(*) FROM tenants")
    if c.fetchone()[0] == 0:
        create_default_tenant_internal(conn)
        
    conn.commit()
    conn.close()

def get_all_tenants():
    """Returns all provisioned tenants."""
    conn = get_db_connection()
    rows = conn.execute("SELECT * FROM tenants").fetchall()
    conn.close()
    return [dict(ix) for ix in rows]

def create_default_tenant_internal(conn):
    try:
        from execution import config
        tid = str(uuid.uuid4())
        now = datetime.now().isoformat()
        # Fallbacks
        t_phone = config.TWILIO_PHONE_NUMBER or "+15550000000"
        p_phone = config.PLUMBER_PHONE_NUMBER or "+15551234567"
        
        conn.execute("""
            INSERT INTO tenants (id, name, twilio_phone_number, plumber_phone_number, timezone, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (tid, "Default Plumber", t_phone, p_phone, config.TIMEZONE, now))
        logger.info(f"✅ Created Default Tenant ({t_phone}) -> {tid}")
    except Exception as e:
        logger.warning(f"⚠️ Failed to create default tenant: {e}")
    
    # Run Migration if needed (using same connection)
    migrate_json_to_sqlite(conn)

def migrate_json_to_sqlite(conn=None):
    """One-time migration from JSON files to SQLite."""
    should_close = False
    if not conn:
        conn = get_db_connection()
        should_close = True
    c = conn.cursor()
    
    try:
        # --- Migrate Jobs ---
        # Check if table exists first to avoid crash
        try:
            c.execute("SELECT count(*) FROM jobs")
        except:
            return # Jobs table doesn't exist yet, skip migration

        if c.fetchone()[0] == 0:
            json_path = os.path.join(DATA_DIR, 'jobs_db.json')
            if os.path.exists(json_path):
                logger.info("📦 Migrating jobs_db.json to SQLite...")
                with open(json_path, 'r') as f:
                    jobs = json.load(f)
                    for job in jobs:
                        # Check if ID is integer (it is in structure)
                        # We interpret job['id'] as the primary key.
                        try:
                            c.execute("""
                                INSERT INTO jobs (id, client_id, customer_name, customer_phone, job_date, status, notes)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, (
                                job.get('id'), 
                                job.get('client_id'), 
                                job.get('customer_name'), 
                                job.get('customer_phone'), 
                                job.get('job_date'), 
                                job.get('status'), 
                                job.get('notes')
                            ))
                        except sqlite3.IntegrityError:
                            pass # specific ID already exists
            logger.info("✅ Jobs migrated.")
            
        # --- Migrate Queue ---
        # Check if table exists
        try:
            c.execute("SELECT count(*) FROM sms_queue")
        except:
            return 
            
        if c.fetchone()[0] == 0:
            json_path = os.path.join(DATA_DIR, 'sms_queue.json')
            if os.path.exists(json_path):
                logger.info("📦 Migrating sms_queue.json to SQLite...")
                with open(json_path, 'r') as f:
                    queue = json.load(f)
                    for msg in queue:
                        try:
                            c.execute("""
                                INSERT INTO sms_queue (id, to_number, body, status, attempts, last_attempt, created_at, sent_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                msg.get('id'),
                                msg.get('to'), # Note: JSON uses 'to', Schema uses 'to_number'
                                msg.get('body'),
                                msg.get('status'),
                                msg.get('attempts'),
                                msg.get('last_attempt'),
                                msg.get('created_at'),
                                msg.get('sent_at') 
                            ))
                        except sqlite3.IntegrityError:
                            pass
            logger.info("✅ SMS Queue migrated.")

        if should_close:
            conn.commit()
    finally:
        if should_close:
            conn.close()

# --- JOB ACCESSORS ---

def get_all_jobs():
    conn = get_db_connection()
    jobs = conn.execute('SELECT * FROM jobs').fetchall()
    conn.close()
    return [dict(ix) for ix in jobs]

def add_job(client_id, customer_name, customer_phone, job_date, notes):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("""
        INSERT INTO jobs (client_id, customer_name, customer_phone, job_date, notes)
        VALUES (?, ?, ?, ?, ?)
    """, (client_id, customer_name, customer_phone, job_date, notes))
    conn.commit()
    conn.close()

def get_tenant_by_twilio_number(twilio_number):
    """
    Finds the tenant config based on the INCOMING phone number (To).
    Optimized to use SQL WHERE clause instead of Python loop.
    """
    if not twilio_number: 
        return None
    
    # Normalize: strip spaces and leading +
    clean_num = str(twilio_number).strip().lstrip('+')
    
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        # Try exact match first
        row = conn.execute(
            "SELECT * FROM tenants WHERE twilio_phone_number = ? OR twilio_phone_number = ? OR twilio_phone_number = ?",
            (twilio_number, clean_num, f"+{clean_num}")
        ).fetchone()
        
        if row:
            return dict(row)
        
        # Fallback: Check all tenants with normalization (for edge cases)
        rows = conn.execute("SELECT * FROM tenants").fetchall()
        for db_row in rows:
            db_num = str(db_row['twilio_phone_number']).strip().lstrip('+')
            if db_num == clean_num:
                return dict(db_row)
        
        return None
    finally:
        conn.close()

def get_tenant_by_id(tenant_id):
    """
    Retrieves tenant by ID. No caching to ensure fresh data in multi-tenant scenarios.
    Validates tenant_id is not None/empty before querying.
    """
    if not tenant_id:
        return None
    
    conn = get_db_connection()
    if not conn:
        return None
    try:
        row = conn.execute("SELECT * FROM tenants WHERE id = ?", (tenant_id,)).fetchone()
        if row: 
            return dict(row)
        return None
    finally:
        conn.close()

# --- QUEUE ACCESSORS ---

def add_sms_to_queue(to_number, body, external_id=None, tenant_id=None, delay_seconds=0):
    conn = get_db_connection()
    if not conn:
        logger.warning(f"⚠️ Failed to get DB connection. Message not queued for {to_number}")
        return False
    
    msg_id = str(uuid.uuid4())
    # 🛡️ BUG #19 FIX: Timezone String Errors (Force ISO8601)
    created_at = datetime.now().isoformat()
    # Calculate scheduled_for if delayed
    scheduled_for = None
    if delay_seconds > 0:
        from datetime import timedelta
        scheduled_for = (datetime.now() + timedelta(seconds=delay_seconds)).isoformat()
    
    try:
        conn.execute("""
            INSERT INTO sms_queue (id, tenant_id, external_id, to_number, body, status, created_at, scheduled_for)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (msg_id, tenant_id, external_id, to_number, body, 'pending', created_at, scheduled_for))
        conn.commit()
        if scheduled_for:
            logger.info(f"⏳ Message scheduled for {to_number} at {scheduled_for}")
        else:
            logger.info(f"📥 Message queued for {to_number} (DB)")
        return True
    except sqlite3.IntegrityError:
        # If external_id exists (Idempotency check)
        if external_id:
            logger.info(f"♻️  Duplicate Event Ignored (External ID: {external_id})")
            return False
        else:
            # Should not happen with UUID but safe to raise or retry
            raise
    except Exception as e:
        logger.warning(f"⚠️ Error queuing message: {e}")
        conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def claim_pending_sms(limit=10, timeout_minutes=5):
    """
    Atomically claim pending rows OR stuck processing rows (Self-Healing).
    Uses single atomic UPDATE with backoff awareness to prevent race conditions.
    """
    from datetime import timedelta
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        now = datetime.now()
        now_str = now.isoformat()
        
        # Exponential Backoff thresholds (seconds)
        # 0: 0, 1: 5, 2: 30, 3: 120, 4: 600, 5+: 1800
        t1 = (now - timedelta(seconds=5)).isoformat()
        t2 = (now - timedelta(seconds=30)).isoformat()
        t3 = (now - timedelta(seconds=120)).isoformat()
        t4 = (now - timedelta(seconds=600)).isoformat()
        t5 = (now - timedelta(seconds=1800)).isoformat()
        
        # Stickiness check for stuck workers
        cutoff = (now - timedelta(minutes=timeout_minutes)).isoformat()
        
        conn.execute("BEGIN IMMEDIATE")
        
        try:
            # Atomic selection and claim
            # Logic: 
            # 1. Row is 'pending' AND (
            #    attempts=0 OR (attempts=1 AND last_attempt < t1) OR (attempts=2 AND last_attempt < t2) ...
            # )
            # 2. OR Row is 'processing' AND locked_at < cutoff (stuck worker)
            conn.execute("""
                UPDATE sms_queue 
                SET status = 'processing', locked_at = ?
                WHERE id IN (
                    SELECT id FROM sms_queue 
                    WHERE (
                        status = 'pending' AND (
                            attempts = 0 
                            OR (attempts = 1 AND last_attempt <= ?)
                            OR (attempts = 2 AND last_attempt <= ?)
                            OR (attempts = 3 AND last_attempt <= ?)
                            OR (attempts = 4 AND last_attempt <= ?)
                            OR (attempts >= 5 AND last_attempt <= ?)
                        ) AND (scheduled_for IS NULL OR scheduled_for <= ?)
                    ) OR (
                        status = 'processing' AND (locked_at IS NULL OR locked_at <= ?)
                    )
                    ORDER BY created_at ASC
                    LIMIT ?
                )
            """, (now_str, t1, t2, t3, t4, t5, now_str, cutoff, limit))
            
            claimed_rows = conn.execute("""
                SELECT * FROM sms_queue 
                WHERE status = 'processing' AND locked_at = ?
                ORDER BY created_at ASC
                LIMIT ?
            """, (now_str, limit)).fetchall()
            
            conn.commit()
            return [dict(ix) for ix in claimed_rows]
        except Exception as e:
            conn.rollback()
            raise e
            
    except Exception as e:
        logger.error(f"DB Claim Error: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_pending_sms():
    # Deprecated in favor of claim_pending_sms for workers
    # But useful for non-mutating checks
    conn = get_db_connection()
    msgs = conn.execute("SELECT * FROM sms_queue WHERE status = 'pending'").fetchall()
    conn.close()
    return [dict(ix) for ix in msgs]

def get_all_sms():
    conn = get_db_connection()
    # Return reverse chronological for dashboard
    msgs = conn.execute("SELECT * FROM sms_queue ORDER BY created_at DESC LIMIT 100").fetchall()
    conn.close()
    
    # Convert 'to_number' to 'to' to match old interface if needed, or update consumers
    return [dict(ix) for ix in msgs]

def get_sms_since(start_date_iso, tenant_id=None):
    """Fetch all messages since a specific date (for reports)"""
    conn = get_db_connection()
    if tenant_id:
        msgs = conn.execute(
            "SELECT * FROM sms_queue WHERE created_at >= ? AND tenant_id = ? ORDER BY created_at ASC", 
            (start_date_iso, tenant_id)
        ).fetchall()
    else:
        msgs = conn.execute(
            "SELECT * FROM sms_queue WHERE created_at >= ? ORDER BY created_at ASC", 
            (start_date_iso,)
        ).fetchall()
    conn.close()
    return [dict(ix) for ix in msgs]

def get_recent_conversation_logs(limit=20, tenant_id=None):
    """
    Returns recent conversation logs (inbound/outbound).
    Joins with leads to get lead info if needed, or just returns raw logs.
    If tenant_id is provided, filters by tenant.
    """
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        # Check if table exists first (migration safety)
        # Assuming it exists based on log_conversation_event presence
        if tenant_id:
            rows = conn.execute("""
                SELECT l.id, l.body, l.direction, l.created_at, l.lead_id, ld.phone as lead_phone, ld.intent
                FROM conversation_logs l
                LEFT JOIN leads ld ON l.lead_id = ld.id
                WHERE l.tenant_id = ?
                ORDER BY l.created_at DESC
                LIMIT ?
            """, (tenant_id, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT l.id, l.body, l.direction, l.created_at, l.lead_id, ld.phone as lead_phone, ld.intent
                FROM conversation_logs l
                LEFT JOIN leads ld ON l.lead_id = ld.id
                ORDER BY l.created_at DESC
                LIMIT ?
            """, (limit,)).fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Error fetching logs: {e}")
        return []
    finally:
        conn.close()

def update_sms_status(msg_id, status, attempts, last_attempt=None, sent_at=None):
    conn = get_db_connection()
    if sent_at:
        conn.execute("""
            UPDATE sms_queue 
            SET status = ?, attempts = ?, last_attempt = ?, sent_at = ?
            WHERE id = ?
        """, (status, attempts, last_attempt, sent_at, msg_id))
    else:
        conn.execute("""
            UPDATE sms_queue 
            SET status = ?, attempts = ?, last_attempt = ?
            WHERE id = ?
        """, (status, attempts, last_attempt, msg_id))
    conn.commit()
    conn.close()

def update_sms_status_by_message_sid(twilio_message_sid, status):
    """
    Updates SMS status by Twilio MessageSid (from status callback).
    Maps Twilio statuses to internal statuses:
    - 'delivered' -> 'delivered'
    - 'undelivered' -> 'failed'
    - 'failed' -> 'failed'
    - 'sent' -> 'sent' (already sent, just confirming)
    - 'queued' -> 'pending'
    """
    if not twilio_message_sid:
        return False
    
    # Map Twilio status to internal status
    status_map = {
        'delivered': 'delivered',
        'undelivered': 'failed',
        'failed': 'failed',
        'sent': 'sent',
        'queued': 'pending',
        'receiving': 'pending',
        'received': 'delivered'
    }
    
    internal_status = status_map.get(status.lower(), status.lower())
    
    conn = get_db_connection()
    try:
        cursor = conn.execute("""
            UPDATE sms_queue 
            SET status = ?
            WHERE twilio_message_sid = ?
        """, (internal_status, twilio_message_sid))
        
        updated = cursor.rowcount > 0
        conn.commit()
        return updated
    except Exception as e:
        logger.warning(f"⚠️ Error updating SMS status by MessageSid: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def update_sms_twilio_sid(msg_id, twilio_message_sid):
    """Stores the Twilio MessageSid after sending a message."""
    conn = get_db_connection()
    try:
        conn.execute("""
            UPDATE sms_queue 
            SET twilio_message_sid = ?
            WHERE id = ?
        """, (twilio_message_sid, msg_id))
        conn.commit()
        return True
    except Exception as e:
        logger.warning(f"⚠️ Error storing Twilio MessageSid: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def update_sms_body(msg_id, new_body):
    """
    Updates the body text of an SMS message in the queue.
    
    This is used when the message body is modified (e.g., auto-appending
    compliance footer) to ensure the database reflects the actual message
    that will be sent.
    
    Args:
        msg_id: The internal message ID
        new_body: The updated message body text
    
    Returns:
        bool: True if update succeeded, False otherwise
    """
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        conn.execute("""
            UPDATE sms_queue 
            SET body = ?
            WHERE id = ?
        """, (new_body, msg_id))
        conn.commit()
        return True
    except Exception as e:
        logger.warning(f"⚠️ Error updating SMS body: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def cancel_pending_sms(external_id_pattern: str) -> bool:
    """
    Cancels pending SMS messages matching an external_id pattern.
    
    Used to cancel scheduled nudges when user replies, preventing duplicate
    messages and improving user experience.
    
    Args:
        external_id_pattern: Pattern to match (e.g., "nudge_+15551234567")
            Uses SQL LIKE pattern matching (e.g., "nudge_%" matches all nudge messages)
    
    Returns:
        bool: True if any messages were cancelled, False otherwise
    
    Error Handling:
        - Logs errors but doesn't crash
        - Returns False on any database error
        - Always closes database connection
    
    Example:
        >>> cancel_pending_sms("nudge_+15551234567")
        True  # Cancelled 1 message
        >>> cancel_pending_sms("nudge_%")
        True  # Cancelled all pending nudge messages
    """
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        # Cancel messages matching the pattern
        cursor = conn.execute("""
            UPDATE sms_queue 
            SET status = 'cancelled'
            WHERE external_id LIKE ? 
            AND status IN ('pending', 'processing')
        """, (f"{external_id_pattern}%",))
        
        cancelled_count = cursor.rowcount
        conn.commit()
        
        if cancelled_count > 0:
            logger.info(f"✅ Cancelled {cancelled_count} pending message(s) matching {external_id_pattern}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error cancelling pending SMS: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def archive_old_sms(days=30):
    """
    Optional: Archiving logic for old messages to keep DB lightweight.
    """
    pass

# --- LEAD MANAGEMENT ---

def create_or_update_lead(phone, tenant_id=None, source="call", bypass_check=False, name=None):
    """
    Creates a new lead linked to a specific tenant.
    Uses transaction to prevent race conditions.
    
    Args:
        phone: Phone number in E.164 format
        tenant_id: Optional tenant ID
        source: Source of the lead (e.g., "call", "website_form")
        bypass_check: If False, logs a warning that add_client.py should be used for compliance.
                      If True, allows direct calls (for inbound calls, webhooks, etc.)
        name: Optional name for the lead (e.g., caller name from CNAM lookup)
    """
    # Compliance warning: Direct calls should use add_client.py for proper consent tracking
    if not bypass_check:
        import traceback
        import sys
        # Check if called from add_client.py (skip warning if so)
        frame = sys._getframe(1)
        caller_file = frame.f_code.co_filename if frame else ""
        if "add_client.py" not in caller_file:
            logger.warning(f"⚠️  WARNING: Direct lead creation detected. Use 'add_client.py' for compliance (consent proof required). Phone: {phone}")
    
    conn = get_db_connection()
    if not conn:
        raise Exception("Failed to get database connection")
    
    now = datetime.now().isoformat()
    
    try:
        # Use transaction to prevent race conditions
        conn.execute("BEGIN IMMEDIATE")
        
        # Check if exists FOR THIS TENANT (within transaction)
        if tenant_id:
            row = conn.execute("SELECT id, status FROM leads WHERE phone = ? AND tenant_id = ?", (phone, tenant_id)).fetchone()
        else:
            row = conn.execute("SELECT id, status FROM leads WHERE phone = ?", (phone,)).fetchone()
        
        if row:
            lead_id = row['id']
            current_status = row['status']
            # Update last_contact
            conn.execute("UPDATE leads SET last_contact_at = ? WHERE id = ?", (now, lead_id))
            conn.commit()
            return lead_id, current_status
        else:
            lead_id = str(uuid.uuid4())
            
            # SAFETY CHECK: Inherit Opt-Out Status from Global History
            # If this user opted out previously (even under a different tenant), 
            # we respect that globally to avoid spam lawsuits.
            is_blocked = check_opt_out_status(phone)
            initial_opt_out_val = 1 if is_blocked else 0
            
            # Insert with name if provided
            if name:
                conn.execute("""
                    INSERT INTO leads (id, tenant_id, phone, name, status, created_at, last_contact_at, opt_out)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (lead_id, tenant_id, phone, name, 'new', now, now, initial_opt_out_val))
            else:
                conn.execute("""
                    INSERT INTO leads (id, tenant_id, phone, status, created_at, last_contact_at, opt_out)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (lead_id, tenant_id, phone, 'new', now, now, initial_opt_out_val))
            conn.commit()
            logger.info(f"🌟 New Lead Created: {phone} (Tenant: {tenant_id}) OptOut={initial_opt_out_val}")
            return lead_id, 'new'
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_lead_by_phone(phone, tenant_id):
    """Retrieves full lead details, including name if linked to a job."""
    conn = get_db_connection()
    # 1. Try to find name from JOBS table first (Most accurate)
    # We join or just query jobs for this phone
    job_row = conn.execute("""
        SELECT customer_name FROM jobs 
        WHERE customer_phone = ? AND tenant_id = ? 
        ORDER BY job_date DESC LIMIT 1
    """, (phone, tenant_id)).fetchone()
    
    lead_row = conn.execute("SELECT * FROM leads WHERE phone = ? AND tenant_id = ?", (phone, tenant_id)).fetchone()
    conn.close()
    
    if not lead_row:
        return None
        
    lead = dict(lead_row)
    if job_row and job_row['customer_name']:
         lead['name'] = job_row['customer_name']
    else:
         lead['name'] = "Unknown"
         
    return lead

def update_lead_status(phone, new_status, tenant_id=None):
    """
    Updates status. Enforces basic state logic prevents regression from 'booked'.
    If tenant_id is provided, updates only that tenant's lead to prevent cross-tenant updates.
    """
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        # Build query with tenant_id if provided
        if tenant_id:
            row = conn.execute("SELECT status, opt_out FROM leads WHERE phone = ? AND tenant_id = ?", (phone, tenant_id)).fetchone()
        else:
            row = conn.execute("SELECT status, opt_out FROM leads WHERE phone = ?", (phone,)).fetchone()
        
        if not row:
            return False
        
        if row['opt_out'] == 1:
            # Cannot change status of opt-out
            return False
            
        current = row['status']
        
        # Simple State Rules
        # Don't regress from booked unless manual intervention (todo)
        if current == 'booked' and new_status != 'booked':
            return False
        
        # Update with tenant_id if provided
        if tenant_id:
            conn.execute("UPDATE leads SET status = ? WHERE phone = ? AND tenant_id = ?", (new_status, phone, tenant_id))
        else:
            conn.execute("UPDATE leads SET status = ? WHERE phone = ?", (new_status, phone))
        conn.commit()
        return True
    finally:
        conn.close()

def update_lead_intent(phone, intent, tenant_id=None):
    """
    Updates the intent of a lead (e.g., 'emergency', 'service').
    If tenant_id is provided, updates only that tenant's lead.
    """
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        if tenant_id:
            conn.execute("UPDATE leads SET intent = ? WHERE phone = ? AND tenant_id = ?", (intent, phone, tenant_id))
        else:
            conn.execute("UPDATE leads SET intent = ? WHERE phone = ?", (intent, phone))
        conn.commit()
        logger.info(f"🏷️  Lead Tagged: {phone} -> {intent}")
        return True
    except Exception as e:
        conn.rollback()
        logger.warning(f"⚠️ Error updating lead intent: {e}")
        return False
    finally:
        conn.close()


def set_opt_out(phone, is_opt_out=True):
    """
    Sets opt-out status. PERMANENT: Once opted out, cannot be overridden by mistake.
    If is_opt_out=True, it's permanent and cannot be changed back except by explicit admin action.
    """
    val = 1 if is_opt_out else 0
    create_or_update_lead(phone, bypass_check=True) # Ensure exists (system call)
    conn = get_db_connection()
    
    # PERMANENT: If already opted out, don't allow override unless explicitly setting to False
    # This prevents accidental re-subscription
    if is_opt_out:
        # Setting to opt-out: PERMANENT - update all leads for this phone across all tenants
        conn.execute("UPDATE leads SET opt_out = 1 WHERE phone = ?", (phone,))
        # Also cancel any pending messages in queue
        conn.execute("UPDATE sms_queue SET status = 'failed_optout' WHERE to_number = ? AND status IN ('pending', 'processing')", (phone,))
    else:
        # Only allow opt-in if explicitly requested (for START/UNSTOP commands)
        conn.execute("UPDATE leads SET opt_out = 0 WHERE phone = ?", (phone,))
    
    conn.commit()
    conn.close()
    logger.info(f"🚫 Opt-Out Set for {phone}: {is_opt_out} (PERMANENT)")

def check_opt_out_status(phone):
    """
    Checks if the phone number is opted out in ANY tenant.
    Returns True if blocked.
    PERMANENT: Once opted out, this always returns True.
    """
    if not phone:
        return False
    conn = get_db_connection()
    # Check for ANY opt-out across all tenants (global opt-out)
    row = conn.execute("SELECT 1 FROM leads WHERE phone = ? AND opt_out = 1 LIMIT 1", (phone,)).fetchone()
    conn.close()
    return bool(row)

def log_conversation_event(phone, direction, body, external_id=None, tenant_id=None):
    """
    Logs a message (inbound/outbound) attached to the lead.
    """
    # Ensure lead exists first (system call)
    lead_id, _ = create_or_update_lead(phone, tenant_id, bypass_check=True)
    
    conn = get_db_connection()
    log_id = str(uuid.uuid4())
    now = datetime.now().isoformat()
    
    try:
        conn.execute("""
            INSERT INTO conversation_logs (id, tenant_id, lead_id, direction, body, external_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (log_id, tenant_id, lead_id, direction, body, external_id, now))
        conn.commit()
    except sqlite3.IntegrityError:
        pass # Duplicate log event
    finally:
        conn.close()

def get_lead_funnel_stats(tenant_id=None, start_date=None, end_date=None):
    """
    Returns counts of leads by status.
    If tenant_id is provided, filters by tenant.
    If start_date and/or end_date are provided, filters by created_at date range.
    
    Args:
        tenant_id: Optional ID to filter by tenant
        start_date: Optional ISO date string to filter leads created on or after this date
        end_date: Optional ISO date string to filter leads created on or before this date
    """
    conn = get_db_connection()
    if not conn:
        return {"new": 0, "contacted": 0, "replied": 0, "booked": 0, "lost": 0, "total": 0}
    
    try:
        # Build query with optional filters
        base_query = "SELECT status, COUNT(*) as count FROM leads WHERE 1=1"
        params = []
        
        if tenant_id:
            base_query += " AND tenant_id = ?"
            params.append(tenant_id)
        
        if start_date:
            base_query += " AND created_at >= ?"
            params.append(start_date)
        
        if end_date:
            base_query += " AND created_at <= ?"
            params.append(end_date)
        
        base_query += " GROUP BY status"
        
        rows = conn.execute(base_query, params).fetchall()
    finally:
        conn.close()
    
    stats = {
        "new": 0, "contacted": 0, "replied": 0, "booked": 0, "lost": 0 
    }
    total = 0
    for r in rows:
        s = r['status']
        c = r['count']
        stats[s] = c
        total += c
    stats['total'] = total
    return stats

def get_revenue_stats(tenant_id=None, start_date=None, end_date=None):
    """
    Calculates revenue saved based on Emergency leads.
    Revenue = (Emergency Lead Count) * (Average Job Value)
    
    Args:
        tenant_id: Optional ID to filter by tenant
        start_date: Combined with end_date to filter 'created_at' (ISO strings or datetime)
        end_date: Combined with start_date to filter 'created_at'
    """
    conn = get_db_connection()
    
    # 1. Get Average Job Value from Tenant (Default 350)
    avg_value = 350
    if tenant_id:
        row = conn.execute("SELECT average_job_value FROM tenants WHERE id = ?", (tenant_id,)).fetchone()
        if row and row['average_job_value']:
            avg_value = row['average_job_value']
    
    # 2. Build Query
    base_query = "SELECT COUNT(*) FROM leads WHERE intent = 'emergency'"
    params = []
    
    if tenant_id:
        base_query += " AND tenant_id = ?"
        params.append(tenant_id)
        
    # 3. Calculate Period Stats (if dates provided)
    if start_date:
        period_query = base_query + " AND created_at >= ?"
        period_params = params + [start_date]
        
        if end_date:
            period_query += " AND created_at <= ?"
            period_params.append(end_date)
            
        period_count = conn.execute(period_query, period_params).fetchone()[0]
    else:
        # If no date range, period is same as lifetime? Or just 0?
        # Let's assume caller handles logic, but if no date, returns 0 for period.
        # Actually, let's just default to all-time if no date given (backward compat)
        period_count = conn.execute(base_query, params).fetchone()[0]

    # 4. Calculate Lifetime Stats (Always)
    lifetime_count = conn.execute(base_query, params).fetchone()[0]
        
    conn.close()
    
    return {
        "revenue_saved": period_count * avg_value,
        "emergency_leads": period_count,
        "average_job_value": avg_value,
        "lifetime_revenue_saved": lifetime_count * avg_value,
        "lifetime_emergency_leads": lifetime_count
    }

def check_rate_limit_db(key, limit, window_seconds):
    """
    Persisted Rate Limiting using SQLite.
    Returns (allowed: bool, wait_time: float)
    """
    conn = get_db_connection()
    now = time.time()
    
    try:
        # Check current status
        row = conn.execute("SELECT count, reset_at FROM rate_limits WHERE key = ?", (key,)).fetchone()
        
        if row and now < row['reset_at']:
            # Window active
            if row['count'] >= limit:
                # Limit exceeded
                return False, row['reset_at'] - now
            else:
                # Increment
                conn.execute("UPDATE rate_limits SET count = count + 1 WHERE key = ?", (key,))
                conn.commit()
                return True, 0
        else:
            # New window (Insert or Replace)
            reset_at = now + window_seconds
            conn.execute("INSERT OR REPLACE INTO rate_limits (key, count, reset_at) VALUES (?, 1, ?)", (key, reset_at))
            conn.commit()
            return True, 0
            
    except Exception as e:
        logger.warning(f"⚠️ Rate Limit DB Error: {e}")
        return True, 0 # Fail open on DB error to prevent service blocking
    finally:
        conn.close()


# --- CASL CONSENT MANAGEMENT ---
# Canada's Anti-Spam Legislation (CASL) requires proof of consent for all commercial electronic messages.
# The CRTC can impose fines up to $10M (individual) or $15M (corporation) for violations.

def record_consent(phone, consent_type, consent_source, tenant_id=None, 
                   ip_address=None, user_agent=None, form_url=None, 
                   consent_text=None, metadata=None):
    """
    Records proof of consent for CASL compliance.
    
    CASL recognizes two types of consent:
    1. EXPRESS CONSENT: The person explicitly agreed to receive messages (e.g., checked a box on a form)
    2. IMPLIED CONSENT: The person initiated contact (e.g., called or texted first)
    
    Args:
        phone: The phone number that gave consent
        consent_type: 'express' or 'implied'
        consent_source: 'inbound_call', 'inbound_sms', 'web_form', 'manual'
        tenant_id: The tenant this consent applies to
        ip_address: IP address (required for web form express consent)
        user_agent: Browser/device info for web forms
        form_url: URL of the form where consent was given
        consent_text: The exact checkbox/disclaimer text they agreed to
        metadata: Dict with additional context (CallSid, MessageSid, etc.)
        
    Returns:
        consent_id: The ID of the created consent record
    """
    from datetime import timedelta
    
    # Ensure lead exists (system call)
    lead_id, _ = create_or_update_lead(phone, tenant_id=tenant_id, bypass_check=True)
    
    conn = get_db_connection()
    consent_id = str(uuid.uuid4())
    now = datetime.now()
    consented_at = now.isoformat()
    
    # CASL: Implied consent expires after 2 years
    # Express consent does not expire unless revoked
    if consent_type == 'implied':
        expires_at = (now + timedelta(days=730)).isoformat()  # 2 years
    else:
        expires_at = None  # Express consent doesn't expire
    
    # Serialize metadata if provided
    metadata_json = json.dumps(metadata) if metadata else None
    
    try:
        conn.execute("""
            INSERT INTO consent_records 
            (id, lead_id, tenant_id, phone, consent_type, consent_source, 
             ip_address, user_agent, form_url, consent_text, 
             consented_at, expires_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (consent_id, lead_id, tenant_id, phone, consent_type, consent_source,
              ip_address, user_agent, form_url, consent_text,
              consented_at, expires_at, metadata_json))
        conn.commit()
        logger.info(f"✅ CASL Consent Recorded: {phone} ({consent_type}/{consent_source})")
        return consent_id
    except Exception as e:
        logger.warning(f"⚠️ Failed to record consent: {e}")
        return None
    finally:
        conn.close()


def verify_valid_consent(phone, tenant_id=None):
    """
    Verifies if there is valid, unexpired, non-revoked consent for a phone number.
    
    CASL Requirements:
    - Express consent never expires unless revoked
    - Implied consent expires after 2 years
    - Any revoked consent invalidates messaging rights
    
    Returns:
        dict: {'has_consent': bool, 'consent_type': str, 'consent_source': str, 'consented_at': str}
              or None if no valid consent exists
    """
    conn = get_db_connection()
    now = datetime.now().isoformat()
    
    # Query for valid consent: not revoked AND (no expiry OR expiry > now)
    if tenant_id:
        row = conn.execute("""
            SELECT consent_type, consent_source, consented_at, expires_at 
            FROM consent_records 
            WHERE phone = ? 
              AND tenant_id = ?
              AND revoked_at IS NULL 
              AND (expires_at IS NULL OR expires_at > ?)
            ORDER BY consented_at DESC
            LIMIT 1
        """, (phone, tenant_id, now)).fetchone()
    else:
        row = conn.execute("""
            SELECT consent_type, consent_source, consented_at, expires_at 
            FROM consent_records 
            WHERE phone = ? 
              AND revoked_at IS NULL 
              AND (expires_at IS NULL OR expires_at > ?)
            ORDER BY consented_at DESC
            LIMIT 1
        """, (phone, now)).fetchone()
    
    conn.close()
    
    if row:
        return {
            'has_consent': True,
            'consent_type': row['consent_type'],
            'consent_source': row['consent_source'],
            'consented_at': row['consented_at'],
            'expires_at': row['expires_at']
        }
    return None


def revoke_consent(phone, reason='STOP', tenant_id=None):
    """
    Revokes all consent for a phone number (CASL opt-out).
    
    This is triggered when someone replies STOP, unsubscribe, etc.
    After revocation, NO messages can be sent until new consent is obtained.
    
    Args:
        phone: The phone number revoking consent
        reason: The opt-out keyword used ('STOP', 'unsubscribe', etc.)
        tenant_id: Optional tenant scope (if None, revokes for all tenants)
    """
    conn = get_db_connection()
    now = datetime.now().isoformat()
    
    if tenant_id:
        conn.execute("""
            UPDATE consent_records 
            SET revoked_at = ?, revocation_reason = ?
            WHERE phone = ? AND tenant_id = ? AND revoked_at IS NULL
        """, (now, reason, phone, tenant_id))
    else:
        # Global revocation (all tenants) - SAFER for CASL
        conn.execute("""
            UPDATE consent_records 
            SET revoked_at = ?, revocation_reason = ?
            WHERE phone = ? AND revoked_at IS NULL
        """, (now, reason, phone))
    
    conn.commit()
    conn.close()
    logger.info(f"🚫 CASL Consent Revoked: {phone} (Reason: {reason})")


def get_consent_audit_trail(phone, tenant_id=None):
    """
    Returns the complete consent history for a phone number.
    
    This is what you would provide to the CRTC if a consumer files a complaint.
    The audit trail proves when and how consent was obtained.
    
    Returns:
        list of dicts with full consent history
    """
    conn = get_db_connection()
    
    if tenant_id:
        rows = conn.execute("""
            SELECT * FROM consent_records 
            WHERE phone = ? AND tenant_id = ?
            ORDER BY consented_at ASC
        """, (phone, tenant_id)).fetchall()
    else:
        rows = conn.execute("""
            SELECT * FROM consent_records 
            WHERE phone = ?
            ORDER BY consented_at ASC
        """, (phone,)).fetchall()
    
    conn.close()
    
    trail = []
    for row in rows:
        record = dict(row)
        # Parse metadata JSON if present
        if record.get('metadata'):
            try:
                record['metadata'] = json.loads(record['metadata'])
            except:
                pass
        trail.append(record)
    
    return trail


# --- WEBHOOK IDEMPOTENCY ---

def check_webhook_processed(provider_id):
    """
    Checks if a webhook (by provider ID like MessageSid/CallSid) was already processed.
    Returns (is_duplicate: bool, internal_id: str or None)
    """
    if not provider_id:
        return False, None
    
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT id, internal_id FROM webhook_events WHERE provider_id = ? LIMIT 1",
            (provider_id,)
        ).fetchone()
        if row:
            return True, row['internal_id']
        return False, None
    finally:
        conn.close()

def record_webhook_processed(provider_id, webhook_type, tenant_id=None, internal_id=None):
    """
    Records that a webhook was processed to prevent duplicate handling.
    
    Args:
        provider_id: Twilio MessageSid, CallSid, etc.
        webhook_type: 'sms', 'voice', 'voice_status'
        tenant_id: Optional tenant ID
        internal_id: Our internal message/event ID
    
    Returns:
        True if recorded, False if duplicate (already exists)
    """
    if not provider_id:
        return False
    
    conn = get_db_connection()
    try:
        webhook_id = str(uuid.uuid4())
        processed_at = datetime.now().isoformat()
        
        conn.execute("""
            INSERT INTO webhook_events (id, provider_id, webhook_type, tenant_id, processed_at, internal_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (webhook_id, provider_id, webhook_type, tenant_id, processed_at, internal_id))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        # Duplicate provider_id - already processed
        return False
    finally:
        conn.close()

def get_consent_stats(tenant_id=None):
    """
    Returns consent statistics for reporting.
    
    Useful for compliance dashboards.
    """
    conn = get_db_connection()
    now = datetime.now().isoformat()
    
    if tenant_id:
        base_query = "FROM consent_records WHERE tenant_id = ?"
        params = (tenant_id,)
    else:
        base_query = "FROM consent_records WHERE 1=1"
        params = ()
    
    # Total consents
    total = conn.execute(f"SELECT COUNT(*) {base_query}", params).fetchone()[0]
    
    # Active consents (not expired, not revoked)
    active_query = f"""
        SELECT COUNT(*) {base_query} 
        AND revoked_at IS NULL 
        AND (expires_at IS NULL OR expires_at > ?)
    """
    active = conn.execute(active_query, params + (now,)).fetchone()[0]
    
    # Revoked consents
    revoked = conn.execute(f"SELECT COUNT(*) {base_query} AND revoked_at IS NOT NULL", params).fetchone()[0]
    
    # By type
    express = conn.execute(f"SELECT COUNT(*) {base_query} AND consent_type = 'express'", params).fetchone()[0]
    implied = conn.execute(f"SELECT COUNT(*) {base_query} AND consent_type = 'implied'", params).fetchone()[0]
    
    conn.close()
    
    return {
        'total_consents': total,
        'active_consents': active,
        'revoked_consents': revoked,
        'express_consents': express,
        'implied_consents': implied
    }

# Initialize on module load (safe?)
# Better to let the app call it, but strictly for "script" usage:
# Initialize on module load (safe?)
# Better to let the app call it, but strictly for "script" usage:
if __name__ == "__main__":
    init_db()
def insert_or_update_alert_buffer(tenant_id, customer_phone, plumber_phone, message_text):
    """
    Inserts a new alert buffer or updates an existing one.
    Resets the timer to 30s from now on every new message (Debounce).
    Uses consistent ISO format for timestamps.
    """
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        # Validate tenant_id exists
        if tenant_id:
            tenant_check = conn.execute("SELECT 1 FROM tenants WHERE id = ?", (tenant_id,)).fetchone()
            if not tenant_check:
                logger.warning(f"⚠️ Invalid tenant_id {tenant_id} in alert buffer")
                return False
        
        c = conn.cursor()
        
        # Check if exists
        c.execute("SELECT messages_text, message_count FROM alert_buffer WHERE tenant_id = ? AND customer_phone = ?", (tenant_id, customer_phone))
        row = c.fetchone()
        
        from datetime import timedelta
        # Use ISO format for consistent timestamp comparison
        send_at = (datetime.now() + timedelta(seconds=30)).isoformat()
        
        if row:
            # Update
            existing_text = row['messages_text']
            new_count = row['message_count'] + 1
            combined_text = f"{existing_text}\n{message_text}"
            
            c.execute("""
                UPDATE alert_buffer 
                SET messages_text = ?, message_count = ?, send_at = ?
                WHERE tenant_id = ? AND customer_phone = ?
            """, (combined_text, new_count, send_at, tenant_id, customer_phone))
        else:
            # Insert
            created_at = datetime.now().isoformat()
            c.execute("""
                INSERT INTO alert_buffer (tenant_id, customer_phone, plumber_phone, messages_text, message_count, send_at, created_at)
                VALUES (?, ?, ?, ?, 1, ?, ?)
            """, (tenant_id, customer_phone, plumber_phone, message_text, send_at, created_at))
            
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        logger.warning(f"⚠️ Error updating alert buffer: {e}")
        return False
    finally:
        conn.close()

def process_alert_buffer():
    """
    Checks for ready-to-send alerts and queues them.
    Uses transaction to prevent race conditions and ensure atomicity.
    """
    conn = get_db_connection()
    if not conn:
        return 0
    
    try:
        # Use transaction to prevent race conditions
        conn.execute("BEGIN IMMEDIATE")
        
        c = conn.cursor()
        
        # Use consistent ISO format for timestamp comparison
        now_iso = datetime.now().isoformat()
        
        # Fetch ready alerts (use ISO string for consistent comparison)
        rows = c.execute("SELECT * FROM alert_buffer WHERE send_at <= ?", (now_iso,)).fetchall()
        
        if not rows:
            conn.rollback()  # No work, rollback transaction
            return 0
        
        from execution.utils.sms_engine import add_to_queue
        
        processed_count = 0
        failed_count = 0
        buffer_ids_to_delete = []
        
        for row in rows:
            buf_id = row['id']
            tenant_id = row['tenant_id']
            cust_phone = row['customer_phone']
            plumber_phone = row['plumber_phone']
            msg_text = row['messages_text']
            count = row['message_count']
            
            # Construct Summary Message
            if count > 1:
                final_msg = f"🔔 Lead Alert: {cust_phone} sent {count} messages:\n---\n{msg_text}\n---"
            else:
                final_msg = f"🔔 Lead Alert: {cust_phone} says: {msg_text}"
                
            # Queue it - check return value
            logger.info(f"🚀 Dispatching Buffered Alert to {plumber_phone} (Count: {count})")
            # Use UUID-based external_id to prevent collisions
            import uuid
            external_id = f"buf_{buf_id}_{uuid.uuid4().hex[:8]}"
            
            queue_success = add_to_queue(plumber_phone, final_msg, external_id=external_id, tenant_id=tenant_id)
            
            if queue_success:
                # Only delete if queueing succeeded
                buffer_ids_to_delete.append(buf_id)
                processed_count += 1
            else:
                failed_count += 1
                logger.warning(f"⚠️ Failed to queue alert buffer {buf_id}, will retry later")
        
        # Delete all successfully queued alerts in one operation
        if buffer_ids_to_delete:
            placeholders = ','.join(['?'] * len(buffer_ids_to_delete))
            c.execute(f"DELETE FROM alert_buffer WHERE id IN ({placeholders})", buffer_ids_to_delete)
        
        conn.commit()
        
        if failed_count > 0:
            logger.warning(f"⚠️ {failed_count} alert(s) failed to queue and will be retried")
        
        return processed_count
    except Exception as e:
        conn.rollback()
        logger.warning(f"⚠️ Error processing alert buffer: {e}")
        return 0
    finally:
        conn.close()

def save_otp(phone, code, valid_minutes=10):
    """
    Saves OTP code for a phone number with expiration.
    Stores hashed code for security.
    """
    import hashlib
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        from datetime import timedelta
        now = datetime.now()
        expires_at = (now + timedelta(minutes=valid_minutes)).isoformat()
        created_at = now.isoformat()
        
        # Hash the OTP code before storing (SHA256)
        code_hash = hashlib.sha256(str(code).encode('utf-8')).hexdigest()
        
        conn.execute("""
            INSERT OR REPLACE INTO otp_codes (phone, code, expires_at, attempts, created_at)
            VALUES (?, ?, ?, 0, ?)
        """, (phone, code_hash, expires_at, created_at))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error saving OTP: {e}")
        return False
    finally:
        conn.close()

def verify_otp_code(phone, code):
    """
    Verifies OTP. Returns (success, message).
    Checks expiry and attempt count.
    Uses transaction to prevent race conditions.
    Tries flexible phone matching to handle variations in phone format.
    """
    import hashlib
    conn = get_db_connection()
    if not conn:
        return False, "System error"
    
    try:
        # Use transaction to prevent race conditions
        conn.execute("BEGIN IMMEDIATE")
        
        # Try exact match first
        row = conn.execute("SELECT * FROM otp_codes WHERE phone = ?", (phone,)).fetchone()
        otp_phone_key = phone  # Track which phone key was used to find the OTP
        
        # If no exact match, try flexible matching (handle +1 prefix variations)
        if not row and len(phone) == 10:
            # Try with leading '1' (user might have entered 10 digits, OTP saved with 11)
            phone_with_1 = f"1{phone}"
            row = conn.execute("SELECT * FROM otp_codes WHERE phone = ?", (phone_with_1,)).fetchone()
            if row:
                otp_phone_key = phone_with_1
        
        if not row and len(phone) == 11 and phone.startswith('1'):
            # Try without leading '1' (user might have entered 11 digits, OTP saved with 10)
            phone_without_1 = phone[1:]
            row = conn.execute("SELECT * FROM otp_codes WHERE phone = ?", (phone_without_1,)).fetchone()
            if row:
                otp_phone_key = phone_without_1
        
        if not row:
            conn.rollback()
            return False, "OTP not found for this number"
            
        # Check Expiry
        if datetime.fromisoformat(row['expires_at']) < datetime.now():
            conn.rollback()
            return False, "OTP expired"
            
        # Check Attempts (within transaction)
        if row['attempts'] >= 5:
            conn.rollback()
            return False, "Too many attempts"
            
        # Hash the provided code and compare
        code_hash = hashlib.sha256(str(code).encode('utf-8')).hexdigest()
        
        # Check Match (compare hashes)
        if row['code'] == code_hash:
            # Success - Clean up using the actual phone key that was found
            conn.execute("DELETE FROM otp_codes WHERE phone = ?", (otp_phone_key,))
            conn.commit()
            return True, "Verified"
        else:
            # Increment attempts (within transaction) using the actual phone key
            conn.execute("UPDATE otp_codes SET attempts = attempts + 1 WHERE phone = ?", (otp_phone_key,))
            conn.commit()
            return False, "Invalid code"
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Error verifying OTP: {e}")
        return False, "System error"
    finally:
        conn.close()
def get_or_create_magic_token(lead_id):
    """Generates or retrieves a magic token for a lead"""
    import secrets
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT magic_token FROM leads WHERE id = ?", (lead_id,)).fetchone()
        if not row:
            return None
            
        token = row['magic_token']
        if not token:
            token = secrets.token_urlsafe(16)
            conn.execute("UPDATE leads SET magic_token = ? WHERE id = ?", (token, lead_id))
            conn.commit()
        return token
    except Exception as e:
        logger.error(f"Error getting/creating magic token: {e}")
        return None
    finally:
        conn.close()

def migrate_db_if_needed():
    """Run specific migrations if columns missing"""
    conn = get_db_connection()
    c = conn.cursor()
    
    # Check for locked_at in sms_queue
    try:
        c.execute("SELECT locked_at FROM sms_queue LIMIT 1")
    except Exception:
        logger.info("🔧 Migrating DB: Adding locked_at to sms_queue...")
        c.execute("ALTER TABLE sms_queue ADD COLUMN locked_at TEXT")
        conn.commit()

    # Check for ai_active in tenants
    try:
        cursor = c.execute("PRAGMA table_info(tenants)")
        columns = [row[1] for row in cursor.fetchall()]
        if 'ai_active' not in columns:
            logger.info("🔧 Migrating DB: Adding ai_active to tenants...")
            c.execute("ALTER TABLE tenants ADD COLUMN ai_active INTEGER DEFAULT 1")
            conn.commit()
        if 'email' not in columns:
            logger.info("🔧 Migrating DB: Adding email col to tenants...")
            c.execute("ALTER TABLE tenants ADD COLUMN email TEXT")
            conn.commit()
    except Exception:
        pass
    
    conn.close()

def get_leads_count_since(days, tenant_id=None):
    """
    Returns the number of leads created in the last N days.
    """
    conn = get_db_connection()
    try:
        from datetime import timedelta
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        
        if tenant_id:
            count = conn.execute("SELECT COUNT(*) FROM leads WHERE created_at >= ? AND tenant_id = ?", (cutoff, tenant_id)).fetchone()[0]
        else:
            count = conn.execute("SELECT COUNT(*) FROM leads WHERE created_at >= ?", (cutoff,)).fetchone()[0]
        return count
    finally:
        conn.close()

```
---

### File: `execution/utils/sms_engine.py`
```python
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
    if tenant_id:
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

```
---

### File: `execution/utils/classification.py`
```python
"""
AI Classification Module for Emergency vs Standard Request Detection

This module provides intelligent classification of customer messages to distinguish
between emergency situations (requiring immediate response) and standard service
requests (can be scheduled normally).

Classification Methods:
1. Keyword-based (fast, reliable for common cases)
2. AI-based (more accurate, handles edge cases)
3. Hybrid (combines both for best accuracy)
"""

from execution.utils.logger import setup_logger
from execution.utils.constants import EMERGENCY_KEYWORDS, NEGATIVE_KEYWORDS
import re
from execution.services.openai_service import get_openai_service

logger = setup_logger("Classification")


def classify_request_urgency(message_text: str, use_ai: bool = False) -> dict:
    """
    Classifies a customer message as Emergency or Standard.
    
    This function analyzes the message text to determine if the customer needs
    immediate emergency service (e.g., burst pipe, flooding) or if it's a
    standard request that can be scheduled normally (e.g., leaky faucet, quote).
    
    Args:
        message_text: The customer's message text to classify
        use_ai: If True, uses AI classification (requires API key). 
                If False, uses fast keyword-based classification.
    
    Returns:
        dict with keys:
            - urgency: 'emergency' | 'standard' | 'unknown'
            - confidence: float (0.0 to 1.0)
            - reasoning: str (explanation of classification)
            - keywords_found: list (emergency keywords detected)
    
    Example:
        >>> classify_request_urgency("My pipe burst and water is everywhere!")
        {
            'urgency': 'emergency',  # String: 'emergency', 'standard', or 'unknown'
            'confidence': 0.95,
            'reasoning': 'Multiple emergency keywords detected: burst, water everywhere',
            'keywords_found': ['burst', 'water everywhere']
        }
    """
    if not message_text or not isinstance(message_text, str):
        return {
            'urgency': 'unknown',  # Changed from integer 0 to string
            'confidence': 0.0,
            'reasoning': 'Empty or invalid message text',
            'keywords_found': []
        }
    
    message_lower = message_text.lower().strip()
    
    # PRIORITY CHECK: Explicit "not urgent" language overrides emergency keywords
    if re.search(r'\b(?:not urgent|not an emergency|can wait|when convenient)\b', message_lower):
        return {
            'urgency': 'standard',  # Changed from integer 1 to string
            'confidence': 0.85,
            'reasoning': 'Standard request: Explicit non-urgent language detected.',
            'keywords_found': []
        }
    
    # Fast keyword-based classification (always runs first)
    keywords_found = []
    emergency_score = 0
    
    # Check for emergency keywords with context
    # Enhanced detection: looks for keywords with surrounding context to avoid false positives
    for keyword in EMERGENCY_KEYWORDS:
        # Use word boundaries to avoid false positives (e.g., "leakproof" shouldn't match "leak")
        pattern = r'\b' + re.escape(keyword) + r'\b'
        if re.search(pattern, message_lower, re.IGNORECASE):
            keywords_found.append(keyword)
            # Weight keywords by severity (refined based on real-world data)
            # High severity: Immediate danger, property damage, health risk
            if keyword in ['burst', 'explode', 'flood', 'flooding', 'sewage', 'gas smell', 'water everywhere', 'overflowing']:
                emergency_score += 3  # High severity - immediate response needed
            # Medium-high severity: Urgent but may not be life-threatening
            elif keyword in ['emergency', 'urgent', 'no water', 'overflow', 'toilet overflow', 'basement', 'ceiling']:
                emergency_score += 2  # Medium-high severity - respond quickly
            else:
                emergency_score += 1  # Standard emergency keyword - needs attention
    
    # Check for urgency indicators
    urgency_phrases = [
        r'\b(?:right now|immediately|asap|as soon as possible|urgent|emergency)\b',
        r'\b(?:can\'?t wait|need help now|please hurry)\b',
        r'\b(?:water (?:is|everywhere|flooding)|flooding|burst|exploded)\b'
    ]
    
    for phrase in urgency_phrases:
        if re.search(phrase, message_lower, re.IGNORECASE):
            emergency_score += 2
    
    # Check for standard/non-emergency indicators
    standard_phrases = [
        r'\b(?:quote|estimate|price|cost|how much)\b',
        r'\b(?:schedule|appointment|when can|next week|next month)\b',
        r'\b(?:small leak|dripping|minor|not urgent|can wait)\b',
        r'\b(?:not urgent|not an emergency|can wait|when convenient)\b'
    ]
    
    standard_score = 0
    for phrase in standard_phrases:
        if re.search(phrase, message_lower, re.IGNORECASE):
            standard_score += 1
    
    # Calculate confidence based on keyword matches
    # FIX: Return string values ('emergency', 'standard', 'unknown') for consistency
    if emergency_score >= 3:
        urgency = 'emergency'  # Changed from integer 3 to string
        confidence = min(0.95, 0.7 + (emergency_score * 0.05))
        reasoning = f"Emergency detected: {len(keywords_found)} keyword(s) found. High urgency indicators present."
    elif emergency_score >= 1 and standard_score == 0:
        urgency = 'emergency'  # Changed from integer 3 to string
        confidence = 0.6 + (emergency_score * 0.1)
        reasoning = f"Possible emergency: {len(keywords_found)} keyword(s) found. No standard indicators."
    elif standard_score >= 2 and emergency_score == 0:
        urgency = 'standard'  # Changed from integer 1 to string
        confidence = 0.85
        reasoning = "Standard request: Multiple scheduling/quote indicators, no emergency keywords."
    elif standard_score >= 1 and emergency_score < 2:
        # Check for explicit "not urgent" language
        if re.search(r'\b(?:not urgent|not an emergency|can wait)\b', message_lower):
            urgency = 'standard'  # Changed from integer 1 to string
            confidence = 0.8
            reasoning = "Standard request: Explicit non-urgent language detected."
        else:
            urgency = 'standard'  # Changed from integer 1 to string
            confidence = 0.7
            reasoning = "Likely standard: Scheduling/quote indicators present, minimal emergency signals."
    else:
        urgency = 'unknown'  # Changed from integer 0 to string
        confidence = 0.5
        reasoning = "Unclear intent: Mixed or no clear indicators. Manual review recommended."
    
    # AI-based classification (if enabled and available)
    if use_ai:
        try:
            ai_result = _classify_with_ai(message_text)
            if ai_result:
                # Normalize AI result to match our string format
                # AI may return 'emergency', 'standard', or 'spam'
                ai_urgency = ai_result.get('urgency', '').lower()
                if ai_urgency in ['emergency', 'standard', 'spam']:
                    # Map 'spam' to 'unknown' for consistency
                    if ai_urgency == 'spam':
                        ai_result['urgency'] = 'unknown'
                    # Use AI result if it's more confident
                    if ai_result.get('confidence', 0) > confidence:
                        return ai_result
        except Exception as e:
            logger.warning(f"AI classification failed, using keyword result: {e}")
    
    return {
        'urgency': urgency,
        'confidence': min(1.0, confidence),
        'reasoning': reasoning,
        'keywords_found': keywords_found
    }


def _classify_with_ai(clean_body):
    """
    Passes the message to the OpenAI Service for classification.
    """
    try:
        ai_service = get_openai_service()
        result = ai_service.classify_intent(clean_body)
        
        if result:
            logger.info(f"🧠 AI Classification Result: {result}")
            return result
        else:
            return None # Fallback to keyword
            
    except Exception as e:
        logger.error(f"AI Classification Wrapper Error: {e}")
        return None


def classify_from_sms(message_body: str, use_ai: bool = False) -> dict:
    """
    Convenience wrapper for SMS message classification.
    
    This function is specifically designed for SMS messages and may include
    SMS-specific preprocessing (e.g., handling abbreviations, emojis).
    
    Args:
        message_body: The SMS message body
        use_ai: Whether to use AI classification
    
    Returns:
        Same format as classify_request_urgency()
    """
    return classify_request_urgency(message_body, use_ai=use_ai)


def classify_from_transcript(transcript_text: str, use_ai: bool = True) -> dict:
    """
    Classifies urgency from a call transcript.
    
    Transcripts may be longer and more conversational than SMS, so this function
    may use different heuristics (e.g., analyzing full conversation context).
    
    Args:
        transcript_text: The full call transcript
        use_ai: Whether to use AI (recommended for transcripts due to length)
    
    Returns:
        Same format as classify_request_urgency()
    """
    # For transcripts, AI classification is more valuable due to context
    return classify_request_urgency(transcript_text, use_ai=use_ai)

```
---

### File: `execution/utils/transcription.py`
```python
"""
Transcription Service for Call Recordings

Key Features:
- Uses Redis Queue (RQ) for background jobs (Grade-A)
- Falls back to threading if Redis unavailable
- Whisper transcription with Twilio polling fallback
"""

import os
import redis
from rq import Queue
from execution.utils.logger import setup_logger
from execution.utils.classification import classify_from_transcript
from execution.utils.database import update_lead_intent, log_conversation_event
from execution.services.openai_service import get_openai_service

logger = setup_logger("Transcription")

# Setup Redis Connection
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
start_rq = False
try:
    if 'redis' in REDIS_URL:
        conn = redis.from_url(REDIS_URL)
        q = Queue(connection=conn)
        start_rq = True
        logger.info("✅ Redis Queue connected for transcription.")
except Exception as e:
    logger.warning(f"⚠️ Redis not available ({e}). Falling back to Threads.")
    import threading

def transcription_task(recording_url, call_sid, caller_number, tenant_id, lead_id=None):
    """
    Background task for processing transcription.
    Must be top-level for RQ pickling.
    """
    try:
        logger.info(f"🎙️ Starting transcription task for {call_sid}")
        
        transcript_text = _fetch_whisper_transcription(recording_url, call_sid)
        if not transcript_text:
            transcript_text = _fetch_twilio_transcription(recording_url, call_sid)
        
        if not transcript_text:
            return
        
        classification = classify_from_transcript(transcript_text, use_ai=True)
        
        if lead_id or caller_number:
            intent = classification.get('urgency', 'inquiry')
            if intent == 'emergency':
                update_lead_intent(caller_number, 'emergency', tenant_id=tenant_id)
            elif intent == 'standard':
                update_lead_intent(caller_number, 'service', tenant_id=tenant_id)
            
            log_conversation_event(caller_number, 'inbound', f"(Transcript) {transcript_text[:200]}...", 
                                   external_id=call_sid, tenant_id=tenant_id)
            logger.info(f"✅ Classified: {intent}")
                
    except Exception as e:
        logger.critical(f"🔥 TRANSCRIPTION TASK FAILED: {e}", exc_info=True)
        from execution.utils.alert_system import send_critical_alert
        send_critical_alert("Critical: Transcription Task Failed", f"Call SID: {call_sid}\nError: {e}")

def transcribe_recording_async(recording_url: str, call_sid: str, caller_number: str, 
                               tenant_id: str, lead_id: str = None):
    """
    Enqueues transcription job to Redis (RQ) or falls back to Thread.
    """
    if start_rq:
        try:
            job = q.enqueue(transcription_task, args=(recording_url, call_sid, caller_number, tenant_id, lead_id),
                            job_timeout='2m', result_ttl=86400)
            logger.info(f"🚀 Transcription queued (RQ): {job.id} for {call_sid}")
            return
        except Exception as e:
            logger.error(f"Failed to enqueue to Redis: {e}. Falling back to Thread.")
            
    # Fallback to Thread
    thread = threading.Thread(target=transcription_task, args=(recording_url, call_sid, caller_number, tenant_id, lead_id), daemon=True)
    thread.start()
    logger.info(f"🚀 Transcription queued (Thread Fallback): {call_sid}")


def _fetch_twilio_transcription(recording_url: str, call_sid: str, timeout: int = 30) -> str:
    """
    Fetches transcription text from a Twilio recording with optimized polling.
    
    This function implements intelligent polling to minimize latency:
    - Starts with 1-second intervals for fast transcriptions
    - Switches to 2-second intervals after 10 seconds
    - Maximum wait time of 45 seconds (reduced from 60s)
    
    The function handles various error conditions gracefully and never crashes
    the application, ensuring system reliability even when Twilio services are down.
    
    Args:
        recording_url (str): Full URL to the Twilio recording
            Format: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Recordings/{RecordingSid}
        call_sid (str): Twilio Call SID for logging and tracking
        timeout (int): HTTP request timeout in seconds (default: 30)
    
    Returns:
        str|None: 
            - Transcription text if successfully retrieved
            - None if transcription unavailable, failed, or timed out
    
    Polling Strategy:
        - Interval 1: 0-10 seconds: Poll every 1 second (fast response)
        - Interval 2: 10-45 seconds: Poll every 2 seconds (reduce API calls)
        - Maximum wait: 45 seconds total
    
    Error Handling:
        - TwilioRestException: 
            - Auth errors (20003): Returns None immediately (no retry)
            - Not found (20404): Returns None immediately (no retry)
            - Other errors: Continues retrying until timeout
        - requests.RequestException: Network errors, retries with backoff
        - All errors: Logged with full context for debugging
    
    Performance:
        - Average transcription time: 20-30 seconds
        - Fastest: 5-10 seconds (short calls)
        - Slowest: 40-45 seconds (long calls or API delays)
    
    Example:
        >>> transcript = _fetch_twilio_transcription(
        ...     "https://api.twilio.com/.../Recordings/RE123",
        ...     "CA1234567890"
        ... )
        >>> if transcript:
        ...     print(f"Got transcript: {transcript[:50]}...")
    """
    try:
        from execution.services.twilio_service import get_twilio_service
        from twilio.base.exceptions import TwilioRestException
        import requests
        
        twilio = get_twilio_service()
        if not twilio.client:
            logger.warning(f"Twilio client not available for transcription")
            return None
        
        # Extract Recording SID from URL
        # Twilio recording URLs format: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Recordings/{RecordingSid}
        if not recording_url or not isinstance(recording_url, str):
            logger.error(f"Invalid recording URL: {recording_url}")
            return None
        
        recording_sid = recording_url.split('/')[-1] if '/' in recording_url else None
        
        if not recording_sid:
            logger.error(f"Could not extract Recording SID from URL: {recording_url}")
            return None
        
        # Fetch transcription from Twilio
        # Note: Twilio transcriptions are created automatically if enabled
        # Optimized polling: Start with 1s intervals, increase to 2s after 10s
        # This reduces latency for fast transcriptions while avoiding excessive API calls
        max_wait = 45  # Wait up to 45 seconds for transcription (reduced from 60s)
        wait_interval = 1  # Start with 1s intervals for faster response
        elapsed = 0
        
        while elapsed < max_wait:
            try:
                # Get transcriptions for this recording
                transcriptions = twilio.client.recordings(recording_sid).transcriptions.list()
                
                if transcriptions and len(transcriptions) > 0:
                    # Get the most recent transcription
                    transcription = transcriptions[0]
                    if transcription.status == 'completed':
                        # Fetch the transcription text
                        transcription_uri = f"{twilio.client.base_url}/Accounts/{twilio.account_sid}/Transcriptions/{transcription.sid}.json"
                        response = requests.get(transcription_uri, auth=(twilio.account_sid, twilio.auth_token), timeout=timeout)
                        
                        if response.status_code == 200:
                            data = response.json()
                            transcript_text = data.get('text', '')
                            if transcript_text:
                                logger.info(f"✅ Transcription retrieved for {call_sid}: {len(transcript_text)} chars")
                                return transcript_text
                    elif transcription.status == 'failed':
                        logger.warning(f"Transcription failed for {call_sid}")
                        return None
                
                # Transcription not ready yet, wait and retry
                # Adaptive polling: Increase interval after 10s to reduce API calls
                if elapsed >= 10:
                    wait_interval = 2  # Switch to 2s intervals after 10s
                time.sleep(wait_interval)
                elapsed += wait_interval
                
            except TwilioRestException as e:
                # Twilio API errors - log and return None (graceful degradation)
                error_code = getattr(e, 'code', None)
                logger.error(f"Twilio API error fetching transcription (Code: {error_code}): {e}")
                # Don't retry on auth errors or invalid recording
                if error_code in [20003, 20404]:  # Auth error or resource not found
                    return None
                # For other errors, continue retrying
                time.sleep(wait_interval)
                elapsed += wait_interval
                continue
            except requests.RequestException as e:
                # Network errors - retry with backoff
                logger.warning(f"Network error fetching transcription (retrying): {e}")
                time.sleep(wait_interval)
                elapsed += wait_interval
                continue
            except Exception as e:
                # Unexpected errors - log and retry
                logger.error(f"Unexpected error fetching transcription (retrying): {e}")
                time.sleep(wait_interval)
                elapsed += wait_interval
                continue
        
        logger.warning(f"Transcription timeout for {call_sid} after {max_wait}s")
        return None
        
    except Exception as e:
        logger.error(f"Error in transcription fetch: {e}", exc_info=True)
        return None


def _fetch_whisper_transcription(recording_url: str, call_sid: str) -> str:
    """
    Downloads recording and transcribes using OpenAI Whisper.
    
    Enhanced with comprehensive error handling for network issues, timeouts,
    and file operations. This ensures the system remains stable even when
    external services are unavailable.
    
    Args:
        recording_url (str): URL to the Twilio recording
        call_sid (str): Call SID for logging and tracking
    
    Returns:
        str|None: Transcribed text if successful, None otherwise
    
    Error Handling:
        - Network errors: Logged and returns None gracefully
        - Timeout errors: 30-second timeout prevents hanging
        - File errors: Temp files always cleaned up
        - API errors: Logged but doesn't crash application
    """
    try:
        # Check if OpenAI is available
        ai_service = get_openai_service()
        if not ai_service or not ai_service.client:
            logger.debug(f"Whisper not available (no OpenAI client) for {call_sid}")
            return None
            
        logger.info(f"🎧 Downloading audio for Whisper transcription: {call_sid}")
        
        # Download with timeout and comprehensive error handling
        try:
            response = requests.get(
                recording_url + ".mp3", 
                stream=True, 
                timeout=30,  # 30s timeout for download
                headers={'User-Agent': 'PlumberAI-Transcription/1.0'}
            )
            response.raise_for_status()  # Raise for 4xx/5xx errors
            
            if response.status_code != 200:
                logger.warning(f"Failed to download recording: HTTP {response.status_code}")
                return None
        except requests.Timeout:
            logger.error(f"Timeout downloading audio for {call_sid} (30s)")
            return None
        except requests.RequestException as e:
            logger.error(f"Network error downloading audio for {call_sid}: {e}")
            return None
            
        # Save to temp file with error handling
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as temp_audio:
                temp_path = temp_audio.name
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # Filter out keep-alive chunks
                        temp_audio.write(chunk)
            
            # Verify file was written
            if not os.path.exists(temp_path) or os.path.getsize(temp_path) == 0:
                logger.error(f"Downloaded audio file is empty for {call_sid}")
                return None
            
            # Transcribe with error handling
            try:
                logger.info(f"🧠 Sending {call_sid} audio to Whisper...")
                text = ai_service.transcribe_audio(temp_path)
                if text and len(text.strip()) > 0:
                    logger.info(f"✅ Whisper Transcription Success: {len(text)} chars")
                    return text.strip()
                else:
                    logger.warning(f"Whisper returned empty transcript for {call_sid}")
                    return None
            except Exception as e:
                logger.error(f"Whisper transcription API error for {call_sid}: {e}")
                return None
        finally:
            # Always cleanup temp file
            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception as e:
                    logger.warning(f"Failed to cleanup temp file {temp_path}: {e}")
                
    except Exception as e:
        logger.error(f"Whisper Transcription Error for {call_sid}: {e}", exc_info=True)
        return None


def get_transcription_streaming_url(recording_url: str) -> str:
    """
    Gets a streaming URL for real-time transcription (if supported).
    
    This is a placeholder for future streaming transcription implementation.
    Streaming would allow classification to happen in real-time as the call progresses.
    
    Args:
        recording_url: URL to the recording
    
    Returns:
        str: Streaming URL, or None if not supported
    
    Note: Twilio doesn't natively support streaming transcription, but this could
    integrate with services like Deepgram, AssemblyAI, or Google Speech-to-Text
    for real-time transcription.
    """
    # TODO: Implement streaming transcription with external service
    # Example: Deepgram, AssemblyAI, or Google Speech-to-Text
    return None

```
---

### File: `execution/services/openai_service.py`
```python
import os
import json
import logging
from execution.utils.logger import setup_logger

logger = setup_logger("OpenAIService")

class OpenAIService:
    _instance = None
    
    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.model = "gpt-4o"
        self.client = None
        
        if not self.api_key:
            logger.warning("⚠️ OPENAI_API_KEY not found in environment. AI features will be disabled.")
        else:
            try:
                from openai import OpenAI
                self.client = OpenAI(api_key=self.api_key)
                logger.info(f"✅ OpenAI Service Initialized (Model: {self.model})")
            except ImportError:
                logger.error("❌ 'openai' library not installed. Please run: pip install openai")
            except Exception as e:
                logger.error(f"❌ Failed to initialize OpenAI client: {e}")

    
    def transcribe_audio(self, audio_file_path):
        """
        Transcribes an audio file using OpenAI Whisper (gpt-whisper).
        
        Args:
            audio_file_path (str): Local path to the audio file (mp3/wav)
            
        Returns:
            str: Transcribed text or None if failed
        """
        if not self.client:
            logger.warning("Skipping Whisper transcription (No Client)")
            return None
            
        try:
            with open(audio_file_path, "rb") as audio_file:
                transcript = self.client.audio.transcriptions.create(
                    model="whisper-1", 
                    file=audio_file
                )
            return transcript.text
        except Exception as e:
            logger.error(f"Whisper Transcription Failed: {e}")
            return None

    def classify_intent(self, message_body):
        """
        Classifies a plumber lead message into Emergency, Standard, or Spam using GPT-4o.
        
        Args:
            message_body (str): The customer message to classify
        
        Returns:
            dict: {
                "urgency": "emergency" | "standard" | "spam",
                "confidence": float (0.0 - 1.0),
                "reasoning": str
            } or None if classification fails
        """
        if not self.client:
            logger.warning("Skipping AI classification (No Client)")
            return None

        # FIX: Define truncated_message_body before using it
        truncated_message_body = message_body[:500] if len(message_body) > 500 else message_body

        prompt = f"""
        You are an expert dispatcher for a plumbing company. Classify the urgency of this customer message.

        Message: "{truncated_message_body}"

        Classification Rules (STRICT):
        1. EMERGENCY (urgency: "emergency"):
           - Active water damage: "water everywhere", "flooding", "gushing", "cannot stop"
           - Burst/exploded pipes: "pipe burst", "pipe exploded", "water shooting out"
           - Complete water loss: "no water at all", "water completely off"
           - Dangerous situations: "gas smell", "sewage backup", "sewage overflow"
           - Immediate danger: "water in basement", "ceiling leaking badly"
           - Context clues: "right now", "immediately", "asap", "emergency"

        2. STANDARD (urgency: "standard"):
           - Routine maintenance: "leaky faucet", "dripping", "small leak"
           - Quotes/estimates: "how much", "quote", "price", "estimate", "cost"
           - Scheduling: "schedule", "appointment", "when can you come", "next week"
           - Non-urgent: "not urgent", "can wait", "when convenient"
           - General questions: "do you do", "can you fix", "what services"

        3. SPAM (urgency: "spam"):
           - Marketing messages
           - Wrong number responses
           - Completely irrelevant text

        Output ONLY valid JSON (no markdown, no explanation):
        {{
            "urgency": "emergency" | "standard" | "spam",
            "confidence": 0.0 to 1.0,
            "reasoning": "brief one-sentence explanation"
        }}
        """

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a helpful JSON-only assistant."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.0
            )
            
            content = response.choices[0].message.content
            # Use the new _extract_json helper for robust parsing
            result = self._extract_json(content)
            return result
            
        except Exception as e:
            logger.error(f"AI Classification Failed: {e}")
            return None

    def _extract_json(self, content: str) -> dict:
        """
        Safely extracts JSON from OpenAI response, handling edge cases.
        
        This method handles various response formats that OpenAI might return:
        - Direct JSON strings
        - JSON wrapped in markdown code blocks (```json ... ```)
        - JSON objects embedded in text
        
        Args:
            content (str): Raw response content from OpenAI API
        
        Returns:
            dict: Parsed JSON or None if parsing fails
        
        Error Handling:
            - Logs errors but doesn't crash
            - Returns None on any parsing failure
        """
        try:
            # Try direct JSON parse first (most common case)
            return json.loads(content)
        except json.JSONDecodeError:
            # Try extracting JSON from markdown code blocks
            import re
            json_match = re.search(r'```json\s*(.*?)\s*```', content, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group(1))
                except json.JSONDecodeError:
                    pass
            
            # Try extracting JSON object from text
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group(0))
                except json.JSONDecodeError:
                    pass
            
            logger.error(f"Failed to extract JSON from OpenAI response: {content[:200]}")
            return None


# Singleton Accessor
def get_openai_service():
    if OpenAIService._instance is None:
        OpenAIService._instance = OpenAIService()
    return OpenAIService._instance

```
---

### File: `dashboard/app/page.tsx`
```typescript
'use client';

import { useState, useEffect } from 'react';
import Image from 'next/image';
import { LayoutDashboard, Users, DollarSign, Activity, Settings, Download, MessageSquare } from 'lucide-react';
import KpiCard from '@/components/KpiCard';
import StatusToggle from '@/components/StatusToggle';
import ActivityFeed from '@/components/ActivityFeed';
import DashboardChart from '@/components/DashboardChart';
import LeadsModal from '@/components/LeadsModal';
import Footer from '@/components/Footer';

import useSWR from 'swr';
import { useRouter } from 'next/navigation';
import { API_URL } from '../lib/api';

const fetcher = async (url: string) => {
    try {
        const res = await fetch(url, { credentials: 'include' });
        if (!res.ok) {
            const error = new Error('An error occurred while fetching the data.') as any;
            try {
                error.info = await res.json();
            } catch (e) {
                error.info = { error: 'Unknown error' };
            }
            error.status = res.status;
            throw error;
        }
        return res.json();
    } catch (err: any) {
        // Network errors or fetch failures
        if (err.name === 'TypeError' || err.message.includes('fetch')) {
            console.error('Network error:', err);
            // Return cached data if available, or throw with network error
            throw new Error('Network error. Please check your connection.');
        }
        throw err;
    }
};

export default function Home() {
    const router = useRouter();
    const [isActive, setIsActive] = useState(true); // Default (optimistic)
    const [isLeadsModalOpen, setIsLeadsModalOpen] = useState(false);
    const [businessName, setBusinessName] = useState('');
    const [period, setPeriod] = useState<'week' | 'month' | 'lifetime'>('week');
    const { data, error, mutate: mutateStats } = useSWR(`${API_URL}/api/stats?period=${period}`, fetcher, {
        refreshInterval: (data) => {
            // Adaptive polling: less frequent if no new data
            return data?.lastUpdate ? 30000 : 5000;
        },
        revalidateOnFocus: false,
        revalidateOnReconnect: true,
        shouldRetryOnError: (error: any) => {
            // Don't retry on 4xx errors (client errors)
            return error?.status >= 500;
        },
        errorRetryCount: 3,
        errorRetryInterval: 5000,
        onError: (err: any) => {
            if (err.status !== 401) {
                console.error('API Error:', err);
                // Could show toast notification here
            }
        }
    });

    // Fetch initial AI Active status
    useEffect(() => {
        const fetchSettings = async () => {
            try {
                const res = await fetch(`${API_URL}/api/settings`, { credentials: 'include' });
                if (res.ok) {
                    const settings = await res.json();
                    setIsActive(settings.ai_active);
                }
            } catch (e) {
                console.error("Failed to fetch settings", e);
            }
        };
        fetchSettings();
    }, []);

    const toggleAI = async () => {
        const newState = !isActive;
        setIsActive(newState); // Optimistic UI update

        try {
            await fetch(`${API_URL}/api/settings/toggle_ai`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ active: newState }),
                credentials: 'include'
            });
        } catch (e) {
            console.error("Failed to toggle AI", e);
            setIsActive(!newState); // Revert on error
        }
    };

    useEffect(() => {
        const tenant = localStorage.getItem('tenant_id');
        const name = localStorage.getItem('business_name');
        if (!tenant) {
            router.push('/login');
        } else if (name) {
            setBusinessName(name);
        }
    }, [router]);

    // Handle Auth Errors
    useEffect(() => {
        if (error?.status === 401) {
            localStorage.removeItem('tenant_id');
            localStorage.removeItem('business_name');
            router.push('/login');
        }
    }, [error, router]);

    const kpi = data?.kpi || { leads: 0, success_rate: '0.0%', revenue: 0, estimated_cost: 0 };
    const loading = !data;

    return (
        <main className="min-h-screen bg-brand-black text-white font-sans relative overflow-x-hidden flex flex-col">
            {/* Background Effects */}
            <div className="absolute top-0 left-0 w-full h-full overflow-hidden pointer-events-none">
                <div className="absolute top-[-10%] left-[-10%] w-[40%] h-[40%] bg-brand-lime/5 rounded-full blur-[120px]" />
                <div className="absolute bottom-[-10%] right-[-10%] w-[40%] h-[40%] bg-brand-lime/5 rounded-full blur-[120px]" />
            </div>

            {/* Content Wrapper - flex-grow applies here */}
            <div className="w-full max-w-7xl mx-auto space-y-8 relative z-10 p-4 md:p-8 flex-1 pb-16">
                <header className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                    <div>
                        <div className="relative w-80 h-20 mb-2 -ml-8">
                            <Image
                                src="/logo.jpg?v=2"
                                alt="YourPlumberAI"
                                fill
                                className="object-contain object-left mix-blend-screen opacity-90"
                            />
                        </div>
                        <p className="text-gray-500 text-[10px] uppercase tracking-[0.2em] font-black mt-1">Never miss a lead</p>
                    </div>

                    <div className="flex items-center gap-6">
                        <div className="hidden md:flex gap-3">
                            <span className="flex items-center gap-1.5 px-3 py-1 rounded-full bg-brand-lime/10 border border-brand-lime/20 text-xs font-bold text-brand-lime uppercase tracking-wider">
                                <div className={`w-1.5 h-1.5 rounded-full ${loading ? 'bg-gray-400' : 'bg-brand-lime animate-pulse'}`} />
                                {loading ? 'Connecting...' : 'Systems Online'}
                            </span>
                        </div>

                        <div className="h-8 w-px bg-white/10 hidden md:block" />
                        <StatusToggle isActive={isActive} onToggle={toggleAI} />
                    </div>
                </header>

                <div className="flex flex-col gap-6 -mt-4">
                    {businessName && (
                        <div className="ml-1 mb-2">
                            <p className="text-2xl uppercase font-black tracking-[0.1em] text-brand-lime leading-none drop-shadow-[0_0_12px_rgba(204,255,0,0.8)]">
                                {businessName}
                            </p>
                        </div>
                    )}
                    <div className="flex flex-col sm:flex-row gap-4 sm:items-center w-full sm:w-auto">
                        <button
                            onClick={() => setIsLeadsModalOpen(true)}
                            className="flex items-center justify-center gap-2 px-6 py-3 rounded-full bg-brand-gray hover:bg-[#1a1a1a] border border-brand-border text-xs font-bold uppercase tracking-wide text-gray-300 hover:text-white hover:border-brand-lime/50 transition-all whitespace-nowrap w-full sm:w-auto"
                        >
                            <Users size={16} />
                            View All Leads
                        </button>

                        {/* Time Period Selector */}
                        <div className="flex w-full sm:w-auto bg-brand-gray border border-brand-border rounded-full p-1 gap-1">
                            {(['week', 'month', 'lifetime'] as const).map((p) => (
                                <button
                                    key={p}
                                    onClick={() => setPeriod(p)}
                                    className={`flex-1 sm:flex-none px-4 py-2 rounded-full text-xs font-bold uppercase tracking-wide transition-all text-center whitespace-nowrap ${period === p
                                        ? 'bg-brand-lime text-black shadow-[0_0_10px_rgba(204,255,0,0.3)]'
                                        : 'text-gray-400 hover:text-white hover:bg-white/5'
                                        }`}
                                >
                                    {p === 'week' ? 'This Week' : p === 'month' ? 'This Month' : 'Lifetime'}
                                </button>
                            ))}
                        </div>
                    </div>
                </div>

                </div>

                {/* KPI Grid - Updated to 4 cols for Cost Metric */}
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                    <KpiCard
                        title="Missed Calls Caught"
                        value={loading ? "..." : kpi.leads.toString()}
                        icon={Users}
                        trend={kpi.leads_trend}
                        trendUp={true}
                        delay={0}
                    />
                    <KpiCard
                        title="Success Rate"
                        value={loading ? "..." : kpi.success_rate}
                        icon={Activity}
                        trend="Top 1% performing"
                        trendUp={true}
                        delay={0.1}
                    />
                    <KpiCard
                        title="Revenue Saved"
                        value={loading ? "..." : `$${kpi.revenue.toLocaleString()}`}
                        icon={DollarSign}
                        trend="Est. based on misses"
                        trendUp={true}
                        delay={0.2}
                    />
                    <KpiCard
                        title="Est. MTD Cost"
                        value={loading ? "..." : `$${(kpi.estimated_cost || 0).toFixed(2)}`}
                        icon={Settings}
                        trend="Real-time Usage"
                        trendUp={false} 
                        delay={0.3}
                    />
                </div>

                {/* Main Content Grid - Responsive with min-heights to prevent overlap */}
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                    {/* Chart Area - Responsive height */}
                    <div className="lg:col-span-2 min-h-[400px] lg:h-[450px] flex flex-col">
                        <DashboardChart data={data?.chart_data} />
                    </div>

                    {/* Activity Feed - Responsive height with overflow handling */}
                    <div className="min-h-[400px] lg:h-[450px] flex flex-col">
                        <ActivityFeed />
                    </div>
                </div>

                {/* Spacer to guarantee footer separation */}
                <div className="h-12" />
            </div>

            <LeadsModal
                isOpen={isLeadsModalOpen}
                onClose={() => setIsLeadsModalOpen(false)}
            />

            <Footer />
        </main >
    );
}

```
---

### File: `dashboard/components/ActivityFeed.tsx`
```typescript
'use client';

import { motion, AnimatePresence } from 'framer-motion';
import { Phone, AlertTriangle, Clock, Activity, RefreshCw } from 'lucide-react';
import useSWR from 'swr';
import { API_URL } from '../lib/api';

interface ActivityItem {
    id: string; // Log entry ID
    lead_id: string; // Lead ID
    phone: string; // Masked phone
    status: string; // EMERGENCY, STANDARD REQ, or empty
    timestamp: string;
    business?: string;
}

const fetcher = async (url: string) => {
    const res = await fetch(url, { credentials: 'include' });
    if (!res.ok) {
        const error = new Error('An error occurred while fetching activity.');
        (error as any).status = res.status;
        throw error;
    }
    return res.json();
};

export default function ActivityFeed() {
    const { data: activities, error, isLoading, mutate } = useSWR(`${API_URL}/api/activity`, fetcher, {
        refreshInterval: 3000, // Auto-refresh every 3s (Real-time feel)
        shouldRetryOnError: (err: any) => err.status !== 401
    });

    const items: ActivityItem[] = Array.isArray(activities) ? activities : [];

    return (
        <div className="bg-brand-gray border border-brand-border rounded-3xl p-6 h-full min-h-[400px] relative overflow-hidden group/feed flex flex-col">
            {/* Background Glow */}
            <div className="absolute -top-24 -right-24 w-48 h-48 bg-brand-lime/5 rounded-full blur-[60px] pointer-events-none group-hover/feed:bg-brand-lime/10 transition-colors" />

            <h3 className="text-lg font-bold text-white mb-6 flex items-center gap-2 relative z-10">
                <Clock size={18} className="text-brand-lime" />
                <span className="uppercase tracking-tight">Live Activity</span>
                <button
                    onClick={() => mutate()}
                    className="p-1 hover:bg-white/10 rounded-full transition-colors ml-auto text-gray-500 hover:text-brand-lime"
                    title="Refresh Now"
                >
                    <RefreshCw size={14} />
                </button>
            </h3>

            <div className="space-y-3 relative z-10 flex-1 overflow-y-auto pr-2 scrollbar-thin scrollbar-thumb-brand-lime/20 scrollbar-track-transparent">
                <AnimatePresence>
                    {items.map((item, index) => (
                        <motion.div
                            key={item.id}
                            initial={{ opacity: 0, x: -10 }}
                            animate={{ opacity: 1, x: 0 }}
                            transition={{ delay: index * 0.05 }}
                            className="flex items-center gap-4 p-4 rounded-2xl bg-[#0a0a0a] border border-brand-border hover:border-brand-lime/30 group transition-all"
                        >
                            <div className={`p-2.5 rounded-xl border ${item.status === 'EMERGENCY' ? 'bg-red-500/10 border-red-500/20 text-red-400' :
                                'bg-gray-500/10 border-white/5 text-gray-500'
                                }`}>
                                {item.status === 'EMERGENCY' ? <AlertTriangle size={18} /> : <Phone size={18} />}
                            </div>

                            <div className="flex-1 min-w-0">
                                <div className="flex items-center justify-between mb-0.5">
                                    <p className="text-sm font-bold text-white tracking-tight">{item.phone}</p>
                                    <span className="text-[10px] text-gray-600 font-mono">{item.timestamp}</span>
                                </div>
                                <div className="flex items-center justify-between">
                                    <p className="text-xs text-gray-500 truncate max-w-[120px]">{item.business}</p>
                                    <span className={`text-[9px] uppercase font-black tracking-widest ${item.status === 'EMERGENCY' ? 'text-red-400' :
                                        'text-gray-600'
                                        }`}>
                                        {item.status}
                                    </span>
                                </div>
                            </div>
                        </motion.div>
                    ))}
                </AnimatePresence>

                {items.length === 0 && !error && !isLoading && (
                    <div className="flex flex-col items-center justify-center py-10 text-gray-600">
                        <Activity size={32} className="mb-2 opacity-20" />
                        <p className="text-[10px] uppercase font-bold tracking-widest">Waiting for activity...</p>
                    </div>
                )}

                {error && (
                    <div className="flex flex-col items-center justify-center py-10 text-red-400/50">
                        <AlertTriangle size={32} className="mb-2 opacity-20" />
                        <p className="text-[10px] uppercase font-bold tracking-widest">{error.status === 401 ? 'Session Expired' : 'Failed to load'}</p>
                    </div>
                )}

                {isLoading && items.length === 0 && (
                    <div className="flex flex-col items-center justify-center py-10 text-gray-600 animate-pulse">
                        <Activity size={32} className="mb-2 opacity-20" />
                        <p className="text-[10px] uppercase font-bold tracking-widest">Syncing...</p>
                    </div>
                )}
            </div>
        </div>
    );
}

```
---

### File: `dashboard/lib/api.ts`
```typescript
export const API_URL = process.env.NEXT_PUBLIC_API_URL ?? ''; // Empty default for relative paths (Caddy)

```
---

### File: `.env.example`
```bash
TWILIO_ACCOUNT_SID=
TWILIO_AUTH_TOKEN=
TWILIO_PHONE_NUMBER=
TWILIO_MESSAGING_SERVICE_SID=
PLUMBER_PHONE_NUMBER=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
OPENAI_API_KEY=
SERVER_URL=
SAFE_MODE=ON
KILL_SWITCH=OFF
TIMEZONE=America/Los_Angeles
FLASK_APP=run_app.py
PORT=5002

```
---
### File: `scripts/install_production_stack.sh`
```bash
#!/bin/bash
# 🚀 Production Stack Installer (Option B)
# Installs PostgreSQL, Redis, PM2, and secures the server.
# Usage: sudo ./scripts/install_production_stack.sh

set -e # Exit on error

echo "📦 Updating Package List..."
apt-get update

# 1. Install Dependencies
echo "🔧 Installing System Dependencies..."
apt-get install -y python3-pip python3-venv postgresql postgresql-contrib redis-server nginx ufw build-essential libpq-dev

# 2. Secure Database (Firewall)
echo "🛡️ Configuring Firewall..."
ufw allow OpenSSH
ufw allow 'Nginx Full'
# Explicitly DENY external access to Postgres (5432) and Redis (6379)
ufw deny 5432
ufw deny 6379
ufw --force enable
echo "✅ Firewall Active. DB ports are locked."

# 3. Configure Redis
echo "⚡ Configuring Redis..."
# Ensure Redis is supervised by systemd
sed -i 's/supervised no/supervised systemd/' /etc/redis/redis.conf
systemctl restart redis.service
systemctl enable redis.service

# 4. Configure PostgreSQL
echo "🐘 Configuring PostgreSQL..."
systemctl start postgresql
systemctl enable postgresql

# Create Database and User (Interactive-less)
# We set a default password 'plumber_strong_password' - CHANGE THIS LATER in .env
sudo -u postgres psql -c "CREATE DATABASE plumber_db;" || echo "DB exists"
sudo -u postgres psql -c "CREATE USER plumber_user WITH PASSWORD 'plumber_strong_password';" || echo "User exists"
sudo -u postgres psql -c "ALTER ROLE plumber_user SET client_encoding TO 'utf8';"
sudo -u postgres psql -c "ALTER ROLE plumber_user SET default_transaction_isolation TO 'read committed';"
sudo -u postgres psql -c "ALTER ROLE plumber_user SET timezone TO 'UTC';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE plumber_db TO plumber_user;"

# 5. Install PM2 (Process Manager) & Log Maintenance
echo "🚀 Installing PM2..."
if ! command -v npm &> /dev/null
then
    apt-get install -y nodejs npm
fi
npm install pm2 -g
# RISK FIX: Install Log Rotation to prevent disk overflow
pm2 install pm2-logrotate
pm2 set pm2-logrotate:max_size 10M
pm2 set pm2-logrotate:retain 7
pm2 startup systemd

echo "✅ Production Stack Installed!"
echo "------------------------------------------------"
echo "Next Steps:"
echo "1. Update your .env file:"
echo "   DATABASE_URL=postgresql://plumber_user:plumber_strong_password@localhost/plumber_db"
echo "   REDIS_URL=redis://localhost:6379/0"
echo "2. Run the migration script: python3 scripts/migrate_to_postgres.py"
echo "3. Start services: ./scripts/run_pm2.sh"
echo "------------------------------------------------"
```
---

### File: `scripts/migrate_to_postgres.py`
```python
import sqlite3
import psycopg2
import os
import sys
from datetime import datetime

# CONFIGURATION
SQLITE_DB = "plumber.db"
# Default local postgres creds from installer script
PG_HOST = "localhost"
PG_DB = "plumber_db"
PG_USER = "plumber_user"
PG_PASS = "plumber_strong_password"

def migrate():
    print(f"🚀 Starting Migration: {SQLITE_DB} -> PostgreSQL...")
    
    if not os.path.exists(SQLITE_DB):
        print(f"❌ SQLite DB not found at {SQLITE_DB}")
        sys.exit(1)

    # 1. Connect to SQLite
    sqlite_conn = sqlite3.connect(SQLITE_DB)
    sqlite_conn.row_factory = sqlite3.Row
    sqlite_cur = sqlite_conn.cursor()

    # 2. Connect to Postgres
    try:
        pg_conn = psycopg2.connect(
            host=PG_HOST,
            database=PG_DB,
            user=PG_USER,
            password=PG_PASS
        )
        pg_cur = pg_conn.cursor()
    except Exception as e:
        print(f"❌ Could not connect to Postgres: {e}")
        print("Did you run ./scripts/install_production_stack.sh?")
        sys.exit(1)

    # 3. Get Tables
    sqlite_cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row['name'] for row in sqlite_cur.fetchall() if row['name'] != 'sqlite_sequence']

    for table in tables:
        print(f"📦 Migrating table: {table}...")
        
        # Get data
        sqlite_cur.execute(f"SELECT * FROM {table}")
        rows = sqlite_cur.fetchall()
        
        if not rows:
            print(f"   Skipping (Empty)")
            continue
            
        # Get columns
        col_names = [description[0] for description in sqlite_cur.description]
        cols_str = ", ".join(col_names)
        placeholders = ", ".join(["%s"] * len(col_names))
        
        # Insert into Postgres
        # Note: We assume schema is already created via init_db logic or we create it dynamically.
        # For simplicity in this script, we assume the application 'init_db' has run against Postgres
        # effectively creating empty tables.
        
        count = 0
        for row in rows:
            try:
                pg_cur.execute(
                    f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                    tuple(row)
                )
                count += 1
            except Exception as e:
                print(f"   ⚠️ Row Error: {e}")
        
        print(f"   ✅ Migrated {count} rows.")

    # Commit and Close
    pg_conn.commit()
    sqlite_conn.close()
    pg_conn.close()
    print("✨ Migration Complete!")

if __name__ == "__main__":
    migrate()
```
---

### File: `scripts/run_pm2.sh`
```bash
#!/bin/bash
# Patch 3: Deployment Safety with PM2
# Usage: ./scripts/run_pm2.sh

# Ensure PM2 is installed
if ! command -v pm2 &> /dev/null
then
    echo "PM2 could not be found. Installing via npm..."
    npm install pm2 -g
fi

echo "🚀 Starting PlumberAI with PM2..."

# 1. Start the Flask App (Webhook Server)
pm2 start execution/run_app.py --interpreter python3 --name plumber-web --restart-delay=3000

# 2. Start the SMS Worker (Background Job)
pm2 start execution/utils/sms_engine.py --interpreter python3 --name plumber-worker --restart-delay=3000

echo "✅ System Online. View logs with: pm2 logs"
pm2 save
```
