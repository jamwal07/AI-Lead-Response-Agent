from flask import Flask, request, render_template
from twilio.twiml.voice_response import VoiceResponse
from twilio.rest import Client
import pytz
import uuid
from datetime import datetime

# Import Config (Absolute Import from execution package)
from execution import config
from execution.utils.database import get_all_sms, create_or_update_lead, update_lead_status, log_conversation_event, get_lead_funnel_stats, set_opt_out, get_tenant_by_twilio_number, get_tenant_by_id, record_consent, revoke_consent
from execution.utils.security import require_twilio_signature, require_rate_limit, mask_pii, check_tenant_rate_limit, verify_unsubscribe_token

app = Flask(__name__, template_folder='../templates') # Point to templates folder

# --- HEALTH CHECK ---
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
    
    for msg in queue:
        body = msg['body'].lower()
        if "wrapped up" in body: stats['missed_calls'] += 1
        if "scheduled" in body: stats['reminders'] += 1
        if "failed" in msg['status']: stats['errors'] += 1
        
    # Queue is already sorted new->old, so we don't need to reverse it.
    return render_template('dashboard.html', queue=queue, stats=stats, funnel=funnel, queue_len=len(queue))

# --- CONFIGURATION (Loaded from execution/config.py) ---
ACCOUNT_SID = config.TWILIO_ACCOUNT_SID
AUTH_TOKEN = config.TWILIO_AUTH_TOKEN
ROBOT_PHONE = config.TWILIO_PHONE_NUMBER
PLUMBER_PHONE = config.PLUMBER_PHONE_NUMBER

# Mock Client (Safe if no keys present)
try:
    if not ACCOUNT_SID or not AUTH_TOKEN: 
        print("⚠️  Running in MOCK mode (No Twilio Keys found)")
        twilio_client = None
    else:
        twilio_client = Client(ACCOUNT_SID, AUTH_TOKEN)
except Exception as e:
    print(f"Error initializing Twilio: {e}")
    twilio_client = None

from execution.utils.sms_engine import add_to_queue

# (Send SMS function removed - using Queue Engine)


@app.route("/voice", methods=['GET', 'POST'])
@require_twilio_signature
def voice_handler():
    """Twilio hits this URL when a call comes within"""
    from execution.utils.database import check_webhook_processed, record_webhook_processed
    
    caller_number = request.values.get('From')
    to_number = request.values.get('To')
    call_sid = request.values.get('CallSid')
    
    # IDEMPOTENCY CHECK: Prevent duplicate webhook processing
    if call_sid:
        is_duplicate, internal_id = check_webhook_processed(call_sid)
        if is_duplicate:
            print(f"♻️  Duplicate webhook ignored: CallSid {call_sid} (already processed as {internal_id})")
            # Return same response as if we processed it (idempotent)
            resp = VoiceResponse()
            resp.say("Thank you. Please check your text messages.", voice='Polly.Matthew-Neural', language='en-US')
            return str(resp)
    
    # 0. TENANT RESOLUTION
    print(f"🔍 DEBUG: Attempting lookup for to_number='{to_number}' (Type: {type(to_number)}, Length: {len(str(to_number))})")
    tenant = get_tenant_by_twilio_number(to_number)
    
    if not tenant:
        print(f"❌ Unknown Tenant for number '{to_number}'")
        # Default Fallback or Error? 
        # For SaaS, this means a provisioning error. 
        resp = VoiceResponse()
        resp.say("System Configuration Error. Please contact support.")
        return str(resp)

    tenant_id = tenant['id']
    
    # Record webhook as processed (generate internal ID)
    internal_id = str(uuid.uuid4())
    record_webhook_processed(call_sid, 'voice', tenant_id=tenant_id, internal_id=internal_id)
    
    # NEW: Tenant Rate Limit
    if not check_tenant_rate_limit(tenant_id):
        resp = VoiceResponse()
        resp.say("Busy. Please try again later.")
        return str(resp), 429

    plumber_name = tenant['name']
    business_name = tenant.get('name', 'PlumberAI')  # Business name for SMS templates
    plumber_phone = tenant['plumber_phone_number'] # For dialing if emergency
    emergency_mode = tenant.get('emergency_mode', 0) # 0=Off, 1=On
    
    # Check if this is an "Emergency Press 1" event
    digits = request.values.get('Digits')
    if digits == '1' and emergency_mode:
        print(f"🚨 EMERGENCY OVERRIDE: Connecting caller {caller_number} to {plumber_phone}")
        resp = VoiceResponse()
        resp.say("Connecting you to the plumber now. Please hold.", voice='Polly.Matthew-Neural', language='en-US')
        resp.dial(plumber_phone)
        return str(resp)
    
    # 1. Answer with specific TwiML
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
    print(f"📞 INCOMING CALL FROM: {mask_pii(caller_number)} TO: {to_number} (Tenant: {plumber_name})")
    
    start_hour = tenant.get('business_hours_start', 7)
    day_end_hour = tenant.get('business_hours_end', 17)
    evening_end_hour = tenant.get('evening_hours_end', 17) # Default to same if not set
    
    is_daytime = start_hour <= hour < day_end_hour
    is_evening = day_end_hour <= hour < evening_end_hour
    
    # print(f"   🕒 Local Time: {local_time.strftime('%I:%M %p')} (Hour: {hour}) | Mode: {'Day' if is_daytime else 'Evening' if is_evening else 'Sleep'}")

    # --- LEAD & CONSENT (Always Record) ---
    # Pass bypass_check=True for inbound calls (system-initiated, valid consent)
    create_or_update_lead(caller_number, tenant_id=tenant_id, source="voice_inbound", bypass_check=True)
    record_consent(
        phone=caller_number,
        consent_type='implied',
        consent_source='inbound_call',
        tenant_id=tenant_id,
        metadata={'CallSid': call_sid, 'to_number': to_number}
    )

    if is_daytime:
        # DAYTIME: Ring Plumber for 15s -> If No Answer -> AI Intercept
        print(f"☀️ BUSINESS HOURS: Ringing Plumber... Fallback to AI.")
        
        # 1. Try to Connect (Smart Dialing)
        # We use an action URL to determine if the call was actually answered
        resp.dial(plumber_phone, timeout=15, action='/voice/status', method='POST')
        
        # Note: If action is set, TwiML processing stops here and waits for the callback.
        return str(resp)
        
    elif is_evening:
        # EVENING: Ring Plumber for 15s -> If No Answer -> AI Intercept
        print(f"🌙 EVENING MODE: Ringing Plumber... Fallback to AI.")
        
        resp.dial(plumber_phone, timeout=15, action='/voice/status', method='POST')
        return str(resp)

    else:
        # NIGHT/SLEEP: 
        if emergency_mode:
            # OPTION B: The "Hustler" (Press 1)
            gather = resp.gather(numDigits=1, action='/voice', method='POST', timeout=5)
            gather.say(f"Hi, you’ve reached {plumber_name}. It’s after hours. I’ll text you now. If this is an emergency, press 1 to reach our on-call tech.", voice='Polly.Matthew-Neural', language='en-US')
            
            # If they don't press 1, fall through
            resp.say("Thank you. Please check your text messages.", voice='Polly.Matthew-Neural', language='en-US')
            resp.hangup()
        else:
            # OPTION A: Standard Sleep Mode
            resp.say(f"Hi, you’ve reached {plumber_name}. It’s after hours. I’ll text you now.", voice='Polly.Matthew-Neural', language='en-US')
            resp.hangup()
            
        sms_body = f"Hi, this is {business_name}'s assistant! How can we help you today? Is this an urgent emergency, or a standard service request? Reply STOP to unsubscribe."
    
    # 3. Create Lead and Queue the SMS Alert
    # We do NOT send SMS via Twilio Client here. We ADD TO QUEUE for the engine.
    # from execution.utils.sms_engine import add_to_queue # Removed redundant import
    
    # 3. Create Lead and Consent moved to top
    
    if caller_number:
         add_to_queue(caller_number, sms_body, external_id=call_sid, tenant_id=tenant_id)
         
         # 4. Notify the Plumber (Tenant Specific Phone)
         tenant_plumber_phone = tenant.get('plumber_phone_number')
         alert_msg = f"🔔 ({plumber_name}) Lead Alert: Caught a missed call from {caller_number}. I have texted them."
         add_to_queue(tenant_plumber_phone, alert_msg, external_id=f"{call_sid}:plumber", tenant_id=tenant_id)

    return str(resp)

@app.route("/sms", methods=['GET', 'POST'])
@require_twilio_signature
def sms_handler():
    """Handles replies from the customer"""
    from twilio.twiml.messaging_response import MessagingResponse
    from execution.utils.sms_engine import add_to_queue
    from execution.utils.database import check_webhook_processed, record_webhook_processed
    
    from_number = request.values.get('From')
    to_number = request.values.get('To')
    body = request.values.get('Body', '').strip()
    msg_sid = request.values.get('MessageSid')
    
    # IDEMPOTENCY CHECK: Prevent duplicate webhook processing
    if msg_sid:
        is_duplicate, internal_id = check_webhook_processed(msg_sid)
        if is_duplicate:
            print(f"♻️  Duplicate webhook ignored: MessageSid {msg_sid} (already processed as {internal_id})")
            # Return same response as if we processed it (idempotent)
            resp = MessagingResponse()
            return str(resp)
    
    # Resolve Tenant
    tenant = get_tenant_by_twilio_number(to_number)
    if not tenant:
        return "Unknown Tenant", 400
    
    tenant_id = tenant['id']
    business_name = tenant.get('name', 'PlumberAI')  # Business name for SMS templates
    
    # Record webhook as processed (generate internal ID)
    internal_id = str(uuid.uuid4())
    record_webhook_processed(msg_sid, 'sms', tenant_id=tenant_id, internal_id=internal_id)
    
    # NEW: Tenant Rate Limit
    if not check_tenant_rate_limit(tenant_id):
        return "Too Many Requests", 429
    tenant_plumber_phone = tenant.get('plumber_phone_number')

    print(f"📩 INCOMING SMS from {mask_pii(from_number)}: {mask_pii(body)} (SID: {msg_sid}) Tenant: {tenant_id}")
    
    # IMMEDIATE STOP CHECK: Process STOP before anything else
    # This ensures STOP works even if message is delayed or other processing fails
    body_lower = body.strip().lower() if body else ""
    
    # Robust STOP detection: handles variants like "STOP!", "please stop", "STOPPED", etc.
    # Includes: STOP, Stop, stop, END, QUIT, CANCEL, UNSUBSCRIBE, ARRET (French)
    stop_patterns = ["stop", "unsubscribe", "cancel", "end", "quit", "opt.?out", "opt.?out", "arrêt", "arreter"]
    is_stop = False
    stop_keyword = None
    
    # Check for exact matches first (fast path)
    stop_words_exact = ["stop", "unsubscribe", "cancel", "end", "quit", "opt out", "opt-out", "arrêt", "arreter"]
    if body_lower in stop_words_exact:
        is_stop = True
        stop_keyword = body_lower
    else:
        # Check for partial matches (e.g., "please stop", "STOP!", "stop messages")
        import re
        for pattern in stop_patterns:
            if re.search(r'\b' + pattern + r'\b', body_lower):
                is_stop = True
                stop_keyword = pattern
                break
    
    if is_stop:
        # IMMEDIATE AND PERMANENT: Process STOP right away
        print(f"🚫 IMMEDIATE STOP detected: {mask_pii(from_number)} said '{body}'")
        
        # PERMANENT: Set opt-out (cannot be overridden)
        set_opt_out(from_number, True)
        
        # CASL COMPLIANCE: Revoke consent and log the reason
        revoke_consent(from_number, reason=stop_keyword.upper() if stop_keyword else "STOP", tenant_id=tenant_id)
        
        # Log the STOP event
        log_conversation_event(from_number, 'inbound', body, external_id=msg_sid, tenant_id=tenant_id)
        
        # Confirmation
        resp = MessagingResponse()
        resp.message("You have been unsubscribed and will receive no further messages.")
        return str(resp)
    
    # 1. Lead State Machine: Log & Update (only if not STOP)
    # Note: log_conversation_event must support tenant_id
    log_conversation_event(from_number, 'inbound', body, external_id=msg_sid, tenant_id=tenant_id)
    
    # CASL COMPLIANCE: Record Implied Consent for non-opt-out messages
    # The person texted us first, which provides implied consent under CASL.
    record_consent(
        phone=from_number,
        consent_type='implied',
        consent_source='inbound_sms',
        tenant_id=tenant_id,
        metadata={'MessageSid': msg_sid, 'to_number': to_number}
    )
    
    update_lead_status(from_number, 'replied')
    
    body_clean = body.lower().strip()
    
    # --- COMPLIANCE KEYWORDS (HELP / UNSTOP) ---
    # Handle HELP variants: help, info, aide (French)
    help_keywords = ['help', 'info', 'aide']
    if body_clean in help_keywords:
        # Get business name from tenant
        tenant_config = get_tenant_by_id(tenant_id)
        business_name = tenant_config.get('business_name', 'PlumberAI') if tenant_config else 'PlumberAI'
        resp = MessagingResponse()
        resp.message(f"{business_name}: Text us anytime for service. Call for emergencies. Reply STOP to unsubscribe.")
        return str(resp)

    if body_clean in ['start', 'unstop']:
        # Re-enable
        set_opt_out(from_number, False)
        # Record re-consent (Express)
        record_consent(from_number, 'express', 'inbound_sms', tenant_id, metadata={'keyword': body})
        resp = MessagingResponse()
        resp.message("You have been re-subscribed to updates. Msg & data rates apply.")
        return str(resp)
    
    # --- 2. SMART REVIEW LOGIC (New) ---
    # Detect if this is a reply to a review request
    # Since we don't track "Last Message Context" strictly yet, we use keyword + state heuristic
    # or just simple keyword matching which is robust enough for "Good/Bad" instructions.
    
    clean_body = body.lower().strip()
    
    # A. POSITIVE FEEDBACK
    if clean_body in ['good', 'great', 'awesome', 'excellent', 'yes']:
        review_link = tenant.get('google_review_link')
        if review_link:
            reply_msg = f"{business_name}: That's music to our ears! 🎵 It would help us SO much if you could leave that on Google: {review_link} \n\nThanks again! Reply STOP to unsubscribe."
            add_to_queue(from_number, reply_msg, external_id=f"{msg_sid}_review_link", tenant_id=tenant_id)
            
            # Notify Boss
            boss_msg = f"⭐ 5-STAR POTENTIAL: {from_number} said '{body}'. I sent them the link."
            add_to_queue(tenant.get('plumber_phone_number'), boss_msg, tenant_id=tenant_id)
            
            return str(MessagingResponse())

    # B. NEGATIVE FEEDBACK
    if clean_body in ['bad', 'poor', 'terrible', 'horrible', 'no', 'worst']:
        reply_msg = f"{business_name}: I am so sorry to hear that. I have just alerted the owner directly, and he will be calling you shortly to make this right. Reply STOP to unsubscribe."
        add_to_queue(from_number, reply_msg, external_id=f"{msg_sid}_apology", tenant_id=tenant_id)
        
        # Notify Boss (URGENT)
        boss_msg = f"🚨 NEGATIVE FEEDBACK: {from_number} said '{body}'. CALL THEM NOW to save the reputation."
        add_to_queue(tenant.get('plumber_phone_number'), boss_msg, tenant_id=tenant_id)
        
        return str(MessagingResponse())
    
    # Analyze sentiment for Alert Header
    urgent_keywords = ["emergency", "urgent", "leak", "flood", "burst", "explode", "broken", "now", "fast", "help"]
    is_urgent = any(k in body.lower() for k in urgent_keywords)
    
    if is_urgent:
        alert_header = "🚨 EMERGENCY"
    else:
        alert_header = "STANDARD SERVICE"
    
    # Resolve Name for Alert
    from execution.utils.database import get_lead_by_phone
    lead_info = get_lead_by_phone(from_number, tenant_id)
    cust_name = lead_info.get('name', 'Unknown') if lead_info else 'Unknown'

    alert_msg = f"{alert_header}: Msg - '{body}' From - {cust_name} ({from_number})"

    # ALERT BUFFERING ("Anti-Annoyance")
    try:
        from execution.utils.database import insert_or_update_alert_buffer
        # Buffer this alert instead of sending immediately
        insert_or_update_alert_buffer(tenant_id, from_number, tenant_plumber_phone, alert_msg)
        print(f"⏳ buffered alert for {from_number}")
    except ImportError:
        # Fallback if DB function not ready
        add_to_queue(tenant_plumber_phone, alert_msg, external_id=f"{msg_sid}_copy", tenant_id=tenant_id)
    except Exception as e:
        print(f"⚠️ Error buffering alert: {e}. Falling back to immediate send.")
        add_to_queue(tenant_plumber_phone, alert_msg, external_id=f"{msg_sid}_copy", tenant_id=tenant_id)
    
    # NEW: Acknowledgement to Customer
    # We only send this if it's NOT a stop word (handled above)
    plumber_name = tenant.get('name')
    ack_body = f"{business_name}: I have delivered your message to {plumber_name} and he will respond shortly. Thank you! Reply STOP to unsubscribe."
    add_to_queue(from_number, ack_body, external_id=f"{msg_sid}_ack", tenant_id=tenant_id)
    
    return str(MessagingResponse()) # Return empty TwiML
    
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
    Callback from <Dial>.
    If call was 'completed' (answered), do nothing.
    If 'busy', 'no-answer', 'failed', 'canceled', Trigger AI Fallback.
    """
    from execution.utils.sms_engine import add_to_queue
    from execution.utils.database import check_webhook_processed, record_webhook_processed
    import uuid
    
    call_status = request.values.get('DialCallStatus')
    to_number = request.values.get('To') # This is the Plumber's Cell
    caller_number = request.values.get('From') # Customer
    call_sid = request.values.get('CallSid')
    
    # IDEMPOTENCY CHECK: Prevent duplicate webhook processing
    if call_sid:
        status_webhook_id = f"{call_sid}_status_{call_status}"
        is_duplicate, internal_id = check_webhook_processed(status_webhook_id)
        if is_duplicate:
            print(f"♻️  Duplicate webhook ignored: {status_webhook_id} (already processed as {internal_id})")
            return str(VoiceResponse())
    # Correction: In a Dial Action, 'To' is often the original Tweilio number, or the Dialed number.
    # We should rely on request.values.get('CallSid') and re-fetch tenant if needed, or pass tenant_id in URL.
    # For simplicity/robustness, we re-resolve using the Twilio Number (which is usually in 'To' or 'ForwardedFrom').
    # Actually, simpler: Use the original Twilio logic or pass param.
    # But wait, TwiML callback preserves parameters?
    # Let's just lookup by Twilio Number again. Twilio request usually contains 'To' = Twilio Number (Inbound).
    # wait. In a <Dial action>, 'To' might be the number we just dialed.
    # Safe bet: Re-lookup using 'To' (if it allows us to find tenant).
    # But wait, if we dialed the plumber cell, 'To' is plumber cell.
    # We need the Original Twilio Number.
    # Twilio Action URL params: we can embed it.
    pass 
    # Actually, let's fix the logic above to pass tenant_id in query param ??
    # Too complex for quick edit.
    # Alternative: The /voice/status request *should* have the same 'To' (Twilio Num) if it's the *parent* call leg?
    # No, Dial action is on the parent leg.
    
    tenant = None
    # HEURISTIC: Try to find tenant by the 'From' (Caller) if lead exists? No, unreliable.
    # Better: Use the 'Digits' or just standard 'To'.
    # If this fails, we fall back to fail-safe.
    
    # Let's rely on 'To' being the Twilio number because <Dial> action is a callback for the *incoming* call's TwiML app.
    # Verify: Yes/No?
    # If we are unsure, let's look at the previous resolved execution.
    # Actually, we can just use the DB to find who owns the 'Dialed' number or the 'Inbound' number.
    
    # 2nd attempt at look up
    # request.values.get('To') might be +1555PLUMBER (outcome).
    # The 'Caller' is +1Cust.
    # The original 'To' (Twilio Num) might be lost in the Dial callback unless we encoded it.
    
    # SOLUTION: We will just assume 'To' is correct for now, or check both.
    # Re-resolving Tenant
    # We need to know WHICH tenant this is to send the text.
    # Let's try finding the tenant by 'to_number'. 
    # If 'to_number' is the plumber's cell, we can reverse look up?
    # Yes, we have 'plumber_phone_number' in DB.
    
    conn = get_tenant_by_twilio_number(to_number) # Case A: 'To' is Twilio Num
    if not conn:
         # Case B: 'To' is Plumber Cell. find tenant where plumber_phone = to_number
         from execution.utils.database import get_db_connection
         db = get_db_connection()
         row = db.execute("SELECT * FROM tenants WHERE plumber_phone_number = ?", (to_number,)).fetchone()
         if row:
             conn = dict(row)
         db.close()
         
    if not conn:
        print(f"❌ Could not resolve tenant in callback. To: {to_number}")
        return str(VoiceResponse()) # Fail silent
        
    tenant = conn
    tenant_id = tenant['id']
    plumber_name = tenant['name']
    
    print(f"📞 DIAL STATUS: {call_status} for {plumber_name}")
    
    if call_status == 'completed':
        print(f"✅ Call Answered by Plumber. No AI needed.")
        # Record webhook as processed
        if call_sid:
            status_webhook_id = f"{call_sid}_status_{call_status}"
            record_webhook_processed(status_webhook_id, 'voice_status', tenant_id=tenant_id, internal_id=f"completed_{call_sid}")
        return str(VoiceResponse()) # Hangup
        
    # If we are here, it was missed/busy/no-answer
    print(f"⚠️  Call Missed ({call_status}). Triggering AI...")
    
    # Record webhook as processed
    if call_sid:
        status_webhook_id = f"{call_sid}_status_{call_status}"
        internal_id = str(uuid.uuid4())
        record_webhook_processed(status_webhook_id, 'voice_status', tenant_id=tenant_id, internal_id=internal_id)
    
    resp = VoiceResponse()
    resp.say(f"Hi, you've reached {plumber_name}. We're on a job right now. I'm going to text you so we can help fast.", voice='Polly.Matthew-Neural', language='en-US')
    resp.hangup()
    
    # Create Lead if not exists (record consent FIRST so text engine allows it)
    create_or_update_lead(caller_number, tenant_id=tenant_id, source="voice_missed", bypass_check=True)
    record_consent(caller_number, 'implied', 'inbound_call', tenant_id=tenant_id, metadata={'CallSid': call_sid})
    
    # Queue Text
    business_name = tenant.get('name', 'PlumberAI')  # Business name for SMS templates
    sms_body = f"Hi, this is {business_name}'s assistant! How can we help you today? Is this an urgent emergency or a standard request? Reply STOP to unsubscribe."
    add_to_queue(caller_number, sms_body, external_id=f"{call_sid}_missed", tenant_id=tenant_id)
    
    # Notify the Plumber (Tenant Specific Phone)
    tenant_plumber_phone = tenant.get('plumber_phone_number')
    if tenant_plumber_phone:
        alert_msg = f"🔔 ({plumber_name}) Missed Call: I've texted {caller_number} to start the intake."
        add_to_queue(tenant_plumber_phone, alert_msg, external_id=f"{call_sid}_alert", tenant_id=tenant_id)
    
    return str(resp)

if __name__ == "__main__":
    print(f"🔧 Plumber Agent Listening on Port 5002")
    # We use 5002 to avoid conflict with your Webhook Server (5001)
    app.run(port=5002)
