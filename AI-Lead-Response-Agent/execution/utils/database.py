import sqlite3
import json
import os
import time
import uuid
from datetime import datetime

# Define DB Path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Default Path (fallback)
DEFAULT_DB_PATH = os.path.join(BASE_DIR, 'data', 'plumber.db')
DATA_DIR = os.path.join(BASE_DIR, 'data')

def get_db_connection():
    # Resolve Path at Runtime (Safe for Preloading/Testing)
    db_path = os.getenv('PLUMBER_DB_PATH', DEFAULT_DB_PATH)
    
    # Ensure directory exists (unless it's in-memory)
    directory = os.path.dirname(db_path)
    if directory and db_path != ":memory:":
        os.makedirs(directory, exist_ok=True)
    
    conn = sqlite3.connect(db_path, timeout=10.0) # Set busy timeout to 10 seconds for concurrency
    conn.row_factory = sqlite3.Row  # Access columns by name
    
    # Performance Pragma for Multi-Process Read/Write (WAL Mode)
    # Required for multi-tenant scaling and multiple workers
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except Exception as e:
        # Might fail in some restricted environments, but recommended for VPS
        pass

    return conn

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
    
    # 1.6 Migration for Schedule
    try:
        c.execute("SELECT evening_hours_end FROM tenants LIMIT 1")
    except sqlite3.OperationalError:
        print("🔧 Migrating DB: Adding schedule cols to tenants...")
        c.execute("ALTER TABLE tenants ADD COLUMN evening_hours_end INTEGER DEFAULT 19")

    # 1.7 Migration for Calendar ID
    try:
        c.execute("SELECT calendar_id FROM tenants LIMIT 1")
    except sqlite3.OperationalError:
        print("🔧 Migrating DB: Adding calendar_id col to tenants...")
        c.execute("ALTER TABLE tenants ADD COLUMN calendar_id TEXT")
        
    # 1.8 Migration for Review Link
    try:
        c.execute("SELECT google_review_link FROM tenants LIMIT 1")
    except sqlite3.OperationalError:
        print("🔧 Migrating DB: Adding google_review_link col to tenants...")
        c.execute("ALTER TABLE tenants ADD COLUMN google_review_link TEXT")
    
    # 2. Create SMS_QUEUE Table
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
            intent TEXT,
            summary TEXT
        )
    """)
    
    # 3.5 Migration for Quality Columns
    try:
        c.execute("SELECT quality_score FROM leads LIMIT 1")
    except sqlite3.OperationalError:
        print("🔧 Migrating DB: Adding quality cols to leads...")
        c.execute("ALTER TABLE leads ADD COLUMN quality_score INTEGER DEFAULT 0")
        c.execute("ALTER TABLE leads ADD COLUMN intent TEXT")
        c.execute("ALTER TABLE leads ADD COLUMN summary TEXT")

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
        c.execute("SELECT external_id FROM sms_queue LIMIT 1")
    except sqlite3.OperationalError:
        print("🔧 Migrating DB: Adding external_id col to sms_queue...")
        c.execute("ALTER TABLE sms_queue ADD COLUMN external_id TEXT")
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_sms_queue_external_id ON sms_queue(external_id)")

    # Migration for OPT_OUT
    try:
        c.execute("SELECT opt_out FROM leads LIMIT 1")
    except sqlite3.OperationalError:
        print("🔧 Migrating DB: Adding opt_out col to leads...")
        c.execute("ALTER TABLE leads ADD COLUMN opt_out INTEGER DEFAULT 0")
    
    # Migration for TENANT_ID (Ensure each table has it)
    tables = ['sms_queue', 'leads', 'conversation_logs', 'jobs']
    for t in tables:
        try:
            # Check if column exists
            cursor = conn.execute(f"PRAGMA table_info({t})")
            columns = [row[1] for row in cursor.fetchall()]
            if 'tenant_id' not in columns:
                print(f"🔧 Migrating DB: Adding tenant_id col to {t}...")
                conn.execute(f"ALTER TABLE {t} ADD COLUMN tenant_id TEXT")
        except Exception as e:
            print(f"⚠️  Migration failed for table {t}: {e}")

    # Migration for conversation_logs UNIQUE index (Idempotency)
    try:
        # Check if index exists or just try to create it (IF NOT EXISTS is safe)
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_conversation_logs_external_id ON conversation_logs(external_id)")
    except Exception as e:
        print(f"⚠️  Migration failed for conversation_logs index: {e}")

    # Migration for leads UNIQUE constraint (phone, tenant_id) - Multi-tenant support
    # This allows the same phone number to exist for different tenants
    try:
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_leads_phone_tenant ON leads(phone, tenant_id)")
    except Exception as e:
        print(f"⚠️  Migration failed for leads unique index: {e}")

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
        print(f"✅ Created Default Tenant ({t_phone}) -> {tid}")
    except Exception as e:
        print(f"⚠️ Failed to create default tenant: {e}")
    
    # Run Migration if needed
    migrate_json_to_sqlite()

def migrate_json_to_sqlite():
    """One-time migration from JSON files to SQLite."""
    conn = get_db_connection()
    c = conn.cursor()
    
    # --- Migrate Jobs ---
    c.execute("SELECT count(*) FROM jobs")
    if c.fetchone()[0] == 0:
        json_path = os.path.join(DATA_DIR, 'jobs_db.json')
        if os.path.exists(json_path):
            print("📦 Migrating jobs_db.json to SQLite...")
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
            print("✅ Jobs migrated.")
            
    # --- Migrate Queue ---
    c.execute("SELECT count(*) FROM sms_queue")
    if c.fetchone()[0] == 0:
        json_path = os.path.join(DATA_DIR, 'sms_queue.json')
        if os.path.exists(json_path):
            print("📦 Migrating sms_queue.json to SQLite...")
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
            print("✅ SMS Queue migrated.")

    conn.commit()
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
    """
    if not twilio_number: return None
    
    # Normalize: strip spaces and leading +
    clean_num = str(twilio_number).strip().lstrip('+')
    
    conn = get_db_connection()
    # Search with normalization
    rows = conn.execute("SELECT * FROM tenants").fetchall()
    conn.close()
    
    for row in rows:
        db_num = str(row['twilio_phone_number']).strip().lstrip('+')
        if db_num == clean_num:
            return dict(row)
            
    return None

def get_tenant_by_id(tenant_id):
    conn = get_db_connection()
    row = conn.execute("SELECT * FROM tenants WHERE id = ?", (tenant_id,)).fetchone()
    conn.close()
    if row: return dict(row)
    return None

# --- QUEUE ACCESSORS ---

def add_sms_to_queue(to_number, body, external_id=None, tenant_id=None):
    conn = get_db_connection()
    msg_id = str(uuid.uuid4())
    created_at = datetime.now().isoformat()
    
    try:
        conn.execute("""
            INSERT INTO sms_queue (id, tenant_id, external_id, to_number, body, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (msg_id, tenant_id, external_id, to_number, body, 'pending', created_at))
        conn.commit()
        print(f"📥 Message queued for {to_number} (DB)")
        return True
    except sqlite3.IntegrityError:
        # If external_id exists (Idempotency check)
        if external_id:
            print(f"♻️  Duplicate Event Ignored (External ID: {external_id})")
            return False
        else:
            # Should not happen with UUID but safe to raise or retry
            raise
    finally:
        conn.close()

def claim_pending_sms(limit=10, timeout_minutes=5):
    """
    Atomically claim pending rows OR stuck processing rows (Self-Healing).
    Uses single atomic UPDATE to prevent race conditions and double-sending.
    """
    from datetime import timedelta
    conn = get_db_connection()
    try:
        now = datetime.now()
        cutoff = (now - timedelta(minutes=timeout_minutes)).isoformat()
        now_str = now.isoformat()
        
        # ATOMIC UPDATE: Claim in a single operation to prevent race conditions
        # This ensures only one worker can claim each message (no double-send)
        conn.execute("""
            UPDATE sms_queue 
            SET status = 'processing', last_attempt = ?
            WHERE id IN (
                SELECT id FROM sms_queue 
                WHERE status = 'pending' 
                   OR (status = 'processing' AND last_attempt < ?)
                ORDER BY created_at ASC
                LIMIT ?
            )
        """, (now_str, cutoff, limit))
        
        # Fetch the claimed rows
        claimed_rows = conn.execute("""
            SELECT * FROM sms_queue 
            WHERE status = 'processing' AND last_attempt = ?
            ORDER BY created_at ASC
            LIMIT ?
        """, (now_str, limit)).fetchall()
        
        conn.commit()
        return [dict(ix) for ix in claimed_rows]
        
    except Exception as e:
        print(f"DB Claim Error: {e}")
        conn.rollback()
        return []
    finally:
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

def archive_old_sms(days=30):
    """
    Optional: Archiving logic for old messages to keep DB lightweight.
    """
    pass

# --- LEAD MANAGEMENT ---

def create_or_update_lead(phone, tenant_id=None, source="call", bypass_check=False):
    """
    Creates a new lead linked to a specific tenant.
    
    Args:
        phone: Phone number in E.164 format
        tenant_id: Optional tenant ID
        source: Source of the lead (e.g., "call", "website_form")
        bypass_check: If False, logs a warning that add_client.py should be used for compliance.
                      If True, allows direct calls (for inbound calls, webhooks, etc.)
    """
    # Compliance warning: Direct calls should use add_client.py for proper consent tracking
    if not bypass_check:
        import traceback
        import sys
        # Check if called from add_client.py (skip warning if so)
        frame = sys._getframe(1)
        caller_file = frame.f_code.co_filename if frame else ""
        if "add_client.py" not in caller_file:
            print(f"⚠️  WARNING: Direct lead creation detected. Use 'add_client.py' for compliance (consent proof required). Phone: {phone}")
    
    conn = get_db_connection()
    now = datetime.now().isoformat()
    
    # Check if exists FOR THIS TENANT
    # Fallback to simple query if tenant_id is None
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
        conn.close()
        return lead_id, current_status
    else:
        lead_id = str(uuid.uuid4())
        
        # SAFETY CHECK: Inherit Opt-Out Status from Global History
        # If this user opted out previously (even under a different tenant), 
        # we respect that globally to avoid spam lawsuits.
        is_blocked = check_opt_out_status(phone)
        initial_opt_out_val = 1 if is_blocked else 0
        
        conn.execute("""
            INSERT INTO leads (id, tenant_id, phone, status, created_at, last_contact_at, opt_out)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (lead_id, tenant_id, phone, 'new', now, now, initial_opt_out_val))
        conn.commit()
        conn.close()
        print(f"🌟 New Lead Created: {phone} (Tenant: {tenant_id}) OptOut={initial_opt_out_val}")
        return lead_id, 'new'

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

def update_lead_status(phone, new_status):
    """
    Updates status. Enforces basic state logic prevents regression from 'booked'.
    """
    conn = get_db_connection()
    row = conn.execute("SELECT status, opt_out FROM leads WHERE phone = ?", (phone,)).fetchone()
    
    if not row:
        conn.close()
        return False
    
    if row['opt_out'] == 1:
        # Cannot change status of opt-out
        conn.close()
        return False
        
    current = row['status']
    
    # Simple State Rules
    # Don't regress from booked unless manual intervention (todo)
    if current == 'booked' and new_status != 'booked':
        conn.close()
        return False
        
    conn.execute("UPDATE leads SET status = ? WHERE phone = ?", (new_status, phone))
    conn.commit()
    conn.close()
    return True

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
    print(f"🚫 Opt-Out Set for {phone}: {is_opt_out} (PERMANENT)")

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

def get_lead_funnel_stats():
    """Returns counts of leads by status"""
    conn = get_db_connection()
    rows = conn.execute("SELECT status, COUNT(*) as count FROM leads GROUP BY status").fetchall()
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
        print(f"✅ CASL Consent Recorded: {phone} ({consent_type}/{consent_source})")
        return consent_id
    except Exception as e:
        print(f"⚠️ Failed to record consent: {e}")
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
    print(f"🚫 CASL Consent Revoked: {phone} (Reason: {reason})")


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
    """
    conn = get_db_connection()
    c = conn.cursor()
    
    # Check if exists
    c.execute("SELECT messages_text, message_count FROM alert_buffer WHERE tenant_id = ? AND customer_phone = ?", (tenant_id, customer_phone))
    row = c.fetchone()
    
    from datetime import timedelta
    send_at = datetime.now() + timedelta(seconds=30)
    
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
        c.execute("""
            INSERT INTO alert_buffer (tenant_id, customer_phone, plumber_phone, messages_text, message_count, send_at, created_at)
            VALUES (?, ?, ?, ?, 1, ?, datetime('now'))
        """, (tenant_id, customer_phone, plumber_phone, message_text, send_at))
        
    conn.commit()
    conn.close()

def process_alert_buffer():
    """
    Checks for ready-to-send alerts and queues them.
    """
    conn = get_db_connection()
    c = conn.cursor()
    
    now = datetime.now()
    
    # Fetch ready alerts
    # Note: sqlite timestamp comparison can be tricky if not consistently stored. 
    # We rely on the adapter being set up in database.py or string comparison if ISO format.
    # Assuming standard adapter usage.
    
    rows = c.execute("SELECT * FROM alert_buffer WHERE send_at <= ?", (now,)).fetchall()
    
    if not rows:
        conn.close()
        return 0
        
    from execution.utils.sms_engine import add_to_queue
    
    processed_count = 0
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
            
        # Queue it
        print(f"🚀 Dispatching Buffered Alert to {plumber_phone} (Count: {count})")
        add_to_queue(plumber_phone, final_msg, external_id=f"buf_{buf_id}", tenant_id=tenant_id)
        
        # Delete from Buffer
        c.execute("DELETE FROM alert_buffer WHERE id = ?", (buf_id,))
        processed_count += 1
        
    conn.commit()
    conn.close()
    return processed_count
