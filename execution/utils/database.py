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

