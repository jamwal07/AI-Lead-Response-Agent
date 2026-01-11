# Plumber Lead Response Agent (Project 1)

## Goal
To intercept missed calls for plumbers, play a "Safe Driving / Busy" message, and immediately engage the lead via SMS.

## Trigger
- **Incoming Call** to the Tenant's Twilio Number (24/7).
- **Incoming SMS** to the Tenant's Twilio Number.

## System Architecture
The agent is built as a modular Python application designed for high availability and reliability.

### Core Components
1.  **Process Manager (`run_app.py`)**:
    *   Single entry point managing all subprocesses (Web, 2x Workers, Watchdog).
    *   **Auto-Healing**: Restarts crashed processes instantly.
2.  **Flask/Gunicorn Server (`execution/handle_incoming_call.py`)**:
    *   **Validated Security**: Every webhook is verified using `@require_twilio_signature` (unless strictly mocked).
    *   **DDoS & Cost Protection**: **Tenant-Level Rate Limiting** (20 req/min) prevents bill spikes.
    *   **Multi-Tenant Router**: Resolves incoming calls/texts to specific Client configs based on the **Twilio Number** dialed on the `To` field.
3.  **Database (`execution/utils/database.py`)**:
    *   **SQLite High-Performance**: Configured with **WAL Mode** and **NORMAL Synchronous** for maximum concurrent throughput.
    *   **Busy Handling**: 10s timeout prevents "Database Locked" errors.
    *   **Partitioned**: Multi-tenant data isolation via `tenant_id`.
4.  **Intelligence Layer (`execution/quality_analysis.py`)**:
    *   Batch heuristic analysis for Intent (Emergency vs Quote) and Lead Scoring.
5.  **Resilience & Ops**:
    *   **Watchdog (`execution/watchdog.py`)**: 24/7 monitoring of queue health, failure rates, and **Cost Guardrails**.
    *   **Backup System (`execution/utils/backup.py`)**: Automated daily DB snapshots with 7-day rotation.
    *   **Compliance**: 
        - Full TCPA/A2P 10DLC support with automated "STOP" opt-out and mandatory compliance text.
        - **CASL Compliance (Canada)**: Full proof-of-consent tracking with `consent_records` table.
            - **Implied Consent**: Automatically recorded when caller/texter initiates contact (valid for 2 years).
            - **Audit Trail**: Timestamps, CallSid/MessageSid, and metadata stored for regulatory audits.
            - **Revocation Tracking**: Stores exact opt-out keyword and timestamp when consent is revoked.

## Core Logic
1.  **System Health & Guardrails:**
    - **Watchdog Integration:** Scans for stuck jobs and failure rates once per minute.
    - **Cost Monitoring:** Once per hour, it verifies that no tenant has exceeded their daily SMS quota to prevent billing runaway.
    - **Automated Backups:** Daily rotation of the `plumber.db` file (Last 7 days retained).

2.  **Voice Handler (`handle_incoming_call.py`):**
    - Answers call immediately.
    - **Timezone Check:** Converts server time to **Tenant Local Time** (Stored in DB).
    - **Lead Creation:** Immediately verifies or creates a Lead record and records Implied Consent (Inbound Call).
    - **Daytime (Business Hours):**
        - **Smart Dialing:** Rings the Plumber's phone for 15 seconds using `<Dial>` with an `action` callback.
        - **Answered Call:** If plumber answers, the system logs it and does NOTHING else.
        - **Missed Call:** If busy/no-answer/failed, the callback triggers the AI Fallback logic:
            - Plays "Busy" message to caller.
            - Queues "Missed Call" SMS to Customer.
            - Queues "Missed Call" Alert to Plumber.
    - **Evening (Ring Through):** Same Smart Dialing logic. If no answer, initiates text flow.
    - **Sleep Mode (Night):**
        - **Standard:** *"Hi, you’ve reached [Business Name]. It’s after hours. I’ll text you now."* -> Hang Up -> Text.
        - **Emergency Mode:** *"Hi, you’ve reached [Business Name]. It’s after hours... If this is an emergency, press 1 to reach our on-call tech."*
    
3.  **SMS Handler:**
    - **Smart Review Logic (Priority):**
        - If text is "GOOD"/"BAD" (in response to a review request):
            - **Positive:** Sends Google Review Link instantly.
            - **Negative:** Sends Apology + Critical Alert to owner.
    - **Standard Flow:**
        - **Compliance Handling:**
            - **STOP:** Opts out users immediately.
            - **HELP:** Returns compliant help message about the service.
            - **START / UNSTOP:** Re-subscribes users and records Express Consent.
        - **New Lead:** Queues an immediate SMS to the caller: *"Hi, this is [Plumber's Name]'s assistant!..."*
        - Queues an alert to the Plumber's Cell Phone with Priority Header:
            - **🚨 EMERGENCY: Msg - '...' From - '...'**
            - **STANDARD SERVICE: Msg - '...' From - '...'**
        - **Anti-Annoyance Buffer:** If multiple texts come in quickly from the same lead, they are grouped into a single alert to avoid spamming the plumber.

4.  **Queue System (`utils/sms_engine.py` & `run_app`):**
    *   **Dual-Worker Throughput**: Two independent workers process the queue to prevent bottlenecks.
    *   **Atomic Claiming**: `UPDATE ... RETURNING` pattern prevents "double-send" errors.
    *   **Self-Healing**: Recovers "stuck" processing messages after 5 mins.
    *   **Cost Guardrails**: Watchdog triggers `send_critical_alert()` if volume spikes.
    *   **Compliance**: Automated inclusion of TCPA mandatory text: *"Msg/data rates may apply"*.
    
    
5.  **Quality Analysis:**
    *   Batch job scans recent chats.
    *   Classifies intent (Booked, Quote, Emergency).
    *   Updates Lead Score.

## Administration
- **Dashboard:** `/dashboard` (View live queue, stats, and client list).
- **Config:** `execution/config.py` (App-wide settings). Tenants are managed via SQLite.
- **Files:**
    - `execution/handle_incoming_call.py` (Flask Server)
    - `execution/utils/sms_engine.py` (The Sender)
    - `templates/dashboard.html` (The UI)
