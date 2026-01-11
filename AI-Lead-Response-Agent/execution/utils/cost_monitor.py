
import os
import time
from datetime import datetime, timedelta
from execution.utils.database import get_db_connection
from execution.utils.alert_system import send_critical_alert
from execution.utils.logger import setup_logger

logger = setup_logger("CostMonitor")

# Thresholds (Configurable via ENV)
# Max SMS per tenant per 24h
TENANT_DAILY_LIMIT = int(os.getenv("TENANT_DAILY_LIMIT", 200))
# Max SMS globally per 24h
GLOBAL_DAILY_LIMIT = int(os.getenv("GLOBAL_DAILY_LIMIT", 1000))

def check_cost_guardrails():
    """
    Scans the database for SMS usage in the last 24h.
    Triggers alerts if thresholds are exceeded.
    """
    logger.info("💸 Checking Cost Guardrails...")
    conn = get_db_connection()
    try:
        # Calculate 24h ago in ISO format
        day_ago = (datetime.now() - timedelta(hours=24)).isoformat()
        
        # 1. Global Check
        global_count = conn.execute(
            "SELECT COUNT(*) FROM sms_queue WHERE status = 'sent' AND sent_at > ?", 
            (day_ago,)
        ).fetchone()[0]
        
        if global_count > GLOBAL_DAILY_LIMIT:
            msg = f"GLOBAL SMS Spike Detected: {global_count} sent in last 24h (Limit: {GLOBAL_DAILY_LIMIT})"
            logger.critical(f"🚨 {msg}")
            send_critical_alert("Global Cost Spike", msg)
            
        # 2. Per-Tenant Check
        tenant_usage = conn.execute("""
            SELECT tenant_id, COUNT(*) as count 
            FROM sms_queue 
            WHERE status = 'sent' AND sent_at > ?
            GROUP BY tenant_id
        """, (day_ago,)).fetchall()
        
        for row in tenant_usage:
            t_id = row['tenant_id']
            t_count = row['count']
            
            if t_count > TENANT_DAILY_LIMIT:
                msg = f"Tenant {t_id} Spike Detected: {t_count} sent in last 24h (Limit: {TENANT_DAILY_LIMIT})"
                logger.critical(f"🚨 {msg}")
                # Fetch tenant name for better alert
                t_name = conn.execute("SELECT name FROM tenants WHERE id = ?", (t_id,)).fetchone()
                t_name = t_name['name'] if t_name else t_id
                send_critical_alert(f"Tenant Cost Spike: {t_name}", msg)
                
        logger.info("✅ Cost Guardrail Check Complete.")
        
    except Exception as e:
        logger.error(f"Cost Monitor Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_cost_guardrails()
