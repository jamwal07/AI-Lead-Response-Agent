
import multiprocessing
import time
import sys
import os
import signal
from datetime import datetime, timedelta

# Add current dir to path
sys.path.append(os.getcwd())

from execution.utils.logger import setup_logger
from execution.utils.database import get_db_connection, get_pending_sms
from execution.utils.cost_monitor import check_cost_guardrails
from execution.utils.alert_system import send_critical_alert

logger = setup_logger("Watchdog")

class Watchdog:
    def __init__(self):
        self.ok_streak = 0
        self.last_cost_check = 0 # Timestamp
        
    def check_queue_health(self):
        conn = get_db_connection()
        try:
            # Metric 1: Queue Depth
            # Count pending for > 5 minutes
            five_mins_ago = (datetime.now() - timedelta(minutes=5)).isoformat()
            
            stuck_count = conn.execute(
                "SELECT count(*) FROM sms_queue WHERE status='pending' AND created_at < ?", 
                (five_mins_ago,)
            ).fetchone()[0]
            
            if stuck_count > 10:
                logger.critical(f"🚨 ALERT: {stuck_count} messages are stuck in queue > 5 mins!", extra={"metric": "queue_stuck", "value": stuck_count})
            elif stuck_count > 0:
                logger.warning(f"⚠️ {stuck_count} messages waiting > 5 mins.", extra={"metric": "queue_stuck", "value": stuck_count})
                
            # Metric 2: Recent Failures (Last 60 mins)
            one_hour_ago = (datetime.now() - timedelta(minutes=60)).isoformat()
            fail_count = conn.execute(
                "SELECT count(*) FROM sms_queue WHERE status LIKE 'failed%' AND last_attempt > ?",
                (one_hour_ago,)
            ).fetchone()[0]
            
            if fail_count > 5:
                 logger.error(f"🚨 High Failure Rate: {fail_count} failed texts in last hour.", extra={"metric": "failure_rate_1h", "value": fail_count})
            
            # Metric 3: Lead Velocity (Just for info)
            new_leads = conn.execute(
                "SELECT count(*) FROM leads WHERE created_at > ?", (one_hour_ago,)
            ).fetchone()[0]
            
            logger.info(f"Health Check: Queue Stuck={stuck_count}, Failures/1h={fail_count}, NewLeads/1h={new_leads}", 
                        extra={"health": "ok", "stuck": stuck_count, "failures": fail_count, "leads": new_leads})

            # Metric 4: Cost Guardrails (Check once per hour)
            now = time.time()
            if now - self.last_cost_check > 3600:
                check_cost_guardrails()
                self.last_cost_check = now

        except Exception as e:
            logger.error(f"Watchdog Error: {e}")
            send_critical_alert("Watchdog Check Failed", str(e))
        finally:
            conn.close()

    def run(self):
        logger.info("🐶 Watchdog System Started.")
        while True:
            self.check_queue_health()
            time.sleep(60) # Run every minute

def run_watchdog():
    w = Watchdog()
    w.run()
