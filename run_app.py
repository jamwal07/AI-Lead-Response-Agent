
import multiprocessing
import time
import sys
import os
import signal

# Add current dir to path
sys.path.append(os.getcwd())

from execution.handle_incoming_call import app
from execution.utils.sms_engine import run_worker as run_sms_worker
from execution.watchdog import run_watchdog
from execution.utils.logger import setup_logger
from execution.utils.alert_system import send_critical_alert

logger = setup_logger("ProcessManager")

def start_flask_app():
    """Runs the Flask Webhook Server via Gunicorn (Production)"""
    print("🌐 Starting Gunicorn Web Server on Port 5002...")
    # 2 Workers, Threads enabled, Bind to 5002
    cmd = [
        "gunicorn", 
        "execution.handle_incoming_call:app", 
        "-w", "2", 
        "-b", "0.0.0.0:5002",
        "--access-logfile", "-",
        "--error-logfile", "-"
    ]
    # We use subprocess.run to block this process (which is already a child process)
    import subprocess
    subprocess.run(cmd)

def start_sms_worker():
    """Runs the Background SMS Worker"""
    print("👷 Starting SMS Worker...")
    run_sms_worker()

def signal_handler(sig, frame):
    print("\n🛑 Shutting down PlumberAI...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    
    print("🚀 Launching PlumberAI SaaS System...")
    
    # Print SAFE_MODE status on startup
    from execution import config
    safe_mode_status = "ON (BLOCKING all SMS sends)" if config.SAFE_MODE else "OFF (ALLOWING SMS sends)"
    print(f"🔒 SAFE_MODE: {safe_mode_status}")
    
    # Process 1: Web Server
    flask_process = multiprocessing.Process(target=start_flask_app, name="FlaskWeb")
    
    # Process 2: SMS Workers (Dual Workers for throughput)
    worker_1 = multiprocessing.Process(target=start_sms_worker, name="SMSWorker-1")
    worker_2 = multiprocessing.Process(target=start_sms_worker, name="SMSWorker-2")
    
    # Process 3: Watchdog
    watchdog_process = multiprocessing.Process(target=run_watchdog, name="Watchdog")
    
    flask_process.start()
    worker_1.start()
    worker_2.start()
    watchdog_process.start()
    
    logger.info(f"Flask PID: {flask_process.pid}")
    logger.info(f"Worker-1 PID: {worker_1.pid}")
    logger.info(f"Worker-2 PID: {worker_2.pid}")
    logger.info(f"Watchdog PID: {watchdog_process.pid}")
    
    try:
        while True:
            # Monitor Health
            if not flask_process.is_alive():
                logger.critical("⚠️ Flask Server died! Restarting...")
                send_critical_alert("Process Crash: FlaskWeb", "The Flask web server process died and is being restarted.")
                flask_process = multiprocessing.Process(target=start_flask_app, name="FlaskWeb")
                flask_process.start()
                
            if not worker_1.is_alive():
                logger.critical("⚠️ SMS Worker-1 died! Restarting...")
                send_critical_alert("Process Crash: SMSWorker-1", "SMS Worker 1 process died and is being restarted.")
                worker_1 = multiprocessing.Process(target=start_sms_worker, name="SMSWorker-1")
                worker_1.start()

            if not worker_2.is_alive():
                logger.critical("⚠️ SMS Worker-2 died! Restarting...")
                send_critical_alert("Process Crash: SMSWorker-2", "SMS Worker 2 process died and is being restarted.")
                worker_2 = multiprocessing.Process(target=start_sms_worker, name="SMSWorker-2")
                worker_2.start()

            if not watchdog_process.is_alive():
                logger.critical("⚠️ Watchdog died! Restarting...")
                send_critical_alert("Process Crash: Watchdog", "The Watchdog process itself died and is being restarted.")
                watchdog_process = multiprocessing.Process(target=run_watchdog, name="Watchdog")
                watchdog_process.start()
                
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        flask_process.terminate()
        worker_1.terminate()
        worker_2.terminate()
        watchdog_process.terminate()
        flask_process.join()
        worker_1.join()
        worker_2.join()
        watchdog_process.join()
        print("✅ System Shutdown Complete.")
