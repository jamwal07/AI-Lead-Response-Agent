import os
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from execution.utils.logger import setup_logger

logger = setup_logger("AlertSystem")

def send_critical_alert(error_title, error_details):
    """
    Sends an email alert to the admin when a CRITICAL failure occurs.
    """
    admin_email = os.getenv("ADMIN_EMAIL", os.getenv("SMTP_EMAIL")) # Fallback to sender if no specific admin set
    sender_email = os.getenv("SMTP_EMAIL")
    sender_password = os.getenv("SMTP_PASSWORD")
    
    if not sender_email or not sender_password or not admin_email:
        logger.error(f"Cannot send alert (Missing Config). Error was: {error_title}")
        return False
        
    subject = f"🚨 CRITICAL ALERT: {error_title}"
    body = f"""
    SYSTEM FAILURE DETECTED
    -----------------------
    Component: PlumberAI SaaS
    Error: {error_title}
    
    Details:
    {error_details}
    
    Action Required:
    Please log in to the server immediately and check 'logs/plumber_ai.log'.
    """
    
    msg = MIMEMultipart()
    msg["From"] = f"PlumberAI Alert <{sender_email}>"
    msg["To"] = admin_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))
    
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, admin_email, msg.as_string())
        logger.info(f"🚨 Admin Alert Sent: {error_title}")
    except Exception as e:
        logger.error(f"Failed to send admin alert email: {e}")

    # --- TELEGRAM ALERT ---
    send_telegram_alert(f"🚨 *CRITICAL ALERT*\n\n*Error:* {error_title}\n\n*Details:*\n{error_details}")
    return True

def send_telegram_alert(message):
    """
    Sends a message to the configured Telegram Chat.
    Uses urllib to avoid adding 'requests' dependency if not present.
    """
    try:
        from execution.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
        import urllib.request
        import json
        
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return False
            
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        }
        
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
        
        with urllib.request.urlopen(req, timeout=5) as response:
             if response.getcode() == 200:
                 logger.info("✅ Telegram Alert Sent")
                 return True
    except Exception as e:
        logger.error(f"Failed to send Telegram alert: {e}")
        return False
