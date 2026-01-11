import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import sys

# Import Config
from execution import config

def send_email_report(to_email, subject, body_html):
    """
    Sends an email using Gmail SMTP (or similar).
    Requires config vars: SMTP_EMAIL, SMTP_PASSWORD
    """
    sender_email = os.getenv("SMTP_EMAIL")
    sender_password = os.getenv("SMTP_PASSWORD")
    
    if not sender_email or not sender_password:
        print("⚠️  EMAIL NOT SENT: Missing SMTP_EMAIL or SMTP_PASSWORD in env/config.")
        return False

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = f"PlumberAI <{sender_email}>"
    message["To"] = to_email

    # Turn the plain text body into HTML 
    # (In a real app, generate proper HTML, here we just wrap it)
    html_content = f"""
    <html>
      <body style="font-family: sans-serif;">
        <pre style="font-family: sans-serif; font-size: 14px;">{body_html}</pre>
        <p style="font-size: 12px; color: #888;">
          <br>--------------------------------<br>
          <i>Trusted by PlumberAI</i>
        </p>
      </body>
    </html>
    """
    
    part = MIMEText(html_content, "html")
    message.attach(part)

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, to_email, message.as_string())
        print(f"   ✅ Email sent successfully")
        return True
    except Exception as e:
        print(f"   ❌ Email Failed: {e}")
        return False
