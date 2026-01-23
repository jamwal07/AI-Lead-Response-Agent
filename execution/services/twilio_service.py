
import os
from twilio.rest import Client
from execution import config
from execution.utils.logger import setup_logger

logger = setup_logger("TwilioService")

class TwilioWrapper:
    def __init__(self, sid, token):
        self.client = Client(sid, token) if sid and token else None
        
    def lookup_number(self, phone_number):
        """
        Looks up phone number type and caller name.
        """
        if not self.client:
            return {'line_type': 'mobile', 'caller_name': None}
            
        try:
            # Twilio Lookup V2 API
            lookup = self.client.lookups.v2.phone_numbers(phone_number).fetch(fields='line_type_intelligence,caller_name')
            
            # Extract line type
            line_type = 'mobile'
            if lookup.line_type_intelligence:
                type_info = lookup.line_type_intelligence.get('type', 'mobile')
                line_type = type_info.lower()
            
            return {
                'line_type': line_type,
                'caller_name': lookup.caller_name.get('caller_name') if lookup.caller_name else None
            }
        except Exception as e:
            logger.warning(f"Twilio Lookup Failed for {phone_number}: {e}")
            return {'line_type': 'mobile', 'caller_name': None}

    def send_sms(self, to, body, from_=None):
        if not self.client:
            logger.warning(f"[MOCK] Would send SMS to {to}: {body}")
            return "mock_sid"
        
        from_number = from_ or config.TWILIO_PHONE_NUMBER
        return self.client.messages.create(
            body=body,
            from_=from_number,
            to=to
        ).sid

_service = None

def get_twilio_service():
    global _service
    if _service is None:
        _service = TwilioWrapper(config.TWILIO_ACCOUNT_SID, config.TWILIO_AUTH_TOKEN)
    return _service
