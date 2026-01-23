import os
import json
import logging
from execution.utils.logger import setup_logger

logger = setup_logger("OpenAIService")

class OpenAIService:
    _instance = None
    
    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.model = "gpt-4o"
        self.client = None
        
        if not self.api_key:
            logger.warning("⚠️ OPENAI_API_KEY not found in environment. AI features will be disabled.")
        else:
            try:
                from openai import OpenAI
                self.client = OpenAI(api_key=self.api_key)
                logger.info(f"✅ OpenAI Service Initialized (Model: {self.model})")
            except ImportError:
                logger.error("❌ 'openai' library not installed. Please run: pip install openai")
            except Exception as e:
                logger.error(f"❌ Failed to initialize OpenAI client: {e}")

    
    def transcribe_audio(self, audio_file_path):
        """
        Transcribes an audio file using OpenAI Whisper (gpt-whisper).
        
        Args:
            audio_file_path (str): Local path to the audio file (mp3/wav)
            
        Returns:
            str: Transcribed text or None if failed
        """
        if not self.client:
            logger.warning("Skipping Whisper transcription (No Client)")
            return None
            
        try:
            with open(audio_file_path, "rb") as audio_file:
                transcript = self.client.audio.transcriptions.create(
                    model="whisper-1", 
                    file=audio_file
                )
            return transcript.text
        except Exception as e:
            logger.error(f"Whisper Transcription Failed: {e}")
            return None

    def classify_intent(self, message_body):
        """
        Classifies a plumber lead message into Emergency, Standard, or Spam using GPT-4o.
        
        Args:
            message_body (str): The customer message to classify
        
        Returns:
            dict: {
                "urgency": "emergency" | "standard" | "spam",
                "confidence": float (0.0 - 1.0),
                "reasoning": str
            } or None if classification fails
        """
        if not self.client:
            logger.warning("Skipping AI classification (No Client)")
            return None

        # FIX: Define truncated_message_body before using it
        truncated_message_body = message_body[:500] if len(message_body) > 500 else message_body

        prompt = f"""
        You are an expert dispatcher for a plumbing company. Classify the urgency of this customer message.

        Message: "{truncated_message_body}"

        Classification Rules (STRICT):
        1. EMERGENCY (urgency: "emergency"):
           - Active water damage: "water everywhere", "flooding", "gushing", "cannot stop"
           - Burst/exploded pipes: "pipe burst", "pipe exploded", "water shooting out"
           - Complete water loss: "no water at all", "water completely off"
           - Dangerous situations: "gas smell", "sewage backup", "sewage overflow"
           - Immediate danger: "water in basement", "ceiling leaking badly"
           - Context clues: "right now", "immediately", "asap", "emergency"

        2. STANDARD (urgency: "standard"):
           - Routine maintenance: "leaky faucet", "dripping", "small leak"
           - Quotes/estimates: "how much", "quote", "price", "estimate", "cost"
           - Scheduling: "schedule", "appointment", "when can you come", "next week"
           - Non-urgent: "not urgent", "can wait", "when convenient"
           - General questions: "do you do", "can you fix", "what services"

        3. SPAM (urgency: "spam"):
           - Marketing messages
           - Wrong number responses
           - Completely irrelevant text

        Output ONLY valid JSON (no markdown, no explanation):
        {{
            "urgency": "emergency" | "standard" | "spam",
            "confidence": 0.0 to 1.0,
            "reasoning": "brief one-sentence explanation"
        }}
        """

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a helpful JSON-only assistant."},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.0
            )
            
            content = response.choices[0].message.content
            # Use the new _extract_json helper for robust parsing
            result = self._extract_json(content)
            return result
            
        except Exception as e:
            logger.error(f"AI Classification Failed: {e}")
            return None

    def _extract_json(self, content: str) -> dict:
        """
        Safely extracts JSON from OpenAI response, handling edge cases.
        
        This method handles various response formats that OpenAI might return:
        - Direct JSON strings
        - JSON wrapped in markdown code blocks (