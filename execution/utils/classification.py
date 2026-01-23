"""
AI Classification Module for Emergency vs Standard Request Detection

This module provides intelligent classification of customer messages to distinguish
between emergency situations (requiring immediate response) and standard service
requests (can be scheduled normally).

Classification Methods:
1. Keyword-based (fast, reliable for common cases)
2. AI-based (more accurate, handles edge cases)
3. Hybrid (combines both for best accuracy)
"""

from execution.utils.logger import setup_logger
from execution.utils.constants import EMERGENCY_KEYWORDS, NEGATIVE_KEYWORDS
import re
from execution.services.openai_service import get_openai_service

logger = setup_logger("Classification")


def classify_request_urgency(message_text: str, use_ai: bool = False) -> dict:
    """
    Classifies a customer message as Emergency or Standard.
    
    This function analyzes the message text to determine if the customer needs
    immediate emergency service (e.g., burst pipe, flooding) or if it's a
    standard request that can be scheduled normally (e.g., leaky faucet, quote).
    
    Args:
        message_text: The customer's message text to classify
        use_ai: If True, uses AI classification (requires API key). 
                If False, uses fast keyword-based classification.
    
    Returns:
        dict with keys:
            - urgency: 'emergency' | 'standard' | 'unknown'
            - confidence: float (0.0 to 1.0)
            - reasoning: str (explanation of classification)
            - keywords_found: list (emergency keywords detected)
    
    Example:
        >>> classify_request_urgency("My pipe burst and water is everywhere!")
        {
            'urgency': 'emergency',  # String: 'emergency', 'standard', or 'unknown'
            'confidence': 0.95,
            'reasoning': 'Multiple emergency keywords detected: burst, water everywhere',
            'keywords_found': ['burst', 'water everywhere']
        }
    """
    if not message_text or not isinstance(message_text, str):
        return {
            'urgency': 'unknown',  # Changed from integer 0 to string
            'confidence': 0.0,
            'reasoning': 'Empty or invalid message text',
            'keywords_found': []
        }
    
    message_lower = message_text.lower().strip()
    
    # PRIORITY CHECK: Explicit "not urgent" language overrides emergency keywords
    if re.search(r'\b(?:not urgent|not an emergency|can wait|when convenient)\b', message_lower):
        return {
            'urgency': 'standard',  # Changed from integer 1 to string
            'confidence': 0.85,
            'reasoning': 'Standard request: Explicit non-urgent language detected.',
            'keywords_found': []
        }
    
    # Fast keyword-based classification (always runs first)
    keywords_found = []
    emergency_score = 0
    
    # Check for emergency keywords with context
    # Enhanced detection: looks for keywords with surrounding context to avoid false positives
    for keyword in EMERGENCY_KEYWORDS:
        # Use word boundaries to avoid false positives (e.g., "leakproof" shouldn't match "leak")
        pattern = r'\b' + re.escape(keyword) + r'\b'
        if re.search(pattern, message_lower, re.IGNORECASE):
            keywords_found.append(keyword)
            # Weight keywords by severity (refined based on real-world data)
            # High severity: Immediate danger, property damage, health risk
            if keyword in ['burst', 'explode', 'flood', 'flooding', 'sewage', 'gas smell', 'water everywhere', 'overflowing']:
                emergency_score += 3  # High severity - immediate response needed
            # Medium-high severity: Urgent but may not be life-threatening
            elif keyword in ['emergency', 'urgent', 'no water', 'overflow', 'toilet overflow', 'basement', 'ceiling']:
                emergency_score += 2  # Medium-high severity - respond quickly
            else:
                emergency_score += 1  # Standard emergency keyword - needs attention
    
    # Check for urgency indicators
    urgency_phrases = [
        r'\b(?:right now|immediately|asap|as soon as possible|urgent|emergency)\b',
        r'\b(?:can\'?t wait|need help now|please hurry)\b',
        r'\b(?:water (?:is|everywhere|flooding)|flooding|burst|exploded)\b'
    ]
    
    for phrase in urgency_phrases:
        if re.search(phrase, message_lower, re.IGNORECASE):
            emergency_score += 2
    
    # Check for standard/non-emergency indicators
    standard_phrases = [
        r'\b(?:quote|estimate|price|cost|how much)\b',
        r'\b(?:schedule|appointment|when can|next week|next month)\b',
        r'\b(?:small leak|dripping|minor|not urgent|can wait)\b',
        r'\b(?:not urgent|not an emergency|can wait|when convenient)\b'
    ]
    
    standard_score = 0
    for phrase in standard_phrases:
        if re.search(phrase, message_lower, re.IGNORECASE):
            standard_score += 1
    
    # Calculate confidence based on keyword matches
    # FIX: Return string values ('emergency', 'standard', 'unknown') for consistency
    if emergency_score >= 3:
        urgency = 'emergency'  # Changed from integer 3 to string
        confidence = min(0.95, 0.7 + (emergency_score * 0.05))
        reasoning = f"Emergency detected: {len(keywords_found)} keyword(s) found. High urgency indicators present."
    elif emergency_score >= 1 and standard_score == 0:
        urgency = 'emergency'  # Changed from integer 3 to string
        confidence = 0.6 + (emergency_score * 0.1)
        reasoning = f"Possible emergency: {len(keywords_found)} keyword(s) found. No standard indicators."
    elif standard_score >= 2 and emergency_score == 0:
        urgency = 'standard'  # Changed from integer 1 to string
        confidence = 0.85
        reasoning = "Standard request: Multiple scheduling/quote indicators, no emergency keywords."
    elif standard_score >= 1 and emergency_score < 2:
        # Check for explicit "not urgent" language
        if re.search(r'\b(?:not urgent|not an emergency|can wait)\b', message_lower):
            urgency = 'standard'  # Changed from integer 1 to string
            confidence = 0.8
            reasoning = "Standard request: Explicit non-urgent language detected."
        else:
            urgency = 'standard'  # Changed from integer 1 to string
            confidence = 0.7
            reasoning = "Likely standard: Scheduling/quote indicators present, minimal emergency signals."
    else:
        urgency = 'unknown'  # Changed from integer 0 to string
        confidence = 0.5
        reasoning = "Unclear intent: Mixed or no clear indicators. Manual review recommended."
    
    # AI-based classification (if enabled and available)
    if use_ai:
        try:
            ai_result = _classify_with_ai(message_text)
            if ai_result:
                # Normalize AI result to match our string format
                # AI may return 'emergency', 'standard', or 'spam'
                ai_urgency = ai_result.get('urgency', '').lower()
                if ai_urgency in ['emergency', 'standard', 'spam']:
                    # Map 'spam' to 'unknown' for consistency
                    if ai_urgency == 'spam':
                        ai_result['urgency'] = 'unknown'
                    # Use AI result if it's more confident
                    if ai_result.get('confidence', 0) > confidence:
                        return ai_result
        except Exception as e:
            logger.warning(f"AI classification failed, using keyword result: {e}")
    
    return {
        'urgency': urgency,
        'confidence': min(1.0, confidence),
        'reasoning': reasoning,
        'keywords_found': keywords_found
    }


def _classify_with_ai(clean_body):
    """
    Passes the message to the OpenAI Service for classification.
    """
    try:
        ai_service = get_openai_service()
        result = ai_service.classify_intent(clean_body)
        
        if result:
            logger.info(f"🧠 AI Classification Result: {result}")
            return result
        else:
            return None # Fallback to keyword
            
    except Exception as e:
        logger.error(f"AI Classification Wrapper Error: {e}")
        return None


def classify_from_sms(message_body: str, use_ai: bool = False) -> dict:
    """
    Convenience wrapper for SMS message classification.
    
    This function is specifically designed for SMS messages and may include
    SMS-specific preprocessing (e.g., handling abbreviations, emojis).
    
    Args:
        message_body: The SMS message body
        use_ai: Whether to use AI classification
    
    Returns:
        Same format as classify_request_urgency()
    """
    return classify_request_urgency(message_body, use_ai=use_ai)


def classify_from_transcript(transcript_text: str, use_ai: bool = True) -> dict:
    """
    Classifies urgency from a call transcript.
    
    Transcripts may be longer and more conversational than SMS, so this function
    may use different heuristics (e.g., analyzing full conversation context).
    
    Args:
        transcript_text: The full call transcript
        use_ai: Whether to use AI (recommended for transcripts due to length)
    
    Returns:
        Same format as classify_request_urgency()
    """
    # For transcripts, AI classification is more valuable due to context
    return classify_request_urgency(transcript_text, use_ai=use_ai)

