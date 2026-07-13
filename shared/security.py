import hmac
import hashlib
import json

def generate_hmac_signature(secret_key: str, payload: dict) -> str:
    """Generates a SHA-256 HMAC signature for the webhook payload."""
    
    # 1. Convert the secret string into raw bytes
    secret_bytes = secret_key.encode('utf-8')
    
    # 2. Convert the payload dictionary into a tight JSON string, then into bytes
    payload_bytes = json.dumps(payload, separators=(',', ':')).encode('utf-8')
    
    # 3. Hash them together using the SHA-256 algorithm
    signature = hmac.new(secret_bytes, payload_bytes, hashlib.sha256).hexdigest()
    
    # 4. Return standard industry format
    return f"sha256={signature}"