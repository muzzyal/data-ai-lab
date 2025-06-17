import hmac
import binascii
import hashlib
import json

SECRET_KEY = "ebaa8f06c41a7c42c71528110ee9758ceb911af78bcbccc560a0323bc08b50f3"

# Read the JSON file
with open("this.json", "rb") as f:
    body = f.read()  # Read raw bytes

# Generate HMAC signature
signature = hmac.new(binascii.a2b_hex(SECRET_KEY), body, hashlib.sha512).hexdigest()

print("Generated Signature:", signature)
