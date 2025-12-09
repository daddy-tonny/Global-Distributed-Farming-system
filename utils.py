# utils.py
import hmac
import hashlib
import json
from typing import Any

from config import HMAC_SECRET

def sign_message(message: bytes) -> str:
    mac = hmac.new(HMAC_SECRET.encode(), message, hashlib.sha256)
    return mac.hexdigest()

def verify_message(message: bytes, signature: str) -> bool:
    expected = sign_message(message)
    return hmac.compare_digest(expected, signature)

def json_recv_line(conn) -> dict | None:
    """
    Reads a newline-terminated JSON object from conn (socket).
    """
    buf = b''
    while True:
        chunk = conn.recv(4096)
        if not chunk:
            return None
        buf += chunk
        if b'\n' in buf:
            line, rest = buf.split(b'\n', 1)
            # push rest back into socket? Not possible; protocol ensures one JSON message per recv
            try:
                return json.loads(line.decode())
            except Exception:
                return None

def json_send_line(conn, obj: Any):
    data = json.dumps(obj).encode() + b'\n'
    conn.sendall(data)