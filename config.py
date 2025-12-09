# config.py
import os

# -------------------------------------------------------------------
# CLOUD HOST CONFIGURATION
# -------------------------------------------------------------------
# Notes:
#  - When running cloud_server.py and node.py on the SAME machine, Windows
#    does NOT allow nodes to connect to "0.0.0.0".
#  - "0.0.0.0" is valid ONLY for server binding, NOT for client connections.
#  - Use CLOUD_BIND_HOST for server binding, and CLOUD_HOST for nodes/clients to connect.

# Cloud binds to this address (server listens here)
CLOUD_BIND_HOST = "0.0.0.0"  # listen on all interfaces

# Cloud host used BY NODES to connect
CLOUD_HOST = "127.0.0.1"      # always use localhost for same-machine nodes

# Cloud server listening port
CLOUD_PORT = 5000

# -------------------------------------------------------------------
# SECURITY + STORAGE CONFIGURATION
# -------------------------------------------------------------------

# HMAC secret for message signing (demo; replace with secure secret for production)
HMAC_SECRET = os.environ.get("CLOUD_HMAC_SECRET", "supersecretkey_for_demo_only")

# State persistence file
CLOUD_STATE_FILE = "cloud_state.json"

# Default node storage capacity (15 GB)
DEFAULT_NODE_CAPACITY = 15 * 1024**3

# Heartbeat timeout (seconds) — node considered offline if no heartbeat in this time
HEARTBEAT_TIMEOUT = 12

# Rebalancer interval (seconds) — how often cloud checks offline nodes / missing chunks
REBALANCER_INTERVAL = 10

# Default chunk size for file transfer (64 KB)
DEFAULT_CHUNK_SIZE = 64 * 1024

# Uploader configuration - Number of times to retry a failed chunk upload
UPLOAD_RETRIES = 3
