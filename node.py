# node.py
import argparse
import json
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from config import (
    CLOUD_HOST,
    CLOUD_PORT,
    DEFAULT_NODE_CAPACITY,
    DEFAULT_CHUNK_SIZE,
    HEARTBEAT_INTERVAL,
)
from utils import sign_message, json_send_line, json_recv_line
from virtualdisk import VirtualDisk


class Node:
    def __init__(self, port: int, storage_path: str, capacity_bytes=DEFAULT_NODE_CAPACITY,
                 chunk_size=DEFAULT_CHUNK_SIZE, max_workers=8):
        self.node_id = str(uuid4())[:8]
        self.ip = "127.0.0.1"
        self.port = int(port)
        self.disk = VirtualDisk(storage_path, capacity_bytes)
        self.chunk_size = int(chunk_size)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.stop_event = threading.Event()

    # build signed message object expected by cloud_server (payload + sig + cmd)
    def _signed_obj(self, cmd: str, payload: dict):
        b = json.dumps(payload).encode()
        return {"cmd": cmd, "payload": payload, "sig": sign_message(b)}

    def register_with_cloud(self, retries=5, delay=2):
        payload = {
            "node_id": self.node_id,
            "ip": self.ip,
            "port": self.port,
            "capacity_bytes": self.disk.capacity_bytes,
        }
        obj = self._signed_obj("REGISTER", payload)

        for attempt in range(1, retries + 1):
            try:
                with socket.create_connection((CLOUD_HOST, CLOUD_PORT), timeout=5) as s:
                    json_send_line(s, obj)
                    resp = json_recv_line(s)
                    if resp and resp.get("status") == "ok":
                        print(f"[INFO] Registered with cloud: {resp.get('resp')}")
                        return True
                    else:
                        print(f"[WARN] Register attempt {attempt} failed: {resp}")
            except Exception as e:
                print(f"[WARN] Attempt {attempt} failed to register with cloud: {e}")
            time.sleep(delay)
        print("[ERROR] Failed to register with cloud after retries.")
        return False

    def heartbeat_loop(self):
        # periodic heartbeat; do not persist on cloud side (cloud handles that)
        while not self.stop_event.is_set():
            payload = {"node_id": self.node_id, "free_bytes": self.disk.free_bytes()}
            obj = self._signed_obj("HEARTBEAT", payload)
            try:
                with socket.create_connection((CLOUD_HOST, CLOUD_PORT), timeout=3) as s:
                    json_send_line(s, obj)
                    # optional: read response but ignore for now
                    _ = json_recv_line(s)
            except Exception:
                # silent fail — cloud may be down; we'll retry next interval
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    def handle_connection(self, conn):
        """
        Handles both upload (store chunk) and download (serve chunk).
        Protocol: client sends one JSON line (json_recv_line) describing action.
        For uploads: header with file_id, chunk_idx, chunk_size followed by raw bytes.
        For downloads: header with cmd=DOWNLOAD_CHUNK and payload {file_id, chunk_idx}.
        """
        try:
            header = json_recv_line(conn)
            if not header:
                return

            # DOWNLOAD request: {"cmd":"DOWNLOAD_CHUNK","payload":{...}}
            if header.get("cmd") == "DOWNLOAD_CHUNK":
                payload = header.get("payload", {})
                file_id = payload.get("file_id")
                chunk_idx = payload.get("chunk_idx")
                if file_id is None or chunk_idx is None:
                    json_send_line(conn, {"status": "error", "error": "bad_request"})
                    return

                chunk_id = f"{file_id}_chunk_{chunk_idx}"
                data = self.disk.read_chunk(chunk_id)
                if data is None:
                    json_send_line(conn, {"status": "error", "error": "chunk_not_found"})
                    return

                # send header then raw bytes
                json_send_line(conn, {"status": "ok", "chunk_size": len(data)})
                conn.sendall(data)
                return

            # Else: treat as upload header (no explicit cmd required)
            # Expect header contains: {"file_id":..., "chunk_idx":..., "chunk_size":...}
            file_id = header.get("file_id") or (header.get("payload") or {}).get("file_id")
            chunk_idx = header.get("chunk_idx") or (header.get("payload") or {}).get("chunk_idx")
            size = header.get("chunk_size") or (header.get("payload") or {}).get("chunk_size")

            if file_id is None or chunk_idx is None or size is None:
                json_send_line(conn, {"status": "error", "error": "bad_header"})
                return

            size = int(size)
            # read exactly `size` bytes
            data = bytearray()
            remaining = size
            while remaining > 0:
                chunk = conn.recv(min(65536, remaining))
                if not chunk:
                    break
                data.extend(chunk)
                remaining -= len(chunk)

            if len(data) != size:
                json_send_line(conn, {"status": "error", "error": "incomplete_chunk"})
                return

            chunk_id = f"{file_id}_chunk_{chunk_idx}"
            try:
                self.disk.write_chunk(chunk_id, bytes(data))
            except IOError as e:
                json_send_line(conn, {"status": "error", "error": str(e)})
                return

            # send ACK
            json_send_line(conn, {"status": "ACK", "node_id": self.node_id, "chunk_idx": chunk_idx})

        except Exception as e:
            print(f"[ERROR] Connection handler exception: {e}")
        finally:
            try:
                conn.close()
            except:
                pass

    def start_listener(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.ip, self.port))
        srv.listen(50)
        print(f"[INFO] Node {self.node_id} listening on {self.ip}:{self.port}")

        try:
            while not self.stop_event.is_set():
                try:
                    conn, addr = srv.accept()
                    # handle each connection in thread pool
                    self.executor.submit(self.handle_connection, conn)
                except Exception:
                    # accept loop should keep running
                    continue
        finally:
            try:
                srv.close()
            except:
                pass

    def run(self):
        registered = self.register_with_cloud()
        # start heartbeat unconditionally (keeps informing cloud when it comes up)
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        if not registered:
            print("[WARN] Initial registration failed; node will continue heartbeats and retry registration in background.")

            # background retry thread for registration (non-blocking)
            def retry_reg():
                while not self.stop_event.is_set():
                    if self.register_with_cloud(retries=3, delay=2):
                        break
                    time.sleep(10)
            threading.Thread(target=retry_reg, daemon=True).start()

        # start listener (blocking)
        self.start_listener()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Node process")
    parser.add_argument("--port", type=int, default=9001, help="Port to listen on")
    parser.add_argument("--storage", type=str, default="./node_storage", help="Storage directory")
    parser.add_argument("--capacity", type=int, default=DEFAULT_NODE_CAPACITY, help="Storage capacity bytes")
    args = parser.parse_args()

    node = Node(port=args.port, storage_path=args.storage, capacity_bytes=args.capacity)
    node.run()
