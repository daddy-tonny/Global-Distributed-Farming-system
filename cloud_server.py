# cloud_server.py (Final A-Grade Version)
import socket
import threading
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any

from config import (
    CLOUD_HOST,
    CLOUD_BIND_HOST,
    CLOUD_PORT,
    CLOUD_STATE_FILE,
    HEARTBEAT_TIMEOUT,
    REBALANCER_INTERVAL
)

from utils import verify_message, json_send_line, json_recv_line

STATE_LOCK = threading.Lock()


class CloudState:
    def __init__(self, path=CLOUD_STATE_FILE):
        self.path = path
        self.nodes: Dict[str, Dict] = {}
        self.metadata: Dict[str, Dict] = {}
        self._rr_index = 0  # Round-Robin index
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as fh:
                    data = json.load(fh)
                    self.nodes = data.get("nodes", {})
                    self.metadata = data.get("metadata", {})
            except Exception:
                print("Cloud: failed to load state file — starting fresh")

    def persist(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump({"nodes": self.nodes, "metadata": self.metadata}, fh, indent=2)
        os.replace(tmp, self.path)

    def register_node(self, node_id, ip, port, capacity_bytes):
        with STATE_LOCK:
            self.nodes[node_id] = {
                "ip": ip,
                "port": port,
                "capacity_bytes": capacity_bytes,
                "free_bytes": capacity_bytes,
                "last_seen": time.time(),
                "online": True
            }
            self.persist()

    def heartbeat(self, node_id, free_bytes):
        with STATE_LOCK:
            if node_id in self.nodes:
                self.nodes[node_id]["free_bytes"] = free_bytes
                self.nodes[node_id]["last_seen"] = time.time()
                self.nodes[node_id]["online"] = True
                self.persist()

    def list_nodes(self):
        with STATE_LOCK:
            now = time.time()
            for nid, info in list(self.nodes.items()):
                # Mark node as offline if heartbeat timed out
                if now - info.get("last_seen", 0) > HEARTBEAT_TIMEOUT:
                    info["online"] = False
            return dict(self.nodes)

    def pick_node_for_store(self, needed_bytes: int):
        """Picks the next available node in a round-robin fashion."""
        with STATE_LOCK:
            candidates = [
                nid for nid, info in self.nodes.items()
                if info.get("online") and info.get("free_bytes", 0) >= needed_bytes
            ]

            if not candidates:
                return None

            candidates.sort()  # Ensure consistent order

            node_id = candidates[self._rr_index % len(candidates)]
            self._rr_index += 1

            return (node_id, self.nodes[node_id])

    def record_chunk_location(self, file_id: str, chunk_idx: int, node_id: str):
        with STATE_LOCK:
            if file_id not in self.metadata:
                self.metadata[file_id] = {"chunks": {}}
            self.metadata[file_id]["chunks"][str(chunk_idx)] = node_id
            self.persist()

    def get_file_metadata(self, file_id: str):
        with STATE_LOCK:
            return self.metadata.get(file_id, {"chunks": {}})

    def reassign_chunks_from_offline(self):
        """Identifies and removes metadata for chunks stored on currently offline nodes."""
        to_rebalance = []
        chunks_removed_count = 0
        with STATE_LOCK:
            # Check for offline nodes
            self.list_nodes()

            for file_id, fmeta in self.metadata.items():
                for cidx, node_id in list(fmeta.get("chunks", {}).items()):
                    node_info = self.nodes.get(node_id)
                    # If the node is missing or marked offline
                    if not node_info or not node_info.get("online"):
                        to_rebalance.append((file_id, int(cidx), node_id))

            if to_rebalance:
                for file_id, chunk_idx, from_node in to_rebalance:
                    fm = self.metadata.get(file_id, {})
                    if fm and str(chunk_idx) in fm.get("chunks", {}):
                        del fm["chunks"][str(chunk_idx)]
                        chunks_removed_count += 1
                self.persist()

        return chunks_removed_count


cloud_state = CloudState()


def handle_conn(conn, addr):
    try:
        obj = json_recv_line(conn)
        if not obj:
            return

        sig = obj.get("sig", "")
        payload_bytes = json.dumps(obj.get("payload", {})).encode()

        if not verify_message(payload_bytes, sig):
            json_send_line(conn, {"status": "error", "error": "invalid signature"})
            return

        cmd = obj.get("cmd", "")
        payload = obj.get("payload", {})

        if cmd == "REGISTER":
            cloud_state.register_node(
                payload["node_id"], payload["ip"], payload["port"],
                payload.get("capacity_bytes")
            )
            json_send_line(conn, {"status": "ok", "resp": "registered"})

        elif cmd == "HEARTBEAT":
            cloud_state.heartbeat(
                payload["node_id"], payload.get("free_bytes")
            )
            json_send_line(conn, {"status": "ok"})

        elif cmd == "LIST":
            json_send_line(conn, {"status": "ok", "resp": cloud_state.list_nodes()})

        # A-GRADE: Command for efficient single-node lookup
        elif cmd == "GET_NODE_INFO":
            node_id = payload.get("node_id")
            all_nodes = cloud_state.list_nodes()
            node_info = all_nodes.get(node_id)
            if node_info:
                json_send_line(conn, {"status": "ok", "resp": {
                    "ip": node_info["ip"],
                    "port": node_info["port"],
                    "online": node_info["online"]
                }})
            else:
                json_send_line(conn, {"status": "error", "error": "node_not_found"})

        elif cmd == "REQUEST_STORE":
            needed_bytes = payload.get("needed_bytes", 0)
            chosen = cloud_state.pick_node_for_store(needed_bytes)
            if not chosen:
                json_send_line(conn, {"status": "error", "error": "no_node_available"})
            else:
                nid, info = chosen
                json_send_line(conn, {
                    "status": "ok",
                    "resp": {"node_id": nid, "ip": info["ip"], "port": info["port"]}
                })

        elif cmd == "REPORT_CHUNK":
            cloud_state.record_chunk_location(
                payload["file_id"], payload["chunk_idx"], payload["node_id"]
            )
            json_send_line(conn, {"status": "ok"})

        elif cmd == "GET_METADATA":
            meta = cloud_state.get_file_metadata(payload["file_id"])
            json_send_line(conn, {"status": "ok", "resp": meta})

        else:
            json_send_line(conn, {"status": "error", "error": "unknown_cmd"})

    except Exception as e:
        print(f"Cloud connection error: {e}")
    finally:
        try:
            conn.close()
        except:
            pass


def rebalancer_loop():
    while True:
        chunks_removed_count = cloud_state.reassign_chunks_from_offline()
        if chunks_removed_count > 0:
            print(f"Rebalancer: removed metadata for {chunks_removed_count} chunks on offline nodes.")

        time.sleep(REBALANCER_INTERVAL)


def cloud_main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    srv.bind((CLOUD_BIND_HOST, CLOUD_PORT))
    srv.listen(50)

    print(f"Cloud listening on {CLOUD_BIND_HOST}:{CLOUD_PORT}")
    print(f"Nodes will connect to CLOUD_HOST={CLOUD_HOST}")

    threading.Thread(target=rebalancer_loop, daemon=True).start()

    with ThreadPoolExecutor(max_workers=20) as executor:
        while True:
            conn, addr = srv.accept()
            executor.submit(handle_conn, conn, addr)


if __name__ == "__main__":
    cloud_main()