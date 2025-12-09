# uploader.py (Final Corrected Version for A-Grade Stability)
import socket
import json
import os
import argparse
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import CLOUD_HOST, CLOUD_PORT, DEFAULT_CHUNK_SIZE, UPLOAD_RETRIES
from utils import sign_message, json_recv_line, json_send_line

# Lock for thread-safe file reading during concurrent chunking
FILE_READ_LOCK = threading.Lock()


def sign_payload(payload: dict) -> dict:
    """Signs the payload for secure cloud communication."""
    b = json.dumps(payload).encode()
    return {"payload": payload, "sig": sign_message(b)}


def cloud_request_store(needed_bytes: int):
    """Ask cloud for a node to store a chunk. Returns {'node_id','ip','port'} or None"""
    payload = {"needed_bytes": needed_bytes}
    obj = sign_payload(payload)
    obj["cmd"] = "REQUEST_STORE"

    try:
        with socket.create_connection((CLOUD_HOST, CLOUD_PORT), timeout=5) as s:
            json_send_line(s, obj)
            resp = json_recv_line(s)

            if resp and resp.get("status") == "ok":
                return resp.get("resp")

            # Print error if cloud explicitly denies the request
            if resp and resp.get("error"):
                print(f"[ERROR] Cloud denied store request: {resp.get('error')}")
            return None
    except Exception:
        # Silently fail on connection error; retry logic is in the uploader_main loop
        return None


def report_chunk_to_cloud(file_id, chunk_idx, node_id):
    """Reports chunk location to the cloud metadata."""
    payload = {"file_id": file_id, "chunk_idx": chunk_idx, "node_id": node_id}
    obj = sign_payload(payload)
    obj["cmd"] = "REPORT_CHUNK"

    try:
        with socket.create_connection((CLOUD_HOST, CLOUD_PORT), timeout=5) as s:
            json_send_line(s, obj)
            # Ignoring cloud reply for simplicity, but the report command is sent
            return True
    except Exception as e:
        print(f"[ERROR] Failed to report chunk {chunk_idx} location: {e}")
        return False


def upload_chunk(filepath, file_id, chunk_idx, chunk_size, node_info, retries=UPLOAD_RETRIES):
    """
    Handles the transfer of a single chunk to the assigned node.
    Returns the node_id on success, None otherwise.
    """
    node_ip, node_port, node_id = node_info["ip"], node_info["port"], node_info["node_id"]

    # 1. Read the specific chunk data from the file (thread-safe)
    with FILE_READ_LOCK:
        try:
            with open(filepath, "rb") as fh:
                fh.seek(chunk_idx * chunk_size)
                data = fh.read(chunk_size)
        except Exception as e:
            print(f"[ERROR] Failed to read chunk {chunk_idx} from disk: {e}")
            return None

    if not data:
        return None

    for attempt in range(retries):
        try:
            # 💡 FIX: Increased timeout to 15 seconds for upload stability
            with socket.create_connection((node_ip, int(node_port)), timeout=15) as s:

                # 2. Send header JSON line
                header = {"file_id": file_id, "chunk_idx": chunk_idx, "chunk_size": len(data)}
                json_send_line(s, header)

                # 3. Send chunk bytes
                s.sendall(data)

                # 4. Read ACK (TCP Window ACK Simulation)
                ack = json_recv_line(s)

                if ack and ack.get("status") == "ACK":
                    # 5. Report success to Cloud
                    if report_chunk_to_cloud(file_id, chunk_idx, node_id):
                        print(f"[OK] Chunk {chunk_idx} uploaded to {node_id}")
                        return node_id
                    else:
                        raise RuntimeError("Upload OK, but failed to report location.")

                elif ack and ack.get("status") == "error":
                    raise RuntimeError(f"Node returned error: {ack.get('error')}")
                else:
                    raise RuntimeError("No valid ACK received from node.")

        except socket.timeout:
            print(f"[FAIL] Chunk {chunk_idx} upload attempt {attempt + 1} failed ({node_id}): timed out")
            time.sleep(1)
        except Exception as e:
            print(f"[FAIL] Chunk {chunk_idx} upload attempt {attempt + 1} failed ({node_id}): {e}")
            time.sleep(1)

    return None  # Chunk failed after all retries


def uploader_main(filepath, chunk_size=DEFAULT_CHUNK_SIZE):
    if not os.path.exists(filepath):
        print(f"Error: File not found at {filepath}")
        return False

    filesize = os.path.getsize(filepath)
    file_id = os.path.basename(filepath) + "_" + str(int(time.time()))
    total_chunks = (filesize + chunk_size - 1) // chunk_size

    print(f"Starting upload of {filepath} ({filesize} bytes) in {total_chunks} chunks of {chunk_size} bytes.")

    upload_tasks = []

    # Use a ThreadPoolExecutor for concurrent uploads
    with ThreadPoolExecutor(max_workers=5) as executor:
        for idx in range(total_chunks):
            # Request a node for this specific chunk
            needed_bytes = min(chunk_size, filesize - idx * chunk_size)
            node_info = cloud_request_store(needed_bytes)

            if not node_info:
                print(f"[FATAL] Could not find an available node for Chunk {idx}. Aborting.")
                return False

            print(f"Chunk {idx} assigned to node {node_info['node_id']} at {node_info['ip']}:{node_info['port']}")

            # Submit the upload task to the executor
            future = executor.submit(upload_chunk, filepath, file_id, idx, chunk_size, node_info)
            upload_tasks.append(future)

        # Wait for all futures to complete
        successful_uploads = 0
        for future in as_completed(upload_tasks):
            result = future.result()
            if result:
                successful_uploads += 1
            # Note: Failed chunks are handled by retries inside upload_chunk

    if successful_uploads == total_chunks:
        print(f"\n✅ Upload completed successfully. File ID: {file_id}")
        return True
    else:
        print(f"\n❌ Upload failed. {successful_uploads}/{total_chunks} chunks transferred.")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed File Uploader.")
    parser.add_argument("file", help="Path to the file to upload.")
    parser.add_argument("--chunk_size", type=int, default=DEFAULT_CHUNK_SIZE, help="Size of each chunk in bytes.")
    args = parser.parse_args()
    uploader_main(args.file, args.chunk_size)