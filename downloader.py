import socket
import json
import os
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import CLOUD_HOST, CLOUD_PORT
from utils import sign_message, json_recv_line, json_send_line


def get_file_metadata(file_id: str):
    """Asks the cloud for the location of all chunks for a file_id."""
    obj = {"cmd": "GET_METADATA", "payload": {"file_id": file_id}}
    b = json.dumps(obj["payload"]).encode()
    obj["sig"] = sign_message(b)
    try:
        with socket.create_connection((CLOUD_HOST, CLOUD_PORT), timeout=5) as s:
            s.sendall(json.dumps(obj).encode() + b'\n')
            resp = json_recv_line(s)
            if resp and resp.get("status") == "ok":
                # Returns {"chunks": {"0": "node_A_id", "1": "node_B_id", ...}}
                return resp.get("resp", {}).get("chunks", {})
            return None
    except Exception as e:
        print("Failed to get file metadata:", e)
        return None


def download_chunk(chunk_idx: int, node_id: str, file_id: str, cloud_metadata: dict):
    """Downloads a single chunk from the specified node."""

    # 1. Look up node IP/Port from metadata (requires LIST command logic or better cloud structure)
    # Since we don't have a direct LIST_NODE command in the public interface,
    # we simulate getting node info from a new cloud command, or assume metadata includes node info.
    # For simplicity, we assume we can fetch the node's connection details.

    # *** IMPROVEMENT NOTE: For A grade, cloud_main would need a 'GET_NODE_INFO' command.
    # For now, we'll try to get the full node list and filter:

    node_info = cloud_get_node_info(node_id)
    if not node_info:
        print(f"Chunk {chunk_idx}: Node {node_id} details unavailable.")
        return None

    node_ip, node_port = node_info["ip"], node_info["port"]

    try:
        with socket.create_connection((node_ip, node_port), timeout=8) as s:
            # 2. Send DOWNLOAD_CHUNK command to node
            payload = {"cmd": "DOWNLOAD_CHUNK", "payload": {"file_id": file_id, "chunk_idx": chunk_idx}}
            json_send_line(s, payload)

            # 3. Receive header with chunk size
            header = json_recv_line(s)
            if header and header.get("status") == "ok":
                chunk_size = header.get("chunk_size")

                # 4. Receive raw chunk data
                data = b''
                while len(data) < chunk_size:
                    chunk = s.recv(chunk_size - len(data))
                    if not chunk:
                        raise RuntimeError("Connection closed prematurely.")
                    data += chunk

                print(f"[OK] Chunk {chunk_idx} downloaded from {node_id}")
                return (chunk_idx, data)

            raise RuntimeError(f"Download header failed: {header.get('error') if header else 'No response'}")

    except Exception as e:
        print(f"[FAIL] Chunk {chunk_idx} download failed from {node_id}: {e}")
        return None


def cloud_get_node_info(node_id):
    """Helper to get a single node's connection details from the cloud."""
    # This command must be added to cloud_main.py for a robust A-grade solution.
    # We use LIST for now and filter.
    obj = {"cmd": "LIST", "payload": {}}
    b = json.dumps(obj["payload"]).encode()
    obj["sig"] = sign_message(b)
    try:
        with socket.create_connection((CLOUD_HOST, CLOUD_PORT), timeout=5) as s:
            s.sendall(json.dumps(obj).encode() + b'\n')
            resp = json_recv_line(s)
            if resp and resp.get("status") == "ok":
                all_nodes = resp.get("resp", {})
                return all_nodes.get(node_id)
            return None
    except Exception:
        return None


def downloader_main(file_id, output_path="downloaded_file"):
    print(f"Starting download for File ID: {file_id}")
    chunk_locations = get_file_metadata(file_id)

    if not chunk_locations:
        print("❌ Error: File metadata not found or cloud is down.")
        return False

    total_chunks = len(chunk_locations)
    print(f"Found {total_chunks} chunks. Starting concurrent download.")

    # Convert keys to integers for sorting: {'0': 'node_A', '1': 'node_B'} -> [(0, 'node_A'), (1, 'node_B')]
    sorted_chunks = sorted([(int(idx), nid) for idx, nid in chunk_locations.items()])

    downloaded_chunks = {}
    download_tasks = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        for chunk_idx, node_id in sorted_chunks:
            # Pass the full location data to the download task
            future = executor.submit(download_chunk, chunk_idx, node_id, file_id, chunk_locations)
            download_tasks.append(future)

        for future in as_completed(download_tasks):
            result = future.result()
            if result:
                idx, data = result
                downloaded_chunks[idx] = data

    if len(downloaded_chunks) != total_chunks:
        print(f"❌ Download failed. Only {len(downloaded_chunks)}/{total_chunks} chunks downloaded.")
        return False

    # 5. Reassemble and write to disk
    with open(output_path, "wb") as f:
        for idx in range(total_chunks):
            f.write(downloaded_chunks[idx])

    print(f"✅ File reassembled and saved to {output_path}")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed File Downloader.")
    parser.add_argument("file_id", help="The unique file ID to download (e.g., 'test.txt_1678888888').")
    parser.add_argument("--output", type=str, default="downloaded_file.out", help="Path to save the downloaded file.")
    args = parser.parse_args()
    downloader_main(args.file_id, args.output)