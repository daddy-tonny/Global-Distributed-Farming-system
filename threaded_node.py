# enhanced_threaded_node.py
"""
Enhanced version of threaded_node.py with file upload/download capabilities
and local file management with cloud storage integration.
"""

import threading
import time
import json
import socket
import uuid
import math
import hashlib
import os
import sqlite3
import shutil
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from pathlib import Path
from network_protocols import NetworkStack, NetworkPacket
import random
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class LocalTransfer:
    """Represents a file transfer from this node's perspective"""
    transfer_id: str
    file_name: str
    file_size: int
    target_node_id: str
    chunks_total: int
    chunks_sent: int = 0
    status: str = "PENDING"
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()

@dataclass
class IncomingTransfer:
    """Represents an incoming file transfer"""
    transfer_id: str
    file_name: str
    file_size: int
    source_node_id: str
    chunks_total: int
    chunks_received: int = 0
    status: str = "PENDING"
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
    
    def get_progress_percent(self) -> float:
        if self.chunks_total == 0:
            return 0.0
        return (self.chunks_received / self.chunks_total) * 100.0

class FileManager:
    """Enhanced file manager with local and cloud file management"""
    
    def __init__(self, node_id: str, storage_path: str = None):
        self.node_id = node_id
        self.storage_path = Path(storage_path) if storage_path else Path(f"./storage_{node_id}")
        self.storage_path.mkdir(exist_ok=True)
        
        # Create separate directories for different types of files
        self.local_files_path = self.storage_path / "local_files"
        self.cloud_files_path = self.storage_path / "cloud_files"
        self.replica_files_path = self.storage_path / "replicas"
        self.downloads_path = self.storage_path / "downloads"
        
        self.local_files_path.mkdir(exist_ok=True)
        self.cloud_files_path.mkdir(exist_ok=True)
        self.replica_files_path.mkdir(exist_ok=True)
        self.downloads_path.mkdir(exist_ok=True)
        
        # Initialize database for file metadata
        self.db_path = self.storage_path / "file_metadata.db"
        self._init_database()
        
        # File locks for concurrent access
        self.file_locks = {}
        self.lock_manager = threading.RLock()
        
    def _init_database(self):
        """Initialize SQLite database for file metadata"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Local files table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS local_files (
                file_id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                original_path TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                checksum TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Cloud files table (files uploaded to the distributed system)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cloud_files (
                file_id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                checksum TEXT NOT NULL,
                upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                replica_nodes TEXT NOT NULL  -- JSON array of node IDs
            )
        ''')
        
        # Replica files table (files replicated from other nodes)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS replica_files (
                file_id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                checksum TEXT NOT NULL,
                source_node_id TEXT NOT NULL,
                replicated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Downloaded files table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS downloaded_files (
                file_id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                checksum TEXT NOT NULL,
                source_node_id TEXT NOT NULL,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        
    def add_local_file(self, filename: str, source_file_path: str) -> dict:
        """Add a file to local storage"""
        if not os.path.exists(source_file_path):
            raise FileNotFoundError(f"File not found: {source_file_path}")
            
        file_id = str(uuid.uuid4())
        file_size = os.path.getsize(source_file_path)
        checksum = self._calculate_file_checksum(source_file_path)
        
        # Copy file to local storage
        local_file_path = self.local_files_path / f"{file_id}_{filename}"
        shutil.copy2(source_file_path, local_file_path)
        
        # Store metadata in database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO local_files (file_id, filename, original_path, file_path, file_size, checksum)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (file_id, filename, source_file_path, str(local_file_path), file_size, checksum))
        conn.commit()
        conn.close()
        
        return {
            'file_id': file_id,
            'filename': filename,
            'file_size': file_size,
            'checksum': checksum,
            'local_path': str(local_file_path)
        }
    
    def get_local_files(self) -> List[dict]:
        """Get list of all local files"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM local_files ORDER BY created_at DESC')
        rows = cursor.fetchall()
        conn.close()
        
        files = []
        for row in rows:
            files.append({
                'file_id': row[0],
                'filename': row[1],
                'original_path': row[2],
                'file_path': row[3],
                'file_size': row[4],
                'checksum': row[5],
                'created_at': row[6]
            })
        return files
    
    def get_local_file_by_id(self, file_id: str) -> Optional[dict]:
        """Get a specific local file by ID"""
        files = self.get_local_files()
        for file_info in files:
            if file_info['file_id'] == file_id:
                return file_info
        return None
    
    def store_replica(self, file_id: str, filename: str, file_data: bytes, 
                     checksum: str, source_node_id: str) -> bool:
        """Store a replica file from another node"""
        try:
            # Verify checksum
            calculated_checksum = hashlib.sha256(file_data).hexdigest()
            if calculated_checksum != checksum:
                logger.error(f"Checksum mismatch for replica {file_id}")
                return False
            
            # Store the file
            replica_path = self.replica_files_path / f"{file_id}_{filename}"
            with open(replica_path, 'wb') as f:
                f.write(file_data)
            
            # Store metadata
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO replica_files 
                (file_id, filename, file_path, file_size, checksum, source_node_id)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (file_id, filename, str(replica_path), len(file_data), checksum, source_node_id))
            conn.commit()
            conn.close()
            
            logger.info(f"Stored replica {file_id} from {source_node_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store replica {file_id}: {e}")
            return False
    
    def get_file_for_download(self, file_id: str) -> Optional[Tuple[str, bytes]]:
        """Get file data for download (check local, then replicas)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check local files first
        cursor.execute('SELECT filename, file_path FROM local_files WHERE file_id = ?', (file_id,))
        row = cursor.fetchone()
        if row:
            try:
                with open(row[1], 'rb') as f:
                    conn.close()
                    return row[0], f.read()
            except Exception as e:
                logger.error(f"Error reading local file {file_id}: {e}")
        
        # Check replicas
        cursor.execute('SELECT filename, file_path FROM replica_files WHERE file_id = ?', (file_id,))
        row = cursor.fetchone()
        if row:
            try:
                with open(row[1], 'rb') as f:
                    conn.close()
                    return row[0], f.read()
            except Exception as e:
                logger.error(f"Error reading replica file {file_id}: {e}")
        
        conn.close()
        return None
    
    def save_downloaded_file(self, file_id: str, filename: str, file_data: bytes, 
                           source_node_id: str) -> str:
        """Save a downloaded file"""
        checksum = hashlib.sha256(file_data).hexdigest()
        download_path = self.downloads_path / f"downloaded_{filename}"
        
        # Ensure unique filename if file already exists
        counter = 1
        while download_path.exists():
            name_part = Path(filename).stem
            ext_part = Path(filename).suffix
            download_path = self.downloads_path / f"downloaded_{name_part}_{counter}{ext_part}"
            counter += 1
        
        # Save file
        with open(download_path, 'wb') as f:
            f.write(file_data)
        
        # Store metadata
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO downloaded_files 
            (file_id, filename, file_path, file_size, checksum, source_node_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (file_id, filename, str(download_path), len(file_data), checksum, source_node_id))
        conn.commit()
        conn.close()
        
        return str(download_path)
    
    def _calculate_file_checksum(self, file_path: str) -> str:
        """Calculate SHA256 checksum of a file"""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    def get_storage_stats(self) -> dict:
        """Get storage statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get counts and sizes for each category
        stats = {}
        
        for table, category in [('local_files', 'local'), ('replica_files', 'replicas'), 
                               ('downloaded_files', 'downloads')]:
            cursor.execute(f'SELECT COUNT(*), SUM(file_size) FROM {table}')
            count, size = cursor.fetchone()
            stats[category] = {
                'count': count or 0,
                'size_mb': (size or 0) / (1024 * 1024)
            }
        
        conn.close()
        return stats

class ThreadedStorageNode:
    """Enhanced storage node with file upload/download capabilities"""
    
    def __init__(self, node_id: str, ip_address: str, mac_address: str,
                 storage_capacity_gb: int = 100, bandwidth_mbps: int = 1000,
                 network_host: str = 'localhost', network_port: int = 8888):
        
        self.node_id = node_id
        self.ip_address = ip_address
        self.mac_address = mac_address
        self.storage_capacity = storage_capacity_gb * 1024 * 1024 * 1024  # Convert to bytes
        self.bandwidth = bandwidth_mbps
        
        # Network connection
        self.network_host = network_host
        self.network_port = network_port
        self.network_socket: Optional[socket.socket] = None
        self.connected_to_network = False
        
        # Initialize network stack for TCP/IP simulation
        self.network_stack = NetworkStack(node_id, ip_address, mac_address)
        
        # NEW: File manager for enhanced file operations
        self.file_manager = FileManager(node_id, f"./storage/{node_id}")
        
        # Storage management (updated to use file manager)
        self.used_storage = 0
        
        # Transfer management with threading
        self.outgoing_transfers: Dict[str, LocalTransfer] = {}
        self.incoming_transfers: Dict[str, IncomingTransfer] = {}
        self.transfer_threads: Dict[str, threading.Thread] = {}
        self.transfer_lock = threading.RLock()
        
        # Performance metrics
        self.total_transfers_sent = 0
        self.total_transfers_received = 0
        self.total_data_transferred = 0
        self.files_uploaded = 0
        self.files_downloaded = 0
        
        # Node status
        self.running = False
        
        # Network message listener thread
        self.network_listener_thread = None
        
    def connect_to_network(self) -> bool:
        """Connect this node to the network manager"""
        try:
            self.network_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.network_socket.connect((self.network_host, self.network_port))
            
            # Register with network
            registration_msg = {
                'type': 'register_node',
                'node_id': self.node_id,
                'node_info': {
                    'ip_address': self.ip_address,
                    'mac_address': self.mac_address,
                    'storage_capacity_gb': self.storage_capacity / (1024**3),
                    'bandwidth_mbps': self.bandwidth
                }
            }
            
            self.network_socket.send(json.dumps(registration_msg).encode())
            response_data = self.network_socket.recv(1024)
            response = json.loads(response_data.decode())
            
            if response.get('status') == 'success':
                self.connected_to_network = True
                print(f" Node {self.node_id} connected to network")
                
                # Start network listener thread for incoming messages
                self.network_listener_thread = threading.Thread(
                    target=self._network_message_listener, 
                    daemon=True
                )
                self.network_listener_thread.start()
                
                return True
            else:
                print(f" Failed to register with network: {response.get('message')}")
                return False
                
        except Exception as e:
            print(f" Network connection failed: {e}")
            return False
            
    def _network_message_listener(self):
        """Listen for incoming messages from the network manager"""
        print(f" Network message listener started for {self.node_id}")
        
        while self.running and self.connected_to_network:
            try:
                self.network_socket.settimeout(2.0)
                data = self.network_socket.recv(8192)  # Increased buffer for file operations
                
                if not data:
                    print(f"🔌 Network connection closed")
                    self.connected_to_network = False
                    break
                    
                try:
                    message = json.loads(data.decode())
                    self._handle_network_message(message)
                except json.JSONDecodeError:
                    print(f" Invalid JSON received from network")
                    continue
                    
            except socket.timeout:
                continue
            except Exception as e:
                print(f" Network listener error: {e}")
                self.connected_to_network = False
                break
                
        print(f" Network message listener stopped for {self.node_id}")
        
    def _handle_network_message(self, message: dict):
        """Enhanced message handling with file operations"""
        msg_type = message.get('type')
        
        # Existing message handlers
        if msg_type == 'incoming_transfer':
            self._handle_incoming_transfer_notification(message)
        elif msg_type == 'transfer_progress':
            self._handle_transfer_progress_update(message)
        elif msg_type == 'transfer_completed':
            self._handle_transfer_completion(message)
        elif msg_type == 'transfer_failed':
            self._handle_transfer_failure(message)
        elif msg_type == 'heartbeat':
            self._respond_to_heartbeat(message)
        
        # NEW: File service message handlers
        elif msg_type == 'replicate_file':
            self._handle_file_replication_request(message)
        elif msg_type == 'download_file_request':
            self._handle_file_download_request(message)
    
    def _handle_file_replication_request(self, message: dict):
        """Handle file replication request"""
        try:
            file_id = message.get('file_id')
            filename = message.get('filename')
            file_size = message.get('file_size')
            checksum = message.get('checksum')
            source_node_id = message.get('source_node_id')
            
            print(f" Replication request: {filename} ({file_size/(1024*1024):.2f}MB) from {source_node_id}")
            
            # Send ready acknowledgment
            response = {'status': 'ready', 'message': 'Ready to receive replica'}
            self.network_socket.send(json.dumps(response).encode())
            
            # Receive file data
            file_data = b''
            while len(file_data) < file_size:
                remaining = file_size - len(file_data)
                chunk = self.network_socket.recv(min(8192, remaining))
                if not chunk:
                    break
                file_data += chunk
                
                # Show progress for large files
                if file_size > 5*1024*1024:  # Show progress for files > 5MB
                    progress = (len(file_data) / file_size) * 100
                    if progress % 20 < (progress - len(chunk)/file_size * 100) % 20:
                        print(f"    Replication progress: {progress:.1f}%")
            
            # Store replica
            success = self.file_manager.store_replica(
                file_id, filename, file_data, checksum, source_node_id
            )
            
            # Send final confirmation
            final_response = {
                'status': 'success' if success else 'error',
                'message': 'Replica stored successfully' if success else 'Failed to store replica'
            }
            self.network_socket.send(json.dumps(final_response).encode())
            
            if success:
                print(f" Replica stored: {filename}")
                self.used_storage += len(file_data)
            else:
                print(f" Failed to store replica: {filename}")
                
        except Exception as e:
            logger.error(f"Replication handling failed: {e}")
            error_response = {'status': 'error', 'message': str(e)}
            try:
                self.network_socket.send(json.dumps(error_response).encode())
            except:
                pass
    
    def _handle_file_download_request(self, message: dict):
        """Handle file download request"""
        try:
            file_id = message.get('file_id')
            
            print(f" Download request for file {file_id[:8]}...")
            
            # Get file data
            result = self.file_manager.get_file_for_download(file_id)
            
            if result:
                filename, file_data = result
                
                # Send success response with file info
                response = {
                    'status': 'success',
                    'filename': filename,
                    'file_size': len(file_data)
                }
                self.network_socket.send(json.dumps(response).encode())
                
                # Send file data in chunks
                total_sent = 0
                while total_sent < len(file_data):
                    chunk_end = min(total_sent + 8192, len(file_data))
                    chunk = file_data[total_sent:chunk_end]
                    self.network_socket.send(chunk)
                    total_sent += len(chunk)
                    
                    # Show progress for large files
                    if len(file_data) > 5*1024*1024:
                        progress = (total_sent / len(file_data)) * 100
                        if progress % 20 < (progress - len(chunk)/len(file_data) * 100) % 20:
                            print(f"    Upload progress: {progress:.1f}%")
                
                print(f" Sent file: {filename} ({len(file_data)/(1024*1024):.2f}MB)")
                
            else:
                # File not found
                response = {'status': 'error', 'message': 'File not found'}
                self.network_socket.send(json.dumps(response).encode())
                print(f" File {file_id[:8]}... not found")
                
        except Exception as e:
            logger.error(f"Download handling failed: {e}")
            error_response = {'status': 'error', 'message': str(e)}
            try:
                self.network_socket.send(json.dumps(error_response).encode())
            except:
                pass
    
    # Keep all existing methods and add new file management methods
    def _handle_incoming_transfer_notification(self, message: dict):
        """Handle notification about incoming transfer"""
        transfer_id = message.get('transfer_id')
        source_node_id = message.get('source_node_id')
        file_name = message.get('file_name')
        file_size = message.get('file_size')
        chunks_total = message.get('chunks_total')
        
        print(f"\n INCOMING TRANSFER NOTIFICATION")
        print(f"   From: {source_node_id}")
        print(f"   File: {file_name} ({file_size/(1024*1024):.1f}MB)")
        print(f"   Transfer ID: {transfer_id[:8]}...")
        
        if self.used_storage + file_size > self.storage_capacity:
            print(f" Insufficient storage for {file_name}")
            return
            
        incoming_transfer = IncomingTransfer(
            transfer_id=transfer_id,
            file_name=file_name,
            file_size=file_size,
            source_node_id=source_node_id,
            chunks_total=chunks_total,
            status="IN_PROGRESS"
        )
        
        with self.transfer_lock:
            self.incoming_transfers[transfer_id] = incoming_transfer
            
        print(f" Incoming transfer registered: {file_name}")
        
    def _handle_transfer_progress_update(self, message: dict):
        """Handle transfer progress update"""
        transfer_id = message.get('transfer_id')
        chunks_completed = message.get('chunks_completed')
        
        with self.transfer_lock:
            if transfer_id in self.incoming_transfers:
                self.incoming_transfers[transfer_id].chunks_received = chunks_completed
                progress = self.incoming_transfers[transfer_id].get_progress_percent()
                print(f" Transfer progress: {self.incoming_transfers[transfer_id].file_name} - {progress:.1f}%")
                
    def _handle_transfer_completion(self, message: dict):
        """Handle transfer completion notification"""
        transfer_id = message.get('transfer_id')
        
        with self.transfer_lock:
            if transfer_id in self.incoming_transfers:
                transfer = self.incoming_transfers[transfer_id]
                transfer.status = "COMPLETED"
                
                self.used_storage += transfer.file_size
                self.total_transfers_received += 1
                self.total_data_transferred += transfer.file_size
                
                storage_percent = (self.used_storage / self.storage_capacity) * 100
                print(f"\n TRANSFER COMPLETED")
                print(f"   File: {transfer.file_name} ({transfer.file_size/(1024*1024):.1f}MB)")
                print(f"   From: {transfer.source_node_id}")
                print(f"   Storage now: {storage_percent:.1f}% used")
                print(f"   Total files received: {self.total_transfers_received}")
                
    def _handle_transfer_failure(self, message: dict):
        """Handle transfer failure notification"""
        transfer_id = message.get('transfer_id')
        error_message = message.get('error_message', 'Unknown error')
        
        with self.transfer_lock:
            if transfer_id in self.incoming_transfers:
                self.incoming_transfers[transfer_id].status = "FAILED"
                print(f" INCOMING TRANSFER FAILED")
                print(f"   File: {self.incoming_transfers[transfer_id].file_name}")
                print(f"   Error: {error_message}")
                
    def _respond_to_heartbeat(self, message: dict):
        """Respond to network heartbeat"""
        try:
            response = {
                'type': 'heartbeat_response',
                'node_id': self.node_id,
                'timestamp': time.time()
            }
            self.network_socket.send(json.dumps(response).encode())
        except Exception as e:
            print(f" Failed to respond to heartbeat: {e}")
            
    def start_node(self):
        """Start the node and begin accepting commands"""
        self.running = True
        
        if not self.connect_to_network():
            print(" Cannot start node without network connection")
            return
            
        print(f"\n{'='*70}")
        print(f"  ENHANCED STORAGE NODE: {self.node_id}")
        print(f"{'='*70}")
        print(f" IP Address: {self.ip_address}")
        print(f" MAC Address: {self.mac_address}")
        print(f" Storage: {self.storage_capacity/(1024**3):.0f}GB")
        print(f" Bandwidth: {self.bandwidth}Mbps")
        print(f" Storage Path: {self.file_manager.storage_path}")
        print(f"{'='*70}")
        print("  addfile <filename> <filepath>        - Add file to local storage")
        print("  localfiles                          - List local files")
        print("  upload <file_id>                    - Upload local file to cloud")
        print("  download <file_id>                  - Download file from cloud")
        print("  cloudfiles                          - List available cloud files")
        print("\nFile transfer COMMANDS:")
        print("  send <target> <filename> <size_mb>  - Send file to specific node")
        print("  status                              - Show node status")
        print("  transfers                           - List active transfers")
        print("  storage                             - Show storage info")
        print("  quit                                - Disconnect and exit")
        print(f"{'='*70}")
        
        # Start status monitor thread
        status_thread = threading.Thread(target=self._status_monitor, daemon=True)
        status_thread.start()
        
        # Command loop
        self._command_loop()
        
    def _command_loop(self):
        """Enhanced command loop with file management"""
        while self.running:
            try:
                cmd_input = input(f"\n{self.node_id}> ").strip()
                if not cmd_input:
                    continue
                    
                parts = cmd_input.split()
                cmd = parts[0].lower()
                
                if cmd == 'quit':
                    break
                    
                # Original commands
                elif cmd == 'send' and len(parts) >= 4:
                    target_node = parts[1]
                    filename = parts[2]
                    try:
                        size_mb = float(parts[3])
                        self._initiate_file_transfer(target_node, filename, size_mb)
                    except ValueError:
                        print(" Invalid file size. Use number in MB.")
                        
                elif cmd == 'status':
                    self._show_node_status()
                elif cmd == 'transfers':
                    self._show_active_transfers()
                elif cmd == 'storage':
                    self._show_storage_info()
                    
                # NEW: Enhanced file management commands
                elif cmd == 'addfile' and len(parts) >= 3:
                    filename = parts[1]
                    file_path = ' '.join(parts[2:])  # Support paths with spaces
                    self._add_local_file(filename, file_path)
                elif cmd == 'localfiles':
                    self._show_local_files()
                elif cmd == 'upload' and len(parts) >= 2:
                    file_id = parts[1]
                    self._upload_file_to_cloud(file_id)
                elif cmd == 'download' and len(parts) >= 2:
                    file_id = parts[1]
                    self._download_file_from_cloud(file_id)
                elif cmd == 'cloudfiles':
                    self._show_cloud_files()
                else:
                    print(" Unknown command or invalid syntax")
                    print("Type one of the available commands shown at startup")
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f" Command error: {e}")
    
    def _add_local_file(self, filename: str, file_path: str):
        """Add a file to local storage"""
        try:
            result = self.file_manager.add_local_file(filename, file_path)
            self.used_storage += result['file_size']
            
            print(f" File added to local storage:")
            print(f"   Name: {result['filename']}")
            print(f"   ID: {result['file_id']}")
            print(f"   Size: {result['file_size']/(1024*1024):.2f}MB")
            print(f"   Checksum: {result['checksum'][:16]}...")
            print(f"   Stored at: {result['local_path']}")
            
        except Exception as e:
            print(f" Failed to add file: {e}")
    
    def _show_local_files(self):
        """Show all local files"""
        try:
            files = self.file_manager.get_local_files()
            
            if not files:
                print("\n No local files")
                return
            
            print(f"\n LOCAL FILES ({len(files)})")
            print("="*80)
            
            for file_info in files:
                size_mb = file_info['file_size'] / (1024 * 1024)
                print(f" {file_info['filename']}")
                print(f"   ID: {file_info['file_id']}")
                print(f"   Size: {size_mb:.2f}MB")
                print(f"   Original: {file_info['original_path']}")
                print(f"   Checksum: {file_info['checksum'][:16]}...")
                print(f"   Added: {file_info['created_at']}")
                print()
                
        except Exception as e:
            print(f" Failed to list local files: {e}")
    
    def _upload_file_to_cloud(self, file_id: str):
        """Upload a local file to the cloud"""
        try:
            if not self.connected_to_network:
                print(" Not connected to network")
                return
            
            # Get local file info
            local_file = self.file_manager.get_local_file_by_id(file_id)
            if not local_file:
                print(f" Local file {file_id} not found")
                return
            
            # Read file data
            with open(local_file['file_path'], 'rb') as f:
                file_data = f.read()
            
            print(f" Uploading {local_file['filename']} to cloud...")
            print(f"   Size: {len(file_data)/(1024*1024):.2f}MB")
            print(f"   Checksum: {local_file['checksum'][:16]}...")
            
            # Send upload request to network manager
            upload_msg = {
                'type': 'upload_file',
                'node_id': self.node_id,
                'file_id': file_id,
                'filename': local_file['filename'],
                'file_size': len(file_data),
                'checksum': local_file['checksum']
            }
            
            self.network_socket.send(json.dumps(upload_msg).encode())
            
            # Wait for ready acknowledgment
            self.network_socket.settimeout(30.0)
            response_data = self.network_socket.recv(1024)
            response = json.loads(response_data.decode())
            
            if response.get('status') == 'ready':
                print(" Network ready, sending file data...")
                
                # Send file data in chunks
                total_sent = 0
                chunk_size = 8192
                
                while total_sent < len(file_data):
                    chunk_end = min(total_sent + chunk_size, len(file_data))
                    chunk = file_data[total_sent:chunk_end]
                    self.network_socket.send(chunk)
                    total_sent += len(chunk)
                    
                    # Show progress for large files
                    if len(file_data) > 5*1024*1024:
                        progress = (total_sent / len(file_data)) * 100
                        if progress % 10 < (progress - len(chunk)/len(file_data) * 100) % 10:
                            print(f"    Upload progress: {progress:.1f}%")
                
                # Wait for final response
                final_response_data = self.network_socket.recv(4096)
                final_response = json.loads(final_response_data.decode())
                
                if final_response.get('success'):
                    replica_nodes = final_response.get('replica_nodes', [])
                    print(f" Upload successful!")
                    print(f"   File replicated to {len(replica_nodes)} nodes")
                    print(f"   Replica nodes: {replica_nodes}")
                    self.files_uploaded += 1
                else:
                    print(f" Upload failed: {final_response.get('message')}")
            else:
                print(f" Upload failed: {response.get('message', 'Network not ready')}")
                
        except Exception as e:
            print(f" Upload error: {e}")
        finally:
            # Reset socket timeout
            try:
                self.network_socket.settimeout(None)
            except:
                pass
    
    def _download_file_from_cloud(self, file_id: str):
        """Download a file from the cloud"""
        try:
            if not self.connected_to_network:
                print(" Not connected to network")
                return
            
            print(f" Requesting download of file {file_id[:8]}...")
            
            # Send download request
            download_msg = {
                'type': 'download_file',
                'node_id': self.node_id,
                'file_id': file_id
            }
            
            self.network_socket.send(json.dumps(download_msg).encode())
            
            # Receive response
            self.network_socket.settimeout(5.0)  # Longer timeout for downloads
            response_data = self.network_socket.recv(4096)
            response = json.loads(response_data.decode())
            
            if response.get('status') == 'success':
                filename = response.get('filename')
                file_size = response.get('file_size')
                source_node = response.get('source_node')
                
                print(f" Receiving {filename} ({file_size/(1024*1024):.2f}MB) from {source_node}...")
                
                # Receive file data
                file_data = b''
                while len(file_data) < file_size:
                    remaining = file_size - len(file_data)
                    chunk = self.network_socket.recv(min(8192, remaining))
                    if not chunk:
                        break
                    file_data += chunk
                    
                    # Show progress for large files
                    if file_size > 5*1024*1024:
                        progress = (len(file_data) / file_size) * 100
                        if progress % 10 < (progress - len(chunk)/file_size * 100) % 10:
                            print(f"    Download progress: {progress:.1f}%")
                
                # Save downloaded file
                saved_path = self.file_manager.save_downloaded_file(
                    file_id, filename, file_data, source_node
                )
                
                print(f" Download complete!")
                print(f"   Filename: {filename}")
                print(f"   Size: {len(file_data)/(1024*1024):.2f}MB")
                print(f"   Saved to: {saved_path}")
                print(f"   Source node: {source_node}")
                
                self.files_downloaded += 1
                self.used_storage += len(file_data)
                
            else:
                print(f" Download failed: {response.get('message')}")
                
        except Exception as e:
            print(f" Download error: {e}")
        finally:
            # Reset socket timeout
            try:
                self.network_socket.settimeout(None)
            except:
                pass
    
    def _show_cloud_files(self):
        """Show all available cloud files"""
        try:
            if not self.connected_to_network:
                print(" Not connected to network")
                return
            
            # Request cloud files list
            list_msg = {
                'type': 'list_cloud_files',
                'node_id': self.node_id
            }
            
            self.network_socket.send(json.dumps(list_msg).encode())
            
            # Receive response
            self.network_socket.settimeout(10.0)
            response_data = self.network_socket.recv(16384)  # Larger buffer for file lists
            response = json.loads(response_data.decode())
            
            if response.get('status') == 'success':
                files = response.get('files', [])
                
                if not files:
                    print("\n No files in cloud")
                    return
                
                print(f"\n CLOUD FILES ({len(files)})")
                print("="*90)
                
                for file_info in files:
                    size_mb = file_info['file_size'] / (1024 * 1024)
                    replica_count = len(file_info.get('replica_nodes', []))
                    downloads = file_info.get('download_count', 0)
                    
                    print(f" {file_info['filename']}")
                    print(f"   ID: {file_info['file_id']}")
                    print(f"   Size: {size_mb:.2f}MB")
                    #print(f"   Uploaded by: {file_info['uploader_node_id']}")
                    print(f"   Replicas: {replica_count} nodes")
                    print(f"   Downloads: {downloads}")
                    print(f"   Uploaded: {file_info['upload_timestamp']}")
                    print(f"   Checksum: {file_info['checksum'][:16]}...")
                    print()
                
            else:
                print(f" Failed to get cloud files: {response.get('message')}")
                
        except Exception as e:
            print(f" Error getting cloud files: {e}")
        finally:
            # Reset socket timeout
            try:
                self.network_socket.settimeout(None)
            except:
                pass
    
    def _initiate_file_transfer(self, target_node_id: str, filename: str, size_mb: float):
        """Initiate a file transfer to another node (original functionality)"""
        if not self.connected_to_network:
            print(" Not connected to network")
            return
            
        file_size = int(size_mb * 1024 * 1024)  # Convert to bytes
        transfer_id = str(uuid.uuid4())
        
        # Calculate chunks needed
        if file_size < 10 * 1024 * 1024:
            chunk_size = 512 * 1024
        elif file_size < 100 * 1024 * 1024:
            chunk_size = 2 * 1024 * 1024
        else:
            chunk_size = 10 * 1024 * 1024
        
        chunks_total = math.ceil(file_size / chunk_size)
        
        # Create local transfer record
        transfer = LocalTransfer(
            transfer_id=transfer_id,
            file_name=filename,
            file_size=file_size,
            target_node_id=target_node_id,
            chunks_total=chunks_total
        )
        
        with self.transfer_lock:
            self.outgoing_transfers[transfer_id] = transfer
            
        # Send initiation request to network
        try:
            init_msg = {
                'type': 'initiate_transfer',
                'source_node_id': self.node_id,
                'target_node_id': target_node_id,
                'file_name': filename,
                'file_size': file_size,
                'chunks_total': chunks_total
            }
            
            self.network_socket.send(json.dumps(init_msg).encode())
            response_data = self.network_socket.recv(1024)
            response = json.loads(response_data.decode())
            
            if response.get('status') == 'success':
                network_transfer_id = response.get('transfer_id')
                print(f" Transfer initiated: {filename} ({size_mb:.1f}MB)")
                print(f"   Target: {target_node_id}")
                print(f"   Transfer ID: {transfer_id[:8]}...")
                print(f"   Network ID: {network_transfer_id[:8]}...")
                
                # Start transfer thread
                transfer_thread = threading.Thread(
                    target=self._process_outgoing_transfer,
                    args=(transfer_id, network_transfer_id),
                    daemon=True
                )
                self.transfer_threads[transfer_id] = transfer_thread
                transfer_thread.start()
                
            else:
                print(f" Transfer failed: {response.get('message')}")
                with self.transfer_lock:
                    del self.outgoing_transfers[transfer_id]
                    
        except Exception as e:
            print(f" Failed to initiate transfer: {e}")
            with self.transfer_lock:
                if transfer_id in self.outgoing_transfers:
                    del self.outgoing_transfers[transfer_id]
    
    def _process_outgoing_transfer(self, transfer_id: str, network_transfer_id: str):
        """Process an outgoing file transfer using TCP/IP simulation"""
        with self.transfer_lock:
            transfer = self.outgoing_transfers.get(transfer_id)
            if not transfer:
                return
                
            transfer.status = "IN_PROGRESS"
            
        print(f" Starting transfer thread for {transfer.file_name}")
        
        try:
            # Simulate TCP/IP encapsulation and transmission
            for chunk_num in range(transfer.chunks_total):
                if not self.running or not self.connected_to_network:
                    break
                    
                print(f"\n PROCESSING CHUNK {chunk_num + 1}/{transfer.chunks_total}")
                print(f"   File: {transfer.file_name}")
                print(f"   Thread: {threading.current_thread().name}")
                
                # Simulate encapsulation (simplified for demo)
                chunk_size = min(2 * 1024 * 1024, transfer.file_size - (chunk_num * 2 * 1024 * 1024))
                fake_checksum = hashlib.md5(f"{transfer.file_name}-{chunk_num}".encode()).hexdigest()
                
                print(f" ENCAPSULATION at {self.node_id}")
                print(f"    Data Link Layer: Ethernet frame created")
                print(f"    Network Layer: IP packet ({self.ip_address} → {transfer.target_node_id})")
                print(f"    Transport Layer: TCP segment (seq: {1000 + chunk_num * chunk_size})")
                print(f"    Application Layer: FTP data chunk {chunk_num}")
                
                # Simulate network transmission time
                transmission_time = (chunk_size / (1024 * 1024)) / (self.bandwidth)
                network_delay = max(0.05, min(transmission_time, 0.3))
                
                print(f"    Network transmission: {network_delay:.3f}s")
                time.sleep(network_delay)
                
                print(f" DECAPSULATION at {transfer.target_node_id}")
                print(f"    TCP sequence validation")
                print(f"    IP routing validation") 
                print(f"    Ethernet frame check")
                print(f"    Data integrity verified")
                
                # Update progress
                with self.transfer_lock:
                    if transfer_id in self.outgoing_transfers:
                        self.outgoing_transfers[transfer_id].chunks_sent = chunk_num + 1
                        
                print(f"    Chunk {chunk_num + 1} successfully transferred")
                
            # Mark transfer as completed
            with self.transfer_lock:
                if transfer_id in self.outgoing_transfers:
                    self.outgoing_transfers[transfer_id].status = "COMPLETED"
                    self.total_transfers_sent += 1
                    self.total_data_transferred += transfer.file_size
                    
            print(f" Transfer completed: {transfer.file_name}")
            print(f"   Total chunks sent: {transfer.chunks_total}")
            print(f"   Protocol stack: Application/TCP/IP/Ethernet")
            print(f"   Time taken: {transmission_time:.2f} seconds")
        
        except Exception as e:
            print(f" Transfer failed {transfer_id[:8]}...: {e}")
            with self.transfer_lock:
                if transfer_id in self.outgoing_transfers:
                    self.outgoing_transfers[transfer_id].status = "FAILED"
                    
        finally:
            # Clean up thread reference
            if transfer_id in self.transfer_threads:
                del self.transfer_threads[transfer_id]
    
    def _show_node_status(self):
        """Show current node status"""
        with self.transfer_lock:
            active_outgoing = len([t for t in self.outgoing_transfers.values() 
                                 if t.status == "IN_PROGRESS"])
            completed_outgoing = len([t for t in self.outgoing_transfers.values() 
                                    if t.status == "COMPLETED"])
            
            active_incoming = len([t for t in self.incoming_transfers.values() 
                                 if t.status == "IN_PROGRESS"])
            completed_incoming = len([t for t in self.incoming_transfers.values() 
                                    if t.status == "COMPLETED"])
            
        storage_used_mb = self.used_storage / (1024 * 1024)
        storage_total_mb = self.storage_capacity / (1024 * 1024)
        storage_percent = (self.used_storage / self.storage_capacity) * 100
        
        # Get file storage stats
        storage_stats = self.file_manager.get_storage_stats()
        
        print(f"\n NODE STATUS - {self.node_id}")
        print(f"{'='*50}")
        print(f" Network: {'Connected' if self.connected_to_network else 'Disconnected'}")
        print(f" Storage: {storage_used_mb:.1f}MB / {storage_total_mb:.0f}MB ({storage_percent:.1f}%)")
        print(f" Outgoing transfers: {active_outgoing} active, {completed_outgoing} completed")
        print(f" Incoming transfers: {active_incoming} active, {completed_incoming} completed")
        print(f" Active threads: {len(self.transfer_threads)}")
        print(f" Total data transferred: {self.total_data_transferred/(1024*1024):.1f}MB")
        print(f" Files sent: {self.total_transfers_sent}, Files received: {self.total_transfers_received}")
        print("\n FILE STORAGE BREAKDOWN:")
        print(f"   Local files: {storage_stats['local']['count']} ({storage_stats['local']['size_mb']:.1f}MB)")
        print(f"   Replicas: {storage_stats['replicas']['count']} ({storage_stats['replicas']['size_mb']:.1f}MB)")
        print(f"   Downloads: {storage_stats['downloads']['count']} ({storage_stats['downloads']['size_mb']:.1f}MB)")
        print("\n CLOUD OPERATIONS:")
        print(f"   Files uploaded: {self.files_uploaded}")
        print(f"   Files downloaded: {self.files_downloaded}")
        print(f"{'='*50}")
        
    def _show_active_transfers(self):
        """Show all active transfers"""
        with self.transfer_lock:
            outgoing = [t for t in self.outgoing_transfers.values()]
            incoming = [t for t in self.incoming_transfers.values()]
            
        if not outgoing and not incoming:
            print("\n No active transfers")
            return
            
        print(f"\n ACTIVE TRANSFERS")
        print(f"{'='*60}")
        
        if outgoing:
            print(" OUTGOING:")
            for transfer in outgoing:
                progress = (transfer.chunks_sent / transfer.chunks_total) * 100
                size_mb = transfer.file_size / (1024 * 1024)
                status_icon = {"PENDING": "", "IN_PROGRESS": "", "COMPLETED": "", "FAILED": ""}
                
                print(f"   {status_icon.get(transfer.status, '❓')} {transfer.file_name}")
                print(f"      → {transfer.target_node_id} | {size_mb:.1f}MB | {progress:.1f}%")
                print(f"      ID: {transfer.transfer_id[:8]}... | Status: {transfer.status}")
                
        if incoming:
            print("\n INCOMING:")
            for transfer in incoming:
                progress = transfer.get_progress_percent()
                size_mb = transfer.file_size / (1024 * 1024)
                status_icon = {"PENDING": "", "IN_PROGRESS": "", "COMPLETED": "", "FAILED": ""}
                
                print(f"   {status_icon.get(transfer.status, '❓')} {transfer.file_name}")
                print(f"      ← {transfer.source_node_id} | {size_mb:.1f}MB | {progress:.1f}%")
                print(f"      ID: {transfer.transfer_id[:8]}... | Status: {transfer.status}")
                
        print(f"{'='*60}")
        
    def _show_storage_info(self):
        """Show detailed storage information"""
        used_mb = self.used_storage / (1024 * 1024)
        total_mb = self.storage_capacity / (1024 * 1024)
        available_mb = total_mb - used_mb
        usage_percent = (self.used_storage / self.storage_capacity) * 100
        
        storage_stats = self.file_manager.get_storage_stats()
        
        print(f"\n STORAGE INFORMATION")
        print(f"{'='*50}")
        print(f" Capacity: {total_mb:.0f}MB")
        print(f" Used: {used_mb:.1f}MB ({usage_percent:.1f}%)")
        print(f" Available: {available_mb:.1f}MB")
        print("\n STORAGE BREAKDOWN:")
        print(f"   Local files: {storage_stats['local']['count']} files ({storage_stats['local']['size_mb']:.1f}MB)")
        print(f"   Replicas: {storage_stats['replicas']['count']} files ({storage_stats['replicas']['size_mb']:.1f}MB)")
        print(f"   Downloads: {storage_stats['downloads']['count']} files ({storage_stats['downloads']['size_mb']:.1f}MB)")
        print("\n STORAGE PATHS:")
        print(f"   Base: {self.file_manager.storage_path}")
        print(f"   Local files: {self.file_manager.local_files_path}")
        print(f"   Replicas: {self.file_manager.replica_files_path}")
        print(f"   Downloads: {self.file_manager.downloads_path}")
        print(f"{'='*50}")
                
    def _status_monitor(self):
        """Monitor node status and show periodic updates"""
        last_update = time.time()
        
        while self.running:
            time.sleep(5)
            
            with self.transfer_lock:
                active_outgoing = len([t for t in self.outgoing_transfers.values() 
                                     if t.status == "IN_PROGRESS"])
                active_incoming = len([t for t in self.incoming_transfers.values() 
                                     if t.status == "IN_PROGRESS"])
                                     
            # Show update only if there are active transfers
            if active_outgoing > 0 or active_incoming > 0:
                current_time = time.time()
                if current_time - last_update >= 15:  # Every 15 seconds
                    print(f"\n [{datetime.now().strftime('%H:%M:%S')}] "
                          f"Node {self.node_id}: {active_outgoing} out, {active_incoming} in | "
                          f"Storage: {(self.used_storage/(1024*1024)):.1f}MB")
                    last_update = current_time
                    
    def disconnect_from_network(self):
        """Disconnect from the network"""
        print(f"🔌 Disconnecting node {self.node_id} from network...")
        
        if self.connected_to_network and self.network_socket:
            try:
                disconnect_msg = {
                    'type': 'disconnect_node',
                    'node_id': self.node_id
                }
                self.network_socket.send(json.dumps(disconnect_msg).encode())
                print(f" Sent disconnect notification to network")
            except Exception as e:
                print(f" Failed to send disconnect message: {e}")
        
        self.running = False
        self.connected_to_network = False
        
        # Wait for active transfers to complete
        print(f" Waiting for {len(self.transfer_threads)} active transfer(s) to complete...")
        for transfer_id, thread in list(self.transfer_threads.items()):
            thread.join(timeout=5)
            
        if self.network_socket:
            try:
                self.network_socket.close()
            except:
                pass
                
        print(f" Node {self.node_id} disconnected from network")

def main():
    """Main function to run an enhanced storage node"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python enhanced_threaded_node.py <node_id> [storage_gb] [bandwidth_mbps] [network_host] [network_port]")
        print("Example: python enhanced_threaded_node.py node1 100 1000 localhost 8888")
        return
        
    node_id = sys.argv[1]
    storage_gb = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    bandwidth_mbps = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
    network_host = sys.argv[4] if len(sys.argv) > 4 else "localhost"
    network_port = int(sys.argv[5]) if len(sys.argv) > 5 else 8888
    
    # Generate network addresses
    import random
    ip_address = f"192.168.1.{random.randint(10, 254)}"
    mac_parts = [0x02, 0x00, 0x00] + [random.randint(0x00, 0xff) for _ in range(3)]
    mac_address = ':'.join(f'{b:02x}' for b in mac_parts)
    
    # Create and start enhanced node
    node = ThreadedStorageNode(
        node_id=node_id,
        ip_address=ip_address,
        mac_address=mac_address,
        storage_capacity_gb=storage_gb,
        bandwidth_mbps=bandwidth_mbps,
        network_host=network_host,
        network_port=network_port
    )
    
    try:
        node.start_node()
    except KeyboardInterrupt:
        pass
    finally:
        node.disconnect_from_network()

if __name__ == "__main__":
    main()
    