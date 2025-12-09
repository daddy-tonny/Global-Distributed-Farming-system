# enhanced_threaded_network.py
"""
Enhanced version of threaded_network.py with file upload/download capabilities
and distributed storage with replication.
"""

import time
import uuid
import queue
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum, auto
import hashlib
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TransferStatus(Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS" 
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

@dataclass
class NetworkTransfer:
    """Represents a network file transfer"""
    transfer_id: str
    source_node_id: str
    target_node_id: str
    file_name: str
    file_size: int
    chunks_total: int
    chunks_completed: int = 0
    status: TransferStatus = TransferStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: str = ""
    
    def get_progress_percent(self) -> float:
        if self.chunks_total == 0:
            return 0.0
        return (self.chunks_completed / self.chunks_total) * 100.0

class DistributedFileService:
    """Enhanced file service with replication and fault tolerance"""
    
    def __init__(self, network_manager):
        self.network_manager = network_manager
        self.replication_factor = 3  # Default replication factor
        self.cloud_files = {}  # Registry of all cloud files
        self.file_lock = threading.RLock()
        
    def upload_file_to_cloud(self, uploader_node_id: str, file_id: str, filename: str, 
                           file_data: bytes, checksum: str) -> dict:
        """Upload a file to the distributed cloud with replication"""
        try:
            logger.info(f"Processing upload: {filename} from {uploader_node_id}")
            
            # Get available nodes for replication (excluding the uploader)
            available_nodes = self._get_available_nodes(exclude_node=uploader_node_id)
            
            if len(available_nodes) < 1:
                return {
                    'success': False,
                    'message': 'No available nodes for replication'
                }
            
            # Select nodes for replication
            replica_count = min(self.replication_factor, len(available_nodes))
            replica_nodes = available_nodes[:replica_count]  # Take first N available nodes
            
            # Replicate file to selected nodes
            successful_replicas = []
            for replica_node_id in replica_nodes:
                if self._replicate_to_node(replica_node_id, file_id, filename, 
                                         file_data, checksum, uploader_node_id):
                    successful_replicas.append(replica_node_id)
            
            if successful_replicas:
                # Store cloud file metadata
                with self.file_lock:
                    self.cloud_files[file_id] = {
                        'file_id': file_id,
                        'filename': filename,
                        'file_size': len(file_data),
                        'checksum': checksum,
                        'uploader_node_id': uploader_node_id,
                        'replica_nodes': successful_replicas,
                        'upload_timestamp': datetime.now().isoformat(),
                        'download_count': 0
                    }
                
                logger.info(f"File {filename} uploaded with {len(successful_replicas)} replicas")
                return {
                    'success': True,
                    'file_id': file_id,
                    'replica_nodes': successful_replicas,
                    'message': f'File uploaded successfully with {len(successful_replicas)} replicas'
                }
            else:
                return {
                    'success': False,
                    'message': 'Failed to create any replicas'
                }
                
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return {
                'success': False,
                'message': f'Upload failed: {str(e)}'
            }
    
    def download_file_from_cloud(self, file_id: str, requester_node_id: str) -> dict:
        """Download a file from the distributed cloud"""
        try:
            # Get file metadata
            with self.file_lock:
                file_metadata = self.cloud_files.get(file_id)
            
            if not file_metadata:
                return {
                    'success': False,
                    'message': 'File not found in cloud'
                }
            
            # Find nodes that have the file
            available_nodes = []
            for replica_node_id in file_metadata['replica_nodes']:
                if self._is_node_available(replica_node_id):
                    available_nodes.append(replica_node_id)
            
            if not available_nodes:
                return {
                    'success': False,
                    'message': 'No available nodes have this file'
                }
            
            # Select the best node (for now, just pick the first available)
            selected_node = available_nodes[0]
            logger.info(f"Downloading {file_metadata['filename']} from {selected_node}")
            
            # Request file from selected node
            file_data = self._request_file_from_node(selected_node, file_id)
            
            if file_data:
                # Increment download count
                with self.file_lock:
                    self.cloud_files[file_id]['download_count'] += 1
                
                return {
                    'success': True,
                    'filename': file_metadata['filename'],
                    'file_data': file_data,
                    'source_node': selected_node,
                    'message': f'File downloaded from node {selected_node}'
                }
            else:
                return {
                    'success': False,
                    'message': 'Failed to retrieve file from storage nodes'
                }
                
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return {
                'success': False,
                'message': f'Download failed: {str(e)}'
            }
    
    def _get_available_nodes(self, exclude_node: str = None) -> List[str]:
        """Get list of available nodes for replication"""
        with self.network_manager.node_connection_lock:
            nodes = list(self.network_manager.connected_nodes.keys())
            if exclude_node and exclude_node in nodes:
                nodes.remove(exclude_node)
            return [node for node in nodes if self._is_node_available(node)]
    
    def _is_node_available(self, node_id: str) -> bool:
        """Check if a node is currently available"""
        with self.network_manager.node_connection_lock:
            return (node_id in self.network_manager.connected_nodes and 
                   self.network_manager.connected_nodes[node_id].get('status') == 'connected')
    
    def _replicate_to_node(self, node_id: str, file_id: str, filename: str, 
                          file_data: bytes, checksum: str, source_node_id: str) -> bool:
        """Replicate a file to a specific node"""
        try:
            with self.network_manager.node_connection_lock:
                if node_id not in self.network_manager.node_connections:
                    return False
                
                node_socket = self.network_manager.node_connections[node_id]
                
                # Send replication request
                replication_msg = {
                    'type': 'replicate_file',
                    'file_id': file_id,
                    'filename': filename,
                    'file_size': len(file_data),
                    'checksum': checksum,
                    'source_node_id': source_node_id
                }
                
                node_socket.send(json.dumps(replication_msg).encode())
                
                # Wait for acknowledgment
                node_socket.settimeout(10.0)
                response_data = node_socket.recv(1024)
                response = json.loads(response_data.decode())
                
                if response.get('status') == 'ready':
                    # Send file data in chunks
                    chunk_size = 8192  # 8KB chunks
                    total_sent = 0
                    
                    while total_sent < len(file_data):
                        chunk_end = min(total_sent + chunk_size, len(file_data))
                        chunk = file_data[total_sent:chunk_end]
                        node_socket.send(chunk)
                        total_sent += len(chunk)
                    
                    # Wait for final confirmation
                    final_response = node_socket.recv(1024)
                    final_result = json.loads(final_response.decode())
                    
                    success = final_result.get('status') == 'success'
                    if success:
                        logger.info(f"Successfully replicated {filename} to {node_id}")
                    else:
                        logger.error(f"Replication to {node_id} failed: {final_result.get('message')}")
                    
                    return success
                
                return False
                
        except Exception as e:
            logger.error(f"Replication to {node_id} failed: {e}")
            return False
        finally:
            # Reset socket timeout
            with self.network_manager.node_connection_lock:
                if node_id in self.network_manager.node_connections:
                    try:
                        self.network_manager.node_connections[node_id].settimeout(None)
                    except:
                        pass
    
    def _request_file_from_node(self, node_id: str, file_id: str) -> Optional[bytes]:
        """Request a file from a specific node"""
        try:
            with self.network_manager.node_connection_lock:
                if node_id not in self.network_manager.node_connections:
                    return None
                
                node_socket = self.network_manager.node_connections[node_id]
                
                # Send download request
                download_msg = {
                    'type': 'download_file_request',
                    'file_id': file_id
                }
                
                node_socket.send(json.dumps(download_msg).encode())
                
                # Receive response
                node_socket.settimeout(30.0)
                response_data = node_socket.recv(1024)
                response = json.loads(response_data.decode())
                
                if response.get('status') == 'success':
                    file_size = response.get('file_size', 0)
                    
                    # Receive file data
                    file_data = b''
                    while len(file_data) < file_size:
                        remaining = file_size - len(file_data)
                        chunk = node_socket.recv(min(8192, remaining))
                        if not chunk:
                            break
                        file_data += chunk
                    
                    logger.info(f"Retrieved {len(file_data)} bytes from {node_id}")
                    return file_data
                
                logger.error(f"File request failed: {response.get('message')}")
                return None
                
        except Exception as e:
            logger.error(f"File request from {node_id} failed: {e}")
            return None
        finally:
            # Reset socket timeout
            with self.network_manager.node_connection_lock:
                if node_id in self.network_manager.node_connections:
                    try:
                        self.network_manager.node_connections[node_id].settimeout(None)
                    except:
                        pass
    
    def list_cloud_files(self) -> List[dict]:
        """List all files available in the cloud"""
        with self.file_lock:
            return list(self.cloud_files.values())

class ThreadedNetworkManager:
    """ network manager with file services"""
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.running = False
        self.server_socket = None
        
        # Thread management
        self.transfer_threads: Dict[str, threading.Thread] = {}
        self.active_transfers: Dict[str, NetworkTransfer] = {}
        self.transfer_lock = threading.RLock()
        
        # Node connections
        self.connected_nodes: Dict[str, dict] = {}
        self.node_connections: Dict[str, socket.socket] = {}
        self.node_connection_lock = threading.RLock()
        
        # Communication queues
        self.command_queue = queue.Queue()
        self.status_queue = queue.Queue()
        
        # Statistics
        self.total_transfers = 0
        self.successful_transfers = 0
        self.failed_transfers = 0
        
        # NEW: File service
        self.file_service = DistributedFileService(self)
        
    def start_network_server(self):
        """Start the network server to accept node connections"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(10)
            self.running = True
            
            print(f"  Network Manager started on {self.host}:{self.port}")
            print(" Waiting for nodes to connect...")
            print(" File upload/download services enabled")
            print("=" * 60)
            
            # Start status monitor thread
            status_thread = threading.Thread(target=self._status_monitor, daemon=True)
            status_thread.start()
            
            # Start connection health monitor
            health_thread = threading.Thread(target=self._connection_health_monitor, daemon=True)
            health_thread.start()
            
            # Accept connections
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    print(f" New connection from {address}")
                    
                    # Handle node connection in separate thread
                    conn_thread = threading.Thread(
                        target=self._handle_node_connection, 
                        args=(client_socket, address),
                        daemon=True
                    )
                    conn_thread.start()
                    
                except Exception as e:
                    if self.running:
                        print(f" Connection error: {e}")
                        
        except Exception as e:
            print(f" Failed to start network server: {e}")
    
    def _handle_node_connection(self, client_socket: socket.socket, address):
        """Handle communication with a connected node"""
        node_id = None
        try:
            while self.running:
                try:
                    client_socket.settimeout(30.0)
                    data = client_socket.recv(8192)  # Increased buffer for file operations
                    
                    if not data:
                        print(f"🔌 Node connection closed: {address}")
                        break
                        
                    message = json.loads(data.decode())
                    response = self._process_node_message(message, client_socket)
                    
                    # Track node_id for cleanup
                    if message.get('type') == 'register_node':
                        node_id = message.get('node_id')
                    
                    if response and response.get('send_response', True):
                        client_socket.send(json.dumps(response).encode())
                        
                except socket.timeout:
                    # Send heartbeat to check if connection is alive
                    try:
                        heartbeat = {'type': 'heartbeat'}
                        client_socket.send(json.dumps(heartbeat).encode())
                    except:
                        print(f" Heartbeat failed for {address}")
                        break
                        
                except json.JSONDecodeError:
                    print(f" Invalid JSON from {address}")
                    continue
                    
                except Exception as e:
                    print(f" Message processing error from {address}: {e}")
                    break
                    
        except Exception as e:
            print(f" Node connection error {address}: {e}")
        finally:
            if node_id:
                self._cleanup_disconnected_node(node_id)
            client_socket.close()
    
    def _process_node_message(self, message: dict, client_socket: socket.socket) -> dict:
        """Enhanced message processing with file operations"""
        msg_type = message.get('type')
        
        # Existing message types
        if msg_type == 'register_node':
            return self._register_node(message, client_socket)
        elif msg_type == 'initiate_transfer':
            return self._initiate_transfer(message)
        elif msg_type == 'chunk_processed':
            return self._update_chunk_progress(message)
        elif msg_type == 'get_transfer_status':
            return self._get_transfer_status(message)
        elif msg_type == 'disconnect_node':
            return self._handle_node_disconnect(message)
        elif msg_type == 'heartbeat_response':
            return {'status': 'success', 'message': 'Heartbeat acknowledged'}
        
        # NEW: File service message types
        elif msg_type == 'upload_file':
            return self._handle_file_upload(message, client_socket)
        elif msg_type == 'download_file':
            return self._handle_file_download(message, client_socket)
        elif msg_type == 'list_cloud_files':
            return self._handle_list_cloud_files(message)
        elif msg_type == 'get_cloud_stats':
            return self._handle_cloud_stats(message)
        else:
            return {'status': 'error', 'message': 'Unknown message type'}
    
    def _handle_file_upload(self, message: dict, client_socket: socket.socket) -> dict:
        """Handle file upload request"""
        try:
            node_id = message.get('node_id')
            file_id = message.get('file_id')
            filename = message.get('filename')
            file_size = message.get('file_size')
            checksum = message.get('checksum')
            
            print(f" Receiving file upload: {filename} ({file_size/(1024*1024):.2f}MB) from {node_id}")
            
            # Send ready acknowledgment
            ready_response = {'status': 'ready', 'message': 'Ready to receive file'}
            client_socket.send(json.dumps(ready_response).encode())
            
            # Receive file data
            file_data = b''
            while len(file_data) < file_size:
                remaining = file_size - len(file_data)
                chunk = client_socket.recv(min(8192, remaining))
                if not chunk:
                    break
                file_data += chunk
                
                # Show progress for large files
                if file_size > 10*1024*1024:  # Show progress for files > 10MB
                    progress = (len(file_data) / file_size) * 100
                    if progress % 10 < (progress - len(chunk)/file_size * 100) % 10:
                        print(f"    Upload progress: {progress:.1f}%")
            
            # Verify checksum
            calculated_checksum = hashlib.sha256(file_data).hexdigest()
            if calculated_checksum != checksum:
                return {
                    'success': False,
                    'message': 'Checksum verification failed',
                    'send_response': False
                }
            
            # Process upload with replication
            result = self.file_service.upload_file_to_cloud(
                node_id, file_id, filename, file_data, checksum
            )
            
            print(f"{'' if result['success'] else ''} Upload result: {result['message']}")
            
            # Don't send automatic response - we'll send it manually with additional data
            result['send_response'] = False
            return result
            
        except Exception as e:
            logger.error(f"File upload failed: {e}")
            return {
                'success': False, 
                'message': str(e),
                'send_response': False
            }
    
    def _handle_file_download(self, message: dict, client_socket: socket.socket) -> dict:
        """Handle file download request"""
        try:
            file_id = message.get('file_id')
            requester_node_id = message.get('node_id')
            
            print(f" Processing download request for file {file_id[:8]}... from {requester_node_id}")
            
            result = self.file_service.download_file_from_cloud(file_id, requester_node_id)
            
            if result['success']:
                # Send success response with file metadata
                response = {
                    'status': 'success',
                    'filename': result['filename'],
                    'file_size': len(result['file_data']),
                    'source_node': result['source_node']
                }
                client_socket.send(json.dumps(response).encode())
                
                # Send file data
                file_data = result['file_data']
                total_sent = 0
                
                while total_sent < len(file_data):
                    chunk_end = min(total_sent + 8192, len(file_data))
                    chunk = file_data[total_sent:chunk_end]
                    client_socket.send(chunk)
                    total_sent += len(chunk)
                    
                    # Show progress for large files
                    if len(file_data) > 10*1024*1024:
                        progress = (total_sent / len(file_data)) * 100
                        if progress % 10 < (progress - len(chunk)/len(file_data) * 100) % 10:
                            print(f"    Download progress: {progress:.1f}%")
                
                print(f" File {result['filename']} sent successfully to {requester_node_id}")
                return {'status': 'success', 'message': 'File sent successfully', 'send_response': False}
            else:
                error_response = {'status': 'error', 'message': result['message']}
                client_socket.send(json.dumps(error_response).encode())
                return {'status': 'error', 'message': result['message'], 'send_response': False}
                
        except Exception as e:
            logger.error(f"File download failed: {e}")
            error_response = {'status': 'error', 'message': str(e)}
            client_socket.send(json.dumps(error_response).encode())
            return {'status': 'error', 'message': str(e), 'send_response': False}
    
    def _handle_list_cloud_files(self, message: dict) -> dict:
        """Handle list cloud files request"""
        try:
            files = self.file_service.list_cloud_files()
            return {
                'status': 'success',
                'files': files,
                'count': len(files)
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def _handle_cloud_stats(self, message: dict) -> dict:
        """Handle cloud statistics request"""
        try:
            files = self.file_service.list_cloud_files()
            total_size = sum(f['file_size'] for f in files)
            total_downloads = sum(f.get('download_count', 0) for f in files)
            
            return {
                'status': 'success',
                'stats': {
                    'total_files': len(files),
                    'total_size_mb': total_size / (1024 * 1024),
                    'total_downloads': total_downloads,
                    'replication_factor': self.file_service.replication_factor,
                    'connected_nodes': len(self.connected_nodes)
                }
            }
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    # Keep all existing methods from the original threaded_network.py
    def _register_node(self, message: dict, client_socket: socket.socket) -> dict:
        """Register a new node with the network"""
        node_id = message.get('node_id')
        node_info = message.get('node_info', {})
        
        with self.node_connection_lock:
            self.connected_nodes[node_id] = {
                'node_info': node_info,
                'connected_at': datetime.now(),
                'active_transfers': [],
                'status': 'connected'
            }
            self.node_connections[node_id] = client_socket
        
        print(f" Node registered: {node_id} ({node_info.get('ip_address', 'unknown')})")
        return {'status': 'success', 'message': 'Node registered successfully'}
    
    def _cleanup_disconnected_node(self, node_id: str):
        """Clean up a disconnected node and cancel its transfers"""
        print(f"🧹 Cleaning up disconnected node: {node_id}")
        
        with self.node_connection_lock:
            if node_id in self.connected_nodes:
                del self.connected_nodes[node_id]
                print(f" Removed {node_id} from connected nodes list")
                
            if node_id in self.node_connections:
                del self.node_connections[node_id]
                
        # Cancel active transfers involving this node
        with self.transfer_lock:
            transfers_to_cancel = []
            for transfer_id, transfer in self.active_transfers.items():
                if (transfer.source_node_id == node_id or 
                    transfer.target_node_id == node_id) and transfer.status == TransferStatus.IN_PROGRESS:
                    transfers_to_cancel.append(transfer_id)
                    
            for transfer_id in transfers_to_cancel:
                self.active_transfers[transfer_id].status = TransferStatus.FAILED
                self.active_transfers[transfer_id].error_message = f"Node {node_id} disconnected"
                self.failed_transfers += 1
                print(f" Cancelled transfer {transfer_id[:8]}... due to node disconnection")
    
    def _connection_health_monitor(self):
        """Monitor connection health and detect disconnected nodes"""
        while self.running:
            time.sleep(10)
            
            with self.node_connection_lock:
                disconnected_nodes = []
                for node_id, socket_conn in list(self.node_connections.items()):
                    try:
                        heartbeat = {'type': 'heartbeat', 'timestamp': time.time()}
                        socket_conn.send(json.dumps(heartbeat).encode())
                    except Exception:
                        disconnected_nodes.append(node_id)
                        
                for node_id in disconnected_nodes:
                    print(f" Detected disconnected node: {node_id}")
                    self._cleanup_disconnected_node(node_id)
    
    def _initiate_transfer(self, message: dict) -> dict:
        """Initiate a new file transfer (existing functionality)"""
        source_node_id = message['source_node_id']
        target_node_id = message['target_node_id']
        
        if not self._is_node_connected(source_node_id):
            error_msg = f"Source node {source_node_id} is not connected"
            print(f" Transfer rejected: {error_msg}")
            return {'status': 'error', 'message': error_msg}
            
        if not self._is_node_connected(target_node_id):
            error_msg = f"Target node {target_node_id} is not connected"
            print(f" Transfer rejected: {error_msg}")
            return {'status': 'error', 'message': error_msg}
        
        transfer_id = str(uuid.uuid4())
        
        try:
            transfer = NetworkTransfer(
                transfer_id=transfer_id,
                source_node_id=source_node_id,
                target_node_id=target_node_id, 
                file_name=message['file_name'],
                file_size=message['file_size'],
                chunks_total=message.get('chunks_total', 1)
            )
            
            with self.transfer_lock:
                self.active_transfers[transfer_id] = transfer
                self.total_transfers += 1
                
            self._notify_target_node_incoming_transfer(target_node_id, transfer)
                
            transfer_thread = threading.Thread(
                target=self._process_transfer,
                args=(transfer_id,),
                daemon=True
            )
            self.transfer_threads[transfer_id] = transfer_thread
            transfer_thread.start()
            
            print(f" Transfer initiated: {transfer.file_name} ({transfer.file_size/(1024*1024):.1f}MB)")
            print(f"   {transfer.source_node_id} → {transfer.target_node_id} [ID: {transfer_id[:8]}...]")
            
            return {
                'status': 'success',
                'transfer_id': transfer_id,
                'message': 'Transfer initiated successfully'
            }
            
        except Exception as e:
            print(f" Failed to initiate transfer: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def _is_node_connected(self, node_id: str) -> bool:
        """Check if a node is currently connected"""
        with self.node_connection_lock:
            return node_id in self.connected_nodes and self.connected_nodes[node_id].get('status') == 'connected'
    
    def _notify_target_node_incoming_transfer(self, target_node_id: str, transfer: NetworkTransfer):
        """Notify target node about incoming transfer"""
        try:
            with self.node_connection_lock:
                if target_node_id in self.node_connections:
                    target_socket = self.node_connections[target_node_id]
                    
                    notification = {
                        'type': 'incoming_transfer',
                        'transfer_id': transfer.transfer_id,
                        'source_node_id': transfer.source_node_id,
                        'file_name': transfer.file_name,
                        'file_size': transfer.file_size,
                        'chunks_total': transfer.chunks_total
                    }
                    
                    target_socket.send(json.dumps(notification).encode())
                    print(f" Notified {target_node_id} about incoming transfer: {transfer.file_name}")
                    
        except Exception as e:
            print(f" Failed to notify target node {target_node_id}: {e}")
    
    def _process_transfer(self, transfer_id: str):
        """Process a file transfer in a separate thread (existing functionality)"""
        with self.transfer_lock:
            transfer = self.active_transfers.get(transfer_id)
            if not transfer:
                return
                
            transfer.status = TransferStatus.IN_PROGRESS
            transfer.started_at = datetime.now()
            
        print(f" Processing transfer {transfer_id[:8]}... in thread {threading.current_thread().name}")
        
        try:
            if not (self._is_node_connected(transfer.source_node_id) and 
                   self._is_node_connected(transfer.target_node_id)):
                raise Exception("One or both nodes disconnected during transfer")
            
            if transfer.file_size < 10 * 1024 * 1024:
                chunk_size = 512 * 1024
            elif transfer.file_size < 100 * 1024 * 1024:
                chunk_size = 2 * 1024 * 1024
            else:
                chunk_size = 10 * 1024 * 1024
            chunks_needed = (transfer.file_size + chunk_size - 1) // chunk_size
            
            for chunk_num in range(chunks_needed):
                if not self.running:
                    break
                    
                if not (self._is_node_connected(transfer.source_node_id) and 
                       self._is_node_connected(transfer.target_node_id)):
                    raise Exception("Node disconnected during transfer")
                    
                time.sleep(0.1 + (chunk_size / (100 * 1024 * 1024)))
                
                with self.transfer_lock:
                    if transfer_id in self.active_transfers:
                        self.active_transfers[transfer_id].chunks_completed = chunk_num + 1
                        
                self._update_target_node_progress(transfer.target_node_id, transfer_id, chunk_num + 1)
                        
                print(f"    Chunk {chunk_num + 1}/{chunks_needed} transferred [ID: {transfer_id[:8]}...]")
                
            with self.transfer_lock:
                if transfer_id in self.active_transfers:
                    self.active_transfers[transfer_id].status = TransferStatus.COMPLETED
                    self.active_transfers[transfer_id].completed_at = datetime.now()
                    self.successful_transfers += 1
                    
            self._notify_transfer_completion(transfer.target_node_id, transfer_id)
            print(f" Transfer completed: {transfer_id[:8]}...")
            
        except Exception as e:
            print(f" Transfer failed {transfer_id[:8]}...: {e}")
            with self.transfer_lock:
                if transfer_id in self.active_transfers:
                    self.active_transfers[transfer_id].status = TransferStatus.FAILED
                    self.active_transfers[transfer_id].error_message = str(e)
                    self.failed_transfers += 1
                    
            self._notify_transfer_failure(transfer.target_node_id, transfer_id, str(e))
                    
        finally:
            if transfer_id in self.transfer_threads:
                del self.transfer_threads[transfer_id]
    
    def _update_target_node_progress(self, target_node_id: str, transfer_id: str, chunks_completed: int):
        """Update target node about transfer progress"""
        try:
            with self.node_connection_lock:
                if target_node_id in self.node_connections:
                    target_socket = self.node_connections[target_node_id]
                    
                    update = {
                        'type': 'transfer_progress',
                        'transfer_id': transfer_id,
                        'chunks_completed': chunks_completed
                    }
                    
                    target_socket.send(json.dumps(update).encode())
                    
        except Exception as e:
            print(f" Failed to update progress for {target_node_id}: {e}")
    
    def _notify_transfer_completion(self, target_node_id: str, transfer_id: str):
        """Notify target node about transfer completion"""
        try:
            with self.node_connection_lock:
                if target_node_id in self.node_connections:
                    target_socket = self.node_connections[target_node_id]
                    
                    completion_msg = {
                        'type': 'transfer_completed',
                        'transfer_id': transfer_id
                    }
                    
                    target_socket.send(json.dumps(completion_msg).encode())
                    print(f" Notified {target_node_id} about transfer completion")
                    
        except Exception as e:
            print(f" Failed to notify completion to {target_node_id}: {e}")
    
    def _notify_transfer_failure(self, target_node_id: str, transfer_id: str, error_msg: str):
        """Notify target node about transfer failure"""
        try:
            with self.node_connection_lock:
                if target_node_id in self.node_connections:
                    target_socket = self.node_connections[target_node_id]
                    
                    failure_msg = {
                        'type': 'transfer_failed',
                        'transfer_id': transfer_id,
                        'error_message': error_msg
                    }
                    
                    target_socket.send(json.dumps(failure_msg).encode())
                    print(f" Notified {target_node_id} about transfer failure")
                    
        except Exception as e:
            print(f" Failed to notify failure to {target_node_id}: {e}")
    
    def _update_chunk_progress(self, message: dict) -> dict:
        """Update progress for a chunk"""
        transfer_id = message.get('transfer_id')
        chunks_completed = message.get('chunks_completed', 0)
        
        with self.transfer_lock:
            if transfer_id in self.active_transfers:
                self.active_transfers[transfer_id].chunks_completed = chunks_completed
                return {'status': 'success'}
                
        return {'status': 'error', 'message': 'Transfer not found'}
    
    def _get_transfer_status(self, message: dict) -> dict:
        """Get status of all or specific transfers"""
        transfer_id = message.get('transfer_id')
        
        with self.transfer_lock:
            if transfer_id:
                transfer = self.active_transfers.get(transfer_id)
                if transfer:
                    return {
                        'status': 'success',
                        'transfer': {
                            'id': transfer.transfer_id,
                            'file_name': transfer.file_name,
                            'progress': transfer.get_progress_percent(),
                            'status': transfer.status.value,
                            'source': transfer.source_node_id,
                            'target': transfer.target_node_id
                        }
                    }
                else:
                    return {'status': 'error', 'message': 'Transfer not found'}
            else:
                transfers = []
                for t in self.active_transfers.values():
                    transfers.append({
                        'id': t.transfer_id[:8] + '...',
                        'file_name': t.file_name,
                        'progress': t.get_progress_percent(),
                        'status': t.status.value,
                        'source': t.source_node_id,
                        'target': t.target_node_id,
                        'file_size_mb': t.file_size / (1024 * 1024)
                    })
                return {'status': 'success', 'transfers': transfers}
    
    def _handle_node_disconnect(self, message: dict) -> dict:
        """Handle explicit node disconnection"""
        node_id = message.get('node_id')
        print(f" Node {node_id} requesting disconnection")
        
        self._cleanup_disconnected_node(node_id)
        return {'status': 'success', 'message': 'Node disconnected successfully'}
    
    def _status_monitor(self):
        """Monitor and display transfer status in real-time"""
        while self.running:
            time.sleep(3)
            
            with self.transfer_lock:
                active_count = len([t for t in self.active_transfers.values() 
                                  if t.status == TransferStatus.IN_PROGRESS])
                pending_count = len([t for t in self.active_transfers.values() 
                                   if t.status == TransferStatus.PENDING])
                
                # Get cloud file stats
                cloud_files_count = len(self.file_service.list_cloud_files())
                
                if active_count > 0 or pending_count > 0 or cloud_files_count > 0:
                    print(f"\n NETWORK STATUS UPDATE - {datetime.now().strftime('%H:%M:%S')}")
                    print(f"   Connected nodes: {len(self.connected_nodes)}")
                    print(f"   Active transfers: {active_count}")
                    print(f"   Pending transfers: {pending_count}")
                    print(f"   Cloud files: {cloud_files_count}")
                    print(f"   Total completed: {self.successful_transfers}")
                    print(f"   Total failed: {self.failed_transfers}")
                    
                    for transfer in self.active_transfers.values():
                        if transfer.status == TransferStatus.IN_PROGRESS:
                            progress = transfer.get_progress_percent()
                            print(f"    {transfer.file_name}: {progress:.1f}% "
                                  f"({transfer.source_node_id}→{transfer.target_node_id})")
                    
                    print("-" * 60)
    
    def get_network_statistics(self) -> dict:
        """Get comprehensive network statistics"""
        with self.transfer_lock:
            with self.node_connection_lock:
                cloud_files = self.file_service.list_cloud_files()
                total_cloud_size = sum(f['file_size'] for f in cloud_files)
                
                return {
                    'connected_nodes': len(self.connected_nodes),
                    'active_transfers': len([t for t in self.active_transfers.values() 
                                           if t.status == TransferStatus.IN_PROGRESS]),
                    'total_transfers': self.total_transfers,
                    'successful_transfers': self.successful_transfers,
                    'failed_transfers': self.failed_transfers,
                    'active_threads': len(self.transfer_threads),
                    'cloud_files': len(cloud_files),
                    'cloud_storage_mb': total_cloud_size / (1024 * 1024)
                }
    
    def stop_network(self):
        """Stop the network manager"""
        print("\n Shutting down enhanced network manager...")
        self.running = False
        
        if self.server_socket:
            self.server_socket.close()
            
        with self.node_connection_lock:
            for node_id, socket_conn in self.node_connections.items():
                try:
                    socket_conn.close()
                except:
                    pass
            
        for transfer_id, thread in self.transfer_threads.items():
            print(f"   Waiting for transfer {transfer_id[:8]}... to complete")
            thread.join(timeout=5)
            
        print(" Enhanced network manager stopped")

def main():
    """Main function to run the enhanced network manager"""
    import sys 
    
    if len(sys.argv) < 3:
        print("Usage: python enhanced_threaded_network.py <host> <port>")
        print("Example: python enhanced_threaded_network.py localhost 8888")
        return
       
    host = sys.argv[1] 
    port_number = int(sys.argv[2])
    network_manager = ThreadedNetworkManager(host=host, port=port_number)
    
    try:
        network_thread = threading.Thread(target=network_manager.start_network_server, daemon=True)
        network_thread.start()
        
        print("\n" + "="*70)
        print(" THREADED NETWORK MANAGER COMMAND LINE")
        print("="*70)
        print("Commands:")
        print("  status     - Show network status")
        print("  stats      - Show network statistics") 
        print("  nodes      - List connected nodes")
        print("  cloud      - Show cloud file statistics")
        print("  files      - List all cloud files")
        print("  quit       - Shutdown network")
        print("="*70)
        
        while True:
            try:
                cmd = input("\nNetwork> ").strip().lower()
                
                if cmd == 'quit':
                    break
                elif cmd == 'status':
                    stats = network_manager.get_network_statistics()
                    print(f"\n Network Status:")
                    print(f"   Connected nodes: {stats['connected_nodes']}")
                    print(f"   Active transfers: {stats['active_transfers']}")
                    print(f"   Cloud files: {stats['cloud_files']}")
                    print(f"   Cloud storage: {stats['cloud_storage_mb']:.1f}MB")
                    print(f"   Active threads: {stats['active_threads']}")
                    
                elif cmd == 'stats':
                    stats = network_manager.get_network_statistics()
                    print(f"\n Detailed Statistics:")
                    for key, value in stats.items():
                        display_key = key.replace('_', ' ').title()
                        if 'mb' in key.lower():
                            print(f"   {display_key}: {value:.2f}")
                        else:
                            print(f"   {display_key}: {value}")
                        
                elif cmd == 'nodes':
                    with network_manager.node_connection_lock:
                        print(f"\n Connected Nodes ({len(network_manager.connected_nodes)}):")
                        for node_id, info in network_manager.connected_nodes.items():
                            connected_time = info['connected_at'].strftime('%H:%M:%S')
                            node_ip = info['node_info'].get('ip_address', 'unknown')
                            status = info.get('status', 'unknown')
                            print(f"   {node_id} ({node_ip}) - {status} since {connected_time}")
                
                elif cmd == 'cloud':
                    files = network_manager.file_service.list_cloud_files()
                    if files:
                        total_size = sum(f['file_size'] for f in files)
                        total_downloads = sum(f.get('download_count', 0) for f in files)
                        print(f"\n Cloud Storage Statistics:")
                        print(f"   Total files: {len(files)}")
                        print(f"   Total size: {total_size/(1024*1024):.2f}MB")
                        print(f"   Total downloads: {total_downloads}")
                        print(f"   Replication factor: {network_manager.file_service.replication_factor}")
                    else:
                        print("\n No files in cloud storage")
                        
                elif cmd == 'files':
                    files = network_manager.file_service.list_cloud_files()
                    if files:
                        print(f"\n Cloud Files ({len(files)}):")
                        for f in files:
                            size_mb = f['file_size'] / (1024 * 1024)
                            replicas = len(f.get('replica_nodes', []))
                            downloads = f.get('download_count', 0)
                            print(f"    {f['filename']} ({size_mb:.1f}MB)")
                            print(f"      ID: {f['file_id'][:8]}... | Replicas: {replicas} | Downloads: {downloads}")
                            print(f"      Uploaded by: {f['uploader_node_id']} at {f['upload_timestamp']}")
                    else:
                        print("\n No files in cloud storage")
                        
                else:
                    print("Unknown command. Use: status, stats, nodes, cloud, files, quit")
                    
            except KeyboardInterrupt:
                break
                
    except KeyboardInterrupt:
        pass
    finally:
        network_manager.stop_network()

if __name__ == "__main__"
    main()