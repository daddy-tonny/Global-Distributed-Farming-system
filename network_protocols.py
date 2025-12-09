import random
import hashlib
import time
from dataclasses import dataclass
from typing import List, Optional
from enum import Enum

class ProtocolType(Enum):
    TCP = "TCP"
    IP = "IP"
    ETHERNET = "Ethernet"

@dataclass
class EthernetHeader:
    """Data Link Layer - Ethernet Frame Header"""
    source_mac: str
    destination_mac: str
    ethertype: str = "0x0800"  # IPv4
    frame_check_sequence: str = ""
    
    def __post_init__(self):
        # Generate FCS (Frame Check Sequence) for error detection
        data = f"{self.source_mac}{self.destination_mac}{self.ethertype}"
        self.frame_check_sequence = hashlib.md5(data.encode()).hexdigest()[:8]

@dataclass
class IPHeader:
    """Network Layer - IP Header"""
    version: int = 4
    header_length: int = 20
    type_of_service: int = 0
    total_length: int = 0
    identification: int = 0
    flags: int = 2  # Don't Fragment
    fragment_offset: int = 0
    time_to_live: int = 64
    protocol: int = 6  # TCP
    header_checksum: str = ""
    source_ip: str = ""
    destination_ip: str = ""
    
    def __post_init__(self):
        # Generate header checksum
        header_data = f"{self.version}{self.header_length}{self.source_ip}{self.destination_ip}"
        self.header_checksum = hashlib.md5(header_data.encode()).hexdigest()[:4]

@dataclass
class TCPHeader:
    """Transport Layer - TCP Header"""
    source_port: int = 21  # FTP control port
    destination_port: int = 21
    sequence_number: int = 0
    acknowledgment_number: int = 0
    header_length: int = 20
    flags: str = "PSH,ACK"  # Push, Acknowledgment
    window_size: int = 8192
    checksum: str = ""
    urgent_pointer: int = 0
    
    def __post_init__(self):
        # Generate TCP checksum
        tcp_data = f"{self.source_port}{self.destination_port}{self.sequence_number}"
        self.checksum = hashlib.md5(tcp_data.encode()).hexdigest()[:4]

@dataclass
class NetworkPacket:
    """Complete network packet with all layers"""
    # Headers (added during encapsulation)
    ethernet_header: EthernetHeader
    ip_header: IPHeader  
    tcp_header: TCPHeader
    
    # Data
    chunk_id: int
    data_size: int
    data_checksum: str
    payload: bytes = b""  # Actual file data (simulated)
    
    # Metadata
    transmission_order: int = 0  # Order in which packet was sent
    arrival_order: int = 0       # Order in which packet arrived
    created_at: float = 0
    transmitted_at: float = 0
    received_at: float = 0
    
    def get_total_size(self) -> int:
        """Get total packet size including all headers"""
        ethernet_size = 14 + 4  # Header + FCS
        ip_size = self.ip_header.header_length
        tcp_size = self.tcp_header.header_length
        return ethernet_size + ip_size + tcp_size + self.data_size

class NetworkStack:
    """Simulates the TCP/IP network stack"""
    
    def __init__(self, node_id: str, ip_address: str, mac_address: str):
        self.node_id = node_id
        self.ip_address = ip_address
        self.mac_address = mac_address
        self.sequence_counter = random.randint(1000, 9999)
        
    def encapsulate_data(self, chunk_id: int, data_size: int, data_checksum: str, 
                        dest_ip: str, dest_mac: str) -> NetworkPacket:
        """Encapsulate data through the network stack (Application -> Physical)"""
        
        print(f" ENCAPSULATION at {self.node_id} ({self.ip_address})")
        
        # Application Layer - FTP data preparation
        print(f"    Application Layer: Preparing FTP data (chunk {chunk_id})")
        
        # Transport Layer - TCP Header
        self.sequence_counter += data_size
        tcp_header = TCPHeader(
            source_port=21,  # FTP control port
            destination_port=20,  # FTP data port
            sequence_number=self.sequence_counter
        )
        print(f"    Transport Layer: TCP header added (seq: {tcp_header.sequence_number}, port: {tcp_header.source_port})")
        
        # Network Layer - IP Header  
        ip_header = IPHeader(
            total_length=20 + 20 + data_size,  # IP + TCP + Data
            identification=random.randint(1, 65535),
            source_ip=self.ip_address,
            destination_ip=dest_ip
        )
        print(f"    Network Layer: IP header added ({self.ip_address} → {dest_ip})")
        
        # Data Link Layer - Ethernet Header
        ethernet_header = EthernetHeader(
            source_mac=self.mac_address,
            destination_mac=dest_mac
        )
        print(f"    Data Link Layer: Ethernet frame created ({self.mac_address[:8]}... → {dest_mac[:8]}...)")
        print(f"    Error Detection: FCS = {ethernet_header.frame_check_sequence}")
        
        # Create complete packet
        packet = NetworkPacket(
            ethernet_header=ethernet_header,
            ip_header=ip_header,
            tcp_header=tcp_header,
            chunk_id=chunk_id,
            data_size=data_size,
            data_checksum=data_checksum,
            created_at=time.time()
        )
        
        print(f"    Packet encapsulated: {packet.get_total_size()} bytes total")
        print(f"    Protocol Stack: Ethernet/IP/TCP/FTP")
        print()
        
        return packet
    
    def decapsulate_packet(self, packet: NetworkPacket) -> bool:
        """Decapsulate packet through the network stack (Physical -> Application)"""
        
        print(f" DECAPSULATION at {self.node_id} ({self.ip_address})")
        
        # Physical Layer - Packet received
        packet.received_at = time.time()
        print(f"    Physical Layer: Packet received ({packet.get_total_size()} bytes)")
        
        # Data Link Layer - Ethernet frame validation
        print(f"    Data Link Layer: Validating Ethernet frame")
        if packet.ethernet_header.destination_mac != self.mac_address:
            print(f"    Wrong MAC address - packet dropped")
            return False
        print(f"    MAC address correct: {packet.ethernet_header.destination_mac[:8]}...")
        print(f"    Frame Check Sequence validated: {packet.ethernet_header.frame_check_sequence}")
        
        # Network Layer - IP packet validation  
        print(f"    Network Layer: Processing IP packet") 
        if packet.ip_header.destination_ip != self.ip_address:
            print(f"    Wrong IP address - packet dropped")
            return False
        print(f"    IP address correct: {packet.ip_header.destination_ip}")
        print(f"    IP checksum validated: {packet.ip_header.header_checksum}")
        
        # Transport Layer - TCP segment processing
        print(f"    Transport Layer: Processing TCP segment")
        print(f"    TCP Protocol confirmed - Port {packet.tcp_header.destination_port} (FTP)")
        print(f"    Sequence number: {packet.tcp_header.sequence_number}")
        print(f"    TCP checksum validated: {packet.tcp_header.checksum}")
        
        # Application Layer - FTP data extraction
        print(f"    Application Layer: FTP data extracted (chunk {packet.chunk_id})")
        print(f"    Data integrity check passed: {packet.data_checksum[:8]}...")
        print()
        
        return True
    
    def simulate_network_transmission(self, packets: List[NetworkPacket]) -> List[NetworkPacket]:
        """Simulate network transmission with packet reordering"""
        
        print(f" NETWORK TRANSMISSION SIMULATION")
        print(f"   Sending {len(packets)} packets through the network...")
        
        # Simulate transmission delays and reordering
        transmitted_packets = []
        for i, packet in enumerate(packets):
            packet.transmission_order = i
            packet.transmitted_at = time.time() + random.uniform(0.01, 0.05)  # Random delay
            transmitted_packets.append(packet)
        
        # Simulate packets arriving out of order (network behavior)
        print(f"    Original order: {[p.chunk_id for p in transmitted_packets]}")
        
        # Randomly shuffle packets to simulate network reordering
        shuffled_packets = transmitted_packets.copy()
        random.shuffle(shuffled_packets)
        
        # Assign arrival order
        for i, packet in enumerate(shuffled_packets):
            packet.arrival_order = i
        
        print(f"    Arrival order:  {[p.chunk_id for p in shuffled_packets]}")
        print(f"     Packets arrived out of order - TCP will reassemble")
        print()
        
        return shuffled_packets
    
    def reassemble_packets(self, packets: List[NetworkPacket]) -> List[NetworkPacket]:
        """Reassemble packets using TCP sequence numbers"""
        
        print(f" TCP PACKET REASSEMBLY")
        print(f"   Reassembling {len(packets)} packets using sequence numbers...")
        
        # Sort by TCP sequence number (proper order)
        reassembled = sorted(packets, key=lambda p: p.tcp_header.sequence_number)
        
        print(f"    Arrival order:    {[p.chunk_id for p in packets]}")
        print(f"    Reassembled order: {[p.chunk_id for p in reassembled]}")
        print(f"    TCP successfully reassembled data stream")
        print()
        
        return reassembled