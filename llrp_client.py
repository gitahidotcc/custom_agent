"""
Simple LLRP client for Zebra FX Series RFID readers.
Connects and listens for tag reports - no complex setup.
"""

import socket
import struct
import logging
import time
from typing import Optional, Callable, Dict, Any, Tuple

logger = logging.getLogger(__name__)


class LLRPClient:
    """Simple LLRP client - connects and listens for tag events."""
    
    # LLRP Message Types
    MSG_RO_ACCESS_REPORT = 61
    MSG_READER_EVENT_NOTIFICATION = 63
    
    def __init__(self, host: str, port: int = 5084, timeout: int = 30):
        """
        Initialize LLRP client.
        
        Args:
            host: Zebra reader hostname or IP address
            port: LLRP port (default: 5084)
            timeout: Socket timeout in seconds
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.socket: Optional[socket.socket] = None
        self.connected = False
        self.tag_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None
        
    def connect(self) -> bool:
        """Connect to the reader and start listening."""
        try:
            logger.info(f"Connecting to Zebra reader at {self.host}:{self.port}")
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(self.timeout)
            self.socket.connect((self.host, self.port))
            self.connected = True
            logger.info("Connected to Zebra reader")
            
            # Read and discard the initial READER_EVENT_NOTIFICATION
            self._read_initial_message()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            self.connected = False
            if self.socket:
                try:
                    self.socket.close()
                except:
                    pass
                self.socket = None
            return False
    
    def disconnect(self):
        """Disconnect from the reader."""
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
            self.socket = None
            self.connected = False
            logger.info("Disconnected from reader")
    
    def set_tag_callback(self, callback: Callable[[str, Dict[str, Any]], None]):
        """Set callback for when tags are read."""
        self.tag_callback = callback
    
    def listen(self):
        """Listen for tag reports from the reader."""
        if not self.connected or not self.socket:
            logger.error("Not connected")
            return
        
        logger.info("Listening for tag reports...")
        
        while self.connected:
            try:
                # Read message header (10 bytes)
                header = self._recv_exact(10)
                if not header:
                    continue
                
                # Parse header
                msg_type = ((header[0] & 0x03) << 8) | header[1]
                msg_length = struct.unpack('>I', header[2:6])[0]
                
                # Read body
                body_length = msg_length - 10
                body = b''
                if body_length > 0:
                    body = self._recv_exact(body_length)
                    if not body:
                        continue
                
                # Handle RO_ACCESS_REPORT (tag reads)
                if msg_type == self.MSG_RO_ACCESS_REPORT:
                    self._handle_tag_report(body)
                    
            except socket.timeout:
                continue
            except Exception as e:
                logger.error(f"Error: {e}")
                self.connected = False
                break
    
    def _recv_exact(self, count: int) -> Optional[bytes]:
        """Receive exact number of bytes."""
        if not self.socket:
            return None
        
        data = b''
        while len(data) < count:
            try:
                chunk = self.socket.recv(count - len(data))
                if not chunk:
                    return None
                data += chunk
            except socket.timeout:
                if data:
                    continue
                return None
            except:
                return None
        return data
    
    def _read_initial_message(self):
        """Read and discard the initial message from reader."""
        try:
            self.socket.settimeout(2)
            header = self._recv_exact(10)
            if header and len(header) >= 10:
                msg_length = struct.unpack('>I', header[2:6])[0]
                body_length = msg_length - 10
                if body_length > 0:
                    self._recv_exact(body_length)
                logger.info("Received initial message from reader")
        except:
            pass
        finally:
            self.socket.settimeout(self.timeout)
    
    def _handle_tag_report(self, body: bytes):
        """Extract EPC IDs from tag report."""
        if not body or not self.tag_callback:
            return
        
        # Parse TLV parameters looking for EPC data
        offset = 0
        while offset < len(body) - 4:
            try:
                param_type = struct.unpack('>H', body[offset:offset+2])[0]
                param_length = struct.unpack('>H', body[offset+2:offset+4])[0]
                
                if param_length < 4:
                    break
                
                # TagReportData contains the EPC
                if param_type == 241:  # TagReportData
                    epc = self._extract_epc(body[offset:offset+param_length])
                    if epc:
                        self.tag_callback(epc, {'timestamp': time.time()})
                
                offset += param_length
            except:
                break
    
    def _extract_epc(self, data: bytes) -> Optional[str]:
        """Extract EPC hex string from TagReportData."""
        # Look for EPC-96 parameter (type 13 as TV parameter = 0x8D)
        for i in range(len(data) - 14):
            if data[i] == 0x8D:  # TV parameter type 13 (EPC-96)
                # EPC-96: 1 byte type + 2 bytes EPC length info + 12 bytes EPC
                epc_bytes = data[i+3:i+15]
                if len(epc_bytes) == 12:
                    return epc_bytes.hex().upper()
        
        # Alternative: look for EPCData TLV (type 241)
        offset = 4  # Skip TagReportData header
        while offset < len(data) - 4:
            try:
                p_type = struct.unpack('>H', data[offset:offset+2])[0]
                p_len = struct.unpack('>H', data[offset+2:offset+4])[0]
                
                if p_type == 241 and p_len >= 16:  # EPCData
                    epc_bytes = data[offset+6:offset+18]
                    return epc_bytes.hex().upper()
                
                if p_len < 4:
                    break
                offset += p_len
            except:
                break
        
        return None
