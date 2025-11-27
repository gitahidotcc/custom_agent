"""
LLRP (Low Level Reader Protocol) client for Zebra FX Series RFID readers.

This module implements a client for connecting to Zebra RFID readers via LLRP
protocol to receive real-time tag reports containing EPC IDs.
"""

import socket
import struct
import logging
import time
from typing import Optional, Callable, Dict, Any

logger = logging.getLogger(__name__)


class LLRPClient:
    """LLRP protocol client for Zebra RFID readers."""
    
    # LLRP Message Types
    MSG_GET_READER_CAPABILITIES = 1
    MSG_GET_READER_CONFIG = 2
    MSG_SET_READER_CONFIG = 3
    MSG_GET_READER_CAPABILITIES_RESPONSE = 11
    MSG_GET_READER_CONFIG_RESPONSE = 12
    MSG_SET_READER_CONFIG_RESPONSE = 13
    MSG_ADD_ROSPEC = 20
    MSG_DELETE_ROSPEC = 21
    MSG_START_ROSPEC = 22
    MSG_STOP_ROSPEC = 23
    MSG_ENABLE_ROSPEC = 24
    MSG_DISABLE_ROSPEC = 25
    MSG_GET_ROSPECS = 26
    MSG_ADD_ROSPEC_RESPONSE = 40
    MSG_DELETE_ROSPEC_RESPONSE = 41
    MSG_START_ROSPEC_RESPONSE = 42
    MSG_STOP_ROSPEC_RESPONSE = 43
    MSG_ENABLE_ROSPEC_RESPONSE = 44
    MSG_DISABLE_ROSPEC_RESPONSE = 45
    MSG_GET_ROSPECS_RESPONSE = 46
    MSG_RO_ACCESS_REPORT = 61
    
    # Parameter Types
    PARAM_RO_SPEC = 237
    PARAM_AI_SPEC = 240
    PARAM_RO_REPORT_SPEC = 239
    PARAM_TAG_REPORT_DATA = 241
    PARAM_EPC_DATA = 242
    PARAM_EPC_96 = 236
    
    # LLRP Version
    LLRP_VERSION = 1
    
    def __init__(self, host: str, port: int = 5084, timeout: int = 30, setup_rospec: bool = False):
        """
        Initialize LLRP client.
        
        Args:
            host: Zebra reader hostname or IP address
            port: LLRP port (default: 5084)
            timeout: Connection timeout in seconds (default: 30)
            setup_rospec: Whether to attempt ROSpec setup (default: False, reader may be pre-configured)
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.setup_rospec = setup_rospec
        self.socket: Optional[socket.socket] = None
        self.message_id = 1
        self.connected = False
        self.tag_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None
        
    def connect(self) -> bool:
        """
        Establish connection to Zebra reader.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            logger.info(f"Connecting to Zebra reader at {self.host}:{self.port}")
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(self.timeout)
            self.socket.connect((self.host, self.port))
            logger.info("Socket connected to Zebra reader")
            
            # Wait a moment for connection to stabilize
            time.sleep(0.2)
            
            # Mark as connected
            self.connected = True
            logger.info("Successfully connected to Zebra reader")
            
            # Skip setup messages by default - many readers are pre-configured
            # If setup is needed, it can be enabled via setup_rospec parameter
            if self.setup_rospec:
                try:
                    # Send GET_READER_CAPABILITIES to initialize
                    logger.debug("Sending GET_READER_CAPABILITIES...")
                    self._send_get_reader_capabilities()
                    
                    # Wait and flush any responses
                    time.sleep(0.3)
                    self._flush_pending_messages()
                except Exception as e:
                    logger.warning(f"GET_READER_CAPABILITIES failed (may be normal): {e}")
                    # Check if connection is still alive
                    if not self.connected:
                        return False
            
            # Configure ROSpec for tag reporting (optional - reader may already be configured)
            if self.setup_rospec:
                try:
                    logger.debug("Setting up ROSpec...")
                    self._setup_rospec()
                    
                    # Wait and flush any responses
                    time.sleep(0.3)
                    self._flush_pending_messages()
                except ConnectionError:
                    # Connection was lost during setup
                    logger.error("Connection lost during ROSpec setup")
                    return False
                except Exception as e:
                    logger.warning(f"ROSpec setup failed (reader may already be configured): {e}")
                    # Check if connection is still alive
                    if not self.connected:
                        logger.error("Connection lost after ROSpec setup failure")
                        return False
                    # Connection is still alive, continue anyway - reader might already be configured
            else:
                logger.info("Skipping ROSpec setup (reader should be pre-configured)")
            
            # Verify connection is still active
            if not self.connected:
                logger.error("Connection lost during setup")
                return False
            
            logger.info("Connection setup complete")
            return True
            
        except socket.timeout:
            logger.error(f"Connection timeout to {self.host}:{self.port}")
            self.connected = False
            if self.socket:
                self.socket.close()
                self.socket = None
            return False
        except socket.gaierror as e:
            logger.error(f"DNS resolution failed for {self.host}: {e}")
            self.connected = False
            if self.socket:
                self.socket.close()
                self.socket = None
            return False
        except (socket.error, ConnectionError) as e:
            logger.error(f"Failed to connect to Zebra reader at {self.host}:{self.port}: {e}")
            self.connected = False
            if self.socket:
                try:
                    self.socket.close()
                except:
                    pass
                self.socket = None
            return False
        except Exception as e:
            logger.error(f"Unexpected error connecting to reader: {e}", exc_info=True)
            self.connected = False
            if self.socket:
                try:
                    self.socket.close()
                except:
                    pass
                self.socket = None
            return False
    
    def disconnect(self):
        """Close connection to Zebra reader."""
        if self.socket:
            try:
                if self.connected:
                    self._send_stop_rospec()
                self.socket.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                self.socket = None
                self.connected = False
                logger.info("Disconnected from Zebra reader")
    
    def set_tag_callback(self, callback: Callable[[str, Dict[str, Any]], None]):
        """
        Set callback function for tag reports.
        
        Args:
            callback: Function that receives (epc_id, tag_data) when tag is detected
        """
        self.tag_callback = callback
    
    def listen(self):
        """
        Start listening for tag reports in blocking mode.
        
        This method will continuously read messages from the reader
        and process tag reports until an error occurs or connection is lost.
        """
        if not self.connected or not self.socket:
            logger.error("Not connected to reader. Call connect() first.")
            return
        
        logger.info("Starting to listen for tag reports...")
        
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        while self.connected:
            try:
                # Read LLRP message header (12 bytes: version + type + length + id)
                header = self._receive_bytes(12)
                if not header or len(header) < 12:
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error(f"Too many consecutive errors ({consecutive_errors}). Connection may be lost.")
                        self.connected = False
                        break
                    if not header:
                        continue
                    logger.warning(f"Incomplete header received ({len(header)} bytes, expected 12)")
                    continue
                
                # Reset error counter on successful header read
                consecutive_errors = 0
                
                # Parse header to get message length (bytes 4-8)
                version = header[0] & 0x3F
                message_type = int.from_bytes(header[1:4], byteorder='big')
                message_length = struct.unpack('>I', header[4:8])[0]
                message_id = struct.unpack('>I', header[8:12])[0]
                
                # Validate message length
                if message_length < 12 or message_length > 65536:
                    logger.debug(f"Invalid message length: {message_length} (type: {message_type}, id: {message_id}). Attempting to resync...")
                    # Try to resync by looking for a valid header pattern
                    # Discard first byte and try again
                    sync_byte = self._receive_bytes(1)
                    if not sync_byte:
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            logger.error(f"Too many consecutive errors ({consecutive_errors}). Connection may be lost.")
                            self.connected = False
                            break
                    continue
                
                # Read the rest of the message body (we already have 12 bytes of header)
                body_length = message_length - 12
                if body_length > 0:
                    message_body = self._receive_bytes(body_length)
                    if not message_body or len(message_body) < body_length:
                        logger.warning(f"Incomplete message body (expected {body_length}, got {len(message_body) if message_body else 0})")
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            logger.error(f"Too many consecutive errors ({consecutive_errors}). Connection may be lost.")
                            self.connected = False
                            break
                        continue
                    full_message = header + message_body
                else:
                    full_message = header
                
                # Parse and process the message
                self._parse_message(full_message)
                
            except socket.timeout:
                # Timeout is expected, continue listening
                continue
            except socket.error as e:
                logger.error(f"Socket error while listening: {e}")
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"Too many consecutive errors ({consecutive_errors}). Connection lost.")
                    self.connected = False
                    break
                time.sleep(0.5)  # Brief pause before retrying
                continue
            except struct.error as e:
                logger.error(f"Error unpacking message: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error processing message: {e}", exc_info=True)
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"Too many consecutive errors ({consecutive_errors}). Stopping listener.")
                    self.connected = False
                    break
                continue
    
    def _receive_bytes(self, count: int) -> Optional[bytes]:
        """Receive exact number of bytes from socket."""
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
                return None
            except socket.error:
                return None
        
        return data
    
    def _send_message(self, message_type: int, message_body: bytes = b'') -> int:
        """
        Send LLRP message to reader.
        
        Args:
            message_type: LLRP message type
            message_body: Message body bytes
            
        Returns:
            Message ID
            
        Raises:
            ConnectionError: If not connected or connection lost
        """
        if not self.socket or not self.connected:
            raise ConnectionError("Not connected to reader")
        
        msg_id = self.message_id
        self.message_id += 1
        
        # LLRP message header: version (1 byte), message_type (3 bytes), message_length (4 bytes), message_id (4 bytes)
        version_byte = self.LLRP_VERSION & 0x3F  # 6 bits
        message_type_bytes = message_type.to_bytes(3, byteorder='big')
        
        message_length = 12 + len(message_body)  # Header (12 bytes) + body
        header = struct.pack('>B', version_byte) + message_type_bytes + struct.pack('>II', message_length, msg_id)
        
        message = header + message_body
        
        try:
            self.socket.sendall(message)
            logger.debug(f"Sent LLRP message type {message_type}, ID {msg_id}")
            return msg_id
        except socket.timeout:
            logger.error(f"Timeout sending message type {message_type}")
            self.connected = False
            raise ConnectionError("Connection timeout")
        except socket.error as e:
            logger.error(f"Socket error sending message: {e}")
            self.connected = False
            raise ConnectionError(f"Connection lost: {e}")
        except Exception as e:
            logger.error(f"Unexpected error sending message: {e}")
            raise
    
    def _parse_message(self, data: bytes):
        """Parse incoming LLRP message."""
        if len(data) < 12:
            return
        
        # Parse header
        version = data[0] & 0x3F
        message_type = int.from_bytes(data[1:4], byteorder='big')
        message_length = struct.unpack('>I', data[4:8])[0]
        message_id = struct.unpack('>I', data[8:12])[0]
        
        body = data[12:] if len(data) > 12 else b''
        
        # Log all message types for debugging
        if message_type == self.MSG_RO_ACCESS_REPORT:
            logger.debug(f"Received RO_ACCESS_REPORT message, ID {message_id}")
        elif message_type in [self.MSG_ADD_ROSPEC_RESPONSE, self.MSG_ENABLE_ROSPEC_RESPONSE, 
                              self.MSG_START_ROSPEC_RESPONSE, self.MSG_GET_READER_CAPABILITIES_RESPONSE]:
            logger.debug(f"Received response message type {message_type}, ID {message_id}")
        else:
            logger.debug(f"Received LLRP message type {message_type}, ID {message_id}")
        
        # Handle RO_ACCESS_REPORT (tag reports)
        if message_type == self.MSG_RO_ACCESS_REPORT:
            self._handle_ro_access_report(body)
    
    def _handle_ro_access_report(self, body: bytes):
        """Parse RO_ACCESS_REPORT message to extract tag data."""
        offset = 0
        
        # Parse TagReportData parameters
        while offset < len(body):
            if offset + 2 > len(body):
                break
            
            param_type = struct.unpack('>H', body[offset:offset+2])[0]
            param_length = struct.unpack('>H', body[offset+2:offset+4])[0] if offset + 4 <= len(body) else 0
            
            if param_type == self.PARAM_TAG_REPORT_DATA:
                # Extract EPC from TagReportData
                tag_data = body[offset:offset+param_length]
                epc_id = self._extract_epc_from_tag_report(tag_data)
                
                if epc_id and self.tag_callback:
                    tag_info = {
                        'timestamp': time.time(),
                        'epc_id': epc_id
                    }
                    self.tag_callback(epc_id, tag_info)
            
            if param_length > 0:
                offset += param_length
            else:
                break
    
    def _extract_epc_from_tag_report(self, tag_data: bytes) -> Optional[str]:
        """Extract EPC ID from TagReportData parameter."""
        offset = 0
        
        # TagReportData contains EPC parameter
        while offset < len(tag_data):
            if offset + 2 > len(tag_data):
                break
            
            param_type = struct.unpack('>H', tag_data[offset:offset+2])[0]
            
            if param_type == self.PARAM_EPC_DATA or param_type == self.PARAM_EPC_96:
                # EPC-96 format: 12 bytes (96 bits)
                if offset + 16 <= len(tag_data):
                    epc_bytes = tag_data[offset+4:offset+16]
                    # Convert to hex string
                    epc_hex = ''.join(f'{b:02X}' for b in epc_bytes)
                    return epc_hex
            
            if offset + 4 <= len(tag_data):
                param_length = struct.unpack('>H', tag_data[offset+2:offset+4])[0]
                if param_length > 0:
                    offset += param_length
                else:
                    break
            else:
                break
        
        return None
    
    def _send_get_reader_capabilities(self):
        """Send GET_READER_CAPABILITIES message."""
        try:
            self._send_message(self.MSG_GET_READER_CAPABILITIES)
        except Exception as e:
            logger.warning(f"Failed to send GET_READER_CAPABILITIES: {e}")
    
    def _setup_rospec(self):
        """Configure ROSpec for tag reporting."""
        # This is a simplified ROSpec setup
        # Note: Many readers are pre-configured and may not need this setup
        # If setup fails, the reader may already be configured to send tag reports
        try:
            if not self.connected:
                raise ConnectionError("Not connected")
            
            # Build ROSpec body (simplified)
            # ROSpec ID = 1, Priority = 0, State = Disabled
            rospec_body = struct.pack('>I', 1)  # ROSpec ID
            rospec_body += struct.pack('>B', 0)  # Priority
            rospec_body += struct.pack('>B', 0)  # CurrentState (0 = Disabled)
            
            # AISpec (Antenna Inventory Spec)
            aispec = self._build_aispec()
            rospec_body += aispec
            
            # ROReportSpec
            roreportspec = self._build_roreportspec()
            rospec_body += roreportspec
            
            # Send ADD_ROSPEC
            if not self.connected:
                raise ConnectionError("Connection lost before ADD_ROSPEC")
            param_header = struct.pack('>HH', self.PARAM_RO_SPEC, len(rospec_body) + 4)
            self._send_message(self.MSG_ADD_ROSPEC, param_header + rospec_body)
            time.sleep(0.1)  # Brief delay between messages
            
            # Enable ROSpec
            if not self.connected:
                raise ConnectionError("Connection lost before ENABLE_ROSPEC")
            enable_body = struct.pack('>I', 1)  # ROSpec ID
            self._send_message(self.MSG_ENABLE_ROSPEC, enable_body)
            time.sleep(0.1)
            
            # Start ROSpec
            if not self.connected:
                raise ConnectionError("Connection lost before START_ROSPEC")
            start_body = struct.pack('>I', 1)  # ROSpec ID
            self._send_message(self.MSG_START_ROSPEC, start_body)
            
            logger.info("ROSpec configured and started")
            
        except ConnectionError as e:
            logger.warning(f"Connection lost during ROSpec setup: {e}")
            raise  # Re-raise to allow caller to handle
        except Exception as e:
            logger.warning(f"Failed to setup ROSpec (reader may already be configured): {e}")
            # Don't raise - allow connection to continue
    
    def _build_aispec(self) -> bytes:
        """Build AISpec parameter."""
        # Simplified AISpec - all antennas, continuous inventory
        aispec_body = struct.pack('>H', 0)  # AntennaIDs count (0 = all)
        aispec_body += struct.pack('>I', 0)  # AISpecStopTrigger (0 = null)
        aispec_body += struct.pack('>I', 0)  # InventoryParameterSpecID
        aispec_body += struct.pack('>HH', 0, 0)  # ProtocolID (0 = EPCGlobal Class1 Gen2)
        
        param_length = 4 + len(aispec_body)
        return struct.pack('>HH', self.PARAM_AI_SPEC, param_length) + aispec_body
    
    def _build_roreportspec(self) -> bytes:
        """Build ROReportSpec parameter."""
        # Configure to report on all tag reads
        roreportspec_body = struct.pack('>I', 0)  # ROReportTrigger (0 = Upon_See_RO_Access_Report)
        roreportspec_body += struct.pack('>H', 0)  # N (0 = report all)
        roreportspec_body += struct.pack('>B', 1)  # TagReportContentSelector (include EPC)
        
        param_length = 4 + len(roreportspec_body)
        return struct.pack('>HH', self.PARAM_RO_REPORT_SPEC, param_length) + roreportspec_body
    
    def _flush_pending_messages(self, timeout: float = 1.0):
        """
        Read and discard any pending messages in the socket buffer.
        
        This is useful after sending setup messages to clear response messages
        before starting to listen for tag reports.
        
        Args:
            timeout: Maximum time to spend flushing messages
        """
        if not self.socket:
            return
        
        original_timeout = self.socket.gettimeout()
        self.socket.settimeout(0.1)  # Short timeout for flushing
        
        end_time = time.time() + timeout
        messages_read = 0
        
        try:
            while time.time() < end_time:
                try:
                    # Try to read a header
                    header = self._receive_bytes(12)
                    if not header or len(header) < 12:
                        break
                    
                    # Get message length
                    message_length = struct.unpack('>I', header[4:8])[0]
                    
                    # Validate and read full message
                    if 12 <= message_length <= 65536:
                        body_length = message_length - 12
                        if body_length > 0:
                            body = self._receive_bytes(body_length)
                            if not body or len(body) < body_length:
                                break
                        messages_read += 1
                        
                        # Parse and log the message type
                        message_type = int.from_bytes(header[1:4], byteorder='big')
                        logger.debug(f"Flushed pending message type {message_type}")
                    else:
                        # Invalid message, stop flushing
                        break
                except socket.timeout:
                    break
                except Exception as e:
                    logger.debug(f"Error flushing message: {e}")
                    break
        finally:
            self.socket.settimeout(original_timeout)
        
        if messages_read > 0:
            logger.debug(f"Flushed {messages_read} pending message(s)")
    
    def _send_stop_rospec(self):
        """Stop and disable ROSpec."""
        try:
            # Stop ROSpec
            stop_body = struct.pack('>I', 1)  # ROSpec ID
            self._send_message(self.MSG_STOP_ROSPEC, stop_body)
            
            # Disable ROSpec
            disable_body = struct.pack('>I', 1)  # ROSpec ID
            self._send_message(self.MSG_DISABLE_ROSPEC, disable_body)
        except Exception as e:
            logger.warning(f"Failed to stop ROSpec: {e}")

