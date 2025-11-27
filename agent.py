"""
Main agent for Zebra RFID reader to SNIPE-IT integration.

This agent continuously listens for RFID tag reads from Zebra readers,
looks up assets in SNIPE-IT, and performs checkout/check-in operations
based on location detection logic.
"""

import logging
import logging.handlers
import signal
import sys
import time
import yaml
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

from llrp_client import LLRPClient
from snipeit_client import SnipeITClient

# Initialize logger for module-level use
logger = logging.getLogger(__name__)


class ZebraSnipeITAgent:
    """Main agent for Zebra RFID to SNIPE-IT integration."""
    
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialize the agent with configuration.
        
        Args:
            config_path: Path to configuration YAML file
        """
        self.config_path = config_path
        self.config: Dict[str, Any] = {}
        self.llrp_client: Optional[LLRPClient] = None
        self.snipeit_client: Optional[SnipeITClient] = None
        
        # Location tracking state
        # Maps asset_id -> last_known_location_id
        self.asset_location_state: Dict[int, int] = {}
        
        # Track last seen time for each EPC ID (for deduplication)
        self.last_seen_tags: Dict[str, float] = {}
        self.tag_cooldown = 2.0  # seconds - ignore same tag for 2 seconds
        
        # Signal handling
        self.running = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Unknown tags logger
        self.unknown_tags_logger: Optional[logging.Logger] = None
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}. Shutting down...")
        self.running = False
    
    def load_config(self) -> bool:
        """
        Load configuration from YAML file.
        
        Returns:
            True if config loaded successfully, False otherwise
        """
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            
            # Validate required configuration
            required_keys = ['zebra_reader', 'snipeit', 'locations', 'logging']
            for key in required_keys:
                if key not in self.config:
                    logger.error(f"Missing required configuration key: {key}")
                    return False
            
            logger.info("Configuration loaded successfully")
            return True
            
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            return False
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration file: {e}")
            return False
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return False
    
    def setup_logging(self):
        """Configure logging system with file and console output, and log rotation."""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO').upper())
        log_file = log_config.get('file', 'logs/agent.log')
        unknown_tags_file = log_config.get('unknown_tags_file', 'logs/unknown_tags.log')
        
        # Ensure log directory exists
        log_dir = Path(log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        
        # Remove existing handlers to avoid duplicates
        root_logger.handlers.clear()
        
        # File handler with rotation (10MB per file, keep 5 backups)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(console_handler)
        
        # Create separate logger for unknown tags with rotation
        self.unknown_tags_logger = logging.getLogger('unknown_tags')
        self.unknown_tags_logger.setLevel(logging.INFO)
        # Prevent propagation to root logger
        self.unknown_tags_logger.propagate = False
        
        unknown_tags_handler = logging.handlers.RotatingFileHandler(
            unknown_tags_file,
            maxBytes=5 * 1024 * 1024,  # 5MB
            backupCount=5,
            encoding='utf-8'
        )
        unknown_tags_handler.setFormatter(simple_formatter)
        self.unknown_tags_logger.addHandler(unknown_tags_handler)
        
        logger.info(f"Logging configured. Main log: {log_file} (with rotation), Unknown tags log: {unknown_tags_file} (with rotation)")
    
    def initialize_clients(self) -> bool:
        """
        Initialize LLRP and SNIPE-IT clients.
        
        Returns:
            True if clients initialized successfully, False otherwise
        """
        # Initialize SNIPE-IT client
        snipeit_config = self.config['snipeit']
        try:
            self.snipeit_client = SnipeITClient(
                base_url=snipeit_config['base_url'],
                api_token=snipeit_config['api_token']
            )
            logger.info("SNIPE-IT client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize SNIPE-IT client: {e}")
            return False
        
        # Initialize LLRP client
        zebra_config = self.config['zebra_reader']
        try:
            self.llrp_client = LLRPClient(
                host=zebra_config['host'],
                port=zebra_config.get('port', 5084)
            )
            
            # Set tag callback
            self.llrp_client.set_tag_callback(self._handle_tag_read)
            
            logger.info("LLRP client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize LLRP client: {e}")
            return False
        
        return True
    
    def _handle_tag_read(self, epc_id: str, tag_data: Dict[str, Any]):
        """
        Handle tag read event from LLRP client.
        
        Args:
            epc_id: EPC ID from RFID tag
            tag_data: Additional tag data (timestamp, etc.)
        """
        current_time = time.time()
        
        # Deduplication: ignore same tag if seen recently
        if epc_id in self.last_seen_tags:
            time_since_last_seen = current_time - self.last_seen_tags[epc_id]
            if time_since_last_seen < self.tag_cooldown:
                logger.debug(f"Ignoring duplicate tag read: {epc_id} (seen {time_since_last_seen:.2f}s ago)")
                return
        
        self.last_seen_tags[epc_id] = current_time
        
        logger.info(f"Tag detected: {epc_id}")
        
        # Lookup asset in SNIPE-IT
        asset = self._lookup_asset(epc_id)
        
        if asset:
            self._process_asset(asset, epc_id)
        else:
            self._handle_unknown_tag(epc_id)
    
    def _lookup_asset(self, epc_id: str) -> Optional[Dict[str, Any]]:
        """
        Lookup asset in SNIPE-IT by tag or serial.
        
        Args:
            epc_id: EPC ID to search for
            
        Returns:
            Asset data if found, None otherwise
        """
        if not self.snipeit_client:
            return None
        
        # Try lookup by tag first
        asset = self.snipeit_client.lookup_asset_by_tag(epc_id)
        
        # If not found, try by serial
        if not asset:
            asset = self.snipeit_client.lookup_asset_by_serial(epc_id)
        
        return asset
    
    def _process_asset(self, asset: Dict[str, Any], epc_id: str):
        """
        Process known asset - determine action based on location logic.
        
        Args:
            asset: Asset data from SNIPE-IT
            epc_id: EPC ID that was detected
        """
        asset_id = asset.get('id')
        if not asset_id:
            logger.warning(f"Asset data missing ID for EPC: {epc_id}")
            return
        
        current_location_id = asset.get('location', {}).get('id') if asset.get('location') else None
        last_location_id = self.asset_location_state.get(asset_id)
        
        logger.info(f"Processing asset ID {asset_id} (EPC: {epc_id})")
        logger.debug(f"Current location: {current_location_id}, Last known: {last_location_id}")
        
        # Determine action based on location logic
        action = self._determine_action(asset, current_location_id, last_location_id)
        
        if action:
            action_type, target_location_id, notes = action
            self._execute_action(asset_id, action_type, target_location_id, notes)
            # Update location state
            if target_location_id:
                self.asset_location_state[asset_id] = target_location_id
        else:
            # No action needed, but update state tracking
            if current_location_id:
                self.asset_location_state[asset_id] = current_location_id
    
    def _determine_action(self, asset: Dict[str, Any], current_location_id: Optional[int], 
                         last_location_id: Optional[int]) -> Optional[tuple]:
        """
        Determine what action to take based on location logic.
        
        Location detection logic for dock entry/exit and warehouse tracking:
        - If asset is detected and was at dock_exit or unknown -> check-in to dock_entry
        - If asset is detected and was at dock_entry -> checkout to warehouse
        - If asset is detected and was at warehouse -> checkout to dock_exit
        
        Args:
            asset: Asset data
            current_location_id: Current location ID from SNIPE-IT
            last_location_id: Last known location ID from our state
            
        Returns:
            Tuple of (action_type, target_location_id, notes) or None
        """
        locations = self.config.get('locations', {})
        dock_entry_id = locations.get('dock_entry')
        dock_exit_id = locations.get('dock_exit')
        warehouse_id = locations.get('warehouse')
        
        # Use last known location if current is not available
        effective_location_id = current_location_id or last_location_id
        
        # Entry detection: asset detected entering from outside
        # If asset was at dock_exit or unknown -> move to dock_entry (check-in)
        if effective_location_id == dock_exit_id or effective_location_id is None:
            if dock_entry_id:
                return ('checkin', dock_entry_id, 'Detected at dock entry')
        
        # Entry -> Warehouse: asset moving from dock to warehouse
        # If asset was at dock_entry -> move to warehouse (checkout)
        elif effective_location_id == dock_entry_id:
            if warehouse_id:
                return ('checkout', warehouse_id, 'Moving from dock to warehouse')
        
        # Warehouse -> Exit: asset moving from warehouse to dock exit
        # If asset was at warehouse -> move to dock_exit (checkout)
        elif effective_location_id == warehouse_id:
            if dock_exit_id:
                return ('checkout', dock_exit_id, 'Moving from warehouse to dock exit')
        
        # No action needed if location hasn't changed meaningfully
        return None
    
    def _execute_action(self, asset_id: int, action_type: str, location_id: int, notes: str):
        """
        Execute checkout or check-in action.
        
        Args:
            asset_id: Asset ID
            action_type: 'checkout' or 'checkin'
            location_id: Target location ID
            notes: Notes for the action
        """
        if not self.snipeit_client:
            logger.error("SNIPE-IT client not initialized")
            return
        
        success = False
        
        if action_type == 'checkout':
            success = self.snipeit_client.checkout_asset(
                asset_id=asset_id,
                location_id=location_id,
                notes=notes
            )
        elif action_type == 'checkin':
            success = self.snipeit_client.checkin_asset(
                asset_id=asset_id,
                location_id=location_id,
                notes=notes
            )
        else:
            logger.error(f"Unknown action type: {action_type}")
            return
        
        if success:
            logger.info(f"Successfully {action_type} asset {asset_id} to location {location_id}")
        else:
            logger.error(f"Failed to {action_type} asset {asset_id}")
    
    def _handle_unknown_tag(self, epc_id: str):
        """
        Handle unknown tag (not found in SNIPE-IT).
        
        Args:
            epc_id: EPC ID that was not found
        """
        if self.unknown_tags_logger:
            self.unknown_tags_logger.info(f"Unknown tag detected: {epc_id}")
        
        logger.warning(f"Unknown tag detected: {epc_id} - logged for manual review")
    
    def connect_to_reader(self) -> bool:
        """
        Connect to Zebra reader via LLRP.
        
        Returns:
            True if connected successfully, False otherwise
        """
        if not self.llrp_client:
            logger.error("LLRP client not initialized")
            return False
        
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            if self.llrp_client.connect():
                logger.info("Successfully connected to Zebra reader")
                return True
            
            if attempt < max_retries - 1:
                logger.warning(f"Connection failed. Retrying in {retry_delay} seconds... (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
        
        logger.error("Failed to connect to Zebra reader after all retries")
        return False
    
    def run(self):
        """Run the agent main loop."""
        # Setup basic logging first (before loading config so we can log errors)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        
        logger.info("Starting Zebra RFID to SNIPE-IT agent...")
        
        # Load configuration
        if not self.load_config():
            logger.error("Failed to load configuration. Exiting.")
            sys.exit(1)
        
        # Setup proper logging (needs config loaded for log file paths)
        self.setup_logging()
        
        # Initialize clients
        if not self.initialize_clients():
            logger.error("Failed to initialize clients. Exiting.")
            sys.exit(1)
        
        # Connect to reader
        if not self.connect_to_reader():
            logger.error("Failed to connect to reader. Exiting.")
            sys.exit(1)
        
        # Main loop
        self.running = True
        logger.info("Agent is running. Listening for tag reads...")
        
        try:
            while self.running:
                if not self.llrp_client or not self.llrp_client.connected:
                    logger.warning("Connection lost. Attempting to reconnect...")
                    time.sleep(5)
                    if not self.connect_to_reader():
                        logger.error("Reconnection failed. Waiting before retry...")
                        time.sleep(10)
                        continue
                
                # Listen for tag reports (blocking call)
                self.llrp_client.listen()
                
                # If listen() returns, connection was lost
                logger.warning("LLRP listen() returned. Connection may be lost.")
                time.sleep(5)
                
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Gracefully shutdown the agent."""
        logger.info("Shutting down agent...")
        
        if self.llrp_client:
            self.llrp_client.disconnect()
        
        logger.info("Agent shutdown complete")


def main():
    """Main entry point."""
    agent = ZebraSnipeITAgent()
    agent.run()


if __name__ == '__main__':
    main()

