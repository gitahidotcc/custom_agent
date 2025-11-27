"""
SNIPE-IT API client for asset management operations.

This module provides a client for interacting with SNIPE-IT's REST API
to lookup assets, perform checkouts/check-ins, and manage asset locations.
"""

import logging
import time
from typing import Optional, Dict, Any, List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class SnipeITClient:
    """Client for SNIPE-IT REST API."""
    
    # SNIPE-IT API rate limit: 60 requests per minute
    RATE_LIMIT_REQUESTS = 60
    RATE_LIMIT_WINDOW = 60  # seconds
    
    def __init__(self, base_url: str, api_token: str, timeout: int = 30):
        """
        Initialize SNIPE-IT API client.
        
        Args:
            base_url: SNIPE-IT API base URL (e.g., 'https://instance.snipe-it.io/api/v1')
            api_token: API token in format 'Bearer <token>' or just '<token>'
            timeout: Request timeout in seconds (default: 30)
        """
        # Ensure base_url doesn't end with /
        self.base_url = base_url.rstrip('/')
        # Ensure api_token has Bearer prefix
        if not api_token.startswith('Bearer '):
            self.api_token = f'Bearer {api_token}'
        else:
            self.api_token = api_token
        
        self.timeout = timeout
        
        # Rate limiting tracking
        self.request_times: List[float] = []
        self.last_request_time = 0
        
        # Create session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PATCH", "PUT"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        self.session.headers.update({
            'Authorization': self.api_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        logger.info(f"Initialized SNIPE-IT client for {self.base_url}")
    
    def _enforce_rate_limit(self):
        """Enforce SNIPE-IT API rate limit (60 requests per minute)."""
        current_time = time.time()
        
        # Remove request times older than the rate limit window
        self.request_times = [t for t in self.request_times if current_time - t < self.RATE_LIMIT_WINDOW]
        
        # If we've hit the rate limit, wait
        if len(self.request_times) >= self.RATE_LIMIT_REQUESTS:
            oldest_request = min(self.request_times)
            wait_time = self.RATE_LIMIT_WINDOW - (current_time - oldest_request) + 0.1
            if wait_time > 0:
                logger.warning(f"Rate limit reached. Waiting {wait_time:.2f} seconds...")
                time.sleep(wait_time)
                # Clean up again after waiting
                current_time = time.time()
                self.request_times = [t for t in self.request_times if current_time - t < self.RATE_LIMIT_WINDOW]
        
        # Record this request time
        self.request_times.append(time.time())
        self.last_request_time = time.time()
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Make HTTP request to SNIPE-IT API with rate limiting and error handling.
        
        Args:
            method: HTTP method (GET, POST, PATCH, PUT)
            endpoint: API endpoint (e.g., '/hardware/bytag/EPC123')
            **kwargs: Additional arguments for requests (json, params, etc.)
            
        Returns:
            Response JSON as dict, or None if request failed
        """
        self._enforce_rate_limit()
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            logger.debug(f"Making {method} request to {endpoint}")
            response = self.session.request(
                method=method,
                url=url,
                timeout=self.timeout,
                **kwargs
            )
            
            # Handle rate limit response
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(f"Rate limit exceeded. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                # Retry once after waiting
                response = self.session.request(
                    method=method,
                    url=url,
                    timeout=self.timeout,
                    **kwargs
                )
            
            response.raise_for_status()
            
            # Handle empty responses
            if response.status_code == 204:
                return {}
            
            return response.json()
            
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for {endpoint} after {self.timeout}s")
            return None
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error for {endpoint}: {e}")
            return None
        except requests.exceptions.HTTPError as e:
            if response and response.status_code == 404:
                logger.debug(f"Asset not found: {endpoint}")
            elif response:
                logger.error(f"HTTP error {response.status_code} for {endpoint}: {e}")
                try:
                    error_data = response.json()
                    if 'messages' in error_data:
                        logger.error(f"Error details: {error_data['messages']}")
                    else:
                        logger.error(f"Error details: {error_data}")
                except:
                    logger.error(f"Error response (non-JSON): {response.text[:200]}")
            else:
                logger.error(f"HTTP error for {endpoint}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception for {endpoint}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for {endpoint}: {e}", exc_info=True)
            return None
    
    def lookup_asset_by_tag(self, asset_tag: str) -> Optional[Dict[str, Any]]:
        """
        Lookup asset by tag (EPC ID).
        
        Args:
            asset_tag: Asset tag or EPC ID to search for
            
        Returns:
            Asset data as dict if found, None otherwise
        """
        endpoint = f"/hardware/bytag/{asset_tag}"
        response = self._make_request('GET', endpoint)
        
        if response and 'rows' in response and len(response['rows']) > 0:
            asset = response['rows'][0]
            logger.debug(f"Found asset by tag '{asset_tag}': {asset.get('id')}")
            return asset
        
        return None
    
    def lookup_asset_by_serial(self, serial: str) -> Optional[Dict[str, Any]]:
        """
        Lookup asset by serial number.
        
        Args:
            serial: Serial number to search for
            
        Returns:
            Asset data as dict if found, None otherwise
        """
        endpoint = f"/hardware/byserial/{serial}"
        response = self._make_request('GET', endpoint)
        
        if response and 'rows' in response and len(response['rows']) > 0:
            asset = response['rows'][0]
            logger.debug(f"Found asset by serial '{serial}': {asset.get('id')}")
            return asset
        
        return None
    
    def get_asset(self, asset_id: int) -> Optional[Dict[str, Any]]:
        """
        Get asset by ID.
        
        Args:
            asset_id: Asset ID
            
        Returns:
            Asset data as dict if found, None otherwise
        """
        endpoint = f"/hardware/{asset_id}"
        response = self._make_request('GET', endpoint)
        
        if response:
            return response
        
        return None
    
    def checkout_asset(self, asset_id: int, location_id: Optional[int] = None, 
                      assigned_to: Optional[int] = None, notes: Optional[str] = None,
                      expected_checkin: Optional[str] = None) -> bool:
        """
        Checkout asset (assign to location or user).
        
        Args:
            asset_id: Asset ID to checkout
            location_id: Location ID to assign asset to
            assigned_to: User ID to assign asset to
            notes: Optional notes for checkout
            expected_checkin: Optional expected check-in date (ISO format)
            
        Returns:
            True if checkout successful, False otherwise
        """
        endpoint = f"/hardware/{asset_id}/checkout"
        
        payload: Dict[str, Any] = {}
        
        if location_id:
            payload['checkout_to_type'] = 'location'
            payload['assigned_location'] = location_id
        elif assigned_to:
            payload['checkout_to_type'] = 'user'
            payload['assigned_user'] = assigned_to
        else:
            logger.error("checkout_asset requires either location_id or assigned_to")
            return False
        
        if notes:
            payload['note'] = notes
        
        if expected_checkin:
            payload['expected_checkin'] = expected_checkin
        
        response = self._make_request('POST', endpoint, json=payload)
        
        if response:
            logger.info(f"Successfully checked out asset {asset_id} to {'location' if location_id else 'user'} {location_id or assigned_to}")
            return True
        else:
            logger.error(f"Failed to checkout asset {asset_id}")
            return False
    
    def checkin_asset(self, asset_id: int, location_id: Optional[int] = None,
                    notes: Optional[str] = None) -> bool:
        """
        Check-in asset (return to inventory/location).
        
        Args:
            asset_id: Asset ID to check in
            location_id: Location ID to return asset to (optional)
            notes: Optional notes for check-in
            
        Returns:
            True if check-in successful, False otherwise
        """
        endpoint = f"/hardware/{asset_id}/checkin"
        
        payload: Dict[str, Any] = {}
        
        if location_id:
            payload['location_id'] = location_id
        
        if notes:
            payload['note'] = notes
        
        response = self._make_request('POST', endpoint, json=payload)
        
        if response:
            logger.info(f"Successfully checked in asset {asset_id} to location {location_id or 'default'}")
            return True
        else:
            logger.error(f"Failed to check in asset {asset_id}")
            return False
    
    def update_asset(self, asset_id: int, **kwargs) -> bool:
        """
        Update asset fields.
        
        Args:
            asset_id: Asset ID to update
            **kwargs: Fields to update (e.g., status_id, location_id, etc.)
            
        Returns:
            True if update successful, False otherwise
        """
        endpoint = f"/hardware/{asset_id}"
        
        response = self._make_request('PATCH', endpoint, json=kwargs)
        
        if response:
            logger.info(f"Successfully updated asset {asset_id}")
            return True
        else:
            logger.error(f"Failed to update asset {asset_id}")
            return False
    
    def create_asset(self, asset_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create new asset.
        
        Note: This method is provided for completeness but the agent
        is configured to log unknown tags instead of auto-creating assets.
        
        Args:
            asset_data: Asset data dict (must include model_id, asset_tag, etc.)
            
        Returns:
            Created asset data as dict if successful, None otherwise
        """
        endpoint = "/hardware"
        
        response = self._make_request('POST', endpoint, json=asset_data)
        
        if response:
            logger.info(f"Successfully created new asset: {response.get('id')}")
            return response
        else:
            logger.error("Failed to create asset")
            return None

