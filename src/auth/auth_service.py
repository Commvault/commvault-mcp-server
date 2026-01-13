# --------------------------------------------------------------------------
# Copyright Commvault Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# --------------------------------------------------------------------------

import hmac
import ipaddress
import keyring
import os
import sys
import time
import threading
from functools import lru_cache
from fastmcp.server.dependencies import get_http_request

from src.logger import logger


class AuthService:
    # tracking for failed authentication attempts per client
    _failed_attempts = {}  # {client_ip: (attempt_count, next_allowed_time)}
    _attempt_lock = threading.Lock()
    
    def __init__(self):
        self.__service_name = "commvault-mcp-server"
        self.__access_token = None
        self.__refresh_token = None

        self.fetch_and_set_tokens()

    def get_tokens(self):
        return self.__access_token, self.__refresh_token
    
    def fetch_and_set_tokens(self):
        access_token = keyring.get_password(self.__service_name, "access_token")
        refresh_token = keyring.get_password(self.__service_name, "refresh_token")
        if access_token is None or refresh_token is None:
            logger.critical("Please set the tokens from command line before running the server. Refer to the documentation for more details.")
            sys.exit(1)
        self.__access_token = access_token
        self.__refresh_token = refresh_token
        return access_token, refresh_token
    
    def set_access_token(self, access_token: str):
        keyring.set_password(self.__service_name, "access_token", access_token)
        self.__access_token = access_token
    
    def set_refresh_token(self, refresh_token: str):
        keyring.set_password(self.__service_name, "refresh_token", refresh_token)
        self.__refresh_token = refresh_token

    def set_tokens(self, access_token: str, refresh_token: str):
        self.set_access_token(access_token)
        self.set_refresh_token(refresh_token)

    @lru_cache(maxsize=None)
    def _get_trusted_proxy_ips(self) -> set:
        trusted_proxies = os.getenv("TRUSTED_PROXY_IPS", "").strip()
        if not trusted_proxies:
            return set()
        
        proxy_ips = set()
        for ip_str in trusted_proxies.split(","):
            ip_str = ip_str.strip()
            if ip_str:
                try:
                    # Validate and normalize IP address
                    ip = ipaddress.ip_address(ip_str)
                    proxy_ips.add(str(ip))
                except ValueError:
                    logger.warning(f"Invalid trusted proxy IP in configuration: {ip_str}")
        
        return proxy_ips
    
    def _is_valid_ip(self, ip_str: str) -> bool:
        try:
            ipaddress.ip_address(ip_str.strip())
            return True
        except (ValueError, AttributeError):
            return False
    
    def _get_direct_connection_ip(self, request) -> str | None:
        if hasattr(request, "client") and request.client:
            return request.client.host
        return None
    
    def _get_client_ip(self, request) -> str | None:
        direct_ip = self._get_direct_connection_ip(request)
        
        trusted_proxies = self._get_trusted_proxy_ips()
        is_behind_trusted_proxy = direct_ip and direct_ip in trusted_proxies
        
        # Only trust X-Forwarded-For if behind a trusted proxy
        if is_behind_trusted_proxy:
            forwarded_for = request.headers.get("X-Forwarded-For")
            if forwarded_for:
                client_ip = forwarded_for.split(",")[0].strip()
                if self._is_valid_ip(client_ip):
                    logger.debug(f"Using X-Forwarded-For IP {client_ip} (trusted proxy: {direct_ip})")
                    return client_ip
                else:
                    logger.warning(
                        f"Invalid IP in X-Forwarded-For header: {client_ip}. "
                        f"Falling back to direct connection IP: {direct_ip}"
                    )
        
        # Use direct connection IP (default)
        return direct_ip
    
    def is_client_token_valid(self) -> (bool, str|None):
        request = get_http_request()
        client_ip = self._get_client_ip(request)
        if client_ip is None:
            logger.error("Authentication validation failed: Unable to determine client IP address")
            return False, "Unable to determine client IP address. Request rejected."
        
        current_time = time.time()
        
        # Check if client is rate-limited
        with self._attempt_lock:
            if client_ip in self._failed_attempts:
                attempt_count, next_allowed_time = self._failed_attempts[client_ip]
                if current_time < next_allowed_time:
                    remaining_time = next_allowed_time - current_time
                    logger.warning(
                        f"Authentication attempt from {client_ip} rejected: "
                        f"rate limited (attempt #{attempt_count + 1}, "
                        f"wait {remaining_time:.1f}s)"
                    )
                    return False, "Too many attempts. Please try again later."
        
        auth_header = request.headers.get("Authorization")
        if auth_header is None:
            logger.error("Authentication validation failed: No Authorization header")
            return False, "Missing token in request."
        
        mcp_client_token = auth_header.split(" ")[1] if auth_header.startswith("Bearer ") else auth_header
        secret = keyring.get_password(self.__service_name, "server_secret")

        if secret is None:
            logger.error("Authentication validation failed: Server secrets missing")
            return False, "Server secrets missing. Please check server configuration."

        expiry_str = keyring.get_password(self.__service_name, "server_secret_expiry")
        if expiry_str is None:
            logger.error("Authentication validation failed: Server secret expiry not set")
            return False, "Server secret expiry is not set. Please rerun the setup script to configure the server secret with an expiration date."
        
        try:
            expiry_timestamp = float(expiry_str)
            if current_time > expiry_timestamp:
                expiry_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(expiry_timestamp))
                logger.error(
                    f"Authentication validation failed: Server secret expired on {expiry_date}"
                )
                return False, f"Server secret has expired. Please regenerate the server secret using the setup script."
        except (ValueError, TypeError) as e:
            logger.error(f"Authentication validation failed: Invalid expiry timestamp format: {e}")
            return False, "Server secret expiry has an invalid format. Please rerun the setup script to regenerate the server secret."

        if not hmac.compare_digest(mcp_client_token, secret):
            logger.warning("Authentication validation failed: Secret mismatch")
            self._record_failed_attempt(client_ip)
            return False, "Invalid token."

        self._reset_failed_attempts(client_ip)
        return True, None
    
    def _record_failed_attempt(self, client_ip: str) -> None:
        """Record a failed authentication attempt and calculate next allowed time."""
        with self._attempt_lock:
            if client_ip in self._failed_attempts:
                attempt_count, _ = self._failed_attempts[client_ip]
                attempt_count += 1
            else:
                attempt_count = 1
            
            # exponential backoff delay
            delay = min(0.5 * (2 ** attempt_count), 60.0)
            next_allowed_time = time.time() + delay
            
            self._failed_attempts[client_ip] = (attempt_count, next_allowed_time)
            logger.info(
                f"Failed authentication attempt #{attempt_count} from {client_ip}. "
                f"Next attempt allowed after {delay:.1f}s"
            )
    
    def _reset_failed_attempts(self, client_ip: str) -> None:
        """Reset failed attempt counter for a client after successful authentication."""
        with self._attempt_lock:
            if client_ip in self._failed_attempts:
                del self._failed_attempts[client_ip]
                logger.debug(f"Reset failed attempt counter for {client_ip} after successful authentication")
