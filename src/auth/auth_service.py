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
import keyring
import sys
import time
import threading
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
    
    def _get_client_ip(self, request) -> str:
        # Check X-Forwarded-For header first (for reverse proxies)
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            # Take the first IP if multiple are present
            return forwarded_for.split(",")[0].strip()
        
        # Fall back to direct client connection
        if hasattr(request, "client") and request.client:
            return request.client.host
        
        return "unknown"
    
    def is_client_token_valid(self) -> (bool, str|None):
        request = get_http_request()
        client_ip = self._get_client_ip(request)
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
